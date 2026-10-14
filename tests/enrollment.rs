//! Runs the wire v1 conformance traces of tests/enrollment on a reference server and the shared client checks
//! (docs/design/enrollment.md § Conformance traces). Chain: cargo test --locked.

use coreq::enroll;
use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::fs;

const PAIRING_TTL: u64 = 600;

struct Pairing {
    request: Value,
    user_code: String,
    created: u64,
    last_poll: Option<u64>,
    interval: u64,
    approved: Option<(String, String)>,
    denied: bool,
    credential: Option<String>,
    acked: bool,
}

struct Connection {
    installation: String,
    generation: u64,
    product: Value,
    workspace: String,
    schemas: Vec<String>,
    capabilities: Option<Value>,
    device_code: String,
    active: bool,
    revoked: bool,
}

struct Event {
    record: Value,
    projected: bool,
}

/// In-memory model of DashQ's machine routes; DashQ's own handlers run the same traces.
// ponytail: no rate limits, request size caps or retention windows (C5); add with DashQ's handler qualification.
#[derive(Default)]
struct Server {
    seq: u64,
    pairings: HashMap<String, Pairing>,
    connections: HashMap<String, Connection>,
    generations: HashMap<String, u64>,
    events: HashMap<(String, u64, String), Event>,
    /// Queued operations and their stored result.
    operations: Vec<(Value, Option<Value>)>,
}

fn err(status: u16, code: &str) -> (u16, Value) {
    (status, json!({"protocol": 1, "error": code}))
}

impl Server {
    fn next(&mut self, prefix: &str) -> String {
        self.seq += 1;
        format!("{prefix}-{}", self.seq)
    }

    fn call(&mut self, at: u64, route: &str, auth: Option<&str>, req: &Value) -> (u16, Value) {
        if req["protocol"].as_u64() != Some(enroll::PROTOCOL) {
            return err(400, "unsupported_protocol");
        }
        match route {
            "/connect/device" => self.device(at, req),
            "/connect/token" => self.token(at, req),
            _ => {
                let Some(cred) = auth.filter(|c| self.connections.contains_key(*c)) else {
                    return err(401, "invalid_credential");
                };
                let c = &self.connections[cred];
                if route == "/connection/revoke" {
                    let generation = c.generation;
                    self.connections.get_mut(cred).unwrap().revoked = true;
                    return (200, json!({"protocol": 1, "state": "revoked", "generation": generation}));
                }
                if c.revoked {
                    return err(401, "revoked");
                }
                match route {
                    "/connect/ack" => self.ack(cred),
                    "/ingest" if !c.active => err(403, "not_active"),
                    "/ingest" => self.ingest(cred, req),
                    "/connection/receipt" => self.receipt(cred, req),
                    "/operations" => self.operations(at, cred, req),
                    "/operations/result" => self.result(cred, req),
                    _ => err(404, "unknown_route"),
                }
            }
        }
    }

    fn device(&mut self, at: u64, req: &Value) -> (u16, Value) {
        let device_code = self.next("device");
        let user_code = format!("UC{:06}", self.seq);
        self.pairings.insert(device_code.clone(), Pairing {
            request: req.clone(),
            user_code: user_code.clone(),
            created: at,
            last_poll: None,
            interval: 5,
            approved: None,
            denied: false,
            credential: None,
            acked: false,
        });
        (200, json!({"protocol": 1, "device_code": device_code, "user_code": user_code,
            "verification_uri": "https://dash.example/connect", "expires_in": PAIRING_TTL, "interval": 5}))
    }

    fn browser(&mut self, at: u64, action: &str, user_code: &str, account: &str, workspace: &str) -> &'static str {
        let Some(p) = self.pairings.values_mut().find(|p| p.user_code == user_code) else { return "invalid" };
        if at >= p.created + PAIRING_TTL || p.acked || p.denied {
            return "invalid";
        }
        if p.approved.is_some() {
            return "already approved";
        }
        if action == "deny" {
            p.denied = true;
            return "denied";
        }
        p.approved = Some((account.into(), workspace.into()));
        "approved"
    }

    fn token(&mut self, at: u64, req: &Value) -> (u16, Value) {
        let code = req["device_code"].as_str().unwrap_or_default().to_string();
        let Some(p) = self.pairings.get_mut(&code) else { return err(400, "invalid_grant") };
        if p.acked {
            return err(400, "invalid_grant");
        }
        if at >= p.created + PAIRING_TTL {
            return err(400, "expired_token");
        }
        if p.denied {
            return err(400, "access_denied");
        }
        let Some((_, workspace)) = p.approved.clone() else {
            let early = p.last_poll.is_some_and(|l| at < l + p.interval);
            p.last_poll = Some(at);
            if early {
                p.interval += 5;
                return err(400, "slow_down");
            }
            return err(400, "authorization_pending");
        };
        let r = p.request.clone();
        let cred = match p.credential.clone() {
            Some(c) => c,
            None => {
                let c = self.next("cred");
                let installation = r["installation"].as_str().unwrap_or_default().to_string();
                let generation = self.generations.entry(installation.clone()).or_default();
                *generation += 1;
                self.connections.insert(c.clone(), Connection {
                    installation,
                    generation: *generation,
                    product: r["product"].clone(),
                    workspace,
                    schemas: r["schemas"].as_array().into_iter().flatten().filter_map(|s| s.as_str().map(String::from)).collect(),
                    capabilities: r.get("capabilities").cloned(),
                    device_code: code.clone(),
                    active: false,
                    revoked: false,
                });
                self.pairings.get_mut(&code).unwrap().credential = Some(c.clone());
                c
            }
        };
        let c = &self.connections[&cred];
        let mut r = json!({"protocol": 1, "credential": cred, "installation": c.installation, "generation": c.generation,
            "product": c.product, "workspace": c.workspace, "schemas": c.schemas});
        if let Some(caps) = &c.capabilities {
            r["capabilities"] = caps.clone();
        }
        (200, r)
    }

    fn ack(&mut self, cred: &str) -> (u16, Value) {
        let (installation, generation, code) = {
            let c = &self.connections[cred];
            (c.installation.clone(), c.generation, c.device_code.clone())
        };
        for c in self.connections.values_mut().filter(|c| c.installation == installation) {
            c.revoked |= c.generation < generation;
        }
        self.connections.get_mut(cred).unwrap().active = true;
        self.pairings.get_mut(&code).unwrap().acked = true;
        (200, json!({"protocol": 1, "state": "active", "generation": generation}))
    }

    fn ingest(&mut self, cred: &str, req: &Value) -> (u16, Value) {
        let c = &self.connections[cred];
        let mut receipts = vec![];
        for r in req["records"].as_array().into_iter().flatten() {
            let id = r["event_id"].as_str().unwrap_or_default().to_string();
            let schema = r["schema"].as_str().unwrap_or_default();
            let key = (c.installation.clone(), c.generation, id.clone());
            let receipt = if !c.schemas.iter().any(|s| s == schema) {
                json!({"event_id": id, "state": "rejected", "reason": "scope"})
            } else if let Some(e) = self.events.get(&key) {
                if e.record == *r {
                    json!({"event_id": id, "state": "accepted"})
                } else {
                    json!({"event_id": id, "state": "rejected", "reason": "replay_mismatch"})
                }
            } else {
                self.events.insert(key, Event { record: r.clone(), projected: false });
                json!({"event_id": id, "state": "accepted"})
            };
            receipts.push(receipt);
        }
        (200, json!({"protocol": 1, "receipts": receipts}))
    }

    fn operations(&self, at: u64, cred: &str, req: &Value) -> (u16, Value) {
        let c = &self.connections[cred];
        let cap = &req["capability"];
        if !c.capabilities.as_ref().and_then(Value::as_array).is_some_and(|g| g.contains(cap)) {
            return err(403, "scope");
        }
        let ops: Vec<&Value> = self.operations.iter().filter(|(o, res)| res.is_none() && o["capability"] == *cap
            && o["installation"] == c.installation.as_str() && o["generation"] == c.generation && at < o["expires_at"].as_u64().unwrap_or(0))
            .map(|(o, _)| o).take(16).collect();
        (200, json!({"protocol": 1, "operations": ops}))
    }

    /// Queues an operation only under the server's own authority: its actor approved a grant in the workspace of
    /// the operation's grant; payload claims play no part.
    fn queue(&mut self, op: Value) -> &'static str {
        let a = &op["authority"];
        let grant = self.connections.values().find(|c| op["installation"] == c.installation.as_str() && op["generation"] == c.generation);
        let authorized = grant.is_some_and(|c| a["workspace"] == c.workspace.as_str()
            && self.pairings.values().any(|p| p.approved.as_ref().is_some_and(|(acct, ws)| a["actor"] == acct.as_str() && *ws == c.workspace)));
        if !authorized {
            return "refused";
        }
        self.operations.push((op, None));
        "queued"
    }

    fn result(&mut self, cred: &str, req: &Value) -> (u16, Value) {
        let c = &self.connections[cred];
        let id = req["operation"].clone();
        let rejected = |reason| (200, json!({"protocol": 1, "operation": id, "state": "rejected", "reason": reason}));
        let Some((op, res)) = self.operations.iter_mut().find(|(o, _)| o["operation"] == id && o["installation"] == c.installation.as_str() && o["generation"] == c.generation) else {
            return rejected("unknown_operation");
        };
        if op["digest"] != req["digest"] {
            return rejected("digest_mismatch");
        }
        match res {
            Some(stored) if *stored != req["result"] => return rejected("replay_mismatch"),
            Some(_) => {}
            None => *res = Some(req["result"].clone()),
        }
        (200, json!({"protocol": 1, "operation": id, "state": "accepted"}))
    }

    fn receipt(&self, cred: &str, req: &Value) -> (u16, Value) {
        let c = &self.connections[cred];
        let id = req["event_id"].as_str().unwrap_or_default().to_string();
        let Some(e) = self.events.get(&(c.installation.clone(), c.generation, id.clone())) else {
            return err(404, "unknown_event");
        };
        (200, json!({"protocol": 1, "installation": c.installation, "generation": c.generation,
            "schema": e.record["schema"], "event_id": id, "state": if e.projected { "projected" } else { "accepted" }}))
    }
}

/// Exact match where `"$name"` binds on first use; a new binding never repeats another's value.
fn matches(want: &Value, got: &Value, vars: &mut HashMap<String, Value>, at: &str) {
    match (want, got) {
        (Value::String(s), _) if s.starts_with('$') => match vars.get(s) {
            Some(v) => assert_eq!(v, got, "{at}: {s}"),
            None => {
                assert!(!vars.values().any(|v| v == got), "{at}: {s} repeats a bound value {got}");
                vars.insert(s.clone(), got.clone());
            }
        },
        (Value::Object(w), Value::Object(g)) => {
            let (mut wk, mut gk): (Vec<_>, Vec<_>) = (w.keys().collect(), g.keys().collect());
            wk.sort();
            gk.sort();
            assert_eq!(wk, gk, "{at}: keys");
            for (k, v) in w {
                matches(v, &g[k], vars, &format!("{at}.{k}"));
            }
        }
        (Value::Array(w), Value::Array(g)) => {
            assert_eq!(w.len(), g.len(), "{at}: length");
            for (i, (w, g)) in w.iter().zip(g).enumerate() {
                matches(w, g, vars, &format!("{at}[{i}]"));
            }
        }
        _ => assert_eq!(want, got, "{at}"),
    }
}

fn subst(v: &Value, vars: &HashMap<String, Value>) -> Value {
    match v {
        Value::String(s) if s.starts_with('$') => vars.get(s).cloned().unwrap_or_else(|| panic!("unbound {s}")),
        Value::Object(m) => Value::Object(m.iter().map(|(k, v)| (k.clone(), subst(v, vars))).collect::<Map<_, _>>()),
        Value::Array(a) => Value::Array(a.iter().map(|v| subst(v, vars)).collect()),
        _ => v.clone(),
    }
}

fn run(name: &str, trace: &Value) {
    assert_eq!(trace["contract"], enroll::CONTRACT, "{name}");
    let mut server = Server::default();
    let mut vars = HashMap::new();
    for (i, step) in trace["steps"].as_array().unwrap().iter().enumerate() {
        let at_s = format!("{name} step {i}");
        let at = step["at"].as_u64().unwrap_or(0);
        if step["server"] == "project" {
            server.events.values_mut().for_each(|e| e.projected = true);
        } else if step["server"] == "queue" {
            let op = subst(&step["operation"], &vars);
            assert_eq!(op["digest"], enroll::digest(&op["payload"]), "{at_s}: digest");
            assert_eq!(server.queue(op), step["result"], "{at_s}");
        } else if let Some(action) = step["browser"].as_str() {
            let s = subst(step, &vars);
            let got = server.browser(at, action, s["user_code"].as_str().unwrap(), s["account"].as_str().unwrap(),
                s["workspace"].as_str().unwrap());
            assert_eq!(got, step["result"], "{at_s}");
        } else {
            let auth = step.get("auth").map(|a| subst(a, &vars));
            let (status, resp) = server.call(at, step["route"].as_str().unwrap(), auth.as_ref().and_then(Value::as_str),
                &subst(&step["request"], &vars));
            assert_eq!(status, step["status"], "{at_s}: {resp}");
            matches(&step["response"], &resp, &mut vars, &at_s);
            assert_eq!(enroll::check(&resp), Ok(()), "{at_s}");
            if let Some(v) = step.get("verify") {
                let v = subst(v, &vars);
                let got = enroll::verify(&resp, v["installation"].as_str().unwrap(), v["generation"].as_u64().unwrap(),
                    v["schema"].as_str().unwrap(), v["event_id"].as_str().unwrap());
                assert_eq!(got.err().unwrap_or("connected".into()), step["client"], "{at_s}");
            }
        }
    }
}

#[test]
fn conformance_traces() {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/enrollment");
    let mut products = vec![];
    for e in fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        let name = p.file_name().unwrap().to_string_lossy().into_owned();
        let trace: Value = serde_json::from_str(&fs::read_to_string(&p).unwrap()).unwrap();
        run(&name, &trace);
        products.push(trace["product"].as_str().unwrap().to_string());
    }
    for p in ["agentq", "serverq"] {
        assert!(products.iter().any(|x| x == p), "no {p} trace");
    }
}

/// Reference servers per destination, a test clock and a user who approves or denies the shown code.
/// `crash` loses the reply of that call after the server processed it, like a client crash.
struct TestWire {
    servers: HashMap<String, Server>,
    at: u64,
    calls: usize,
    crash: Option<usize>,
    /// Seconds a wait is short of what the client asked for; makes polls early.
    skew: u64,
    user: &'static str,
    /// The user acts at this wait after the code is shown.
    waits: usize,
    last: String,
    shown: Option<(String, String, usize)>,
    errors: Vec<String>,
    /// No server is reached: every call fails without a reply.
    offline: bool,
    /// The next reply, instead of the server's.
    inject: Option<(u16, Value)>,
}

const A: &str = "https://a.example";
const B: &str = "https://b.example";

impl TestWire {
    fn new(user: &'static str) -> TestWire {
        let servers = [A, B].map(|d| (d.to_string(), Server::default())).into();
        TestWire { servers, at: 1000, calls: 0, crash: None, skew: 0, user, waits: 1, last: String::new(), shown: None, errors: vec![], offline: false, inject: None }
    }
}

impl enroll::Wire for TestWire {
    fn post(&mut self, destination: &str, route: &str, auth: Option<&str>, body: &Value) -> Result<(u16, Value), String> {
        self.last = destination.into();
        if self.offline {
            return Err("offline".into());
        }
        if let Some(r) = self.inject.take() {
            return Ok(r);
        }
        let (status, r) = self.servers.get_mut(destination).unwrap().call(self.at, route, auth, body);
        if let Some(e) = r["error"].as_str() {
            self.errors.push(e.into());
        }
        self.calls += 1;
        self.at += 1;
        if self.crash == Some(self.calls) { Err("lost reply".into()) } else { Ok((status, r)) }
    }

    fn now(&self) -> u64 {
        self.at
    }

    fn sleep(&mut self, secs: u64) {
        self.at += secs - self.skew;
        if let Some((d, code, left)) = &mut self.shown {
            *left -= 1;
            if *left > 0 {
                return;
            }
            let (d, code) = (d.clone(), code.clone());
            self.shown = None;
            self.servers.get_mut(&d).unwrap().browser(self.at, self.user, &code, "acct-1", "ws-1");
        }
    }

    fn show(&mut self, _uri: &str, user_code: &str) {
        self.shown = Some((self.last.clone(), user_code.into(), self.waits));
    }
}

fn dir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("coreq-enroll-{tag}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&d);
    d
}

const REQ: enroll::Request = enroll::Request { installation: None, product: "agentq", host_label: "mac-1", schemas: &["agentq-snapshot/1"], scope: None, capabilities: &[] };

/// Runs `pair` losing the k-th reply for every k, then once more without loss; it always ends with one active connection.
fn pair_through_crashes(tag: &str, destination: &str, before: impl Fn(&std::path::Path, &mut TestWire)) {
    for k in 1.. {
        let d = dir(&format!("{tag}-{k}"));
        let mut w = TestWire::new("approve");
        before(&d, &mut w);
        let old = enroll::active(&d).unwrap();
        w.crash = Some(w.calls + k);
        let first = enroll::pair(&d, destination, &REQ, &mut w);
        if first.is_ok() && w.calls < w.crash.unwrap() {
            return;
        }
        let now = enroll::active(&d).unwrap();
        assert!(now == old || now.as_ref().is_some_and(|a| a["destination"] == destination), "{tag} k={k}: neither old nor new active");
        fs::write(d.join("connection.json.tmp"), "torn write").unwrap();
        w.crash = None;
        if first.is_ok() {
            // only the old destination's revoke lost its reply: it stays queued for the next pass
            assert!(enroll::retry_revokes(&d, &mut w).is_ok(), "{tag} k={k}");
        }
        let a = enroll::pair(&d, destination, &REQ, &mut w).unwrap_or_else(|e| panic!("{tag} k={k}: {e}"));
        assert_eq!(enroll::active(&d).unwrap().as_ref(), Some(&a));
        assert_eq!(a["destination"], destination);
        let c = &w.servers[destination].connections[a["credential"].as_str().unwrap()];
        assert!(c.active && !c.revoked, "{tag} k={k}: server agrees");
        let grants = w.servers[destination].connections.values().filter(|c| c.active && !c.revoked).count();
        assert_eq!(grants, 1, "{tag} k={k}: one active grant");
        if let Some(old) = old.filter(|o| o["destination"] != destination) {
            let oc = &w.servers[old["destination"].as_str().unwrap()].connections[old["credential"].as_str().unwrap()];
            assert!(oc.revoked, "{tag} k={k}: old destination revoked");
        }
        let s: Value = serde_json::from_str(&fs::read_to_string(d.join("connection.json")).unwrap()).unwrap();
        assert_eq!(s["revoke"], json!([]), "{tag} k={k}");
        assert_eq!(s["pending"], Value::Null, "{tag} k={k}");
    }
}

#[test]
fn client_pairs_and_switches_through_crashes() {
    pair_through_crashes("first", A, |_, _| {});
    pair_through_crashes("switch", B, |d, w| {
        enroll::pair(d, A, &REQ, w).unwrap();
    });
}

#[test]
fn client_is_idempotent_and_polls_slower() {
    let d = dir("idem");
    let mut w = TestWire::new("approve");
    w.skew = 2;
    w.waits = 3;
    let a = enroll::pair(&d, A, &REQ, &mut w).unwrap();
    assert!(w.errors.iter().any(|e| e == "slow_down"), "{:?}", w.errors);
    let calls = w.calls;
    assert_eq!(enroll::pair(&d, A, &REQ, &mut w).unwrap(), a);
    assert_eq!(w.calls, calls, "same destination: no request");
    let s = fs::read_to_string(d.join("connection.json")).unwrap();
    let out = format!("{:?}", w.errors);
    assert!(!out.contains(a["credential"].as_str().unwrap()) && s.contains(a["credential"].as_str().unwrap()));
    assert_eq!(enroll::pair(&d, "http://a.example", &REQ, &mut w), Err("destination must be https".into()));
    let mut h = enroll::Https { browser: false };
    assert_eq!(enroll::Wire::post(&mut h, A, "/ingest", Some("x\noutput = /tmp/x"), &json!({})), Err("control character in request".into()));
}

/// An optional scope rides on `/connect/device` and is part of the request identity; none omits the field.
#[test]
fn scope_is_sent_only_when_given() {
    let d = dir("scope");
    let mut w = TestWire::new("approve");
    enroll::pair(&d, A, &REQ, &mut w).unwrap();
    let sent = |w: &TestWire| w.servers[A].pairings.values().map(|p| p.request["scope"].clone()).collect::<Vec<_>>();
    assert_eq!(sent(&w), vec![Value::Null]);
    let req = enroll::Request { scope: Some(("taskq-board/1", "owner/repo")), ..REQ };
    let a = enroll::pair(&d, A, &req, &mut w).unwrap();
    let scope = json!({"name": "taskq-board/1", "value": "owner/repo"});
    assert!(sent(&w).contains(&scope), "{:?}", sent(&w));
    assert_eq!(a["scope"], scope);
    let calls = w.calls;
    assert_eq!(enroll::pair(&d, A, &req, &mut w).unwrap(), a);
    assert_eq!(w.calls, calls, "same scope: no request");
}

/// A caller id is sent and echoed unchanged; changing it re-pairs under the new id and revokes the old grant.
#[test]
fn caller_installation_is_sent_and_echoed() {
    let d = dir("caller-id");
    let mut w = TestWire::new("approve");
    let old = enroll::pair(&d, A, &REQ, &mut w).unwrap();
    assert_eq!(old["installation"].as_str().unwrap().len(), 36, "None keeps the UUID");
    let id = "ab".repeat(32);
    let req = enroll::Request { installation: Some(&id), ..REQ };
    let a = enroll::pair(&d, A, &req, &mut w).unwrap();
    assert_eq!(a["installation"], id.as_str());
    assert!(w.servers[A].pairings.values().any(|p| p.request["installation"] == id.as_str()));
    assert!(w.servers[A].connections[old["credential"].as_str().unwrap()].revoked, "old identity revoked");
    let calls = w.calls;
    assert_eq!(enroll::pair(&d, A, &req, &mut w).unwrap(), a);
    assert_eq!(w.calls, calls, "same id: no request");
    assert_eq!(enroll::pair(&d, A, &REQ, &mut w).unwrap(), a, "None keeps the stored id");
    let r = enroll::stamp(&d, "agentq-snapshot/1", "snap-1", 1000, json!({})).unwrap();
    enroll::deliver(&d, &[r.clone()], &mut enroll::Backoff::default(), &mut w).unwrap();
    w.servers.get_mut(A).unwrap().events.values_mut().for_each(|e| e.projected = true);
    assert_eq!(enroll::receipt(&d, &r, &mut w), Ok("projected"), "receipt names the caller id");
    let bad = "x".repeat(65);
    assert_eq!(enroll::pair(&d, A, &enroll::Request { installation: Some(&bad), ..REQ }, &mut w), Err("bad installation".into()));
    assert_eq!(enroll::pair(&d, A, &enroll::Request { installation: Some("a b"), ..REQ }, &mut w), Err("bad installation".into()));
}

#[test]
fn deny_cancel_and_disconnect_leave_no_active_connection() {
    let d = dir("deny");
    let mut w = TestWire::new("deny");
    assert_eq!(enroll::pair(&d, A, &REQ, &mut w), Err("access_denied".into()));
    assert_eq!(enroll::active(&d), Ok(None));
    assert!(w.servers[A].connections.is_empty());

    // cancel after the token reply, before ack: staged credential is revoked, nothing active
    let d = dir("cancel");
    let mut w = TestWire::new("approve");
    w.crash = Some(4); // device, pending poll, token, ack
    assert!(enroll::pair(&d, A, &REQ, &mut w).is_err());
    assert!(w.servers[A].connections.values().all(|c| c.active), "server acked");
    w.crash = None;
    enroll::cancel(&d, &mut w).unwrap();
    assert_eq!(enroll::active(&d), Ok(None));
    assert!(w.servers[A].connections.values().all(|c| c.revoked));

    // disconnect offline: sending stops at once, the revoke waits for the server
    let d = dir("disconnect");
    let mut w = TestWire::new("approve");
    let a = enroll::pair(&d, A, &REQ, &mut w).unwrap();
    w.crash = Some(w.calls + 1);
    assert!(enroll::disconnect(&d, &mut w).is_err());
    assert_eq!(enroll::active(&d), Ok(None));
    w.crash = None;
    enroll::retry_revokes(&d, &mut w).unwrap();
    assert!(w.servers[A].connections[a["credential"].as_str().unwrap()].revoked);
}

fn state(d: &std::path::Path) -> Value {
    serde_json::from_str(&fs::read_to_string(d.join("connection.json")).unwrap()).unwrap()
}

/// AgentQ and ServerQ fixtures: the same client, no product branch, only product and schema differ.
#[test]
fn two_consumers_deliver_and_verify() {
    for (product, schema) in [("agentq", "agentq-snapshot/1"), ("serverq", "serverq-report/1")] {
        let d = dir(&format!("deliver-{product}"));
        let mut w = TestWire::new("approve");
        let req = enroll::Request { installation: None, product, host_label: "host-1", schemas: &[schema], scope: None, capabilities: &[] };
        let a = enroll::pair(&d, A, &req, &mut w).unwrap();
        let r = enroll::stamp(&d, schema, "snap-1", 1000, json!({"ok": true})).unwrap();
        let foreign = enroll::stamp(&d, "other/1", "x-1", 1000, json!({})).unwrap();
        let mut b = enroll::Backoff::default();
        let got = enroll::deliver(&d, &[r.clone(), foreign], &mut b, &mut w).unwrap();
        assert_eq!(got, vec![json!({"event_id": "snap-1", "state": "accepted"}),
            json!({"event_id": "x-1", "state": "rejected", "reason": "scope"})], "{product}");
        assert_eq!(enroll::receipt(&d, &r, &mut w), Ok("accepted"), "{product}: durable, not yet Connected");
        w.servers.get_mut(A).unwrap().events.values_mut().for_each(|e| e.projected = true);
        assert_eq!(enroll::receipt(&d, &r, &mut w), Ok("projected"), "{product}: Connected");
        let key = (a["installation"].as_str().unwrap().to_string(), a["generation"].as_u64().unwrap(), "snap-1".to_string());
        assert_eq!(w.servers[A].events[&key].record["payload"], json!({"ok": true}), "{product}");
    }
}

#[test]
fn delivery_retries_bounded_with_the_same_event() {
    let d = dir("retry");
    let mut w = TestWire::new("approve");
    enroll::pair(&d, A, &REQ, &mut w).unwrap();
    let r = enroll::stamp(&d, "agentq-snapshot/1", "snap-1", 1000, json!({"n": 1})).unwrap();
    let mut b = enroll::Backoff::default();
    w.crash = Some(w.calls + 1);
    assert_eq!(enroll::deliver(&d, &[r.clone()], &mut b, &mut w), Err(enroll::Delivery::Retry(1)), "lost reply");
    w.inject = Some((429, json!({"protocol": 1, "error": "rate_limited", "retry_after": 7})));
    assert_eq!(enroll::deliver(&d, &[r.clone()], &mut b, &mut w), Err(enroll::Delivery::Retry(7)), "429 Retry-After");
    w.inject = Some((503, json!({"protocol": 1, "error": "unavailable"})));
    assert!(matches!(enroll::deliver(&d, &[r.clone()], &mut b, &mut w), Err(enroll::Delivery::Retry(2..=4))));
    w.inject = Some((200, json!({"protocol": 2})));
    assert_eq!(enroll::deliver(&d, &[r.clone()], &mut b, &mut w), Err(enroll::Delivery::Stopped("unsupported protocol 2".into())));
    let got = enroll::deliver(&d, &[r.clone()], &mut b, &mut w).unwrap();
    assert_eq!(got, vec![json!({"event_id": "snap-1", "state": "accepted"})], "replay of the lost send");
    assert_eq!(w.servers[A].events.len(), 1, "one stored event");
    let mut changed = r.clone();
    changed["payload"] = json!({"n": 2});
    assert_eq!(enroll::deliver(&d, &[changed], &mut b, &mut w).unwrap()[0]["reason"], "replay_mismatch");
    let many: Vec<Value> = (0..=enroll::MAX_RECORDS).map(|i| enroll::stamp(&d, "agentq-snapshot/1", &format!("e{i}"), 1, json!({})).unwrap()).collect();
    assert_eq!(enroll::deliver(&d, &many, &mut b, &mut w), Err(enroll::Delivery::Stopped("batch over limit".into())));
}

#[test]
fn disconnect_and_reenrollment_never_revive_old_grants() {
    let d = dir("revive");
    let mut w = TestWire::new("approve");
    let old = enroll::pair(&d, A, &REQ, &mut w).unwrap();
    let r = enroll::stamp(&d, "agentq-snapshot/1", "snap-1", 1000, json!({})).unwrap();
    let mut b = enroll::Backoff::default();

    // offline disconnect: no send, a revoke-only record, honest error
    w.offline = true;
    assert_eq!(enroll::disconnect(&d, &mut w), Err("1 revoke pending".into()));
    let calls = w.calls;
    assert_eq!(enroll::deliver(&d, &[r.clone()], &mut b, &mut w), Err(enroll::Delivery::Stopped("disconnected".into())));
    assert_eq!(enroll::stamp(&d, "agentq-snapshot/1", "snap-2", 1, json!({})), Err("disconnected".into()));
    assert_eq!(enroll::receipt(&d, &r, &mut w), Err("disconnected".into()));
    let s = state(&d);
    assert_eq!(s["revoke"], json!([{"destination": A, "credential": old["credential"]}]), "revoke-only");
    assert_eq!(s["active"], Value::Null);

    // bounded: repeated offline disconnects keep at most 16 revoke records
    for _ in 0..20 {
        let mut s = state(&d);
        s["active"] = json!({"destination": A, "credential": "cred-x"});
        fs::write(d.join("connection.json"), s.to_string()).unwrap();
        assert!(enroll::disconnect(&d, &mut w).is_err());
    }
    assert_eq!(state(&d)["revoke"].as_array().unwrap().len(), 16);
    assert_eq!(w.calls, calls, "nothing reached a server");

    // restart and reenroll: a new generation; the old record is bound to the old grant and never sent
    w.offline = false;
    let new = enroll::pair(&d, A, &REQ, &mut w).unwrap();
    assert_eq!(new["generation"], 2);
    assert_ne!(new["credential"], old["credential"]);
    assert!(w.servers[A].connections[old["credential"].as_str().unwrap()].revoked, "old grant stays dead");
    let got = enroll::deliver(&d, &[r.clone()], &mut b, &mut w).unwrap();
    assert_eq!(got, vec![json!({"event_id": "snap-1", "state": "rejected", "reason": "grant"})]);
    assert!(w.servers[A].events.is_empty(), "old event never enters the new generation");
    assert_eq!(enroll::receipt(&d, &r, &mut w), Err("grant mismatch".into()));
    let _ = enroll::retry_revokes(&d, &mut w);
    assert_eq!(state(&d)["revoke"], json!([]), "200 or 401 ends every revoke");

    // server-side revoke: 401 ends the connection, nothing resumes it
    let r2 = enroll::stamp(&d, "agentq-snapshot/1", "snap-2", 1000, json!({})).unwrap();
    w.servers.get_mut(A).unwrap().connections.get_mut(new["credential"].as_str().unwrap()).unwrap().revoked = true;
    assert_eq!(enroll::deliver(&d, &[r2.clone()], &mut b, &mut w), Err(enroll::Delivery::Stopped("revoked".into())));
    assert_eq!(enroll::active(&d), Ok(None));
    assert_eq!(enroll::deliver(&d, &[r2], &mut b, &mut w), Err(enroll::Delivery::Stopped("disconnected".into())));
}

fn op(a: &Value, id: &str, generation: Value, expires_at: u64, payload: Value) -> Value {
    json!({"operation": id.repeat(32), "capability": "agentq.configure", "installation": a["installation"], "generation": generation,
        "expires_at": expires_at, "digest": enroll::digest(&payload), "payload": payload,
        "authority": {"actor": "acct-1", "workspace": a["workspace"], "profiles": ["p1"]}})
}

/// AgentQ configure delivery: only the agentq.configure grant pulls; out-of-scope, stale, expired or altered operations
/// never reach the executor; a result repeats to the same receipt and a lost reply retries unchanged.
#[test]
fn operations_pull_and_post_results() {
    let d = dir("ops");
    let mut w = TestWire::new("approve");
    let mut b = enroll::Backoff::default();
    let observe = enroll::pair(&d, A, &REQ, &mut w).unwrap();
    assert_eq!(enroll::pull(&d, "agentq.configure", &mut b, &mut w), Err(enroll::Delivery::Stopped("scope".into())), "observation grant");
    let req = enroll::Request { capabilities: &["agentq.configure"], ..REQ };
    let a = enroll::pair(&d, A, &req, &mut w).unwrap();
    assert_eq!(a["capabilities"], json!(["agentq.configure"]));
    assert_eq!(a["generation"], 2, "a capability needs a new approval");
    assert!(w.servers[A].connections[observe["credential"].as_str().unwrap()].revoked);

    let now = w.at;
    let good = op(&a, "a", json!(2), now + 300, json!({"kind": "apply", "changes": [{"profile": "p1", "field": "routing", "value": "direct"}]}));
    let mut altered = op(&a, "b", json!(2), now + 300, json!({"n": 1}));
    altered["payload"] = json!({"n": 2});
    let mut foreign = op(&a, "c", json!(2), now + 300, json!({}));
    foreign["capability"] = json!("serverq.restart");
    // authority comes from the server: missing or malformed fails closed, another workspace is refused
    let with = |id: &str, authority: Value| {
        let mut o = op(&a, id, json!(2), now + 300, json!({}));
        o["authority"] = authority;
        o
    };
    let ops = vec![good.clone(), altered, foreign, op(&a, "d", json!(1), now + 300, json!({})), op(&a, "e", json!(2), now, json!({})),
        op(&a, "f", json!(2), now + 300, json!({"big": "x".repeat(enroll::MAX_PAYLOAD)})), op(&a, "G", json!(2), now + 300, json!({})),
        with("1", Value::Null), with("2", json!({"actor": "acct-1", "workspace": "ws-1", "profiles": []})),
        with("3", json!({"workspace": "ws-1", "profiles": ["p1"]})), with("4", json!({"actor": "a/b", "workspace": "ws-1", "profiles": ["p1"]})),
        with("5", json!({"actor": "acct-1", "workspace": "ws-1", "profiles": ["p1", "p1"]})),
        with("6", json!({"actor": "acct-1", "workspace": "ws-2", "profiles": ["p1"]}))];
    w.inject = Some((200, json!({"protocol": 1, "operations": ops})));
    let got = enroll::pull(&d, "agentq.configure", &mut b, &mut w).unwrap();
    assert_eq!(got.operations, vec![good.clone()]);
    let reasons: Vec<&str> = got.rejected.iter().map(|r| r["reason"].as_str().unwrap()).collect();
    assert_eq!(reasons, ["digest_mismatch", "scope", "stale_generation", "expired", "payload_too_large", "invalid",
        "authority", "authority", "authority", "authority", "authority", "workspace"]);

    // payload claims never add authority: matching or absent claims pass, any other actor, workspace or profile fails
    assert_eq!(enroll::admit(&good, Some("acct-1"), Some("ws-1"), &["p1"]), Ok(()));
    assert_eq!(enroll::admit(&good, None, None, &["p1"]), Ok(()), "absent claims take the authority's");
    assert_eq!(enroll::admit(&good, Some("acct-2"), None, &["p1"]), Err("wrong_actor"));
    assert_eq!(enroll::admit(&good, None, Some("ws-2"), &["p1"]), Err("wrong_workspace"));
    assert_eq!(enroll::admit(&good, None, None, &["p1", "p2"]), Err("profile_out_of_scope"));
    assert_eq!(enroll::admit(&good, None, None, &[]), Err("profile_out_of_scope"));
    assert_eq!(enroll::admit(&with("7", Value::Null), Some("acct-1"), Some("ws-1"), &["p1"]), Err("authority"), "missing authority");

    // the reference server: pending of this grant only, until a result is stored
    let server = w.servers.get_mut(A).unwrap();
    assert_eq!(server.queue(good.clone()), "queued");
    assert_eq!(server.queue(op(&observe, "d", json!(1), now + 300, json!({}))), "queued");
    let mut stranger = op(&a, "8", json!(2), now + 300, json!({}));
    stranger["authority"]["actor"] = json!("acct-9");
    assert_eq!(server.queue(stranger), "refused", "an actor without authority in the workspace");
    assert_eq!(server.queue(with("9", json!({"actor": "acct-1", "workspace": "ws-2", "profiles": ["p1"]}))), "refused");
    assert_eq!(enroll::pull(&d, "agentq.configure", &mut b, &mut w).unwrap(), enroll::Pulled { operations: vec![good.clone()], rejected: vec![] });
    let result = json!({"schema": 1, "kind": "result", "outcome": "applied"});
    w.crash = Some(w.calls + 1);
    assert_eq!(enroll::post_result(&d, &good, result.clone(), &mut b, &mut w), Err(enroll::Delivery::Retry(1)), "lost reply");
    w.crash = None;
    let receipt = json!({"protocol": 1, "operation": good["operation"], "state": "accepted"});
    assert_eq!(enroll::post_result(&d, &good, result.clone(), &mut b, &mut w), Ok(receipt.clone()), "retry unchanged");
    assert_eq!(enroll::post_result(&d, &good, result.clone(), &mut b, &mut w), Ok(receipt), "duplicate: same receipt");
    assert_eq!(enroll::post_result(&d, &good, json!({"outcome": "rejected"}), &mut b, &mut w).unwrap()["reason"], "replay_mismatch");
    assert_eq!(enroll::pull(&d, "agentq.configure", &mut b, &mut w).unwrap(), enroll::Pulled::default(), "resulted: no longer pending");
    let old = op(&observe, "d", json!(1), now + 300, json!({}));
    assert_eq!(enroll::post_result(&d, &old, result, &mut b, &mut w), Err(enroll::Delivery::Stopped("grant".into())));

    // a revoked grant stops pulling before anything runs
    w.servers.get_mut(A).unwrap().connections.get_mut(a["credential"].as_str().unwrap()).unwrap().revoked = true;
    assert_eq!(enroll::pull(&d, "agentq.configure", &mut b, &mut w), Err(enroll::Delivery::Stopped("revoked".into())));
    assert_eq!(enroll::active(&d), Ok(None));
}
