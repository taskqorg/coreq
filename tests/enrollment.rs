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
                    device_code: code.clone(),
                    active: false,
                    revoked: false,
                });
                self.pairings.get_mut(&code).unwrap().credential = Some(c.clone());
                c
            }
        };
        let c = &self.connections[&cred];
        (200, json!({"protocol": 1, "credential": cred, "installation": c.installation, "generation": c.generation,
            "product": c.product, "workspace": c.workspace, "schemas": c.schemas}))
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
}

const A: &str = "https://a.example";
const B: &str = "https://b.example";

impl TestWire {
    fn new(user: &'static str) -> TestWire {
        let servers = [A, B].map(|d| (d.to_string(), Server::default())).into();
        TestWire { servers, at: 1000, calls: 0, crash: None, skew: 0, user, waits: 1, last: String::new(), shown: None, errors: vec![] }
    }
}

impl enroll::Wire for TestWire {
    fn post(&mut self, destination: &str, route: &str, auth: Option<&str>, body: &Value) -> Result<(u16, Value), String> {
        self.last = destination.into();
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

const REQ: enroll::Request = enroll::Request { product: "agentq", host_label: "mac-1", schemas: &["agentq-snapshot/1"] };

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
