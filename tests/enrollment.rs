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
