//! Product connection wire v1: client checks shared by every consumer (docs/design/enrollment.md § Wire v1).

use serde_json::{Value, json};
use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

pub const CONTRACT: &str = "coreq-enrollment/1";
pub const PROTOCOL: u64 = 1;

/// A response of this major; any other or missing major is refused, unknown fields are ignored.
pub fn check(resp: &Value) -> Result<(), String> {
    match resp["protocol"].as_u64() {
        Some(PROTOCOL) => Ok(()),
        p => Err(format!("unsupported protocol {}", p.map_or("none".into(), |p| p.to_string()))),
    }
}

/// `Connected` only when the receipt is projected and names this installation, generation, schema and event.
pub fn verify(receipt: &Value, installation: &str, generation: u64, schema: &str, event_id: &str) -> Result<(), String> {
    check(receipt)?;
    for (k, want) in [("installation", installation), ("schema", schema), ("event_id", event_id)] {
        if receipt[k].as_str() != Some(want) {
            return Err(format!("{k} mismatch"));
        }
    }
    if receipt["generation"].as_u64() != Some(generation) {
        return Err("generation mismatch".into());
    }
    match receipt["state"].as_str() {
        Some("projected") => Ok(()),
        s => Err(format!("not projected: {}", s.unwrap_or("none"))),
    }
}

/// Transport, clock and display of the pairing client (docs/design/enrollment.md § Client).
pub trait Wire {
    /// `POST` of `route` on `destination`; `Err` when no reply arrived (the server may have processed the request).
    fn post(&mut self, destination: &str, route: &str, auth: Option<&str>, body: &Value) -> Result<(u16, Value), String>;
    /// Unix seconds.
    fn now(&self) -> u64;
    fn sleep(&mut self, secs: u64);
    /// Shows the code; opens the URI in browser mode.
    fn show(&mut self, verification_uri: &str, user_code: &str);
}

/// What a consumer connects: its product, host label, schemas and optional scope `(name, value)`,
/// e.g. `("taskq-board/1", "owner/repo")`, shown on the approval page. `installation`: the consumer's own id
/// (1-64 of `[A-Za-z0-9-]`, e.g. TaskQ's 64-hex id); `None` keeps the stored one, a random UUID at first use.
/// `capabilities`: operation grants to ask for, e.g. `agentq.configure`; empty asks for none.
pub struct Request<'a> {
    pub installation: Option<&'a str>,
    pub product: &'a str,
    pub host_label: &'a str,
    pub schemas: &'a [&'a str],
    pub scope: Option<(&'a str, &'a str)>,
    pub capabilities: &'a [&'a str],
}

const FILE: &str = "connection.json";
/// DashQ C5 server input: at most 100 records and 1 MiB per request.
pub const MAX_RECORDS: usize = 100;
pub const MAX_BODY: usize = 1 << 20;
/// Operations per `/operations` reply and bytes per operation payload or result.
pub const MAX_OPERATIONS: usize = 16;
pub const MAX_PAYLOAD: usize = 16 << 10;
/// Revoke-only records kept for offline recovery.
const MAX_REVOKES: usize = 16;

/// The active connection: token response plus `destination`; `None` before the first activation or after `disconnect`.
pub fn active(dir: &Path) -> Result<Option<Value>, String> {
    Ok(Some(load(dir)?["active"].clone()).filter(Value::is_object))
}

/// Pairs and activates one connection; idempotent for the active destination, product and schemas.
pub fn pair(dir: &Path, destination: &str, req: &Request, wire: &mut dyn Wire) -> Result<Value, String> {
    if !destination.starts_with("https://") {
        return Err("destination must be https".into());
    }
    let mut s = load(dir)?;
    if let Some(id) = req.installation {
        if id.is_empty() || id.len() > 64 || !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
            return Err("bad installation".into());
        }
        if s["installation"] != id {
            // a new identity: pending and active belong to the old one; ack queues the old active for revoke
            s["installation"] = json!(id);
            drop_pending(&mut s);
            save(dir, &s)?;
        }
    }
    let scope = req.scope.map_or(Value::Null, |(name, value)| json!({"name": name, "value": value}));
    let caps = if req.capabilities.is_empty() { Value::Null } else { json!(req.capabilities) };
    let same = |v: &Value| v["destination"] == destination && v["product"] == req.product && v["schemas"] == json!(req.schemas) && v["scope"] == scope && v["requested"] == caps;
    if s["active"].is_object() && same(&s["active"]) && s["active"]["installation"] == s["installation"] {
        return Ok(s["active"].clone());
    }
    let p = &s["pending"];
    if p.is_object() && (!same(p) || (p["staged"].is_null() && wire.now() >= p["expires_at"].as_u64().unwrap_or(0))) {
        drop_pending(&mut s);
        save(dir, &s)?;
    }
    if s["pending"].is_null() {
        let mut body = json!({"product": req.product, "installation": s["installation"], "runtime": runtime(), "host_label": req.host_label, "schemas": req.schemas});
        if !scope.is_null() {
            body["scope"] = scope.clone();
        }
        if !caps.is_null() {
            body["capabilities"] = caps.clone();
        }
        let r = ok(call(wire, destination, "/connect/device", None, body)?)?;
        if !r["verification_uri"].as_str().is_some_and(|u| u.starts_with("https://") && !u.chars().any(|c| c.is_whitespace() || c.is_control())) {
            return Err("verification_uri must be https".into());
        }
        if !r["user_code"].as_str().is_some_and(|c| !c.is_empty() && c.len() <= 32 && c.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')) {
            return Err("bad user_code".into());
        }
        s["pending"] = json!({"destination": destination, "product": req.product, "schemas": req.schemas,
            "device_code": r["device_code"], "user_code": r["user_code"], "verification_uri": r["verification_uri"],
            "expires_at": wire.now() + r["expires_in"].as_u64().unwrap_or(0), "interval": r["interval"].as_u64().unwrap_or(5), "staged": null});
        if !scope.is_null() {
            s["pending"]["scope"] = scope.clone();
        }
        if !caps.is_null() {
            s["pending"]["requested"] = caps.clone();
        }
        save(dir, &s)?;
    }
    if s["pending"]["staged"].is_null() {
        wire.show(s["pending"]["verification_uri"].as_str().unwrap_or_default(), s["pending"]["user_code"].as_str().unwrap_or_default());
    }
    while s["pending"]["staged"].is_null() {
        let p = &s["pending"];
        if wire.now() >= p["expires_at"].as_u64().unwrap_or(0) {
            drop_pending(&mut s);
            save(dir, &s)?;
            return Err("expired_token".into());
        }
        let interval = p["interval"].as_u64().unwrap_or(5);
        let (status, r) = call(wire, destination, "/connect/token", None, json!({"device_code": p["device_code"]}))?;
        match (status, r["error"].as_str()) {
            (200, _) if r["installation"] == s["installation"] && r["product"] == req.product && r["credential"].is_string() => {
                let mut staged = r;
                staged["destination"] = json!(destination);
                staged.as_object_mut().unwrap().remove("protocol");
                // the client's own scope, never one the server echoes
                staged.as_object_mut().unwrap().remove("scope");
                if !scope.is_null() {
                    staged["scope"] = scope.clone();
                }
                // only capabilities both asked for and granted: no reply broadens a grant
                let granted = staged.as_object_mut().unwrap().remove("capabilities").unwrap_or_default();
                if !caps.is_null() {
                    staged["capabilities"] = json!(req.capabilities.iter().filter(|c| granted.as_array().is_some_and(|g| g.contains(&json!(c)))).collect::<Vec<_>>());
                    staged["requested"] = caps.clone();
                }
                s["pending"]["staged"] = staged;
                save(dir, &s)?;
            }
            (_, Some("authorization_pending")) => wire.sleep(interval),
            (_, Some("slow_down")) => {
                s["pending"]["interval"] = json!(interval + 5);
                save(dir, &s)?;
                wire.sleep(interval + 5);
            }
            (_, e) => {
                drop_pending(&mut s);
                save(dir, &s)?;
                return Err(e.unwrap_or("bad token response").into());
            }
        }
    }
    let staged = s["pending"]["staged"].clone();
    let (status, r) = call(wire, destination, "/connect/ack", staged["credential"].as_str(), json!({}))?;
    if status != 200 || r["state"] != "active" || r["generation"] != staged["generation"] {
        drop_pending(&mut s);
        save(dir, &s)?;
        return Err(r["error"].as_str().unwrap_or("ack refused").into());
    }
    // one write: the new connection is active, the old one queued for revoke
    let old = std::mem::replace(&mut s["active"], staged.clone());
    if old.is_object() && (old["destination"] != staged["destination"] || old["credential"] != staged["credential"]) {
        queue_revoke(&mut s, &old);
    }
    s["pending"] = Value::Null;
    save(dir, &s)?;
    let _ = flush(dir, &mut s, wire);
    Ok(staged)
}

/// A record bound to the active grant: `schema`, `event_id`, `observed_at`, `payload` and `grant` (destination,
/// generation). The consumer keeps it unchanged until a receipt ends it, so retries repeat its event ID to its grant.
pub fn stamp(dir: &Path, schema: &str, event_id: &str, observed_at: u64, payload: Value) -> Result<Value, String> {
    let a = active(dir)?.ok_or("disconnected")?;
    Ok(json!({"schema": schema, "event_id": event_id, "observed_at": observed_at, "payload": payload, "grant": grant(&a)}))
}

fn grant(a: &Value) -> Value {
    json!({"destination": a["destination"], "generation": a["generation"]})
}

/// Why a delivery try sent nothing durable.
#[derive(Debug, PartialEq)]
pub enum Delivery {
    /// Try the same records again after these seconds.
    Retry(u64),
    /// No retry helps: disconnected, authorization ended (needs a new `pair`), a refused batch or protocol.
    Stopped(String),
}

/// Retry timing of one consumer's sender (DashQ C5 Delivery): jittered 1 s to 60 s, doubling per failure; a 429
/// `Retry-After` wins, at most an hour. A success resets it.
#[derive(Default)]
pub struct Backoff {
    fails: u32,
}

impl Backoff {
    pub fn next(&mut self, retry_after: Option<u64>) -> u64 {
        use ring::rand::SecureRandom;
        self.fails += 1;
        let cap = (1u64 << (self.fails - 1).min(6)).min(60);
        let mut b = [0u8; 8];
        let _ = ring::rand::SystemRandom::new().fill(&mut b);
        let jittered = cap / 2 + u64::from_le_bytes(b) % (cap - cap / 2 + 1);
        retry_after.map_or(jittered, |r| r.min(3600)).max(1)
    }
}

/// One delivery try of the consumer's pass. Ok: one receipt per record, in order. `accepted` is durable (the consumer
/// may drop the record), `rejected` ends it; a record of another grant is `rejected` with reason `grant`, never sent.
/// `401` ends the active connection: sending stops until a new `pair`.
pub fn deliver(dir: &Path, records: &[Value], backoff: &mut Backoff, wire: &mut dyn Wire) -> Result<Vec<Value>, Delivery> {
    let a = active(dir).map_err(Delivery::Stopped)?.ok_or(Delivery::Stopped("disconnected".into()))?;
    let g = grant(&a);
    let send: Vec<Value> = records.iter().filter(|r| r["grant"] == g).map(|r| {
        let mut r = r.clone();
        r.as_object_mut().map(|o| o.remove("grant"));
        r
    }).collect();
    let body = json!({"records": send});
    if send.len() > MAX_RECORDS || body.to_string().len() > MAX_BODY {
        return Err(Delivery::Stopped("batch over limit".into()));
    }
    let mut got = vec![];
    if !send.is_empty() {
        got = send_active(dir, &a, "/ingest", body, backoff, wire)?["receipts"].as_array().cloned().unwrap_or_default();
        if got.len() != send.len() || got.iter().zip(&send).any(|(r, s)| r["event_id"] != s["event_id"]) {
            return Err(Delivery::Stopped("receipts do not match records".into()));
        }
    }
    *backoff = Backoff::default();
    let mut got = got.into_iter();
    Ok(records.iter().map(|r| match r["grant"] == g {
        true => got.next().unwrap_or_default(),
        false => json!({"event_id": r["event_id"], "state": "rejected", "reason": "grant"}),
    }).collect())
}

/// One authorized request of the active connection with `deliver`'s status handling; a success resets `backoff`.
fn send_active(dir: &Path, a: &Value, route: &str, body: Value, backoff: &mut Backoff, wire: &mut dyn Wire) -> Result<Value, Delivery> {
    match call(wire, a["destination"].as_str().unwrap_or_default(), route, a["credential"].as_str(), body) {
        Ok((200, r)) => {
            *backoff = Backoff::default();
            Ok(r)
        }
        Ok((401, r)) => {
            let mut s = load(dir).map_err(Delivery::Stopped)?;
            s["active"] = Value::Null;
            save(dir, &s).map_err(Delivery::Stopped)?;
            Err(Delivery::Stopped(r["error"].as_str().unwrap_or("revoked").into()))
        }
        Ok((429, r)) => Err(Delivery::Retry(backoff.next(r["retry_after"].as_u64()))),
        Ok((400..=499, r)) => Err(Delivery::Stopped(r["error"].as_str().unwrap_or("http error").into())),
        Err(e) if e.starts_with("unsupported protocol") => Err(Delivery::Stopped(e)),
        _ => Err(Delivery::Retry(backoff.next(None))),
    }
}

/// Operations of one `pull`: `operations` may run; `rejected` (`operation`, `digest`, `reason`) never run.
#[derive(Debug, Default, PartialEq)]
pub struct Pulled {
    pub operations: Vec<Value>,
    pub rejected: Vec<Value>,
}

/// SHA-256 hex of `payload` as JSON with sorted keys and no whitespace.
pub fn digest(payload: &Value) -> String {
    fn sorted(v: &Value) -> Value {
        match v {
            Value::Object(m) => {
                let mut keys: Vec<_> = m.keys().collect();
                keys.sort();
                Value::Object(keys.into_iter().map(|k| (k.clone(), sorted(&m[k]))).collect())
            }
            Value::Array(a) => Value::Array(a.iter().map(sorted).collect()),
            _ => v.clone(),
        }
    }
    crate::sha256_hex(sorted(payload).to_string().as_bytes())
}

/// One pull of the consumer's service pass: pending operations of `capability` for the active grant. Each one is
/// checked locally; one of another capability, installation, generation or workspace, without valid `authority`,
/// expired, oversized or altered is rejected.
pub fn pull(dir: &Path, capability: &str, backoff: &mut Backoff, wire: &mut dyn Wire) -> Result<Pulled, Delivery> {
    let a = active(dir).map_err(Delivery::Stopped)?.ok_or(Delivery::Stopped("disconnected".into()))?;
    if !a["capabilities"].as_array().is_some_and(|c| c.contains(&json!(capability))) {
        return Err(Delivery::Stopped("scope".into()));
    }
    let r = send_active(dir, &a, "/operations", json!({"capability": capability}), backoff, wire)?;
    let ops = r["operations"].as_array().cloned().unwrap_or_default();
    if ops.len() > MAX_OPERATIONS {
        return Err(Delivery::Stopped("operations over limit".into()));
    }
    let now = wire.now();
    let mut got = Pulled::default();
    for op in ops {
        let id = op["operation"].as_str().unwrap_or_default();
        let reason = if id.len() != 32 || !id.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f')) || !op["digest"].is_string() || !op["expires_at"].is_u64() {
            Some("invalid")
        } else if op["payload"].to_string().len() > MAX_PAYLOAD {
            Some("payload_too_large")
        } else if op["digest"] != digest(&op["payload"]) {
            Some("digest_mismatch")
        } else if op["capability"] != capability {
            Some("scope")
        } else if op["installation"] != a["installation"] {
            Some("installation")
        } else if op["generation"] != a["generation"] {
            Some("stale_generation")
        } else if !authority_ok(&op["authority"]) {
            Some("authority")
        } else if op["authority"]["workspace"] != a["workspace"] {
            Some("workspace")
        } else if now >= op["expires_at"].as_u64().unwrap_or(0) {
            Some("expired")
        } else {
            None
        };
        match reason {
            None => got.operations.push(op),
            Some(reason) => got.rejected.push(json!({"operation": op["operation"], "digest": op["digest"], "reason": reason})),
        }
    }
    Ok(got)
}

/// Server-set authority: opaque `actor` and `workspace`, 1-64 distinct `profiles`; anything else fails closed.
fn authority_ok(a: &Value) -> bool {
    let id = |v: &Value| v.as_str().is_some_and(|s| (1..=64).contains(&s.len()) && s.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | ':' | '-')));
    let profiles = a["profiles"].as_array().map(Vec::as_slice).unwrap_or_default();
    id(&a["actor"]) && id(&a["workspace"]) && (1..=64).contains(&profiles.len()) && profiles.iter().all(id)
        && profiles.iter().enumerate().all(|(i, p)| !profiles[..i].contains(p))
}

/// Checks the consumer's payload claims against the pulled operation's `authority`: a claim never adds authority.
/// An absent actor or workspace claim takes the authority's; every claimed profile must be in its scope.
pub fn admit(op: &Value, actor: Option<&str>, workspace: Option<&str>, profiles: &[&str]) -> Result<(), &'static str> {
    let a = &op["authority"];
    if !authority_ok(a) {
        return Err("authority");
    }
    if actor.is_some_and(|x| a["actor"] != x) {
        return Err("wrong_actor");
    }
    if workspace.is_some_and(|x| a["workspace"] != x) {
        return Err("wrong_workspace");
    }
    if profiles.is_empty() || !profiles.iter().all(|p| a["profiles"].as_array().unwrap().contains(&json!(p))) {
        return Err("profile_out_of_scope");
    }
    Ok(())
}

/// Posts the consumer's `result` of a pulled operation and returns the receipt (`accepted`, or `rejected` with a
/// reason); a repeat returns the same receipt, so a lost reply retries unchanged. An operation of another grant is
/// `Stopped("grant")` and never sent.
pub fn post_result(dir: &Path, op: &Value, result: Value, backoff: &mut Backoff, wire: &mut dyn Wire) -> Result<Value, Delivery> {
    let a = active(dir).map_err(Delivery::Stopped)?.ok_or(Delivery::Stopped("disconnected".into()))?;
    if op["installation"] != a["installation"] || op["generation"] != a["generation"] {
        return Err(Delivery::Stopped("grant".into()));
    }
    if result.to_string().len() > MAX_PAYLOAD {
        return Err(Delivery::Stopped("result over limit".into()));
    }
    let r = send_active(dir, &a, "/operations/result", json!({"operation": op["operation"], "digest": op["digest"], "result": result}), backoff, wire)?;
    if r["operation"] != op["operation"] {
        return Err(Delivery::Stopped("receipt does not match operation".into()));
    }
    Ok(r)
}

/// Receipt of a delivered record: `accepted` (durable) or `projected`, which only `verify` grants (`Connected`).
pub fn receipt(dir: &Path, record: &Value, wire: &mut dyn Wire) -> Result<&'static str, String> {
    let a = active(dir)?.ok_or("disconnected")?;
    if record["grant"] != grant(&a) {
        return Err("grant mismatch".into());
    }
    let r = ok(call(wire, a["destination"].as_str().unwrap_or_default(), "/connection/receipt", a["credential"].as_str(),
        json!({"event_id": record["event_id"]}))?)?;
    let field = |v: &Value| v.as_str().unwrap_or_default().to_string();
    match verify(&r, &field(&a["installation"]), a["generation"].as_u64().unwrap_or(0), &field(&record["schema"]), &field(&record["event_id"])) {
        Ok(()) => Ok("projected"),
        Err(e) if e == "not projected: accepted" => Ok("accepted"),
        Err(e) => Err(e),
    }
}

/// Drops a pairing in progress; never an active connection.
pub fn cancel(dir: &Path, wire: &mut dyn Wire) -> Result<(), String> {
    let mut s = load(dir)?;
    drop_pending(&mut s);
    save(dir, &s)?;
    flush(dir, &mut s, wire)
}

/// Stops sending at once; the revoke retries until the server confirms it.
pub fn disconnect(dir: &Path, wire: &mut dyn Wire) -> Result<(), String> {
    let mut s = load(dir)?;
    let old = std::mem::take(&mut s["active"]);
    if old.is_object() {
        queue_revoke(&mut s, &old);
    }
    save(dir, &s)?;
    flush(dir, &mut s, wire)
}

/// Posts every queued revoke; `Err` when one stays queued.
pub fn retry_revokes(dir: &Path, wire: &mut dyn Wire) -> Result<(), String> {
    let mut s = load(dir)?;
    flush(dir, &mut s, wire)
}

fn flush(dir: &Path, s: &mut Value, wire: &mut dyn Wire) -> Result<(), String> {
    let all = std::mem::take(&mut s["revoke"]);
    let keep: Vec<Value> = all.as_array().into_iter().flatten().filter(|r| {
        let d = r["destination"].as_str().unwrap_or_default();
        !matches!(call(wire, d, "/connection/revoke", r["credential"].as_str(), json!({})), Ok((200 | 401, _)))
    }).cloned().collect();
    let left = keep.len();
    s["revoke"] = json!(keep);
    if s["revoke"] != all {
        save(dir, s)?;
    }
    if left == 0 { Ok(()) } else { Err(format!("{left} revoke pending")) }
}

fn queue_revoke(s: &mut Value, c: &Value) {
    let r = json!({"destination": c["destination"], "credential": c["credential"]});
    let q = s["revoke"].as_array_mut().unwrap();
    q.push(r);
    // ponytail: the oldest revoke drops past the bound; that server grant stays until DashQ's Revoke action
    if q.len() > MAX_REVOKES {
        q.remove(0);
    }
}

fn drop_pending(s: &mut Value) {
    let staged = std::mem::take(&mut s["pending"])["staged"].clone();
    if staged.is_object() {
        queue_revoke(s, &staged);
    }
}

fn call(wire: &mut dyn Wire, destination: &str, route: &str, auth: Option<&str>, mut body: Value) -> Result<(u16, Value), String> {
    body["protocol"] = json!(PROTOCOL);
    let (status, r) = wire.post(destination, route, auth, &body)?;
    check(&r)?;
    Ok((status, r))
}

fn ok((status, r): (u16, Value)) -> Result<Value, String> {
    if status == 200 { Ok(r) } else { Err(r["error"].as_str().unwrap_or("http error").into()) }
}

/// `wsl` inside WSL, else `native`.
pub fn runtime() -> &'static str {
    let wsl = cfg!(target_os = "linux") && (std::env::var_os("WSL_DISTRO_NAME").is_some() || Path::new("/proc/sys/fs/binfmt_misc/WSLInterop").exists());
    if wsl { "wsl" } else { "native" }
}

fn load(dir: &Path) -> Result<Value, String> {
    protect(dir)?;
    match fs::read_to_string(dir.join(FILE)) {
        Ok(t) => serde_json::from_str(&t).map_err(|_| format!("{}: unreadable connection state", dir.join(FILE).display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let s = json!({"installation": crate::incident::uuid(), "pending": null, "active": null, "revoke": []});
            save(dir, &s)?;
            Ok(s)
        }
        Err(e) => Err(format!("{}: {e}", dir.join(FILE).display())),
    }
}

/// Whole-file atomic write of protected state: temp file 0600, fsync, rename.
// ponytail: no directory fsync; a power loss right after rename may keep the old state, which every step resumes from.
fn save(dir: &Path, s: &Value) -> Result<(), String> {
    let (tmp, file) = (dir.join(format!("{FILE}.tmp")), dir.join(FILE));
    let err = |e: std::io::Error| format!("{}: {e}", tmp.display());
    let mut o = fs::OpenOptions::new();
    o.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut o, 0o600);
    let mut f = o.open(&tmp).map_err(err)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        f.set_permissions(fs::Permissions::from_mode(0o600)).map_err(err)?;
        if f.metadata().map_err(err)?.permissions().mode() & 0o077 != 0 {
            return Err(format!("{}: storage not protected", tmp.display()));
        }
    }
    f.write_all(s.to_string().as_bytes()).and_then(|_| f.sync_all()).map_err(err)?;
    drop(f);
    fs::rename(&tmp, &file).map_err(|e| format!("{}: {e}", file.display()))
}

/// Folder only this user can open; refuses when the OS does not keep the protection.
pub(crate) fn protect(dir: &Path) -> Result<(), String> {
    fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let refused = || format!("{}: storage not protected", dir.display());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).map_err(|_| refused())?;
        if fs::metadata(dir).map_err(|_| refused())?.permissions().mode() & 0o077 != 0 {
            return Err(refused());
        }
    }
    #[cfg(windows)]
    {
        let o = crate::quiet(&mut Command::new("whoami")).output().map_err(|_| refused())?;
        let user = String::from_utf8_lossy(&o.stdout).trim().to_string();
        let ok = !user.is_empty()
            && crate::quiet(Command::new("icacls").arg(dir).args(["/inheritance:r", "/grant:r", format!("{user}:(OI)(CI)F").as_str(), "/q"]))
                .output()
                .is_ok_and(|o| o.status.success());
        if !ok {
            return Err(refused());
        }
    }
    Ok(())
}

/// The OS `curl` over HTTPS; URL, credential and body go on stdin, never in argv.
pub struct Https {
    /// Open the verification URI in the default browser; headless only prints it.
    pub browser: bool,
}

impl Wire for Https {
    fn post(&mut self, destination: &str, route: &str, auth: Option<&str>, body: &Value) -> Result<(u16, Value), String> {
        // a server-sent credential with a line break would add curl options
        if [destination, route, auth.unwrap_or_default()].iter().any(|v| v.chars().any(|c| c.is_control())) {
            return Err("control character in request".into());
        }
        let esc = |s: &str| s.replace('\\', "\\\\").replace('"', "\\\"");
        let mut cfg = format!("url = \"{}{route}\"\nrequest = \"POST\"\nheader = \"Content-Type: application/json\"\ndata-binary = \"{}\"\n",
            esc(destination.trim_end_matches('/')), esc(&body.to_string()));
        if let Some(a) = auth {
            cfg += &format!("header = \"Authorization: Bearer {}\"\n", esc(a));
        }
        let mut c = crate::quiet(Command::new("curl").args(["-sS", "--proto", "=https", "--max-time", "10", "-D", "-", "-w", "\n%{http_code}", "--config", "-"]))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| format!("curl: {e}"))?;
        c.stdin.take().unwrap().write_all(cfg.as_bytes()).map_err(|e| e.to_string())?;
        let o = c.wait_with_output().map_err(|e| e.to_string())?;
        reply(&String::from_utf8_lossy(&o.stdout))
    }

    fn now(&self) -> u64 {
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_secs())
    }

    fn sleep(&mut self, secs: u64) {
        std::thread::sleep(std::time::Duration::from_secs(secs));
    }

    fn show(&mut self, verification_uri: &str, user_code: &str) {
        eprintln!("open {verification_uri} and enter code {user_code}");
        if self.browser {
            let (bin, args): (&str, &[&str]) = if cfg!(windows) { ("rundll32", &["url.dll,FileProtocolHandler"]) } else if cfg!(target_os = "macos") { ("open", &[]) } else { ("xdg-open", &[]) };
            let _ = crate::quiet(Command::new(bin).args(args).arg(verification_uri)).stdout(Stdio::null()).stderr(Stdio::null()).spawn();
        }
    }
}

/// curl's stdout under `-D - -w "\n%{http_code}"`: header blocks, body, status.
fn reply(text: &str) -> Result<(u16, Value), String> {
    let (mut body, code) = text.rsplit_once('\n').unwrap_or(("", text));
    let status = code.parse().ok().filter(|&c| c != 0).ok_or("no reply")?;
    // header blocks (interim replies included) come first; Retry-After in seconds joins the reply as `retry_after`
    let mut retry_after = None;
    while body.starts_with("HTTP/") {
        let (head, rest) = body.split_once("\r\n\r\n").unwrap_or((body, ""));
        retry_after = head.lines().find_map(|l| l.split_once(':').filter(|(k, _)| k.eq_ignore_ascii_case("retry-after"))?.1.trim().parse::<u64>().ok());
        body = rest;
    }
    let mut r: Value = serde_json::from_str(body).map_err(|_| format!("http:{status} bad json"))?;
    if let (Some(n), Some(o)) = (retry_after, r.as_object_mut()) {
        o.insert("retry_after".into(), json!(n));
    }
    Ok((status, r))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_refuses_other_major() {
        assert!(check(&json!({"protocol": 1, "future": true})).is_ok());
        assert_eq!(check(&json!({"protocol": 2})), Err("unsupported protocol 2".into()));
        assert_eq!(check(&json!({})), Err("unsupported protocol none".into()));
    }

    #[test]
    fn reply_keeps_retry_after() {
        let t = "HTTP/1.1 100 Continue\r\n\r\nHTTP/2 429\r\nretry-after: 7\r\n\r\n{\"protocol\":1,\"error\":\"rate_limited\"}\n429";
        assert_eq!(reply(t), Ok((429, json!({"protocol": 1, "error": "rate_limited", "retry_after": 7}))));
        assert_eq!(reply("HTTP/2 200\r\n\r\n{}\n200"), Ok((200, json!({}))));
        assert_eq!(reply("\n000"), Err("no reply".into()));
    }

    #[test]
    fn backoff_is_bounded() {
        let mut b = Backoff::default();
        let waits: Vec<u64> = (0..10).map(|_| b.next(None)).collect();
        assert!(waits[0] == 1 && waits.iter().all(|&w| (1..=60).contains(&w)), "{waits:?}");
        assert!(waits[9] >= 30, "{waits:?}");
        assert_eq!(b.next(Some(90)), 90);
        assert_eq!(b.next(Some(0)), 1);
    }

    #[test]
    fn state_is_protected_and_refuses_unreadable() {
        let d = crate::scratch("enroll-protect").join("conn");
        assert_eq!(active(&d), Ok(None));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(fs::metadata(&d).unwrap().permissions().mode() & 0o777, 0o700);
            assert_eq!(fs::metadata(d.join(FILE)).unwrap().permissions().mode() & 0o777, 0o600);
        }
        #[cfg(windows)]
        {
            let o = Command::new("icacls").arg(&d).output().unwrap();
            assert!(!String::from_utf8_lossy(&o.stdout).contains("(I)"), "inherited access stays");
        }
        let id = load(&d).unwrap()["installation"].clone();
        assert_eq!(load(&d).unwrap()["installation"], id, "installation is stable");
        fs::write(d.join(FILE), "{broken").unwrap();
        assert!(active(&d).is_err());
        assert_eq!(fs::read_to_string(d.join(FILE)).unwrap(), "{broken", "never overwritten");
    }
}
