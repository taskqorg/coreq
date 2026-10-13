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

/// What a consumer connects: its product, host label and schemas.
pub struct Request<'a> {
    pub product: &'a str,
    pub host_label: &'a str,
    pub schemas: &'a [&'a str],
}

const FILE: &str = "connection.json";

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
    let same = |v: &Value| v["destination"] == destination && v["product"] == req.product && v["schemas"] == json!(req.schemas);
    if s["active"].is_object() && same(&s["active"]) {
        return Ok(s["active"].clone());
    }
    let p = &s["pending"];
    if p.is_object() && (!same(p) || (p["staged"].is_null() && wire.now() >= p["expires_at"].as_u64().unwrap_or(0))) {
        drop_pending(&mut s);
        save(dir, &s)?;
    }
    if s["pending"].is_null() {
        let body = json!({"product": req.product, "installation": s["installation"], "runtime": runtime(), "host_label": req.host_label, "schemas": req.schemas});
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
    s["revoke"].as_array_mut().unwrap().push(r);
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
fn protect(dir: &Path) -> Result<(), String> {
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
        let mut c = crate::quiet(Command::new("curl").args(["-sS", "--proto", "=https", "--max-time", "30", "-w", "\n%{http_code}", "--config", "-"]))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| format!("curl: {e}"))?;
        c.stdin.take().unwrap().write_all(cfg.as_bytes()).map_err(|e| e.to_string())?;
        let o = c.wait_with_output().map_err(|e| e.to_string())?;
        let text = String::from_utf8_lossy(&o.stdout);
        let (body, code) = text.rsplit_once('\n').unwrap_or(("", &text));
        let status = code.parse().ok().filter(|&c| c != 0).ok_or("no reply")?;
        Ok((status, serde_json::from_str(body).map_err(|_| format!("http:{status} bad json"))?))
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
