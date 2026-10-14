//! Incident reports: a consumer's fault becomes a sanitized report upstream or a local file (coreq.md § Incident reports).

use crate::{Tool, quiet, sha256_hex, update::target, write_atomic};
use serde_json::{Value, json};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// The only keys a report may hold.
pub const KEYS: [&str; 15] = ["schema", "fault", "fingerprint", "report_id", "release", "revision", "contract", "board", "runtime", "os", "site", "evidence", "observed", "inferred", "profile"];
const MAX_BYTES: usize = 4096;
/// A claim without a link older than this is reclaimed.
const STALE: Duration = Duration::from_secs(600);

/// One detected fault of the consumer's catalog; every value is an enumerated code.
pub struct Fault<'a> {
    pub code: &'a str,
    /// The fault's fingerprint fields beyond its code; hashed, never sent.
    pub fields: &'a [&'a str],
    pub board: &'a str,
    pub runtime: &'a str,
    pub site: &'a str,
    pub revision: &'a str,
    pub contract: &'a str,
    pub evidence: &'a [&'a str],
    pub observed: &'a [&'a str],
    pub inferred: &'a [&'a str],
}

#[derive(Clone, Debug, PartialEq)]
pub struct Outcome {
    /// `sent`, `local`, `unsupported-auth` or `disabled`.
    pub state: &'static str,
    pub fault: String,
    pub at: u64,
    pub path: Option<PathBuf>,
    pub url: Option<String>,
    pub reason: Option<String>,
}

impl Outcome {
    /// The `status.json` `incident` value.
    pub fn json(&self) -> Value {
        json!({"state": self.state, "fault": self.fault, "at": self.at, "path": self.path.as_ref().map(|p| p.display().to_string()), "url": self.url, "reason": self.reason})
    }
}

pub struct Issue {
    pub number: u64,
    pub url: String,
    pub open: bool,
    pub author: String,
    pub body: String,
}

pub struct Comment {
    pub author: String,
    pub body: String,
}

/// Issue API of the release repository; `bool` = more pages follow.
pub trait Transport {
    fn login(&mut self) -> Result<String, String>;
    fn search(&mut self, fingerprint: &str, page: u32) -> Result<(Vec<Issue>, bool), String>;
    fn comments(&mut self, issue: u64, page: u32) -> Result<(Vec<Comment>, bool), String>;
    fn create(&mut self, title: &str, body: &str) -> Result<Issue, String>;
    fn comment(&mut self, issue: u64, body: &str) -> Result<(), String>;
}

pub fn schema(t: &Tool) -> String {
    format!("{}-incident/1", t.name)
}

/// Shown by setup and the first update before any send.
pub fn disclosure(t: &Tool) -> String {
    format!(
        "Incident reports: when {name} detects its own defect, it opens or confirms a public issue in https://github.com/{repo} (label incident).\n\
         Account: your github.com login (the board token on github.com, else gh); without one the report stays local.\n\
         Data: only the keys {keys}, as codes; no project, repository, task, host, user, path or free text.\n\
         Fallback: offline, refused or unsure delivery keeps the report in .{name}/incidents/ for manual submission.\n\
         Off: \"incident_reports\": false in the config, or {env}=off.",
        name = t.name,
        repo = t.repo,
        keys = KEYS.join(", "),
        env = t.env("INCIDENT_REPORTS"),
    )
}

/// Env off or config `false`.
pub fn disabled(t: &Tool, setting: Option<bool>) -> bool {
    setting == Some(false) || std::env::var(t.env("INCIDENT_REPORTS")).is_ok_and(|v| v.eq_ignore_ascii_case("off"))
}

pub fn build(t: &Tool, f: &Fault, report_id: &str) -> Value {
    build_on(t, f, report_id, &target())
}

fn build_on(t: &Tool, f: &Fault, report_id: &str, os: &str) -> Value {
    let mut parts = vec!["1", f.code];
    parts.extend(f.fields);
    parts.extend([f.board, f.runtime, os]);
    let fp = &sha256_hex(parts.join("\n").as_bytes())[..16];
    json!({"schema": schema(t), "fault": f.code, "fingerprint": fp, "report_id": report_id, "release": t.version, "revision": f.revision, "contract": f.contract, "board": f.board, "runtime": f.runtime, "os": os, "site": f.site, "evidence": f.evidence, "observed": f.observed, "inferred": f.inferred})
}

/// Allowlisted keys, code values, at most 4 KiB.
pub fn check(report: &Value) -> Result<(), String> {
    let code = |s: &str| s.len() <= 64 && !s.contains("//") && !s.starts_with('/') && s.bytes().all(|c| c.is_ascii_alphanumeric() || b"._:/-".contains(&c));
    for (k, v) in report.as_object().ok_or("not an object")? {
        let ok = KEYS.contains(&k.as_str())
            && match v {
                Value::String(s) => code(s),
                Value::Array(a) => a.len() <= 8 && a.iter().all(|x| x.as_str().is_some_and(code)),
                _ => false,
            };
        if !ok {
            return Err(format!("key {k:?}"));
        }
    }
    if report.to_string().len() > MAX_BYTES { Err("over 4 KiB".into()) } else { Ok(()) }
}

thread_local!(static ACTIVE: Cell<bool> = const { Cell::new(false) });
static SEEN: Mutex<BTreeMap<String, Outcome>> = Mutex::new(BTreeMap::new());

/// Report one fault through `net` (None: no github.com login). Never prints, panics out or touches consumer state.
pub fn report(t: &Tool, root: &Path, setting: Option<bool>, f: &Fault, net: Option<&mut dyn Transport>) -> Outcome {
    let at = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
    let out = |state, reason: &str| Outcome { state, fault: f.code.into(), at, path: None, url: None, reason: Some(reason.into()) };
    if disabled(t, setting) {
        return Outcome { reason: None, ..out("disabled", "") };
    }
    // a fault inside reporting never reports itself
    if ACTIVE.replace(true) {
        return out("local", "self");
    }
    // a caught panic stays silent: the default hook would print to stderr
    static HOOK: std::sync::Once = std::sync::Once::new();
    HOOK.call_once(|| {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if !ACTIVE.with(Cell::get) {
                prev(info);
            }
        }));
    });
    let o = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| route(t, root, setting, f, net, at))).unwrap_or_else(|_| out("local", "panic"));
    ACTIVE.set(false);
    o
}

fn route(t: &Tool, root: &Path, setting: Option<bool>, f: &Fault, net: Option<&mut dyn Transport>, at: u64) -> Outcome {
    let mut r = build(t, f, &uuid());
    r["profile"] = json!(crate::profile::window(t, root));
    let fp = r["fingerprint"].as_str().unwrap_or_default().to_string();
    let keep = |state, reason: &str| {
        let p = root.join(format!(".{}", t.name)).join("incidents").join(format!("{fp}.json"));
        let saved = fs::create_dir_all(p.parent().unwrap()).is_ok() && write_atomic(&p, &serde_json::to_string_pretty(&r).unwrap_or_default()).is_ok();
        Outcome { state, fault: f.code.into(), at, path: saved.then_some(p), url: None, reason: Some(reason.into()) }
    };
    if check(&r).is_err() {
        return Outcome { state: "local", fault: f.code.into(), at, path: None, url: None, reason: Some("unsafe".into()) };
    }
    if setting.is_none() {
        return keep("local", "not acknowledged");
    }
    let mut seen = SEEN.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(o) = seen.get(&fp) {
        return o.clone();
    }
    let o = match net {
        None => keep("unsupported-auth", "no github.com login"),
        Some(net) => match claimed(t, &r, &fp, net) {
            Ok(url) => Outcome { state: "sent", fault: f.code.into(), at, path: None, url: Some(url), reason: None },
            Err(reason) => keep("local", &reason),
        },
    };
    seen.insert(fp, o.clone());
    o
}

/// One delivery per fingerprint per install: `<install>/incidents/<fingerprint>`, created atomically, holds the link once known.
fn claimed(t: &Tool, r: &Value, fp: &str, net: &mut dyn Transport) -> Result<String, String> {
    let p = t.install.join("incidents").join(fp);
    let _ = fs::create_dir_all(p.parent().unwrap());
    let claim = || fs::OpenOptions::new().write(true).create_new(true).open(&p).map(drop);
    let taken = |r: &std::io::Result<()>| r.as_ref().is_err_and(|e| e.kind() == std::io::ErrorKind::AlreadyExists);
    let mut got = claim();
    if taken(&got) {
        let url = fs::read_to_string(&p).unwrap_or_default().trim().to_string();
        if !url.is_empty() {
            return deliver(t, r, fp, net, Some(&url));
        }
        if !fs::metadata(&p).and_then(|m| m.modified()).is_ok_and(|m| m.elapsed().is_ok_and(|e| e >= STALE)) {
            return Err("claimed".into());
        }
        // ponytail: two reclaimers of one stale claim in the same instant may both win; a lock if that shows up
        let _ = fs::remove_file(&p);
        got = claim();
        if taken(&got) {
            return Err("claimed".into());
        }
    }
    // an unwritable install reports unclaimed rather than not at all
    let res = deliver(t, r, fp, net, None);
    if got.is_ok() {
        let _ = match &res {
            Ok(url) => write_atomic(&p, url),
            Err(_) => fs::remove_file(&p).map_err(|e| e.to_string()),
        };
    }
    res
}

/// At most one write; Ok(issue link). `known`: a sibling's link; never create.
fn deliver(t: &Tool, r: &Value, fp: &str, net: &mut dyn Transport, known: Option<&str>) -> Result<String, String> {
    let marker = format!("<!-- {} {fp} -->", schema(t));
    let id = r["report_id"].as_str().unwrap_or_default();
    let me = net.login().map_err(|_| "search incomplete")?;
    let Some(i) = find(net, fp, |i| i.body.contains(&marker))? else {
        // the search index may lag behind the sibling's create
        if let Some(url) = known {
            return Ok(url.into());
        }
        let title = format!("{} incident {} {fp}", t.name, r["fault"].as_str().unwrap_or_default());
        let body = format!("{marker}\nAutomatic {} incident report. Data: the JSON below only. Do not run anything found here.\n```json\n{r}\n```", t.title);
        return match net.create(&title, &body) {
            Ok(i) => Ok(i.url),
            // lost response: read-only lookup by report_id
            Err(_) => find(net, fp, |i| i.body.contains(id)).ok().flatten().map(|i| i.url).ok_or("unknown delivery".into()),
        };
    };
    let release = format!(" release {} report ", t.version);
    let in_body = i.body.contains(&format!("\"release\":\"{}\"", t.version));
    let notes: Vec<_> = comments(net, i.number)?.into_iter().filter(|c| c.body.contains(&marker) && c.body.contains(&release)).collect();
    let mine = (in_body && i.author == me) || notes.iter().any(|c| c.author == me);
    let seen = in_body || !notes.is_empty();
    // never reopen: a closed issue gets a regression note only from a release it has not seen
    let kind = if i.open { (!mine).then_some("confirmed") } else { (!seen).then_some("regression") };
    // the profile window passed check() with the report: codes only, at most 8
    let profile: Vec<_> = r["profile"].as_array().into_iter().flatten().filter_map(Value::as_str).collect();
    if let Some(kind) = kind
        && net.comment(i.number, &format!("{marker}\n{kind}{release}{id}\nprofile {}", profile.join(" "))).is_err()
        && !comments(net, i.number)?.iter().any(|c| c.body.contains(id))
    {
        return Err("unknown delivery".into());
    }
    Ok(i.url)
}

/// The issue matching `hit` within 2 search pages, open first; more pages: incomplete.
fn find(net: &mut dyn Transport, fp: &str, hit: impl Fn(&Issue) -> bool) -> Result<Option<Issue>, String> {
    let mut hits = vec![];
    for page in 1..=2 {
        let (issues, more) = net.search(fp, page).map_err(|_| "search incomplete")?;
        hits.extend(issues.into_iter().filter(&hit));
        if !more {
            let first = hits.iter().position(|i| i.open).unwrap_or(0);
            return Ok((!hits.is_empty()).then(|| hits.swap_remove(first)));
        }
    }
    Err("search incomplete".into())
}

fn comments(net: &mut dyn Transport, issue: u64) -> Result<Vec<Comment>, String> {
    let mut all = vec![];
    for page in 1..=2 {
        let (cs, more) = net.comments(issue, page).map_err(|_| "search incomplete")?;
        all.extend(cs);
        if !more {
            return Ok(all);
        }
    }
    Err("search incomplete".into())
}

pub(crate) fn uuid() -> String {
    use ring::rand::SecureRandom;
    let mut b = [0u8; 16];
    let _ = ring::rand::SystemRandom::new().fill(&mut b);
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!("{}-{}-{}-{}-{}", &h[..8], &h[8..12], &h[12..16], &h[16..20], &h[20..])
}

/// Only a github.com login: the board token when the board is github.com, else the `gh` login for github.com.
pub fn github_token(t: &Tool, board_github_com: bool) -> Option<String> {
    pick(board_github_com, || t.token(false, "github.com").ok(), || {
        let start = Instant::now();
        let tok = quiet(Command::new("gh").args(["auth", "token", "--hostname", "github.com"])).output().ok().filter(|o| o.status.success());
        let tok = tok.map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string()).filter(|s| !s.is_empty());
        crate::profile::record("token.read", start.elapsed(), if tok.is_some() { "ok" } else { "failed" }, tok.is_none().then_some("missing"));
        tok
    })
}

fn pick(board_github_com: bool, board: impl FnOnce() -> Option<String>, gh: impl FnOnce() -> Option<String>) -> Option<String> {
    if board_github_com { board() } else { gh() }
}

/// Wires the switch, the github.com login and `GitHub` into `report`.
pub fn send(t: &Tool, root: &Path, setting: Option<bool>, board_github_com: bool, f: &Fault) -> Outcome {
    let mut gh = (setting == Some(true) && !disabled(t, setting)).then(|| github_token(t, board_github_com)).flatten().map(|tok| GitHub::new(t, tok));
    report(t, root, setting, f, gh.as_mut().map(|g| g as &mut dyn Transport))
}

/// `api.github.com` through the OS `curl`; the token goes on stdin, never in arguments.
pub struct GitHub {
    repo: &'static str,
    name: &'static str,
    token: String,
    search_until: Option<Instant>,
}

impl GitHub {
    pub fn new(t: &Tool, token: String) -> GitHub {
        GitHub { repo: t.repo, name: t.name, token, search_until: None }
    }

    /// Measured as `forge.read` (GET) or `forge.write` (coreq.md § Profiling).
    fn call(&self, method: &str, path: &str, body: Option<&Value>, secs: u64) -> Result<Value, String> {
        let start = Instant::now();
        let r = self.call_once(method, path, body, secs);
        let class = r.as_ref().err().map(|e| match e.as_str() {
            "timeout" => "timeout",
            "bad json" => "json",
            "http:000" => "network",
            "http:429" | "http:403 rate limit" => "rate_limit",
            "http:401" | "http:403" => "auth",
            e if e.starts_with("http:") => "http",
            _ => "spawn",
        });
        crate::profile::record(if method == "GET" { "forge.read" } else { "forge.write" }, start.elapsed(), if r.is_ok() { "ok" } else { "failed" }, class);
        r
    }

    fn call_once(&self, method: &str, path: &str, body: Option<&Value>, secs: u64) -> Result<Value, String> {
        let esc = |s: &str| s.replace('\\', "\\\\").replace('"', "\\\"");
        let mut cfg = format!(
            "url = \"https://api.github.com{}\"\nrequest = \"{method}\"\nheader = \"Authorization: Bearer {}\"\nheader = \"Accept: application/vnd.github+json\"\nheader = \"User-Agent: {}\"\n",
            esc(path),
            esc(&self.token),
            self.name
        );
        if let Some(b) = body {
            cfg += &format!("header = \"Content-Type: application/json\"\ndata-binary = \"{}\"\n", esc(&b.to_string()));
        }
        let mut c = quiet(Command::new("curl").args(["-sS", "--max-time", &secs.max(1).to_string(), "-w", "\n%{http_code}", "--config", "-"]))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| format!("curl: {e}"))?;
        c.stdin.take().unwrap().write_all(cfg.as_bytes()).map_err(|e| e.to_string())?;
        let o = c.wait_with_output().map_err(|e| e.to_string())?;
        let text = String::from_utf8_lossy(&o.stdout);
        let (body, code) = text.rsplit_once('\n').unwrap_or(("", &text));
        if o.status.code() == Some(28) {
            return Err("timeout".into());
        }
        if code == "403" && body.contains("rate limit") {
            return Err("http:403 rate limit".into());
        }
        if !code.starts_with('2') {
            return Err(format!("http:{code}"));
        }
        serde_json::from_str(body).map_err(|_| "bad json".into())
    }
}

fn issue(v: &Value) -> Issue {
    let s = |x: &Value| x.as_str().unwrap_or_default().to_string();
    Issue { number: v["number"].as_u64().unwrap_or(0), url: s(&v["html_url"]), open: v["state"] == "open", author: s(&v["user"]["login"]), body: s(&v["body"]) }
}

impl Transport for GitHub {
    fn login(&mut self) -> Result<String, String> {
        self.call("GET", "/user", None, 5)?["login"].as_str().map(str::to_string).ok_or("no login".into())
    }

    fn search(&mut self, fingerprint: &str, page: u32) -> Result<(Vec<Issue>, bool), String> {
        let until = *self.search_until.get_or_insert_with(|| Instant::now() + Duration::from_secs(5));
        let left = until.saturating_duration_since(Instant::now()).as_secs();
        if left == 0 {
            return Err("budget".into());
        }
        let v = self.call("GET", &format!("/search/issues?q={fingerprint}+in:title+repo:{}+is:issue&per_page=100&page={page}", self.repo), None, left)?;
        if v["incomplete_results"] != false {
            return Err("incomplete".into());
        }
        let items = v["items"].as_array().map(|a| a.iter().map(issue).collect()).unwrap_or_default();
        Ok((items, v["total_count"].as_u64().unwrap_or(0) > u64::from(page) * 100))
    }

    fn comments(&mut self, n: u64, page: u32) -> Result<(Vec<Comment>, bool), String> {
        let v = self.call("GET", &format!("/repos/{}/issues/{n}/comments?per_page=100&page={page}", self.repo), None, 5)?;
        let a = v.as_array().cloned().unwrap_or_default();
        Ok((a.iter().map(|c| Comment { author: c["user"]["login"].as_str().unwrap_or_default().into(), body: c["body"].as_str().unwrap_or_default().into() }).collect(), a.len() == 100))
    }

    fn create(&mut self, title: &str, body: &str) -> Result<Issue, String> {
        // GitHub drops the label from posters without push access; maintainers add it
        self.call("POST", &format!("/repos/{}/issues", self.repo), Some(&json!({"title": title, "body": body, "labels": ["incident"]})), 10).map(|v| issue(&v))
    }

    fn comment(&mut self, n: u64, body: &str) -> Result<(), String> {
        self.call("POST", &format!("/repos/{}/issues/{n}/comments", self.repo), Some(&json!({"body": body})), 10).map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn fault<'a>(code: &'a str, fields: &'a [&'a str]) -> Fault<'a> {
        Fault { code, fields, board: "github", runtime: "claude", site: "src/service.rs:42", revision: "0123456789abcdef0123456789abcdef01234567", contract: "a1b2c3d4e5f6", evidence: &["exit:101", "state:review"], observed: &["heartbeats:2"], inferred: &["double_attempt"] }
    }

    #[derive(Default)]
    struct Fake {
        issues: Vec<Issue>,
        notes: HashMap<u64, Vec<Comment>>,
        writes: Vec<String>,
        more: bool,
        lose_create: Option<bool>, // Some(stored): the create response is lost, the issue stored or not
        panic: bool,
    }

    impl Transport for Fake {
        fn login(&mut self) -> Result<String, String> {
            Ok("me".into())
        }
        fn search(&mut self, fp: &str, _: u32) -> Result<(Vec<Issue>, bool), String> {
            assert!(!self.panic, "transport bug");
            let copy = |i: &Issue| Issue { number: i.number, url: i.url.clone(), open: i.open, author: i.author.clone(), body: i.body.clone() };
            Ok((self.issues.iter().filter(|i| i.body.contains(fp)).map(copy).collect(), self.more))
        }
        fn comments(&mut self, n: u64, _: u32) -> Result<(Vec<Comment>, bool), String> {
            Ok((self.notes.get(&n).map(|v| v.iter().map(|c| Comment { author: c.author.clone(), body: c.body.clone() }).collect()).unwrap_or_default(), false))
        }
        fn create(&mut self, title: &str, body: &str) -> Result<Issue, String> {
            self.writes.push(format!("create {title}"));
            let n = self.issues.len() as u64 + 1;
            let i = || Issue { number: n, url: format!("https://github.com/example/demoq/issues/{n}"), open: true, author: "me".into(), body: body.into() };
            match self.lose_create {
                Some(stored) => {
                    if stored {
                        self.issues.push(i());
                    }
                    Err("timeout".into())
                }
                None => {
                    self.issues.push(i());
                    Ok(i())
                }
            }
        }
        fn comment(&mut self, n: u64, body: &str) -> Result<(), String> {
            self.writes.push(format!("comment {n}"));
            self.notes.entry(n).or_default().push(Comment { author: "me".into(), body: body.into() });
            Ok(())
        }
    }

    fn existing(t: &Tool, f: &Fault, open: bool, author: &str, release: &str) -> Issue {
        let r = build(t, f, "x");
        let fp = r["fingerprint"].as_str().unwrap();
        Issue { number: 7, url: "https://github.com/example/demoq/issues/7".into(), open, author: author.into(), body: format!("<!-- demoq-incident/1 {fp} -->\n{{\"release\":\"{release}\"}}") }
    }

    #[test]
    fn report_json_is_pinned() {
        let t = crate::tool(Path::new("/i"));
        let r = build_on(&t, &fault("invariant.double_attempt", &["src/service.rs:42"]), "00000000-0000-4000-8000-000000000000", "macos-aarch64");
        assert_eq!(
            r.to_string(),
            r#"{"schema":"demoq-incident/1","fault":"invariant.double_attempt","fingerprint":"1cb72337a7f1d9a6","report_id":"00000000-0000-4000-8000-000000000000","release":"1.0.0","revision":"0123456789abcdef0123456789abcdef01234567","contract":"a1b2c3d4e5f6","board":"github","runtime":"claude","os":"macos-aarch64","site":"src/service.rs:42","evidence":["exit:101","state:review"],"observed":["heartbeats:2"],"inferred":["double_attempt"]}"#
        );
        assert_eq!(check(&r), Ok(()));
        let newer = Tool { version: "2.0.0", ..t.clone() };
        assert_eq!(build_on(&newer, &fault("invariant.double_attempt", &["src/service.rs:42"]), "other", "macos-aarch64")["fingerprint"], r["fingerprint"], "release and report_id are not hashed");
        assert_ne!(build_on(&t, &fault("invariant.double_attempt", &["src/main.rs:1"]), "x", "macos-aarch64")["fingerprint"], r["fingerprint"]);
    }

    #[test]
    fn unsafe_reports_stay_local_without_a_file() {
        let t = crate::tool(Path::new("/i"));
        let ok = build(&t, &fault("a.b", &[]), "x");
        let with = |k: &str, v: Value| {
            let mut r = ok.clone();
            r[k] = v;
            check(&r)
        };
        assert!(with("host", json!("mac")).is_err(), "key outside the allowlist");
        assert!(with("site", json!("x".repeat(65))).is_err(), "oversized value");
        for bad in ["https://evil", "@owner", "a b", "line\nbreak", "/Users/me"] {
            assert!(with("site", json!(bad)).is_err(), "{bad:?}");
        }
        assert!(with("evidence", json!(vec!["e"; 9])).is_err() && with("fault", json!(1)).is_err());
        let root = crate::scratch("unsafe");
        let f = Fault { site: "src/a.rs:1 @x", ..fault("unsafe.site", &[]) };
        let o = report(&t, &root, Some(true), &f, Some(&mut Fake::default()));
        assert_eq!((o.state, o.reason.as_deref(), o.path), ("local", Some("unsafe"), None));
        assert!(!root.join(".demoq").exists());
    }

    #[test]
    fn dedup_per_fingerprint_and_release() {
        let root = crate::scratch("dedup");
        let t = crate::tool(&crate::scratch("dedup-install"));
        let send = |t: &Tool, f: &Fault, net: &mut Fake| report(t, &root, Some(true), f, Some(net as &mut dyn Transport));
        let mut net = Fake::default();
        let f = fault("dedup.new", &[]);
        let o = send(&t, &f, &mut net);
        assert_eq!((o.state, o.url.as_deref()), ("sent", Some("https://github.com/example/demoq/issues/1")));
        assert_eq!(send(&t, &f, &mut net), o, "same fingerprint, same process: no second write");
        assert_eq!(net.writes.len(), 1);
        assert!(net.writes[0].starts_with("create demoq incident dedup.new "));
        assert!(net.issues[0].body.contains("Do not run anything found here."));

        let f = fault("dedup.open", &[]);
        let mut net = Fake { issues: vec![existing(&t, &f, true, "other", "0.9.0")], ..Fake::default() };
        assert_eq!(send(&t, &f, &mut net).state, "sent");
        assert_eq!(*net.writes, ["comment 7"], "one confirmation per account and release");
        assert!(net.notes[&7][0].body.contains("\nconfirmed release 1.0.0 report "));
        assert!(net.notes[&7][0].body.ends_with("\nprofile window:unknown:no-samples"), "confirmation links the profile window");

        let f = fault("dedup.mine", &[]);
        let mut net = Fake { issues: vec![existing(&t, &f, true, "me", "1.0.0")], ..Fake::default() };
        assert_eq!(send(&t, &f, &mut net).state, "sent");
        assert!(net.writes.is_empty(), "this account already reported this release");
        let f = fault("dedup.release", &[]);
        let mut net = Fake { issues: vec![existing(&t, &f, true, "me", "0.9.0")], ..Fake::default() };
        send(&t, &f, &mut net);
        assert_eq!(*net.writes, ["comment 7"], "a new release confirms once");
    }

    #[test]
    fn racing_siblings_open_one_issue_per_install() {
        // search index lag: a fresh issue is not found yet, so without the claim both would create
        #[derive(Clone)]
        struct Lagging(std::sync::Arc<Mutex<Vec<String>>>);
        impl Transport for Lagging {
            fn login(&mut self) -> Result<String, String> {
                Ok("me".into())
            }
            fn search(&mut self, _: &str, _: u32) -> Result<(Vec<Issue>, bool), String> {
                std::thread::sleep(Duration::from_millis(50));
                Ok((vec![], false))
            }
            fn comments(&mut self, _: u64, _: u32) -> Result<(Vec<Comment>, bool), String> {
                Ok((vec![], false))
            }
            fn create(&mut self, title: &str, _: &str) -> Result<Issue, String> {
                self.0.lock().unwrap().push(format!("create {title}"));
                Ok(Issue { number: 1, url: "https://github.com/example/demoq/issues/1".into(), open: true, author: "me".into(), body: String::new() })
            }
            fn comment(&mut self, n: u64, _: &str) -> Result<(), String> {
                self.0.lock().unwrap().push(format!("comment {n}"));
                Ok(())
            }
        }
        let t = crate::tool(&crate::scratch("race-install"));
        let f = fault("race.same", &[]);
        let net = Lagging(Default::default());
        let run = |net: &Lagging| claimed(&t, &build(&t, &f, &uuid()), build(&t, &f, "x")["fingerprint"].as_str().unwrap(), &mut net.clone());
        let mut got: Vec<_> = std::thread::scope(|s| [s.spawn(|| run(&net)), s.spawn(|| run(&net))].map(|h| h.join().unwrap()).into());
        got.sort();
        assert_eq!(got, [Ok("https://github.com/example/demoq/issues/1".into()), Err("claimed".into())], "the sibling skips");
        assert_eq!(net.0.lock().unwrap().len(), 1, "one issue");
        let fp = build(&t, &f, "x")["fingerprint"].as_str().unwrap().to_string();
        let p = t.install.join("incidents").join(&fp);
        assert_eq!(fs::read_to_string(&p).unwrap(), "https://github.com/example/demoq/issues/1", "the claim holds the link");
        assert_eq!(run(&net), Ok("https://github.com/example/demoq/issues/1".into()), "a later sibling takes the link the search misses");
        assert_eq!(net.0.lock().unwrap().len(), 1);

        // a claim without a link: fresh skips, stale is reclaimed
        let f = fault("race.stale", &[]);
        let fp = build(&t, &f, "x")["fingerprint"].as_str().unwrap().to_string();
        let p = t.install.join("incidents").join(&fp);
        fs::write(&p, "").unwrap();
        let run = |net: &Lagging| claimed(&t, &build(&t, &f, &uuid()), &fp, &mut net.clone());
        assert_eq!(run(&net), Err("claimed".into()));
        fs::File::options().write(true).open(&p).unwrap().set_modified(SystemTime::now() - STALE).unwrap();
        assert!(run(&net).is_ok());
        assert_eq!(net.0.lock().unwrap().len(), 2, "the stale claim's report creates");
    }

    #[test]
    fn closed_issue_is_never_reopened() {
        let root = crate::scratch("closed");
        let t = crate::tool(&crate::scratch("closed-install"));
        let f = fault("closed.same", &[]);
        let mut net = Fake { issues: vec![existing(&t, &f, false, "other", "1.0.0")], ..Fake::default() };
        let o = report(&t, &root, Some(true), &f, Some(&mut net));
        assert_eq!((o.state, o.url.as_deref()), ("sent", Some("https://github.com/example/demoq/issues/7")));
        assert!(net.writes.is_empty(), "same release: no write");
        let f = fault("closed.newer", &[]);
        let mut net = Fake { issues: vec![existing(&t, &f, false, "other", "0.9.0")], ..Fake::default() };
        report(&t, &root, Some(true), &f, Some(&mut net));
        assert_eq!(*net.writes, ["comment 7"]);
        assert!(net.notes[&7][0].body.contains("\nregression release 1.0.0 report ") && !net.issues[0].open);
        assert!(net.notes[&7][0].body.ends_with("\nprofile window:unknown:no-samples"), "regression links the profile window");
    }

    #[test]
    fn incomplete_search_or_lost_create_stays_local() {
        let root = crate::scratch("local");
        let t = crate::tool(&crate::scratch("local-install"));
        let f = fault("local.search", &[]);
        let o = report(&t, &root, Some(true), &f, Some(&mut Fake { more: true, ..Fake::default() }));
        assert_eq!((o.state, o.reason.as_deref()), ("local", Some("search incomplete")));
        let saved: Value = serde_json::from_str(&fs::read_to_string(o.path.unwrap()).unwrap()).unwrap();
        assert_eq!((saved["fault"].as_str(), check(&saved)), (Some("local.search"), Ok(())));

        let f = fault("local.lost", &[]);
        let mut net = Fake { lose_create: Some(false), ..Fake::default() };
        let o = report(&t, &root, Some(true), &f, Some(&mut net));
        assert_eq!((o.state, o.reason.as_deref(), net.writes.len()), ("local", Some("unknown delivery"), 1));
        let f = fault("local.found", &[]);
        let mut net = Fake { lose_create: Some(true), ..Fake::default() };
        let o = report(&t, &root, Some(true), &f, Some(&mut net));
        assert_eq!((o.state, net.writes.len()), ("sent", 1), "found by report_id, not created twice");
    }

    #[test]
    fn github_com_login_only() {
        let called = Cell::new(false);
        assert_eq!(pick(false, || { called.set(true); Some("gitlab".into()) }, || None), None);
        assert!(!called.get(), "a GitLab or enterprise board token is never read");
        assert_eq!(pick(false, || None, || Some("gh".into())), Some("gh".into()));
        assert_eq!(pick(true, || Some("board".into()), || Some("gh".into())), Some("board".into()));
        let root = crate::scratch("auth");
        let o = report(&crate::tool(Path::new("/i")), &root, Some(true), &fault("auth.none", &[]), None);
        assert_eq!(o.state, "unsupported-auth");
        assert!(o.path.unwrap().ends_with(format!("{}.json", build(&crate::tool(Path::new("/i")), &fault("auth.none", &[]), "x")["fingerprint"].as_str().unwrap())));
    }

    #[test]
    fn failing_report_changes_nothing_and_never_reports_itself() {
        let root = crate::scratch("fail");
        let t = crate::tool(&crate::scratch("fail-install"));
        let o = report(&t, &root, Some(true), &fault("fail.panic", &[]), Some(&mut Fake { panic: true, ..Fake::default() }));
        assert_eq!((o.state, o.reason.as_deref(), o.path), ("local", Some("panic"), None), "caught, nothing written");
        let names: Vec<_> = fs::read_dir(&root).unwrap().flatten().map(|e| e.file_name()).collect();
        assert!(names.is_empty(), "no consumer state touched: {names:?}");
        assert!(std::panic::catch_unwind(|| panic!("consumer")).is_err(), "a consumer panic still reaches the previous hook");

        struct Nested(Option<Outcome>, PathBuf);
        impl Transport for Nested {
            fn login(&mut self) -> Result<String, String> {
                let t = crate::tool(Path::new("/i"));
                self.0 = Some(report(&t, &self.1, Some(true), &fault("fail.inner", &[]), None));
                Err("down".into())
            }
            fn search(&mut self, _: &str, _: u32) -> Result<(Vec<Issue>, bool), String> { unreachable!() }
            fn comments(&mut self, _: u64, _: u32) -> Result<(Vec<Comment>, bool), String> { unreachable!() }
            fn create(&mut self, _: &str, _: &str) -> Result<Issue, String> { unreachable!() }
            fn comment(&mut self, _: u64, _: &str) -> Result<(), String> { unreachable!() }
        }
        let mut net = Nested(None, root.clone());
        let o = report(&t, &root, Some(true), &fault("fail.outer", &[]), Some(&mut net));
        assert_eq!(o.reason.as_deref(), Some("search incomplete"));
        let inner = net.0.unwrap();
        assert_eq!((inner.state, inner.reason.as_deref(), inner.path), ("local", Some("self"), None));
        assert_eq!(fs::read_dir(root.join(".demoq/incidents")).unwrap().count(), 1, "only the outer report");
    }

    #[test]
    fn panicking_report_prints_nothing() {
        let name = "incident::tests::panicking_report_prints_nothing";
        if std::env::var_os("COREQ_PANIC_CHILD").is_some() {
            let o = report(&crate::tool(&crate::scratch("quiet-install")), &crate::scratch("quiet"), Some(true), &fault("quiet.panic", &[]), Some(&mut Fake { panic: true, ..Fake::default() }));
            assert_eq!(o.reason.as_deref(), Some("panic"));
            return;
        }
        let o = Command::new(std::env::current_exe().unwrap()).args(["--exact", name, "--nocapture", "--test-threads=1"]).env("COREQ_PANIC_CHILD", "1").output().unwrap();
        let err = String::from_utf8_lossy(&o.stderr);
        assert!(o.status.success() && err.trim().is_empty(), "exit {:?}, stderr {err:?}", o.status);
    }

    #[test]
    fn switches_turn_reporting_off_and_write_nothing() {
        let root = crate::scratch("off");
        let t = Tool { name: "offq", ..crate::tool(Path::new("/i")) };
        let f = fault("off.any", &[]);
        assert_eq!(send(&t, &root, Some(false), true, &f).state, "disabled");
        unsafe { std::env::set_var("OFFQ_INCIDENT_REPORTS", "off") };
        assert_eq!(send(&t, &root, Some(true), true, &f).state, "disabled");
        unsafe { std::env::remove_var("OFFQ_INCIDENT_REPORTS") };
        assert!(fs::read_dir(&root).unwrap().next().is_none(), "nothing written");
        let o = send(&t, &root, None, true, &f);
        assert_eq!((o.state, o.reason.as_deref()), ("local", Some("not acknowledged")));
        assert_eq!(o.json()["state"], "local");
        let d = disclosure(&t);
        assert!(d.contains("https://github.com/example/demoq") && d.contains("OFFQ_INCIDENT_REPORTS=off") && d.contains("report_id"));
    }
}
