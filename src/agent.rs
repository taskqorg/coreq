//! Claude Code and Codex CLI turns for every consumer (coreq.md § Agent runtimes, docs/design/agent.md).
//! One native session per conversation or attempt; each turn is one client process resumed by session ID.
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

mod native;
mod app;
pub use native::{BackgroundJob, NativeTurn, background_command, background_launch, background_jobs, background_stop, app_queue};
pub use app::{AppServer, AppSession, app_server_command};

type Res<T> = Result<T, String>;

/// A supported client: Claude Code (`claude -p`) or Codex CLI (`codex exec`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Client { Claude, Codex }

impl Client {
    pub fn parse(code: &str) -> Option<Client> {
        match code { "claude" => Some(Client::Claude), "codex" => Some(Client::Codex), _ => None }
    }
    /// The runtime code in observations; also the CLI binary name.
    pub fn code(self) -> &'static str { match self { Client::Claude => "claude", Client::Codex => "codex" } }
    /// The variable that names a login home to the client.
    pub fn home_var(self) -> &'static str { match self { Client::Claude => "CLAUDE_CONFIG_DIR", Client::Codex => "CODEX_HOME" } }
    fn home_dir(self) -> &'static str { match self { Client::Claude => ".claude", Client::Codex => ".codex" } }
}

// ---------- executable ----------

/// `name` on `PATH`; on Windows also `name.exe` and `name.cmd` (npm installs `.cmd` shims). Unresolved: the bare name.
pub fn which(name: &str) -> PathBuf { which_in(name, &std::env::var_os("PATH").unwrap_or_default()) }

pub fn which_in(name: &str, path: &OsStr) -> PathBuf {
    let exts: &[&str] = if cfg!(windows) { &[".exe", ".cmd", ""] } else { &[""] };
    for dir in std::env::split_paths(path) {
        for e in exts {
            let p = dir.join(format!("{name}{e}"));
            if p.is_file() { return p; }
        }
    }
    PathBuf::from(name)
}

/// `PATH` with `<home>/.local/bin` appended when it is a directory `PATH` lacks; `None` when nothing changes.
/// Linux user installs (codex, npm prefixes, pipx) land there, but a non-login shell or a service unit may lack it.
pub fn user_bin_path(path: &OsStr, home: Option<&OsStr>) -> Option<OsString> {
    let bin = Path::new(home.filter(|h| !h.is_empty())?).join(".local/bin");
    let mut dirs: Vec<PathBuf> = std::env::split_paths(path).collect();
    if !bin.is_dir() || dirs.contains(&bin) { return None; }
    dirs.push(bin);
    std::env::join_paths(dirs).ok()
}

/// Why `client` cannot start here: its CLI does not resolve on `PATH`.
pub fn missing_cli(client: Client) -> Option<String> {
    let bin = client.code();
    (!which(bin).is_absolute()).then(|| format!("{bin} CLI not found on the service PATH"))
}

/// The native Codex executor behind an npm `codex.js` launcher: a kill cannot reach the native child through Node.
pub fn codex_native(bin: PathBuf) -> Res<PathBuf> {
    if cfg!(windows) || !bin.exists() { return Ok(bin); }
    let bin = bin.canonicalize().map_err(|e| format!("Codex executable unavailable: {e}"))?;
    let native = |path: &Path| -> Res<bool> {
        let mut magic = [0; 4];
        File::open(path).and_then(|mut f| f.read_exact(&mut magic)).map_err(|e| format!("Codex executable unreadable: {e}"))?;
        Ok(matches!(magic, [0x7f, b'E', b'L', b'F'] | [0xcf, 0xfa, 0xed, 0xfe] | [0xfe, 0xed, 0xfa, 0xcf] | [0xce, 0xfa, 0xed, 0xfe] | [0xfe, 0xed, 0xfa, 0xce] | [0xca, 0xfe, 0xba, 0xbe] | [0xbe, 0xba, 0xfe, 0xca]))
    };
    if native(&bin)? { return Ok(bin); }
    let package = bin.parent().filter(|p| p.file_name().is_some_and(|n| n == "bin")).and_then(Path::parent)
        .filter(|_| bin.file_name().is_some_and(|n| n == "codex.js"))
        .ok_or("Codex launcher ownership unsupported; select a native Codex executable")?;
    let metadata: Value = serde_json::from_reader(File::open(package.join("package.json")).map_err(|e| format!("Codex package unavailable: {e}"))?.take(64 * 1024))
        .map_err(|e| format!("Codex package invalid: {e}"))?;
    if metadata["name"] != "@openai/codex" { return Err("Codex launcher package identity differs".into()); }
    let (platform, target) = match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => ("darwin-arm64", "aarch64-apple-darwin"),
        ("macos", "x86_64") => ("darwin-x64", "x86_64-apple-darwin"),
        ("linux", "aarch64") => ("linux-arm64", "aarch64-unknown-linux-musl"),
        ("linux", "x86_64") => ("linux-x64", "x86_64-unknown-linux-musl"),
        _ => return Err("Codex native package unsupported on this platform".into()),
    };
    // Match Node's package lookup along the entrypoint's ancestors; never scan other installations.
    let installed = package.ancestors().map(|p| p.join(format!("node_modules/@openai/codex-{platform}")))
        .find(|p| p.join("package.json").is_file()).unwrap_or_else(|| package.to_path_buf());
    let executable = installed.join(format!("vendor/{target}/bin/codex"));
    if !native(&executable)? { return Err("Codex package executor is not a native binary".into()); }
    executable.canonicalize().map_err(|e| format!("Codex native executable unavailable: {e}"))
}

/// The client's CLI executable: `claude` on `PATH`, Codex's native executor. A caller that wraps the client (a
/// sandbox) builds its own base command around this path.
pub fn executable(client: Client) -> Res<PathBuf> {
    match client { Client::Claude => Ok(which("claude")), Client::Codex => codex_native(which("codex")) }
}

// ---------- accounts ----------

/// The client's own login home account.
pub const DEFAULT: &str = "default";
/// A limit without a reported reset ends this many seconds after it was seen.
pub const PROBE: i64 = 1800;

#[derive(Clone, Debug, PartialEq)]
pub struct Entry { pub label: String, pub enabled: bool }

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Limit {
    /// When the limit was seen (unix seconds).
    pub at: i64,
    /// The reported reset, when the client named one.
    pub until: Option<i64>,
}

impl Limit {
    /// When the limit ends: the reported reset, else the probe after it.
    pub fn end(&self) -> i64 { self.until.unwrap_or(self.at + PROBE) }
}

/// An account label: 1-32 of `[a-z0-9-]`, not starting with `-`.
pub fn valid_label(l: &str) -> bool {
    (1..=32).contains(&l.len()) && !l.starts_with('-') && l.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// One client's ordered accounts and their recorded limits; the caller owns where both are stored.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Accounts { pub list: Vec<Entry>, pub limits: BTreeMap<String, Limit> }

impl Accounts {
    /// The list in priority order; an empty list is one enabled `default` account.
    pub fn entries(&self) -> Vec<Entry> {
        if self.list.is_empty() { vec![Entry { label: DEFAULT.into(), enabled: true }] } else { self.list.clone() }
    }
    fn free(&self, label: &str, now: i64) -> bool { self.limits.get(label).is_none_or(|l| l.end() <= now) }
    /// The first enabled account whose limit has ended.
    pub fn active(&self, now: i64) -> Option<String> {
        self.entries().into_iter().find(|e| e.enabled && self.free(&e.label, now)).map(|e| e.label)
    }
    /// The account a new start takes: the active one, else the first enabled one; None when all are disabled.
    pub fn pick(&self, now: i64) -> Option<String> {
        self.active(now).or_else(|| self.entries().into_iter().find(|e| e.enabled).map(|e| e.label))
    }
    /// More than one account: failover applies.
    pub fn multi(&self) -> bool { self.entries().len() > 1 }
    /// The account a usage limit moves to: only with more than one account.
    pub fn spare(&self, now: i64) -> Option<String> { self.multi().then(|| self.active(now)).flatten() }
    /// With none left: the earliest live limit end of an enabled account and whether it was reported.
    pub fn hold(&self, now: i64) -> Option<(i64, bool)> {
        self.entries().iter().filter(|e| e.enabled).filter_map(|e| self.limits.get(&e.label)).filter(|l| l.end() > now)
            .map(|l| (l.end(), l.until.is_some())).min()
    }
    /// Every account disabled: the client admits nothing.
    pub fn off(&self) -> bool { self.entries().iter().all(|e| !e.enabled) }
}

/// The client's own home: `CLAUDE_CONFIG_DIR` / `CODEX_HOME` of this process, else `~/.claude` / `~/.codex`
/// (a Windows service has no `HOME`, so `USERPROFILE`).
pub fn base_home(client: Client) -> Option<PathBuf> {
    std::env::var_os(client.home_var()).filter(|v| !v.is_empty()).map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")).map(|h| Path::new(&h).join(client.home_dir())))
}

/// `<base home>/<tool>-accounts/<label>` for a non-default account; None for `default` (the child keeps the
/// caller's own home). `tool` is the consumer's `Tool.name`.
pub fn account_home(client: Client, tool: &str, label: &str) -> Option<PathBuf> {
    (label != DEFAULT && valid_label(label)).then(|| base_home(client).map(|b| b.join(format!("{tool}-accounts")).join(label))).flatten()
}

/// Every home of `client`: the default first, then each existing account home in label order.
pub fn account_homes(client: Client, tool: &str) -> Vec<PathBuf> {
    let Some(b) = base_home(client) else { return vec![] };
    let mut more: Vec<PathBuf> = std::fs::read_dir(b.join(format!("{tool}-accounts"))).into_iter().flatten().flatten()
        .filter(|e| e.file_name().to_str().is_some_and(valid_label) && e.path().is_dir()).map(|e| e.path()).collect();
    more.sort();
    let mut v = vec![b];
    v.extend(more);
    v
}

// ---------- start and resume ----------

/// A new session ID: Claude takes a caller-chosen UUID; Codex names its thread in `thread.started` (`-` until then).
pub fn new_session(client: Client) -> String {
    match client { Client::Claude => crate::incident::uuid(), Client::Codex => "-".into() }
}

/// One turn of one session.
#[derive(Clone, Debug)]
pub struct Spec {
    pub client: Client,
    /// The project root: working directory of the client and Codex `-C` on a new start.
    pub root: PathBuf,
    /// The native session ID: `--session-id` / `--resume` (Claude), `resume <thread>` (Codex).
    pub session: String,
    /// Continue `session` instead of starting it.
    pub resume: bool,
    /// Caller-owned client arguments in TaskQ's position: Claude permission mode and `--settings`, Codex sandbox flags.
    pub args: Vec<OsString>,
    /// The account's login home (`account_home`); None keeps the caller's own.
    pub home: Option<PathBuf>,
    /// Extra child environment; values never enter argv or the log.
    pub env: Vec<(OsString, OsString)>,
}

/// The client argument list after the executable, in TaskQ's order.
pub fn args(spec: &Spec) -> Vec<OsString> {
    let mut a: Vec<OsString> = Vec::new();
    let s = |v: &str| OsString::from(v);
    match spec.client {
        Client::Claude => {
            a.extend(["-p", "--output-format", "stream-json", "--verbose"].map(s));
            a.extend(spec.args.iter().cloned());
            a.push(s(if spec.resume { "--resume" } else { "--session-id" }));
            a.push(s(&spec.session));
        }
        Client::Codex => {
            a.extend(["exec", "--json"].map(s));
            a.extend(spec.args.iter().cloned());
            if spec.resume { a.extend([s("resume"), s(&spec.session), s("-")]); }
            else { a.extend([s("-C"), spec.root.clone().into_os_string(), s("-")]); }
        }
    }
    a
}

/// `base` (the client executable, or a sandbox wrapper around it) with the turn's arguments and environment. The
/// inherited session variables of an enclosing client are removed so a child never joins its parent's session.
pub fn command(mut base: Command, spec: &Spec) -> Command {
    base.args(args(spec));
    environment(base, spec)
}

fn environment(mut base: Command, spec: &Spec) -> Command {
    base.current_dir(&spec.root).env_remove("CLAUDE_CODE_SESSION_ID").env_remove("CODEX_THREAD_ID")
        .envs(spec.env.iter().map(|(k, v)| (k, v)));
    if let Some(home) = &spec.home { base.env(spec.client.home_var(), home); }
    crate::quiet(&mut base);
    base
}

/// Claude `--settings` JSON: a `SessionStart` hook on `compact` that runs `compact_hook` (rule re-delivery after
/// compaction) and `env` for the session, which survives resume. None omits the hook.
pub fn claude_settings(compact_hook: Option<&str>, env: &[(&str, &str)]) -> String {
    let env: serde_json::Map<String, Value> = env.iter().map(|(k, v)| (k.to_string(), json!(v))).collect();
    let mut v = json!({"env": env});
    if let Some(cmd) = compact_hook {
        v = json!({"hooks": {"SessionStart": [{"matcher": "compact", "hooks": [{"type": "command", "command": cmd}]}]}, "env": v["env"]});
    }
    v.to_string()
}

// ---------- running turn ----------

/// One running turn: the client process with stdout and stderr appended to the caller's log.
pub struct Turn {
    client: Client,
    child: Child,
    log: PathBuf,
    start: u64,
    pos: u64,
    ended: bool,
    stopping: bool,
    timed_out: bool,
    started: Instant,
    started_ms: i64,
    exit: Option<i32>,
    home: Option<PathBuf>,
    /// The resumed session; a resume's output need not name it again.
    resumed: Option<String>,
}

fn unix_ms() -> i64 { SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as i64) }

impl Turn {
    /// Spawn `command(base, spec)` with `text` on stdin; output appends to `log`, so a resume continues earlier turns.
    pub fn start(base: Command, spec: &Spec, log: &Path, text: &str) -> Res<Turn> {
        if let Some(dir) = log.parent() { std::fs::create_dir_all(dir).map_err(|e| format!("agent log directory: {e}"))?; }
        let out = File::options().create(true).append(true).open(log).map_err(|e| format!("agent log: {e}"))?;
        let err = out.try_clone().map_err(|e| format!("agent log: {e}"))?;
        let start = out.metadata().map_err(|e| format!("agent log: {e}"))?.len();
        let mut c = command(base, spec);
        crate::telemetry::capture::inherit_context(&mut c);
        let started = Instant::now();
        let started_ms = unix_ms();
        let mut child = c.stdin(Stdio::piped()).stdout(out).stderr(err).spawn().map_err(|e| format!("{} spawn: {e}", spec.client.code()))?;
        let mut stdin = child.stdin.take().ok_or("agent stdin unavailable")?;
        let input = text.as_bytes().to_vec();
        // A client that never reads stdin must not block the caller; the writer ends with the pipe.
        std::thread::spawn(move || { let _ = stdin.write_all(&input); });
        Ok(Turn { client: spec.client, child, log: log.to_path_buf(), start, pos: start, ended: false, stopping: false, timed_out: false,
            started, started_ms, exit: None, home: spec.home.clone(), resumed: spec.resume.then(|| spec.session.clone()) })
    }

    pub fn id(&self) -> u32 { self.child.id() }

    /// Read complete new log lines; a turn-end event ends the turn even when the process lingers.
    fn read(&mut self) -> Res<()> {
        let mut f = File::open(&self.log).map_err(|e| format!("agent log unreadable: {e}"))?;
        if f.metadata().map_err(|e| e.to_string())?.len() < self.pos { return Err("agent log truncated; turn boundary unknown".into()); }
        let mut rest = vec![];
        f.seek(SeekFrom::Start(self.pos)).and_then(|_| f.read_to_end(&mut rest)).map_err(|e| format!("agent log read: {e}"))?;
        let Some(end) = rest.iter().rposition(|b| *b == b'\n') else { return Ok(()) };
        self.pos += end as u64 + 1;
        self.ended |= String::from_utf8_lossy(&rest[..end]).lines().filter_map(|l| serde_json::from_str::<Value>(l).ok()).any(|v| ends(&v));
        Ok(())
    }

    /// The exit code once the process exited, else None. After a turn-end event a lingering process is stopped
    /// (Codex on Windows kept running after its turn ended).
    pub fn poll(&mut self) -> Res<Option<i32>> {
        if self.exit.is_some() { return Ok(self.exit); }
        let read = self.read();
        if let Some(status) = self.child.try_wait().map_err(|e| format!("agent wait: {e}"))? {
            self.exit = Some(status.code().unwrap_or(-1));
            let _ = self.read();
            return Ok(self.exit);
        }
        read?;
        if self.ended && !self.stopping { self.stopping = true; let _ = self.child.kill(); }
        Ok(None)
    }

    /// Request termination; `poll` confirms the exit.
    pub fn kill(&mut self) -> Res<()> { self.stopping = true; self.child.kill().map_err(|e| format!("agent kill: {e}")) }

    /// Poll until exit; at `deadline` stop the process, and the turn reads as timed out.
    pub fn wait(&mut self, deadline: Option<Instant>) -> Res<i32> {
        loop {
            if let Some(code) = self.poll()? { return Ok(code); }
            if deadline.is_some_and(|d| Instant::now() >= d) && !self.timed_out {
                self.timed_out = true;
                let _ = self.kill();
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// The turn's report from its log bytes; call after `poll` returned an exit.
    pub fn report(&self) -> Report {
        let text = read_from(&self.log, self.start).unwrap_or_default();
        let events: Vec<Value> = text.lines().filter_map(|l| serde_json::from_str(l).ok()).collect();
        let session = session_of(self.client, &text).or_else(|| self.resumed.clone());
        let rollout = match self.client { Client::Codex => session.as_deref().and_then(|t| rollout(t, self.home.as_deref())), Client::Claude => None };
        let mut r = Report::read(self.client, &text, &events, rollout.as_deref());
        r.session = session;
        r.duration_us = self.started.elapsed().as_micros() as u64;
        r.started_ms = self.started_ms;
        r.exit = self.exit;
        if self.timed_out { (r.outcome, r.error) = ("cancelled", "timeout"); }
        else if r.outcome == "unknown" && self.exit.is_some_and(|c| c != 0) && r.error == "no-end" { (r.outcome, r.error) = ("failed", "exit"); }
        r
    }
}

fn read_from(log: &Path, pos: u64) -> Option<String> {
    let mut f = File::open(log).ok()?;
    let mut b = vec![];
    f.seek(SeekFrom::Start(pos)).and_then(|_| f.read_to_end(&mut b)).ok()?;
    Some(String::from_utf8_lossy(&b).into_owned())
}

/// A turn-end event: Claude `result`, Codex `turn.completed` or `turn.failed`.
fn ends(v: &Value) -> bool { matches!(v["type"].as_str(), Some("result" | "turn.completed" | "turn.failed")) }

/// The observation context the caller owns; every value is a code (R27).
#[derive(Clone, Debug)]
pub struct Context<'a> {
    pub product: &'a str,
    pub version: &'a str,
    /// `owner/name` of the project, or `other`.
    pub project: &'a str,
    /// Task and attempt of a task turn.
    pub task: Option<(u64, u64)>,
    /// Route code of a turn outside a task (a manager conversation).
    pub route: Option<&'a str>,
    pub account: &'a str,
}

/// Start, wait and report one turn, then append its `agent.turn` observation to `store`. A failed append is kept in
/// `Report::recorded` and never changes the turn's result (R30).
pub fn run(base: Command, spec: &Spec, log: &Path, text: &str, deadline: Option<Instant>, ctx: &Context, store: &crate::telemetry::Store) -> Res<Report> {
    let mut turn = Turn::start(base, spec, log, text)?;
    turn.wait(deadline)?;
    let mut r = turn.report();
    r.recorded = crate::telemetry::Event::new(r.observation(spec.client, ctx)).and_then(|e| store.append(&[e])).map(|_| ());
    Ok(r)
}

// ---------- reading client output ----------

/// Token use of one turn; a count the client did not report is None.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Usage {
    /// Claude assistant messages; Codex reports no call count.
    pub calls: Option<u64>,
    pub input: Option<u64>,
    pub cache_read: Option<u64>,
    pub cache_write: Option<u64>,
    pub output: Option<u64>,
    pub model: Option<String>,
    /// Why no usage was read; None when some was.
    pub unknown: Option<&'static str>,
}

/// Model use of the turn's events: Claude assistant messages (each message id once: input, cache read, cache write,
/// output, the model of most calls), Codex `turn.completed` (`input_tokens` includes `cached_input_tokens`, no cache
/// writes). A counted call without a value makes that sum unknown, never smaller.
pub fn usage(events: &[Value]) -> Usage {
    let (mut ids, mut turns, mut models) = (std::collections::HashSet::new(), false, BTreeMap::<String, u64>::new());
    let mut sums = [Some(0u64); 4];
    for v in events {
        let codex = v["type"] == "turn.completed";
        let counted = match v["type"].as_str() {
            Some("assistant") => v["message"]["id"].as_str().is_some_and(|id| ids.insert(id.to_string())),
            Some("turn.completed") => { turns = true; true }
            _ => false,
        };
        if !counted { continue }
        let u = if codex { &v["usage"] } else { &v["message"]["usage"] };
        let input = u["input_tokens"].as_u64();
        let read = u[if codex { "cached_input_tokens" } else { "cache_read_input_tokens" }].as_u64();
        let call = [if codex { input.zip(read).and_then(|(i, r)| i.checked_sub(r)) } else { input }, read,
            if codex { Some(0) } else { u["cache_creation_input_tokens"].as_u64() }, u["output_tokens"].as_u64()];
        for (s, c) in sums.iter_mut().zip(call) { *s = s.zip(c).map(|(a, b)| a + b) }
        if let Some(m) = v["message"]["model"].as_str().filter(|m| model_code(m)) { *models.entry(m.into()).or_default() += 1; }
    }
    let [input, cache_read, cache_write, output] = sums;
    let model = models.into_iter().max_by(|a, b| a.1.cmp(&b.1).then(b.0.cmp(&a.0))).map(|m| m.0);
    let calls = (!ids.is_empty()).then_some(ids.len() as u64);
    if calls.is_some() || turns { Usage { calls, input, cache_read, cache_write, output, model, unknown: None } }
    else { Usage { unknown: Some("no-usage"), ..Usage::default() } }
}

/// A model id is a short code; anything else stays unreported.
fn model_code(m: &str) -> bool { !m.is_empty() && m.len() <= 64 && m.bytes().all(|c| c.is_ascii_alphanumeric() || b"._:-".contains(&c)) }

/// A usage or quota limit reported before any work started: `Some(reset)` with the reset (unix seconds) when known;
/// None after work started or without a limit.
pub fn limit_reached(events: &[Value]) -> Option<Option<i64>> {
    for v in events {
        let kind = v["type"].as_str().unwrap_or_default();
        if kind == "assistant" {
            if v["apiError"] == "usage_limit_reached" || v["quotaLimits"]["status"] == "rejected" {
                return Some(v["quotaLimits"]["resetsAt"].as_i64().or(v["apiErrorParams"]["rate_limit_info"]["resetsAt"].as_i64()));
            }
            return None;
        }
        if kind.starts_with("item.") { return None; }
        // ponytail: Codex limit text matched by words; it reports no reset. A structured field replaces this when one exists.
        let message = v["message"].as_str().or(v["error"]["message"].as_str()).unwrap_or_default().to_lowercase();
        if matches!(kind, "error" | "turn.failed") && message.contains("usage limit") { return Some(None); }
    }
    None
}

/// A Codex login the auth server rejected: Codex's own stderr `ERROR codex_` line or a top-level `error`/`turn.failed`
/// event naming a rejected refresh token or a 401; worker output (`item.*`) never matches.
pub fn auth_rejected(text: &str) -> Option<&'static str> {
    text.lines().find_map(|l| {
        let m = match serde_json::from_str::<Value>(l) {
            Ok(v) if matches!(v["type"].as_str(), Some("error" | "turn.failed")) => v["message"].as_str().or(v["error"]["message"].as_str()).unwrap_or_default().to_lowercase(),
            Err(_) if l.contains("ERROR codex_") => l.to_lowercase(),
            _ => return None,
        };
        if m.contains("refresh token") && (m.contains("already used") || m.contains("expired") || m.contains("revoked") || m.contains("invalid")) || m.contains("refresh_token_") {
            Some("refresh token rejected")
        } else if m.contains("401 unauthorized") || m.contains("auth error: 401") { Some("401 Unauthorized") } else { None }
    })
}

/// The last turn end is a Codex `turn.failed` naming model capacity: its message.
pub fn transient_failure(events: &[Value]) -> Option<String> {
    let end = events.iter().filter(|v| matches!(v["type"].as_str(), Some("turn.completed" | "turn.failed"))).last()?;
    // ponytail: only the "Selected model is at capacity" text is verified; other overload texts read as failed.
    let m = end["error"]["message"].as_str().filter(|_| end["type"] == "turn.failed")?;
    m.to_lowercase().contains("model is at capacity").then(|| m.to_string())
}

/// The final text of a turn: Claude's last `result`, else Codex's last `agent_message`.
pub fn final_text(events: &[Value]) -> Option<String> {
    events.iter().rev().find_map(|v| {
        if v["type"] == "result" { v["result"].as_str().map(String::from) }
        else if v["type"] == "item.completed" && v["item"]["type"] == "agent_message" { v["item"]["text"].as_str().map(String::from) }
        else { None }
    })
}

/// Codex's thread ID: the first `thread.started` line.
pub fn codex_thread(text: &str) -> Option<String> {
    text.lines().find_map(|l| {
        let v: Value = serde_json::from_str(l).ok()?;
        (v["type"] == "thread.started").then(|| v["thread_id"].as_str().map(String::from)).flatten()
    })
}

/// The native session of a turn: Claude's `session_id` (init or result), Codex's thread.
pub fn session_of(client: Client, text: &str) -> Option<String> {
    match client {
        Client::Codex => codex_thread(text),
        Client::Claude => text.lines().filter_map(|l| serde_json::from_str::<Value>(l).ok())
            .find_map(|v| (matches!(v["type"].as_str(), Some("system" | "result"))).then(|| v["session_id"].as_str().map(String::from)).flatten()),
    }
}

/// The thread's rollout `<home>/sessions/**/rollout-*-<thread>.jsonl`, in `home` or the client's own home.
pub fn rollout(thread: &str, home: Option<&Path>) -> Option<PathBuf> {
    fn find(dir: &Path, end: &str, depth: u8) -> Option<PathBuf> {
        if depth == 0 { return None; }
        std::fs::read_dir(dir).ok()?.flatten().find_map(|e| {
            let p = e.path();
            if p.is_dir() { find(&p, end, depth - 1) } else { p.file_name()?.to_str()?.ends_with(end).then_some(p) }
        })
    }
    if thread.is_empty() || !thread.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') { return None; }
    let home = home.map(Path::to_path_buf).or_else(|| base_home(Client::Codex))?;
    // sessions/<yyyy>/<mm>/<dd>/<file>
    find(&home.join("sessions"), &format!("{thread}.jsonl"), 4)
}

/// A subscription limit snapshot; each None has `unknown`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Snapshot {
    /// Used share of the binding window, whole percent.
    pub used_percent: Option<u64>,
    /// When the window resets (unix seconds).
    pub resets_at: Option<u64>,
    /// The window code: Claude `rateLimitType` (`five_hour`, `seven_day`), Codex `<window_minutes>m`.
    pub window: Option<String>,
    pub unknown: Option<&'static str>,
}

fn percent(v: f64) -> Option<u64> { (v.is_finite() && v >= 0.0).then(|| v.round() as u64) }

/// Claude: the last `rate_limit_event` (`rate_limit_info.utilization` 0-1, `resetsAt`, `rateLimitType`). Codex: the
/// last `rate_limits` (`primary`/`secondary`: `used_percent` 0-100, `resets_at`, `window_minutes`) in the turn's events,
/// else the last `token_count` of the thread rollout; the window with the higher use binds.
pub fn snapshot(client: Client, events: &[Value], rollout: Option<&Path>) -> Snapshot {
    let found = match client {
        Client::Claude => events.iter().rev().find(|v| v["type"] == "rate_limit_event").map(|v| {
            let i = &v["rate_limit_info"];
            Snapshot { used_percent: i["utilization"].as_f64().and_then(|u| percent(u * 100.0)), resets_at: i["resetsAt"].as_u64(),
                window: i["rateLimitType"].as_str().filter(|w| model_code(w)).map(String::from), unknown: None }
        }),
        Client::Codex => {
            let own = events.iter().rev().find_map(|v| codex_limits(v));
            own.or_else(|| {
                let f = File::open(rollout?).ok()?;
                BufReader::new(f).lines().map_while(Result::ok).filter(|l| l.contains("\"rate_limits\""))
                    .filter_map(|l| serde_json::from_str::<Value>(&l).ok()).filter_map(|v| codex_limits(&v)).last()
            })
        }
    };
    match found {
        Some(mut s) => {
            if s.used_percent.is_none() || s.resets_at.is_none() { s.unknown = Some("partial-snapshot"); }
            s
        }
        None => Snapshot { unknown: Some(if client == Client::Codex && rollout.is_none() { "no-rollout" } else { "not-reported" }), ..Snapshot::default() },
    }
}

fn codex_limits(v: &Value) -> Option<Snapshot> {
    let r = [&v["rate_limits"], &v["payload"]["rate_limits"], &v["msg"]["rate_limits"]].into_iter().find(|r| r.is_object())?;
    let (used, w) = [&r["primary"], &r["secondary"]].into_iter().filter_map(|w| w["used_percent"].as_f64().map(|u| (u, w)))
        .fold(None::<(f64, &Value)>, |best, c| if best.is_none_or(|b| c.0 > b.0) { Some(c) } else { best })?;
    let resets = w["resets_at"].as_u64().or_else(|| w["resets_in_seconds"].as_u64().map(|s| (unix_ms() / 1000) as u64 + s));
    Some(Snapshot { used_percent: percent(used), resets_at: resets, window: w["window_minutes"].as_u64().map(|m| format!("{m}m")), unknown: None })
}

/// What one turn did, read from its log.
#[derive(Clone, Debug)]
pub struct Report {
    pub session: Option<String>,
    pub usage: Usage,
    pub limit: Snapshot,
    /// A limit reached before work started, with its reset when known: the caller records it for the account switch.
    pub reached: Option<Option<i64>>,
    /// Telemetry outcome code and error code.
    pub outcome: &'static str,
    pub error: &'static str,
    pub text: Option<String>,
    pub exit: Option<i32>,
    pub started_ms: i64,
    pub duration_us: u64,
    /// The observation append; Ok(()) before `run` appends.
    pub recorded: Res<()>,
}

impl Report {
    /// Classify the turn's log text.
    pub fn read(client: Client, text: &str, events: &[Value], rollout: Option<&Path>) -> Report {
        let reached = limit_reached(events);
        let end = events.iter().rev().find(|v| ends(v));
        let (outcome, error) = if reached.is_some() { ("refused", "usage-limit") }
            else if auth_rejected(text).is_some() { ("failed", "auth-rejected") }
            else if transient_failure(events).is_some() { ("failed", "model-capacity") }
            else {
                match end {
                    Some(v) if v["type"] == "turn.failed" => ("failed", "turn-failed"),
                    Some(v) if v["type"] == "result" && v["is_error"] == true => ("failed", "client-error"),
                    Some(_) => ("ok", "none"),
                    None => ("unknown", "no-end"),
                }
            };
        Report { session: session_of(client, text), usage: usage(events), limit: snapshot(client, events, rollout), reached, outcome, error,
            text: final_text(events), exit: None, started_ms: unix_ms(), duration_us: 0, recorded: Ok(()) }
    }

    /// The `agent.turn` observation: codes and numbers only; every null names its reason.
    pub fn observation(&self, client: Client, ctx: &Context) -> Value {
        let mut unknown = serde_json::Map::new();
        let mut num = |name: &str, v: Option<u64>, why: &str| -> Value {
            if v.is_none() { unknown.insert(name.into(), json!(why)); }
            json!(v)
        };
        let why = self.usage.unknown.unwrap_or("not-reported");
        let (task, attempt) = ctx.task.map_or((None, None), |(t, a)| (Some(t), Some(a)));
        let limit_why = self.limit.unknown.unwrap_or("not-reported");
        let mut v = json!({
            "product": ctx.product, "version": ctx.version, "operation": "agent.turn", "kind": "agent.turn",
            "trace": null, "span": null, "started_ms": self.started_ms.max(0), "duration_us": self.duration_us,
            "outcome": self.outcome, "error": self.error, "project": ctx.project, "runtime": client.code(), "account": ctx.account,
            "task": num("task", task, "no-task"), "attempt": num("attempt", attempt, "no-task"),
            "input_tokens": num("input_tokens", self.usage.input, why), "cache_read_tokens": num("cache_read_tokens", self.usage.cache_read, why),
            "cache_write_tokens": num("cache_write_tokens", self.usage.cache_write, why), "output_tokens": num("output_tokens", self.usage.output, why),
            "limit_used_percent": num("limit_used_percent", self.limit.used_percent, limit_why),
            "limit_resets_at": num("limit_resets_at", self.limit.resets_at, limit_why),
        });
        unknown.insert("trace".into(), json!("not-traced"));
        unknown.insert("span".into(), json!("not-traced"));
        let text = |v: &mut Value, unknown: &mut serde_json::Map<String, Value>, k: &str, s: Option<&str>, why: &str| {
            v[k] = json!(s);
            if s.is_none() { unknown.insert(k.into(), json!(why)); }
        };
        text(&mut v, &mut unknown, "model", self.usage.model.as_deref(), if self.usage.unknown.is_some() { why } else { "not-reported" });
        text(&mut v, &mut unknown, "route", ctx.route, "task-turn");
        text(&mut v, &mut unknown, "limit_window", self.limit.window.as_deref(), limit_why);
        v["unknown"] = Value::Object(unknown);
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn events(text: &str) -> Vec<Value> { text.lines().filter_map(|l| serde_json::from_str(l).ok()).collect() }

    // Chain: coreq.md R51 (TaskQ § 3 Runtime accounts, unchanged): order and disable pick the start account; a limit
    // moves to the next enabled account; with none left the hold ends at the earliest limit end.
    // Risk: a disabled or limited account takes new turns, or a spare account is ignored.
    #[test]
    fn account_order_disable_failover_and_hold() {
        let none = Accounts::default();
        assert_eq!((none.pick(0).as_deref(), none.spare(0)), (Some(DEFAULT), None), "no list: one default account, no failover");
        let list = |v: &[(&str, bool)]| v.iter().map(|(l, e)| Entry { label: l.to_string(), enabled: *e }).collect::<Vec<_>>();
        let a = Accounts { list: list(&[("default", false), ("work", true)]), limits: BTreeMap::new() };
        assert_eq!(a.pick(0).as_deref(), Some("work"), "a disabled account takes no start");
        let mut a = Accounts { list: list(&[("default", true), ("work", true), ("spare", false)]), limits: BTreeMap::new() };
        a.limits.insert("default".into(), Limit { at: 100, until: None });
        assert_eq!(a.spare(200).as_deref(), Some("work"), "a limit moves to the next enabled account");
        assert_eq!(a.hold(200), Some((100 + PROBE, false)));
        assert_eq!(a.spare(100 + PROBE).as_deref(), Some("default"), "the probe ends the limit and order returns");
        a.limits.insert("work".into(), Limit { at: 150, until: Some(9000) });
        assert_eq!((a.spare(200), a.pick(200).as_deref()), (None, Some("default")), "all limited: no spare, a start still names one");
        assert_eq!(a.hold(200), Some((100 + PROBE, false)), "the hold ends at the earliest enabled limit end");
        let off = Accounts { list: list(&[("default", false)]), limits: BTreeMap::new() };
        assert!(off.off() && off.pick(0).is_none(), "all disabled admits nothing");
        assert!(valid_label("work-2") && !valid_label("-x") && !valid_label("Work") && !valid_label(""));
        assert_eq!(account_home(Client::Codex, "taskq", DEFAULT), None, "default keeps the caller's home");
        assert!(account_home(Client::Claude, "taskq", "work").is_none_or(|h| h.ends_with(Path::new("taskq-accounts").join("work"))));
    }

    // Chain: coreq.md R48 and docs/design/agent.md § Arguments: TaskQ's argument order for start and resume.
    // Risk: a client format fix in one consumer diverges, or a resume starts a second session.
    #[test]
    fn start_and_resume_arguments_keep_taskq_order() {
        let s = |v: &[OsString]| v.iter().map(|a| a.to_string_lossy().into_owned()).collect::<Vec<_>>().join(" ");
        let extra = vec![OsString::from("--permission-mode"), OsString::from("auto")];
        let mut spec = Spec { client: Client::Claude, root: PathBuf::from("proj"), session: "id1".into(), resume: false, args: extra, home: None, env: vec![] };
        assert_eq!(s(&args(&spec)), "-p --output-format stream-json --verbose --permission-mode auto --session-id id1");
        spec.resume = true;
        assert_eq!(s(&args(&spec)), "-p --output-format stream-json --verbose --permission-mode auto --resume id1");
        spec.client = Client::Codex;
        spec.args = vec!["--sandbox".into(), "workspace-write".into()];
        assert_eq!(s(&args(&spec)), "exec --json --sandbox workspace-write resume id1 -");
        spec.resume = false;
        assert_eq!(s(&args(&spec)), "exec --json --sandbox workspace-write -C proj -");
        let settings: Value = serde_json::from_str(&claude_settings(Some("rules compact"), &[("TASKQ_TASK", "7")])).unwrap();
        assert_eq!(settings["hooks"]["SessionStart"][0]["matcher"], "compact");
        assert_eq!(settings["hooks"]["SessionStart"][0]["hooks"][0]["command"], "rules compact");
        assert_eq!(settings["env"]["TASKQ_TASK"], "7");
        assert!(serde_json::from_str::<Value>(&claude_settings(None, &[])).unwrap().get("hooks").is_none());
        assert_eq!(new_session(Client::Claude).len(), 36);
        assert_eq!(new_session(Client::Codex), "-");
    }

    // Chain: coreq.md R49 and TaskQ § 4 Profiling usage (unchanged): Claude counts each message id once; Codex input
    // includes cached input; a missing count makes the sum unknown. Risk: double-counted or invented zero tokens.
    #[test]
    fn usage_counts_each_message_once_and_unknown_stays_null() {
        let claude = events(concat!(
            r#"{"type":"assistant","message":{"id":"m1","model":"claude-opus-5-5","usage":{"input_tokens":2,"cache_read_input_tokens":30,"cache_creation_input_tokens":5,"output_tokens":7}}}"#, "\n",
            r#"{"type":"assistant","message":{"id":"m1","model":"claude-opus-5-5","usage":{"input_tokens":2,"cache_read_input_tokens":30,"cache_creation_input_tokens":5,"output_tokens":7}}}"#, "\n",
            r#"{"type":"assistant","message":{"id":"m2","model":"claude-opus-5-5","usage":{"input_tokens":1,"cache_read_input_tokens":40,"cache_creation_input_tokens":0,"output_tokens":3}}}"#, "\n"));
        let u = usage(&claude);
        assert_eq!((u.calls, u.input, u.cache_read, u.cache_write, u.output, u.model.as_deref()), (Some(2), Some(3), Some(70), Some(5), Some(10), Some("claude-opus-5-5")));
        let codex = events(r#"{"type":"turn.completed","usage":{"input_tokens":100,"cached_input_tokens":80,"output_tokens":9}}"#);
        let u = usage(&codex);
        assert_eq!((u.calls, u.input, u.cache_read, u.cache_write, u.output, u.model), (None, Some(20), Some(80), Some(0), Some(9), None));
        let partial = events(r#"{"type":"turn.completed","usage":{"input_tokens":100,"output_tokens":9}}"#);
        assert_eq!((usage(&partial).input, usage(&partial).cache_read), (None, None), "a missing count is unknown, never zero");
        assert_eq!(usage(&[]).unknown, Some("no-usage"));
    }

    // Chain: coreq.md R50, docs/design/agent.md § Limit snapshot: shapes copied from a Claude 2026-10 `rate_limit_event`
    // and a Codex 2026-11 rollout `token_count`. Risk: a limit reads as 0 % or a reset is invented.
    #[test]
    fn limit_snapshot_fields_per_client_or_reason() {
        let claude = events(r#"{"type":"rate_limit_event","rate_limit_info":{"status":"allowed_warning","resetsAt":1792386000,"rateLimitType":"seven_day","utilization":0.56,"unifiedWindows":{"five_hour":{"utilization":0.11,"resetsAt":1791964200}}}}"#);
        assert_eq!(snapshot(Client::Claude, &claude, None), Snapshot { used_percent: Some(56), resets_at: Some(1792386000), window: Some("seven_day".into()), unknown: None });
        assert_eq!(snapshot(Client::Claude, &[], None).unknown, Some("not-reported"));
        let dir = crate::scratch("agent-rollout");
        let path = dir.join("rollout-2026-11-01T01-00-14-t1.jsonl");
        std::fs::write(&path, concat!(
            r#"{"type":"event_msg","payload":{"type":"token_count","rate_limits":{"primary":{"used_percent":40.0,"window_minutes":10080,"resets_at":1793774000},"secondary":null}}}"#, "\n",
            r#"{"type":"event_msg","payload":{"type":"token_count","rate_limits":{"primary":{"used_percent":51.0,"window_minutes":10080,"resets_at":1793774205},"secondary":{"used_percent":12.0,"window_minutes":300,"resets_at":1793700000}}}}"#, "\n")).unwrap();
        assert_eq!(snapshot(Client::Codex, &[], Some(&path)), Snapshot { used_percent: Some(51), resets_at: Some(1793774205), window: Some("10080m".into()), unknown: None });
        assert_eq!(snapshot(Client::Codex, &[], None).unknown, Some("no-rollout"));
        let partial = events(r#"{"type":"rate_limit_event","rate_limit_info":{"status":"allowed","rateLimitType":"five_hour"}}"#);
        assert_eq!(snapshot(Client::Claude, &partial, None).unknown, Some("partial-snapshot"));
    }

    // Chain: TaskQ usage limit, rejected login and transient failure classification (unchanged), now in Report::read.
    // Risk: a limit or a rejected login reads as a passing turn, so the account switch never happens.
    #[test]
    fn limits_logins_and_capacity_classify_the_turn() {
        let limited = r#"{"type":"assistant","apiError":"usage_limit_reached","quotaLimits":{"status":"rejected","resetsAt":1700000000}}"#;
        let r = Report::read(Client::Claude, limited, &events(limited), None);
        assert_eq!((r.reached, r.outcome, r.error), (Some(Some(1700000000)), "refused", "usage-limit"));
        let worked = "{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"usage limit\"}}\n{\"type\":\"turn.failed\",\"error\":{\"message\":\"You've hit your usage limit\"}}";
        assert_eq!(limit_reached(&events(worked)), None, "after work started a limit is not a start refusal");
        let codex = r#"{"type":"turn.failed","error":{"message":"You've hit your usage limit."}}"#;
        assert_eq!(limit_reached(&events(codex)), Some(None));
        assert_eq!(auth_rejected("2026 ERROR codex_core::auth: refresh token was already used"), Some("refresh token rejected"));
        assert_eq!(auth_rejected(r#"{"type":"item.completed","item":{"text":"401 Unauthorized"}}"#), None, "worker output never matches");
        let busy = r#"{"type":"turn.failed","error":{"message":"Selected model is at capacity. Please try a different model."}}"#;
        let r = Report::read(Client::Codex, busy, &events(busy), None);
        assert_eq!((r.outcome, r.error), ("failed", "model-capacity"));
        let done = "{\"type\":\"thread.started\",\"thread_id\":\"t-1\"}\n{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"hi\"}}\n{\"type\":\"turn.completed\",\"usage\":{}}";
        let r = Report::read(Client::Codex, done, &events(done), None);
        assert_eq!((r.outcome, r.session.as_deref(), r.text.as_deref()), ("ok", Some("t-1"), Some("hi")));
    }

    // Chain: coreq.md R49 and R27: the observation holds codes and numbers, every null names its reason, and the
    // store accepts it. Risk: free text or an unexplained null enters telemetry, or the store rejects every turn.
    #[test]
    fn observation_is_valid_metadata() {
        let text = r#"{"type":"turn.completed","usage":{"input_tokens":10,"cached_input_tokens":4,"output_tokens":2}}"#;
        let r = Report::read(Client::Codex, text, &events(text), None);
        let ctx = Context { product: "taskq", version: "4.6.57", project: "taskqorg/coreq", task: Some((107, 1)), route: None, account: "work" };
        let v = r.observation(Client::Codex, &ctx);
        assert_eq!((v["input_tokens"].as_u64(), v["cache_read_tokens"].as_u64(), v["output_tokens"].as_u64()), (Some(6), Some(4), Some(2)));
        assert_eq!((v["unknown"]["model"].as_str(), v["unknown"]["route"].as_str(), v["unknown"]["limit_used_percent"].as_str()), (Some("not-reported"), Some("task-turn"), Some("no-rollout")));
        crate::telemetry::Event::new(v).expect("valid task observation");
        let route = Context { route: Some("pilotq.manager"), task: None, ..ctx };
        let v = r.observation(Client::Codex, &route);
        assert_eq!((v["route"].as_str(), v["unknown"]["task"].as_str()), (Some("pilotq.manager"), Some("no-task")));
        crate::telemetry::Event::new(v).expect("valid route observation");
    }
}
