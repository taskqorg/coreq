//! Shared core of taskqorg tools (coreq.md).

pub mod enroll;
pub mod file_lock;
pub mod incident;
pub mod launcher;
#[cfg(unix)]
pub mod local_socket;
pub mod profile;
pub mod process;
pub mod service;
pub mod status;
pub mod telemetry;
pub mod update;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

/// One consumer: every name, folder and repository comes from here, never from a constant (coreq.md § Tool).
#[derive(Clone, Debug)]
pub struct Tool {
    pub name: &'static str,
    /// Windows Scheduled Task prefix.
    pub title: &'static str,
    /// GitHub `owner/name` of releases; built into the binary, never from project config.
    pub repo: &'static str,
    pub version: &'static str,
    pub install: PathBuf,
    /// Receives the faults the launcher detects (coreq.md § Launcher); the consumer reports them.
    pub fault: Option<fn(&Tool, &incident::Fault)>,
}

impl Tool {
    pub fn new(name: &'static str, title: &'static str, repo: &'static str, version: &'static str) -> Tool {
        let mut t = Tool { name, title, repo, version, install: PathBuf::new(), fault: None };
        t.install = match std::env::var_os(t.env("INSTALL")) {
            Some(p) => p.into(),
            None if cfg!(windows) => PathBuf::from(std::env::var_os("LOCALAPPDATA").unwrap_or_default()).join(name),
            None => PathBuf::from(std::env::var_os("HOME").unwrap_or_default()).join(".local/share").join(name),
        };
        t
    }

    /// `<NAME>_<key>`.
    pub fn env(&self, key: &str) -> String {
        format!("{}_{key}", self.name.to_uppercase())
    }

    pub fn exe_name(&self) -> String {
        format!("{}{}", self.name, std::env::consts::EXE_SUFFIX)
    }

    pub fn release_exe(&self, v: &str) -> PathBuf {
        self.install.join("releases").join(v).join(self.exe_name())
    }

    pub fn current(&self) -> Option<String> {
        read(&self.install.join("current"))
    }

    pub fn die(&self, msg: &str) -> ! {
        eprintln!("{}: {msg}", self.name);
        crate::telemetry::capture::exit(1);
    }

    /// Env `<NAME>_TOKEN`, else `GITHUB_TOKEN`/`GITLAB_TOKEN`, else `<install>/token`, else the `gh`/`glab` login for
    /// the board host (coreq.md § Board token).
    /// Measured as `token.read` (coreq.md § Profiling).
    pub fn token(&self, gitlab: bool, host: &str) -> Result<String, String> {
        let start = std::time::Instant::now();
        let tok = self.token_once(gitlab, host);
        profile::record("token.read", start.elapsed(), if tok.is_ok() { "ok" } else { "failed" }, tok.as_ref().err().map(|e| if e.starts_with("no board token:") { "missing" } else { "unknown" }));
        tok
    }

    fn token_once(&self, gitlab: bool, host: &str) -> Result<String, String> {
        let named = if gitlab { "GITLAB_TOKEN" } else { "GITHUB_TOKEN" };
        for k in [self.env("TOKEN").as_str(), named] {
            if let Ok(t) = std::env::var(k)
                && !t.trim().is_empty()
            {
                return Ok(t.trim().to_string());
            }
        }
        if let Some(token) = read(&self.install.join("token")) { return Ok(token); }
        // Only the board host's login: a default-host token never goes to another host.
        let (bin, args): (&str, &[&str]) = if gitlab { ("glab", &["config", "get", "token", "--host"]) } else { ("gh", &["auth", "token", "--hostname"]) };
        let o = process::output("token.lookup", Command::new(bin).args(args).arg(host), std::time::Duration::from_secs(10))
            .map_err(|e| format!("board token lookup unavailable: {e}"))?;
        let token = String::from_utf8(o.stdout).map_err(|_| "board token lookup returned invalid UTF-8")?.trim().to_string();
        if token.is_empty() { Err(format!("no board token: set {} or write {}", self.env("TOKEN"), self.install.join("token").display())) }
        else { Ok(token) }
    }
}

pub(crate) fn read(p: &Path) -> Option<String> {
    fs::read_to_string(p).ok().map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    ring::digest::digest(&ring::digest::SHA256, bytes).as_ref().iter().map(|b| format!("{b:02x}")).collect()
}

/// Atomic small-file write: temp file, then rename.
pub fn write_atomic(p: &Path, text: &str) -> Result<(), String> {
    let tmp = p.with_extension("tmp");
    fs::write(&tmp, text).map_err(|e| format!("{}: {e}", tmp.display()))?;
    fs::rename(&tmp, p).map_err(|e| format!("{}: {e}", p.display()))
}

/// Persist an acknowledged replacement. Caller serializes this target and its reserved `.pending` file.
pub fn write_durable(p: &Path, text: &str) -> Result<(), String> {
    use std::io::Write;
    let mut name=p.file_name().ok_or("durable file has no name")?.to_os_string();
    name.push(".pending");
    let tmp=p.with_file_name(name);
    let mut file=fs::OpenOptions::new().write(true).create_new(true).open(&tmp)
        .map_err(|e|format!("durable file prepare: {e}"))?;
    let prepared=file.write_all(text.as_bytes()).and_then(|_|file.sync_all());
    drop(file);
    let result=prepared.and_then(|_| {
        #[cfg(windows)]
        {
            use std::os::windows::ffi::OsStrExt;
            #[link(name="kernel32")]
            unsafe extern "system" { fn MoveFileExW(from:*const u16,to:*const u16,flags:u32)->i32; }
            let from:Vec<u16>=tmp.as_os_str().encode_wide().chain(Some(0)).collect();
            let to:Vec<u16>=p.as_os_str().encode_wide().chain(Some(0)).collect();
            // Both paths are terminated and live; replace existing and wait for the move's disk flush.
            if unsafe { MoveFileExW(from.as_ptr(),to.as_ptr(),0x1|0x8) }==0 { return Err(std::io::Error::last_os_error()); }
        }
        #[cfg(not(windows))]
        {
            fs::rename(&tmp,p)?;
            let parent=p.parent().filter(|p|!p.as_os_str().is_empty()).unwrap_or_else(||Path::new("."));
            fs::File::open(parent)?.sync_all()?;
        }
        Ok(())
    });
    // Never remove a temporary that we did not create, including after an interrupted earlier write.
    if result.is_err() { let _=fs::remove_file(&tmp); }
    result.map_err(|e|format!("durable file write uncertain: {e}"))
}

/// Windows: no console window for a child; a console-less parent would otherwise get one per child.
pub fn quiet(c: &mut Command) -> &mut Command {
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        c.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    }
    c
}

#[cfg(test)]
pub(crate) fn scratch(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("coreq-{tag}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&d);
    fs::create_dir_all(&d).unwrap();
    d
}

#[cfg(test)]
pub(crate) fn tool(install: &Path) -> Tool {
    Tool { name: "demoq", title: "DemoQ", repo: "example/demoq", version: "1.0.0", install: install.into(), fault: None }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Chain: host-scoped CLI credential lookup -> bounded execution -> private token or explicit unknown.
    #[cfg(unix)]
    #[test]
    fn credential_lookup_preserves_failure_and_private_output() {
        use std::os::unix::fs::PermissionsExt;
        let d = scratch("credential-lookup");
        for (mode, body) in [("ok", "printf private-token"), ("exit", "echo private-error >&2; exit 7"), ("empty", ":")] {
            let dir = d.join(mode); fs::create_dir_all(&dir).unwrap();
            let script = dir.join("gh");
            fs::write(&script, format!("#!/bin/sh\n[ \"$1 $2 $3 $4\" = 'auth token --hostname fixture.invalid' ] || exit 9\n{body}\n")).unwrap();
            fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).unwrap();
            let out = Command::new(std::env::current_exe().unwrap()).args(["--exact", "tests::credential_lookup_child", "--ignored", "--nocapture"])
                .env("COREQ_TOKEN_FIXTURE", &dir).env("PATH", &dir).env_remove("DEMOQ_TOKEN").env_remove("GITHUB_TOKEN")
                .output().unwrap();
            assert!(out.status.success(), "{mode}: {}", String::from_utf8_lossy(&out.stderr));
        }
        fs::remove_dir_all(d).unwrap();
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "isolated credential environment"]
    fn credential_lookup_child() {
        use tracing_subscriber::prelude::*;
        let dir = PathBuf::from(std::env::var_os("COREQ_TOKEN_FIXTURE").unwrap());
        let store = std::sync::Arc::new(telemetry::Store::open(&dir.join("db")).unwrap());
        let capture = telemetry::capture::Capture::new(store.clone(), "fixtureq", "1").unwrap();
        let result = tracing::subscriber::with_default(tracing_subscriber::registry().with(capture.clone()), || tool(&dir).token(false, "fixture.invalid"));
        let mode = dir.file_name().unwrap().to_str().unwrap();
        match mode {
            "ok" => assert_eq!(result.unwrap(), "private-token"),
            "exit" => assert!(result.unwrap_err().starts_with("board token lookup unavailable:")),
            "empty" => assert!(result.unwrap_err().starts_with("no board token:")),
            _ => unreachable!(),
        }
        capture.flush().unwrap();
        let rows = store.export(0, 0, 100).unwrap();
        let row = rows["events"].as_array().unwrap().iter().map(|r| &r["observation"])
            .find(|r| r["operation"] == "fixtureq.operation.token.lookup" && r["kind"] == "method").unwrap();
        assert_eq!(row["error"], if mode == "exit" { "exit" } else { "none" });
        assert!(!rows.to_string().contains("private-"));
    }

    #[test]
    fn tool_names_come_from_parameters() {
        let t = tool(Path::new("/i"));
        assert_eq!(t.env("TOKEN"), "DEMOQ_TOKEN");
        assert_eq!(t.release_exe("1.2.3"), Path::new("/i/releases/1.2.3").join(t.exe_name()));
        assert!(t.exe_name().starts_with("demoq"));
        let d = scratch("token");
        fs::write(d.join("token"), "  tok\n").unwrap();
        let t = Tool { name: "coreqtokentest", ..tool(&d) };
        if std::env::var_os("GITHUB_TOKEN").is_none() {
            assert_eq!(t.token(false, "github.com"), Ok("tok".into()), "install token file before the gh login");
        }
    }
}
