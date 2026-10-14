//! Shared core of taskqorg tools (coreq.md).

pub mod enroll;
pub mod incident;
pub mod launcher;
pub mod profile;
pub mod service;
pub mod status;
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
        std::process::exit(1);
    }

    /// Env `<NAME>_TOKEN`, else `GITHUB_TOKEN`/`GITLAB_TOKEN`, else `<install>/token`, else the `gh`/`glab` login for
    /// the board host (coreq.md § Board token).
    /// Measured as `token.read` (coreq.md § Profiling).
    pub fn token(&self, gitlab: bool, host: &str) -> Result<String, String> {
        let start = std::time::Instant::now();
        let tok = self.token_once(gitlab, host);
        profile::record("token.read", start.elapsed(), if tok.is_ok() { "ok" } else { "failed" }, tok.is_err().then_some("missing"));
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
        let cli = || {
            // only the board host's login: a default-host token never goes to another host
            let (bin, args): (&str, &[&str]) = if gitlab { ("glab", &["config", "get", "token", "--host"]) } else { ("gh", &["auth", "token", "--hostname"]) };
            let o = quiet(Command::new(bin).args(args).arg(host)).output().ok()?;
            o.status.success().then(|| String::from_utf8_lossy(&o.stdout).trim().to_string())
        };
        read(&self.install.join("token"))
            .or_else(cli)
            .filter(|t| !t.is_empty())
            .ok_or(format!("no board token: set {} or write {}", self.env("TOKEN"), self.install.join("token").display()))
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
