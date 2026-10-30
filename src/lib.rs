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

/// One consumer: every name, folder and repository comes from here, never from a constant (coreq.md § Tool identity).
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
        t.install = install_dir(name,title,&[]).unwrap_or_else(|error|t.die(&error));
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
        use std::io::Write;
        let _=writeln!(std::io::stderr(),"{}: {msg}", self.name);
        crate::telemetry::capture::exit(1);
    }

    /// Env `<NAME>_TOKEN`, else `GITHUB_TOKEN`/`GITLAB_TOKEN`, else `<install>/token`/`<install>/gitlab-token`, else the
    /// `gh`/`glab` login for the board host (coreq.md § Board token).
    /// Measured as `token.read` (coreq.md § Legacy profiling).
    pub fn token(&self, gitlab: bool, host: &str) -> Result<String, String> {
        profile::measure("token.read",||self.token_once(gitlab,host),|e|if e.starts_with("no board token:") {"missing"}else{"unknown"})
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
        // Per board kind: a GitHub token file never goes to a GitLab host.
        let file = self.install.join(if gitlab { "gitlab-token" } else { "token" });
        if let Some(token) = read(&file) { return Ok(token); }
        // Only the board host's login: a default-host token never goes to another host.
        let (bin, args): (&str, &[&str]) = if gitlab { ("glab", &["config", "get", "token", "--host"]) } else { ("gh", &["auth", "token", "--hostname"]) };
        let o = process::output("token.lookup", Command::new(bin).args(args).arg(host), std::time::Duration::from_secs(10))
            .map_err(|e| format!("board token lookup unavailable: {e}"))?;
        let token = String::from_utf8(o.stdout).map_err(|_| "board token lookup returned invalid UTF-8")?.trim().to_string();
        if token.is_empty() { Err(format!("no board token: set {} or write {}", self.env("TOKEN"), file.display())) }
        else { Ok(token) }
    }
}

/// Resolve once per product before capture; aliases name consumer-owned auxiliary entrypoints exactly.
pub fn install_dir(name:&'static str,title:&str,aliases:&[&str])->Result<PathBuf,String> {
    use std::{collections::HashMap,sync::{Mutex,OnceLock}};
    static ROOTS:OnceLock<Mutex<HashMap<&'static str,Result<PathBuf,String>>>>=OnceLock::new();
    let mut roots=ROOTS.get_or_init(Default::default).lock().map_err(|_|"installation root cache unavailable")?;
    roots.entry(name).or_insert_with(|| {
        if let Some(root)=std::env::var_os(format!("{}_INSTALL",name.to_uppercase())) {
            if root.is_empty() {return Err("explicit installation root is empty".into());}
            return std::path::absolute(root).map_err(|error|format!("explicit installation root unavailable: {error}"));
        }
        let executable=std::env::current_exe().and_then(fs::canonicalize).map_err(|error|format!("installation executable unavailable: {error}"))?;
        if let Some(root)=installed_root(name,title,aliases,&executable)? {return Ok(root);}
        let variable=if cfg!(windows) {"LOCALAPPDATA"}else{"HOME"};
        let home=std::env::var_os(variable).map(PathBuf::from).filter(|p|p.is_absolute()).ok_or_else(||format!("installation default unavailable: {variable}"))?;
        Ok(if cfg!(windows) {home.join(name)}else{home.join(".local/share").join(name)})
    }).clone()
}

fn installed_root(name:&str,title:&str,aliases:&[&str],executable:&Path)->Result<Option<PathBuf>,String> {
    use std::io::Read;
    let primary=format!("{name}{}",std::env::consts::EXE_SUFFIX);
    let Some(file)=executable.file_name().and_then(|f|f.to_str()) else {return Ok(None)};
    let old=format!("{primary}.old");
    let renamed=file==old||file.strip_prefix(&format!("{old}.")).is_some_and(|n|!n.is_empty()&&n.bytes().all(|b|b.is_ascii_digit()));
    if file!=primary&&!renamed&&!aliases.contains(&file) {return Ok(None);}
    let parent=executable.parent().ok_or("installation executable has no directory")?;
    let release=parent.parent().filter(|p|p.file_name().is_some_and(|p|p=="releases"));
    let bundle=PathBuf::from(format!("{title}.app")).join("Contents/MacOS");
    let root=if let Some(releases)=release {
        if file!=primary||parent.file_name().and_then(|v|v.to_str()).is_none_or(|v|!launcher::valid_version(v)||v.ends_with(".tmp")) {
            return Err("installation release entrypoint invalid".into());
        }
        releases.parent().ok_or("installation release has no root")?
    } else if parent.ends_with(&bundle) {
        if file!=primary {return Err("installation bundle entrypoint invalid".into());}
        parent.ancestors().nth(3).ok_or("installation bundle has no root")?
    } else {
        let mut claimed=false;
        for marker in ["current","releases"] {
            match fs::symlink_metadata(parent.join(marker)) {
                Ok(_)=>claimed=true,Err(error) if error.kind()==std::io::ErrorKind::NotFound=>{},
                Err(error)=>return Err(format!("installation marker unreadable: {error}")),
            }
        }
        if !claimed {return Ok(None);}
        parent
    };
    let current=root.join("current");
    if !fs::symlink_metadata(&current).map_err(|error|format!("installation current unavailable: {error}"))?.is_file()
        ||!fs::symlink_metadata(root.join("releases")).map_err(|error|format!("installation releases unavailable: {error}"))?.is_dir() {
        return Err("installation layout is not regular storage".into());
    }
    let mut selected=String::new();fs::File::open(current).and_then(|file|file.take(129).read_to_string(&mut selected)).map_err(|error|format!("installation current unreadable: {error}"))?;
    if selected.len()>128||!launcher::valid_version(selected.trim())||selected.trim().ends_with(".tmp") {return Err("installation current invalid".into());}
    if !fs::symlink_metadata(root.join("releases").join(selected.trim()).join(primary)).map_err(|error|format!("installation primary release unavailable: {error}"))?.is_file() {
        return Err("installation primary release is not a regular file".into());
    }
    Ok(Some(root.to_path_buf()))
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
pub fn write_durable(p: &Path, bytes: impl AsRef<[u8]>) -> Result<(), String> {
    use std::io::Write;
    let mut name=p.file_name().ok_or("durable file has no name")?.to_os_string();
    name.push(".pending");
    let tmp=p.with_file_name(name);
    let mut file=fs::OpenOptions::new().write(true).create_new(true).open(&tmp)
        .map_err(|e|format!("durable file prepare: {e}"))?;
    let prepared=file.write_all(bytes.as_ref()).and_then(|_|file.sync_all());
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

/// Bulk FULL-sync store tests run one at a time: in parallel they share the runner disk,
/// and on Windows one checkpoint stalled 3-8 s, past the 3 s capture acknowledgment.
#[cfg(test)]
pub(crate) fn bulk_disk() -> std::sync::MutexGuard<'static, ()> {
    static DISK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    DISK.lock().unwrap_or_else(|e| e.into_inner())
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
        for (mode, body) in [("ok", "printf private-token"), ("exit", "echo private-error >&2; exit 7"), ("empty", ":"), ("local", "exit 8"), ("gitlab", "printf private-token")] {
            let dir = d.join(mode); fs::create_dir_all(&dir).unwrap();
            if mode=="local" {fs::write(dir.join("token")," private-token\n").unwrap();}
            // A GitLab lookup skips the GitHub token file and asks the glab login.
            if mode=="gitlab" {fs::write(dir.join("token"),"github-file\n").unwrap();}
            let (bin, args) = if mode=="gitlab" {("glab", "config get token --host fixture.invalid")} else {("gh", "auth token --hostname fixture.invalid")};
            let script = dir.join(bin);
            fs::write(&script, format!("#!/bin/sh\n[ \"$*\" = '{args}' ] || exit 9\n{body}\n")).unwrap();
            fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).unwrap();
            let out = Command::new(std::env::current_exe().unwrap()).args(["--exact", "tests::credential_lookup_child", "--ignored", "--nocapture"])
                .env("COREQ_TOKEN_FIXTURE", &dir).env("PATH", &dir).env_remove("DEMOQ_TOKEN").env_remove("GITHUB_TOKEN").env_remove("GITLAB_TOKEN")
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
        let mode = dir.file_name().unwrap().to_str().unwrap();
        let result = tracing::subscriber::with_default(tracing_subscriber::registry().with(capture.clone()), || tool(&dir).token(mode=="gitlab", "fixture.invalid"));
        match mode {
            "ok"|"local"|"gitlab" => assert_eq!(result.unwrap(), "private-token"),
            "exit" => assert!(result.unwrap_err().starts_with("board token lookup unavailable:")),
            "empty" => assert!(result.unwrap_err().starts_with("no board token:")),
            _ => unreachable!(),
        }
        capture.flush().unwrap();
        let rows = store.export(0, 0, 100).unwrap();
        let values=rows["events"].as_array().unwrap().iter().map(|r| &r["observation"]).collect::<Vec<_>>();
        let measured=values.iter().find(|r|r["operation"]=="fixtureq.operation.token.read"&&r["kind"]=="method").expect("logical token outcome absent from shared capture");
        assert_eq!(values.iter().filter(|r|r["operation"]=="fixtureq.operation.token.read"&&r["kind"]=="method").count(),1);
        assert_eq!(measured["error"],match mode {"exit"=>"unknown","empty"=>"missing",_=>"none"});
        assert_eq!(measured["outcome"],if matches!(mode,"exit"|"empty") {"failed"}else{"ok"});
        let lookup=values.iter().find(|r|r["operation"]=="fixtureq.operation.token.lookup"&&r["kind"]=="method");
        if mode=="local" {assert!(lookup.is_none());} else {
            let row=lookup.unwrap();assert_eq!(row["error"],if mode=="exit" {"exit"}else{"none"});
            assert_eq!(row["trace"],measured["trace"]);assert_eq!(row["parent"],measured["span"]);
        }
        assert!(!dir.join(".demoq/profile.json").exists(),"capture must not create a compatibility sink");
        assert!(!rows.to_string().contains("private-"));
        profile::flush(&tool(&dir),&dir).unwrap();
        let compatibility=profile::report(&tool(&dir),&dir,&[]);
        assert_eq!(compatibility["ops"]["token.read"]["count"],1);
        assert_eq!(compatibility["ops"]["token.read"]["failure"],if matches!(mode,"exit"|"empty") {1.0}else{0.0});
        if mode=="exit"||mode=="empty" {assert_eq!(compatibility["ops"]["token.read"]["classes"][if mode=="exit" {"unknown"}else{"missing"}],1);}
    }

    #[test]
    fn tool_names_come_from_parameters() {
        let t = tool(Path::new("/i"));
        assert_eq!(t.env("TOKEN"), "DEMOQ_TOKEN");
        assert_eq!(t.release_exe("1.2.3"), Path::new("/i/releases/1.2.3").join(t.exe_name()));
        assert!(t.exe_name().starts_with("demoq"));
        let d = scratch("token");
        fs::write(d.join("token"), "  tok\n").unwrap();
        fs::write(d.join("gitlab-token"), "lab\n").unwrap();
        let t = Tool { name: "coreqtokentest", ..tool(&d) };
        if std::env::var_os("GITHUB_TOKEN").is_none() {
            assert_eq!(t.token(false, "github.com"), Ok("tok".into()), "install token file before the gh login");
        }
        if std::env::var_os("GITLAB_TOKEN").is_none() {
            assert_eq!(t.token(true, "gitlab.com"), Ok("lab".into()), "GitLab reads its own token file");
        }
    }
}
