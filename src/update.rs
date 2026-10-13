//! Release names, self-update and first install (coreq.md § Release, § Update).

use crate::{Tool, write_atomic};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

/// `<os>-<arch>` of release files.
pub fn target() -> String {
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}

/// `4.1.0` > `4.0.12` > `4.0.0-dev`: numeric parts, a pre-release suffix sorts first.
pub fn newer(a: &str, b: &str) -> bool {
    let key = |v: &str| {
        let (core, pre) = v.split_once('-').map_or((v, None), |(c, p)| (c, Some(p.to_string())));
        let nums: Vec<u64> = core.split('.').map(|x| x.parse().unwrap_or(0)).collect();
        (nums, pre.is_none(), pre)
    };
    key(a) > key(b)
}

/// HTTP through the OS `curl`: no client library.
pub fn download(url: &str, to: &Path) -> Result<(), String> {
    let out = Command::new("curl").args(["-fsSL", "--max-time", "300", "-o"]).arg(to).arg(url).output().map_err(|e| format!("curl: {e}"))?;
    if out.status.success() { Ok(()) } else { Err(format!("download {url}: {}", String::from_utf8_lossy(&out.stderr).trim())) }
}

/// `SHA256SUMS` lists `name` with the sha256 of `bytes`.
pub fn verify(sums: &[u8], name: &str, bytes: &[u8]) -> Result<(), String> {
    let digest = crate::sha256_hex(bytes);
    let listed = String::from_utf8_lossy(sums).lines().any(|l| l.split_whitespace().collect::<Vec<_>>() == [digest.as_str(), name]);
    if listed { Ok(()) } else { Err(format!("{name}: sha256 {digest} not in SHA256SUMS")) }
}

/// Both become paths under `releases/`: only the exact expected names pass.
fn check_latest(t: &Tool, ver: &str, name: &str) -> Result<(), String> {
    let ok = ver.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'.' || c == b'-') && !ver.contains("..") && name == format!("{}-{ver}-{}{}", t.name, target(), std::env::consts::EXE_SUFFIX);
    if ok { Ok(()) } else { Err(format!("latest.json: unexpected version {ver:?} or file {name:?}")) }
}

/// Update steps: Ok(Some(version)) when a new release became current; with `check`, when one is available.
pub fn update(t: &Tool, check: bool) -> Result<Option<String>, String> {
    let rel = t.install.join("releases");
    fs::create_dir_all(&rel).map_err(|e| e.to_string())?;
    prune(t);
    let base = format!("https://github.com/{}/releases/latest/download", t.repo);
    let probe = rel.join("latest.json.tmp");
    download(&format!("{base}/latest.json"), &probe)?;
    let latest: serde_json::Value = serde_json::from_slice(&fs::read(&probe).map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
    let _ = fs::remove_file(&probe);
    let ver = latest["version"].as_str().ok_or("latest.json: no version")?.to_string();
    let running = t.current().unwrap_or_else(|| t.version.to_string());
    if !newer(&ver, &running) || t.release_exe(&ver).exists() {
        return Ok(None);
    }
    let name = latest["files"][target()].as_str().ok_or(format!("latest.json: no file for {}", target()))?.to_string();
    check_latest(t, &ver, &name)?;
    if check {
        return Ok(Some(ver));
    }
    let tmp = rel.join(format!("{ver}.tmp"));
    fs::create_dir_all(&tmp).map_err(|e| e.to_string())?;
    let exe = t.exe_name();
    let result = (|| {
        for f in ["SHA256SUMS", name.as_str()] {
            download(&format!("{base}/{f}"), &tmp.join(f))?;
        }
        let bytes = fs::read(tmp.join(&name)).map_err(|e| e.to_string())?;
        verify(&fs::read(tmp.join("SHA256SUMS")).map_err(|e| e.to_string())?, &name, &bytes)?;
        fs::rename(tmp.join(&name), tmp.join(&exe)).map_err(|e| e.to_string())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(tmp.join(&exe), fs::Permissions::from_mode(0o755)).map_err(|e| e.to_string())?;
        }
        let smoke = Command::new(tmp.join(&exe)).arg("selftest").env(t.env("SELFTEST_OFFLINE"), "1").output().map_err(|e| e.to_string())?;
        if !smoke.status.success() {
            return Err(format!("{ver} selftest failed: {}", String::from_utf8_lossy(&smoke.stderr).trim()));
        }
        fs::rename(&tmp, rel.join(&ver)).map_err(|e| e.to_string())
    })();
    if let Err(e) = result {
        let _ = fs::remove_dir_all(&tmp);
        return Err(e);
    }
    if let Some(cur) = t.current() {
        write_atomic(&t.install.join("previous"), &cur)?;
    }
    write_atomic(&t.install.join("current"), &ver)?;
    replace_launcher(t, &t.release_exe(&ver))?;
    prune(t);
    Ok(Some(ver))
}

/// Deletes `releases/*.tmp` and every release that is neither `current` nor `previous`; one whose exe is in use
/// (Windows) is reported on stderr and kept. Returns the kept stale versions.
fn prune(t: &Tool) -> Vec<String> {
    let rel = t.install.join("releases");
    let cur = t.current();
    let prev = crate::read(&t.install.join("previous"));
    let mut kept = vec![];
    for e in fs::read_dir(&rel).into_iter().flatten().flatten() {
        let v = e.file_name().to_string_lossy().to_string();
        if v.ends_with(".tmp") {
            let _ = fs::remove_dir_all(e.path());
            continue;
        }
        if !e.path().is_dir() || cur.is_none() || Some(&v) == cur.as_ref() || Some(&v) == prev.as_ref() {
            continue;
        }
        // the exe first: a running one fails before its counters go
        let exe = e.path().join(t.exe_name());
        let gone = match fs::remove_file(&exe) {
            Err(err) if err.kind() != std::io::ErrorKind::NotFound => Err(err),
            _ => fs::remove_dir_all(e.path()),
        };
        if let Err(err) = gone {
            eprintln!("{}: keep releases/{v}: {err}", t.name);
            kept.push(v);
        }
    }
    kept
}

/// Writes `src` over the launcher `<install>/<name>`; a running launcher keeps its process.
fn replace_launcher(t: &Tool, src: &Path) -> Result<(), String> {
    let launcher = t.install.join(t.exe_name());
    let err = |p: &Path, e: std::io::Error| format!("{}: {e}", p.display());
    #[cfg(unix)]
    {
        let tmp = t.install.join(format!("{}.tmp", t.exe_name()));
        fs::copy(src, &tmp).map_err(|e| err(&tmp, e))?;
        fs::rename(&tmp, &launcher).map_err(|e| err(&launcher, e))
    }
    #[cfg(windows)]
    {
        // a running .exe cannot be written or deleted, only renamed
        let old = free_old(t);
        let moved = launcher.exists();
        if moved {
            fs::rename(&launcher, &old).map_err(|e| err(&old, e))?;
        }
        fs::copy(src, &launcher).map(|_| ()).map_err(|e| {
            if moved {
                let _ = fs::rename(&old, &launcher);
            }
            err(&launcher, e)
        })
    }
}

/// `<name>.old`, else the first `<name>.old.<n>` that is free or removable: an earlier one may still run.
#[cfg_attr(unix, allow(dead_code))]
fn free_old(t: &Tool) -> PathBuf {
    let base = format!("{}.old", t.exe_name());
    (0..)
        .map(|n| t.install.join(if n == 0 { base.clone() } else { format!("{base}.{n}") }))
        .find(|p| {
            let _ = fs::remove_file(p);
            fs::symlink_metadata(p).is_err()
        })
        .unwrap()
}

/// First install of this binary: it becomes the current release and the launcher.
pub fn install_self(t: &Tool) -> Result<(), String> {
    let me = std::env::current_exe().map_err(|e| e.to_string())?;
    let dst = t.release_exe(t.version);
    fs::create_dir_all(dst.parent().unwrap()).map_err(|e| e.to_string())?;
    if me != dst {
        fs::copy(&me, &dst).map_err(|e| e.to_string())?;
    }
    if me != t.install.join(t.exe_name()) {
        replace_launcher(t, &me)?;
    }
    // a newer binary installs over an older one as an update does; never a downgrade
    match t.current() {
        Some(c) if !newer(t.version, &c) => {}
        old => {
            if let Some(c) = old {
                write_atomic(&t.install.join("previous"), &c)?;
            }
            write_atomic(&t.install.join("current"), t.version)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_and_checksums() {
        assert!(newer("4.1.0", "4.0.12") && newer("4.0.0", "4.0.0-dev") && !newer("4.0.0", "4.0.0"));
        let bin = b"binary";
        let digest: String = ring::digest::digest(&ring::digest::SHA256, bin).as_ref().iter().map(|b| format!("{b:02x}")).collect();
        let sums = format!("{digest}  taskq-4.0.0-macos-aarch64\n");
        assert_eq!(verify(sums.as_bytes(), "taskq-4.0.0-macos-aarch64", bin), Ok(()));
        assert!(verify(sums.as_bytes(), "taskq-4.0.0-macos-aarch64", b"evil").is_err());
        assert!(verify(sums.as_bytes(), "taskq-4.0.0-linux-x86_64", bin).is_err());
    }

    #[test]
    fn launcher_is_replaced_by_each_install() {
        let d = crate::scratch("replace-launcher");
        let t = crate::tool(&d);
        let launcher = d.join(t.exe_name());
        for v in ["old", "new"] {
            fs::write(d.join("src"), v).unwrap();
            replace_launcher(&t, &d.join("src")).unwrap();
            assert_eq!(fs::read_to_string(&launcher).unwrap(), v);
        }
        assert!(!d.join(format!("{}.tmp", t.exe_name())).exists());
        if cfg!(windows) {
            assert_eq!(fs::read_to_string(d.join(format!("{}.old", t.exe_name()))).unwrap(), "old");
        }
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn old_launcher_in_use_gets_a_free_name() {
        let d = crate::scratch("free-old");
        let t = crate::tool(&d);
        let old = d.join(format!("{}.old", t.exe_name()));
        assert_eq!(free_old(&t), old);
        // a directory cannot be removed as a file, like a running .exe
        fs::create_dir_all(old.join("busy")).unwrap();
        fs::write(old.with_extension("old.1"), "stale").unwrap();
        assert_eq!(free_old(&t), old.with_extension("old.1"));
        assert!(!old.with_extension("old.1").exists());
        let _ = fs::remove_dir_all(&d);
    }

    #[cfg(windows)]
    #[test]
    fn second_update_while_old_launcher_runs() {
        let d = crate::scratch("old-in-use");
        let t = crate::tool(&d);
        let launcher = d.join(t.exe_name());
        fs::copy(std::env::var("ComSpec").unwrap(), &launcher).unwrap();
        // the running launcher: cmd waits on its open stdin
        let mut run = Command::new(&launcher).stdin(std::process::Stdio::piped()).spawn().unwrap();
        fs::write(d.join("src"), "new").unwrap();
        replace_launcher(&t, &d.join("src")).unwrap();
        fs::copy(std::env::var("ComSpec").unwrap(), &launcher).unwrap();
        let mut run2 = Command::new(&launcher).stdin(std::process::Stdio::piped()).spawn().unwrap();
        replace_launcher(&t, &d.join("src")).unwrap();
        assert_eq!(fs::read_to_string(&launcher).unwrap(), "new");
        assert!(d.join(format!("{}.old.1", t.exe_name())).exists());
        let _ = (run.kill(), run2.kill(), run.wait(), run2.wait());
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn prune_keeps_current_and_previous() {
        let d = crate::scratch("prune");
        let t = crate::tool(&d);
        for v in ["1.0.0", "1.1.0", "1.2.0", "1.3.0.tmp"] {
            fs::create_dir_all(d.join("releases").join(v).join("starts")).unwrap();
            fs::write(t.release_exe(v), "bin").unwrap();
        }
        assert_eq!(prune(&t), Vec::<String>::new(), "no current: nothing pruned");
        assert!(d.join("releases/1.0.0").exists());
        assert!(!d.join("releases/1.3.0.tmp").exists(), "no current: tmp still deleted");
        write_atomic(&d.join("current"), "1.2.0").unwrap();
        write_atomic(&d.join("previous"), "1.1.0").unwrap();
        assert_eq!(prune(&t), Vec::<String>::new());
        let left: Vec<_> = ["1.0.0", "1.1.0", "1.2.0", "1.3.0.tmp"].into_iter().filter(|v| d.join("releases").join(v).exists()).collect();
        assert_eq!(left, ["1.1.0", "1.2.0"]);
        let _ = fs::remove_dir_all(&d);
    }

    #[cfg(windows)]
    #[test]
    fn prune_skips_a_release_in_use() {
        let d = crate::scratch("prune-in-use");
        let t = crate::tool(&d);
        for v in ["1.0.0", "1.1.0", "1.2.0"] {
            fs::create_dir_all(d.join("releases").join(v)).unwrap();
            fs::copy(std::env::var("ComSpec").unwrap(), t.release_exe(v)).unwrap();
        }
        write_atomic(&d.join("current"), "1.2.0").unwrap();
        write_atomic(&d.join("previous"), "1.1.0").unwrap();
        let mut run = Command::new(t.release_exe("1.0.0")).stdin(std::process::Stdio::piped()).spawn().unwrap();
        assert_eq!(prune(&t), ["1.0.0"]);
        assert!(t.release_exe("1.0.0").exists());
        let _ = (run.kill(), run.wait());
        assert_eq!(prune(&t), Vec::<String>::new(), "the next update removes it");
        assert!(!d.join("releases/1.0.0").exists());
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn latest_names_only_this_tool() {
        let t = crate::tool(Path::new("/i"));
        let own = format!("demoq-1.1.0-{}{}", target(), std::env::consts::EXE_SUFFIX);
        assert_eq!(check_latest(&t, "1.1.0", &own), Ok(()));
        assert!(check_latest(&t, "1.1.0", &own.replace("demoq", "taskq")).is_err(), "another tool's file");
        assert!(check_latest(&t, "../1", &own).is_err() && check_latest(&t, "1/0", &own).is_err());
    }
}
