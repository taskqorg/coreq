//! Release names, self-update and first install (coreq.md § Release, § Update).

use crate::{Tool, write_atomic};
use std::fs;
use std::path::Path;
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
    let digest: String = ring::digest::digest(&ring::digest::SHA256, bytes).as_ref().iter().map(|b| format!("{b:02x}")).collect();
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
    for e in fs::read_dir(&rel).map_err(|e| e.to_string())?.flatten() {
        if e.file_name().to_string_lossy().ends_with(".tmp") {
            let _ = fs::remove_dir_all(e.path());
        }
    }
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
    Ok(Some(ver))
}

/// First install of this binary: it becomes the current release and the launcher.
pub fn install_self(t: &Tool) -> Result<(), String> {
    let me = std::env::current_exe().map_err(|e| e.to_string())?;
    let dst = t.release_exe(t.version);
    fs::create_dir_all(dst.parent().unwrap()).map_err(|e| e.to_string())?;
    if me != dst {
        fs::copy(&me, &dst).map_err(|e| e.to_string())?;
    }
    let launcher = t.install.join(t.exe_name());
    if me != launcher {
        // ponytail: the launcher stays at the version that installed it; it only reads `current` and `previous`
        fs::copy(&me, &launcher).map_err(|e| format!("{}: {e}", launcher.display()))?;
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
    fn latest_names_only_this_tool() {
        let t = crate::tool(Path::new("/i"));
        let own = format!("demoq-1.1.0-{}{}", target(), std::env::consts::EXE_SUFFIX);
        assert_eq!(check_latest(&t, "1.1.0", &own), Ok(()));
        assert!(check_latest(&t, "1.1.0", &own.replace("demoq", "taskq")).is_err(), "another tool's file");
        assert!(check_latest(&t, "../1", &own).is_err() && check_latest(&t, "1/0", &own).is_err());
    }
}
