//! The one OS unit per project that keeps `<install>/<name> service run` running (coreq.md § Service units).

use crate::{Tool, quiet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

pub struct Unit {
    pub path: PathBuf,
    pub text: String,
    pub load: Vec<Vec<String>>,
    pub unload: Vec<Vec<String>>,
    /// Stops the service until the next logon; the unit stays.
    pub stop: Vec<Vec<String>>,
}

/// The service's output file; Linux logs to the journal.
pub fn log_path(t: &Tool, project: &str) -> Option<PathBuf> {
    if cfg!(target_os = "macos") {
        Some(t.install.join(format!("{project}.log")))
    } else if cfg!(windows) {
        Some(t.install.join(format!("{}.log", t.name)))
    } else {
        None
    }
}

fn xml(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;")
}

fn sv(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

pub fn unit(t: &Tool, root: &Path, project: &str) -> Unit {
    let path_env = std::env::var("PATH").unwrap_or_default();
    let home = PathBuf::from(std::env::var_os("HOME").unwrap_or_default());
    if cfg!(target_os = "macos") {
        let uid = Command::new("id").arg("-u").output().ok().map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string()).unwrap_or_default();
        launchd(t, root, project, &home, &path_env, &uid)
    } else if cfg!(windows) {
        // `whoami`, not USERDOMAIN: an ssh session reports WORKGROUP and schtasks rejects `WORKGROUP\<user>`
        let user = Command::new("whoami").output().ok().filter(|o| o.status.success()).map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string()).filter(|u| !u.is_empty())
            .unwrap_or_else(|| format!("{}\\{}", std::env::var("USERDOMAIN").unwrap_or_default(), std::env::var("USERNAME").unwrap_or_default()));
        schtasks(t, root, project, &user)
    } else {
        systemd(t, root, project, &home, &path_env)
    }
}

fn launchd(t: &Tool, root: &Path, project: &str, home: &Path, path_env: &str, uid: &str) -> Unit {
    let label = format!("com.{}.{project}", t.name);
    let path = home.join("Library/LaunchAgents").join(format!("{label}.plist"));
    let log = t.install.join(format!("{project}.log")).display().to_string();
    let l = t.install.join(t.exe_name()).display().to_string();
    let text = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict>\n<key>Label</key><string>{}</string>\n<key>ProgramArguments</key><array><string>{}</string><string>service</string><string>run</string></array>\n<key>WorkingDirectory</key><string>{}</string>\n<key>RunAtLoad</key><true/>\n<key>KeepAlive</key><true/>\n<key>StandardOutPath</key><string>{}</string>\n<key>StandardErrorPath</key><string>{}</string>\n<key>EnvironmentVariables</key><dict><key>PATH</key><string>{}</string></dict>\n</dict></plist>\n",
        xml(&label), xml(&l), xml(&root.display().to_string()), xml(&log), xml(&log), xml(path_env)
    );
    let p = path.display().to_string();
    Unit {
        load: vec![sv(&["launchctl", "bootstrap", &format!("gui/{uid}"), &p])],
        unload: vec![sv(&["launchctl", "bootout", &format!("gui/{uid}/{label}")])],
        stop: vec![sv(&["launchctl", "bootout", &format!("gui/{uid}/{label}")])],
        path,
        text,
    }
}

/// This user's logon only: an any-user logon trigger needs admin to register.
fn schtasks(t: &Tool, root: &Path, project: &str, user: &str) -> Unit {
    let name = format!("{}-{project}", t.title);
    let path = t.install.join(format!("{name}.xml"));
    let l = t.install.join(t.exe_name()).display().to_string();
    let text = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<Task version=\"1.2\" xmlns=\"http://schemas.microsoft.com/windows/2004/02/mit/task\">\n<Triggers><LogonTrigger><Enabled>true</Enabled><UserId>{u}</UserId></LogonTrigger></Triggers>\n<Principals><Principal id=\"Author\"><UserId>{u}</UserId><LogonType>InteractiveToken</LogonType><RunLevel>LeastPrivilege</RunLevel></Principal></Principals>\n<Settings><MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy><DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries><StopIfGoingOnBatteries>false</StopIfGoingOnBatteries><ExecutionTimeLimit>PT0S</ExecutionTimeLimit><RestartOnFailure><Interval>PT1M</Interval><Count>999</Count></RestartOnFailure><Hidden>true</Hidden></Settings>\n<Actions Context=\"Author\"><Exec><Command>{l}</Command><Arguments>service run</Arguments><WorkingDirectory>{r}</WorkingDirectory></Exec></Actions>\n</Task>\n",
        u = xml(user), l = xml(&l), r = xml(&root.display().to_string())
    );
    let p = path.display().to_string();
    Unit {
        load: vec![sv(&["schtasks", "/Create", "/TN", &name, "/XML", &p, "/F"]), sv(&["schtasks", "/Run", "/TN", &name])],
        unload: vec![sv(&["schtasks", "/End", "/TN", &name]), sv(&["schtasks", "/Delete", "/TN", &name, "/F"])],
        stop: vec![sv(&["schtasks", "/End", "/TN", &name])],
        path,
        text,
    }
}

fn systemd(t: &Tool, root: &Path, project: &str, home: &Path, path_env: &str) -> Unit {
    let name = format!("{}-{project}.service", t.name);
    let path = home.join(".config/systemd/user").join(&name);
    let (l, r, tn) = (t.install.join(t.exe_name()).display().to_string(), root.display(), t.name);
    let text = format!(
        "[Unit]\nDescription={tn} {project}\n\n[Service]\nExecStart=\"{l}\" service run\nWorkingDirectory={r}\nRestart=always\nRestartSec=10\nEnvironment=PATH={path_env}\n\n[Install]\nWantedBy=default.target\n"
    );
    Unit {
        load: vec![sv(&["systemctl", "--user", "daemon-reload"]), sv(&["systemctl", "--user", "enable", "--now", &name])],
        unload: vec![sv(&["systemctl", "--user", "disable", "--now", &name])],
        stop: vec![sv(&["systemctl", "--user", "stop", &name])],
        path,
        text,
    }
}

pub fn write_unit(u: &Unit) -> Result<(), String> {
    fs::create_dir_all(u.path.parent().unwrap()).map_err(|e| e.to_string())?;
    let bytes: Vec<u8> = if cfg!(windows) {
        // schtasks reads task XML as UTF-16 with a BOM
        std::iter::once(0xFEFFu16).chain(u.text.encode_utf16()).flat_map(|c| c.to_le_bytes()).collect()
    } else {
        u.text.clone().into_bytes()
    };
    fs::write(&u.path, bytes).map_err(|e| format!("{}: {e}", u.path.display()))
}

pub fn run_all(cmds: &[Vec<String>], ignore: bool) -> Result<(), String> {
    for c in cmds {
        // captured, not inherited: a console-less caller (a tray) would give the child its own window
        let o = quiet(Command::new(&c[0]).args(&c[1..])).output().map_err(|e| format!("{}: {e}", c[0]))?;
        print!("{}", String::from_utf8_lossy(&o.stdout));
        eprint!("{}", String::from_utf8_lossy(&o.stderr));
        if !o.status.success() && !ignore {
            return Err(format!("`{}` failed: {}", c.join(" "), o.status));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn units_per_os_name_the_tool() {
        let t = crate::tool(Path::new("/i"));
        let (root, home) = (Path::new("/p"), Path::new("/h"));
        let m = launchd(&t, root, "maps", home, "/bin", "501");
        assert_eq!(m.path, Path::new("/h/Library/LaunchAgents/com.demoq.maps.plist"));
        assert!(m.text.contains("<key>Label</key><string>com.demoq.maps</string>") && m.text.contains("<key>KeepAlive</key><true/>"));
        assert_eq!(m.load, [sv(&["launchctl", "bootstrap", "gui/501", "/h/Library/LaunchAgents/com.demoq.maps.plist"])]);
        assert_eq!(m.stop, [sv(&["launchctl", "bootout", "gui/501/com.demoq.maps"])]);
        let w = schtasks(&t, root, "maps", "pc\\a&b");
        assert_eq!(w.path, Path::new("/i/DemoQ-maps.xml"));
        assert!(w.text.contains("<UserId>pc\\a&amp;b</UserId>") && w.text.contains("<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>"));
        assert_eq!(w.unload, [sv(&["schtasks", "/End", "/TN", "DemoQ-maps"]), sv(&["schtasks", "/Delete", "/TN", "DemoQ-maps", "/F"])]);
        let s = systemd(&t, root, "maps", home, "/bin");
        assert_eq!(s.path, Path::new("/h/.config/systemd/user/demoq-maps.service"));
        assert!(s.text.contains("ExecStart=\"/i/demoq") && s.text.contains("Restart=always"));
        assert_eq!(s.load[1], sv(&["systemctl", "--user", "enable", "--now", "demoq-maps.service"]));
    }
}
