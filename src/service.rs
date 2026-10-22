//! Host and legacy project OS units that keep `<install>/<name> service run` running (coreq.md § Service units).

use crate::{Tool, process::{output, Failure}};
use std::time::Duration;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

pub struct Unit {
    pub path: PathBuf,
    pub text: String,
    pub load: Vec<Vec<String>>,
    pub unload: Vec<Vec<String>>,
    /// Stops the service, retaining its native autostart definition.
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
        let uid = output("service.identity", Command::new("id").arg("-u"), Duration::from_secs(30)).ok().map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string()).unwrap_or_default();
        launchd(t, root, project, &home, &path_env, &uid)
    } else if cfg!(windows) {
        // `whoami`, not USERDOMAIN: an ssh session reports WORKGROUP and schtasks rejects `WORKGROUP\<user>`
        let user = output("service.identity", &mut Command::new("whoami"), Duration::from_secs(30)).ok().filter(|o| o.status.success()).map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string()).filter(|u| !u.is_empty())
            .unwrap_or_else(|| format!("{}\\{}", std::env::var("USERDOMAIN").unwrap_or_default(), std::env::var("USERNAME").unwrap_or_default()));
        schtasks(t, root, project, &user)
    } else {
        systemd(t, root, project, &home, &path_env)
    }
}

/// Install the host unit. The caller must first establish that loading it is safe.
pub fn host_unit(t: &Tool) -> Result<Unit, String> {
    let u = unit(t, &t.install, "");
    #[cfg(windows)] install_windows_host(t, &u)?;
    #[cfg(not(windows))] {write_unit(&u)?;run_all(&u.load, false)?;}
    Ok(u)
}

/// PowerShell definition for exact Windows filesystem identity, including directories.
#[cfg(windows)]
pub fn windows_path_identity() -> &'static str {r#"
if (-not ('CoreQPathIdentity' -as [type])) { Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;
public static class CoreQPathIdentity {
  [StructLayout(LayoutKind.Sequential)] private struct FileId128 {
    [MarshalAs(UnmanagedType.ByValArray, SizeConst=16)] public byte[] Value;
  }
  [StructLayout(LayoutKind.Sequential)] private struct FileIdInfo {
    public ulong VolumeSerialNumber;
    public FileId128 FileId;
  }
  [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
  private static extern SafeFileHandle CreateFileW(string path, uint access, uint share, IntPtr security, uint creation, uint flags, IntPtr template);
  [DllImport("kernel32.dll", SetLastError=true)]
  private static extern bool GetFileInformationByHandleEx(SafeFileHandle handle, int kind, out FileIdInfo info, uint size);
  private static string Id(string path) {
    using (SafeFileHandle handle = CreateFileW(path, 0, 7, IntPtr.Zero, 3, 0x02000000, IntPtr.Zero)) {
      if (handle.IsInvalid) throw new Win32Exception(Marshal.GetLastWin32Error());
      FileIdInfo info;
      if (!GetFileInformationByHandleEx(handle, 18, out info, (uint)Marshal.SizeOf(typeof(FileIdInfo)))) throw new Win32Exception(Marshal.GetLastWin32Error());
      return info.VolumeSerialNumber.ToString("X16") + BitConverter.ToString(info.FileId.Value);
    }
  }
  public static bool Same(string left, string right) { return Id(left) == Id(right); }
}
'@ }
function Test-CoreQPath([string]$left,[string]$right) {
  if([string]::IsNullOrWhiteSpace($left) -or [string]::IsNullOrWhiteSpace($right)) { throw 'path identity requires two paths' }
  [CoreQPathIdentity]::Same($left,$right)
}
"#}

#[cfg(windows)]
fn install_windows_host(t:&Tool,u:&Unit)->Result<(),String> {
    fs::create_dir_all(&t.install).map_err(|e|e.to_string())?;
    t.install.canonicalize().map_err(|e|format!("host installation directory unknown: {e}"))?;
    let q=|text:&str|format!("'{}'",text.replace('\'',"''"));
    let code=format!(r#"{}$ErrorActionPreference='Stop';$s=New-Object -ComObject 'Schedule.Service';$s.Connect();$r=$s.GetFolder('\');$name={name};$identity=[Security.Principal.WindowsIdentity]::GetCurrent();$sid=$identity.User.Value;$xml={xml}
if(!$sid -or !$identity.IsAuthenticated){{throw 'current user SID unknown'}}
try{{$old=$r.GetTask($name)}}catch{{if(($_.Exception.GetBaseException()).HResult -ne -2147024894){{throw}};$old=$null}}
$check={{param($task) $d=$task.Definition;[xml]$native=$task.Xml;$owner=[Security.Principal.SecurityIdentifier]::new([string]$native.Task.Principals.Principal.UserId).Value
if($d.Principal.LogonType -ne 3 -or $d.Principal.RunLevel -ne 0 -or $owner -ne $sid){{throw 'host task principal scope differs'}}
$a=$d.Actions.Item(1);if($d.Actions.Count -ne 1 -or !(Test-CoreQPath $a.Path {exe}) -or $a.Arguments -ne 'service run' -or !(Test-CoreQPath $a.WorkingDirectory {cwd})){{throw 'host task action scope differs'}}}}
if($old){{& $check $old}}
[IO.File]::WriteAllText({path},$xml,[Text.Encoding]::Unicode)
$sddl='D:P(A;;GA;;;'+$sid+')(A;;GA;;;SY)';$t=$r.RegisterTask($name,$xml,22,$null,$null,3,$sddl)
$t=$r.GetTask($name);& $check $t;$acl=[Security.AccessControl.RawSecurityDescriptor]::new($t.GetSecurityDescriptor(4));$aces=@($acl.DiscretionaryAcl)
if(($acl.ControlFlags -band [Security.AccessControl.ControlFlags]::DiscretionaryAclProtected) -eq 0 -or $aces.Count -ne 2){{throw 'host task management rights differ'}}
foreach($who in @($sid,'S-1-5-18')){{$rights=@($aces|Where-Object {{$_.AceQualifier -eq 'AccessAllowed' -and $_.SecurityIdentifier.Value -eq $who -and (($_.AccessMask -band 0x10000000) -ne 0 -or ($_.AccessMask -band 0x1f01ff) -eq 0x1f01ff)}});if($rights.Count -ne 1){{throw 'host task management rights not observed'}}}}
$null=$t.Run($null)
"#,windows_path_identity(),name=q(t.title),exe=q(&t.install.join(t.exe_name()).to_string_lossy()),cwd=q(&t.install.to_string_lossy()),path=q(&u.path.to_string_lossy()),xml=q(&u.text));
    let bytes:Vec<_>=code.encode_utf16().flat_map(u16::to_le_bytes).collect();
    let alphabet=b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";let mut encoded=String::new();
    for part in bytes.chunks(3) {
        let n=(u32::from(part[0])<<16)|(u32::from(*part.get(1).unwrap_or(&0))<<8)|u32::from(*part.get(2).unwrap_or(&0));
        for (i,shift) in [18,12,6,0].into_iter().enumerate() {encoded.push(if i>part.len() {'='}else{alphabet[((n>>shift)&63) as usize] as char});}
    }
    output("service.host.install",Command::new("powershell.exe").args(["-NoProfile","-NonInteractive","-WindowStyle","Hidden","-EncodedCommand",&encoded]),Duration::from_secs(30))
        .map(|_|()).map_err(|error|match error {Failure::Exit(out)=>format!("host task installation failed: {}: {}",out.status,String::from_utf8_lossy(&out.stderr)),other=>other.to_string()})
}

/// Remove an idle legacy unit, from a helper outside that unit's process tree.
/// A failed or incomplete native readback never establishes removal.
pub fn legacy_unit(t: &Tool, project: &str) -> Result<(), String> {
    if project.is_empty() || project == "." || project == ".." || (cfg!(target_os = "macos") && project == "service")
        || project.contains(['/', '\\']) || project.chars().any(char::is_control)
    {
        return Err("invalid legacy project name".into());
    }
    let u = unit(t, &t.install, project);
    remove_legacy(&u, || {
        if !cfg!(target_os = "macos") && !cfg!(windows) {
            run_all(&[sv(&["systemctl", "--user", "daemon-reload"])], false)?;
        }
        let (cmd, name) = if cfg!(target_os = "macos") {
            let uid = output("service.identity", Command::new("id").arg("-u"), Duration::from_secs(30)).map_err(|e| e.to_string())?;
            if !uid.status.success() { return Err("cannot read user id for legacy unit readback".into()); }
            let uid = String::from_utf8(uid.stdout).map_err(|e| e.to_string())?;
            (sv(&["launchctl", "print", &format!("gui/{}", uid.trim())]), format!("com.{}.{project}", t.name))
        } else if cfg!(windows) {
            (sv(&["schtasks", "/Query", "/FO", "CSV", "/NH"]), format!("\\{}-{project}", t.title))
        } else {
            (sv(&["systemctl", "--user", "list-units", "--all", "--plain", "--no-legend", "--no-pager"]), format!("{}-{project}.service", t.name))
        };
        let o = match output("service.readback", Command::new(&cmd[0]).args(&cmd[1..]), Duration::from_secs(30)) {
            Ok(out) | Err(Failure::Exit(out)) => out,
            Err(e) => return Err(e.to_string()),
        };
        if !o.status.success() {
            return Err(format!("legacy unit readback failed: {}", String::from_utf8_lossy(&o.stderr).trim()));
        }
        let text = String::from_utf8(o.stdout).map_err(|e| format!("legacy unit readback: {e}"))?;
        let present = text.lines().any(|line| {
            if cfg!(windows) { line.starts_with(&format!("\"{}\",", name.replace('"', "\"\""))) }
            else { line.split_whitespace().any(|field| field.trim_matches('"') == name) }
        });
        Ok(present)
    })
}

fn remove_legacy(u: &Unit, readback: impl FnOnce() -> Result<bool, String>) -> Result<(), String> {
    run_all(&u.unload, true)?;
    match fs::remove_file(&u.path) {
        Ok(()) => (),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
        Err(e) => return Err(format!("{}: {e}", u.path.display())),
    }
    if readback()? || u.path.try_exists().map_err(|e| e.to_string())? {
        return Err(format!("legacy unit {} still present", u.path.display()));
    }
    Ok(())
}

fn launchd(t: &Tool, root: &Path, project: &str, home: &Path, path_env: &str, uid: &str) -> Unit {
    let label = if project.is_empty() { format!("com.{}.service", t.name) } else { format!("com.{}.{project}", t.name) };
    let path = home.join("Library/LaunchAgents").join(format!("{label}.plist"));
    let log = t.install.join(format!("{}.log", if project.is_empty() { t.name } else { project })).display().to_string();
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
/// RestartOnFailure covers only a failed start; the 5-minute time trigger (IgnoreNew while running) restarts any exit.
fn schtasks(t: &Tool, root: &Path, project: &str, user: &str) -> Unit {
    let name = if project.is_empty() { t.title.to_string() } else { format!("{}-{project}", t.title) };
    let path = t.install.join(format!("{name}.xml"));
    let l = t.install.join(t.exe_name()).display().to_string();
    let text = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<Task version=\"1.2\" xmlns=\"http://schemas.microsoft.com/windows/2004/02/mit/task\">\n<Triggers><LogonTrigger><Enabled>true</Enabled><UserId>{u}</UserId></LogonTrigger><TimeTrigger><Enabled>true</Enabled><StartBoundary>2026-01-01T00:00:00</StartBoundary><Repetition><Interval>PT5M</Interval><StopAtDurationEnd>false</StopAtDurationEnd></Repetition></TimeTrigger></Triggers>\n<Principals><Principal id=\"Author\"><UserId>{u}</UserId><LogonType>InteractiveToken</LogonType><RunLevel>LeastPrivilege</RunLevel></Principal></Principals>\n<Settings><MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy><DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries><StopIfGoingOnBatteries>false</StopIfGoingOnBatteries><ExecutionTimeLimit>PT0S</ExecutionTimeLimit><RestartOnFailure><Interval>PT1M</Interval><Count>999</Count></RestartOnFailure><Hidden>true</Hidden></Settings>\n<Actions Context=\"Author\"><Exec><Command>{l}</Command><Arguments>service run</Arguments><WorkingDirectory>{r}</WorkingDirectory></Exec></Actions>\n</Task>\n",
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
    let name = if project.is_empty() { format!("{}.service", t.name) } else { format!("{}-{project}.service", t.name) };
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
        let o = match output("service.command", Command::new(&c[0]).args(&c[1..]), Duration::from_secs(30)) {
            Ok(out) | Err(Failure::Exit(out)) => out,
            Err(e) => return Err(format!("{}: {e}", c[0])),
        };
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

    // A1: the real installer registers only an owned temporary task; the external fixture checks Limited removal.
    #[cfg(windows)]
    #[test]
    #[ignore="requires COREQ_HOST_INSTALL_FIXTURE; owned Task Scheduler probe, no production task"]
    fn native_host_task_install() {
        let install=PathBuf::from(std::env::var_os("COREQ_HOST_INSTALL_FIXTURE").expect("owned fixture root required"));
        let nonce=install.file_name().unwrap().to_str().unwrap().strip_prefix("coreq-host-install-").unwrap();
        assert_eq!(nonce.len(),32);assert!(nonce.bytes().all(|c|c.is_ascii_hexdigit()));
        let name=Box::leak(format!("coreq-host-probe-{nonce}").into_boxed_str());
        let title=Box::leak(format!("CoreQ-Host-Probe-{nonce}").into_boxed_str());
        let tool=Tool {name,title,repo:"fixture/unused",version:"fixture",install:install.clone(),fault:None};
        let launcher=install.join(tool.exe_name());
        if !launcher.try_exists().unwrap() {
            // A finite OS program rejects service arguments; this fixture proves task management, not daemon health.
            let system=PathBuf::from(std::env::var_os("SystemRoot").unwrap());
            fs::copy(system.join("System32/whoami.exe"),launcher).unwrap();
        }
        if std::env::var("COREQ_HOST_INSTALL_FOREIGN").as_deref()==Ok("1") {
            let path=install.join(format!("{title}.xml"));let before=fs::read(&path).ok();
            assert!(host_unit(&tool).err().unwrap().contains("scope differs"));
            assert_eq!(fs::read(path).ok(),before,"foreign refusal must retain the saved definition too");
        }else {
            let unit=host_unit(&tool).unwrap();assert!(unit.path.is_file());
        }
    }

    // A4 (#86): an owned probe task whose launcher (a long-running fixture exe) is ended by exact pid comes back
    // through the repeating trigger, with no logon. Takes up to 6 minutes.
    #[cfg(windows)]
    #[test]
    #[ignore="requires COREQ_RESTART_FIXTURE with a long-running coreq-restart-probe-<nonce>.exe; owned probe task"]
    fn native_task_restarts_after_exit() {
        let install=PathBuf::from(std::env::var_os("COREQ_RESTART_FIXTURE").expect("owned fixture root required"));
        let nonce=install.file_name().unwrap().to_str().unwrap().strip_prefix("coreq-restart-").unwrap();
        assert_eq!(nonce.len(),32);assert!(nonce.bytes().all(|c|c.is_ascii_hexdigit()));
        let name=Box::leak(format!("coreq-restart-probe-{nonce}").into_boxed_str());
        let title=Box::leak(format!("CoreQ-Restart-Probe-{nonce}").into_boxed_str());
        let tool=Tool {name,title,repo:"fixture/unused",version:"fixture",install:install.clone(),fault:None};
        let pid=|| {
            let query=format!("(Get-CimInstance Win32_Process -Filter \"Name='{name}.exe'\").ProcessId");
            let o=output("test.service-restart",Command::new("powershell.exe").args(["-NoProfile","-NonInteractive","-Command",&query]),Duration::from_secs(30)).unwrap();
            String::from_utf8_lossy(&o.stdout).trim().parse::<u32>().ok()
        };
        let u=unit(&tool,&install,"");write_unit(&u).unwrap();run_all(&u.load,false).unwrap();
        let start=std::time::Instant::now();
        let first=loop {if let Some(p)=pid() {break p} assert!(start.elapsed()<Duration::from_secs(30),"probe never started");std::thread::sleep(Duration::from_secs(1));};
        crate::process::terminate(&crate::process::identity(first).unwrap().unwrap()).unwrap();
        let ended=std::time::Instant::now();
        let back=loop {
            if let Some(p)=pid().filter(|p|*p!=first) {break Some(p)}
            if ended.elapsed()>Duration::from_secs(360) {break None}
            std::thread::sleep(Duration::from_secs(5));
        };
        eprintln!("trace: started pid {first}; ended by exact pid; back {:?} after {} s",back,ended.elapsed().as_secs());
        run_all(&u.unload,true).unwrap();
        assert!(back.is_some(),"not back within 6 minutes");
    }

    #[cfg(windows)]
    #[test]
    fn windows_path_identity_uses_file_ids() {
        let dir=crate::scratch("service-path-identity");let file=dir.join("one");let other=dir.join("other");
        fs::write(&file,"one").unwrap();fs::write(&other,"other").unwrap();
        let alias=file.canonicalize().unwrap();
        let quote=|path:&Path|format!("'{}'",path.to_string_lossy().replace('\'',"''"));
        let code=format!(r#"{}
$file={file};$alias={alias};$other={other};$dir={dir}
if(!(Test-CoreQPath $file $alias)){{throw 'canonical alias differs'}}
if(Test-CoreQPath $file $other){{throw 'different files match'}}
if(Test-CoreQPath $file $dir){{throw 'file and directory match'}}
$missing=$false;try{{[void](Test-CoreQPath $file ($dir+'\missing'))}}catch{{$missing=$true}};if(!$missing){{throw 'missing path accepted'}}
"#,windows_path_identity(),file=quote(&file),alias=quote(&alias),other=quote(&other),dir=quote(&dir));
        let mut command=Command::new("powershell.exe");command.args(["-NoProfile","-NonInteractive","-Command",&code]);
        assert!(output("test.service-path-identity",&mut command,Duration::from_secs(15)).unwrap().status.success());
        fs::remove_dir_all(dir).unwrap();
    }

    // Chain: native command adapter -> ignored exit or launch failure -> durable observed failure.
    #[test]
    fn native_command_ignore_preserves_execution_evidence() {
        use crate::telemetry::{Store, capture::Capture};
        use std::sync::Arc;
        use tracing_subscriber::prelude::*;
        let dir = crate::scratch("service-command");
        let store = Arc::new(Store::open(&dir.join("db")).unwrap());
        let capture = Capture::new(store.clone(), "fixtureq", "1").unwrap();
        tracing::subscriber::with_default(tracing_subscriber::registry().with(capture.clone()), || {
            let cmd = vec![std::env::current_exe().unwrap().to_str().unwrap().to_string(),
                "--exact".into(), "service::tests::failed_native_command".into(), "--ignored".into()];
            assert!(run_all(&[cmd.clone()], false).unwrap_err().contains("failed"));
            assert!(run_all(&[cmd], true).is_ok());
            assert!(run_all(&[vec![dir.join("absent").to_str().unwrap().into()]], true).is_err());
        });
        capture.flush().unwrap();
        let exported = store.export(0, 0, 1024).unwrap();
        let rows: Vec<_> = exported["events"].as_array().unwrap().iter().map(|r| &r["observation"])
            .filter(|r| r["operation"] == "fixtureq.operation.service.command" && r["kind"] == "method").collect();
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().all(|r| r["outcome"] == "failed"));
        assert_eq!(rows.iter().filter(|r| r["error"] == "exit").count(), 2);
        assert_eq!(rows.iter().filter(|r| r["error"] == "spawn").count(), 1);
        assert_eq!(capture.health()["lost"], 0);
        drop(capture); drop(store); fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    #[ignore = "native command failure fixture; invokes no service manager"]
    fn failed_native_command() { std::process::exit(23); }

    // A1: filesystem deletion alone must not report successful legacy removal.
    // Native managers are replaced by readback results; live platform qualification stays separate.
    #[test]
    fn legacy_removal_requires_readback() {
        let d = crate::scratch("legacy-removal");
        let u = Unit { path: d.join("legacy.unit"), text: String::new(), load: vec![], unload: vec![], stop: vec![] };
        fs::write(&u.path, "unit").unwrap();
        assert!(remove_legacy(&u, || {
            assert!(!u.path.exists(), "remove the definition before reading the manager");
            Ok(true)
        }).is_err());
        assert!(remove_legacy(&u, || Err("manager unavailable".into())).is_err());
        remove_legacy(&u, || Ok(false)).unwrap();
        fs::write(&u.path, "unit").unwrap();
        assert!(remove_legacy(&u, || {
            fs::write(&u.path, "reinstalled").unwrap();
            Ok(false)
        }).is_err(), "a concurrent reinstall is not removal");
        for project in ["", ".", "..", "../other", "a/b", "a\\b", "a\n"] {
            assert!(legacy_unit(&crate::tool(&d), project).is_err());
        }
    }

    // A1: host definitions retain the launcher, install cwd and native lifecycle commands on all OSes.
    #[test]
    fn host_units_per_os() {
        let t = crate::tool(Path::new("/i"));
        let home = Path::new("/h");
        let m = launchd(&t, &t.install, "", home, "/bin", "501");
        assert_eq!(m.path, Path::new("/h/Library/LaunchAgents/com.demoq.service.plist"));
        assert!(m.text.contains("<key>WorkingDirectory</key><string>/i</string>"));
        assert!(m.text.contains(&xml(&t.install.join("demoq.log").display().to_string())));
        assert_eq!(m.stop, [sv(&["launchctl", "bootout", "gui/501/com.demoq.service"])]);
        assert_eq!(m.load, [sv(&["launchctl", "bootstrap", "gui/501", &m.path.display().to_string()])]);
        let w = schtasks(&t, &t.install, "", "pc\\user");
        assert_eq!(w.path, Path::new("/i/DemoQ.xml"));
        assert!(w.text.contains("<WorkingDirectory>/i</WorkingDirectory>"));
        assert!(w.text.contains("<Arguments>service run</Arguments>"));
        assert_eq!(w.load[1], sv(&["schtasks", "/Run", "/TN", "DemoQ"]));
        let l = systemd(&t, &t.install, "", home, "/bin");
        assert_eq!(l.path, Path::new("/h/.config/systemd/user/demoq.service"));
        assert!(l.text.contains("WorkingDirectory=/i\n"));
        assert!(l.text.contains(&format!("ExecStart=\"{}\" service run", t.install.join(t.exe_name()).display())));
        assert_eq!(l.load[1], sv(&["systemctl", "--user", "enable", "--now", "demoq.service"]));
    }

    #[test]
    fn units_per_os_name_the_tool() {
        let t = crate::tool(Path::new("/i"));
        let (root, home) = (Path::new("/p"), Path::new("/h"));
        let m = launchd(&t, root, "maps", home, "/bin", "501");
        assert_eq!(m.path, Path::new("/h/Library/LaunchAgents/com.demoq.maps.plist"));
        assert!(m.text.contains("<key>Label</key><string>com.demoq.maps</string>") && m.text.contains("<key>KeepAlive</key><true/>"));
        assert_eq!(m.load, [sv(&["launchctl", "bootstrap", "gui/501", &m.path.display().to_string()])]);
        assert_eq!(m.stop, [sv(&["launchctl", "bootout", "gui/501/com.demoq.maps"])]);
        let w = schtasks(&t, root, "maps", "pc\\a&b");
        assert_eq!(w.path, Path::new("/i/DemoQ-maps.xml"));
        assert!(w.text.contains("<UserId>pc\\a&amp;b</UserId>") && w.text.contains("<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>"));
        // #86: RestartOnFailure misses a silent exit; the repeating trigger restarts it and never doubles a live one.
        assert!(w.text.contains("<TimeTrigger><Enabled>true</Enabled><StartBoundary>2026-01-01T00:00:00</StartBoundary><Repetition><Interval>PT5M</Interval><StopAtDurationEnd>false</StopAtDurationEnd></Repetition></TimeTrigger>"));
        assert!(w.text.contains("<MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>") && !w.text.contains("<Duration>"));
        assert_eq!(w.unload, [sv(&["schtasks", "/End", "/TN", "DemoQ-maps"]), sv(&["schtasks", "/Delete", "/TN", "DemoQ-maps", "/F"])]);
        let s = systemd(&t, root, "maps", home, "/bin");
        assert_eq!(s.path, Path::new("/h/.config/systemd/user/demoq-maps.service"));
        assert!(s.text.contains(&format!("ExecStart=\"{}\"", t.install.join(t.exe_name()).display())) && s.text.contains("Restart=always"));
        assert_eq!(s.load[1], sv(&["systemctl", "--user", "enable", "--now", "demoq-maps.service"]));
    }
}
