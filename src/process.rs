//! Owned, bounded subprocess exchange; payloads never enter telemetry.
use std::{
    fmt, io::{self, Read, Write}, process::{Child, Command, Output, Stdio},
    sync::{Mutex, OnceLock, atomic::{AtomicUsize, Ordering}, mpsc::{self, SyncSender}},
    time::{Duration, Instant},
};

const CAPACITY: usize = 16;
const OUTPUT_LIMIT: usize = 1024 * 1024;
const INPUT_LIMIT: usize = 1024 * 1024;
const RESPONSE_LIMIT: usize = 8 * 1024 * 1024;
type Input = std::thread::JoinHandle<io::Result<()>>;
static ACTIVE: AtomicUsize = AtomicUsize::new(0);
static REAPER: OnceLock<Result<SyncSender<Owned>, String>> = OnceLock::new();
static UNREAPED: Mutex<Vec<(Child, Option<Input>, Permit)>> = Mutex::new(Vec::new());

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity { pub pid: u32, pub started: String }

/// Native creation identity; a missing process is distinct from an unreadable one.
pub fn identity(pid: u32) -> io::Result<Option<Identity>> {
    crate::telemetry::capture::operation("process.identity", || {
        if pid == 0 || pid > i32::MAX as u32 { return Err(io::Error::new(io::ErrorKind::InvalidInput,"invalid process id")); }
        identity_inner(pid).map(|started| started.map(|started| Identity {pid,started}))
    }, |_| "process-identity")
}

/// Request termination of an already-authorized execution, never signal a reused PID or infer exit.
pub fn terminate(expected:&Identity)->io::Result<()> {
    crate::telemetry::capture::operation("process.terminate", || terminate_inner(expected), |_|"process-termination")
}

#[cfg(target_os="macos")]
fn mac_pid_version(pid:u32)->io::Result<u32> {
    #[repr(C)]
    #[derive(Default)]
    struct Unique { uuid:[u8;16], id:u64, parent:u64, version:i32, parent_version:i32, reserved:[u64;2] }
    const _:()=assert!(std::mem::size_of::<Unique>()==56);
    if pid==0 || pid>i32::MAX as u32 { return Err(io::Error::new(io::ErrorKind::InvalidInput,"invalid process id")); }
    let mut info=Unique::default();
    // XNU PROC_PIDUNIQIDENTIFIERINFO (17), proc_info_private.h; reject incompatible native layouts.
    let read=unsafe { libc::proc_pidinfo(pid as i32,17,0,(&mut info as *mut Unique).cast(),56) };
    if read==0 { return Err(io::Error::last_os_error()); }
    if read!=56 || info.id==0 { return Err(io::Error::other("incomplete native process version")); }
    Ok(info.version as u32)
}

#[cfg(target_os="macos")]
fn mac_signal(pid:u32,version:u32)->io::Result<()> {
    // Resolve at runtime so older hosts refuse the capability instead of failing to load the service.
    let symbol=unsafe { libc::dlsym(libc::RTLD_DEFAULT,c"proc_signal_with_audittoken".as_ptr()) };
    if symbol.is_null() { return Err(io::Error::new(io::ErrorKind::Unsupported,"native identity-bound signal unavailable")); }
    // libproc.h signature and mach audit_token_t ABI; XNU validates PID/version before signaling.
    let signal:unsafe extern "C" fn(*mut [u32;8],i32)->i32=unsafe { std::mem::transmute(symbol) };
    let mut token=[0u32;8];token[5]=pid;token[7]=version;
    let error=unsafe { signal(&mut token,libc::SIGKILL) };
    if error==0 { Ok(()) } else { Err(io::Error::from_raw_os_error(error)) }
}

#[cfg(target_os="macos")]
fn terminate_inner(expected:&Identity)->io::Result<()> {
    let version=mac_pid_version(expected.pid)?;
    if identity(expected.pid)?.as_ref()!=Some(expected) {
        return Err(io::Error::new(io::ErrorKind::NotFound,"recorded process incarnation no longer present"));
    }
    // Version is captured before identity validation; reuse or exec afterward fails inside the kernel.
    mac_signal(expected.pid,version)
}

#[cfg(not(target_os="macos"))]
fn terminate_inner(_expected:&Identity)->io::Result<()> {
    Err(io::Error::new(io::ErrorKind::Unsupported,"native identity-bound termination unsupported"))
}

#[cfg(unix)]
fn missing_identity(pid:u32)->io::Result<Option<String>> {
    // A hidden or zombie process can be absent from the identity API while its PID still exists.
    if unsafe { libc::kill(pid as i32,0) } != 0 && io::Error::last_os_error().raw_os_error()==Some(libc::ESRCH) { Ok(None) }
    else { Err(io::Error::other("creation identity unavailable for an existing process")) }
}

#[cfg(target_os="macos")]
fn identity_inner(pid: u32) -> io::Result<Option<String>> {
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::zeroed();
    let size = std::mem::size_of::<libc::proc_bsdinfo>() as i32;
    // SAFETY: native API writes at most size bytes into the correctly sized buffer.
    let read = unsafe { libc::proc_pidinfo(pid as i32,libc::PROC_PIDTBSDINFO,0,info.as_mut_ptr().cast(),size) };
    if read == 0 {
        let e=io::Error::last_os_error();
        return if e.raw_os_error() == Some(libc::ESRCH) { missing_identity(pid) } else { Err(e) };
    }
    if read != size { return Err(io::Error::other("incomplete process identity")); }
    // SAFETY: proc_pidinfo returned the entire initialized structure.
    let info=unsafe { info.assume_init() };
    if info.pbi_pid != pid || info.pbi_start_tvsec == 0 || info.pbi_start_tvusec >= 1_000_000 { return Err(io::Error::other("invalid native process identity")); }
    Ok(Some(format!("{}-{}",info.pbi_start_tvsec,info.pbi_start_tvusec)))
}

#[cfg(target_os="linux")]
fn identity_inner(pid: u32) -> io::Result<Option<String>> {
    let stat=match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(s)=>s, Err(e) if e.kind()==io::ErrorKind::NotFound=>return missing_identity(pid), Err(e)=>return Err(e),
    };
    let ticks=stat.rsplit_once(')').and_then(|(_,tail)|tail.split_whitespace().nth(19))
        .and_then(|s|s.parse::<u64>().ok()).ok_or_else(||io::Error::other("invalid native process start ticks"))?;
    let boot=std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
    let boot=boot.trim();
    if boot.len()!=36 || !boot.bytes().all(|b|b.is_ascii_hexdigit() || b==b'-') { return Err(io::Error::other("invalid native boot identity")); }
    Ok(Some(format!("{boot}-{ticks}")))
}

#[cfg(windows)]
fn identity_inner(pid: u32) -> io::Result<Option<String>> {
    #[repr(C)]
    #[derive(Default)]
    struct FileTime { low: u32, high: u32 }
    #[link(name="kernel32")]
    unsafe extern "system" {
        fn OpenProcess(access:u32,inherit:i32,pid:u32)->*mut std::ffi::c_void;
        fn GetProcessTimes(handle:*mut std::ffi::c_void,created:*mut FileTime,exit:*mut FileTime,kernel:*mut FileTime,user:*mut FileTime)->i32;
        fn CloseHandle(handle:*mut std::ffi::c_void)->i32;
    }
    // SAFETY: handle and all four output buffers stay valid through the query; the handle always closes.
    unsafe {
        let handle=OpenProcess(0x1000,0,pid);
        if handle.is_null() {
            let e=io::Error::last_os_error();
            return if e.raw_os_error()==Some(87) { Ok(None) } else { Err(e) };
        }
        let (mut created,mut exit,mut kernel,mut user)=(FileTime::default(),FileTime::default(),FileTime::default(),FileTime::default());
        let read=GetProcessTimes(handle,&mut created,&mut exit,&mut kernel,&mut user);
        let error=io::Error::last_os_error(); CloseHandle(handle);
        if read==0 { return Err(error); }
        let started=((created.high as u64)<<32)|created.low as u64;
        if started==0 { return Err(io::Error::other("invalid native process creation time")); }
        Ok(Some(started.to_string()))
    }
}

#[cfg(not(any(target_os="macos",target_os="linux",windows)))]
fn identity_inner(_pid:u32)->io::Result<Option<String>> { Err(io::Error::new(io::ErrorKind::Unsupported,"native process identity unsupported")) }

#[derive(Debug)]
pub enum Failure {
    InvalidDeadline, Capacity, Owner(String), Spawn(io::Error), Wait(io::Error), Read(io::Error),
    Timeout, InputLimit, OutputLimit, Write(io::Error), Exit(Output),
}
impl Failure {
    pub fn class(&self) -> &'static str {
        match self {
            Self::InvalidDeadline => "deadline-invalid", Self::Capacity => "capacity", Self::Owner(_) => "cleanup-owner",
            Self::Spawn(_) => "spawn", Self::Wait(_) => "wait", Self::Read(_) => "output-read",
            Self::Timeout => "timeout", Self::InputLimit => "input-limit", Self::OutputLimit => "output-limit",
            Self::Write(_) => "input-write", Self::Exit(_) => "exit",
        }
    }
}
impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Spawn(e) | Self::Wait(e) | Self::Read(e) | Self::Write(e) => write!(f, "process {}: {e}", self.class()),
            Self::Owner(e) => write!(f, "process cleanup owner: {e}"),
            Self::Exit(out) => write!(f, "process exited with {}", out.status),
            _ => write!(f, "process {}", self.class()),
        }
    }
}
impl std::error::Error for Failure {}

struct Permit;
impl Permit {
    fn acquire() -> Result<Self, Failure> {
        ACTIVE.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| (n < CAPACITY).then_some(n + 1))
            .map(|_| Self).map_err(|_| Failure::Capacity)
    }
}
impl Drop for Permit { fn drop(&mut self) { ACTIVE.fetch_sub(1, Ordering::Relaxed); } }

fn finish_input(input: &mut Option<Input>) -> io::Result<bool> {
    if input.as_ref().is_some_and(|h| !h.is_finished()) { return Ok(false); }
    if let Some(input) = input.take() {
        input.join().map_err(|_| io::Error::other("input writer panicked"))??;
    }
    Ok(true)
}

struct Owned { child: Child, input: Option<Input>, span: tracing::Span, _permit: Permit }
fn reaper() -> Result<&'static SyncSender<Owned>, Failure> {
    REAPER.get_or_init(|| {
        let (tx, rx) = mpsc::sync_channel::<Owned>(CAPACITY);
        std::thread::Builder::new().name("coreq-process-cleanup".into()).spawn(move || {
            let mut pending: Vec<Owned> = Vec::new();
            loop {
                let item = if pending.is_empty() { rx.recv().ok() } else { rx.recv_timeout(Duration::from_millis(20)).ok() };
                if let Some(item) = item { pending.push(item); }
                pending.retain_mut(|item| {
                    let exited = match item.child.try_wait() {
                        Ok(status) => status.is_some(),
                        Err(_) => { item.span.record("coreq_error", "cleanup-wait"); false }
                    };
                    let written = finish_input(&mut item.input).unwrap_or_else(|_| { item.span.record("coreq_error", "cleanup-input"); true });
                    if exited && written { item.span.record("coreq_result", "observed"); false } else { true }
                });
            }
        }).map(|_| tx).map_err(|e| e.to_string())
    }).as_ref().map_err(|e| Failure::Owner(e.clone()))
}

struct Guard { child: Option<Child>, input: Option<Input>, permit: Option<Permit>, reaper: &'static SyncSender<Owned> }
impl Guard {
    fn child(&mut self) -> &mut Child { self.child.as_mut().unwrap() }
}
impl Drop for Guard {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else { return };
        let wait = child.try_wait();
        if matches!(wait, Ok(Some(_))) && self.input.is_none() { return; }
        let span = crate::telemetry::capture::detached_span("process.cleanup");
        span.record("process_id", u64::from(child.id()));
        if wait.is_err() { span.record("coreq_error", "cleanup-wait"); }
        if !matches!(wait, Ok(Some(_))) && child.kill().is_err() && !matches!(child.try_wait(), Ok(Some(_))) {
            span.record("coreq_error", "cleanup-kill");
        }
        let owned = Owned { child, input: self.input.take(), span, _permit: self.permit.take().unwrap() };
        // Every queued child retains a permit, so this bounded queue has room for this owner.
        if let Err(error) = self.reaper.send(owned) {
            error.0.span.record("coreq_error", "cleanup-owner");
            eprintln!("Process cleanup: unknown (owner unavailable)");
            // Fail closed: retain the handle and capacity until process exit if the owner dies.
            let Owned { child, input, span, _permit } = error.0;
            drop(span);
            UNREAPED.lock().unwrap_or_else(|e| e.into_inner()).push((child, input, _permit));
        }
    }
}

/// Success requires an observed zero exit and complete bounded output; cleanup never joins pipe readers.
pub fn output(name: &'static str, command: &mut Command, budget: Duration) -> Result<Output, Failure> {
    execute(name, command, None, budget, OUTPUT_LIMIT)
}

/// Bounded full-duplex exchange; input stays on stdin and its writer remains owned through cleanup.
pub fn exchange(name: &'static str, command: &mut Command, input: &[u8], budget: Duration) -> Result<Output, Failure> {
    execute(name, command, Some(input), budget, RESPONSE_LIMIT)
}

fn execute(name: &'static str, command: &mut Command, input: Option<&[u8]>, budget: Duration, output_limit: usize) -> Result<Output, Failure> {
    crate::telemetry::capture::operation(name, || {
        if input.is_some_and(|v| v.len() > INPUT_LIMIT) { return Err(Failure::InputLimit); }
        if budget.is_zero() || budget > Duration::from_secs(3600) { return Err(Failure::InvalidDeadline); }
        let permit = Permit::acquire()?;
        let owner = reaper()?;
        let until = Instant::now() + budget;
        let input = input.filter(|v| !v.is_empty());
        crate::telemetry::capture::inherit_context(command);
        let mut child = crate::quiet(command).stdin(if input.is_some() { Stdio::piped() } else { Stdio::null() })
            .stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().map_err(Failure::Spawn)?;
        let stdout = child.stdout.take().unwrap(); let stderr = child.stderr.take().unwrap();
        let mut guard = Guard { child: Some(child), input: None, permit: Some(permit), reaper: owner };
        if let Some(input) = input {
            let input = input.to_vec();
            let mut stdin = guard.child().stdin.take().unwrap();
            guard.input = Some(std::thread::Builder::new().name("coreq-process-input".into())
                .spawn(move || stdin.write_all(&input)).map_err(Failure::Write)?);
        }
        let mut stdout = Pipe::new(stdout).map_err(Failure::Read)?;
        let mut stderr = Pipe::new(stderr).map_err(Failure::Read)?;
        let mut status = None;
        loop {
            if Instant::now() >= until { return Err(Failure::Timeout); }
            if status.is_none() { status = guard.child().try_wait().map_err(Failure::Wait)?; }
            let written = finish_input(&mut guard.input).map_err(Failure::Write)?;
            let mut progress = stdout.read(output_limit - stderr.bytes.len())?;
            progress |= stderr.read(output_limit - stdout.bytes.len())?;
            if Instant::now() >= until { return Err(Failure::Timeout); }
            if let Some(status) = status.filter(|_| written && stdout.eof && stderr.eof) {
                let out = Output { status, stdout: stdout.bytes, stderr: stderr.bytes };
                return if status.success() { Ok(out) } else { Err(Failure::Exit(out)) };
            }
            if !progress { std::thread::sleep(Duration::from_millis(5).min(until.saturating_duration_since(Instant::now()))); }
        }
    }, Failure::class)
}

#[cfg(test)]
mod tests {
    // Chain: recorded identity -> kernel version fence -> stop request -> native exit.
    // Risk: PID reuse between validation and signal kills another incarnation.
    #[cfg(target_os="macos")]
    #[test]
    fn native_termination_checks_creation_identity_and_kernel_pid_version() {
        let mut child=fixture("hang").stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap();
        let id=identity(child.id()).unwrap().unwrap();
        let version=mac_pid_version(id.pid).unwrap();
        let mut wrong=id.clone();wrong.started.push_str("-wrong");
        assert_eq!(terminate(&wrong).unwrap_err().kind(),io::ErrorKind::NotFound);
        assert_eq!(mac_signal(id.pid,version.wrapping_add(1)).unwrap_err().raw_os_error(),Some(libc::ESRCH));
        assert!(child.try_wait().unwrap().is_none(),"rejected identity/version cannot touch the current process");
        terminate(&id).unwrap();
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(child.wait().unwrap().signal(),Some(libc::SIGKILL));
        assert_eq!(identity(id.pid).unwrap(),None);
        assert!(terminate(&id).is_err(),"request success never fabricates exit evidence");
    }
    // Chain: live native child -> recorded creation identity -> observed exit. Risk: PID alone is treated as ownership.
    #[test]
    fn native_identity_survives_reads_and_distinguishes_processes() {
        let this=super::identity(std::process::id()).unwrap().unwrap();
        assert_eq!(super::identity(this.pid).unwrap(),Some(this.clone()));
        assert!(super::identity(0).is_err());
        let mut child=fixture("hang").stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap();
        let id=super::identity(child.id()).unwrap().unwrap();
        assert_ne!(id,this);
        assert_eq!(super::identity(id.pid).unwrap(),Some(id.clone()));
        child.kill().unwrap(); child.wait().unwrap(); drop(child);
        assert_eq!(super::identity(id.pid).unwrap(),None);
    }

    use super::*;
    use crate::telemetry::{Store, capture::Capture};
    use std::sync::Arc;
    use tracing_subscriber::prelude::*;

    fn fixture(mode: &str) -> Command {
        let mut cmd = Command::new(std::env::current_exe().unwrap());
        cmd.args(["--exact", "process::tests::command_child", "--ignored", "--nocapture"])
            .env("COREQ_PROCESS_FIXTURE", mode);
        cmd
    }

    // Chain: real dependency process -> bounded complete output or failure -> owned cleanup and causal history.
    #[test]
    fn bounded_commands_keep_outcomes_and_own_pipe_lifetimes() {
        let dir = crate::scratch("process-output");
        let store = Arc::new(Store::open(&dir.join("db")).unwrap());
        let capture = Capture::new(store.clone(), "fixtureq", "1").unwrap();
        tracing::subscriber::with_default(tracing_subscriber::registry().with(capture.clone()), || {
            let _parent = tracing::info_span!("command").entered();
            let out = output("both", &mut fixture("both"), Duration::from_secs(5)).unwrap();
            assert!(out.stdout.len() >= 80_000); assert_eq!(out.stderr.len(), 80_000);
            let Err(Failure::Exit(out)) = output("exit", &mut fixture("exit"), Duration::from_secs(5)) else { panic!("nonzero exit lost") };
            assert_eq!(out.status.code(), Some(23)); assert!(String::from_utf8_lossy(&out.stderr).contains("private-error"));
            assert!(matches!(output("missing", &mut Command::new(dir.join("absent")), Duration::from_secs(1)), Err(Failure::Spawn(_))));
            let began = Instant::now();
            assert!(matches!(output("hang", &mut fixture("hang"), Duration::from_millis(100)), Err(Failure::Timeout)));
            assert!(began.elapsed() < Duration::from_secs(2));
            assert!(matches!(output("flood", &mut fixture("flood"), Duration::from_secs(5)), Err(Failure::OutputLimit)));
            let began = Instant::now();
            assert!(matches!(output("inherited", &mut fixture("inherited"), Duration::from_millis(500)), Err(Failure::Timeout)));
            assert!(began.elapsed() < Duration::from_secs(2));
            assert!(matches!(output("invalid", &mut fixture("both"), Duration::ZERO), Err(Failure::InvalidDeadline)));
            output("capacity", &mut fixture("capacity"), Duration::from_secs(5)).unwrap();
            output("owner", &mut fixture("owner"), Duration::from_secs(5)).unwrap();
            let input = b"private-input".repeat(10_000);
            let duplex = exchange("duplex", &mut fixture("duplex"), &input, Duration::from_secs(15)).unwrap();
            assert!(duplex.stdout.len() >= 2 * OUTPUT_LIMIT);
            assert!(matches!(exchange("too-large", &mut Command::new(dir.join("absent")), &vec![0; INPUT_LIMIT + 1], Duration::from_secs(1)), Err(Failure::InputLimit)));
            assert!(matches!(exchange("input-failed", &mut fixture("exit-ok"), &vec![0; INPUT_LIMIT], Duration::from_secs(5)), Err(Failure::Write(_))));
            output("input-lifetime", &mut fixture("input-lifetime"), Duration::from_secs(8)).unwrap();
        });
        let until = Instant::now() + Duration::from_secs(3);
        let rows = loop {
            capture.flush().unwrap();
            let exported = store.export(0, 0, 1024).unwrap();
            let rows: Vec<_> = exported["events"].as_array().unwrap().iter().map(|r| r["observation"].clone()).collect();
            let cleanups: Vec<_> = rows.iter().filter(|r| r["operation"] == "fixtureq.operation.process.cleanup").collect();
            let finished: Vec<_> = cleanups.iter().copied().filter(|r| r["kind"] == "method").collect();
            let hang = rows.iter().find(|r| r["operation"] == "fixtureq.operation.hang" && r["kind"] == "method");
            // Fast exits can be reaped in Guard::drop without entering asynchronous cleanup.
            if hang.is_some_and(|hang| finished.iter().any(|r| r["parent"] == hang["span"]))
                && cleanups.iter().filter(|r| r["kind"] == "method.start").all(|start| finished.iter().any(|r| r["span"] == start["span"]))
                && rows.iter().any(|r| r["operation"] == "fixtureq.command" && r["kind"] == "method") { break rows; }
            assert!(Instant::now() < until, "direct subprocess cleanup did not finish: active={} rows={:?}", ACTIVE.load(Ordering::Relaxed), rows.iter().filter(|r| r["operation"]=="fixtureq.operation.process.cleanup").collect::<Vec<_>>());
            std::thread::sleep(Duration::from_millis(20));
        };
        let parent = rows.iter().find(|r| r["operation"] == "fixtureq.command" && r["kind"] == "method").unwrap();
        assert_eq!(parent["outcome"], "ok", "handled dependency errors poisoned the command");
        for (name, class) in [("both", "none"), ("exit", "exit"), ("missing", "spawn"), ("hang", "timeout"), ("flood", "output-limit"), ("inherited", "timeout"), ("invalid", "deadline-invalid"), ("capacity", "none"), ("duplex", "none"), ("too-large", "input-limit"), ("input-failed", "input-write"), ("input-lifetime", "none")] {
            let row = rows.iter().find(|r| r["operation"] == format!("fixtureq.operation.{name}") && r["kind"] == "method").unwrap();
            assert_eq!(row["error"], class); assert_eq!(row["parent"], parent["span"]); assert_eq!(row["trace"], parent["trace"]);
        }
        assert!(!serde_json::to_string(&rows).unwrap().contains("private-error"));
        assert!(!serde_json::to_string(&rows).unwrap().contains("private-input"));
        assert!(!serde_json::to_string(&rows).unwrap().contains(dir.to_str().unwrap()));
        assert_eq!(capture.health()["lost"], 0);
        drop(capture); drop(store); std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    #[ignore = "subprocess fixture for bounded dependency execution"]
    fn command_child() {
        use std::io::Write;
        match std::env::var("COREQ_PROCESS_FIXTURE").unwrap().as_str() {
            "both" => {
                std::io::stdout().write_all(&vec![b'a'; 80_000]).unwrap();
                std::io::stderr().write_all(&vec![b'b'; 80_000]).unwrap();
            }
            "exit" => { eprintln!("private-error"); std::process::exit(23); }
            "exit-ok" => std::process::exit(0),
            "hang" => std::thread::sleep(Duration::from_secs(5)),
            "flood" => std::io::stdout().write_all(&vec![b'x'; OUTPUT_LIMIT * 2]).unwrap(),
            "inherited" => {
                fixture("linger").stdin(Stdio::null()).stdout(Stdio::inherit()).stderr(Stdio::inherit()).spawn().unwrap();
            }
            "linger" => std::thread::sleep(Duration::from_millis(1500)),
            "duplex" => {
                std::io::stdout().write_all(&vec![b'a'; 2 * OUTPUT_LIMIT]).unwrap();
                let mut input = Vec::new();
                std::io::stdin().read_to_end(&mut input).unwrap();
                assert_eq!(input, b"private-input".repeat(10_000));
            }
            "input-inherited" => {
                fixture("input-linger").stdin(Stdio::inherit()).stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap();
            }
            "input-linger" => std::thread::sleep(Duration::from_secs(3)),
            "input-lifetime" => {
                let started = Instant::now();
                assert!(matches!(exchange("blocked-input", &mut fixture("input-inherited"), &vec![0; INPUT_LIMIT], Duration::from_secs(1)), Err(Failure::Timeout)));
                assert!(started.elapsed() < Duration::from_secs(2));
                assert_eq!(ACTIVE.load(Ordering::Relaxed), 1, "pending input lost its execution permit");
                let until = Instant::now() + Duration::from_secs(4);
                while ACTIVE.load(Ordering::Relaxed) != 0 {
                    assert!(Instant::now() < until, "input cleanup did not retire");
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
            "capacity" => {
                let mut permits: Vec<_> = (0..CAPACITY).map(|_| Permit::acquire().unwrap()).collect();
                assert!(matches!(output("full", &mut fixture("both"), Duration::from_secs(5)), Err(Failure::Capacity)));
                permits.pop();
                output("released", &mut fixture("both"), Duration::from_secs(5)).unwrap();
            }
            "owner" => {
                let (tx, rx) = mpsc::sync_channel(CAPACITY);
                drop(rx);
                let child = fixture("hang").stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap();
                drop(Guard { child: Some(child), input: None, permit: Some(Permit::acquire().unwrap()), reaper: Box::leak(Box::new(tx)) });
                let mut retained = UNREAPED.lock().unwrap();
                assert_eq!(retained.len(), 1);
                assert_eq!(ACTIVE.load(Ordering::Relaxed), 1);
                retained[0].0.wait().unwrap();
                retained.clear();
                assert_eq!(ACTIVE.load(Ordering::Relaxed), 0);
            }
            other => panic!("unknown fixture {other}"),
        }
    }
}

struct Pipe<T> { reader: T, bytes: Vec<u8>, eof: bool }
impl<T: platform::Handle> Pipe<T> {
    fn new(reader: T) -> io::Result<Self> {
        platform::prepare(&reader)?;
        Ok(Self { reader, bytes: Vec::new(), eof: false })
    }
    fn read(&mut self, limit: usize) -> Result<bool, Failure> {
        if self.eof { return Ok(false); }
        let mut buf = [0; 8192];
        match platform::read(&mut self.reader, &mut buf) {
            Ok(Some(0)) => { self.eof = true; Ok(true) }
            Ok(Some(n)) => {
                if self.bytes.len() + n > limit { return Err(Failure::OutputLimit); }
                self.bytes.extend_from_slice(&buf[..n]); Ok(true)
            }
            Ok(None) => Ok(false),
            Err(e) => Err(Failure::Read(e)),
        }
    }
}

#[cfg(unix)]
mod platform {
    use super::*;
    use std::os::fd::AsRawFd;
    pub trait Handle: Read + AsRawFd {}
    impl<T: Read + AsRawFd> Handle for T {}
    pub fn prepare(pipe: &impl Handle) -> io::Result<()> {
        // SAFETY: this owner keeps the pipe descriptor live; only its read end becomes nonblocking.
        let flags = unsafe { libc::fcntl(pipe.as_raw_fd(), libc::F_GETFL) };
        if flags < 0 || unsafe { libc::fcntl(pipe.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 { return Err(io::Error::last_os_error()); }
        Ok(())
    }
    pub fn read(pipe: &mut impl Handle, buf: &mut [u8]) -> io::Result<Option<usize>> {
        match pipe.read(buf) {
            Ok(n) => Ok(Some(n)),
            Err(e) if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[cfg(windows)]
mod platform {
    use super::*;
    use std::{ffi::c_void, os::windows::io::AsRawHandle};
    pub trait Handle: Read + AsRawHandle {}
    impl<T: Read + AsRawHandle> Handle for T {}
    #[link(name="kernel32")]
    unsafe extern "system" {
        fn PeekNamedPipe(pipe: *mut c_void, buffer: *mut c_void, size: u32, read: *mut u32, available: *mut u32, left: *mut u32) -> i32;
    }
    pub fn prepare(_: &impl Handle) -> io::Result<()> { Ok(()) }
    pub fn read(pipe: &mut impl Handle, buf: &mut [u8]) -> io::Result<Option<usize>> {
        let mut available = 0;
        // SAFETY: the read handle and output count stay live; no buffer is passed to the peek.
        if unsafe { PeekNamedPipe(pipe.as_raw_handle(), std::ptr::null_mut(), 0, std::ptr::null_mut(), &mut available, std::ptr::null_mut()) } == 0 {
            let error = io::Error::last_os_error();
            return if matches!(error.raw_os_error(), Some(109 | 232)) { Ok(Some(0)) } else { Err(error) };
        }
        if available == 0 { return Ok(None); }
        let n = buf.len().min(available as usize);
        pipe.read(&mut buf[..n]).map(Some)
    }
}
