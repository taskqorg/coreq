//! Owned, bounded output capture for short dependency commands; never records their payloads.
use std::{
    fmt, io::{self, Read}, process::{Child, Command, Output, Stdio},
    sync::{Mutex, OnceLock, atomic::{AtomicUsize, Ordering}, mpsc::{self, SyncSender}},
    time::{Duration, Instant},
};

const CAPACITY: usize = 16;
const OUTPUT_LIMIT: usize = 1024 * 1024;
static ACTIVE: AtomicUsize = AtomicUsize::new(0);
static REAPER: OnceLock<Result<SyncSender<Owned>, String>> = OnceLock::new();
static UNREAPED: Mutex<Vec<(Child, Permit)>> = Mutex::new(Vec::new());

#[derive(Debug)]
pub enum Failure {
    InvalidDeadline, Capacity, Owner(String), Spawn(io::Error), Wait(io::Error), Read(io::Error),
    Timeout, OutputLimit, Exit(Output),
}
impl Failure {
    pub fn class(&self) -> &'static str {
        match self {
            Self::InvalidDeadline => "deadline-invalid", Self::Capacity => "capacity", Self::Owner(_) => "cleanup-owner",
            Self::Spawn(_) => "spawn", Self::Wait(_) => "wait", Self::Read(_) => "output-read",
            Self::Timeout => "timeout", Self::OutputLimit => "output-limit", Self::Exit(_) => "exit",
        }
    }
}
impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Spawn(e) | Self::Wait(e) | Self::Read(e) => write!(f, "process {}: {e}", self.class()),
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

struct Owned { child: Child, span: tracing::Span, _permit: Permit }
fn reaper() -> Result<&'static SyncSender<Owned>, Failure> {
    REAPER.get_or_init(|| {
        let (tx, rx) = mpsc::sync_channel::<Owned>(CAPACITY);
        std::thread::Builder::new().name("coreq-process-cleanup".into()).spawn(move || {
            let mut pending: Vec<Owned> = Vec::new();
            loop {
                let item = if pending.is_empty() { rx.recv().ok() } else { rx.recv_timeout(Duration::from_millis(20)).ok() };
                if let Some(item) = item { pending.push(item); }
                pending.retain_mut(|item| match item.child.try_wait() {
                    Ok(Some(_)) => { item.span.record("coreq_result", "observed"); false }
                    Ok(None) => true,
                    Err(_) => { item.span.record("coreq_error", "cleanup-wait"); true }
                });
            }
        }).map(|_| tx).map_err(|e| e.to_string())
    }).as_ref().map_err(|e| Failure::Owner(e.clone()))
}

struct Guard { child: Option<Child>, permit: Option<Permit>, reaper: &'static SyncSender<Owned> }
impl Guard {
    fn child(&mut self) -> &mut Child { self.child.as_mut().unwrap() }
}
impl Drop for Guard {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else { return };
        let wait = child.try_wait();
        if matches!(wait, Ok(Some(_))) { return; }
        let span = crate::telemetry::capture::detached_span("process.cleanup");
        span.record("process_id", u64::from(child.id()));
        if wait.is_err() { span.record("coreq_error", "cleanup-wait"); }
        if child.kill().is_err() {
            if matches!(child.try_wait(), Ok(Some(_))) { span.record("coreq_result", "observed"); return; }
            span.record("coreq_error", "cleanup-kill");
        }
        let owned = Owned { child, span, _permit: self.permit.take().unwrap() };
        // Every queued child retains a permit, so this bounded queue has room for this owner.
        if let Err(error) = self.reaper.send(owned) {
            error.0.span.record("coreq_error", "cleanup-owner");
            eprintln!("Process cleanup: unknown (owner unavailable)");
            // Fail closed: retain the handle and capacity until process exit if the owner dies.
            let Owned { child, span, _permit } = error.0;
            drop(span);
            UNREAPED.lock().unwrap_or_else(|e| e.into_inner()).push((child, _permit));
        }
    }
}

/// Success requires an observed zero exit and complete bounded output; cleanup never joins pipe readers.
pub fn output(name: &'static str, command: &mut Command, budget: Duration) -> Result<Output, Failure> {
    crate::telemetry::capture::operation(name, || {
        if budget.is_zero() || budget > Duration::from_secs(3600) { return Err(Failure::InvalidDeadline); }
        let permit = Permit::acquire()?;
        let owner = reaper()?;
        let until = Instant::now() + budget;
        let mut child = crate::quiet(command).stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().map_err(Failure::Spawn)?;
        let stdout = child.stdout.take().unwrap(); let stderr = child.stderr.take().unwrap();
        let mut guard = Guard { child: Some(child), permit: Some(permit), reaper: owner };
        let mut stdout = Pipe::new(stdout).map_err(Failure::Read)?;
        let mut stderr = Pipe::new(stderr).map_err(Failure::Read)?;
        let mut status = None;
        loop {
            if Instant::now() >= until { return Err(Failure::Timeout); }
            if status.is_none() { status = guard.child().try_wait().map_err(Failure::Wait)?; }
            let mut progress = stdout.read(OUTPUT_LIMIT - stderr.bytes.len())?;
            progress |= stderr.read(OUTPUT_LIMIT - stdout.bytes.len())?;
            if Instant::now() >= until { return Err(Failure::Timeout); }
            if let Some(status) = status.filter(|_| stdout.eof && stderr.eof) {
                let out = Output { status, stdout: stdout.bytes, stderr: stderr.bytes };
                return if status.success() { Ok(out) } else { Err(Failure::Exit(out)) };
            }
            if !progress { std::thread::sleep(Duration::from_millis(5).min(until.saturating_duration_since(Instant::now()))); }
        }
    }, Failure::class)
}

#[cfg(test)]
mod tests {
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
    fn bounded_commands_keep_outcomes_and_reap_without_pipe_threads() {
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
        });
        let until = Instant::now() + Duration::from_secs(3);
        let rows = loop {
            capture.flush().unwrap();
            let exported = store.export(0, 0, 1024).unwrap();
            let rows: Vec<_> = exported["events"].as_array().unwrap().iter().map(|r| r["observation"].clone()).collect();
            if rows.iter().filter(|r| r["operation"] == "fixtureq.operation.process.cleanup" && r["kind"] == "method").count() >= 2
                && rows.iter().any(|r| r["operation"] == "fixtureq.command" && r["kind"] == "method") { break rows; }
            assert!(Instant::now() < until, "direct subprocess cleanup did not finish");
            std::thread::sleep(Duration::from_millis(20));
        };
        let parent = rows.iter().find(|r| r["operation"] == "fixtureq.command" && r["kind"] == "method").unwrap();
        assert_eq!(parent["outcome"], "ok", "handled dependency errors poisoned the command");
        for (name, class) in [("both", "none"), ("exit", "exit"), ("missing", "spawn"), ("hang", "timeout"), ("flood", "output-limit"), ("inherited", "timeout"), ("invalid", "deadline-invalid"), ("capacity", "none")] {
            let row = rows.iter().find(|r| r["operation"] == format!("fixtureq.operation.{name}") && r["kind"] == "method").unwrap();
            assert_eq!(row["error"], class); assert_eq!(row["parent"], parent["span"]); assert_eq!(row["trace"], parent["trace"]);
        }
        assert!(!serde_json::to_string(&rows).unwrap().contains("private-error"));
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
            "hang" => std::thread::sleep(Duration::from_secs(5)),
            "flood" => std::io::stdout().write_all(&vec![b'x'; OUTPUT_LIMIT * 2]).unwrap(),
            "inherited" => {
                fixture("linger").stdin(Stdio::null()).stdout(Stdio::inherit()).stderr(Stdio::inherit()).spawn().unwrap();
            }
            "linger" => std::thread::sleep(Duration::from_millis(1500)),
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
                drop(Guard { child: Some(child), permit: Some(Permit::acquire().unwrap()), reaper: Box::leak(Box::new(tx)) });
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
