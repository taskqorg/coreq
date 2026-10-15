use coreq::telemetry::{Store, capture::{Capture, TraceContext}};
use std::{
    fs,
    sync::{Arc, atomic::Ordering},
    task::{Context, Poll, Wake, Waker},
};
use tracing_subscriber::prelude::*;
include!(concat!(
    env!("OUT_DIR"),
    "/telemetry-fixture/application.rs"
));

struct Noop;
impl Wake for Noop {
    fn wake(self: Arc<Self>) {}
}

// Chain: command -> serialized context -> independent service collector -> generated descendant.
// Risk: a new thread/process silently starts a separate trace and loses the command's observed result.
#[test]
fn a_handoff_preserves_causality_without_leaking_parent_payloads() {
    let dir = std::env::temp_dir().join(format!("coreq-handoff-{}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    let store = Arc::new(Store::open(&dir.join("db")).unwrap());
    let command = Capture::new(store.clone(), "fixtureq", "1").unwrap();
    let encoded = tracing::subscriber::with_default(tracing_subscriber::registry().with(command.clone()), || {
        let _command = tracing::info_span!("command", private = "secret /private/path").entered();
        TraceContext::current().unwrap().encode()
    });
    command.flush().unwrap();
    drop(command);
    for invalid in ["".to_string(), encoded.replace("1 ", "2 "), format!("{encoded} extra"), encoded.replace('.', "/"),
        format!("1 {}.0 {}.1", "0".repeat(32), "0".repeat(32)), "x".repeat(129)] {
        assert!(TraceContext::decode(&invalid).is_err());
    }
    assert!(tracing::subscriber::with_default(tracing::subscriber::NoSubscriber::default(), TraceContext::current).is_none());
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "handoff_child", "--ignored"])
        .env("COREQ_HANDOFF_DB", dir.join("db")).env("COREQ_HANDOFF_CONTEXT", encoded).output().unwrap();
    assert!(child.status.success(), "{}\n{}", String::from_utf8_lossy(&child.stdout), String::from_utf8_lossy(&child.stderr));
    let exported = store.export(0, 0, 1024).unwrap();
    let observations: Vec<_> = exported["events"].as_array().unwrap().iter().map(|r| &r["observation"]).collect();
    let terminal = |name: &str| observations.iter().copied().find(|r| r["operation"] == name && r["kind"] == "method").unwrap();
    let command = terminal("fixtureq.command");
    let wake = terminal("fixtureq.continuation");
    assert_eq!(wake["trace"], command["trace"]);
    assert_eq!(wake["parent"], command["span"]);
    assert_eq!(terminal("fixtureq.ordinary")["trace"], command["trace"]);
    assert_eq!(terminal("fixtureq.ordinary")["parent"], wake["span"]);
    assert!(wake["project"].is_null(), "remote link inherited unrelated ambient metadata");
    assert_ne!(terminal("fixtureq.sibling")["trace"], command["trace"]);
    assert_eq!(terminal("fixtureq.sibling")["parent"], terminal("fixtureq.unrelated")["span"]);
    assert_eq!(terminal("fixtureq.refusal")["outcome"], "refused");
    assert_eq!(terminal("fixtureq.refusal")["error"], "host-held");
    assert_eq!(terminal("fixtureq.ordinary")["outcome"], "ok", "refusal leaked into a child");
    assert_eq!(terminal("fixtureq.failed_refusal")["outcome"], "failed");
    assert_eq!(terminal("fixtureq.failed_refusal")["error"], "returned-error");
    assert_eq!(terminal("fixtureq.panicked_refusal")["error"], "panic");
    assert!(!exported.to_string().contains("secret"));
    let service = Capture::new(store.clone(), "fixtureq", "1").unwrap();
    tracing::subscriber::with_default(tracing_subscriber::registry().with(service.clone()), || {
        let _invalid = tracing::info_span!("invalid", coreq_trace = "secret /private/path", coreq_parent = "bad").entered();
    });
    assert!(service.flush().is_err(), "invalid raw handoff reported healthy collection");
    assert_eq!(service.health()["lost"], 1);
    drop(service); drop(store);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
#[ignore = "subprocess helper exercised by a_handoff_preserves_causality_without_leaking_parent_payloads"]
fn handoff_child() {
    let store = Arc::new(Store::open(&std::path::PathBuf::from(std::env::var_os("COREQ_HANDOFF_DB").unwrap())).unwrap());
    let capture = Capture::new(store, "fixtureq", "1").unwrap();
    let encoded = std::env::var("COREQ_HANDOFF_CONTEXT").unwrap();
    tracing::subscriber::with_default(tracing_subscriber::registry().with(capture.clone()), || {
        let _ambient = tracing::info_span!("unrelated", project = "private/ambient").entered();
        let link = TraceContext::decode(&encoded).unwrap();
        assert_eq!(link.encode(), encoded);
        {
            let _wake = link.span().entered();
            ordinary(3).unwrap();
            {
                let _refused = tracing::info_span!("refusal").entered();
                tracing::info!(coreq_refusal = "host-held");
                ordinary(4).unwrap();
            }
            {
                let _failed = tracing::info_span!("failed_refusal", coreq_refusal = "host-held").entered();
                let _: Result<(), ()> = coreq::telemetry::capture::returned_result(Err(()));
            }
            let _ = std::panic::catch_unwind(|| {
                let _panic = tracing::info_span!("panicked_refusal", coreq_refusal = "host-held").entered();
                panic!("secret failure text");
            });
        }
        let _sibling = tracing::info_span!("sibling").entered();
    });
    capture.flush().unwrap();
}
// Chain: unannotated methods and a new method -> shared automatic layer -> durable metadata export,
// including Result errors, panic, nested module calls and an async task suspended alongside unrelated work.
#[test]
fn every_added_method_inherits_capture_without_payloads() {
    let dir = std::env::temp_dir().join(format!("coreq-auto-capture-{}", std::process::id()));
    if dir.exists() {
        fs::remove_dir_all(&dir).unwrap();
    }
    fs::create_dir_all(&dir).unwrap();
    let store = Arc::new(Store::open(&dir.join("db")).unwrap());
    let capture = Capture::new(store.clone(), "fixtureq", "1.0.0").unwrap();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    tracing::subscriber::with_default(subscriber, || {
        let root = tracing::info_span!(
            "request",
            project = "org/project",
            runtime = "codex",
            task = 123u64
        );
        let _entered = root.enter();
        assert_eq!(ordinary(4).unwrap(), 8);
        assert_eq!(explicit_path_probe(), 11);
        let mut c = Counter::new();
        assert_eq!(c.added_without_annotation(), 1);
        assert_eq!(boxed().unwrap().get(), 12);
        assert_eq!(opaque().unwrap().sum::<u32>(), 3);
        assert!(no_format_error().is_err());
        assert!(typed_callback().is_err());
        assert!(inferred_callback().is_err());
        assert!(conditional_return().is_err());
        assert_eq!(*c.borrowed().unwrap(), 1);
        assert!(return_error().is_err());
        let (file, line) = source_location();
        assert!(file.ends_with("/application.rs") || file.ends_with("\\application.rs"));
        let mapped = coreq::telemetry::capture::original_source(
            file,
            COREQ_TELEMETRY_SOURCE_ROOT,
            COREQ_TELEMETRY_SOURCE_BASE,
        );
        assert_eq!(mapped, "tests/fixtures/telemetry/application.rs");
        let source = include_str!("fixtures/telemetry/application.rs");
        assert_eq!(
            line as usize,
            source
                .lines()
                .position(|s| s.starts_with("pub fn source_location"))
                .unwrap()
                + 1
        );
        assert!(generic_error(1).is_err());
        let _ = std::panic::catch_unwind(panic_probe);
        let ready = AtomicUsize::new(0);
        let mut future = Box::pin(suspended(&ready));
        let waker = Waker::from(Arc::new(Noop));
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));
        ordinary(7).unwrap();
        ready.store(1, Ordering::SeqCst);
        assert!(matches!(future.as_mut().poll(&mut cx), Poll::Ready(Ok(10))));
    });
    capture.flush().unwrap();
    let export = store.export(0, 0, 1024).unwrap();
    let rows: Vec<_> = export["events"].as_array().unwrap().iter()
        .filter(|row| row["observation"]["kind"] == "method").collect();
    for terminal in &rows {
        assert!(export["events"].as_array().unwrap().iter().any(|start|
            start["observation"]["kind"] == "method.start"
            && start["observation"]["span"] == terminal["observation"]["span"]
            && start["observation"]["trace"] == terminal["observation"]["trace"]
            && start["observation"]["operation"] == terminal["observation"]["operation"]));
    }
    assert!(
        rows.iter()
            .any(|r| r["observation"]["operation"] == "fixtureq.Counter.added_without_annotation")
    );
    assert!(
        rows.iter()
            .any(|r| r["observation"]["operation"] == "fixtureq.child.double")
    );
    assert!(rows.iter().any(|r| {
        r["observation"]["operation"]
            .as_str()
            .is_some_and(|s| s.starts_with("fixtureq.inferred_callback.callback"))
            && r["observation"]["outcome"] == "unknown"
    }));
    assert!(
        rows.iter()
            .any(|r| r["observation"]["operation"] == "fixtureq.explicit.value")
    );
    assert!(rows.iter().any(|r| {
        r["observation"]["operation"]
            .as_str()
            .is_some_and(|s| s.starts_with("fixtureq.typed_callback.callback"))
            && r["observation"]["outcome"] == "failed"
    }));
    assert!(rows.iter().any(
        |r| r["observation"]["operation"] == "fixtureq.inferred_callback"
            && r["observation"]["outcome"] == "failed"
    ));
    assert!(
        rows.iter()
            .any(|r| r["observation"]["operation"] == "fixtureq.return_error"
                && r["observation"]["outcome"] == "failed")
    );
    assert!(rows.iter().any(
        |r| r["observation"]["operation"] == "fixtureq.generic_error"
            && r["observation"]["outcome"] == "failed"
    ));
    assert!(
        rows.iter()
            .any(|r| r["observation"]["operation"] == "fixtureq.panic_probe"
                && r["observation"]["error"] == "panic")
    );
    let suspended = rows
        .iter()
        .find(|r| r["observation"]["operation"] == "fixtureq.suspended")
        .unwrap();
    let request = rows
        .iter()
        .find(|r| r["observation"]["operation"] == "fixtureq.request")
        .unwrap();
    let unrelated = rows
        .iter()
        .find(|r| {
            r["observation"]["operation"] == "fixtureq.ordinary"
                && r["observation"]["parent"] == request["observation"]["span"]
        })
        .unwrap();
    assert_ne!(
        unrelated["observation"]["parent"], suspended["observation"]["span"],
        "suspended future must not steal unrelated parent context"
    );
    assert!(
        rows.iter()
            .all(|r| r["observation"]["project"] == "org/project")
    );
    assert!(export.to_string().find("secret").is_none());
    assert!(export.to_string().find("/private/path").is_none());
    assert!(
        COREQ_TELEMETRY_COVERAGE.contains(&("Counter.added_without_annotation", "instrumented"))
    );
    assert!(COREQ_TELEMETRY_COVERAGE.contains(&("static_value", "const-evaluation")));
    assert_eq!(capture.health()["lost"], 0);
    drop(capture);
    drop(store);
    fs::remove_dir_all(dir).unwrap();
}

// A live span must survive forced process death without acquiring a fabricated terminal result.
#[test]
fn committed_open_span_survives_process_kill() {
    let dir = std::env::temp_dir().join(format!("coreq-open-span-{}", std::process::id()));
    if dir.exists() { fs::remove_dir_all(&dir).unwrap(); }
    fs::create_dir_all(&dir).unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "open_span_child", "--ignored"])
        .env("COREQ_OPEN_SPAN_TEST", &dir)
        .spawn().unwrap();
    let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !dir.join("committed").exists() && std::time::Instant::now() < until {
        assert!(child.try_wait().unwrap().is_none(), "child exited before durable start");
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let committed = dir.join("committed").exists();
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(committed, "child never acknowledged durable start");
    let store = Store::open(&dir.join("db")).unwrap();
    let export = store.export(0, 0, 1024).unwrap();
    let rows = export["events"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    let start = &rows[0]["observation"];
    assert_eq!(start["operation"], "fixtureq.unfinished");
    assert_eq!(start["kind"], "method.start");
    assert_eq!(start["outcome"], "unknown");
    assert!(start["duration_us"].is_null());
    assert_eq!(start["unknown"]["duration_us"], "span-open");
    drop(store);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
#[ignore = "subprocess helper exercised by committed_open_span_survives_process_kill"]
fn open_span_child() {
    let dir = std::path::PathBuf::from(std::env::var_os("COREQ_OPEN_SPAN_TEST").unwrap());
    let store = Arc::new(Store::open(&dir.join("db")).unwrap());
    let capture = Capture::new(store, "fixtureq", "1.0.0").unwrap();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    tracing::subscriber::with_default(subscriber, || {
        let span = tracing::info_span!("unfinished");
        let _entered = span.enter();
        capture.flush().unwrap();
        fs::write(dir.join("committed"), b"committed").unwrap();
        loop { std::thread::park(); }
    });
}

#[test]
fn global_runtime_flushes_normal_and_generated_exit() {
    for mode in ["return", "exit", "occupied", "loss"] {
        let dir = std::env::temp_dir().join(format!("coreq-runtime-{mode}-{}", std::process::id()));
        if dir.exists() { fs::remove_dir_all(&dir).unwrap(); }
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "runtime_child", "--ignored"])
            .env("COREQ_RUNTIME_TEST", &dir).env("COREQ_RUNTIME_MODE", mode)
            .status().unwrap();
        assert_eq!(status.code(), Some(if mode == "exit" { 23 } else { 0 }));
        let store = Store::open(&dir.join("db")).unwrap();
        let export = store.export(0, 0, 1024).unwrap();
        let rows = export["events"].as_array().unwrap();
        if mode == "occupied" || mode == "loss" {
            assert!(rows.is_empty(), "unqualified collector invented committed rows");
            drop(store);
            fs::remove_dir_all(dir).unwrap();
            continue;
        }
        assert!(rows.iter().any(|r| r["observation"]["operation"] == "fixtureq.ordinary"
            && r["observation"]["kind"] == "method" && r["observation"]["outcome"] == "ok"));
        if mode == "exit" {
            assert!(rows.iter().any(|r| r["observation"]["operation"] == "fixtureq.exit_probe"
                && r["observation"]["kind"] == "method.start"));
            assert!(!rows.iter().any(|r| r["observation"]["operation"] == "fixtureq.exit_probe"
                && r["observation"]["kind"] == "method"));
        }
        drop(store);
        fs::remove_dir_all(dir).unwrap();
    }
}

#[test]
#[ignore = "subprocess helper exercised by global_runtime_flushes_normal_and_generated_exit"]
fn runtime_child() {
    let dir = std::path::PathBuf::from(std::env::var_os("COREQ_RUNTIME_TEST").unwrap());
    if std::env::var("COREQ_RUNTIME_MODE").unwrap() == "occupied" {
        tracing::subscriber::set_global_default(tracing_subscriber::registry()).unwrap();
        assert!(coreq::telemetry::capture::Runtime::install(&dir.join("db"), "fixtureq", "1.0.0").is_err());
        assert!(coreq::telemetry::capture::qualify().unwrap_err().contains("collector unavailable"));
        ordinary(2).unwrap();
        return;
    }
    let _runtime = coreq::telemetry::capture::Runtime::install(&dir.join("db"), "fixtureq", "1.0.0").unwrap();
    assert!(coreq::telemetry::capture::Runtime::install(&dir.join("db"), "fixtureq", "1.0.0").is_err());
    if std::env::var("COREQ_RUNTIME_MODE").unwrap()=="loss" {
        rusqlite::Connection::open(dir.join("db")).unwrap().execute_batch("CREATE TRIGGER fail_capture BEFORE INSERT ON events BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
    }
    assert!(coreq::telemetry::capture::qualify().unwrap_err().contains("no committed observations"));
    ordinary(2).unwrap();
    if std::env::var("COREQ_RUNTIME_MODE").unwrap()=="loss" {
        assert!(coreq::telemetry::capture::qualify().is_err());return;
    }
    assert!(coreq::telemetry::capture::qualify().unwrap()["committed"].as_u64().unwrap()>0);
    if std::env::var("COREQ_RUNTIME_MODE").unwrap() == "exit" { exit_probe(); }
}
