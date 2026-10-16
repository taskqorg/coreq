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

// Chain: shared execution -> handled result/panic -> durable causal outcome with diagnostics off.
#[test]
fn execution_observes_errors_before_callers_discard_them() {
    use coreq::telemetry::capture::operation;
    struct Private(&'static str);
    let dir = std::env::temp_dir().join(format!("coreq-execution-{}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    let store = Arc::new(Store::open(&dir.join("db")).unwrap());
    let capture = Capture::new(store.clone(), "fixtureq", "1").unwrap();
    tracing::subscriber::with_default(tracing_subscriber::registry().with(capture.clone()), || {
        let _parent = tracing::info_span!("command").entered();
        let result: Result<u32, Private> = operation("read", || Ok(7), |_| panic!("success classified as error"));
        assert_eq!(result.ok(), Some(7));
        let mut calls = 0;
        let error = operation("request", || { calls += 1; Err::<(), _>(Private("secret /private/path")) }, |_| "timeout").err().unwrap();
        assert_eq!(calls, 1); assert_eq!(error.0, "secret /private/path");
        let _ = operation("bad-class", || Err::<(), _>(()), |_| "secret /private/path");
        let _ = std::panic::catch_unwind(|| operation::<(), ()>("panic", || panic!("private panic"), |_| "unused"));
    });
    capture.flush().unwrap();
    assert_eq!(capture.health()["diagnostics"]["state"], "off");
    assert_eq!(capture.health()["lost"], 0);
    let exported = store.export(0, 0, 1024).unwrap();
    let rows: Vec<_> = exported["events"].as_array().unwrap().iter().map(|r| &r["observation"]).filter(|r| r["kind"] == "method").collect();
    let find = |name: &str| rows.iter().copied().find(|r| r["operation"] == name).unwrap();
    let parent = find("fixtureq.command"); assert_eq!(parent["outcome"], "ok");
    assert_eq!(find("fixtureq.operation.read")["outcome"], "ok");
    for (name, class) in [("request", "timeout"), ("bad-class", "returned-error"), ("panic", "panic")] {
        let row = find(&format!("fixtureq.operation.{name}"));
        assert_eq!(row["outcome"], "failed"); assert_eq!(row["error"], class);
        assert_eq!(row["parent"], parent["span"]); assert_eq!(row["trace"], parent["trace"]);
    }
    assert_eq!(rows.len(), 5); assert!(!exported.to_string().contains("private"));
    drop(capture); drop(store); fs::remove_dir_all(dir).unwrap();
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
    capture.diagnose("*", std::time::Duration::from_secs(60), 256).unwrap();
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
    capture.diagnose("*", std::time::Duration::from_secs(60), 256).unwrap();
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
        fn assert_send<T: Send>(_: T) {}
        assert_send(send_not_sync(std::cell::Cell::new(1)));
        let ready = AtomicUsize::new(0);
        let mut future = Box::pin(suspended(&ready));
        let waker = Waker::from(Arc::new(Noop));
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));
        ordinary(7).unwrap();
        ready.store(1, Ordering::SeqCst);
        assert!(matches!(future.as_mut().poll(&mut cx), Poll::Ready(Ok(10))));
        let mut callback = Box::pin(async_callback());
        assert!(matches!(callback.as_mut().poll(&mut cx), Poll::Ready(Err(()))));
        let pending = AtomicUsize::new(0);
        let mut cancelled = Box::pin(suspended(&pending));
        assert!(matches!(cancelled.as_mut().poll(&mut cx), Poll::Pending));
        drop(cancelled);
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
    assert!(rows.iter().any(|r| r["observation"]["operation"] == "fixtureq.suspended" && r["observation"]["outcome"] == "unknown"), "dropped pending future reported success");
    assert!(rows.iter().any(|r| r["observation"]["operation"] == "fixtureq.async_callback" && r["observation"]["outcome"] == "failed"));
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
        assert!(rows.iter().any(|r| r["observation"]["operation"] == "fixtureq.command"
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
    { let _action = tracing::info_span!("command").entered(); ordinary(2).unwrap(); }
    if std::env::var("COREQ_RUNTIME_MODE").unwrap()=="loss" {
        assert!(coreq::telemetry::capture::qualify().is_err());return;
    }
    assert!(coreq::telemetry::capture::qualify().unwrap()["committed"].as_u64().unwrap()>0);
    if std::env::var("COREQ_RUNTIME_MODE").unwrap() == "exit" { exit_probe(); }
}

// Chain: normal action -> selected diagnostic window -> quota/expiry -> normal action.
// Risk: optional method capture floods the writer, poisons a handled result, or disables action evidence.
#[test]
fn diagnostic_selection_is_bounded_and_does_not_change_action_results() {
    use std::time::Duration;
    let dir = std::env::temp_dir().join(format!("coreq-diagnostic-{}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    let store = Arc::new(Store::open(&dir.join("db")).unwrap());
    let capture = Capture::new(store.clone(), "fixtureq", "1").unwrap();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(capture.clone()));
    tracing::dispatcher::with_default(&dispatch, || {
        let _action = tracing::info_span!("normal").entered();
        for _ in 0..10_000 { ordinary(2).unwrap(); }
        assert!(return_error().is_err()); // handled; the parent must remain successful
    });
    capture.flush().unwrap();
    let initial = store.export(0, 0, 1024).unwrap();
    assert_eq!(initial["events"].as_array().unwrap().len(), 2, "normal methods produced per-call rows");
    assert_eq!(initial["events"][1]["observation"]["outcome"], "ok");
    for (selector, period, cap) in [("/private/path", 1, 1), ("*", 61, 1), ("*", 1, 257), ("*", 1, 0)] {
        assert!(capture.diagnose(selector, Duration::from_secs(period), cap).is_err());
    }
    capture.diagnose("ordinary", Duration::from_secs(60), 5).unwrap();
    assert!(capture.diagnose("*", Duration::from_secs(1), 1).is_err());
    let workers: Vec<_> = (0..4).map(|_| {
        let dispatch = dispatch.clone();
        std::thread::spawn(move || tracing::dispatcher::with_default(&dispatch, || {
            let _action = tracing::info_span!("thread").entered();
            for _ in 0..100 { ordinary(3).unwrap(); }
        }))
    }).collect();
    for worker in workers { worker.join().unwrap(); }
    assert_eq!(capture.health()["diagnostics"]["state"], "limited");
    assert_eq!(capture.health()["diagnostics"]["admitted"], 5);
    capture.diagnose("recover_recursively", Duration::from_secs(1), 1).unwrap();
    tracing::dispatcher::with_default(&dispatch, || { recover_recursively(2).unwrap(); });
    capture.diagnose("suspended", Duration::from_millis(10), 256).unwrap();
    tracing::dispatcher::with_default(&dispatch, || {
        let _action = tracing::info_span!("expiry").entered();
        let ready = AtomicUsize::new(0);
        let mut future = Box::pin(suspended(&ready));
        let waker = Waker::from(Arc::new(Noop));
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));
        std::thread::sleep(Duration::from_millis(20));
        ready.store(1, Ordering::SeqCst);
        assert!(matches!(future.as_mut().poll(&mut cx), Poll::Ready(Ok(10))));
        ordinary(4).unwrap();
    });
    assert_eq!(capture.health()["diagnostics"]["state"], "expired");
    capture.flush().unwrap();
    let export = store.export(0, 0, 1024).unwrap();
    let rows: Vec<_> = export["events"].as_array().unwrap().iter().map(|r| &r["observation"]).collect();
    let detail: Vec<_> = rows.iter().copied().filter(|r| r["source"] == "diagnostic" && r["kind"] == "method").collect();
    assert_eq!(detail.iter().filter(|r| r["operation"] == "fixtureq.ordinary").count(), 5);
    assert!(detail.iter().any(|r| r["operation"] == "fixtureq.recover_recursively" && r["outcome"] == "ok"), "disabled recursion poisoned the observed caller");
    assert!(!detail.iter().any(|r| r["operation"] == "fixtureq.child.double"), "selector admitted another module");
    assert!(detail.iter().any(|r| r["operation"] == "fixtureq.suspended"), "expiry lost an admitted terminal");
    for terminal in &detail {
        assert!(rows.iter().any(|r| r["kind"] == "method.start" && r["span"] == terminal["span"]));
        if !terminal["parent"].is_null() { assert!(rows.iter().any(|r| r["span"] == terminal["parent"]), "concurrent admission lost the action parent"); }
    }
    assert_eq!(rows.iter().filter(|r| r["operation"] == "fixtureq.thread" && r["kind"] == "method").count(), 4);
    assert!(rows.iter().any(|r| r["operation"] == "collector.diagnostic.state" && r["source"] == "expired"));
    assert!(rows.iter().any(|r| r["operation"] == "collector.diagnostic.state" && r["source"] == "limited" && r["from"] == "ordinary" && r["calls"] == 5), "replacing a session lost its truncation evidence");
    assert_eq!(capture.health()["lost"], 0);
    drop(dispatch); drop(capture); drop(store);
    fs::remove_dir_all(dir).unwrap();
}
