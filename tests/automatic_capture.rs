use coreq::telemetry::{Store, capture::Capture};
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
    let rows = export["events"].as_array().unwrap();
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
