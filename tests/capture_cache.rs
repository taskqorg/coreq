use coreq::telemetry::{Store,capture::{Capture,TraceContext,operation,detached_span,returned_result,returned_method_result}};
use std::{fs,sync::Arc,time::Duration};
use tracing_subscriber::prelude::*;

// One dispatcher in its own executable exercises tracing's single-dispatcher cache path.
#[test]
fn unobserved_calls_and_diagnostic_flush_preserve_operation_capture() {
    let dir=std::env::temp_dir().join(format!("coreq-capture-cache-{}",std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    let store=Arc::new(Store::open(&dir.join("db")).unwrap());
    let capture=Capture::new(store.clone(),"fixtureq","1").unwrap();
    let dispatch=tracing::Dispatch::new(tracing_subscriber::registry().with(capture.clone()));
    let link=TraceContext::decode(&format!("1 {}.1 {}.2","a".repeat(32),"a".repeat(32))).unwrap();
    operation("unobserved",||Ok::<_,()>(()), |_|"fixture").unwrap();
    assert!(detached_span("unobserved").is_disabled());
    assert!(link.span().is_disabled());
    returned_result(Err::<(),_>(())).unwrap_err();
    tracing::dispatcher::with_default(&dispatch,|| {
        operation("before",||Err::<(),_>(()), |_|"refused").unwrap_err();
        let cleanup=detached_span("cleanup");cleanup.record("coreq_result","observed");drop(cleanup);
        drop(link.span());
        let _typed=tracing::info_span!("typed").entered();
        returned_result(Err::<(),_>(())).unwrap_err();
    });
    capture.flush().unwrap();
    let first=store.export(0,0,1024).unwrap();
    assert!(first["events"].as_array().unwrap().iter().any(|r|r["observation"]["operation"]=="fixtureq.operation.before"&&r["observation"]["kind"]=="method"),"ambient call suppressed the observed operation");
    capture.diagnose("fixture.detached_method",Duration::from_secs(60),1).unwrap();
    let method=tracing::dispatcher::with_default(&dispatch,|| {
        tracing::info_span!(target:"coreq::method","fixture.detached_method",coreq_result="unclassified")
    });
    returned_method_result(&method,Err::<(),_>(())).unwrap_err();drop(method);
    capture.diagnose("fixture.method",Duration::from_secs(60),1).unwrap();
    tracing::dispatcher::with_default(&dispatch,|| {
        let _detail=tracing::info_span!(target:"coreq::method","fixture.method").entered();
    });
    capture.flush().unwrap();
    tracing::dispatcher::with_default(&dispatch,|| {
        operation("after",||Ok::<_,()>(()), |_|"fixture").unwrap();
    });
    capture.flush().unwrap();
    assert_eq!(capture.health()["diagnostics"]["state"],"limited");
    capture.diagnose("fixture.method",Duration::from_millis(1),1).unwrap();
    std::thread::sleep(Duration::from_millis(5));
    capture.flush().unwrap();
    tracing::dispatcher::with_default(&dispatch,|| {
        operation("expired",||Ok::<_,()>(()), |_|"fixture").unwrap();
    });
    capture.flush().unwrap();
    let exported=store.export(0,0,1024).unwrap();
    let rows:Vec<_>=exported["events"].as_array().unwrap().iter().map(|r|&r["observation"]).collect();
    assert!(!rows.iter().any(|r|r["operation"]=="fixtureq.operation.unobserved"));
    for (name,error) in [("before","refused"),("after","none"),("expired","none")] {
        let hits:Vec<_>=rows.iter().filter(|r|r["operation"]==format!("fixtureq.operation.{name}")).collect();
        assert_eq!(hits.len(),2,"operation {name} lost start or terminal");
        assert_eq!(hits[1]["error"],error);
    }
    for (name,error) in [("operation.cleanup","none"),("continuation","none"),("typed","returned-error"),("fixture.detached_method","returned-error")] {
        let terminal=rows.iter().find(|r|r["operation"]==format!("fixtureq.{name}")&&r["kind"]=="method").expect("shared boundary disappeared");
        assert_eq!(terminal["error"],error);
    }
    assert_eq!(capture.health()["diagnostics"]["state"],"expired");
    assert_eq!(capture.health()["lost"],0);
    drop(dispatch);drop(capture);drop(store);fs::remove_dir_all(dir).unwrap();
}
