use coreq::telemetry::{Reader, Store, capture::{Capture, checkpoint, CommandReceipt}};
use std::{fs, process::Command, sync::Arc};
use tracing_subscriber::prelude::*;

// Chain: command commit -> producer exit -> service observation -> restart/replay -> same original identity.
#[test]
fn receipt_survives_process_handoff_and_observer_restart() {
    let dir = std::env::temp_dir().join(format!("coreq-receipt-handoff-{}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    let capture = Capture::new(Arc::new(Store::open(&dir.join("db")).unwrap()), "fixtureq", "1").unwrap();
    tracing::subscriber::with_default(tracing_subscriber::registry().with(capture.clone()), || {
        let _action = tracing::info_span!("resume").entered();
        checkpoint("received").unwrap();
        fs::write(dir.join("receipt"), CommandReceipt::current().unwrap().encode()).unwrap();
    });
    capture.flush().unwrap();
    drop(capture);
    for _ in 0..2 {
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "receipt_observer", "--ignored"])
            .env("COREQ_RECEIPT_TEST", &dir).output().unwrap();
        assert!(output.status.success(), "{}\n{}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
    }
    let reader = Reader::open(&dir.join("db")).unwrap();
    let page = reader.export(0, 0, 1024).unwrap();
    let rows: Vec<_> = page["events"].as_array().unwrap().iter()
        .filter(|row| row["observation"]["source"] == "command").collect();
    let received = &rows.iter().find(|row| row["observation"]["kind"] == "command.received").unwrap()["observation"];
    for row in rows.iter().filter(|row| row["observation"]["kind"] == "command.observed") {
        for key in ["product", "version", "operation", "trace", "span", "parent", "started_ms"] {
            assert_eq!(row["observation"][key], received[key], "handoff changed {key}");
        }
    }
    assert_eq!(rows.len(), 2, "observer restart created another receipt");
    drop(reader);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
#[ignore = "subprocess helper for receipt_survives_process_handoff_and_observer_restart"]
fn receipt_observer() {
    let dir = std::path::PathBuf::from(std::env::var_os("COREQ_RECEIPT_TEST").unwrap());
    let capture = Capture::new(Arc::new(Store::open(&dir.join("db")).unwrap()), "fixtureq", "2").unwrap();
    tracing::subscriber::with_default(tracing_subscriber::registry().with(capture.clone()), || {
        let _action = tracing::info_span!("service_pass").entered();
        let receipt = CommandReceipt::decode(&fs::read_to_string(dir.join("receipt")).unwrap()).unwrap();
        receipt.checkpoint("observed").unwrap();
    });
    capture.flush().unwrap();
}

#[test]
fn descriptor_validation_and_replay_never_invent_acknowledgments() {
    use serde_json::{Value, json};
    let valid = json!({"wire":1,"product":"fixtureq","version":"1","operation":"fixtureq.resume",
        "trace":"0123456789abcdef0123456789abcdef.1","span":"0123456789abcdef0123456789abcdef.2",
        "parent":"0123456789abcdef0123456789abcdef.1","started_ms":1});
    let receipt = CommandReceipt::decode(&valid.to_string()).unwrap();
    assert!(receipt.checkpoint("observed").is_err(), "no collector must refuse");
    assert!(CommandReceipt::current().is_err(), "no action must refuse");
    for (key, value) in [
        ("wire", json!(2)), ("wire", json!(1.0)), ("extra", json!("private payload")),
        ("product", json!(null)), ("version", json!(null)), ("operation", json!("/private/path")),
        ("trace", json!("unknown")), ("span", json!("0123456789abcdef0123456789abcdef.0")),
        ("span", json!("0123456789abcdef0123456789abcdef.01")), ("parent", json!(false)),
        ("parent", json!("private payload")), ("started_ms", json!(-1)),
        ("started_ms", json!(u64::MAX)), ("started_ms", json!(null)), ("started_ms", json!(1.5)),
    ] {
        let mut invalid = valid.clone(); invalid[key] = value;
        assert_eq!(CommandReceipt::decode(&invalid.to_string()).unwrap_err(), "invalid command receipt descriptor", "{key}");
    }
    for key in valid.as_object().unwrap().keys() {
        let mut invalid = valid.clone(); invalid.as_object_mut().unwrap().remove(key);
        assert!(CommandReceipt::decode(&invalid.to_string()).is_err(), "missing {key}");
    }
    for text in ["[]".into(), "null".into(), "{".into(), " ".repeat(1025)] {
        assert_eq!(CommandReceipt::decode(&text).unwrap_err(), "invalid command receipt descriptor");
    }
    let mut no_parent = valid.clone(); no_parent["parent"] = Value::Null;
    assert!(CommandReceipt::decode(&no_parent.to_string()).is_ok());

    let dir = std::env::temp_dir().join(format!("coreq-receipt-validation-{}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    let store = Arc::new(Store::open(&dir.join("db")).unwrap());
    let foreign = Capture::new(store.clone(), "otherq", "1").unwrap();
    tracing::subscriber::with_default(tracing_subscriber::registry().with(foreign.clone()), || {
        assert_eq!(receipt.checkpoint("observed").unwrap_err(), "command receipt product mismatch");
    });
    foreign.flush().unwrap(); drop(foreign);
    let capture = Capture::new(store.clone(), "fixtureq", "2").unwrap();
    tracing::subscriber::with_default(tracing_subscriber::registry().with(capture.clone()), || {
        assert!(receipt.checkpoint("arbitrary").is_err());
        let observed = receipt.checkpoint("observed").unwrap();
        let reordered: serde_json::Map<String, Value> = valid.as_object().unwrap().iter().rev().map(|(k,v)|(k.clone(),v.clone())).collect();
        assert_eq!(CommandReceipt::decode(&Value::Object(reordered).to_string()).unwrap().checkpoint("observed").unwrap(), observed);
        let mut changed = valid.clone(); changed["operation"] = json!("fixtureq.pause");
        assert!(CommandReceipt::decode(&changed.to_string()).unwrap().checkpoint("observed").is_err(), "same identity cannot overwrite history");
        receipt.checkpoint("superseded").unwrap();
    });
    // A deliberately conflicting write latches loss health; inspect committed data independently.
    let reader = Reader::open(&dir.join("db")).unwrap();
    let page = reader.export(0, 0, 1024).unwrap();
    let rows: Vec<_> = page["events"].as_array().unwrap().iter().filter(|r| r["observation"]["source"] == "command").collect();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["observation"]["kind"], "command.observed");
    assert_eq!(rows[1]["observation"]["kind"], "command.superseded");
    assert!(rows.iter().all(|r| r["observation"]["operation"] == "fixtureq.resume"));
    drop(reader); drop(capture); drop(store);
    fs::remove_dir_all(dir).unwrap();
}
