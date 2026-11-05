//! The one shipper: local records to DashQ D11 with a durable cursor (docs/design/telemetry.md § Shipper).
use super::{Batch, Res, Store, code, err, sidecar};
use crate::enroll::{self, Backoff, Delivery, Wire};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::{Map, Value, json};
use std::path::Path;

/// Enrollment schema of one shipped observation.
pub const OBSERVATION_SCHEMA: &str = "coreq-observation/1";
const SHIPPER: &str = "dashq";
const RECORD_VERSION: u64 = 1;
/// Shared fields that may be absent; `kind`, `operation`, `outcome` and `error` stay required by validation.
const OPTIONAL: &[&str] = &["started_ms", "duration_us", "project", "task", "attempt", "actor", "entry", "pipeline", "pipeline_revision", "step", "units", "unit_kind", "disk_bytes", "runtime", "runtime_version", "host"];
/// Fields of specq's `agent.call`, and of `agent.turn` with its totals; other kinds do not gain them.
const CALL: &[&str] = &["model", "input_tokens", "cache_read_tokens", "cache_write_tokens", "output_tokens", "reasoning_tokens"];
const TURN: &[&str] = &["model", "input_tokens", "cache_read_tokens", "cache_write_tokens", "output_tokens", "reasoning_tokens", "exit_code", "session_id", "user_cpu_us", "system_cpu_us", "peak_rss_bytes"];
pub(super) const NOT_COLLECTED: &str = "not-collected";
/// Requests per pass: below the D11 per-device burst of 10.
const REQUESTS: usize = 8;
/// Room left for the request envelope inside `enroll::MAX_BODY`.
const BODY: usize = enroll::MAX_BODY - 4096;
/// Receipt reasons about the record itself; the cursor passes it. Any other reason holds the cursor.
const RECORD_REASONS: &[&str] = &["payload", "expired", "replay_mismatch"];

/// The shared record of `value`: every shared field present, null with its reason when not collected.
/// A non-object stays as it is, and validation refuses it.
pub fn shared(value: &Value) -> Value {
    let Some(object) = value.as_object() else { return value.clone() };
    let mut record = Map::new();
    record.insert("schema".into(), json!(RECORD_VERSION));
    let mut unknown = object.get("unknown").and_then(Value::as_object).cloned().unwrap_or_default();
    let agent = match object.get("kind").and_then(Value::as_str) {
        Some("agent.turn") => TURN,
        Some("agent.call") => CALL,
        _ => &[],
    };
    for name in OPTIONAL.iter().chain(agent) {
        let found = object.get(*name).cloned().unwrap_or(Value::Null);
        if found.is_null() && !unknown.contains_key(*name) {
            unknown.insert((*name).into(), json!(NOT_COLLECTED));
        }
        record.insert((*name).into(), found);
    }
    for (name, field) in object {
        if name != "unknown" && !record.contains_key(name) {
            record.insert(name.clone(), if name == "inventory" { super::inventory::shared(field) } else { field.clone() });
        }
    }
    if !unknown.is_empty() || object.contains_key("unknown") {
        record.insert("unknown".into(), Value::Object(unknown));
    }
    Value::Object(record)
}

/// One pass of [`Store::ship`]. `more`: rows remain for the consumer's next pass.
#[derive(Debug, Default, PartialEq)]
pub struct Shipped {
    pub sent: usize,
    pub rejected: usize,
    pub more: bool,
}

/// Rows at or below this sequence may be pruned: the shipper's cursor, or every row without a shipper.
pub(super) fn floor(db: &Connection) -> Res<i64> {
    Ok(db.query_row("SELECT seq FROM cursors WHERE destination=?1", [SHIPPER], |r| r.get(0)).optional().map_err(err)?.unwrap_or(i64::MAX))
}

/// Counts in the caller's cursor transaction, so a counter and its cursor move together.
pub(super) fn count_rejected(db: &Connection, reasons: &[String]) -> Res<()> {
    if reasons.is_empty() {
        return Ok(());
    }
    db.execute_batch("CREATE TABLE IF NOT EXISTS shipper_rejected(reason TEXT PRIMARY KEY,count INTEGER NOT NULL)").map_err(err)?;
    for reason in reasons {
        db.execute("INSERT INTO shipper_rejected VALUES(?1,1) ON CONFLICT(reason) DO UPDATE SET count=count+1", params![reason]).map_err(err)?;
    }
    Ok(())
}

impl Batch {
    /// Stable bytes per row: a retry repeats its event id and content, so D11 stores it once.
    fn records(&self, connection: &Path) -> Res<Vec<Value>> {
        self.rows.iter().map(|row| {
            let observation: Value = serde_json::from_str(&row.payload).map_err(err)?;
            let observed_ms = if row.started_ms >= 0 { row.started_ms } else { row.received_ms };
            let payload = json!({"seq": row.seq, "id": row.id, "received_ms": row.received_ms, "observation": shared(&observation)});
            enroll::stamp(connection, OBSERVATION_SCHEMA, &row.id, (observed_ms / 1000) as u64, payload)
        }).collect()
    }
}

impl Store {
    /// One bounded pass: sends unshipped records over the enrolled connection in `connection` and moves the cursor
    /// past each durable receipt. The first call creates the cursor, which from then on holds back pruning.
    pub fn ship(&self, connection: &Path, backoff: &mut Backoff, wire: &mut dyn Wire) -> Result<Shipped, Delivery> {
        let stopped = |error: String| Delivery::Stopped(error);
        let _one = crate::file_lock::try_lock(&sidecar(&self.path, ".ship.lock")).map_err(|_| stopped("shipper-busy".into()))?;
        self.start_shipper().map_err(stopped)?;
        let mut done = Shipped::default();
        for _ in 0..REQUESTS {
            let mut batch = self.offer(SHIPPER, enroll::MAX_RECORDS).map_err(stopped)?;
            if batch.is_empty() {
                return Ok(done);
            }
            let mut records = batch.records(connection).map_err(stopped)?;
            let mut bytes = 0;
            let fit = records.iter().take_while(|r| { bytes += r.to_string().len() + 1; bytes <= BODY }).count().max(1);
            if fit < records.len() {
                batch = self.offer(SHIPPER, fit).map_err(stopped)?;
                records.truncate(fit);
            }
            let mut rejected = Vec::new();
            for receipt in enroll::deliver(connection, &records, backoff, wire)? {
                match (receipt["state"].as_str(), receipt["reason"].as_str()) {
                    (Some("accepted"), _) => {}
                    (Some("rejected"), Some(reason)) if RECORD_REASONS.contains(&reason) => rejected.push(reason.to_owned()),
                    (Some("rejected"), Some(reason)) if code(reason) => return Err(stopped(reason.to_owned())),
                    _ => return Err(stopped("invalid receipt".into())),
                }
            }
            self.advance(&batch, &batch.receipt(), &rejected).map_err(stopped)?;
            done.sent += records.len() - rejected.len();
            done.rejected += rejected.len();
        }
        done.more = !self.offer(SHIPPER, 1).map_err(stopped)?.is_empty();
        Ok(done)
    }

    /// `state` `off` means no shipper has run, never that nothing is unsent.
    pub fn shipper(&self) -> Res<Value> {
        let db = self.db.lock().map_err(|_| "telemetry reader poisoned")?;
        let cursor: Option<i64> = db.query_row("SELECT seq FROM cursors WHERE destination=?1", [SHIPPER], |r| r.get(0)).optional().map_err(err)?;
        let Some(cursor) = cursor else {
            return Ok(json!({"state": "off", "cursor": null, "backlog_rows": null, "gap_through": null, "rejected": {}}));
        };
        let backlog: i64 = db.query_row("SELECT count(*) FROM events WHERE seq>?1", [cursor], |r| r.get(0)).map_err(err)?;
        let pruned: i64 = db.query_row("SELECT pruned_through FROM metadata WHERE id=1", [], |r| r.get(0)).map_err(err)?;
        let mut rejected = Map::new();
        if db.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='shipper_rejected')", [], |r| r.get::<_, bool>(0)).map_err(err)? {
            let mut counts = db.prepare("SELECT reason,count FROM shipper_rejected ORDER BY reason").map_err(err)?;
            for row in counts.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))).map_err(err)? {
                let (reason, count) = row.map_err(err)?;
                rejected.insert(reason, json!(count));
            }
        }
        Ok(json!({"state": "on", "cursor": cursor, "backlog_rows": backlog, "gap_through": (pruned > cursor).then_some(pruned), "rejected": rejected}))
    }

    fn start_shipper(&self) -> Res<()> {
        let mut db = self.db.lock().map_err(|_| "telemetry writer poisoned")?;
        if db.query_row("SELECT 1 FROM cursors WHERE destination=?1", [SHIPPER], |_| Ok(())).optional().map_err(err)?.is_some() {
            return Ok(());
        }
        let _budget = self.budget.as_ref().map(super::Budget::acquire).transpose()?;
        let _write = crate::file_lock::lock(&self.write_lock).map_err(err)?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(err)?;
        tx.execute("INSERT OR IGNORE INTO cursors(destination,seq,receipt) VALUES(?1,0,'')", [SHIPPER]).map_err(err)?;
        tx.commit().map_err(err)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{AGE_MS, Event, clock, prune};
    use super::*;
    use std::collections::HashMap;
    use std::path::PathBuf;

    const KINDS: [&str; 7] = ["operation", "stage", "transition", "service.pass", "check", "agent.turn", "agent.call"];

    fn value(kind: &str) -> Value {
        json!({"product": "taskq", "version": "1.0.0", "operation": "op.run", "kind": kind, "trace": "a".repeat(32), "span": "b".repeat(16),
            "started_ms": 1_700_000_000_000u64, "outcome": "ok", "error": "none"})
    }
    fn event(kind: &str) -> Event {
        Event::observation(value(kind)).unwrap()
    }

    /// D11 by its contract: stores an event id once, a replay returns the same receipt, changed content is refused.
    #[derive(Default)]
    struct D11 {
        stored: Vec<(String, String)>,
        requests: usize,
        replays: usize,
        mismatches: usize,
        lose_reply_of_request: Option<usize>,
        offline: bool,
        reject: Option<&'static str>,
    }
    impl Wire for D11 {
        fn post(&mut self, _: &str, route: &str, _: Option<&str>, body: &Value) -> Result<(u16, Value), String> {
            if self.offline {
                return Err("offline".into());
            }
            assert_eq!(route, "/ingest");
            self.requests += 1;
            let index: HashMap<String, String> = self.stored.iter().cloned().collect();
            let mut receipts = Vec::new();
            for record in body["records"].as_array().unwrap() {
                assert_eq!(record["schema"], OBSERVATION_SCHEMA);
                assert!(record.get("grant").is_none(), "the grant stays local");
                let id = record["event_id"].as_str().unwrap().to_owned();
                if let Some(reason) = self.reject {
                    receipts.push(json!({"event_id": id, "state": "rejected", "reason": reason}));
                    continue;
                }
                let content = record.to_string();
                match index.get(&id) {
                    Some(old) if *old != content => self.mismatches += 1,
                    Some(_) => self.replays += 1,
                    None => self.stored.push((id.clone(), content)),
                }
                receipts.push(json!({"event_id": id, "state": "accepted"}));
            }
            if self.lose_reply_of_request == Some(self.requests) {
                return Err("lost reply".into());
            }
            Ok((200, json!({"protocol": 1, "receipts": receipts})))
        }
        fn now(&self) -> u64 { 0 }
        fn sleep(&mut self, _: u64) {}
        fn show(&mut self, _: &str, _: &str) {}
    }

    fn setup(name: &str) -> (PathBuf, PathBuf, PathBuf) {
        let dir = crate::scratch(name);
        let connection = dir.join("connection");
        std::fs::create_dir_all(&connection).unwrap();
        std::fs::write(connection.join("connection.json"), json!({"installation": "i", "active": {"destination": "https://d.example", "credential": "invented", "generation": 1}}).to_string()).unwrap();
        (dir.join("db"), connection, dir)
    }
    fn seqs(d11: &D11) -> Vec<i64> {
        d11.stored.iter().map(|(_, c)| serde_json::from_str::<Value>(c).unwrap()["payload"]["seq"].as_i64().unwrap()).collect()
    }
    fn drain(store: &Store, connection: &Path, d11: &mut D11) -> Result<Shipped, Delivery> {
        let mut total = Shipped::default();
        loop {
            let pass = store.ship(connection, &mut Backoff::default(), d11)?;
            total.sent += pass.sent;
            total.rejected += pass.rejected;
            if !pass.more {
                return Ok(total);
            }
        }
    }
    fn age_all(store: &Store) {
        store.db.lock().unwrap().execute("UPDATE events SET received_ms=1", []).unwrap();
    }

    // Defect: kinds could omit shared fields, and DashQ read an absent value as zero. Protects R52 for each kind.
    #[test]
    fn each_kind_carries_every_shared_field_with_a_reason_for_null() {
        let _disk = crate::bulk_disk();
        let (path, connection, dir) = setup("telemetry-ship-kinds");
        let store = Store::open(&path).unwrap();
        for kind in KINDS {
            store.append(&[event(kind)]).unwrap();
        }
        // A real zero stays zero; the producer's own reason wins; a row from before this record gets the same shape.
        let mut collected = value("agent.call");
        collected.as_object_mut().unwrap().extend(json!({"duration_us": 0, "project": "owner/repo", "task": 7, "attempt": 3, "actor": "worker", "entry": 11,
            "pipeline": "asset.build", "pipeline_revision": "v1", "step": "render",
            "units": 900, "unit_kind": "tokens", "disk_bytes": 0, "runtime": "claude", "runtime_version": "2.1.0", "host": "mac",
            "unknown": {"model": "not-reported"}}).as_object().unwrap().clone());
        let mut reasoned = value("check");
        for name in ["pipeline", "pipeline_revision", "step"] {
            let mut invalid = collected.clone();
            invalid[name] = json!("private content/path");
            assert!(Event::observation(invalid).is_err(), "{name} accepts only fixed codes");
        }
        reasoned["task"] = Value::Null;
        reasoned["unknown"] = json!({"task": "no-board"});
        store.append(&[Event::observation(collected.clone()).unwrap(), Event::observation(reasoned).unwrap(), Event::new(value("stage")).unwrap()]).unwrap();
        let mut d11 = D11::default();
        drain(&store, &connection, &mut d11).unwrap();
        assert_eq!(d11.stored.len(), KINDS.len() + 3);
        let observations: Vec<Value> = d11.stored.iter().map(|(_, c)| serde_json::from_str::<Value>(c).unwrap()["payload"]["observation"].clone()).collect();
        for (observation, kind) in observations.iter().zip(KINDS) {
            assert_eq!(observation["kind"], kind);
            assert_eq!(observation["schema"], 1);
            for required in ["operation", "outcome", "error"] {
                assert!(observation[required].is_string(), "{kind} {required}");
            }
            for name in ["pipeline", "pipeline_revision", "step"] {
                assert_eq!(observation.get(name), Some(&Value::Null), "{kind} lacks {name}");
                assert_eq!(observation["unknown"][name], NOT_COLLECTED, "{kind} {name}");
            }
            for name in OPTIONAL {
                assert!(observation.get(*name).is_some(), "{kind} lacks {name}");
                if observation[*name].is_null() {
                    assert_eq!(observation["unknown"][*name], NOT_COLLECTED, "{kind} {name}");
                }
            }
        }
        let got = &observations[KINDS.len()];
        for name in OPTIONAL {
            assert_eq!(got[*name], collected[*name], "{name}");
            assert!(got["unknown"].get(*name).is_none(), "{name} was collected");
        }
        assert_eq!(got["duration_us"], 0);
        assert_eq!(got["unknown"]["model"], "not-reported");
        assert_eq!(observations[KINDS.len() + 1]["unknown"]["task"], "no-board");
        assert_eq!(observations[KINDS.len() + 2]["unknown"]["actor"], NOT_COLLECTED);
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }

    // Defect: the allowlist refused turn and call measurements, so agentq could not record them. Protects R52 and R27.
    #[test]
    fn agent_fields_reach_d11_and_an_unknown_one_reads_null_with_a_reason() {
        let _disk = crate::bulk_disk();
        let (path, connection, dir) = setup("telemetry-ship-agent");
        let store = Store::open(&path).unwrap();
        let session = "6d8a75f4-07bb-4a78-b46a-cd224e1fda7a";
        let mut turn = value("agent.turn");
        turn.as_object_mut().unwrap().extend(json!({"model": "claude-x", "input_tokens": 10, "cache_read_tokens": 20, "cache_write_tokens": 30, "output_tokens": 40,
            "reasoning_tokens": 0, "exit_code": 0, "session_id": session, "user_cpu_us": 1_500_000, "system_cpu_us": 0, "peak_rss_bytes": 734_003_200u64}).as_object().unwrap().clone());
        let mut call = value("agent.call");
        call.as_object_mut().unwrap().extend(json!({"model": "claude-x", "input_tokens": 1, "cache_read_tokens": 2, "cache_write_tokens": 3, "output_tokens": 4, "reasoning_tokens": 5}).as_object().unwrap().clone());
        // A signalled child has no exit code; the producer's own reason wins over the default.
        let mut signalled = value("agent.turn");
        signalled["exit_code"] = Value::Null;
        signalled["session_id"] = Value::Null;
        signalled["unknown"] = json!({"exit_code": "signal", "session_id": "not-started"});
        store.append(&[turn.clone(), call.clone(), signalled, value("agent.turn"), value("agent.call"), value("check")].map(|v| Event::observation(v).unwrap())).unwrap();
        let mut d11 = D11::default();
        drain(&store, &connection, &mut d11).unwrap();
        let shipped: Vec<Value> = d11.stored.iter().map(|(_, c)| serde_json::from_str::<Value>(c).unwrap()["payload"]["observation"].clone()).collect();
        for (sent, got) in [(&turn, &shipped[0]), (&call, &shipped[1])] {
            for (name, field) in sent.as_object().unwrap() {
                assert_eq!(&got[name], field, "{name}");
                assert!(got["unknown"].get(name).is_none(), "{name} was collected");
            }
        }
        assert_eq!(shipped[1].get("exit_code"), None, "turn fields stay off agent.call");
        assert_eq!((&shipped[2]["exit_code"], &shipped[2]["unknown"]["exit_code"]), (&Value::Null, &json!("signal")));
        assert_eq!((&shipped[2]["session_id"], &shipped[2]["unknown"]["session_id"]), (&Value::Null, &json!("not-started")));
        for (absent, names) in [(&shipped[3], TURN), (&shipped[4], CALL)] {
            for name in names {
                assert_eq!(absent.get(*name), Some(&Value::Null), "{name} is not zero");
                assert_eq!(absent["unknown"][*name], NOT_COLLECTED, "{name}");
            }
        }
        assert_eq!(shipped[5].get("reasoning_tokens"), None, "other kinds gain no agent fields");
        // A null without a reason, or a value that is not metadata, is refused before it can be stored.
        for (name, bad) in [("session_id", Value::Null), ("exit_code", Value::Null), ("peak_rss_bytes", Value::Null), ("session_id", json!("private content/path")), ("exit_code", json!(-1)), ("user_cpu_us", json!(1.5))] {
            let mut invalid = value("agent.turn");
            invalid[name] = bad.clone();
            assert!(Event::new(invalid).is_err(), "{name} accepted {bad}");
        }
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }

    // Defect: a crash between D11 acceptance and the cursor commit could lose or repeat records. Protects R53.
    #[test]
    fn restart_loses_and_duplicates_nothing() {
        let _disk = crate::bulk_disk();
        let (path, connection, dir) = setup("telemetry-ship-restart");
        let mut d11 = D11 { lose_reply_of_request: Some(2), ..D11::default() };
        {
            let store = Store::open(&path).unwrap();
            store.append(&(0..250).map(|_| event("operation")).collect::<Vec<_>>()).unwrap();
            let lost = store.ship(&connection, &mut Backoff::default(), &mut d11).unwrap_err();
            assert!(matches!(lost, Delivery::Retry(_)), "{lost:?}");
            assert_eq!(store.shipper().unwrap()["cursor"], 100, "only the first request was acknowledged");
            assert_eq!(d11.stored.len(), 200, "D11 durably holds the batch whose reply was lost");
        }
        let store = Store::open(&path).unwrap();
        assert_eq!(store.shipper().unwrap()["cursor"], 100, "the cursor survives the restart");
        drain(&store, &connection, &mut d11).unwrap();
        assert_eq!(seqs(&d11), (1..=250).collect::<Vec<_>>(), "every record once, in order");
        assert_eq!(d11.replays, 100, "the lost batch was repeated");
        assert_eq!(d11.mismatches, 0, "a repeat carries the same event id and bytes");
        assert_eq!(store.shipper().unwrap()["backlog_rows"], 0);
        let sent = d11.requests;
        drain(&store, &connection, &mut d11).unwrap();
        assert_eq!(d11.requests, sent, "nothing left to send");
        drop(store);
        let store = Store::open(&path).unwrap();
        store.append(&[event("check")]).unwrap();
        assert_eq!(drain(&store, &connection, &mut d11).unwrap().sent, 1);
        assert_eq!(seqs(&d11).last(), Some(&251));
        assert_eq!(d11.replays, 100);
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }

    // Defect: 30-day and capacity pruning deleted records nobody had shipped. Protects R31.
    #[test]
    fn pruning_takes_only_records_the_cursor_has_passed() {
        let _disk = crate::bulk_disk();
        let (path, connection, dir) = setup("telemetry-ship-prune");
        let store = Store::open(&path).unwrap();
        store.append(&(0..150).map(|_| event("operation")).collect::<Vec<_>>()).unwrap();
        let mut offline = D11 { offline: true, ..D11::default() };
        assert!(store.ship(&connection, &mut Backoff::default(), &mut offline).is_err());
        assert_eq!(store.shipper().unwrap()["state"], "on");
        age_all(&store);
        store.prune().unwrap();
        assert_eq!(store.storage().unwrap()["pruned_count"], 0, "an unshipped record survives age pruning");
        let batch = store.offer(SHIPPER, 100).unwrap();
        store.acknowledge(&batch, &batch.receipt()).unwrap();
        age_all(&store);
        store.prune().unwrap();
        assert_eq!(store.storage().unwrap()["pruned_count"], 100);
        let left = store.export(0, 0, 1024).unwrap();
        assert_eq!(left["events"][0]["seq"], 101, "the first unshipped record stays");
        assert_eq!(left["events"].as_array().unwrap().len(), 50);
        // The next append reaches the same pruning path and keeps the same floor.
        store.append(&[event("check")]).unwrap();
        assert_eq!(store.storage().unwrap()["pruned_count"], 100);
        {
            let db = store.db.lock().unwrap();
            prune(&db, clock().unwrap(), AGE_MS, 0).unwrap();
        }
        assert_eq!(store.storage().unwrap()["pruned_count"], 100, "the logical bound stops at the cursor too");
        let mut d11 = D11::default();
        drain(&store, &connection, &mut d11).unwrap();
        assert_eq!(seqs(&d11), (101..=151).collect::<Vec<_>>(), "no unshipped record was lost to pruning");
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }

    // Defect: capacity pruning ignored the cursor. Protects R31: the cap holds by refusing, and retained rows stay.
    #[test]
    fn a_store_full_of_unshipped_records_refuses_and_recovers() {
        let _disk = crate::bulk_disk();
        let (path, connection, dir) = setup("telemetry-ship-full");
        let budget = super::super::Budget { lock: path.with_extension("budget"), peers: vec![], total: 2 << 20, reserved: 0 };
        let store = Store::open_bounded(&path, budget).unwrap();
        store.ship(&connection, &mut Backoff::default(), &mut D11::default()).unwrap();
        let big = |n: usize| {
            let mut v = value("operation");
            // Leave room for all shared fields in the recovery batch's database share.
            v["counts"] = (0..60).map(|i| (format!("{n:06}.{i:02}.{}", crate::sha256_hex(format!("{n}.{i}").as_bytes())), json!(i))).collect::<Map<_, _>>().into();
            Event::observation(v).unwrap()
        };
        // Steps down to single records, so appends fill the last page they may use.
        let (mut appended, mut size) = (0, 32);
        let refusal = loop {
            match store.append(&(0..size).map(|i| big(appended + i)).collect::<Vec<_>>()) {
                Ok(_) => appended += size,
                Err(error) if size == 1 => break error,
                Err(_) => size /= 2,
            }
            assert!(appended < 2_000_000, "the cap must refuse");
        };
        assert_eq!(refusal, "telemetry physical capacity exhausted");
        assert_eq!(store.storage().unwrap()["pruned_count"], 0);
        assert_eq!(store.shipper().unwrap()["backlog_rows"], appended);
        let mut d11 = D11::default();
        drain(&store, &connection, &mut d11).expect("a full store still moves its cursor");
        assert_eq!(d11.stored.len(), appended);
        assert_eq!(store.shipper().unwrap()["cursor"], appended, "the cursor reaches every record");
        store.append(&(0..32).map(|i| big(appended + i)).collect::<Vec<_>>()).expect("the refused batch fits once shipped rows are pruned");
        assert!(store.storage().unwrap()["pruned_count"].as_u64().unwrap() > 0, "shipped rows make room");
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }

    // Defect: pruning one part behind the cursor would ship half a summary. Protects whole-summary retention.
    #[test]
    fn a_split_summary_across_the_cursor_stays_whole() {
        let _disk = crate::bulk_disk();
        let (path, connection, dir) = setup("telemetry-ship-split");
        let store = Store::open(&path).unwrap();
        let mut logical = value("operation");
        logical["counts"] = (0..1000).map(|i| (format!("GET:issues.N.route{i:05}"), json!(i))).collect::<Map<_, _>>().into();
        let parts = Event::split(logical).unwrap();
        assert!(parts.len() > 1);
        store.append(&parts).unwrap();
        store.ship(&connection, &mut Backoff::default(), &mut D11 { offline: true, ..D11::default() }).unwrap_err();
        let batch = store.offer(SHIPPER, 1).unwrap();
        store.acknowledge(&batch, &batch.receipt()).unwrap();
        age_all(&store);
        store.prune().unwrap();
        assert_eq!(store.storage().unwrap()["pruned_count"], 0);
        let mut d11 = D11::default();
        drain(&store, &connection, &mut d11).unwrap();
        age_all(&store);
        store.prune().unwrap();
        assert_eq!(store.storage().unwrap()["pruned_count"], parts.len());
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }

    // Defect: a grant without the schema would pass the cursor over every record. Protects R53 no-loss.
    #[test]
    fn a_grant_refusal_holds_the_cursor_and_a_record_refusal_is_counted_once() {
        let _disk = crate::bulk_disk();
        let (path, connection, dir) = setup("telemetry-ship-reject");
        let store = Store::open(&path).unwrap();
        assert_eq!(store.shipper().unwrap()["state"], "off");
        store.append(&[event("operation"), event("check")]).unwrap();
        let mut scope = D11 { reject: Some("scope"), ..D11::default() };
        assert_eq!(store.ship(&connection, &mut Backoff::default(), &mut scope), Err(Delivery::Stopped("scope".into())));
        assert_eq!(store.shipper().unwrap()["cursor"], 0);
        let mut payload = D11 { reject: Some("payload"), ..D11::default() };
        assert_eq!(store.ship(&connection, &mut Backoff::default(), &mut payload), Ok(Shipped { sent: 0, rejected: 2, more: false }));
        assert_eq!(store.ship(&connection, &mut Backoff::default(), &mut payload), Ok(Shipped::default()));
        let status = store.shipper().unwrap();
        assert_eq!(status["cursor"], 2);
        assert_eq!(status["rejected"]["payload"], 2);
        drop(store);
        assert_eq!(Store::open(&path).unwrap().shipper().unwrap()["rejected"]["payload"], 2);
        std::fs::remove_dir_all(dir).unwrap();
    }

    // Defect: one pass could exceed the D11 body limit or burst. Protects R53 bounds with maximum-size records.
    #[test]
    fn a_pass_stays_inside_the_request_limits() {
        let _disk = crate::bulk_disk();
        let (path, connection, dir) = setup("telemetry-ship-limits");
        let store = Store::open(&path).unwrap();
        let big = |n: usize| {
            let mut v = value("operation");
            v["counts"] = (0..300).map(|i| (format!("{n:06}.route.{i:04}.{}", "x".repeat(16)), json!(i))).collect::<Map<_, _>>().into();
            Event::observation(v).unwrap()
        };
        // One oversized batch covers the byte bound; small rows cover the per-pass request bound.
        store.append(&(0..100).map(big).collect::<Vec<_>>()).unwrap();
        store.append(&(0..701).map(|_| event("operation")).collect::<Vec<_>>()).unwrap();
        struct Limits(D11);
        impl Wire for Limits {
            fn post(&mut self, d: &str, route: &str, auth: Option<&str>, body: &Value) -> Result<(u16, Value), String> {
                assert!(body.to_string().len() <= enroll::MAX_BODY);
                if self.0.requests == 0 {
                    assert!(body["records"].as_array().unwrap().len() < enroll::MAX_RECORDS, "large records must exercise the byte bound");
                }
                self.0.post(d, route, auth, body)
            }
            fn now(&self) -> u64 { 0 }
            fn sleep(&mut self, _: u64) {}
            fn show(&mut self, _: &str, _: &str) {}
        }
        let mut wire = Limits(D11::default());
        let pass = store.ship(&connection, &mut Backoff::default(), &mut wire).unwrap();
        assert!(pass.more);
        assert_eq!(wire.0.requests, REQUESTS);
        while store.ship(&connection, &mut Backoff::default(), &mut wire).unwrap().more {}
        assert_eq!(seqs(&wire.0), (1..=801).collect::<Vec<_>>());
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
