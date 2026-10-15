//! Shared local observations. Storage never performs network I/O or changes consumer state.
use ring::rand::{SecureRandom, SystemRandom};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::{Value, json};
use std::{fs::TryLockError, path::Path, sync::Mutex, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};

pub mod capture;
mod broker;

type Res<T> = Result<T, String>;
const AGE_MS: i64 = 30 * 24 * 60 * 60 * 1000;
const PAYLOAD_BYTES: i64 = 64 * 1024 * 1024;
const EVENT_BYTES: usize = 16 * 1024;
const BATCH: usize = 1024;
const APPLICATION: i64 = 0x43515431;
const TEXT: &[&str] = &["product", "version", "operation", "kind", "trace", "span", "parent", "outcome", "error", "project", "host", "runtime", "model", "project_spec", "shared_spec", "client_version"];
const NUMBERS: &[&str] = &["started_ms", "duration_us", "active_us", "task", "attempt", "input_tokens", "output_tokens", "cache_read_tokens", "cache_write_tokens", "context_tokens", "compressed_before", "compressed_after", "width", "height", "scale_milli"];

fn clock() -> Res<i64> {
    SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| String::from("telemetry clock before epoch"))?
        .as_millis().try_into().map_err(|_| "telemetry clock overflow".into())
}
fn code(s: &str) -> bool {
    !s.is_empty() && s.len() <= 128 && s.bytes().all(|c| c.is_ascii_alphanumeric() || b"._:-".contains(&c))
}
fn err(e: impl std::fmt::Display) -> String { format!("telemetry: {e}") }

/// Validated immutable observation; cloning it preserves its id for idempotent retry.
#[derive(Clone, Debug)]
pub struct Event { id: String, started_ms: i64, payload: String }
impl Event {
    pub fn new(value: Value) -> Res<Self> {
        let object = value.as_object().ok_or("telemetry event must be an object")?;
        for required in ["product", "version", "operation", "kind", "trace", "span", "started_ms", "outcome", "error"] {
            if object.get(required).is_none_or(Value::is_null) { return Err(format!("telemetry missing {required}")); }
        }
        for (k, v) in object {
            let valid = if TEXT.contains(&k.as_str()) {
                v.is_null() || v.as_str().is_some_and(|s| if k == "project" {
                    s.split_once('/').is_some_and(|(a,b)| code(a) && code(b))
                } else { code(s) })
            } else if NUMBERS.contains(&k.as_str()) {
                v.is_null() || v.as_u64().is_some_and(|n| n <= i64::MAX as u64)
            } else if k == "unknown" {
                v.as_object().is_some_and(|m| m.iter().all(|(name, reason)| NUMBERS.contains(&name.as_str()) && reason.as_str().is_some_and(code)))
            } else { false };
            if !valid { return Err(format!("telemetry invalid field {k}")); }
        }
        if !matches!(value["outcome"].as_str(), Some("ok" | "failed" | "refused" | "cancelled" | "retried" | "unknown")) {
            return Err("telemetry invalid outcome".into());
        }
        for k in NUMBERS {
            if object.get(*k).is_some_and(Value::is_null) && value["unknown"][*k].as_str().is_none() {
                return Err(format!("telemetry null {k} needs an unknown reason"));
            }
        }
        let payload = value.to_string();
        if payload.len() > EVENT_BYTES { return Err("telemetry event too large".into()); }
        let mut id = [0;16]; SystemRandom::new().fill(&mut id).map_err(|_| "telemetry random id unavailable")?;
        Ok(Self { id: id.iter().map(|b| format!("{b:02x}")).collect(), started_ms: value["started_ms"].as_i64().ok_or("telemetry invalid start")?, payload })
    }
    pub fn id(&self) -> &str { &self.id }
}

#[derive(Clone, Debug, PartialEq)]
struct Row { seq: i64, id: String, received_ms: i64, payload: String }
impl Row {
    fn json(&self) -> Res<Value> {
        Ok(json!({"seq":self.seq,"id":self.id,"received_ms":self.received_ms,"observation":serde_json::from_str::<Value>(&self.payload).map_err(err)?}))
    }
}

/// Opaque next batch: only the store creates it; acknowledgment cannot skip an unoffered range.
#[derive(Clone, Debug)]
pub struct Batch { destination: String, after: i64, rows: Vec<Row>, pruned_through: i64 }
impl Batch {
    pub fn is_empty(&self) -> bool { self.rows.is_empty() }
    pub fn json(&self) -> Res<Value> {
        Ok(json!({"schema":1,"after":self.after,"gap":self.pruned_through > self.after,
            "pruned_through":self.pruned_through,"receipt":self.receipt(),
            "events":self.rows.iter().map(Row::json).collect::<Res<Vec<_>>>()?}))
    }
    pub fn receipt(&self) -> String {
        crate::sha256_hex(&serde_json::to_vec(&json!([self.destination,self.after,self.rows.iter().map(|r| (&r.id,r.seq)).collect::<Vec<_>>()])).expect("numeric batch serialization"))
    }
}

/// Reuse one store per process. SQLite WAL serializes separate processes, too.
pub struct Store { db: Mutex<Connection> }
impl Store {
    pub fn open(path: &Path) -> Res<Self> {
        let mut name = path.as_os_str().to_os_string(); name.push(".init.lock");
        let until = Instant::now() + Duration::from_secs(1);
        let _initialization = loop { match crate::file_lock::try_lock(Path::new(&name)) {
            Ok(lock) => break lock,
            Err(TryLockError::WouldBlock) if Instant::now() < until => std::thread::sleep(Duration::from_millis(5)),
            Err(e) => return Err(err(e)),
        }};
        let existed = path.exists();
        let db = Connection::open(path).map_err(err)?;
        db.busy_timeout(Duration::from_secs(1)).map_err(err)?;
        if existed {
            let version: i64 = db.pragma_query_value(None,"user_version",|r|r.get(0)).map_err(err)?;
            let application: i64 = db.pragma_query_value(None,"application_id",|r|r.get(0)).map_err(err)?;
            if version != 1 || application != APPLICATION { return Err(format!("telemetry unsupported schema {version} or foreign application; store preserved")); }
            let check: String = db.query_row("PRAGMA quick_check",[],|r|r.get(0)).map_err(err)?;
            if check != "ok" { return Err("telemetry corrupt store preserved".into()); }
        }
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA journal_size_limit=8388608;").map_err(err)?;
        if !existed {
            db.execute_batch("BEGIN IMMEDIATE;
                CREATE TABLE events(seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL UNIQUE,received_ms INTEGER NOT NULL,started_ms INTEGER NOT NULL,payload TEXT NOT NULL,bytes INTEGER NOT NULL);
                CREATE INDEX events_started ON events(started_ms,seq);
                CREATE INDEX events_received ON events(received_ms,seq);
                CREATE TABLE metadata(id INTEGER PRIMARY KEY CHECK(id=1),retained_bytes INTEGER NOT NULL,pruned_count INTEGER NOT NULL,pruned_through INTEGER NOT NULL);
                INSERT INTO metadata VALUES(1,0,0,0);
                CREATE TABLE cursors(destination TEXT PRIMARY KEY,seq INTEGER NOT NULL,receipt TEXT NOT NULL);
                CREATE TRIGGER inserted AFTER INSERT ON events BEGIN UPDATE metadata SET retained_bytes=retained_bytes+new.bytes WHERE id=1; END;
                CREATE TRIGGER deleted AFTER DELETE ON events BEGIN UPDATE metadata SET retained_bytes=retained_bytes-old.bytes,pruned_count=pruned_count+1,pruned_through=max(pruned_through,old.seq) WHERE id=1; END;
                PRAGMA user_version=1; PRAGMA application_id=1129403441; COMMIT;").map_err(err)?;
        }
        Ok(Self { db: Mutex::new(db) })
    }
    /// One durable group commit. On failure no observation in this batch becomes visible.
    pub fn append(&self, events: &[Event]) -> Res<Vec<i64>> {
        if events.len() > BATCH { return Err("telemetry batch too large".into()); }
        let mut db = self.db.lock().map_err(|_| "telemetry writer poisoned")?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(err)?;
        let now = clock()?;
        let mut ids = Vec::with_capacity(events.len());
        {
            let mut insert = tx.prepare_cached("INSERT INTO events(id,received_ms,started_ms,payload,bytes) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(id) DO NOTHING").map_err(err)?;
            let mut read = tx.prepare_cached("SELECT seq,payload FROM events WHERE id=?1").map_err(err)?;
            for e in events {
                insert.execute(params![e.id,now,e.started_ms,e.payload,e.payload.len() as i64]).map_err(err)?;
                let (seq,payload): (i64,String)=read.query_row([&e.id],|r|Ok((r.get(0)?,r.get(1)?))).map_err(err)?;
                if payload!=e.payload { return Err("telemetry immutable id payload mismatch".into()); }
                ids.push(seq);
            }
        }
        prune(&tx,now,AGE_MS,PAYLOAD_BYTES)?;
        tx.commit().map_err(err)?;
        Ok(ids)
    }
    pub fn export(&self, since_ms: i64, after: i64, limit: usize) -> Res<Value> {
        if !(1..=BATCH).contains(&limit) { return Err("telemetry limit must be 1..1024".into()); }
        let mut connection = self.db.lock().map_err(|_| "telemetry reader poisoned")?;
        let db = connection.transaction().map_err(err)?;
        let mut stmt = db.prepare("SELECT seq,id,received_ms,payload FROM events WHERE started_ms>=?1 AND seq>?2 ORDER BY seq LIMIT ?3").map_err(err)?;
        let rows = stmt.query_map(params![since_ms,after,limit as i64],row).map_err(err)?.collect::<Result<Vec<_>,_>>().map_err(err)?;
        let (pruned_count,pruned_through): (i64,i64) = db.query_row("SELECT pruned_count,pruned_through FROM metadata WHERE id=1",[],|r|Ok((r.get(0)?,r.get(1)?))).map_err(err)?;
        let bounds: (Option<i64>,Option<i64>,Option<i64>,Option<i64>) = db.query_row("SELECT min(seq),max(seq),min(started_ms),max(started_ms) FROM events",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).map_err(err)?;
        let next = rows.last().map_or(after,|r|r.seq);
        let more: bool = db.query_row("SELECT EXISTS(SELECT 1 FROM events WHERE seq>?1 AND started_ms>=?2)",params![next,since_ms],|r|r.get(0)).map_err(err)?;
        Ok(json!({"schema":1,"since_ms":since_ms,"after":after,"next":next,"more":more,"retained_seq":[bounds.0,bounds.1],"retained_ms":[bounds.2,bounds.3],"pruned_count":pruned_count,"pruned_through":pruned_through,
            "events":rows.iter().map(Row::json).collect::<Res<Vec<_>>>()?}))
    }
    pub fn offer(&self, destination: &str, limit: usize) -> Res<Batch> {
        if !code(destination) || !(1..=BATCH).contains(&limit) { return Err("telemetry invalid destination or limit".into()); }
        let mut connection = self.db.lock().map_err(|_| "telemetry reader poisoned")?;
        let db = connection.transaction().map_err(err)?;
        let after = db.query_row("SELECT seq FROM cursors WHERE destination=?1",[destination],|r|r.get(0)).optional().map_err(err)?.unwrap_or(0);
        offer(&db,destination,after,limit)
    }
    /// Call only after the authenticated destination proves durable acceptance of this receipt.
    pub fn acknowledge(&self, batch: &Batch, receipt: &str) -> Res<()> {
        if batch.is_empty() || receipt != batch.receipt() { return Err("telemetry receipt mismatch or empty batch".into()); }
        let mut db = self.db.lock().map_err(|_| "telemetry writer poisoned")?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(err)?;
        let current: Option<(i64,String)> = tx.query_row("SELECT seq,receipt FROM cursors WHERE destination=?1",[&batch.destination],|r|Ok((r.get(0)?,r.get(1)?))).optional().map_err(err)?;
        let through = batch.rows.last().ok_or("telemetry empty batch")?.seq;
        if current.as_ref().is_some_and(|(s,r)| *s==through && r==receipt) { return Ok(()); }
        let new_destination = current.is_none();
        if current.map(|v|v.0).unwrap_or(0) != batch.after || offer(&tx,&batch.destination,batch.after,batch.rows.len())?.rows != batch.rows {
            return Err("telemetry stale batch; reconcile before retry".into());
        }
        let destinations: i64 = tx.query_row("SELECT count(*) FROM cursors",[],|r|r.get(0)).map_err(err)?;
        if new_destination && destinations >= 32 { return Err("telemetry destination limit reached".into()); }
        tx.execute("INSERT INTO cursors(destination,seq,receipt) VALUES(?1,?2,?3) ON CONFLICT(destination) DO UPDATE SET seq=excluded.seq,receipt=excluded.receipt",params![batch.destination,through,receipt]).map_err(err)?;
        tx.commit().map_err(err)
    }
    pub fn prune(&self) -> Res<()> {
        let mut db = self.db.lock().map_err(|_| "telemetry writer poisoned")?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(err)?;
        prune(&tx,clock()?,AGE_MS,PAYLOAD_BYTES)?;
        tx.commit().map_err(err)?;
        let busy: i64 = db.query_row("PRAGMA wal_checkpoint(TRUNCATE)",[],|r|r.get(0)).map_err(err)?;
        if busy != 0 { return Err("telemetry checkpoint busy; retained data preserved".into()); }
        Ok(())
    }
}
fn row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Row> {
    Ok(Row { seq:r.get(0)?,id:r.get(1)?,received_ms:r.get(2)?,payload:r.get(3)? })
}
fn offer(db: &Connection, destination: &str, after: i64, limit: usize) -> Res<Batch> {
    let mut stmt = db.prepare("SELECT seq,id,received_ms,payload FROM events WHERE seq>?1 ORDER BY seq LIMIT ?2").map_err(err)?;
    let rows = stmt.query_map(params![after,limit as i64],row).map_err(err)?.collect::<Result<Vec<_>,_>>().map_err(err)?;
    let pruned_through = db.query_row("SELECT pruned_through FROM metadata WHERE id=1",[],|r|r.get(0)).map_err(err)?;
    Ok(Batch { destination:destination.into(),after,rows,pruned_through })
}
fn prune(db: &Connection, now: i64, age: i64, bytes: i64) -> Res<()> {
    db.execute("DELETE FROM events WHERE received_ms<?1",[now.saturating_sub(age)]).map_err(err)?;
    let mut total: i64 = db.query_row("SELECT retained_bytes FROM metadata WHERE id=1",[],|r|r.get(0)).map_err(err)?;
    while total > bytes {
        let (seq,n): (i64,i64) = db.query_row("SELECT seq,bytes FROM events ORDER BY seq LIMIT 1",[],|r|Ok((r.get(0)?,r.get(1)?))).map_err(err)?;
        db.execute("DELETE FROM events WHERE seq=?1",[seq]).map_err(err)?;
        total-=n;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, io::{BufRead, Write}, process::{Command, Stdio}, sync::Arc};
    fn event(op: &str) -> Event {
        Event::new(json!({"product":"testq","version":"1.0.0","operation":op,"kind":"operation",
            "trace":"trace1","span":"span1","started_ms":1000,"duration_us":10,"outcome":"ok","error":"none"})).unwrap()
    }
    // Chain: local commit -> agent export -> optional receipt -> retained local history, with failed batches isolated.
    #[test]
    fn durable_export_sync_and_failure_are_one_chain() {
        let dir = crate::scratch("telemetry-chain"); let path = dir.join("telemetry.sqlite3");
        let store = Store::open(&path).unwrap(); let e = [event("ui.resume"),event("window.resize")];
        let seq = store.append(&e).unwrap(); assert_eq!(store.append(&e).unwrap(),seq,"retry preserves immutable ids");
        let page = store.export(0,0,1).unwrap(); assert_eq!(page["more"],true);
        let next = store.export(0,page["next"].as_i64().unwrap(),1).unwrap(); assert_eq!(next["events"][0]["id"],e[1].id());
        let batch = store.offer("global",1).unwrap();
        assert!(store.acknowledge(&batch,"uncertain-network-write").is_err());
        assert_eq!(store.offer("global",1).unwrap().receipt(),batch.receipt());
        store.acknowledge(&batch,&batch.receipt()).unwrap(); store.acknowledge(&batch,&batch.receipt()).unwrap();
        assert_ne!(store.offer("global",1).unwrap().receipt(),batch.receipt());
        assert_eq!(store.offer("second-destination",1).unwrap().rows[0].id,e[0].id());
        assert_eq!(store.export(0,0,1024).unwrap()["events"].as_array().unwrap().len(),2,"sync never consumes local history");
        store.db.lock().unwrap().execute_batch("CREATE TRIGGER fail_probe BEFORE INSERT ON events WHEN new.payload LIKE '%refused.probe%' BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
        assert!(store.append(&[event("ordinary.probe"),event("refused.probe")]).is_err());
        assert_eq!(store.export(0,0,1024).unwrap()["events"].as_array().unwrap().len(),2,"whole batch rolled back");
        drop(store); let store = Store::open(&path).unwrap();
        assert_eq!(store.export(0,0,1024).unwrap()["events"].as_array().unwrap().len(),2);
        assert_eq!(store.offer("global",1).unwrap().rows[0].id,e[1].id());
        drop(store); fs::remove_dir_all(dir).unwrap();
    }
    // Chain: oldest whole observations expire -> export names missing history -> stale sync cannot hide the gap.
    #[test]
    fn retention_reports_gaps_and_preserves_newest_whole_rows() {
        let dir = crate::scratch("telemetry-retention"); let store = Store::open(&dir.join("events.sqlite3")).unwrap();
        let e = [event("first"),event("second")]; let seq = store.append(&e).unwrap(); let batch = store.offer("global",2).unwrap();
        { let db = store.db.lock().unwrap(); prune(&db,clock().unwrap(),AGE_MS,e[1].payload.len() as i64).unwrap(); }
        let export = store.export(0,0,10).unwrap(); assert_eq!(export["pruned_count"],1); assert_eq!(export["events"][0]["id"],e[1].id());
        assert!(store.acknowledge(&batch,&batch.receipt()).is_err());
        assert_eq!(store.offer("global",2).unwrap().json().unwrap()["gap"],true);
        { let db = store.db.lock().unwrap();
          db.execute("UPDATE events SET received_ms=1 WHERE seq=?1",[seq[1]]).unwrap();
          prune(&db,AGE_MS+2,AGE_MS,PAYLOAD_BYTES).unwrap(); }
        assert_eq!(store.export(0,0,10).unwrap()["pruned_count"],2);
        assert!(store.offer("global",2).unwrap().is_empty());
        drop(store); fs::remove_dir_all(dir).unwrap();
    }
    // Chain: independent products/process connections append concurrently -> one complete committed sequence.
    #[test]
    fn concurrent_connections_do_not_lose_observations() {
        let dir = crate::scratch("telemetry-concurrent"); let path = Arc::new(dir.join("events.sqlite3"));
        std::thread::scope(|scope| { for _ in 0..4 { let p=path.clone(); scope.spawn(move || {
            let writer = Store::open(&p).unwrap(); for _ in 0..25 { writer.append(&[event("method.call")]).unwrap(); }
        }); }});
        let store = Store::open(&path).unwrap();
        assert_eq!(store.export(0,0,1024).unwrap()["events"].as_array().unwrap().len(),100);
        drop(store); fs::remove_dir_all(dir).unwrap();
    }
    // Chain: unsafe observations or foreign/corrupt/newer databases -> explicit refusal with original bytes retained.
    #[test]
    fn invalid_data_and_foreign_stores_are_preserved() {
        let mut bad = serde_json::from_str::<Value>(&event("test").payload).unwrap(); bad["arguments"]=json!("secret");
        assert!(Event::new(bad).is_err());
        let mut bad = serde_json::from_str::<Value>(&event("test").payload).unwrap(); bad["duration_us"]=Value::Null;
        assert!(Event::new(bad.clone()).is_err()); bad["unknown"]=json!({"duration_us":"not-observed"}); assert!(Event::new(bad).is_ok());
        let dir=crate::scratch("telemetry-corrupt"); let p=dir.join("db"); let bytes=b"foreign file must survive";fs::write(&p,bytes).unwrap();
        assert!(Store::open(&p).is_err());assert_eq!(fs::read(&p).unwrap(),bytes);
        fs::remove_file(&p).unwrap();let store=Store::open(&p).unwrap();store.db.lock().unwrap().pragma_update(None,"user_version",2).unwrap();drop(store);
        let before=fs::read(&p).unwrap();assert!(Store::open(&p).is_err());assert_eq!(fs::read(&p).unwrap(),before);
        let foreign=dir.join("foreign.sqlite3");
        {let db=Connection::open(&foreign).unwrap();db.execute_batch("PRAGMA user_version=1; CREATE TABLE foreign_state(value TEXT); INSERT INTO foreign_state VALUES('keep');").unwrap();}
        let before=fs::read(&foreign).unwrap();assert!(Store::open(&foreign).is_err());assert_eq!(fs::read(&foreign).unwrap(),before);
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    #[ignore = "subprocess of acknowledged_commit_survives_kill"]
    fn crash_writer() {
        let Some(p)=std::env::var_os("COREQ_TELEMETRY_CRASH_PROBE") else {return};
        let store=Store::open(Path::new(&p)).unwrap();store.append(&[event("acknowledged.before.kill")]).unwrap();
        println!("COMMITTED");std::io::stdout().flush().unwrap();loop {std::thread::park();}
    }
    // Chain: acknowledged FULL commit -> forced process termination -> reopen contains that exact observation.
    #[test]
    fn acknowledged_commit_survives_kill() {
        let dir=crate::scratch("telemetry-crash");let p=dir.join("db");
        let mut child=Command::new(std::env::current_exe().unwrap()).args(["--exact","telemetry::tests::crash_writer","--ignored","--nocapture"])
            .env("COREQ_TELEMETRY_CRASH_PROBE",&p).stdout(Stdio::piped()).stderr(Stdio::inherit()).spawn().unwrap();
        let mut committed=false;
        for line in std::io::BufReader::new(child.stdout.take().unwrap()).lines() {if line.unwrap().trim()=="COMMITTED" {committed=true;break;}}
        let _=child.kill();child.wait().unwrap();assert!(committed,"child never acknowledged a commit");
        let store=Store::open(&p).unwrap();let rows=store.export(0,0,10).unwrap();assert_eq!(rows["events"][0]["observation"]["operation"],"acknowledged.before.kill");
        drop(store);fs::remove_dir_all(dir).unwrap();
    }
}
