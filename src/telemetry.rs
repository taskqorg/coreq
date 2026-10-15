//! Shared local observations. Storage never performs network I/O or changes consumer state.
use ring::rand::{SecureRandom, SystemRandom};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::{Value, json};
use std::{fs::TryLockError, path::{Path,PathBuf}, sync::Mutex, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};

pub mod capture;
mod broker;

type Res<T> = Result<T, String>;
const AGE_MS: i64 = 30 * 24 * 60 * 60 * 1000;
const PAYLOAD_BYTES: i64 = 64 * 1024 * 1024;
const DATABASE_BYTES: i64 = 28 * 1024 * 1024;
const WAL_BYTES: u64 = 29 * 1024 * 1024;
const SHM_BYTES: u64 = 1024 * 1024;
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

const FAILURE_CODES: &[&str]=&["auth","invalid-frame","invalid-observation","id-conflict","sqlite-busy","store-full","store-readonly","store-io","store-failed","flush-deadline","queue-unavailable","transport-unavailable","collection-incomplete","unknown"];
fn failure_code(error: &str) -> &'static str {
    if let Some(code)=error.strip_prefix("telemetry IPC rejected: ") {return FAILURE_CODES.iter().copied().find(|c|*c==code).unwrap_or("unknown");}
    if error=="telemetry IPC refused" {return "auth";}
    if error=="telemetry checkpoint busy; retained data preserved" {return "sqlite-busy";}
    if error=="telemetry physical capacity exhausted" {return "store-full";}
    if error=="telemetry immutable id payload mismatch" {return "id-conflict";}
    if error.starts_with("telemetry invalid")||error.starts_with("telemetry missing")||error.starts_with("telemetry null")||error=="telemetry event too large" {return "invalid-observation";}
    if error.starts_with("telemetry append.")||error.starts_with("telemetry import.")||error.starts_with("telemetry: ") {
        if error.contains("database is locked")||error.contains("database table is locked") {return "sqlite-busy";}
        if error.contains("database or disk is full") {return "store-full";}
        if error.contains("readonly database") {return "store-readonly";}
        if error.contains("disk I/O error") {return "store-io";}
        return "store-failed";
    }
    if error.starts_with("telemetry flush") {return "flush-deadline";}
    if error=="telemetry collection incomplete; observations lost" {return "collection-incomplete";}
    if error=="telemetry writer stopped" {return "queue-unavailable";}
    if error.starts_with("telemetry IPC") {
        if error.contains("unavailable")||error.contains("unacknowledged")||error.contains("deadline") {return "transport-unavailable";}
        return "invalid-frame";
    }
    "unknown"
}

/// Validated immutable observation; cloning it preserves its id for idempotent retry.
#[derive(Clone, Debug)]
pub struct Event { id: String, started_ms: i64, payload: String }
impl Event {
    pub fn new(value: Value) -> Res<Self> {Self::with_id(value,None)}
    fn with_id(value: Value, supplied: Option<&str>) -> Res<Self> {
        if supplied.is_some_and(|id|id.len()!=32||!id.bytes().all(|b|b.is_ascii_hexdigit()&&!b.is_ascii_uppercase())) {return Err("telemetry invalid event id".into());}
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
        let id=if let Some(id)=supplied {id.to_owned()} else {
            let mut id=[0;16];SystemRandom::new().fill(&mut id).map_err(|_|"telemetry random id unavailable")?;
            id.iter().map(|b|format!("{b:02x}")).collect()
        };
        Ok(Self { id, started_ms: value["started_ms"].as_i64().ok_or("telemetry invalid start")?, payload })
    }
    /// Stable source position, independent of record contents: a changed row conflicts rather than overwriting.
    pub fn identified(namespace: &str, key: &str, value: Value) -> Res<Self> {
        if !code(namespace)||!code(key) { return Err("telemetry invalid import identity".into()); }
        Self::with_id(value,Some(&crate::sha256_hex(format!("{namespace}\0{key}").as_bytes())[..32]))
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
pub struct Store { db: Mutex<Connection>, write_lock: PathBuf, path: PathBuf }
impl Store {
    pub fn open(path: &Path) -> Res<Self> {
        let mut name = path.as_os_str().to_os_string(); name.push(".init.lock");
        let until = Instant::now() + Duration::from_secs(1);
        let _initialization = loop { match crate::file_lock::try_lock(Path::new(&name)) {
            Ok(lock) => break lock,
            Err(TryLockError::WouldBlock) if Instant::now() < until => std::thread::sleep(Duration::from_millis(5)),
            Err(e) => return Err(err(e)),
        }};
        let mut lock_path=path.as_os_str().to_os_string();lock_path.push(".write.lock");
        let write_lock=PathBuf::from(lock_path);
        let _write=crate::file_lock::lock(&write_lock).map_err(err)?;
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
        let page_size: i64=db.pragma_query_value(None,"page_size",|r|r.get(0)).map_err(err)?;
        let pages: i64=db.pragma_query_value(None,"page_count",|r|r.get(0)).map_err(err)?;
        if page_size<1024||pages>DATABASE_BYTES/page_size {return Err("telemetry oversized or unsupported-page store preserved; migration required".into());}
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA temp_store=MEMORY; PRAGMA cache_spill=OFF; PRAGMA cache_size=-32768; PRAGMA wal_autocheckpoint=0;").map_err(err)?;
        db.pragma_update(None,"max_page_count",DATABASE_BYTES/page_size).map_err(err)?;
        checkpoint(&db,true)?;
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
        Ok(Self { db: Mutex::new(db), write_lock, path: path.to_owned() })
    }
    /// One durable group commit. On failure no observation in this batch becomes visible.
    pub fn append(&self, events: &[Event]) -> Res<Vec<i64>> {
        if events.len() > BATCH { return Err("telemetry batch too large".into()); }
        let mut db = self.db.lock().map_err(|_| "telemetry writer poisoned")?;
        let _write=crate::file_lock::lock(&self.write_lock).map_err(err)?;
        checkpoint(&db,false)?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(|e|format!("telemetry append.begin: {e}"))?;
        let ids=append_to(&tx,events,clock()?)?;
        tx.commit().map_err(|e|format!("telemetry append.commit: {e}"))?;
        Ok(ids)
    }
    pub fn import_cursor(&self, source: &str) -> Res<Option<String>> {
        if !code(source) { return Err("telemetry invalid import source".into()); }
        let db=self.db.lock().map_err(|_|"telemetry reader poisoned")?;
        let exists: bool=db.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='imports')",[],|r|r.get(0)).map_err(err)?;
        if !exists { return Ok(None); }
        db.query_row("SELECT cursor FROM imports WHERE source=?1",[source],|r|r.get(0)).optional().map_err(err)
    }
    /// Batch and progress are one transaction; no partial import can advance the source position.
    pub fn import(&self, source: &str, expected: Option<&str>, next: &str, events: &[Event]) -> Res<()> {
        if !code(source)||!code(next)||expected.is_some_and(|v|!code(v)) { return Err("telemetry invalid import cursor".into()); }
        if events.len()>BATCH { return Err("telemetry batch too large".into()); }
        let receipt=crate::sha256_hex(&serde_json::to_vec(&json!([source,expected,next,events.iter().map(|e|(&e.id,&e.payload)).collect::<Vec<_>>()])).map_err(err)?);
        let mut db=self.db.lock().map_err(|_|"telemetry writer poisoned")?;
        let _write=crate::file_lock::lock(&self.write_lock).map_err(err)?;
        checkpoint(&db,false)?;
        let tx=db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(|e|format!("telemetry import.begin: {e}"))?;
        reserve(&tx,events,32)?;
        tx.execute_batch("CREATE TABLE IF NOT EXISTS imports(source TEXT PRIMARY KEY,cursor TEXT NOT NULL,receipt TEXT NOT NULL)").map_err(err)?;
        let previous: Option<(String,String)>=tx.query_row("SELECT cursor,receipt FROM imports WHERE source=?1",[source],|r|Ok((r.get(0)?,r.get(1)?))).optional().map_err(err)?;
        if previous.as_ref().is_some_and(|(cursor,_)|cursor==next) {
            if previous.as_ref().unwrap().1==receipt { return Ok(()); }
            return Err("telemetry import receipt mismatch".into());
        }
        if previous.as_ref().map(|(cursor,_)|cursor.as_str())!=expected { return Err("telemetry import cursor conflict".into()); }
        let count: i64=tx.query_row("SELECT count(*) FROM imports",[],|r|r.get(0)).map_err(err)?;
        if previous.is_none()&&count>=32 { return Err("telemetry import source limit reached".into()); }
        append_to(&tx,events,clock()?)?;
        tx.execute("INSERT INTO imports(source,cursor,receipt) VALUES(?1,?2,?3) ON CONFLICT(source) DO UPDATE SET cursor=excluded.cursor,receipt=excluded.receipt",params![source,next,receipt]).map_err(err)?;
        tx.commit().map_err(|e|format!("telemetry import.commit: {e}"))
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
        let _write=crate::file_lock::lock(&self.write_lock).map_err(err)?;
        checkpoint(&db,false)?;
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
        reserve(&tx,&[],32)?;
        tx.execute("INSERT INTO cursors(destination,seq,receipt) VALUES(?1,?2,?3) ON CONFLICT(destination) DO UPDATE SET seq=excluded.seq,receipt=excluded.receipt",params![batch.destination,through,receipt]).map_err(err)?;
        tx.commit().map_err(err)
    }
    /// Metadata only; missing sidecars are zero, inaccessible files never masquerade as empty.
    pub fn storage(&self) -> Res<Value> {
        let db=self.db.lock().map_err(|_|"telemetry reader poisoned")?;
        let file_bytes=|path: &Path,optional: bool| -> Res<u64> {match std::fs::metadata(path) {
            Ok(meta)=>Ok(meta.len()),Err(error) if optional&&error.kind()==std::io::ErrorKind::NotFound=>Ok(0),Err(error)=>Err(err(error)),
        }};
        let sidecar=|suffix: &str| {let mut name=self.path.as_os_str().to_os_string();name.push(suffix);PathBuf::from(name)};
        let database=file_bytes(&self.path,false)?;let wal=file_bytes(&sidecar("-wal"),true)?;let shm=file_bytes(&sidecar("-shm"),true)?;
        let (retained,pruned): (i64,i64)=db.query_row("SELECT retained_bytes,pruned_count FROM metadata WHERE id=1",[],|r|Ok((r.get(0)?,r.get(1)?))).map_err(err)?;
        Ok(json!({"state":if database<=DATABASE_BYTES as u64&&wal<=WAL_BYTES&&shm<=SHM_BYTES {"bounded"} else {"unknown"},
            "database_bytes":database,"wal_bytes":wal,"shm_bytes":shm,"total_bytes":database+wal+shm,
            "database_limit":DATABASE_BYTES,"wal_limit":WAL_BYTES,"shm_limit":SHM_BYTES,
            "retained_payload_bytes":retained,"pruned_count":pruned}))
    }
    pub fn prune(&self) -> Res<()> {
        let mut db = self.db.lock().map_err(|_| "telemetry writer poisoned")?;
        let _write=crate::file_lock::lock(&self.write_lock).map_err(err)?;
        checkpoint(&db,false)?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(err)?;
        prune(&tx,clock()?,AGE_MS,PAYLOAD_BYTES)?;
        tx.commit().map_err(err)?;
        checkpoint(&db,true)?;
        Ok(())
    }
}
fn checkpoint(db: &Connection, truncate: bool) -> Res<()> {
    let sql=if truncate {"PRAGMA wal_checkpoint(TRUNCATE)"} else {"PRAGMA wal_checkpoint(RESTART)"};
    let (busy,frames,done): (i64,i64,i64)=db.query_row(sql,[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).map_err(err)?;
    if busy!=0||frames!=done {return Err("telemetry checkpoint busy; retained data preserved".into());}
    Ok(())
}
fn reserve(db: &Connection, events: &[Event], extra_pages: i64) -> Res<()> {
    let mut bytes=0i64;let mut count=0i64;
    let mut existing=db.prepare_cached("SELECT payload FROM events WHERE id=?1").map_err(err)?;
    for event in events {
        match existing.query_row([&event.id],|r|r.get::<_,String>(0)).optional().map_err(err)? {
            Some(payload) if payload!=event.payload=>return Err("telemetry immutable id payload mismatch".into()),
            Some(_)=>{},None=>{bytes+=event.payload.len() as i64;count+=1;}
        }
    }
    drop(existing);
    if count==0&&extra_pages==0 {return Ok(());}
    let page_size: i64=db.pragma_query_value(None,"page_size",|r|r.get(0)).map_err(err)?;
    let required=(bytes+page_size-1)/page_size+2*count+32+extra_pages;
    loop {
        let pages: i64=db.pragma_query_value(None,"page_count",|r|r.get(0)).map_err(err)?;
        let free: i64=db.pragma_query_value(None,"freelist_count",|r|r.get(0)).map_err(err)?;
        if pages-free+required<DATABASE_BYTES/page_size {return Ok(());}
        if db.execute("DELETE FROM events WHERE seq IN (SELECT seq FROM events ORDER BY seq LIMIT 256)",[]).map_err(err)?==0 {
            return Err("telemetry physical capacity exhausted".into());
        }
    }
}
fn append_to(db: &Connection, events: &[Event], now: i64) -> Res<Vec<i64>> {
    reserve(db,events,0)?;
    let mut ids=Vec::with_capacity(events.len());
    {
        let mut insert=db.prepare_cached("INSERT INTO events(id,received_ms,started_ms,payload,bytes) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(id) DO NOTHING").map_err(err)?;
        let mut read=db.prepare_cached("SELECT seq,payload FROM events WHERE id=?1").map_err(err)?;
        for event in events {
            let inserted=insert.execute(params![event.id,now,event.started_ms,event.payload,event.payload.len() as i64]).map_err(|e|format!("telemetry append.insert: {e}"))?;
            if inserted==1 {ids.push(db.last_insert_rowid());continue;}
            let (seq,payload): (i64,String)=read.query_row([&event.id],|r|Ok((r.get(0)?,r.get(1)?))).map_err(|e|format!("telemetry append.read: {e}"))?;
            if payload!=event.payload { return Err("telemetry immutable id payload mismatch".into()); }
            ids.push(seq);
        }
    }
    prune(db,now,AGE_MS,PAYLOAD_BYTES)?;
    Ok(ids)
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
    fn large_event(operation: &str) -> Event {
        let mut value=serde_json::from_str::<Value>(&event(operation).payload).unwrap();
        for key in TEXT {if !value.as_object().unwrap().contains_key(*key)&&*key!="project" {value[*key]=json!("x".repeat(128));}}
        value["project"]=json!("fixture/project");
        let mut unknown=serde_json::Map::new();
        for key in NUMBERS {if !value.as_object().unwrap().contains_key(*key) {value[*key]=Value::Null;unknown.insert((*key).into(),json!("x".repeat(128)));}}
        value["unknown"]=Value::Object(unknown);Event::new(value).unwrap()
    }
    #[test]
    fn physical_pressure_prunes_atomically_and_bounds_database_wal_shm() {
        let dir=crate::scratch("telemetry-physical");let store=Store::open(&dir.join("db")).unwrap();
        let mut first=String::new();let mut last=String::new();let mut last_seq=0;let mut peak=0;
        for batch in 0..40 {
            let events=(0..512).map(|n|large_event(&format!("pressure.{batch}.{n}"))).collect::<Vec<_>>();
            if batch==0 {first=events[0].id.clone();}last=events.last().unwrap().id.clone();
            last_seq=*store.append(&events).unwrap().last().unwrap();
            let storage=store.storage().unwrap();assert_eq!(storage["state"],"bounded");
            peak=peak.max(storage["total_bytes"].as_u64().unwrap());
            let files=fs::read_dir(&dir).unwrap().map(|entry|entry.unwrap().metadata().unwrap().len()).sum::<u64>();
            assert!(files<64*1024*1024,"total files exceeded the consumer limit: {files}");
        }
        let before=store.storage().unwrap();assert!(before["pruned_count"].as_u64().unwrap()>0);
        let oldest=store.export(0,0,1).unwrap()["events"][0].clone();assert_ne!(oldest["id"],first);
        assert_eq!(store.export(0,last_seq-1,1).unwrap()["events"][0]["id"],last);
        let mut conflict=event("conflict");conflict.id=oldest["id"].as_str().unwrap().into();
        assert!(store.append(&[conflict]).unwrap_err().contains("immutable id"));
        assert_eq!(store.storage().unwrap()["pruned_count"],before["pruned_count"],"id validation precedes capacity deletion");
        store.db.lock().unwrap().execute_batch("CREATE TRIGGER physical_failure BEFORE INSERT ON events WHEN new.payload LIKE '%refused.probe%' BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
        let mut batch=(0..512).map(|n|large_event(&format!("rollback.{n}"))).collect::<Vec<_>>();batch.push(large_event("refused.probe"));
        assert!(store.append(&batch).unwrap_err().contains("injected"));
        assert_eq!(store.storage().unwrap()["state"],"bounded");
        assert_eq!(store.storage().unwrap()["pruned_count"],before["pruned_count"],"failed insert rolls back physical-capacity pruning");
        assert_eq!(store.export(0,0,1).unwrap()["events"][0]["id"],oldest["id"]);
        println!("physical peak file bytes: {peak}; retained payload: {}; pruned: {}",before["retained_payload_bytes"],before["pruned_count"]);
        drop(store);fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn explicit_prune_reclaims_reused_wal_without_changing_live_export() {
        let dir=crate::scratch("telemetry-wal-reuse");let path=dir.join("db");
        let store=Store::open(&path).unwrap();
        store.append(&(0..64).map(|_|event("retained")).collect::<Vec<_>>()).unwrap();
        store.append(&[event("next")]).unwrap();
        let before=store.export(0,0,1024).unwrap();assert_eq!(before["events"].as_array().unwrap().len(),65);
        assert!(store.storage().unwrap()["wal_bytes"].as_u64().unwrap()>0);
        store.prune().unwrap();assert_eq!(store.storage().unwrap()["wal_bytes"],0);
        assert_eq!(store.export(0,0,1024).unwrap(),before);
        drop(store);let reopened=Store::open(&path).unwrap();assert_eq!(reopened.export(0,0,1024).unwrap(),before);
        drop(reopened);fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn pinned_reader_refuses_mutations_without_data_or_cursor_changes() {
        let dir=crate::scratch("telemetry-pinned");let path=dir.join("db");let store=Store::open(&path).unwrap();
        store.append(&[event("committed")]).unwrap();let batch=store.offer("global",1).unwrap();
        let reader=Connection::open(&path).unwrap();reader.execute_batch("BEGIN; SELECT * FROM events;").unwrap();
        let before=store.storage().unwrap();
        assert!(store.append(&[event("refused")]).unwrap_err().contains("checkpoint busy"));
        assert!(store.import("archive",None,"position.1",&[event("refused.import")]).unwrap_err().contains("checkpoint busy"));
        assert!(store.acknowledge(&batch,&batch.receipt()).unwrap_err().contains("checkpoint busy"));
        assert!(store.prune().unwrap_err().contains("checkpoint busy"));
        assert_eq!(store.import_cursor("archive").unwrap(),None);
        assert_eq!(store.offer("global",1).unwrap().receipt(),batch.receipt());
        assert_eq!(store.export(0,0,1024).unwrap()["events"].as_array().unwrap().len(),1);
        assert_eq!(store.storage().unwrap()["wal_bytes"],before["wal_bytes"]);
        reader.execute_batch("ROLLBACK").unwrap();drop(reader);
        store.append(&[event("after.reader")]).unwrap();assert_eq!(store.export(0,0,1024).unwrap()["events"].as_array().unwrap().len(),2);
        drop(store);fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn oversized_existing_store_is_refused_without_reset() {
        let dir=crate::scratch("telemetry-oversized");let path=dir.join("db");drop(Store::open(&path).unwrap());
        let db=Connection::open(&path).unwrap();db.pragma_update(None,"max_page_count",100000).unwrap();
        db.execute_batch("CREATE TABLE foreign_padding(value BLOB);").unwrap();db.execute("INSERT INTO foreign_padding(value) VALUES(zeroblob(?1))",[DATABASE_BYTES+8192]).unwrap();drop(db);
        let before=fs::read(&path).unwrap();assert!(before.len()>DATABASE_BYTES as usize);
        assert!(Store::open(&path).err().unwrap().contains("store preserved"));
        assert_eq!(fs::read(&path).unwrap(),before);
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn historical_import_commits_rows_and_cursor_together() {
        let dir=crate::scratch("telemetry-import");let path=dir.join("db");let store=Store::open(&path).unwrap();
        let value=serde_json::from_str::<Value>(&event("same").payload).unwrap();
        let first=Event::identified("installation.archive","1",value.clone()).unwrap();
        let same=Event::identified("installation.archive","1",value.clone()).unwrap();
        let second=Event::identified("installation.archive","2",value).unwrap();
        assert_eq!(first.id(),same.id());assert_ne!(first.id(),second.id(),"identical observations at distinct source positions are distinct");
        let mut altered=serde_json::from_str::<Value>(&first.payload).unwrap();altered["operation"]=json!("different");
        let one=Event::identified("batch","same-key",serde_json::from_str::<Value>(&first.payload).unwrap()).unwrap();
        let two=Event::identified("batch","same-key",altered).unwrap();
        assert!(store.append(&[one,two]).unwrap_err().contains("immutable id"),"same-batch collision must roll back the new-row fast path");
        assert!(store.export(0,0,1024).unwrap()["events"].as_array().unwrap().is_empty());
        assert_eq!(store.import_cursor("archive").unwrap(),None);
        store.import("archive",None,"position.1",&[first.clone()]).unwrap();
        assert_eq!(store.import_cursor("archive").unwrap().as_deref(),Some("position.1"));
        {
            let db=store.db.lock().unwrap();db.execute_batch("CREATE TRIGGER import_failure BEFORE UPDATE ON imports BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
        }
        assert!(store.import("archive",Some("position.1"),"position.2",&[second.clone()]).is_err());
        assert_eq!(store.export(0,0,1024).unwrap()["events"].as_array().unwrap().len(),1,"failed cursor update rolls back inserted events");
        assert_eq!(store.import_cursor("archive").unwrap().as_deref(),Some("position.1"));
        store.db.lock().unwrap().execute_batch("DROP TRIGGER import_failure").unwrap();
        store.import("archive",Some("position.1"),"position.2",&[second.clone()]).unwrap();
        drop(store);let store=Store::open(&path).unwrap();
        store.import("archive",Some("position.1"),"position.2",&[second.clone()]).unwrap();
        assert!(store.import("archive",Some("position.1"),"position.2",&[first.clone()]).is_err(),"same cursor cannot acknowledge a changed batch");
        assert!(store.import("archive",None,"position.3",&[]).is_err(),"stale expected cursor is refused");
        assert_eq!(store.export(0,0,1024).unwrap()["events"].as_array().unwrap().len(),2);
        let mut changed=first.clone();changed.payload=changed.payload.replace("same","changed");
        assert!(store.import("other",None,"position.1",&[changed]).is_err());
        assert_eq!(store.import_cursor("other").unwrap(),None,"id conflict cannot advance import progress");
        for n in 0..31 {store.import(&format!("source.{n}"),None,"position.0",&[]).unwrap();}
        assert!(store.import("overflow",None,"position.0",&[]).is_err());
        assert!(Event::identified("/private/path","1",json!({})).is_err());
        drop(store);fs::remove_dir_all(dir).unwrap();
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
    #[test]
    fn independent_processes_commit_without_writer_starvation() {
        let dir=crate::scratch("telemetry-processes");let path=dir.join("db");
        let mut children=(0..4).map(|_|Command::new(std::env::current_exe().unwrap())
            .args(["--exact","telemetry::tests::many_writer","--ignored"])
            .env("COREQ_TELEMETRY_MULTIPROCESS_DB",&path).stdout(Stdio::null()).spawn().unwrap()).collect::<Vec<_>>();
        let until=Instant::now()+Duration::from_secs(20);let mut results=Vec::new();
        for child in &mut children {
            let result=loop {if let Some(status)=child.try_wait().unwrap(){break Some(status);}
                if Instant::now()>=until {let _=child.kill();child.wait().unwrap();break None;}
                std::thread::sleep(Duration::from_millis(10));};
            results.push(result);
        }
        assert!(results.iter().all(|r|r.is_some_and(|s|s.success())),"one of four native writers failed or exceeded the deadline: {results:?}");
        let store=Store::open(&path).unwrap();assert_eq!(store.export(0,0,1024).unwrap()["events"].as_array().unwrap().len(),100);
        drop(store);fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    #[ignore="subprocess helper exercised by independent_processes_commit_without_writer_starvation"]
    fn many_writer() {
        let path=std::env::var_os("COREQ_TELEMETRY_MULTIPROCESS_DB").unwrap();let store=Store::open(Path::new(&path)).unwrap();
        for _ in 0..25 {store.append(&[event("process.call")]).unwrap();}
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
        let value=serde_json::from_str::<Value>(&event("imported.before.kill").payload).unwrap();
        store.import("crash.archive",None,"position.1",&[Event::identified("installation.archive","1",value).unwrap()]).unwrap();
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
        assert_eq!(store.import_cursor("crash.archive").unwrap().as_deref(),Some("position.1"));
        assert_eq!(rows["events"][1]["observation"]["operation"],"imported.before.kill");
        let value=serde_json::from_str::<Value>(&event("imported.before.kill").payload).unwrap();
        store.import("crash.archive",None,"position.1",&[Event::identified("installation.archive","1",value).unwrap()]).unwrap();
        assert_eq!(store.export(0,0,10).unwrap()["events"].as_array().unwrap().len(),2,"replay after kill cannot duplicate the imported row");
        drop(store);fs::remove_dir_all(dir).unwrap();
    }
}
