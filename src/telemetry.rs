//! Shared local observations. Storage never performs network I/O or changes consumer state.
use ring::rand::{SecureRandom, SystemRandom};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::{Value, json};
use std::{fs::TryLockError, path::{Path,PathBuf}, sync::Mutex, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};

pub mod capture;
mod broker;
mod compact;
mod summary;
pub use summary::{assemble,Assembler};

type Res<T> = Result<T, String>;
const AGE_MS: i64 = 30 * 24 * 60 * 60 * 1000;
const PAYLOAD_BYTES: i64 = 64 * 1024 * 1024;
const DATABASE_BYTES: i64 = 28 * 1024 * 1024;
const WAL_BYTES: u64 = 29 * 1024 * 1024;
const SHM_BYTES: u64 = 1024 * 1024;
const EVENT_BYTES: usize = 16 * 1024;
const BATCH: usize = 1024;
const COMMIT_EVENTS: usize = 8192;
const COMMIT_BYTES: usize = BATCH * EVENT_BYTES;
const APPLICATION: i64 = 0x43515431;
const TEXT: &[&str] = &["product", "version", "operation", "kind", "trace", "span", "parent", "outcome", "error", "project", "host", "runtime", "model", "project_spec", "shared_spec", "client_version", "from", "to", "source", "usage_status"];
const NUMBERS: &[&str] = &["started_ms", "duration_us", "active_us", "task", "attempt", "input_tokens", "output_tokens", "cache_read_tokens", "cache_write_tokens", "context_tokens", "compressed_before", "compressed_after", "width", "height", "scale_milli", "run_id", "process_id", "span_id", "parent_id", "entry", "previous_seconds", "remaining", "calls", "tokens"];

fn clock() -> Res<i64> {
    SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| String::from("telemetry clock before epoch"))?
        .as_millis().try_into().map_err(|_| "telemetry clock overflow".into())
}
fn code(s: &str) -> bool {
    !s.is_empty() && s.len() <= 128 && s.bytes().all(|c| c.is_ascii_alphanumeric() || b"._:-".contains(&c))
}
fn hex(text: &str, length: usize) -> bool {text.len()==length&&text.bytes().all(|v|v.is_ascii_digit()||(b'a'..=b'f').contains(&v))}
fn random_id() -> Res<String> {let mut id=[0;16];SystemRandom::new().fill(&mut id).map_err(|_|"telemetry random id unavailable")?;Ok(id.iter().map(|b|format!("{b:02x}")).collect())}
fn identity(namespace: &str,key: &str) -> Res<String> {if !code(namespace)||!code(key) {return Err("telemetry invalid import identity".into());}Ok(crate::sha256_hex(format!("{namespace}\0{key}").as_bytes())[..32].into())}
fn err(e: impl std::fmt::Display) -> String { format!("telemetry: {e}") }
fn store_trace() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(||std::env::var("COREQ_TELEMETRY_STORE_TRACE").is_ok_and(|v|v=="1"))
}

const FAILURE_CODES: &[&str]=&["auth","invalid-frame","invalid-observation","id-conflict","sqlite-busy","store-full","store-readonly","store-io","store-failed","flush-deadline","queue-unavailable","transport-unavailable","collection-incomplete","unknown"];
fn failure_code(error: &str) -> &'static str {
    if let Some(code)=error.strip_prefix("telemetry IPC rejected: ") {return FAILURE_CODES.iter().copied().find(|c|*c==code).unwrap_or("unknown");}
    if error=="telemetry IPC refused" {return "auth";}
    if error=="telemetry checkpoint busy; retained data preserved" {return "sqlite-busy";}
    if error=="telemetry physical capacity exhausted" {return "store-full";}
    if matches!(error,"telemetry immutable id payload mismatch"|"telemetry immutable retention time mismatch") {return "id-conflict";}
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

fn validate(value: &Value, supplied: Option<&str>) -> Res<i64> {
    if supplied.is_some_and(|id|!hex(id,32)) {return Err("telemetry invalid event id".into());}
    let object = value.as_object().ok_or("telemetry event must be an object")?;
    for required in ["product", "version", "operation", "kind", "trace", "span", "started_ms", "outcome", "error"] {
        if object.get(required).is_none() || (!["started_ms","version","trace","span"].contains(&required)&&object[required].is_null()) { return Err(format!("telemetry missing {required}")); }
    }
    for (k, v) in object {
        let valid = if TEXT.contains(&k.as_str()) {
            v.is_null() || v.as_str().is_some_and(|s| if k == "project" {
                s=="other" || (s.len()<=128&&s.contains('/')&&s.split('/').all(|part|code(part)&&!matches!(part,"."|"..")))
            } else { code(s) })
        } else if NUMBERS.contains(&k.as_str()) {
            v.is_null() || v.as_u64().is_some_and(|n| k!="started_ms"||n <= i64::MAX as u64)
        } else if k == "open" {
            v.is_boolean()
        } else if k == "counts" {
            v.as_object().is_some_and(|m|m.iter().all(|(name,count)|code(name)&&count.as_u64().is_some()))
        } else if summary::FIELDS.contains(&k.as_str()) {
            true
        } else if k == "unknown" {
            v.as_object().is_some_and(|m| m.iter().all(|(name, reason)| (NUMBERS.contains(&name.as_str())||TEXT.contains(&name.as_str())) && reason.as_str().is_some_and(code)))
        } else { false };
        if !valid { return Err(format!("telemetry invalid field {k}")); }
    }
    if !matches!(value["outcome"].as_str(), Some("ok" | "failed" | "refused" | "cancelled" | "retried" | "unknown" | "ask")) {
        return Err("telemetry invalid outcome".into());
    }
    for k in NUMBERS.iter().copied().chain(["version","trace","span"]) {
        if object.get(k).is_some_and(Value::is_null) && value["unknown"][k].as_str().is_none() {
            return Err(format!("telemetry null {k} needs an unknown reason"));
        }
    }
    summary::validate(value)?;
    if let (Some(id),Some(group))=(supplied,value["group_id"].as_str()) {
        if id!=summary::part_id(group,value["part_index"].as_u64().unwrap() as usize) {return Err("telemetry invalid count summary identity".into());}
    }
    if value["started_ms"].is_null() {Ok(-1)} else {value["started_ms"].as_i64().ok_or("telemetry invalid start".into())}
}

/// Validated immutable observation; cloning it preserves its id for idempotent retry.
#[derive(Clone, Debug)]
pub struct Event { id: String, started_ms: i64, kind: String, payload: String }
impl Event {
    pub fn new(value: Value) -> Res<Self> {Self::with_id(value,None)}
    fn with_id(value: Value, supplied: Option<&str>) -> Res<Self> {
        let started_ms=validate(&value,supplied)?;
        let payload = value.to_string();
        if payload.len() > EVENT_BYTES { return Err("telemetry event too large".into()); }
        let id=if let Some(id)=supplied {id.to_owned()} else if let Some(group)=value["group_id"].as_str() {summary::part_id(group,value["part_index"].as_u64().unwrap() as usize)} else {random_id()?};
        Ok(Self { id, started_ms, kind:value["kind"].as_str().unwrap().to_owned(), payload })
    }
    /// Stable source position, independent of record contents: a changed row conflicts rather than overwriting.
    pub fn identified(namespace: &str, key: &str, value: Value) -> Res<Self> {
        Self::with_id(value,Some(&identity(namespace,key)?))
    }
    /// Split only oversized count maps; normal observations retain the existing representation.
    pub fn split(value: Value) -> Res<Vec<Self>> {summary::split(value,&random_id()?)}
    pub fn split_identified(namespace: &str,key: &str,value: Value) -> Res<Vec<Self>> {summary::split(value,&identity(namespace,key)?)}
    pub fn id(&self) -> &str { &self.id }
}

#[derive(Clone, Debug, PartialEq)]
struct Row { seq: i64, id: String, received_ms: i64, started_ms: i64, kind:String, payload: String }
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

/// A committed read snapshot. Never creates, upgrades, checkpoints or prunes the store.
pub struct Reader { db: Connection }
impl Reader {
    pub fn open(path: &Path) -> Res<Self> {
        let db=Connection::open_with_flags(path,rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(err)?;
        db.busy_timeout(Duration::from_secs(1)).map_err(err)?;
        db.execute_batch("PRAGMA query_only=ON; BEGIN;").map_err(err)?;
        let version:i64=db.pragma_query_value(None,"user_version",|r|r.get(0)).map_err(err)?;
        let application:i64=db.pragma_query_value(None,"application_id",|r|r.get(0)).map_err(err)?;
        if version!=6||application!=APPLICATION {return Err("telemetry read requires current owned schema; owner migration required".into());}
        validate_store(&db)?;
        Ok(Self{db})
    }
    pub fn export(&self,since_ms:i64,after:i64,limit:usize) -> Res<Value> {
        export_page(&self.db,since_ms,after,limit)
    }
}

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
        let mut db = Connection::open(path).map_err(err)?;
        db.busy_timeout(Duration::from_secs(1)).map_err(err)?;
        let version: i64=db.pragma_query_value(None,"user_version",|r|r.get(0)).map_err(err)?;
        let application:i64=db.pragma_query_value(None,"application_id",|r|r.get(0)).map_err(err)?;
        let empty=version==0&&application==0&&!db.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master)",[],|r|r.get::<_,bool>(0)).map_err(err)?;
        if !empty&&(!matches!(version,1|2|3|4|5|6)||application!=APPLICATION) {return Err(format!("telemetry unsupported schema {version} or foreign application; store preserved"));}
        let check:String=db.query_row("PRAGMA quick_check",[],|r|r.get(0)).map_err(err)?;
        if check!="ok" {return Err("telemetry corrupt store preserved".into());}
        let page_size: i64=db.pragma_query_value(None,"page_size",|r|r.get(0)).map_err(err)?;
        let pages: i64=db.pragma_query_value(None,"page_count",|r|r.get(0)).map_err(err)?;
        if page_size<1024||pages>DATABASE_BYTES/page_size {return Err("telemetry oversized or unsupported-page store preserved; migration required".into());}
        db.execute_batch("PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA temp_store=MEMORY; PRAGMA cache_spill=OFF; PRAGMA cache_size=-32768; PRAGMA wal_autocheckpoint=0;").map_err(err)?;
        db.pragma_update(None,"max_page_count",DATABASE_BYTES/page_size).map_err(err)?;
        checkpoint(&db,true)?;
        if empty {
            let tx=db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(err)?;
            tx.execute_batch("
                CREATE TABLE events(seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL UNIQUE,received_ms INTEGER NOT NULL,started_ms INTEGER NOT NULL,payload TEXT NOT NULL,bytes INTEGER NOT NULL,block INTEGER REFERENCES blocks(id) DEFERRABLE INITIALLY DEFERRED,slot INTEGER,kind TEXT);
                CREATE INDEX events_kind ON events(kind,seq);
                CREATE INDEX events_started ON events(started_ms,seq);
                CREATE INDEX events_received ON events(received_ms,seq);
                CREATE TABLE metadata(id INTEGER PRIMARY KEY CHECK(id=1),retained_bytes INTEGER NOT NULL,pruned_count INTEGER NOT NULL,pruned_through INTEGER NOT NULL);
                INSERT INTO metadata VALUES(1,0,0,0);
                CREATE TABLE cursors(destination TEXT PRIMARY KEY,seq INTEGER NOT NULL,receipt TEXT NOT NULL);
                CREATE TRIGGER inserted AFTER INSERT ON events BEGIN UPDATE metadata SET retained_bytes=retained_bytes+new.bytes WHERE id=1; END;
                CREATE TRIGGER deleted AFTER DELETE ON events BEGIN UPDATE metadata SET retained_bytes=retained_bytes-old.bytes,pruned_count=pruned_count+1,pruned_through=max(pruned_through,old.seq) WHERE id=1; END;
                PRAGMA user_version=6; PRAGMA application_id=1129403441;").map_err(err)?;
            tx.execute_batch(BLOCK_SCHEMA).map_err(err)?;tx.commit().map_err(err)?;
        }
        let raw=matches!(version,2|3|4|5|6)&&db.query_row(if version==6 {"SELECT EXISTS(SELECT 1 FROM events WHERE block IS NULL OR kind IS NULL)"} else {"SELECT EXISTS(SELECT 1 FROM events WHERE block IS NULL)"},[],|r|r.get::<_,bool>(0)).map_err(err)?;
        if matches!(version,1|2|3|4|5)||raw {upgrade(&mut db,version)?;checkpoint(&db,true)?;} else {validate_store(&db)?;}
        Ok(Self { db: Mutex::new(db), write_lock, path: path.to_owned() })
    }
    /// One durable group commit. On failure no observation in this batch becomes visible.
    pub fn append(&self, events: &[Event]) -> Res<Vec<i64>> {
        if events.len() > BATCH { return Err("telemetry batch too large".into()); }
        self.append_group(events)
    }
    fn append_group(&self, events: &[Event]) -> Res<Vec<i64>> {
        if events.len()>COMMIT_EVENTS||events.iter().map(|e|e.payload.len()).sum::<usize>()>COMMIT_BYTES {
            return Err("telemetry commit group too large".into());
        }
        let began=Instant::now();
        let mut db = self.db.lock().map_err(|_| "telemetry writer poisoned")?;
        let _write=crate::file_lock::lock(&self.write_lock).map_err(err)?;
        let locked=began.elapsed().as_micros();
        append_checkpoint(&db)?;
        let checkpointed=began.elapsed().as_micros();
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(|e|format!("telemetry append.begin: {e}"))?;
        let opened=began.elapsed().as_micros();
        if events.len()>BATCH {incoming(&tx,events)?;}
        let now=clock()?;let mut ids=Vec::with_capacity(events.len());
        if events.is_empty() {append_to(&tx,events,now,None)?;}
        for chunk in events.chunks(BATCH) {ids.extend(append_to(&tx,chunk,now,None)?);}
        let inserted=began.elapsed().as_micros();
        tx.commit().map_err(|e|format!("telemetry append.commit: {e}"))?;
        let committed=began.elapsed().as_micros();
        if store_trace()&&committed>=100_000 {
            eprintln!("Telemetry store us: events={} lock={locked} checkpoint={} begin={} rows={} commit={}",events.len(),checkpointed-locked,opened-checkpointed,inserted-opened,committed-inserted);
        }
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
        self.import_at(source,expected,next,events,None)
    }
    /// Preserve the original retention clock; this owner-side API is never exposed by append-only IPC.
    pub fn import_history(&self, source: &str, expected: Option<&str>, next: &str, events: &[Event], received_ms: &[i64]) -> Res<()> {
        if received_ms.len()!=events.len()||received_ms.iter().any(|v|*v<0) {return Err("telemetry invalid historical receipt clocks".into());}
        self.import_at(source,expected,next,events,Some(received_ms))
    }
    fn import_at(&self, source: &str, expected: Option<&str>, next: &str, events: &[Event], received_ms: Option<&[i64]>) -> Res<()> {
        if !code(source)||!code(next)||expected.is_some_and(|v|!code(v)) { return Err("telemetry invalid import cursor".into()); }
        if events.len()>BATCH { return Err("telemetry batch too large".into()); }
        let mut identity=json!([source,expected,next,events.iter().map(|e|(&e.id,&e.payload)).collect::<Vec<_>>()]);
        if let Some(times)=received_ms {identity.as_array_mut().unwrap().push(json!(times));}
        let receipt=crate::sha256_hex(&serde_json::to_vec(&identity).map_err(err)?);
        let mut db=self.db.lock().map_err(|_|"telemetry writer poisoned")?;
        let _write=crate::file_lock::lock(&self.write_lock).map_err(err)?;
        checkpoint(&db,false)?;
        let tx=db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(|e|format!("telemetry import.begin: {e}"))?;
        if let Some(times)=received_ms {
            let mut seen=std::collections::HashMap::new();
            let mut read=tx.prepare_cached("SELECT received_ms FROM events WHERE id=?1").map_err(err)?;
            for (event,time) in events.iter().zip(times) {
                if seen.insert(event.id.as_str(),*time).is_some_and(|previous|previous!=*time)
                    ||read.query_row([&event.id],|r|r.get::<_,i64>(0)).optional().map_err(err)?.is_some_and(|old|old!=*time) {
                    return Err("telemetry immutable retention time mismatch".into());
                }
            }
        }
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
        append_to(&tx,events,clock()?,received_ms)?;
        tx.execute("INSERT INTO imports(source,cursor,receipt) VALUES(?1,?2,?3) ON CONFLICT(source) DO UPDATE SET cursor=excluded.cursor,receipt=excluded.receipt",params![source,next,receipt]).map_err(err)?;
        tx.commit().map_err(|e|format!("telemetry import.commit: {e}"))
    }
    pub fn export(&self, since_ms: i64, after: i64, limit: usize) -> Res<Value> {
        let mut connection = self.db.lock().map_err(|_| "telemetry reader poisoned")?;
        let db = connection.transaction().map_err(err)?;
        export_page(&db,since_ms,after,limit)
    }
    /// Indexed semantic pages. Null kinds from already-open older writers are examined, never silently skipped.
    pub fn export_kinds(&self, kinds: &[&str], since_ms: i64, after: i64, limit: usize) -> Res<Value> {
        if kinds.is_empty()||kinds.len()>16||kinds.iter().any(|kind|!code(kind))||kinds.iter().collect::<std::collections::HashSet<_>>().len()!=kinds.len() {return Err("telemetry invalid kind selection".into());}
        if !(1..=BATCH).contains(&limit) {return Err("telemetry limit must be 1..1024".into());}
        let mut connection=self.db.lock().map_err(|_|"telemetry reader poisoned")?;let db=connection.transaction().map_err(err)?;
        let predicate=kind_predicate(kinds.len());
        let mut values=kinds.iter().map(|kind|rusqlite::types::Value::Text((*kind).into())).collect::<Vec<_>>();values.extend([since_ms.into(),after.into()]);
        let mut page_values=values.clone();page_values.push((limit as i64).into());
        let candidates=read_rows(&db,&kind_query(kinds.len()),rusqlite::params_from_iter(page_values.iter()),&mut Cache::default())?;
        let examined=candidates.len();let next=candidates.last().map_or(after,|row|row.seq);*values.last_mut().unwrap()=next.into();
        let more:bool=db.query_row(&format!("SELECT EXISTS(SELECT 1 FROM events WHERE {predicate})"),rusqlite::params_from_iter(values.iter()),|r|r.get(0)).map_err(err)?;
        let rows=candidates.into_iter().filter(|row|kinds.contains(&row.kind.as_str())).collect::<Vec<_>>();
        let mut page=exported(&db,since_ms,after,&rows,next,more)?;page["kinds"]=json!(kinds);page["examined"]=json!(examined);
        page["unindexed_count"]=json!(db.query_row("SELECT count(*) FROM events WHERE kind IS NULL",[],|r|r.get::<_,i64>(0)).map_err(err)?);Ok(page)
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
        let (blocks,compressed): (i64,i64)=db.query_row("SELECT count(*),coalesce(sum(length(payload)),0) FROM blocks",[],|r|Ok((r.get(0)?,r.get(1)?))).map_err(err)?;
        let raw:i64=db.query_row("SELECT coalesce(sum(length(cast(payload AS BLOB))),0) FROM events WHERE block IS NULL",[],|r|r.get(0)).map_err(err)?;
        let free:i64=db.pragma_query_value(None,"freelist_count",|r|r.get(0)).map_err(err)?;let page:i64=db.pragma_query_value(None,"page_size",|r|r.get(0)).map_err(err)?;
        Ok(json!({"codec":"zlib-block/1","block_count":blocks,"compressed_payload_bytes":compressed,"legacy_payload_bytes":raw,"reusable_database_bytes":free*page,
            "state":if database<=DATABASE_BYTES as u64&&wal<=WAL_BYTES&&shm<=SHM_BYTES {"bounded"} else {"unknown"},
            "database_bytes":database,"wal_bytes":wal,"shm_bytes":shm,"total_bytes":database+wal+shm,
            "database_limit":DATABASE_BYTES,"wal_limit":WAL_BYTES,"shm_limit":SHM_BYTES,
            "retained_payload_bytes":retained,"pruned_count":pruned}))
    }
    /// Explicit quiesced maintenance. SQLite's transient vacuum database stays in memory.
    pub fn reclaim(&self) -> Res<()> {
        let _election=broker::maintenance_lock(&self.path)?;
        let db=self.db.lock().map_err(|_|"telemetry writer poisoned")?;
        let _write=crate::file_lock::lock(&self.write_lock).map_err(err)?;
        validate_store(&db)?;checkpoint(&db,true)?;
        let free:i64=db.pragma_query_value(None,"freelist_count",|r|r.get(0)).map_err(err)?;
        if free!=0 {
            db.execute_batch("PRAGMA temp_store=MEMORY; PRAGMA synchronous=FULL; PRAGMA cache_spill=OFF; VACUUM;").map_err(|e|format!("telemetry reclaim: {e}"))?;
            validate_store(&db)?;checkpoint(&db,true)?;
        }
        Ok(())
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
fn export_page(db:&Connection,since_ms:i64,after:i64,limit:usize) -> Res<Value> {
    if !(1..=BATCH).contains(&limit) {return Err("telemetry limit must be 1..1024".into());}
    let rows=read_rows(db,&format!("SELECT {SELECT_ROW} FROM events WHERE (started_ms=-1 OR started_ms>=?1) AND seq>?2 ORDER BY seq LIMIT ?3"),params![since_ms,after,limit as i64],&mut Cache::default())?;
    let next=rows.last().map_or(after,|r|r.seq);
    let more:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM events WHERE seq>?1 AND (started_ms=-1 OR started_ms>=?2))",params![next,since_ms],|r|r.get(0)).map_err(err)?;
    exported(db,since_ms,after,&rows,next,more)
}
fn kind_predicate(count: usize) -> String {format!("(kind IN ({}) OR kind IS NULL) AND (started_ms=-1 OR started_ms>=?) AND seq>?",std::iter::repeat_n("?",count).collect::<Vec<_>>().join(","))}
fn kind_query(count: usize) -> String {format!("SELECT {SELECT_ROW} FROM events WHERE {} ORDER BY seq LIMIT ?",kind_predicate(count))}
fn exported(db: &Connection,since_ms:i64,after:i64,rows:&[Row],next:i64,more:bool) -> Res<Value> {
    let (pruned_count,pruned_through):(i64,i64)=db.query_row("SELECT pruned_count,pruned_through FROM metadata WHERE id=1",[],|r|Ok((r.get(0)?,r.get(1)?))).map_err(err)?;
    let bounds:(Option<i64>,Option<i64>,Option<i64>,Option<i64>)=db.query_row("SELECT (SELECT min(seq) FROM events),(SELECT max(seq) FROM events),(SELECT min(started_ms) FROM events WHERE started_ms>=0),(SELECT max(started_ms) FROM events WHERE started_ms>=0)",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).map_err(err)?;
    let unknown:i64=db.query_row("SELECT count(*) FROM events WHERE started_ms=-1",[],|r|r.get(0)).map_err(err)?;
    Ok(json!({"schema":1,"since_ms":since_ms,"after":after,"next":next,"more":more,"retained_seq":[bounds.0,bounds.1],"retained_ms":[bounds.2,bounds.3],"unknown_started_count":unknown,"pruned_count":pruned_count,"pruned_through":pruned_through,"events":rows.iter().map(Row::json).collect::<Res<Vec<_>>>()?}))
}

fn wal_has_room(frames: i64, page: i64) -> bool {
    if frames<0 || !(1024..=65536).contains(&page) || !(page as u64).is_power_of_two() { return false; }
    let frame=page as u64+24;
    // Spill is disabled: at most one frame per database page, plus FULL's sector padding.
    let maximum=(DATABASE_BYTES as u64/page as u64+65536u64.div_ceil(frame))*frame;
    (frames as u64).checked_mul(frame).and_then(|n|n.checked_add(32+maximum)).is_some_and(|n|n<=WAL_BYTES)
}
fn append_checkpoint(db: &Connection) -> Res<()> {
    let (busy,frames): (i64,i64)=db.query_row("PRAGMA wal_checkpoint(NOOP)",[],|r|Ok((r.get(0)?,r.get(1)?))).map_err(err)?;
    let page:i64=db.pragma_query_value(None,"page_size",|r|r.get(0)).map_err(err)?;
    if busy==0&&wal_has_room(frames,page) {return Ok(());}
    checkpoint(db,false)
}
fn checkpoint(db: &Connection, truncate: bool) -> Res<()> {
    let sql=if truncate {"PRAGMA wal_checkpoint(TRUNCATE)"} else {"PRAGMA wal_checkpoint(RESTART)"};
    let (busy,frames,done): (i64,i64,i64)=db.query_row(sql,[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).map_err(err)?;
    if busy!=0||frames!=done {return Err("telemetry checkpoint busy; retained data preserved".into());}
    Ok(())
}
fn incoming(db: &Connection, events: &[Event]) -> Res<(i64,i64)> {
    let mut bytes=0i64;let mut count=0i64;let mut seen=std::collections::HashMap::new();
    let mut existing=db.prepare_cached(&format!("SELECT {SELECT_ROW} FROM events WHERE id=?1")).map_err(err)?;let mut cache=Cache::default();
    for event in events {
        if let Some(previous)=seen.insert(event.id.as_str(),event.payload.as_str()) {
            if previous!=event.payload {return Err("telemetry immutable id payload mismatch".into());}
            continue;
        }
        match existing.query_row([&event.id],row).optional().map_err(err)? {
            Some(stored)=>{if stored.decode(db,&mut cache)?.payload!=event.payload {return Err("telemetry immutable id payload mismatch".into());}},
            None=>{bytes+=event.payload.len() as i64;count+=1;}
        }
    }
    Ok((bytes,count))
}
fn reserve(db: &Connection, events: &[Event], extra_pages: i64) -> Res<()> {
    let (bytes,count)=incoming(db,events)?;
    if count==0&&extra_pages==0 {return Ok(());}
    let page_size: i64=db.pragma_query_value(None,"page_size",|r|r.get(0)).map_err(err)?;
    let required=(bytes+page_size-1)/page_size+(512*count+page_size-1)/page_size+32+extra_pages;
    loop {
        let pages: i64=db.pragma_query_value(None,"page_count",|r|r.get(0)).map_err(err)?;
        let free: i64=db.pragma_query_value(None,"freelist_count",|r|r.get(0)).map_err(err)?;
        if pages-free+required<DATABASE_BYTES/page_size {return Ok(());}
        let rows=read_rows(db,&format!("SELECT {SELECT_ROW} FROM events ORDER BY seq LIMIT 256"),[],&mut Cache::default())?;
        if remove_rows(db,rows)?==0 {
            return Err("telemetry physical capacity exhausted".into());
        }
    }
}
fn append_to(db: &Connection, events: &[Event], now: i64, received_ms: Option<&[i64]>) -> Res<Vec<i64>> {
    let began=Instant::now();
    reserve(db,events,0)?;
    let reserved=began.elapsed().as_micros();
    let mut sequences=std::collections::HashMap::new();let mut pending=Vec::new();let mut seen=std::collections::HashSet::new();let mut cache=Cache::default();
    let mut read=db.prepare_cached(&format!("SELECT {SELECT_ROW} FROM events WHERE id=?1")).map_err(err)?;
    for (index,event) in events.iter().enumerate() {
        if !seen.insert(event.id.as_str()) {continue;}
        if let Some(stored)=read.query_row([&event.id],row).optional().map_err(err)? {
            let stored=stored.decode(db,&mut cache)?;
            if stored.payload!=event.payload {return Err("telemetry immutable id payload mismatch".into());}
            sequences.insert(event.id.as_str(),stored.seq);
        } else {pending.push((event,received_ms.map_or(now,|times|times[index])));}
    }
    drop(read);
    let payloads=pending.iter().map(|(e,_)|e.payload.as_str()).collect::<Vec<_>>();
    let mut insert=db.prepare_cached("INSERT INTO events(id,received_ms,started_ms,payload,bytes,block,slot,kind) VALUES(?1,?2,?3,'',?4,?5,?6,?7)").map_err(err)?;
    for group in compact::groups(&payloads)? {
        let encoded=compact::encode(&payloads[group.clone()])?;
        db.execute("INSERT INTO blocks(payload) VALUES(?1)",[encoded]).map_err(err)?;let block=db.last_insert_rowid();
        for (slot,(event,received)) in pending[group].iter().enumerate() {
            insert.execute(params![event.id,received,event.started_ms,event.payload.len() as i64,block,slot as i64,event.kind]).map_err(|e|format!("telemetry append.insert: {e}"))?;
            sequences.insert(event.id.as_str(),db.last_insert_rowid());
        }
    }
    drop(insert);
    let ids=events.iter().map(|e|sequences.get(e.id.as_str()).copied().ok_or("telemetry missing appended sequence".into())).collect::<Res<Vec<_>>>()?;
    let inserted=began.elapsed().as_micros();
    prune(db,now,AGE_MS,PAYLOAD_BYTES)?;
    let pruned=began.elapsed().as_micros();
    if store_trace()&&pruned>=100_000 {
        eprintln!("Telemetry rows us: events={} reserve={reserved} insert={} prune={}",events.len(),inserted-reserved,pruned-inserted);
    }
    Ok(ids)
}
const SELECT_ROW: &str="seq,id,received_ms,payload,bytes,started_ms,block,slot,kind";
const BLOCK_SCHEMA: &str="CREATE TABLE blocks(id INTEGER PRIMARY KEY,payload BLOB NOT NULL);
    CREATE INDEX events_block ON events(block);
    CREATE TRIGGER delete_block AFTER DELETE ON events WHEN old.block IS NOT NULL BEGIN
      DELETE FROM blocks WHERE id=old.block AND NOT EXISTS(SELECT 1 FROM events WHERE block=old.block); END;";
#[derive(Default)]
struct Cache(Vec<(i64,Vec<String>)>);
impl Cache {
    fn payload(&mut self, db: &Connection, block: i64, slot: i64) -> Res<String> {
        if block<=0||!(0..BATCH as i64).contains(&slot) {return Err("telemetry corrupt block reference".into());}
        let index=if let Some(i)=self.0.iter().position(|(id,_)|*id==block) {i} else {
            let bytes: Option<Vec<u8>>=db.query_row("SELECT payload FROM blocks WHERE id=?1 AND typeof(payload)='blob' AND length(payload)<=?2",params![block,compact::ENCODED_BYTES as i64],|r|r.get(0)).optional().map_err(err)?;
            let payloads=compact::decode(&bytes.ok_or("telemetry missing or oversized block")?)?;
            if self.0.len()==8 {self.0.remove(0);}self.0.push((block,payloads));self.0.len()-1
        };
        self.0[index].1.get(slot as usize).cloned().ok_or("telemetry corrupt block slot".into())
    }
}
struct StoredRow {seq:i64,id:String,received_ms:i64,payload:String,bytes:i64,started_ms:i64,block:Option<i64>,slot:Option<i64>,kind:Option<String>}
impl StoredRow {
    fn decode(self, db: &Connection, cache: &mut Cache) -> Res<Row> {
        let payload=match (self.block,self.slot) {
            (None,None)=>self.payload,
            (Some(block),Some(slot)) if self.payload.is_empty()=>cache.payload(db,block,slot)?,
            _=>return Err("telemetry corrupt row reference".into()),
        };
        if self.seq<=0||self.received_ms<0||payload.is_empty()||payload.len()>EVENT_BYTES||payload.len() as i64!=self.bytes {return Err("telemetry corrupt observation size or identity".into());}
        let value=serde_json::from_str(&payload).map_err(|_|"telemetry corrupt observation JSON")?;
        if validate(&value,Some(&self.id)).map_err(|_|"telemetry corrupt observation metadata")?!=self.started_ms {return Err("telemetry corrupt source timestamp".into());}
        let kind=value["kind"].as_str().unwrap();
        if self.kind.as_deref().is_some_and(|indexed|indexed!=kind) {return Err("telemetry corrupt kind index metadata".into());}
        Ok(Row{seq:self.seq,id:self.id,received_ms:self.received_ms,started_ms:self.started_ms,kind:kind.to_owned(),payload})
    }
}
fn row(r: &rusqlite::Row<'_>) -> rusqlite::Result<StoredRow> {
    Ok(StoredRow{seq:r.get(0)?,id:r.get(1)?,received_ms:r.get(2)?,payload:r.get(3)?,bytes:r.get(4)?,started_ms:r.get(5)?,block:r.get(6)?,slot:r.get(7)?,kind:r.get(8)?})
}
fn read_rows(db: &Connection,sql: &str,params: impl rusqlite::Params,cache: &mut Cache) -> Res<Vec<Row>> {
    let mut stmt=db.prepare(sql).map_err(err)?;
    let rows=stmt.query_map(params,row).map_err(err)?.collect::<Result<Vec<_>,_>>().map_err(err)?;
    rows.into_iter().map(|r|r.decode(db,cache)).collect()
}
fn validate_store(db: &Connection) -> Res<()> {
    let (retained,pruned,floor,sum): (i64,i64,i64,i64)=db.query_row("SELECT retained_bytes,pruned_count,pruned_through,(SELECT coalesce(sum(bytes),0) FROM events) FROM metadata WHERE id=1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).map_err(err)?;
    if retained<0||pruned<0||floor<0||retained!=sum {return Err("telemetry corrupt retention metadata".into());}
    let orphan:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM blocks WHERE NOT EXISTS(SELECT 1 FROM events WHERE block=blocks.id))",[],|r|r.get(0)).map_err(err)?;
    if orphan {return Err("telemetry unreferenced compressed block".into());}
    let mut after=0;let mut cache=Cache::default();
    loop {let rows=read_rows(db,&format!("SELECT {SELECT_ROW} FROM events WHERE seq>?1 ORDER BY seq LIMIT ?2"),params![after,BATCH as i64],&mut cache)?;
        if rows.is_empty() {break;}after=rows.last().unwrap().seq;}
    let high: Option<i64>=db.query_row("SELECT seq FROM sqlite_sequence WHERE name='events'",[],|r|r.get(0)).optional().map_err(err)?;
    if high.unwrap_or(0)<after.max(floor) {return Err("telemetry corrupt sequence high-water mark".into());}Ok(())
}
fn upgrade(db: &mut Connection, version: i64) -> Res<()> {
    let tx=db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(err)?;
    if version==1 {
        tx.execute_batch("ALTER TABLE events ADD COLUMN block INTEGER REFERENCES blocks(id) DEFERRABLE INITIALLY DEFERRED; ALTER TABLE events ADD COLUMN slot INTEGER;").map_err(err)?;
        tx.execute_batch(BLOCK_SCHEMA).map_err(err)?;
    }
    if version<6 {tx.execute_batch("ALTER TABLE events ADD COLUMN kind TEXT; CREATE INDEX events_kind ON events(kind,seq);").map_err(err)?;}
    let enabled=tx.db_config(rusqlite::config::DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER).map_err(err)?;
    // Repacking preserves logical rows, so retention counters and user triggers must not observe deletes.
    tx.set_db_config(rusqlite::config::DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER,false).map_err(err)?;
    let packed=(|| -> Res<()> {
    let mut after=0;let mut cache=Cache::default();
    loop {
        let rows=read_rows(&tx,&format!("SELECT {SELECT_ROW} FROM events WHERE block IS NULL AND seq>?1 ORDER BY seq LIMIT ?2"),params![after,BATCH as i64],&mut cache)?;
        if rows.is_empty() {break;}
        let payloads=rows.iter().map(|r|r.payload.as_str()).collect::<Vec<_>>();
        for group in compact::groups(&payloads)? {
            let encoded=compact::encode(&payloads[group.clone()])?;
            let block:i64=tx.query_row("SELECT coalesce(max(id),0)+1 FROM blocks",[],|r|r.get(0)).map_err(err)?;
            for row in &rows[group.clone()] {tx.execute("DELETE FROM events WHERE seq=?1",[row.seq]).map_err(err)?;}
            tx.execute("INSERT INTO blocks(id,payload) VALUES(?1,?2)",params![block,encoded]).map_err(err)?;
            for (slot,row) in rows[group].iter().enumerate() {
                tx.execute("INSERT INTO events(seq,id,received_ms,started_ms,payload,bytes,block,slot,kind) VALUES(?1,?2,?3,?4,'',?5,?6,?7,?8)",params![row.seq,row.id,row.received_ms,row.started_ms,row.payload.len() as i64,block,slot as i64,row.kind]).map_err(err)?;
            }
        }
        #[cfg(test)]
        if after==0 {if let Some(marker)=std::env::var_os("COREQ_TELEMETRY_TEST_UPGRADE_PAUSE") {
            std::fs::write(marker,"transaction-open").map_err(err)?;
            let mut line=String::new();std::io::stdin().read_line(&mut line).map_err(err)?;
        }}
        after=rows.last().unwrap().seq;
    }
    let mut after=0;let mut cache=Cache::default();
    loop {let rows=read_rows(&tx,&format!("SELECT {SELECT_ROW} FROM events WHERE kind IS NULL AND seq>?1 ORDER BY seq LIMIT ?2"),params![after,BATCH as i64],&mut cache)?;
        if rows.is_empty() {break;}
        for row in &rows {tx.execute("UPDATE events SET kind=?1 WHERE seq=?2",params![row.kind,row.seq]).map_err(err)?;}
        after=rows.last().unwrap().seq;
    }
    Ok(())})();
    tx.set_db_config(rusqlite::config::DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER,enabled).map_err(err)?;
    packed?;
    tx.pragma_update(None,"user_version",6).map_err(err)?;validate_store(&tx)?;
    tx.commit().map_err(err)
}
fn offer(db: &Connection, destination: &str, after: i64, limit: usize) -> Res<Batch> {
    let rows=read_rows(db,&format!("SELECT {SELECT_ROW} FROM events WHERE seq>?1 ORDER BY seq LIMIT ?2"),params![after,limit as i64],&mut Cache::default())?;
    let pruned_through = db.query_row("SELECT pruned_through FROM metadata WHERE id=1",[],|r|r.get(0)).map_err(err)?;
    Ok(Batch { destination:destination.into(),after,rows,pruned_through })
}
fn remove_rows(db: &Connection, rows: Vec<Row>) -> Res<usize> {
    let mut ids=std::collections::HashSet::new();let mut groups=std::collections::HashSet::new();
    for row in rows {
        let value:Value=serde_json::from_str(&row.payload).map_err(err)?;
        if let Some(group)=value["group_id"].as_str() {groups.insert(group.to_owned());} else {ids.insert(row.id);}
    }
    let candidates=groups.iter().flat_map(|group|summary::removal_ids(group)).collect::<Vec<_>>();let mut cache=Cache::default();
    for chunk in candidates.chunks(256) {
        let sql=format!("SELECT {SELECT_ROW} FROM events WHERE id IN ({})",std::iter::repeat_n("?",chunk.len()).collect::<Vec<_>>().join(","));
        for row in read_rows(db,&sql,rusqlite::params_from_iter(chunk.iter()),&mut cache)? {
            let value:Value=serde_json::from_str(&row.payload).map_err(err)?;
            if value["group_id"].as_str().is_some_and(|group|groups.contains(group)) {ids.insert(row.id);}
        }
    }
    let ids=ids.into_iter().collect::<Vec<_>>();let mut removed=0;
    for chunk in ids.chunks(256) {
        let sql=format!("DELETE FROM events WHERE id IN ({})",std::iter::repeat_n("?",chunk.len()).collect::<Vec<_>>().join(","));
        removed+=db.execute(&sql,rusqlite::params_from_iter(chunk.iter())).map_err(err)?;
    }
    Ok(removed)
}
fn prune(db: &Connection, now: i64, age: i64, bytes: i64) -> Res<()> {
    let mut cache=Cache::default();
    loop {let rows=read_rows(db,&format!("SELECT {SELECT_ROW} FROM events WHERE received_ms<?1 ORDER BY seq LIMIT 256"),[now.saturating_sub(age)],&mut cache)?;
        if rows.is_empty() {break;}remove_rows(db,rows)?;}
    while db.query_row("SELECT retained_bytes FROM metadata WHERE id=1",[],|r|r.get::<_,i64>(0)).map_err(err)?>bytes {
        let rows=read_rows(db,&format!("SELECT {SELECT_ROW} FROM events ORDER BY seq LIMIT 1"),[],&mut cache)?;
        if remove_rows(db,rows)?==0 {return Err("telemetry corrupt retention metadata".into());}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Chain: protected analysis -> fixed committed pages while the owner keeps appending.
    #[test]
    fn readonly_analysis_keeps_one_snapshot_without_writer_authority() {
        let dir=crate::scratch("readonly-analysis");let path=dir.join("db");
        assert!(Reader::open(&path).is_err());assert!(!path.exists());
        let owner=Store::open(&path).unwrap();owner.append(&[event("first"),event("second")]).unwrap();
        let reader=Reader::open(&path).unwrap();
        assert!(reader.db.execute("DELETE FROM events",[]).is_err());
        let first=reader.export(0,0,1).unwrap();assert_eq!(first["events"].as_array().unwrap().len(),1);assert_eq!(first["more"],true);
        owner.append(&[event("later")]).unwrap();
        let second=reader.export(0,first["next"].as_i64().unwrap(),1).unwrap();
        assert_eq!(second["events"][0]["observation"]["operation"],"second");assert_eq!(second["more"],false);
        assert_eq!(second["retained_seq"],first["retained_seq"]);
        assert!(reader.export(0,0,1025).is_err());drop(reader);
        assert_eq!(Reader::open(&path).unwrap().export(0,0,1024).unwrap()["events"].as_array().unwrap().len(),3);
        drop(owner);
        for version in [5,7] {
            Connection::open(&path).unwrap().pragma_update(None,"user_version",version).unwrap();
            let before=std::fs::read(&path).unwrap();assert!(Reader::open(&path).is_err());assert_eq!(std::fs::read(&path).unwrap(),before);
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
    use std::{fs, io::{BufRead, Write}, process::{Command, Stdio}, sync::Arc};
    // Frozen schema-1 producer: the upgrader must preserve its rows and independent cursors.
    fn legacy(path: &Path, events: &[Event]) -> Vec<Row> {
        let db=Connection::open(path).unwrap();db.execute_batch("BEGIN;
            CREATE TABLE events(seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL UNIQUE,received_ms INTEGER NOT NULL,started_ms INTEGER NOT NULL,payload TEXT NOT NULL,bytes INTEGER NOT NULL);
            CREATE INDEX events_started ON events(started_ms,seq);CREATE INDEX events_received ON events(received_ms,seq);
            CREATE TABLE metadata(id INTEGER PRIMARY KEY CHECK(id=1),retained_bytes INTEGER NOT NULL,pruned_count INTEGER NOT NULL,pruned_through INTEGER NOT NULL);INSERT INTO metadata VALUES(1,0,0,0);
            CREATE TABLE cursors(destination TEXT PRIMARY KEY,seq INTEGER NOT NULL,receipt TEXT NOT NULL);
            CREATE TRIGGER inserted AFTER INSERT ON events BEGIN UPDATE metadata SET retained_bytes=retained_bytes+new.bytes WHERE id=1; END;
            CREATE TRIGGER deleted AFTER DELETE ON events BEGIN UPDATE metadata SET retained_bytes=retained_bytes-old.bytes,pruned_count=pruned_count+1,pruned_through=max(pruned_through,old.seq) WHERE id=1; END;
            PRAGMA user_version=1;PRAGMA application_id=1129403441;").unwrap();
        let now=clock().unwrap()-60_000;let mut rows=Vec::new();
        for e in events {
            db.execute("INSERT INTO events(id,received_ms,started_ms,payload,bytes) VALUES(?1,?2,?3,?4,?5)",params![e.id,now,e.started_ms,e.payload,e.payload.len() as i64]).unwrap();
            rows.push(Row{seq:db.last_insert_rowid(),id:e.id.clone(),received_ms:now,started_ms:e.started_ms,kind:e.kind.clone(),payload:e.payload.clone()});
        }
        db.execute_batch("COMMIT").unwrap();rows
    }
    fn all_rows(store: &Store) -> Vec<Value> {
        let mut after=0;let mut rows=Vec::new();
        loop {let page=store.export(0,after,1024).unwrap();rows.extend(page["events"].as_array().unwrap().iter().cloned());
            after=page["next"].as_i64().unwrap();if page["more"]==false {break;}}
        rows
    }
    #[test]
    fn interrupted_empty_initialization_recovers_without_adopting_foreign_objects() {
        let dir=crate::scratch("telemetry-empty-bootstrap");
        for initialized_header in [false,true] {
            let path=dir.join(if initialized_header {"header"} else {"empty"});
            if initialized_header {Connection::open(&path).unwrap().execute_batch("VACUUM").unwrap();} else {fs::write(&path,[]).unwrap();}
            let store=Store::open(&path).unwrap();assert_eq!(store.append(&[event("after.restart")]).unwrap(),vec![1]);drop(store);
            let store=Store::open(&path).unwrap();assert_eq!(all_rows(&store).len(),1);drop(store);
        }
        let path=dir.join("foreign");{let db=Connection::open(&path).unwrap();db.execute_batch("CREATE TABLE kept(value TEXT);INSERT INTO kept VALUES('preserve')").unwrap();}
        let before=fs::read(&path).unwrap();assert!(Store::open(&path).is_err());assert_eq!(fs::read(&path).unwrap(),before);
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn schema_one_upgrade_preserves_export_receipts_imports_and_legacy_writer_rows() {
        let dir=crate::scratch("telemetry-upgrade");let path=dir.join("db");
        let events=(0..1537).map(|n| {let mut v:Value=serde_json::from_str(&event("history.row").payload).unwrap();v["started_ms"]=json!(1000+n%17);v["duration_us"]=Value::Null;v["unknown"]=json!({"duration_us":"not-reported"});Event::new(v).unwrap()}).collect::<Vec<_>>();
        let rows=legacy(&path,&events);let expected=rows.iter().map(|r|r.json().unwrap()).collect::<Vec<_>>();
        let first=Batch{destination:"global".into(),after:0,rows:rows[..1024].to_vec(),pruned_through:0};
        let old=Connection::open(&path).unwrap();
        old.execute("INSERT INTO cursors VALUES('global',1024,?1)",[first.receipt()]).unwrap();
        old.execute_batch("CREATE TABLE imports(source TEXT PRIMARY KEY,cursor TEXT NOT NULL,receipt TEXT NOT NULL); CREATE TABLE deletion_audit(seq INTEGER); CREATE TRIGGER audit_delete AFTER DELETE ON events BEGIN INSERT INTO deletion_audit VALUES(old.seq); END;").unwrap();
        old.execute("INSERT INTO imports VALUES('archive','offset.1537',?1)",["a".repeat(64)]).unwrap();
        let mut writer=old.prepare("INSERT INTO events(id,received_ms,started_ms,payload,bytes) VALUES(?1,?2,?3,?4,?5)").unwrap();
        let store=Store::open(&path).unwrap();assert_eq!(all_rows(&store),expected);
        assert_eq!(store.db.lock().unwrap().query_row("SELECT count(*) FROM deletion_audit",[],|r|r.get::<_,i64>(0)).unwrap(),0);
        assert_eq!(store.import_cursor("archive").unwrap().as_deref(),Some("offset.1537"));
        let next=Batch{destination:"global".into(),after:1024,rows:rows[1024..].to_vec(),pruned_through:0};
        assert_eq!(store.offer("global",1024).unwrap().json().unwrap(),next.json().unwrap());
        assert_eq!(store.db.lock().unwrap().pragma_query_value(None,"user_version",|r|r.get::<_,i64>(0)).unwrap(),6);
        assert!(store.storage().unwrap()["compressed_payload_bytes"].as_i64().unwrap()<store.storage().unwrap()["retained_payload_bytes"].as_i64().unwrap()/4);
        let late=event("legacy.late");writer.execute(params![late.id,clock().unwrap(),late.started_ms,late.payload,late.payload.len() as i64]).unwrap();
        assert_eq!(store.append(&[late.clone()]).unwrap(),vec![1538]);assert_eq!(all_rows(&store).len(),1538);
        assert!(store.storage().unwrap()["legacy_payload_bytes"].as_i64().unwrap()>0);
        drop(writer);drop(old);let before=all_rows(&store);drop(store);
        let store=Store::open(&path).unwrap();assert_eq!(all_rows(&store),before);assert_eq!(store.storage().unwrap()["legacy_payload_bytes"],0);
        assert_eq!(store.append(&[event("new.row")]).unwrap(),vec![1539]);validate_store(&store.db.lock().unwrap()).unwrap();drop(store);fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn empty_history_upgrade_keeps_the_sequence_floor_and_original_retention_clock() {
        let dir=crate::scratch("telemetry-empty-upgrade");let path=dir.join("db");let rows=legacy(&path,&[event("old"),event("older")]);
        {let db=Connection::open(&path).unwrap();db.execute_batch("DELETE FROM events; INSERT INTO cursors VALUES('global',2,'prior-receipt')").unwrap();}
        let store=Store::open(&path).unwrap();assert_eq!(store.append(&[event("new")]).unwrap(),vec![3]);
        assert_eq!(store.offer("global",1024).unwrap().rows[0].seq,3);drop(store);
        let second=dir.join("expired");legacy(&second,&[event("expired")]);
        {let db=Connection::open(&second).unwrap();db.execute("UPDATE events SET received_ms=?1",[clock().unwrap()-AGE_MS-1]).unwrap();}
        let store=Store::open(&second).unwrap();assert_eq!(all_rows(&store)[0]["received_ms"].as_i64().unwrap(),store.db.lock().unwrap().query_row("SELECT received_ms FROM events",[],|r|r.get::<_,i64>(0)).unwrap());
        store.prune().unwrap();assert!(all_rows(&store).is_empty());assert_eq!(rows[1].seq,2);drop(store);fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn failed_schema_upgrade_rolls_back_ddl_rows_and_cursors() {
        let dir=crate::scratch("telemetry-upgrade-rollback");let path=dir.join("db");let events=(0..1100).map(|_|event("preserved")).collect::<Vec<_>>();let expected=legacy(&path,&events);
        {let db=Connection::open(&path).unwrap();db.execute("UPDATE events SET payload='not-json',bytes=8 WHERE seq=1050",[]).unwrap();
            db.execute_batch("UPDATE metadata SET retained_bytes=(SELECT sum(bytes) FROM events); INSERT INTO cursors VALUES('global',999,'prior-receipt')").unwrap();}
        assert!(Store::open(&path).err().unwrap().contains("corrupt observation JSON"));
        {let db=Connection::open(&path).unwrap();assert_eq!(db.pragma_query_value(None,"user_version",|r|r.get::<_,i64>(0)).unwrap(),1);
            assert!(!db.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='blocks')",[],|r|r.get::<_,bool>(0)).unwrap());
            let found=db.prepare("SELECT id,payload,received_ms FROM events ORDER BY seq").unwrap().query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,i64>(2)?))).unwrap().collect::<Result<Vec<_>,_>>().unwrap();
            let reference=expected.iter().enumerate().map(|(i,r)|(r.id.clone(),if i==1049 {"not-json".into()} else {r.payload.clone()},r.received_ms)).collect::<Vec<_>>();assert_eq!(found,reference);
            assert_eq!(db.query_row("SELECT seq FROM sqlite_sequence WHERE name='events'",[],|r|r.get::<_,i64>(0)).unwrap(),1100);
            assert_eq!(db.query_row("SELECT seq,receipt FROM cursors WHERE destination='global'",[],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?))).unwrap(),(999,"prior-receipt".into()));
            db.execute("UPDATE events SET payload=?1,bytes=?2 WHERE seq=1050",params![events[1049].payload,events[1049].payload.len() as i64]).unwrap();db.execute_batch("UPDATE metadata SET retained_bytes=(SELECT sum(bytes) FROM events)").unwrap();}
        let store=Store::open(&path).unwrap();assert_eq!(all_rows(&store),expected.iter().map(|r|r.json().unwrap()).collect::<Vec<_>>());drop(store);fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn compact_blocks_and_retention_work_with_minimum_and_large_database_pages() {
        let dir=crate::scratch("telemetry-compact-pages");
        for size in [1024,65536] {
            let path=dir.join(format!("page-{size}"));
            {let db=Connection::open(&path).unwrap();db.pragma_update(None,"page_size",size).unwrap();db.execute_batch("VACUUM").unwrap();}
            legacy(&path,&[event("original")]);let store=Store::open(&path).unwrap();
            assert_eq!(store.db.lock().unwrap().pragma_query_value(None,"page_size",|r|r.get::<_,i64>(0)).unwrap(),size);
            for n in 0..4 {store.append(&(0..512).map(|i|large_event(&format!("pages.{n}.{i}"))).collect::<Vec<_>>()).unwrap();assert_eq!(store.storage().unwrap()["state"],"bounded");}
            assert_eq!(all_rows(&store).len(),2049);
            {let db=store.db.lock().unwrap();prune(&db,clock().unwrap(),AGE_MS,0).unwrap();validate_store(&db).unwrap();}
            store.prune().unwrap();assert_eq!(store.storage().unwrap()["block_count"],0);assert_eq!(store.append(&[event("after.prune")]).unwrap(),vec![2050]);
            drop(store);let reopened=Store::open(&path).unwrap();assert_eq!(all_rows(&reopened).len(),1);drop(reopened);
        }
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn corrupt_block_is_refused_and_preserved_instead_of_reset() {
        let dir=crate::scratch("telemetry-corrupt-block");let path=dir.join("db");let store=Store::open(&path).unwrap();store.append(&[event("preserved")]).unwrap();let before=all_rows(&store);drop(store);
        let original={let db=Connection::open(&path).unwrap();let data:Vec<u8>=db.query_row("SELECT payload FROM blocks",[],|r|r.get(0)).unwrap();let mut bad=data.clone();let last=bad.len()-1;bad[last]^=1;db.execute("UPDATE blocks SET payload=?1",[bad]).unwrap();data};
        let bytes=fs::read(&path).unwrap();assert!(Store::open(&path).err().unwrap().contains("corrupt compressed block"));assert_eq!(fs::read(&path).unwrap(),bytes);
        {let db=Connection::open(&path).unwrap();db.execute("UPDATE blocks SET payload=?1",[original]).unwrap();}
        let store=Store::open(&path).unwrap();assert_eq!(all_rows(&store),before);drop(store);fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn killed_schema_upgrade_preserves_old_rows_and_can_resume() {
        let dir=crate::scratch("telemetry-upgrade-kill");let path=dir.join("db");let expected=legacy(&path,&(0..2100).map(|_|event("preserved")).collect::<Vec<_>>());let marker=dir.join("paused");
        let mut child=Command::new(std::env::current_exe().unwrap()).args(["--exact","telemetry::tests::upgrade_process","--ignored"])
            .env("COREQ_TELEMETRY_TEST_UPGRADE_DB",&path).env("COREQ_TELEMETRY_TEST_UPGRADE_PAUSE",&marker).stdin(Stdio::piped()).stdout(Stdio::null()).spawn().unwrap();
        let until=Instant::now()+Duration::from_secs(10);while !marker.exists()&&Instant::now()<until {if child.try_wait().unwrap().is_some() {break;}std::thread::sleep(Duration::from_millis(10));}
        let paused=marker.exists();let _=child.kill();child.wait().unwrap();assert!(paused,"child never reached the uncommitted upgrade");
        {let db=Connection::open(&path).unwrap();assert_eq!(db.pragma_query_value(None,"user_version",|r|r.get::<_,i64>(0)).unwrap(),1);
            assert!(!db.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='blocks')",[],|r|r.get::<_,bool>(0)).unwrap());
            assert_eq!(db.query_row("SELECT count(*) FROM events WHERE payload!=''",[],|r|r.get::<_,i64>(0)).unwrap(),2100);}
        let store=Store::open(&path).unwrap();assert_eq!(all_rows(&store),expected.iter().map(|r|r.json().unwrap()).collect::<Vec<_>>());drop(store);fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    #[ignore="subprocess helper exercised by killed_schema_upgrade_preserves_old_rows_and_can_resume"]
    fn upgrade_process() {let path=std::env::var_os("COREQ_TELEMETRY_TEST_UPGRADE_DB").unwrap();Store::open(Path::new(&path)).unwrap();}
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
        let rejected=event("refused.probe");
        store.db.lock().unwrap().execute_batch(&format!("CREATE TRIGGER fail_probe BEFORE INSERT ON events WHEN new.id='{}' BEGIN SELECT RAISE(ABORT,'injected'); END;",rejected.id)).unwrap();
        assert!(store.append(&[event("ordinary.probe"),rejected]).is_err());
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
        let export = store.export(0,0,10).unwrap(); assert_eq!(store.storage().unwrap()["block_count"],1); assert_eq!(export["pruned_count"],1); assert_eq!(export["events"][0]["id"],e[1].id());
        assert!(store.acknowledge(&batch,&batch.receipt()).is_err());
        assert_eq!(store.offer("global",2).unwrap().json().unwrap()["gap"],true);
        { let db = store.db.lock().unwrap();
          db.execute("UPDATE events SET received_ms=1 WHERE seq=?1",[seq[1]]).unwrap();
          prune(&db,AGE_MS+2,AGE_MS,PAYLOAD_BYTES).unwrap(); }
        assert_eq!(store.export(0,0,10).unwrap()["pruned_count"],2);assert_eq!(store.storage().unwrap()["block_count"],0);
        assert!(store.offer("global",2).unwrap().is_empty());
        drop(store); fs::remove_dir_all(dir).unwrap();
    }
    fn large_event(operation: &str) -> Event {
        let mut value=serde_json::from_str::<Value>(&event(operation).payload).unwrap();
        let mut seed=1u64;for byte in operation.bytes() {seed=seed.wrapping_mul(131).wrapping_add(byte as u64);}
        let mut noise=|| (0..128).map(|_| {seed^=seed<<13;seed^=seed>>7;seed^=seed<<17;b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-"[(seed&63) as usize] as char}).collect::<String>();
        for key in TEXT {if !value.as_object().unwrap().contains_key(*key)&&*key!="project" {value[*key]=json!(noise());}}
        value["project"]=json!("fixture/project");
        let mut unknown=serde_json::Map::new();
        for key in NUMBERS {if !value.as_object().unwrap().contains_key(*key) {value[*key]=Value::Null;unknown.insert((*key).into(),json!(noise()));}}
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
        assert!(before["retained_payload_bytes"].as_i64().unwrap()<PAYLOAD_BYTES,"physical pressure must precede the logical payload limit");
        let oldest=store.export(0,0,1).unwrap()["events"][0].clone();assert_ne!(oldest["id"],first);
        assert_eq!(store.export(0,last_seq-1,1).unwrap()["events"][0]["id"],last);
        let mut conflict=event("conflict");conflict.id=oldest["id"].as_str().unwrap().into();
        assert!(store.append(&[conflict]).unwrap_err().contains("immutable id"));
        assert_eq!(store.storage().unwrap()["pruned_count"],before["pruned_count"],"id validation precedes capacity deletion");
        let mut batch=(0..512).map(|n|large_event(&format!("rollback.{n}"))).collect::<Vec<_>>();batch.push(large_event("refused.probe"));
        store.db.lock().unwrap().execute_batch(&format!("CREATE TRIGGER physical_failure BEFORE INSERT ON events WHEN new.id='{}' BEGIN SELECT RAISE(ABORT,'injected'); END;",batch.last().unwrap().id)).unwrap();
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
    fn queued_commit_group_rolls_back_late_chunks_and_retries_with_the_same_ids() {
        let dir=crate::scratch("telemetry-group");let path=dir.join("db");let store=Store::open(&path).unwrap();
        let retained=event("retained");store.append(&[retained.clone()]).unwrap();let before=store.export(0,0,1024).unwrap();
        let mut events=(0..BATCH).map(|_|event("group.row")).collect::<Vec<_>>();events.push(event("group.refused"));
        store.db.lock().unwrap().execute_batch(&format!("CREATE TRIGGER reject_group BEFORE INSERT ON events WHEN new.id='{}' BEGIN SELECT RAISE(ABORT,'injected late group failure'); END;",events.last().unwrap().id)).unwrap();
        assert!(store.append_group(&events).unwrap_err().contains("injected late group failure"));
        assert_eq!(store.export(0,0,1024).unwrap(),before);
        store.db.lock().unwrap().execute_batch("DROP TRIGGER reject_group").unwrap();
        let ids=store.append_group(&events).unwrap();assert_eq!(ids.len(),BATCH+1);assert_eq!(ids[0],2);
        assert!(ids.windows(2).all(|p|p[0]<p[1]));assert_eq!(store.append_group(&events).unwrap(),ids);
        let mut conflicting=(0..BATCH).map(|_|event("group.row")).collect::<Vec<_>>();let mut changed=conflicting[0].clone();changed.payload=changed.payload.replace("group.row","group.changed");conflicting.push(changed);
        assert!(store.append_group(&conflicting).unwrap_err().contains("immutable id payload mismatch"));
        let mut conflicting=(0..BATCH).map(|_|event("group.row")).collect::<Vec<_>>();let mut changed=retained.clone();changed.payload=changed.payload.replace("retained","changed");conflicting.push(changed);
        assert!(store.append_group(&conflicting).unwrap_err().contains("immutable id payload mismatch"));
        assert_eq!(store.db.lock().unwrap().query_row("SELECT count(*) FROM events",[],|r|r.get::<_,i64>(0)).unwrap(),BATCH as i64+2);
        drop(store);fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn queued_commit_group_enforces_both_count_and_decoded_byte_bounds() {
        let dir=crate::scratch("telemetry-group-bounds");let store=Store::open(&dir.join("db")).unwrap();
        let mut value: Value=serde_json::from_str(&event("group.large").payload).unwrap();
        for key in ["product","version","operation","kind","trace","span","parent","error","runtime","model","host","client_version","project_spec","shared_spec"] {value[key]=json!("x".repeat(128));}
        value["unknown"]=json!({});for key in NUMBERS.iter().filter(|key|**key!="started_ms") {value[*key]=Value::Null;value["unknown"][*key]=json!("x".repeat(128));}
        let event=Event::new(value).unwrap();assert!(event.payload.len()*COMMIT_EVENTS>COMMIT_BYTES);
        assert!(store.append_group(&vec![event.clone();COMMIT_EVENTS]).unwrap_err().contains("commit group too large"));
        assert!(store.append_group(&vec![event.clone();COMMIT_EVENTS+1]).is_err());
        assert!(store.append(&vec![event;BATCH+1]).unwrap_err().contains("batch too large"));
        assert_eq!(store.export(0,0,1024).unwrap()["events"],json!([]));drop(store);fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn append_checkpoint_credit_is_bounded_and_preserves_pinned_readers() {
        for page in [1024,2048,4096,8192,16384,32768,65536] {
            let frame=page as u64+24;
            let maximum=(DATABASE_BYTES as u64/page as u64+65536u64.div_ceil(frame))*frame;
            let credit=((WAL_BYTES-32-maximum)/frame) as i64;
            assert!(wal_has_room(credit,page));assert!(!wal_has_room(credit+1,page));
        }
        assert!(!wal_has_room(i64::MAX,4096));assert!(!wal_has_room(-1,4096));assert!(!wal_has_room(0,512));assert!(!wal_has_room(0,3072));
        let dir=crate::scratch("telemetry-checkpoint-credit");let path=dir.join("db");let store=Store::open(&path).unwrap();
        store.append(&[event("first")]).unwrap();
        let frames=||{let db=store.db.lock().unwrap();db.query_row("PRAGMA wal_checkpoint(NOOP)",[],|r|r.get::<_,i64>(1)).unwrap()};
        let before=frames();assert!(before>0);
        let reader=Connection::open(&path).unwrap();reader.execute_batch("BEGIN; SELECT * FROM events").unwrap();
        store.append(&[event("second")]).unwrap();assert!(frames()>before,"small append unnecessarily restarted WAL");
        assert_eq!(reader.query_row("SELECT count(*) FROM events",[],|r|r.get::<_,i64>(0)).unwrap(),1);
        assert_eq!(store.export(0,0,1024).unwrap()["events"].as_array().unwrap().len(),2);
        assert!(store.storage().unwrap()["wal_bytes"].as_u64().unwrap()<=WAL_BYTES);
        reader.execute_batch("COMMIT").unwrap();drop(reader);drop(store);
        assert_eq!(Store::open(&path).unwrap().export(0,0,1024).unwrap()["events"].as_array().unwrap().len(),2);
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn pinned_reader_refuses_mutations_without_data_or_cursor_changes() {
        let dir=crate::scratch("telemetry-pinned");let path=dir.join("db");let store=Store::open(&path).unwrap();
        store.append(&(0..512).map(|n|large_event(&format!("committed.{n}"))).collect::<Vec<_>>()).unwrap();let batch=store.offer("global",1).unwrap();
        let reader=Connection::open(&path).unwrap();reader.execute_batch("BEGIN; SELECT * FROM events;").unwrap();
        let before=store.storage().unwrap();
        assert!(store.append(&[event("refused")]).unwrap_err().contains("checkpoint busy"));
        assert!(store.import("archive",None,"position.1",&[event("refused.import")]).unwrap_err().contains("checkpoint busy"));
        assert!(store.acknowledge(&batch,&batch.receipt()).unwrap_err().contains("checkpoint busy"));
        assert!(store.prune().unwrap_err().contains("checkpoint busy"));
        assert_eq!(store.import_cursor("archive").unwrap(),None);
        assert_eq!(store.offer("global",1).unwrap().receipt(),batch.receipt());
        assert_eq!(store.export(0,0,1024).unwrap()["events"].as_array().unwrap().len(),512);
        assert_eq!(store.storage().unwrap()["wal_bytes"],before["wal_bytes"]);
        reader.execute_batch("ROLLBACK").unwrap();drop(reader);
        store.append(&[event("after.reader")]).unwrap();assert_eq!(store.export(0,0,1024).unwrap()["events"].as_array().unwrap().len(),513);
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
    fn semantic_metadata_preserves_unknown_identity_unsigned_counts_and_fixed_codes() {
        let dir=crate::scratch("telemetry-semantic");let path=dir.join("db");let store=Store::open(&path).unwrap();
        let mut value:Value=serde_json::from_str(&event("build").payload).unwrap();
        for key in ["version","trace","span"] {value[key]=Value::Null;value["unknown"][key]=json!("not-recorded");}
        value["kind"]=json!("stage");value["outcome"]=json!("ask");value["project"]=json!("group/nested/repo");value["open"]=json!(true);
        for key in ["run_id","process_id","span_id","parent_id","entry","previous_seconds","remaining","calls","tokens"] {value[key]=json!(u64::MAX);}
        value["from"]=json!("doing");value["to"]=json!("ask");value["source"]=json!("worker.result");value["usage_status"]=json!("no-model");value["counts"]=json!({"GET:issues.N.comments":u64::MAX,"POST:graphql":0});
        let observation=Event::new(value.clone()).unwrap();store.append(&[observation.clone()]).unwrap();drop(store);
        let store=Store::open(&path).unwrap();assert_eq!(store.export(0,0,1).unwrap()["events"][0]["observation"],value);assert_eq!(store.append(&[observation]).unwrap(),vec![1]);
        for (key,bad) in [("open",json!("true")),("counts",json!({"GET:/private/path":1})),("counts",json!({"GET:issues":-1})),("counts",json!({"GET:issues":1.5})),("project",json!("/tmp/repo")),("project",json!("group/../repo")),("started_ms",json!(u64::MAX))] {
            let mut invalid=value.clone();invalid[key]=bad;assert!(Event::new(invalid).is_err(),"invalid {key}");
        }
        let mut missing=value.clone();missing["unknown"].as_object_mut().unwrap().remove("version");assert!(Event::new(missing).is_err());
        value["project"]=json!("other");assert!(Event::new(value).is_ok());drop(store);fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn reclaim_frees_pages_without_changing_observations_receipts_or_sequence_floor() {
        let dir=crate::scratch("telemetry-reclaim");let path=dir.join("db");let store=Store::open(&path).unwrap();store.append(&(0..512).map(|n|large_event(&format!("reclaim.{n}"))).collect::<Vec<_>>()).unwrap();
        store.import_history("archive",None,"position.1",&[event("imported.keep")],&[clock().unwrap()-60_000]).unwrap();let batch=store.offer("global",2).unwrap();store.acknowledge(&batch,&batch.receipt()).unwrap();store.db.lock().unwrap().execute("DELETE FROM events WHERE seq<=500",[]).unwrap();store.prune().unwrap();
        let before=store.export(0,0,1024).unwrap();let storage=store.storage().unwrap();let offered=store.offer("global",1024).unwrap().json().unwrap();assert!(storage["reusable_database_bytes"].as_u64().unwrap()>0);
        let old=Connection::open(&path).unwrap();let mut writer=old.prepare("INSERT INTO events(id,received_ms,started_ms,payload,bytes) VALUES(?1,?2,?3,?4,?5)").unwrap();
        store.reclaim().unwrap();let after=store.storage().unwrap();assert!(after["database_bytes"].as_u64().unwrap()<storage["database_bytes"].as_u64().unwrap());assert_eq!(after["reusable_database_bytes"],0);assert_eq!(store.export(0,0,1024).unwrap(),before);assert_eq!(store.offer("global",1024).unwrap().json().unwrap(),offered);assert_eq!(store.import_cursor("archive").unwrap().as_deref(),Some("position.1"));
        assert_eq!(store.append(&[event("next")]).unwrap(),vec![514]);let late=event("older.writer");writer.execute(params![late.id,clock().unwrap(),late.started_ms,late.payload,late.payload.len() as i64]).unwrap();assert_eq!(store.export(0,514,1024).unwrap()["events"][0]["id"],late.id);drop(writer);drop(old);drop(store);let store=Store::open(&path).unwrap();assert_eq!(store.append(&[event("after.reopen")]).unwrap(),vec![516]);drop(store);fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn reclaim_refuses_a_live_ipc_owner_before_rewriting_retained_data() {
        let dir=crate::scratch("telemetry-reclaim-owner");let path=dir.join("db");let store=Store::open(&path).unwrap();
        store.append(&(0..512).map(|n|large_event(&format!("old.{n}"))).collect::<Vec<_>>()).unwrap();
        store.append(&[event("retained")]).unwrap();
        {let db=store.db.lock().unwrap();db.execute("UPDATE events SET received_ms=1 WHERE seq<=512",[]).unwrap();}
        store.prune().unwrap();let owner=broker::open(&path).unwrap();
        let before=all_rows(&store);let allocation=store.storage().unwrap();assert!(allocation["reusable_database_bytes"].as_u64().unwrap()>0);
        assert!(store.reclaim().is_err(),"maintenance must refuse a live IPC writer before compaction");
        assert_eq!(all_rows(&store),before);assert_eq!(store.storage().unwrap(),allocation);
        drop(owner);store.reclaim().unwrap();assert_eq!(all_rows(&store),before);
        assert!(store.storage().unwrap()["database_bytes"].as_u64().unwrap()<allocation["database_bytes"].as_u64().unwrap());
        drop(store);fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn reclaim_refuses_pinned_reader_and_corrupt_metadata_without_data_loss() {
        let dir=crate::scratch("telemetry-reclaim-refuse");let path=dir.join("db");let store=Store::open(&path).unwrap();store.append(&[event("one"),event("two")]).unwrap();store.db.lock().unwrap().execute("DELETE FROM events WHERE seq=1",[]).unwrap();
        let before=store.export(0,0,1024).unwrap();let reader=Connection::open(&path).unwrap();reader.execute_batch("BEGIN; SELECT * FROM events").unwrap();assert!(store.reclaim().unwrap_err().contains("checkpoint busy"));assert_eq!(store.export(0,0,1024).unwrap(),before);reader.execute_batch("COMMIT").unwrap();drop(reader);
        store.db.lock().unwrap().execute("UPDATE events SET kind='wrong'",[]).unwrap();assert!(store.reclaim().unwrap_err().contains("corrupt kind"));assert_eq!(store.db.lock().unwrap().query_row("SELECT count(*) FROM events",[],|r|r.get::<_,i64>(0)).unwrap(),1);drop(store);fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn indexed_kind_pages_avoid_generic_payloads_and_use_the_kind_index() {
        let dir=crate::scratch("telemetry-kind-query");let store=Store::open(&dir.join("db")).unwrap();
        for _ in 0..8 {store.append(&(0..1024).map(|_|event("generic.method")).collect::<Vec<_>>()).unwrap();}
        let make=|kind:&str|{let mut value:Value=serde_json::from_str(&event("semantic").payload).unwrap();value["kind"]=json!(kind);Event::new(value).unwrap()};
        let stage=make("stage");let moved=make("transition");store.append(&[stage.clone(),moved.clone()]).unwrap();
        let one=store.export_kinds(&["stage","transition"],0,0,1).unwrap();assert_eq!(one["events"].as_array().unwrap().len(),1);assert_eq!(one["events"][0]["id"],stage.id);assert_eq!(one["examined"],1);assert_eq!(one["more"],true);assert_eq!(one["unindexed_count"],0);
        let two=store.export_kinds(&["stage","transition"],0,one["next"].as_i64().unwrap(),1).unwrap();assert_eq!(two["events"][0]["id"],moved.id);assert_eq!(two["more"],false);
        {let db=store.db.lock().unwrap();let sql=format!("EXPLAIN QUERY PLAN {}",kind_query(2));let mut read=db.prepare(&sql).unwrap();let plan=read.query_map(params!["stage","transition",0,0,1],|row|row.get::<_,String>(3)).unwrap().collect::<Result<Vec<_>,_>>().unwrap().join("\n");assert!(plan.contains("events_kind"),"{plan}");assert!(!plan.contains("SCAN events"),"{plan}");
        db.execute("UPDATE blocks SET payload=x'00' WHERE id=(SELECT block FROM events WHERE seq=1)",[]).unwrap();}
        assert_eq!(store.export_kinds(&["stage"],0,0,1024).unwrap()["events"][0]["id"],stage.id);assert!(store.export(0,0,1024).is_err(),"unselected generic block was intentionally corrupted");
        assert!(store.export_kinds(&[],0,0,1).is_err());assert!(store.export_kinds(&["stage","stage"],0,0,1).is_err());assert!(store.export_kinds(&["unsafe/path"],0,0,1).is_err());assert!(store.export_kinds(&["stage"],0,0,0).is_err());
        drop(store);fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn late_older_writer_null_kinds_advance_empty_pages_and_backfill_on_reopen() {
        let dir=crate::scratch("telemetry-kind-legacy");let path=dir.join("db");let store=Store::open(&path).unwrap();let old=Connection::open(&path).unwrap();
        let generic=event("old.generic");let mut value:Value=serde_json::from_str(&event("old.stage").payload).unwrap();value["kind"]=json!("stage");value["started_ms"]=Value::Null;value["unknown"]=json!({"started_ms":"not-recorded"});let stage=Event::new(value).unwrap();
        for event in [&generic,&stage] {old.execute("INSERT INTO events(id,received_ms,started_ms,payload,bytes) VALUES(?1,?2,?3,?4,?5)",params![event.id,clock().unwrap(),event.started_ms,event.payload,event.payload.len() as i64]).unwrap();}
        let first=store.export_kinds(&["stage"],i64::MAX,0,1).unwrap();assert_eq!(first["events"][0]["id"],stage.id,"unknown source time stays included");
        let empty=store.export_kinds(&["stage"],0,0,1).unwrap();assert_eq!(empty["events"],json!([]));assert_eq!(empty["more"],true);assert_eq!(empty["next"],1);assert_eq!(empty["unindexed_count"],2);
        let found=store.export_kinds(&["stage"],0,empty["next"].as_i64().unwrap(),1).unwrap();assert_eq!(found["events"][0]["id"],stage.id);assert_eq!(found["more"],false);
        drop(old);drop(store);let store=Store::open(&path).unwrap();let indexed=store.export_kinds(&["stage"],0,0,1).unwrap();assert_eq!(indexed["unindexed_count"],0);assert_eq!(indexed["examined"],1);assert_eq!(indexed["events"][0]["id"],stage.id);drop(store);fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn compressed_kind_backfill_preserves_payloads_and_does_not_fire_update_callbacks() {
        let dir=crate::scratch("telemetry-kind-backfill");let path=dir.join("db");let store=Store::open(&path).unwrap();store.append(&[event("one"),event("two")]).unwrap();let before=store.export(0,0,1024).unwrap();let bytes=store.storage().unwrap()["compressed_payload_bytes"].clone();
        {let db=store.db.lock().unwrap();db.execute_batch("DROP INDEX events_kind; ALTER TABLE events DROP COLUMN kind; PRAGMA user_version=5; CREATE TABLE updates(n INTEGER); INSERT INTO updates VALUES(0); CREATE TRIGGER count_updates AFTER UPDATE ON events BEGIN UPDATE updates SET n=n+1; END").unwrap();}drop(store);
        let store=Store::open(&path).unwrap();assert_eq!(store.export(0,0,1024).unwrap(),before);assert_eq!(store.storage().unwrap()["compressed_payload_bytes"],bytes);assert_eq!(store.db.lock().unwrap().query_row("SELECT n FROM updates",[],|r|r.get::<_,i64>(0)).unwrap(),0);
        store.db.lock().unwrap().execute("UPDATE events SET kind=kind",[]).unwrap();assert_eq!(store.db.lock().unwrap().query_row("SELECT n FROM updates",[],|r|r.get::<_,i64>(0)).unwrap(),2,"trigger mode restored after backfill");drop(store);fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn late_corrupt_block_rolls_back_kind_column_index_and_reader_floor() {
        let dir=crate::scratch("telemetry-kind-rollback");let path=dir.join("db");let store=Store::open(&path).unwrap();store.append(&(0..1024).map(|_|event("first.block")).collect::<Vec<_>>()).unwrap();store.append(&[event("second.block")]).unwrap();
        {let db=store.db.lock().unwrap();db.execute_batch("DROP INDEX events_kind; ALTER TABLE events DROP COLUMN kind; PRAGMA user_version=5; UPDATE blocks SET payload=x'00' WHERE id=(SELECT max(id) FROM blocks)").unwrap();}drop(store);
        assert!(Store::open(&path).is_err());let db=Connection::open(&path).unwrap();assert_eq!(db.pragma_query_value(None,"user_version",|r|r.get::<_,i64>(0)).unwrap(),5);assert_eq!(db.query_row("SELECT count(*) FROM pragma_table_info('events') WHERE name='kind'",[],|r|r.get::<_,i64>(0)).unwrap(),0);assert_eq!(db.query_row("SELECT count(*) FROM sqlite_master WHERE name='events_kind'",[],|r|r.get::<_,i64>(0)).unwrap(),0);assert_eq!(db.query_row("SELECT count(*) FROM events",[],|r|r.get::<_,i64>(0)).unwrap(),1025);drop(db);fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn mismatched_indexed_kind_is_refused_as_corruption() {
        let dir=crate::scratch("telemetry-kind-corrupt");let path=dir.join("db");let store=Store::open(&path).unwrap();store.append(&[event("normal")]).unwrap();store.db.lock().unwrap().execute("UPDATE events SET kind='stage'",[]).unwrap();
        assert!(store.export_kinds(&["stage"],0,0,1).unwrap_err().contains("corrupt kind index"));drop(store);assert!(Store::open(&path).is_err());let db=Connection::open(&path).unwrap();assert_eq!(db.query_row("SELECT count(*) FROM events",[],|r|r.get::<_,i64>(0)).unwrap(),1);drop(db);fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn unknown_source_time_survives_import_reopen_filters_and_sync() {
        let dir=crate::scratch("telemetry-unknown-source-time");let path=dir.join("db");let store=Store::open(&path).unwrap();
        let mut value:Value=serde_json::from_str(&event("unknown.time").payload).unwrap();value["started_ms"]=Value::Null;
        assert!(Event::new(value.clone()).is_err());value["unknown"]=json!({"started_ms":"not-recorded"});
        let unknown=Event::identified("archive","position.1",value.clone()).unwrap();let received=clock().unwrap()-60_000;
        store.append(&[event("known.time")]).unwrap();
        store.import_history("archive",None,"position.1",&[unknown.clone()],&[received]).unwrap();
        let report=store.export(0,0,1).unwrap();assert_eq!(report["unknown_started_count"],1);assert_eq!(report["retained_ms"],json!([1000,1000]));assert_eq!(report["more"],true);
        let next=store.export(0,report["next"].as_i64().unwrap(),1).unwrap();assert_eq!(next["events"][0]["observation"],value);assert_eq!(next["more"],false);
        let filtered=store.export(i64::MAX,0,1024).unwrap();assert_eq!(filtered["events"].as_array().unwrap().len(),1);assert_eq!(filtered["events"][0]["observation"],value);assert_eq!(filtered["events"][0]["received_ms"],received);
        let batch=store.offer("global",1024).unwrap();assert_eq!(batch.json().unwrap()["events"][1]["observation"],value);store.acknowledge(&batch,&batch.receipt()).unwrap();
        drop(store);let store=Store::open(&path).unwrap();assert_eq!(store.export(i64::MAX,0,1024).unwrap(),filtered);assert!(store.offer("global",1024).unwrap().is_empty());
        store.import_history("archive",None,"position.1",&[unknown],&[received]).unwrap();assert_eq!(store.export(i64::MAX,0,1024).unwrap(),filtered);
        store.db.lock().unwrap().execute("DELETE FROM events WHERE started_ms>=0",[]).unwrap();assert_eq!(store.export(0,0,1024).unwrap()["retained_ms"],json!([null,null]));
        drop(store);fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn prior_reader_floor_upgrade_preserves_history_and_rolls_back_on_corruption() {
        for version in [2,3,4,5] {
        let dir=crate::scratch("telemetry-schema-two-floor");let path=dir.join("db");let store=Store::open(&path).unwrap();
        store.append(&[event("retained")]).unwrap();let batch=store.offer("global",1).unwrap();store.acknowledge(&batch,&batch.receipt()).unwrap();
        store.import("archive",None,"position.1",&[event("imported")]).unwrap();let before=all_rows(&store);let storage=store.storage().unwrap();
        store.db.lock().unwrap().execute_batch("DROP INDEX events_kind; ALTER TABLE events DROP COLUMN kind").unwrap();store.db.lock().unwrap().pragma_update(None,"user_version",version).unwrap();drop(store);
        let store=Store::open(&path).unwrap();assert_eq!(all_rows(&store),before);assert_eq!(store.import_cursor("archive").unwrap().as_deref(),Some("position.1"));assert_eq!(store.offer("global",1).unwrap().rows[0].seq,2);
        assert_eq!(store.storage().unwrap()["compressed_payload_bytes"],storage["compressed_payload_bytes"]);assert_eq!(store.storage().unwrap()["pruned_count"],storage["pruned_count"]);
        assert_eq!(store.db.lock().unwrap().pragma_query_value(None,"user_version",|r|r.get::<_,i64>(0)).unwrap(),6);assert_eq!(store.append(&[event("next")]).unwrap(),vec![3]);
        store.db.lock().unwrap().execute_batch("DROP INDEX events_kind; ALTER TABLE events DROP COLUMN kind").unwrap();store.db.lock().unwrap().pragma_update(None,"user_version",version).unwrap();store.db.lock().unwrap().execute("UPDATE events SET started_ms=-1 WHERE seq=1",[]).unwrap();drop(store);
        assert!(Store::open(&path).is_err());let db=Connection::open(&path).unwrap();assert_eq!(db.pragma_query_value(None,"user_version",|r|r.get::<_,i64>(0)).unwrap(),version,"failed validation rolls back the reader floor");drop(db);fs::remove_dir_all(dir).unwrap();
        }
    }
    #[test]
    fn historical_receipt_clocks_expire_without_rejuvenation_or_replay_resurrection() {
        let dir=crate::scratch("telemetry-history-clock");let path=dir.join("db");let store=Store::open(&path).unwrap();
        let now=clock().unwrap();let events=[event("expired.history"),event("retained.history")];let times=[now-AGE_MS-1,now-60_000];
        store.import_history("archive",None,"position.1",&events,&times).unwrap();
        let rows=all_rows(&store);assert_eq!(rows.len(),1);assert_eq!(rows[0]["id"],events[1].id);assert_eq!(rows[0]["received_ms"],times[1]);
        let storage=store.storage().unwrap();assert_eq!(storage["pruned_count"],1);
        store.import_history("archive",None,"position.1",&events,&times).unwrap();assert_eq!(all_rows(&store),rows);assert_eq!(store.storage().unwrap()["pruned_count"],1);
        assert!(store.import_history("archive",None,"position.1",&events,&[times[0]+1,times[1]]).unwrap_err().contains("receipt mismatch"));
        assert!(store.import_history("other",None,"position.1",&events[1..],&[times[1]+1]).unwrap_err().contains("retention time mismatch"));
        assert_eq!(store.import_cursor("other").unwrap(),None);assert_eq!(store.storage().unwrap()["pruned_count"],1);
        assert!(store.import_history("invalid",None,"position.1",&events,&[1]).is_err());
        assert!(store.import_history("invalid",None,"position.1",&events,&[-1,1]).is_err());
        assert!(store.import_history("duplicates",None,"position.1",&[events[1].clone(),events[1].clone()],&[times[1],times[1]+1]).is_err());
        assert_eq!(all_rows(&store),rows);drop(store);let store=Store::open(&path).unwrap();assert_eq!(all_rows(&store),rows);
        assert_eq!(store.import_cursor("archive").unwrap().as_deref(),Some("position.1"));drop(store);fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn historical_receipt_clocks_and_progress_rollback_with_a_late_failure() {
        let dir=crate::scratch("telemetry-history-atomic");let path=dir.join("db");let store=Store::open(&path).unwrap();
        let now=clock().unwrap();let first=[event("first.history")];store.import_history("archive",None,"position.1",&first,&[now-10_000]).unwrap();
        let before=all_rows(&store);let next=[event("next.history"),event("later.history")];let times=[now-9000,now-8000];
        store.db.lock().unwrap().execute_batch("CREATE TRIGGER reject_history BEFORE UPDATE ON imports BEGIN SELECT RAISE(ABORT,'injected historical cursor failure'); END;").unwrap();
        assert!(store.import_history("archive",Some("position.1"),"position.2",&next,&times).unwrap_err().contains("injected historical cursor failure"));
        assert_eq!(all_rows(&store),before);assert_eq!(store.import_cursor("archive").unwrap().as_deref(),Some("position.1"));
        store.db.lock().unwrap().execute_batch("DROP TRIGGER reject_history").unwrap();store.import_history("archive",Some("position.1"),"position.2",&next,&times).unwrap();
        let rows=all_rows(&store);assert_eq!(rows.len(),3);assert_eq!(rows[1]["received_ms"],times[0]);assert_eq!(rows[2]["received_ms"],times[1]);
        assert_eq!(rows[1]["id"],next[0].id);assert_eq!(rows[2]["id"],next[1].id);
        let ordinary=[event("ordinary.import")];store.import("plain",None,"plain.1",&ordinary).unwrap();
        let expected=crate::sha256_hex(&serde_json::to_vec(&json!(["plain",Value::Null,"plain.1",ordinary.iter().map(|e|(&e.id,&e.payload)).collect::<Vec<_>>()])).unwrap());
        assert_eq!(store.db.lock().unwrap().query_row("SELECT receipt FROM imports WHERE source='plain'",[],|r|r.get::<_,String>(0)).unwrap(),expected,"plain import receipt stays compatible");
        drop(store);fs::remove_dir_all(dir).unwrap();
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
        fs::remove_file(&p).unwrap();let store=Store::open(&p).unwrap();store.db.lock().unwrap().pragma_update(None,"user_version",7).unwrap();drop(store);
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
        let historical=std::env::var("COREQ_TELEMETRY_CRASH_TIME").unwrap().parse::<i64>().unwrap();
        store.import_history("crash.history",None,"position.1",&[event("history.before.kill")],&[historical]).unwrap();
        println!("COMMITTED");std::io::stdout().flush().unwrap();loop {std::thread::park();}
    }
    // Chain: acknowledged FULL commit -> forced process termination -> reopen contains that exact observation.
    #[test]
    fn acknowledged_commit_survives_kill() {
        let dir=crate::scratch("telemetry-crash");let p=dir.join("db");let historical=clock().unwrap()-AGE_MS/2;
        let mut child=Command::new(std::env::current_exe().unwrap()).args(["--exact","telemetry::tests::crash_writer","--ignored","--nocapture"])
            .env("COREQ_TELEMETRY_CRASH_PROBE",&p).env("COREQ_TELEMETRY_CRASH_TIME",historical.to_string()).stdout(Stdio::piped()).stderr(Stdio::inherit()).spawn().unwrap();
        let mut committed=false;
        for line in std::io::BufReader::new(child.stdout.take().unwrap()).lines() {if line.unwrap().trim()=="COMMITTED" {committed=true;break;}}
        let _=child.kill();child.wait().unwrap();assert!(committed,"child never acknowledged a commit");
        let store=Store::open(&p).unwrap();let rows=store.export(0,0,10).unwrap();assert_eq!(rows["events"][0]["observation"]["operation"],"acknowledged.before.kill");
        assert_eq!(store.import_cursor("crash.archive").unwrap().as_deref(),Some("position.1"));
        assert_eq!(rows["events"][1]["observation"]["operation"],"imported.before.kill");
        let value=serde_json::from_str::<Value>(&event("imported.before.kill").payload).unwrap();
        store.import("crash.archive",None,"position.1",&[Event::identified("installation.archive","1",value).unwrap()]).unwrap();
        assert_eq!(store.export(0,0,10).unwrap()["events"].as_array().unwrap().len(),3,"replay after kill cannot duplicate either imported row");
        assert_eq!(rows["events"][2]["received_ms"],historical);assert_eq!(store.import_cursor("crash.history").unwrap().as_deref(),Some("position.1"));
        let history=Event::with_id(rows["events"][2]["observation"].clone(),Some(rows["events"][2]["id"].as_str().unwrap())).unwrap();
        store.import_history("crash.history",None,"position.1",&[history],&[historical]).unwrap();
        assert_eq!(store.export(0,0,10).unwrap()["events"].as_array().unwrap().len(),3,"historical replay after kill is idempotent");
        drop(store);fs::remove_dir_all(dir).unwrap();
    }
}
