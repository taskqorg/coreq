//! Shared local observations. Storage never performs network I/O or changes consumer state.
use ring::rand::{SecureRandom, SystemRandom};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::{Value, json};
use std::io::Write;
use std::{fs::TryLockError, path::{Path,PathBuf}, sync::Mutex, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};

pub mod capture;
/// Browser module/effect wrappers; the consumer supplies its existing bounded capture transport.
pub const JAVASCRIPT_EXECUTION:&str=include_str!("telemetry/runtime.js");
mod broker;
mod compact;
mod summary;
pub use summary::{assemble,Assembler};

type Res<T> = Result<T, String>;
const AGE_MS: i64 = 30 * 24 * 60 * 60 * 1000;
// ponytail: physical caps bound retention; this logical bound stays only as an overflow guard.
const PAYLOAD_BYTES: i64 = i64::MAX;
/// Unbounded `Store::open` keeps its original cap.
const DATABASE_BYTES: i64 = 28 * 1024 * 1024;
/// Bounded consumer installation: half working share, half reserve that steady retention never uses.
pub const INSTALLATION_BYTES: u64 = 1 << 30;
/// Working share for legacy text, IPC metadata and peer stores; pass it as `Budget::reserved`.
pub const PEER_BYTES: u64 = 64 << 20;
const BOUNDED_DATABASE_BYTES: u64 = 384 << 20;
/// Capacity pruning keeps poll aggregates and change records while other history remains.
const AGGREGATES: &str = "kind IS NULL OR kind NOT IN ('poll','poll.change')";
#[cfg(test)]
const WAL_BYTES: u64 = 29 * 1024 * 1024;
const SHM_BYTES: u64 = 1024 * 1024;
/// A bounded append retries a checkpoint that a pinned reader left unfinished after this delay.
const CHECKPOINT_RETRY: Duration = Duration::from_secs(1);
/// coreq#85: bounded stores checkpoint at this much live WAL and truncate a larger file, so one checkpoint stays short.
const LIVE_WAL_BYTES: u64 = 4 << 20;
const EVENT_BYTES: usize = 16 * 1024;
const BATCH: usize = 1024;
const COMMIT_EVENTS: usize = 8192;
const COMMIT_BYTES: usize = BATCH * EVENT_BYTES;
const APPLICATION: i64 = 0x43515431;
/// CoreQ's own observation fields; TaskQ fields come from its pinned release fixture.
const TEXT: &[&str] = &["product", "version", "operation", "kind", "trace", "span", "parent", "outcome", "error", "project", "host", "runtime", "model", "project_spec", "shared_spec", "client_version", "from", "to", "source"];
const NUMBERS: &[&str] = &["started_ms", "duration_us", "active_us", "task", "attempt", "context_tokens", "compressed_before", "compressed_after", "width", "height", "scale_milli", "process_id", "remaining", "calls"];
/// Fields with their own validation below.
const OBJECTS: &[&str] = &["counts", "unknown"];
/// TaskQ v4.6.53 release asset; SHA-256 from that release's `SHA256SUMS` (coreq.md § Universal telemetry).
const TASKQ_FIELDS: &str = include_str!("telemetry/taskq-4.6.53-telemetry-fields-fixtures.json");
#[cfg(test)]
const TASKQ_FIELDS_SHA256: &str = "1c8bc4c5e11f53de09007d80cd59084b8d48737a948e2bb23ea3fc04fe1adc6a";

#[derive(Clone, Copy, Debug, PartialEq)]
enum Field { Text, Number, Flag }
/// Field kinds from a `taskq-telemetry-fields/1` fixture: its single non-null JSON type, or CoreQ's own kind.
fn fixture_fields(text: &str) -> Res<Vec<(String, Field)>> {
    let value: Value = serde_json::from_str(text).map_err(err)?;
    if value["schema"] != "taskq-telemetry-fields/1" {return Err("telemetry fixture schema".into());}
    let mut fields = Vec::new();
    for (name, types) in value["fields"].as_object().ok_or("telemetry fixture fields")? {
        if TEXT.contains(&name.as_str()) || NUMBERS.contains(&name.as_str()) || OBJECTS.contains(&name.as_str()) {continue;}
        let types: Vec<_> = types.as_array().ok_or("telemetry fixture types")?.iter().filter(|t| *t != "null").collect();
        let kind = match types.as_slice() {
            [t] if *t == "string" => Field::Text,
            [t] if *t == "number" => Field::Number,
            [t] if *t == "boolean" => Field::Flag,
            _ => return Err(format!("telemetry fixture field {name} has no CoreQ validation")),
        };
        fields.push((name.clone(), kind));
    }
    Ok(fields)
}
/// CoreQ fields plus the pinned TaskQ fixture; a test proves the fixture maps, so the fallback never drops a pinned field.
fn fields() -> &'static std::collections::HashMap<String, Field> {
    static FIELDS: std::sync::OnceLock<std::collections::HashMap<String, Field>> = std::sync::OnceLock::new();
    FIELDS.get_or_init(|| {
        TEXT.iter().map(|k| (k.to_string(), Field::Text)).chain(NUMBERS.iter().map(|k| (k.to_string(), Field::Number)))
            .chain(fixture_fields(TASKQ_FIELDS).unwrap_or_default()).collect()
    })
}
fn field(name: &str) -> Option<Field> {fields().get(name).copied()}
#[cfg(test)]
fn names(kind: Field) -> Vec<&'static str> {
    let mut names: Vec<_> = fields().iter().filter(|(_, k)| **k == kind).map(|(n, _)| n.as_str()).collect();names.sort_unstable();names
}

fn clock() -> Res<i64> {
    SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| String::from("telemetry clock before epoch"))?
        .as_millis().try_into().map_err(|_| "telemetry clock overflow".into())
}
fn code(s: &str) -> bool {
    !s.is_empty() && s.len() <= 128 && s.bytes().all(|c| c.is_ascii_alphanumeric() || b"._:-".contains(&c))
}
fn hex(text: &str, length: usize) -> bool {text.len()==length&&text.bytes().all(|v|v.is_ascii_digit()||(b'a'..=b'f').contains(&v))}
fn random_id() -> Res<String> {
    use std::fmt::Write as _;
    let mut id=[0;16];SystemRandom::new().fill(&mut id).map_err(|_|"telemetry random id unavailable")?;
    let mut text=String::with_capacity(32);for byte in id {write!(&mut text,"{byte:02x}").map_err(err)?;}Ok(text)
}
fn identity(namespace: &str,key: &str) -> Res<String> {if !code(namespace)||!code(key) {return Err("telemetry invalid import identity".into());}Ok(crate::sha256_hex(format!("{namespace}\0{key}").as_bytes())[..32].into())}
fn err(e: impl std::fmt::Display) -> String { format!("telemetry: {e}") }
fn store_trace() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(||std::env::var("COREQ_TELEMETRY_STORE_TRACE").is_ok_and(|v|v=="1"))
}
/// Writer election, owner open and checkpoint span: `coreq_from` names the trigger, `coreq_to` the outcome.
fn store_span(operation: &'static str) -> tracing::Span {
    if !capture::observing() {return tracing::Span::none();}
    tracing::info_span!("operation",coreq_operation=operation,coreq_from=tracing::field::Empty,coreq_to=tracing::field::Empty,remaining=tracing::field::Empty,coreq_error=tracing::field::Empty)
}

const FAILURE_CODES: &[&str]=&["budget-exhausted","budget-locked","budget-invalid","auth","invalid-frame","invalid-observation","id-conflict","sqlite-busy","store-full","store-readonly","store-io","store-failed","flush-deadline","queue-unavailable","transport-unavailable","collection-incomplete","unknown"];
fn failure_code(error: &str) -> &'static str {
    if let Some(code)=error.strip_prefix("telemetry IPC rejected: ") {return FAILURE_CODES.iter().copied().find(|c|*c==code).unwrap_or("unknown");}
    if error.starts_with("telemetry installation budget exhausted") {return "budget-exhausted";}
    if error=="telemetry installation budget lock unavailable" {return "budget-locked";}
    if error=="telemetry invalid installation budget"||error.starts_with("telemetry budget peer") {return "budget-invalid";}
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
        let valid = if field(k) == Some(Field::Text) {
            v.is_null() || v.as_str().is_some_and(|s| if k == "project" {
                s=="other" || (s.len()<=128&&s.contains('/')&&s.split('/').all(|part|code(part)&&!matches!(part,"."|"..")))
            } else { code(s) })
        } else if field(k) == Some(Field::Number) {
            v.is_null() || v.as_u64().is_some_and(|n| k!="started_ms"||n <= i64::MAX as u64)
        } else if field(k) == Some(Field::Flag) {
            v.is_boolean()
        } else if k == "counts" {
            v.as_object().is_some_and(|m|m.iter().all(|(name,count)|code(name)&&count.as_u64().is_some()))
        } else if summary::FIELDS.contains(&k.as_str()) {
            true
        } else if k == "unknown" {
            v.as_object().is_some_and(|m| m.iter().all(|(name, reason)| matches!(field(name),Some(Field::Number|Field::Text)) && reason.as_str().is_some_and(code)))
        } else { false };
        if !valid { return Err(format!("telemetry invalid field {k}")); }
    }
    if !matches!(value["outcome"].as_str(), Some("ok" | "failed" | "refused" | "cancelled" | "retried" | "unknown" | "ask")) {
        return Err("telemetry invalid outcome".into());
    }
    for (k,v) in object {
        if v.is_null() && (field(k)==Some(Field::Number)||matches!(k.as_str(),"version"|"trace"|"span")) && value["unknown"][k].as_str().is_none() {
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

/// Installation-wide file accounting. Every peer writer must hold `lock` while applying this budget.
#[derive(Clone)]
pub struct Budget {
    pub lock: PathBuf,
    pub peers: Vec<PathBuf>,
    pub total: u64,
    pub reserved: u64,
}
impl Budget {
    fn identity(&self)->Res<String> {
        if self.total==0||self.reserved>=self.total||self.peers.len()>16||!self.lock.is_absolute()
            ||self.peers.iter().any(|p|!p.is_absolute()) {return Err("telemetry invalid installation budget".into());}
        let physical=|path:&Path| {
            let parent=path.parent().unwrap_or(path);
            let parent=std::fs::canonicalize(parent).map_err(|_|"telemetry installation budget path unavailable")?;
            Ok::<_,String>(path.file_name().map_or(parent.clone(),|name|parent.join(name)))
        };
        let lock=physical(&self.lock)?;let peers=self.peers.iter().map(|path|physical(path)).collect::<Res<Vec<_>>>()?;
        Ok(crate::sha256_hex(&serde_json::to_vec(&(&lock,&peers,self.total,self.reserved)).map_err(err)?))
    }
    fn acquire(&self)->Res<crate::file_lock::Lock> {
        self.identity()?;
        let until=Instant::now()+Duration::from_secs(1);
        loop {match crate::file_lock::try_lock(&self.lock) {
            Ok(lock)=>return Ok(lock),
            Err(TryLockError::WouldBlock) if Instant::now()<until=>std::thread::sleep(Duration::from_millis(5)),
            Err(_)=>return Err("telemetry installation budget lock unavailable".into()),
        }}
    }
    fn occupied(&self)->Res<u64> {
        self.identity()?;
        let mut used=self.reserved;
        for path in &self.peers {
            let bytes=match std::fs::symlink_metadata(path) {
                Ok(meta) if meta.file_type().is_file()=>meta.len(),
                Ok(_)=>return Err("telemetry budget peer is not a regular file; preserved".into()),
                Err(e) if e.kind()==std::io::ErrorKind::NotFound=>0,
                Err(_)=>return Err("telemetry budget peer unreadable; preserved".into()),
            };
            used=used.checked_add(bytes).ok_or("telemetry installation budget exhausted")?;
        }
        Ok(used)
    }
    /// Working share `total/2`: `reserved` for peers, the rest database and WAL+SHM at 6:1, database at most 384 MiB.
    fn shares(&self)->Res<(u64,u64)> {
        self.identity()?;
        let working=(self.total/2).checked_sub(self.reserved).ok_or("telemetry invalid installation budget")?;
        let database=(working/7*6).min(BOUNDED_DATABASE_BYTES);
        if database<256*1024 {return Err("telemetry invalid installation budget".into());}
        Ok((database,database/6))
    }
    /// Fixed by the budget; peer growth never shrinks it.
    pub fn database_bytes(&self)->Res<i64> {Ok(self.shares()?.0 as i64)}
    /// WAL and SHM share before an append checkpoints; larger transactions use the reserve transiently.
    pub fn wal_bytes(&self)->Res<u64> {Ok(self.shares()?.1)}
    /// Over-share files are a degraded diagnostic, never a caller error; non-regular files stay refused.
    pub fn check_files(&self,path:&Path)->Res<i64> {
        let limit=self.database_bytes()?;
        for suffix in ["","-wal","-shm"] {
            let mut name=path.as_os_str().to_os_string();name.push(suffix);
            match std::fs::symlink_metadata(Path::new(&name)) {
                Ok(meta) if meta.file_type().is_file()=>{},
                Err(e) if e.kind()==std::io::ErrorKind::NotFound=>{},
                _=>return Err("telemetry budget file is not a regular file; preserved".into()),
            }
        }
        Ok(limit)
    }
    /// Shares over their limit; storage() and one stderr line report them.
    fn degraded(&self,database:u64,wal:u64)->Res<Vec<&'static str>> {
        let (database_limit,wal_limit)=self.shares()?;let peers=self.occupied()?-self.reserved;
        let mut reasons=Vec::new();
        if peers>self.reserved {reasons.push("peer-share");}
        if database>database_limit {reasons.push("database-over-cap");}
        if wal>wal_limit {reasons.push("wal-over-share");}
        if peers+database+wal>self.total/2 {reasons.push("reserve-used");}
        Ok(reasons)
    }

}

/// Reuse one store per process. SQLite WAL serializes separate processes, too.
pub struct Store { db: Mutex<Connection>, write_lock: PathBuf, path: PathBuf, budget: Option<Budget>, capacity_pruned: std::sync::atomic::AtomicBool,
    /// Bounded append checkpoints a pinned reader left unfinished, and when the next one is due.
    unfinished: std::sync::atomic::AtomicU64, retry: Mutex<Option<Instant>> }

thread_local! {
    /// Set by `prune_capacity` on this thread; a store reads it after its own commit under its writer lock.
    static CAPACITY_PRUNED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Hold existing writer election before opening an owner Store for quiesced maintenance.
pub fn maintenance(path:&Path)->Res<crate::file_lock::Lock> {broker::maintenance_lock(path)}

fn import_cursor(db:&Connection,source:&str)->Res<Option<String>> {
    if !code(source) {return Err("telemetry invalid import source".into());}
    let exists:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='imports')",[],|r|r.get(0)).map_err(err)?;
    if !exists {return Ok(None);}
    db.query_row("SELECT cursor FROM imports WHERE source=?1",[source],|r|r.get(0)).optional().map_err(err)
}

fn sidecar(path:&Path,suffix:&str)->PathBuf {let mut name=path.as_os_str().to_os_string();name.push(suffix);PathBuf::from(name)}
/// Missing sidecars are zero; inaccessible files never masquerade as empty.
fn file_bytes(path:&Path,optional:bool)->Res<u64> {match std::fs::metadata(path) {
    Ok(meta)=>Ok(meta.len()),Err(error) if optional&&error.kind()==std::io::ErrorKind::NotFound=>Ok(0),Err(error)=>Err(err(error)),
}}

/// Frames not yet checkpointed, from SQLite's WAL-index header (wal.c): two 48-byte copies with `mxFrame` at +16,
/// then `nBackfill` at 96, native byte order. Reading bytes 0..100 stays clear of the lock bytes at 120, which
/// Windows enforces. Unequal copies are a header in flux: reread, then unknown.
fn live_frames(shm:&Path)->Res<u64> {
    use std::io::Read;
    for _ in 0..8 {
        let mut header=[0u8;100];
        let read=match std::fs::File::open(shm) {
            Ok(file)=>file.take(100).read(&mut header).map_err(err)?,
            Err(error) if error.kind()==std::io::ErrorKind::NotFound=>return Ok(0),Err(error)=>return Err(err(error)),
        };
        if read==0 {return Ok(0);}
        if read==100&&header[..48]==header[48..96] {
            if header[12]==0 {return Ok(0);}
            let word=|at:usize|u32::from_ne_bytes(header[at..at+4].try_into().unwrap()) as u64;
            return Ok(word(16).saturating_sub(word(96)));
        }
        std::thread::yield_now();
    }
    Err("telemetry WAL index unknown".into())
}

/// A committed read snapshot. Never creates, upgrades, checkpoints or prunes the store.
pub struct Reader { db: Connection, path: PathBuf }
impl Reader {
    pub fn open(path: &Path) -> Res<Self> {
        let db=Connection::open_with_flags(path,rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(err)?;
        db.busy_timeout(Duration::from_secs(1)).map_err(err)?;
        db.execute_batch("PRAGMA query_only=ON; BEGIN;").map_err(err)?;
        let version:i64=db.pragma_query_value(None,"user_version",|r|r.get(0)).map_err(err)?;
        let application:i64=db.pragma_query_value(None,"application_id",|r|r.get(0)).map_err(err)?;
        if version!=6||application!=APPLICATION {return Err("telemetry read requires current owned schema; owner migration required".into());}
        // Cheap checks only: each page decodes and validates the rows it reads; the owner Store runs the full scan.
        validate_header(&db)?;
        Ok(Self{db,path:path.to_owned()})
    }
    pub fn import_cursor(&self,source:&str)->Res<Option<String>> {import_cursor(&self.db,source)}
    pub fn export(&self,since_ms:i64,after:i64,limit:usize) -> Res<Value> {
        export_page(&self.db,since_ms,after,limit)
    }
    pub fn export_kinds(&self,kinds:&[&str],since_ms:i64,after:i64,limit:usize)->Res<Value> {
        export_kinds_page(&self.db,kinds,since_ms,after,limit)
    }
    /// File sizes and live WAL bytes without the writer lock or a checkpoint (even NOOP is refused inside the snapshot).
    pub fn storage(&self)->Res<Value> {
        let (database,wal,shm)=(file_bytes(&self.path,false)?,file_bytes(&sidecar(&self.path,"-wal"),true)?,file_bytes(&sidecar(&self.path,"-shm"),true)?);
        let page=self.db.pragma_query_value(None,"page_size",|r|r.get::<_,i64>(0)).map_err(err)? as u64;
        Ok(json!({"database_bytes":database,"wal_bytes":wal,"wal_live_bytes":live_frames(&sidecar(&self.path,"-shm"))?*(page+24),"shm_bytes":shm,"total_bytes":database+wal+shm}))
    }
}

fn export_kinds_page(db:&Connection,kinds:&[&str],since_ms:i64,after:i64,limit:usize)->Res<Value> {
        if kinds.is_empty()||kinds.len()>16||kinds.iter().any(|kind|!code(kind))||kinds.iter().collect::<std::collections::HashSet<_>>().len()!=kinds.len() {return Err("telemetry invalid kind selection".into());}
        if !(1..=BATCH).contains(&limit) {return Err("telemetry limit must be 1..1024".into());}
        let predicate=kind_predicate(kinds.len());
        let mut values=kinds.iter().map(|kind|rusqlite::types::Value::Text((*kind).into())).collect::<Vec<_>>();values.extend([since_ms.into(),after.into()]);
        let mut page_values=values.clone();page_values.push((limit as i64).into());
        let candidates=read_rows(db,&kind_query(kinds.len()),rusqlite::params_from_iter(page_values.iter()),&mut Cache::default())?;
        let examined=candidates.len();let next=candidates.last().map_or(after,|row|row.seq);*values.last_mut().unwrap()=next.into();
        let more:bool=db.query_row(&format!("SELECT EXISTS(SELECT 1 FROM events WHERE {predicate})"),rusqlite::params_from_iter(values.iter()),|r|r.get(0)).map_err(err)?;
        let rows=candidates.into_iter().filter(|row|kinds.contains(&row.kind.as_str())).collect::<Vec<_>>();
        let mut page=exported(db,since_ms,after,&rows,next,more)?;page["kinds"]=json!(kinds);page["examined"]=json!(examined);
        page["unindexed_count"]=json!(db.query_row("SELECT count(*) FROM events WHERE kind IS NULL",[],|r|r.get::<_,i64>(0)).map_err(err)?);Ok(page)
}

impl Store {
    pub fn open(path: &Path) -> Res<Self> {Self::open_with_budget(path,None,true)}
    pub fn open_bounded(path:&Path,budget:Budget)->Res<Self> {Self::open_with_budget(path,Some(budget),true)}
    /// `full: false` is writer promotion: a current store gets only the cheap checks, as in Reader::open.
    fn open_with_budget(path:&Path,budget:Option<Budget>,full:bool)->Res<Self> {
        let span=store_span("telemetry.open");
        let store=span.in_scope(||Self::open_owner(path,budget,full,&span));
        if let Err(error)=&store {span.record("coreq_error",failure_code(error));}
        store
    }
    fn open_owner(path:&Path,budget:Option<Budget>,requested:bool,span:&tracing::Span)->Res<Self> {
        let _budget=budget.as_ref().map(Budget::acquire).transpose()?;
        let database_bytes=budget.as_ref().map(|budget|budget.check_files(path)).transpose()?.unwrap_or(DATABASE_BYTES);
        let diagnostic = store_trace().then(Instant::now);
        let elapsed = || diagnostic.map(|start| start.elapsed().as_micros()).unwrap_or(0);
        let mut name = path.as_os_str().to_os_string(); name.push(".init.lock");
        let until = Instant::now() + Duration::from_secs(1);
        let _initialization = loop { match crate::file_lock::try_lock(Path::new(&name)) {
            Ok(lock) => break lock,
            Err(TryLockError::WouldBlock) if Instant::now() < until => std::thread::sleep(Duration::from_millis(5)),
            Err(e) => {
                if diagnostic.is_some() { let _=writeln!(std::io::stderr(),"Telemetry open init-lock failed us={}", elapsed()); }
                return Err(err(e));
            },
        }};
        let initialized = elapsed();
        let mut lock_path=path.as_os_str().to_os_string();lock_path.push(".write.lock");
        let write_lock=PathBuf::from(lock_path);
        let _write=crate::file_lock::lock(&write_lock).map_err(err)?;
        let locked = elapsed();
        let mut db = Connection::open(path).map_err(err)?;
        // Last-close checkpoint runs outside our mutation guard; bounded stores checkpoint explicitly.
        if budget.is_some() {db.set_db_config(rusqlite::config::DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,true).map_err(err)?;}
        let connected = elapsed();
        db.busy_timeout(Duration::from_secs(1)).map_err(err)?;
        let version: i64=db.pragma_query_value(None,"user_version",|r|r.get(0)).map_err(err)?;
        let application:i64=db.pragma_query_value(None,"application_id",|r|r.get(0)).map_err(err)?;
        let empty=version==0&&application==0&&!db.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master)",[],|r|r.get::<_,bool>(0)).map_err(err)?;
        if !empty&&(!matches!(version,1|2|3|4|5|6)||application!=APPLICATION) {return Err(format!("telemetry unsupported schema {version} or foreign application; store preserved"));}
        // A seal holds only for the unchanged state it recorded; an open always consumes it.
        let mut sealed=path.as_os_str().to_os_string();sealed.push(".sealed");
        let clean=version==6&&matches!((std::fs::read_to_string(&sealed),sealed_state(&db,path)),(Ok(seal),Ok(state)) if seal==state);
        let _=std::fs::remove_file(&sealed);
        let full=version!=6||(requested&&!clean);
        if full {
            let check:String=db.query_row("PRAGMA quick_check",[],|r|r.get(0)).map_err(err)?;
            if check!="ok" {return Err("telemetry corrupt store preserved".into());}
        }
        let page_size: i64=db.pragma_query_value(None,"page_size",|r|r.get(0)).map_err(err)?;
        let pages: i64=db.pragma_query_value(None,"page_count",|r|r.get(0)).map_err(err)?;
        if page_size<1024||(budget.is_none()&&pages>database_bytes/page_size) {return Err("telemetry oversized or unsupported-page store preserved; migration required".into());}
        // A bounded store over its cap opens, prunes to 60% and stays degraded until reclaim shrinks the file.
        let oversized=pages>database_bytes/page_size;
        let checked = elapsed();
        db.execute_batch("PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA temp_store=MEMORY; PRAGMA cache_spill=OFF; PRAGMA cache_size=-32768; PRAGMA wal_autocheckpoint=0;").map_err(err)?;
        db.pragma_update(None,"max_page_count",(database_bytes/page_size).max(pages)).map_err(err)?;
        let configured = elapsed();
        checkpoint(&db,true,"open")?;
        let checkpointed = elapsed();
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
        span.record("coreq_from",if empty {"first-open"} else if version!=6||raw {"schema"} else if !requested {"promotion"} else if clean {"orderly-exit"} else {"unclean-exit"});
        span.record("coreq_to",if full||raw {"scan"} else {"skip"});
        if matches!(version,1|2|3|4|5)||raw {upgrade(&mut db,version)?;checkpoint(&db,true,"upgrade")?;} else if full {validate_store(&db)?;} else {validate_header(&db)?;}
        if oversized {
            let tx=db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(err)?;
            prune_capacity(&tx,database_bytes/page_size*3/5)?;tx.commit().map_err(err)?;checkpoint(&db,true,"capacity")?;
        }
        let completed = elapsed();
        if completed >= 100_000 {
            let _=writeln!(std::io::stderr(),"Telemetry open us: init={initialized} write={} connect={} check={} configure={} checkpoint={} schema={}", locked-initialized, connected-locked, checked-connected, configured-checked, checkpointed-configured, completed-checkpointed);
        }
        Ok(Self { db: Mutex::new(db), write_lock, path: path.to_owned(), budget, capacity_pruned: std::sync::atomic::AtomicBool::new(false),
            unfinished: std::sync::atomic::AtomicU64::new(0), retry: Mutex::new(None) })
    }
    /// Reaching a cap is a degraded diagnostic for this store's lifetime; one stderr line, never a caller error.
    // ponytail: per-process flag; persist it in metadata if another process must see it.
    fn note_capacity(&self,committed:bool) {
        if CAPACITY_PRUNED.take()&&committed&&self.budget.is_some()&&!self.capacity_pruned.swap(true,std::sync::atomic::Ordering::Relaxed) {
            let _=writeln!(std::io::stderr(),"Telemetry: degraded (capacity-pruned)");
        }
    }
    /// Orderly close: checkpoint and record this exact state in `<database>.sealed`, so the next owner open
    /// skips the full scan while the store stays unchanged. Failure leaves no seal: the next open scans.
    pub(crate) fn seal(&self) {
        let Ok(db)=self.db.lock() else {return};
        let Ok(_budget)=self.budget.as_ref().map(Budget::acquire).transpose() else {return};
        let Ok(_write)=crate::file_lock::lock(&self.write_lock) else {return};
        let Ok(state)=checkpoint(&db,true,"seal").and_then(|_|sealed_state(&db,&self.path)) else {return};
        let (sealed,tmp)=(self.sidecar(".sealed"),self.sidecar(".sealed.tmp"));
        if std::fs::write(&tmp,state).and_then(|_|std::fs::rename(&tmp,&sealed)).is_err() {let _=std::fs::remove_file(&tmp);}
    }
    fn sidecar(&self,suffix:&str)->PathBuf {sidecar(&self.path,suffix)}
    fn apply_budget(&self,db:&Connection)->Res<()> {
        if let Some(budget)=&self.budget {
            let bytes=budget.check_files(&self.path)?;
            let page:i64=db.pragma_query_value(None,"page_size",|r|r.get(0)).map_err(err)?;
            let pages:i64=db.pragma_query_value(None,"page_count",|r|r.get(0)).map_err(err)?;
            let cap:i64=db.pragma_query_value(None,"max_page_count",|r|r.get(0)).map_err(err)?;
            // SQLite cannot cap below the current file; an oversized store reuses its free pages.
            let target=(bytes/page).max(pages);
            if cap!=target {
                checkpoint(db,true,"budget")?;
                db.pragma_update(None,"max_page_count",target).map_err(err)?;
            }
            let length=|path:PathBuf|std::fs::metadata(path).map_or(0,|m|m.len());
            let reasons=budget.degraded(length(self.path.clone()),length(self.sidecar("-wal"))+length(self.sidecar("-shm")))?;
            static REPORTED:std::sync::atomic::AtomicBool=std::sync::atomic::AtomicBool::new(false);
            if !reasons.is_empty()&&!REPORTED.swap(true,std::sync::atomic::Ordering::Relaxed) {
                let _=writeln!(std::io::stderr(),"Telemetry: degraded ({})",reasons.join(","));
            }
        }
        Ok(())
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
        let _budget=self.budget.as_ref().map(Budget::acquire).transpose()?;
        let _write=crate::file_lock::lock(&self.write_lock).map_err(err)?;
        self.apply_budget(&db)?;
        let locked=began.elapsed().as_micros();
        let wal=self.budget.as_ref().map(|b|b.wal_bytes().map(|cap|(cap,std::fs::metadata(self.sidecar("-wal")).map_or(0,|m|m.len())))).transpose()?;
        self.append_checkpoint(&db,wal,events)?;
        let checkpointed=began.elapsed().as_micros();
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(|e|format!("telemetry append.begin: {e}"))?;
        let opened=began.elapsed().as_micros();
        CAPACITY_PRUNED.set(false);
        let ids=(|| {
            if events.len()>BATCH {incoming(&tx,events)?;}
            let now=clock()?;let mut ids=Vec::with_capacity(events.len());
            if events.is_empty() {append_to(&tx,events,now,None)?;}
            for chunk in events.chunks(BATCH) {ids.extend(append_to(&tx,chunk,now,None)?);}
            Ok::<_,String>(ids)
        })();
        let inserted=began.elapsed().as_micros();
        let committed=ids.and_then(|ids|tx.commit().map(|_|ids).map_err(|e|format!("telemetry append.commit: {e}")));
        self.note_capacity(committed.is_ok());
        let ids=committed?;
        // Committed already: a busy truncate leaves the WAL degraded for the next append, never fails or delays this one.
        if let Some((share,_))=wal {
            let length=|suffix|std::fs::metadata(self.sidecar(suffix)).map_or(0,|m|m.len());
            let mut retry=self.retry.lock().map_err(|_|"telemetry writer poisoned")?;
            if retry.is_none_or(|at|Instant::now()>=at)&&length("-wal")+length("-shm")>share {
                match checkpoint_run(&db,true,false,"wal-share") {Ok(true)=>*retry=None,Ok(false)=>self.unfinished_checkpoint(&mut retry),Err(_)=>{}}
            }
        }
        let committed=began.elapsed().as_micros();
        if store_trace()&&committed>=100_000 {
            let _=writeln!(std::io::stderr(),"Telemetry store us: events={} lock={locked} checkpoint={} begin={} rows={} commit={}",events.len(),checkpointed-locked,opened-checkpointed,inserted-opened,committed-inserted);
        }
        Ok(ids)
    }
    /// Unbounded: room for a full-database transaction, else a checkpoint that waits for readers up to busy_timeout.
    /// Bounded: room for this transaction inside the WAL share, else a checkpoint that never waits. A pinned
    /// reader leaves it unfinished: the append commits into the reserve while the installation total holds,
    /// and the checkpoint is retried after CHECKPOINT_RETRY; a larger capacity prune also spills into the reserve.
    fn append_checkpoint(&self, db: &Connection, wal: Option<(u64,u64)>, events: &[Event]) -> Res<()> {
        let (busy,frames): (i64,i64)=db.query_row("PRAGMA wal_checkpoint(NOOP)",[],|r|Ok((r.get(0)?,r.get(1)?))).map_err(err)?;
        let page:i64=db.pragma_query_value(None,"page_size",|r|r.get(0)).map_err(err)?;
        let pages:i64=db.pragma_query_value(None,"max_page_count",|r|r.get(0)).map_err(err)?;
        let database=(pages*page) as u64;
        let group=database.min((COMMIT_BYTES+512*COMMIT_EVENTS) as u64+32*page as u64);
        // taskq#1533: the worst-case group exceeds a small WAL share, so sizing room by it checkpointed every append.
        let transaction=database.min(events.iter().map(|e|e.payload.len() as u64+512).sum::<u64>()+32*page as u64);
        let room=match wal {
            None=>wal_has_room(frames,page,database,database+SHM_BYTES),
            Some((share,_)) if share<=SHM_BYTES=>false,
            Some((share,length))=>length<=share&&wal_has_room(frames,page,transaction,share-SHM_BYTES)
                &&(frames==0||wal_has_room(frames,page,transaction,LIVE_WAL_BYTES.min(share-SHM_BYTES))),
        };
        if busy==0&&room {return Ok(());}
        let (Some((share,length)),Some(budget))=(wal,&self.budget) else {return checkpoint(db,false,"wal-room")};
        let mut retry=self.retry.lock().map_err(|_|"telemetry writer poisoned")?;
        if retry.is_none_or(|at|Instant::now()>=at) {
            if checkpoint_run(db,length>LIVE_WAL_BYTES.min(share),false,"wal-room")? {*retry=None;return Ok(());}
            self.unfinished_checkpoint(&mut retry);
        }
        let length=|suffix:&str|std::fs::metadata(self.sidecar(suffix)).map_or(0,|m|m.len());
        let used=budget.occupied()?.checked_add(length("")+length("-wal")+length("-shm")+wal_growth(page as u64,group));
        if used.is_some_and(|used|used<=budget.total) {Ok(())} else {Err(CHECKPOINT_BUSY.into())}
    }
    fn unfinished_checkpoint(&self, retry: &mut Option<Instant>) {
        self.unfinished.fetch_add(1,std::sync::atomic::Ordering::Relaxed);
        *retry=Some(Instant::now()+CHECKPOINT_RETRY);
    }
    pub fn import_cursor(&self, source: &str) -> Res<Option<String>> {
        let db=self.db.lock().map_err(|_|"telemetry reader poisoned")?;
        import_cursor(&db,source)
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
        let _budget=self.budget.as_ref().map(Budget::acquire).transpose()?;
        let _write=crate::file_lock::lock(&self.write_lock).map_err(err)?;
        self.apply_budget(&db)?;
        checkpoint(&db,false,"import")?;
        let tx=db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(|e|format!("telemetry import.begin: {e}"))?;
        CAPACITY_PRUNED.set(false);
        let result=Self::import_in(tx,source,expected,next,events,received_ms,&receipt);
        self.note_capacity(result.is_ok());
        result
    }
    fn import_in(tx:rusqlite::Transaction<'_>, source: &str, expected: Option<&str>, next: &str, events: &[Event], received_ms: Option<&[i64]>, receipt:&str) -> Res<()> {
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
        let mut connection=self.db.lock().map_err(|_|"telemetry reader poisoned")?;
        let db=connection.transaction().map_err(err)?;
        export_kinds_page(&db,kinds,since_ms,after,limit)
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
        let _budget=self.budget.as_ref().map(Budget::acquire).transpose()?;
        let _write=crate::file_lock::lock(&self.write_lock).map_err(err)?;
        self.apply_budget(&db)?;
        checkpoint(&db,false,"acknowledge")?;
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
        let _budget=self.budget.as_ref().map(Budget::acquire).transpose()?;
        let database=file_bytes(&self.path,false)?;let wal=file_bytes(&self.sidecar("-wal"),true)?;let shm=file_bytes(&self.sidecar("-shm"),true)?;
        let (retained,pruned): (i64,i64)=db.query_row("SELECT retained_bytes,pruned_count FROM metadata WHERE id=1",[],|r|Ok((r.get(0)?,r.get(1)?))).map_err(err)?;
        let (blocks,compressed): (i64,i64)=db.query_row("SELECT count(*),coalesce(sum(length(payload)),0) FROM blocks",[],|r|Ok((r.get(0)?,r.get(1)?))).map_err(err)?;
        let raw:i64=db.query_row("SELECT coalesce(sum(length(cast(payload AS BLOB))),0) FROM events WHERE block IS NULL",[],|r|r.get(0)).map_err(err)?;
        let free:i64=db.pragma_query_value(None,"freelist_count",|r|r.get(0)).map_err(err)?;let page:i64=db.pragma_query_value(None,"page_size",|r|r.get(0)).map_err(err)?;
        let pages:i64=db.pragma_query_value(None,"max_page_count",|r|r.get(0)).map_err(err)?;
        let (frames,done):(i64,i64)=db.query_row("PRAGMA wal_checkpoint(NOOP)",[],|r|Ok((r.get(1)?,r.get(2)?))).map_err(err)?;
        let database_limit=self.budget.as_ref().map(Budget::database_bytes).transpose()?.unwrap_or(pages*page);
        let wal_limit=self.budget.as_ref().map(Budget::wal_bytes).transpose()?.unwrap_or(database_limit as u64+SHM_BYTES);
        let accounted=self.budget.as_ref().map(|b|b.occupied().and_then(|used|used.checked_add(database+wal+shm).ok_or("telemetry budget size overflow".into()))).transpose()?;
        let mut degraded=self.budget.as_ref().map(|b|b.degraded(database,wal+shm)).transpose()?.unwrap_or_default();
        if self.capacity_pruned.load(std::sync::atomic::Ordering::Relaxed) {degraded.push("capacity-pruned");}
        Ok(json!({"codec":"zlib-block/1","block_count":blocks,"compressed_payload_bytes":compressed,"legacy_payload_bytes":raw,"reusable_database_bytes":free*page,
            "state":if self.budget.is_some() {if degraded.is_empty() {"bounded"} else {"degraded"}} else if database<=database_limit as u64&&wal<=wal_limit&&shm<=SHM_BYTES&&accounted.zip(self.budget.as_ref()).is_none_or(|(used,b)|used<=b.total) {"bounded"} else {"unknown"},
            "degraded":degraded,
            "database_bytes":database,"wal_bytes":wal,"wal_live_bytes":(frames-done).max(0)*(page+24),"shm_bytes":shm,"total_bytes":database+wal+shm,
            "database_limit":database_limit,"wal_limit":wal_limit,"shm_limit":SHM_BYTES,
            "installation_accounted_bytes":accounted,"installation_limit":self.budget.as_ref().map(|b|b.total),
            "retained_payload_bytes":retained,"pruned_count":pruned,"unfinished_checkpoints":self.unfinished.load(std::sync::atomic::Ordering::Relaxed)}))
    }
    /// Explicit quiesced maintenance. SQLite's transient vacuum database stays in memory.
    pub fn reclaim(&self) -> Res<()> {
        let _election=broker::maintenance_lock(&self.path)?;
        let db=self.db.lock().map_err(|_|"telemetry writer poisoned")?;
        let _budget=self.budget.as_ref().map(Budget::acquire).transpose()?;
        let _write=crate::file_lock::lock(&self.write_lock).map_err(err)?;
        self.apply_budget(&db)?;
        validate_store(&db)?;checkpoint(&db,true,"reclaim")?;
        let free:i64=db.pragma_query_value(None,"freelist_count",|r|r.get(0)).map_err(err)?;
        if free!=0 {
            db.execute_batch("PRAGMA temp_store=MEMORY; PRAGMA synchronous=FULL; PRAGMA cache_spill=OFF; VACUUM;").map_err(|e|format!("telemetry reclaim: {e}"))?;
            validate_store(&db)?;checkpoint(&db,true,"reclaim")?;
        }
        Ok(())
    }
    pub fn prune(&self) -> Res<()> {
        let mut db = self.db.lock().map_err(|_| "telemetry writer poisoned")?;
        let _budget=self.budget.as_ref().map(Budget::acquire).transpose()?;
        let _write=crate::file_lock::lock(&self.write_lock).map_err(err)?;
        self.apply_budget(&db)?;
        checkpoint(&db,false,"prune")?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(err)?;
        prune(&tx,clock()?,AGE_MS,PAYLOAD_BYTES)?;
        tx.commit().map_err(err)?;
        checkpoint(&db,true,"prune")?;
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

fn wal_has_room(frames: i64, page: i64, transaction_bytes:u64, cap:u64) -> bool {
    if frames<0 || !(1024..=65536).contains(&page) || !(page as u64).is_power_of_two() { return false; }
    (frames as u64).checked_mul(page as u64+24).and_then(|n|n.checked_add(32+wal_growth(page as u64,transaction_bytes))).is_some_and(|n|n<=cap)
}
/// Spill is disabled: at most one frame per written page, plus FULL's sector padding.
fn wal_growth(page: u64, transaction_bytes: u64) -> u64 {
    let frame=page+24;
    (transaction_bytes/page+65536u64.div_ceil(frame))*frame
}
const CHECKPOINT_BUSY: &str = "telemetry checkpoint busy; retained data preserved";
fn checkpoint(db: &Connection, truncate: bool, trigger: &'static str) -> Res<()> {
    if checkpoint_run(db,truncate,true,trigger)? {Ok(())} else {Err(CHECKPOINT_BUSY.into())}
}
/// True when every WAL frame is checkpointed and the WAL restarted. `wait: false` never calls the busy
/// handler, so a pinned reader leaves the checkpoint unfinished instead of delaying the writer.
/// The span's duration is the wait.
fn checkpoint_run(db: &Connection, truncate: bool, wait: bool, trigger: &'static str) -> Res<bool> {
    let span=store_span("telemetry.checkpoint");span.record("coreq_from",trigger);
    let _entered=span.enter();
    let sql=if truncate {"PRAGMA wal_checkpoint(TRUNCATE)"} else {"PRAGMA wal_checkpoint(RESTART)"};
    if !wait {db.busy_timeout(Duration::ZERO).map_err(err)?;}
    let row=db.query_row(sql,[],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,i64>(1)?,r.get::<_,i64>(2)?))).map_err(err);
    if !wait {db.busy_timeout(Duration::from_secs(1)).map_err(err)?;}
    let (busy,frames,done)=row.inspect_err(|error|{span.record("coreq_error",failure_code(error));})?;
    let complete=busy==0&&frames==done;
    span.record("coreq_to",if complete {"complete"} else {"busy"});span.record("remaining",(frames-done).max(0));
    Ok(complete)
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
    reserve_space(db,bytes,count,extra_pages).map(|_|())
}
fn reserve_space(db: &Connection, bytes:i64, count:i64, extra_pages:i64) -> Res<bool> {
    if count==0&&extra_pages==0 {return Ok(false);}
    let page_size: i64=db.pragma_query_value(None,"page_size",|r|r.get(0)).map_err(err)?;
    let max_pages:i64=db.pragma_query_value(None,"max_page_count",|r|r.get(0)).map_err(err)?;
    let required=(bytes+page_size-1)/page_size+(512*count+page_size-1)/page_size+32+extra_pages;
    // Hysteresis: at 80% of the cap prune oldest rows down to 60%, so pruning stays rare.
    if used_pages(db)?+required<max_pages*4/5 {return Ok(false);}
    let pruned=prune_capacity(db,(max_pages*3/5-required).max(0))?;
    if pruned {CAPACITY_PRUNED.set(true);}
    if used_pages(db)?+required>=max_pages {return Err("telemetry physical capacity exhausted".into());}
    Ok(pruned)
}
fn used_pages(db: &Connection) -> Res<i64> {
    let pages: i64=db.pragma_query_value(None,"page_count",|r|r.get(0)).map_err(err)?;
    let free: i64=db.pragma_query_value(None,"freelist_count",|r|r.get(0)).map_err(err)?;
    Ok(pages-free)
}
/// Remove oldest whole observations until `target` pages are used; aggregates go last.
fn prune_capacity(db: &Connection, target: i64) -> Res<bool> {
    let mut pruned=false;
    for filter in [AGGREGATES,"1"] {
        while used_pages(db)?>target {
            let rows=read_rows(db,&format!("SELECT {SELECT_ROW} FROM events WHERE {filter} ORDER BY seq LIMIT 256"),[],&mut Cache::default())?;
            if remove_rows(db,rows)?==0 {break;}
            pruned=true;
        }
    }
    Ok(pruned)
}
fn append_to(db: &Connection, events: &[Event], now: i64, received_ms: Option<&[i64]>) -> Res<Vec<i64>> {
    let began=Instant::now();
    let (mut sequences,pending)=loop {
    let mut sequences=std::collections::HashMap::new();let mut pending=Vec::new();let mut seen=std::collections::HashMap::new();let mut cache=Cache::default();
    let mut read=db.prepare_cached(&format!("SELECT {SELECT_ROW} FROM events WHERE id=?1")).map_err(err)?;
    for (index,event) in events.iter().enumerate() {
        if let Some(previous)=seen.insert(event.id.as_str(),event.payload.as_str()) {
            if previous!=event.payload {return Err("telemetry immutable id payload mismatch".into());}
            continue;
        }
        if let Some(stored)=read.query_row([&event.id],row).optional().map_err(err)? {
            let stored=stored.decode(db,&mut cache)?;
            if stored.payload!=event.payload {return Err("telemetry immutable id payload mismatch".into());}
            sequences.insert(event.id.as_str(),stored.seq);
        } else {pending.push((event,received_ms.map_or(now,|times|times[index])));}
    }
    drop(read);
    let bytes=pending.iter().map(|(event,_)|event.payload.len() as i64).sum();
    // Capacity pruning can remove an existing ID from this chunk; resolve again before insertion.
    if !reserve_space(db,bytes,pending.len() as i64,0)? {break (sequences,pending);}
    };
    let reserved=began.elapsed().as_micros();
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
        let _=writeln!(std::io::stderr(),"Telemetry rows us: events={} reserve={reserved} insert={} prune={}",events.len(),inserted-reserved,pruned-inserted);
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
/// Metadata and sequence bounds; reads O(1) rows.
fn validate_header(db: &Connection) -> Res<()> {
    let (retained,pruned,floor,last): (i64,i64,i64,i64)=db.query_row("SELECT retained_bytes,pruned_count,pruned_through,(SELECT coalesce(max(seq),0) FROM events) FROM metadata WHERE id=1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).map_err(err)?;
    if retained<0||pruned<0||floor<0 {return Err("telemetry corrupt retention metadata".into());}
    let high: Option<i64>=db.query_row("SELECT seq FROM sqlite_sequence WHERE name='events'",[],|r|r.get(0)).optional().map_err(err)?;
    if high.unwrap_or(0)<last.max(floor) {return Err("telemetry corrupt sequence high-water mark".into());}Ok(())
}
/// What a seal binds: an empty WAL, the database file length and modification time, and the sequence and retention metadata.
fn sealed_state(db: &Connection, path: &Path) -> Res<String> {
    let mut wal=path.as_os_str().to_os_string();wal.push("-wal");
    if std::fs::metadata(&wal).map_or_else(|e|e.kind()!=std::io::ErrorKind::NotFound,|wal|wal.len()>0) {return Err("telemetry WAL not empty".into());}
    let file=std::fs::metadata(path).map_err(err)?;
    let modified=file.modified().map_err(err)?.duration_since(std::time::UNIX_EPOCH).map_err(err)?.as_nanos();
    let (high,retained,pruned,floor): (i64,i64,i64,i64)=db.query_row("SELECT coalesce((SELECT seq FROM sqlite_sequence WHERE name='events'),0),retained_bytes,pruned_count,pruned_through FROM metadata WHERE id=1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).map_err(err)?;
    Ok(format!("{} {modified} {high} {retained} {pruned} {floor}",file.len()))
}
fn validate_store(db: &Connection) -> Res<()> {
    validate_header(db)?;
    let (retained,sum): (i64,i64)=db.query_row("SELECT retained_bytes,(SELECT coalesce(sum(bytes),0) FROM events) FROM metadata WHERE id=1",[],|r|Ok((r.get(0)?,r.get(1)?))).map_err(err)?;
    if retained!=sum {return Err("telemetry corrupt retention metadata".into());}
    let orphan:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM blocks WHERE NOT EXISTS(SELECT 1 FROM events WHERE block=blocks.id))",[],|r|r.get(0)).map_err(err)?;
    if orphan {return Err("telemetry unreferenced compressed block".into());}
    let mut after=0;let mut cache=Cache::default();
    loop {let rows=read_rows(db,&format!("SELECT {SELECT_ROW} FROM events WHERE seq>?1 ORDER BY seq LIMIT ?2"),params![after,BATCH as i64],&mut cache)?;
        if rows.is_empty() {break;}after=rows.last().unwrap().seq;}
    Ok(())
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
const EXPIRED: &str="SELECT EXISTS(SELECT 1 FROM events WHERE received_ms<?1)";
fn prune(db: &Connection, now: i64, age: i64, bytes: i64) -> Res<()> {
    let mut cache=Cache::default();
    let cutoff=now.saturating_sub(age);
    // Ordering by seq scans retained history even when the received-time index has no expired row.
    while db.query_row(EXPIRED,[cutoff],|r|r.get::<_,bool>(0)).map_err(err)? {
        let rows=read_rows(db,&format!("SELECT {SELECT_ROW} FROM events WHERE received_ms<?1 ORDER BY seq LIMIT 256"),[cutoff],&mut cache)?;
        remove_rows(db,rows)?;
    }
    while db.query_row("SELECT retained_bytes FROM metadata WHERE id=1",[],|r|r.get::<_,i64>(0)).map_err(err)?>bytes {
        let rows=read_rows(db,&format!("SELECT {SELECT_ROW} FROM events ORDER BY seq LIMIT 1"),[],&mut cache)?;
        if remove_rows(db,rows)?==0 {return Err("telemetry corrupt retention metadata".into());}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_empty_check_uses_received_index_and_preserves_cutoff() {
        use rusqlite::StatementStatus::{FullscanStep,VmStep};
        let dir=crate::scratch("telemetry-retention-index");let store=Store::open(&dir.join("db")).unwrap();
        for _ in 0..12 {store.append(&(0..1024).map(|_|event("retained")).collect::<Vec<_>>()).unwrap();}
        let db=store.db.lock().unwrap();
        db.execute("UPDATE events SET received_ms=100",[]).unwrap();
        let mut old=db.prepare(&format!("SELECT {SELECT_ROW} FROM events WHERE received_ms<?1 ORDER BY seq LIMIT 256")).unwrap();
        assert!(old.query([100]).unwrap().next().unwrap().is_none());
        let scans=old.get_status(FullscanStep);assert!(scans>=12287,"{scans}");drop(old);
        let mut check=db.prepare(EXPIRED).unwrap();assert!(!check.query_row([100],|r|r.get::<_,bool>(0)).unwrap());
        assert_eq!(check.get_status(FullscanStep),0);assert!(check.get_status(VmStep)<50);drop(check);
        prune(&db,200,100,PAYLOAD_BYTES).unwrap();
        assert_eq!(db.query_row("SELECT count(*) FROM events",[],|r|r.get::<_,i64>(0)).unwrap(),12288);
        db.execute("UPDATE events SET received_ms=99 WHERE seq%2=0 AND seq<=1200",[]).unwrap();
        let retained=||db.prepare("SELECT id,received_ms FROM events WHERE received_ms=100 ORDER BY seq").unwrap().query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?))).unwrap().collect::<Result<Vec<_>,_>>().unwrap();
        let before=retained();
        prune(&db,200,100,PAYLOAD_BYTES).unwrap();
        assert_eq!(retained(),before);
        assert_eq!(db.query_row("SELECT count(*) FROM events WHERE received_ms=100",[],|r|r.get::<_,i64>(0)).unwrap(),11688);
        assert_eq!(db.query_row("SELECT count(*) FROM events WHERE received_ms<100",[],|r|r.get::<_,i64>(0)).unwrap(),0);
        assert_eq!(db.query_row("SELECT pruned_count FROM metadata",[],|r|r.get::<_,i64>(0)).unwrap(),600);
        assert_eq!(db.query_row("SELECT pruned_through FROM metadata",[],|r|r.get::<_,i64>(0)).unwrap(),1200);
        eprintln!("retention indexed empty check: {scans} prior fullscan steps -> 0; strict cutoff and 600 mixed-clock removals preserved");
        drop(db);drop(store);fs::remove_dir_all(dir).unwrap();
    }

    // Chain: protected analysis -> fixed committed pages while the owner keeps appending.
    #[test]
    fn bounded_close_preserves_wal_without_unlocked_checkpoint_growth() {
        let dir=crate::scratch("telemetry-budget-close");let path=dir.join("db");
        let budget=Budget {lock:dir.join("installation.lock"),peers:vec![],total:8<<20,reserved:1<<20};
        let store=Store::open_bounded(&path,budget.clone()).unwrap();store.append(&[large_event("close")]).unwrap();
        let sizes=||["db","db-wal"].map(|p|std::fs::metadata(dir.join(p)).map_or(0,|m|m.len()));
        let before=sizes();assert!(before[1]>0);
        let guard=budget.acquire().unwrap();drop(store);assert_eq!(sizes(),before);drop(guard);
        let reader=Reader::open(&path).unwrap();assert_eq!(reader.export(0,0,1024).unwrap()["events"].as_array().unwrap().len(),1);drop(reader);
        let store=Store::open_bounded(&path,budget).unwrap();assert_eq!(store.export(0,0,1024).unwrap()["events"].as_array().unwrap().len(),1);
        drop(store);std::fs::remove_dir_all(dir).unwrap();
    }

    // Owner decision 2026-10-19 (A9): budget pressure degrades and prunes; callers never see budget-exhausted.
    #[test]
    fn shared_budget_rechecks_peers_and_degrades_instead_of_refusing() {
        let dir=crate::scratch("telemetry-shared-budget");let peer=dir.join("archive");
        let budget=Budget {lock:dir.join("installation.lock"),peers:vec![peer.clone()],total:8<<20,reserved:1<<20};
        let store=Store::open_bounded(&dir.join("db"),budget.clone()).unwrap();
        store.append(&[event("before.pressure")]).unwrap();assert_eq!(store.storage().unwrap()["state"],"bounded");
        {let _guard=budget.acquire().unwrap();std::fs::File::create(&peer).unwrap().set_len(7<<20).unwrap();}
        store.append(&[event("under.pressure")]).unwrap();
        let storage=store.storage().unwrap();assert_eq!(storage["state"],"degraded");assert_eq!(storage["degraded"],json!(["peer-share","reserve-used"]));
        assert_eq!(store.export(0,0,1024).unwrap()["events"].as_array().unwrap().len(),2,"pressure dropped history");
        {let _guard=budget.acquire().unwrap();std::fs::remove_file(&peer).unwrap();}
        store.append(&[event("after.pressure")]).unwrap();assert_eq!(store.storage().unwrap()["state"],"bounded");
        let _guard=budget.acquire().unwrap();let began=Instant::now();
        assert!(store.append(&[event("locked")]).unwrap_err().contains("budget lock"));
        assert!(began.elapsed()<Duration::from_secs(2));drop(_guard);
        std::fs::create_dir(&peer).unwrap();assert!(store.append(&[event("non.file")]).is_err());
        std::fs::remove_dir(&peer).unwrap();
        assert!(Store::open_bounded(&dir.join("other"),Budget {total:2<<20,..budget}).err().unwrap().contains("invalid installation budget"));
        drop(store);std::fs::remove_dir_all(dir).unwrap();
    }

    // A8: pruning starts at 80% of the cap, stops at 60% and removes poll aggregates last.
    #[test]
    fn capacity_pruning_has_hysteresis_and_keeps_aggregates() {
        let dir=crate::scratch("telemetry-hysteresis");
        let budget=Budget {lock:dir.join("installation.lock"),peers:vec![],total:8<<20,reserved:1<<20};
        let store=Store::open_bounded(&dir.join("db"),budget.clone()).unwrap();
        let mut aggregate:Value=serde_json::from_str(&event("poll").payload).unwrap();aggregate["kind"]=json!("poll");aggregate["calls"]=json!(42);
        let aggregate=Event::new(aggregate).unwrap();store.append(&[aggregate.clone()]).unwrap();
        let cap=budget.database_bytes().unwrap();let used=|store:&Store|{let db=store.db.lock().unwrap();used_pages(&db).unwrap()*db.pragma_query_value(None,"page_size",|r|r.get::<_,i64>(0)).unwrap()};
        let mut peak=0;let mut batch=0;
        while store.storage().unwrap()["pruned_count"]==0 {
            peak=peak.max(used(&store));
            store.append(&(0..16).map(|n|large_event(&format!("fill.{batch}.{n}"))).collect::<Vec<_>>()).unwrap();batch+=1;
        }
        assert!(peak>=cap*3/5,"pruned before reaching the high mark: {peak} of {cap}");
        assert!(used(&store)<=cap*3/5+512*1024,"pruning stopped above the low mark: {} of {cap}",used(&store));
        let rows=store.export(0,0,1024).unwrap();assert_eq!(rows["events"][0]["id"],aggregate.id(),"aggregate pruned before older detail");
        // Defect: capacity pruning reported `bounded`; reaching the cap must be visible (A9).
        let storage=store.storage().unwrap();assert_eq!(storage["state"],"degraded");assert_eq!(storage["degraded"],json!(["capacity-pruned"]));
        drop(store);std::fs::remove_dir_all(dir).unwrap();
    }

    // A9: a store and peers over the old 64 MiB budget open, prune and recover without manual deletion.
    #[test]
    fn oversized_bounded_store_opens_prunes_and_recovers() {
        let _disk=crate::bulk_disk();
        let dir=crate::scratch("telemetry-oversized-bounded");let path=dir.join("db");let peer=dir.join("legacy.log");
        let budget=|total:u64|Budget {lock:dir.join("installation.lock"),peers:vec![peer.clone()],total,reserved:1<<20};
        let store=Store::open_bounded(&path,budget(64<<20)).unwrap();
        let mut batch=0;while std::fs::metadata(&path).unwrap().len()<6<<20 {store.append(&(0..64).map(|n|large_event(&format!("old.{batch}.{n}"))).collect::<Vec<_>>()).unwrap();batch+=1;}
        let newest=store.export(0,0,1024).unwrap()["retained_seq"][1].clone();drop(store);
        std::fs::File::create(&peer).unwrap().set_len(70<<20).unwrap();
        let small=budget(8<<20);
        let store=Store::open_bounded(&path,small.clone()).unwrap();
        let storage=store.storage().unwrap();assert_eq!(storage["state"],"degraded");
        assert!(storage["pruned_count"].as_u64().unwrap()>0);
        assert_eq!(store.export(0,0,1024).unwrap()["retained_seq"][1],newest,"recovery dropped the newest rows");
        store.append(&[event("after.recovery")]).unwrap();
        std::fs::remove_file(&peer).unwrap();drop(store);
        let store=Store::open_bounded(&path,small.clone()).unwrap();store.reclaim().unwrap();
        assert_eq!(store.storage().unwrap()["state"],"bounded");assert!(std::fs::metadata(&path).unwrap().len()<=small.database_bytes().unwrap() as u64);
        drop(store);std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn two_stores_share_one_physical_ceiling_under_pressure() {
        let dir=crate::scratch("telemetry-two-budgets");let total=12<<20;
        let budget=|peer:&str| Budget {lock:dir.join("installation.lock"),peers:[peer.to_owned(),format!("{peer}-wal"),format!("{peer}-shm")].into_iter().map(|n|dir.join(n)).collect(),total,reserved:1<<20};
        let a=Store::open_bounded(&dir.join("a"),budget("b")).unwrap();
        let b=Store::open_bounded(&dir.join("b"),budget("a")).unwrap();
        let mut peak=0;
        for i in 0..80 {
            let store=if i%2==0 {&a} else {&b};
            let events=(0..32).map(|n|large_event(&format!("batch.{i}.{n}"))).collect::<Vec<_>>();
            if let Err(error)=store.append(&events) {panic!("budget pressure must prune, not refuse: {error}");}
            let bytes=std::fs::read_dir(&dir).unwrap().map(|e|e.unwrap().metadata().unwrap().len()).sum::<u64>();
            peak=peak.max(bytes);assert!(bytes+(1<<20)<=total,"combined bytes {bytes}");
        }
        assert!(a.storage().unwrap()["pruned_count"].as_i64().unwrap()>0&&b.storage().unwrap()["pruned_count"].as_i64().unwrap()>0);
        println!("combined peak {peak}");
        drop(a);drop(b);std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn readonly_analysis_keeps_one_snapshot_without_writer_authority() {
        let dir=crate::scratch("readonly-analysis");let path=dir.join("db");
        assert!(Reader::open(&path).is_err());assert!(!path.exists());
        let owner=Store::open(&path).unwrap();owner.append(&[event("first"),event("second")]).unwrap();
        owner.import("archive",None,"position.1",&[]).unwrap();
        let reader=Reader::open(&path).unwrap();
        assert_eq!(reader.import_cursor("archive").unwrap().as_deref(),Some("position.1"));
        assert_eq!(reader.import_cursor("absent").unwrap(),None);assert!(reader.import_cursor("invalid/source").is_err());
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
    // taskq#1341: a full-store scan inside Reader::open pinned the WAL and failed writer checkpoints.
    #[test]
    fn reader_open_does_not_pin_large_store_against_writer() {
        let _disk=crate::bulk_disk();
        use std::sync::atomic::{AtomicBool,Ordering::Relaxed};
        let dir=crate::scratch("reader-large");let path=dir.join("db");let store=Store::open(&path).unwrap();
        for batch in 0..64 {store.append(&(0..1024).map(|i|event(&format!("op.{batch}.{i}"))).collect::<Vec<_>>()).unwrap();}
        let stop=Arc::new(AtomicBool::new(false));
        let readers={let (path,stop)=(path.clone(),stop.clone());std::thread::spawn(move||{
            let mut opens=0;while !stop.load(Relaxed) {Reader::open(&path).unwrap().export(0,0,1).unwrap();opens+=1;}opens})};
        // The writer deadline is busy_timeout: a pinned snapshot fails append or prune with "checkpoint busy".
        // Wall time is not asserted; a loaded Windows runner exceeded 2 s on disk I/O alone.
        for i in 0..8 {store.append(&[event(&format!("later.{i}"))]).unwrap();store.prune().unwrap();}
        stop.store(true,Relaxed);assert!(readers.join().unwrap()>0);
        // A4: a corrupt row outside the open checks is still reported, with its reason, by the page that reads it.
        drop(store);Connection::open(&path).unwrap().execute("UPDATE events SET kind='other' WHERE seq=2",[]).unwrap();
        let reader=Reader::open(&path).unwrap();
        assert_eq!(reader.export(0,0,1024).unwrap_err(),"telemetry corrupt kind index metadata");drop(reader);
        Connection::open(&path).unwrap().execute("UPDATE metadata SET pruned_through=-1",[]).unwrap();
        assert_eq!(Reader::open(&path).err().unwrap(),"telemetry corrupt retention metadata");
        std::fs::remove_dir_all(dir).unwrap();
    }
    // taskq#1499: the full scan may be skipped only for the exact state an orderly close sealed.
    #[test]
    fn sealed_close_skips_the_full_scan_only_while_the_store_is_unchanged() {
        let dir=crate::scratch("sealed-close");let path=dir.join("db");
        // An orphan block fails only the full scan; an open that succeeds skipped it.
        let orphan=||Connection::open(&path).unwrap().execute("INSERT INTO blocks(payload) VALUES(x'00')",[]).unwrap();
        let store=Store::open(&path).unwrap();store.append(&[event("before")]).unwrap();orphan();store.seal();drop(store);
        let store=Store::open(&path).unwrap();store.append(&[event("after")]).unwrap();drop(store);
        // Unclean stop: the seal was consumed and the state changed, so the next owner runs the full scan.
        assert_eq!(Store::open(&path).err().unwrap(),"telemetry unreferenced compressed block");
        fs::remove_dir_all(&dir).unwrap();fs::create_dir_all(&dir).unwrap();
        let store=Store::open(&path).unwrap();orphan();store.seal();store.append(&[event("late")]).unwrap();drop(store);
        assert_eq!(Store::open(&path).err().unwrap(),"telemetry unreferenced compressed block","a write after the seal invalidates it");
        fs::remove_dir_all(&dir).unwrap();fs::create_dir_all(&dir).unwrap();
        let store=Store::open(&path).unwrap();store.seal();drop(store);orphan();
        assert_eq!(Store::open(&path).err().unwrap(),"telemetry unreferenced compressed block","a foreign write after the seal invalidates it");
        fs::remove_dir_all(dir).unwrap();
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
        let _disk=crate::bulk_disk();
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
        for key in names(Field::Text) {if !value.as_object().unwrap().contains_key(key)&&key!="project" {value[key]=json!(noise());}}
        value["project"]=json!("fixture/project");
        let mut unknown=serde_json::Map::new();
        for key in names(Field::Number) {if !value.as_object().unwrap().contains_key(key) {value[key]=Value::Null;unknown.insert(key.into(),json!(noise()));}}
        value["unknown"]=Value::Object(unknown);Event::new(value).unwrap()
    }
    #[test]
    fn physical_pressure_prunes_atomically_and_bounds_database_wal_shm() {
        let _disk=crate::bulk_disk();
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
    fn capacity_pruning_resolves_removed_duplicates_and_preserves_late_conflicts() {
        let dir=crate::scratch("telemetry-resolve-capacity");let store=Store::open(&dir.join("db")).unwrap();
        let old=(0..64).map(|i|large_event(&format!("retained.{i}"))).collect::<Vec<_>>();let original=store.append(&old).unwrap();
        {let db=store.db.lock().unwrap();checkpoint(&db,true,"reclaim").unwrap();let pages:i64=db.pragma_query_value(None,"page_count",|r|r.get(0)).unwrap();db.pragma_update(None,"max_page_count",pages).unwrap();}
        let before=store.export(0,0,1024).unwrap();let mut conflict=(0..BATCH).map(|_|event("new")).collect::<Vec<_>>();
        let mut changed=event("changed");changed.id=old[0].id.clone();conflict.push(changed);
        assert!(store.append_group(&conflict).unwrap_err().contains("immutable id payload mismatch"));assert_eq!(store.export(0,0,1024).unwrap(),before);
        let events=[old[0].clone(),event("new")];let seq=store.append(&events).unwrap();
        assert!(seq[0]>original[63]);assert!(seq[1]>seq[0]);assert_eq!(store.append(&events).unwrap(),seq);
        let after=store.export(0,0,1024).unwrap();assert_eq!(after["events"].as_array().unwrap().len(),2);assert_eq!(after["pruned_count"],64);
        assert_eq!(after["events"][0]["id"],old[0].id);assert_eq!(after["events"][0]["observation"],serde_json::from_str::<Value>(&old[0].payload).unwrap());
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
        value["unknown"]=json!({});for key in names(Field::Number).iter().filter(|key|**key!="started_ms") {value[*key]=Value::Null;value["unknown"][*key]=json!("x".repeat(128));}
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
            assert!(wal_has_room(credit,page,DATABASE_BYTES as u64,DATABASE_BYTES as u64+SHM_BYTES));assert!(!wal_has_room(credit+1,page,DATABASE_BYTES as u64,DATABASE_BYTES as u64+SHM_BYTES));
        }
        assert!(!wal_has_room(i64::MAX,4096,DATABASE_BYTES as u64,DATABASE_BYTES as u64+SHM_BYTES));assert!(!wal_has_room(-1,4096,DATABASE_BYTES as u64,DATABASE_BYTES as u64+SHM_BYTES));assert!(!wal_has_room(0,512,DATABASE_BYTES as u64,DATABASE_BYTES as u64+SHM_BYTES));assert!(!wal_has_room(0,3072,DATABASE_BYTES as u64,DATABASE_BYTES as u64+SHM_BYTES));
        for bounded in [false,true] {
            let dir=crate::scratch("telemetry-checkpoint-credit");let path=dir.join("db");
            let store=if bounded {Store::open_bounded(&path,Budget {lock:dir.join("installation.lock"),peers:vec![],total:INSTALLATION_BYTES,reserved:PEER_BYTES}).unwrap()} else {Store::open(&path).unwrap()};
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
    }
    // taskq#1533: room sized by the worst-case group never fit a small WAL share, so every append checkpointed.
    #[test]
    fn small_appends_under_the_legacy_budget_do_not_checkpoint_each_time() {
        use tracing_subscriber::prelude::*;
        let dir=crate::scratch("telemetry-legacy-room");let path=dir.join("db");
        let budget=Budget {lock:dir.join("installation.lock"),peers:vec![],total:64<<20,reserved:6<<20};
        let spans=Arc::new(Store::open(&dir.join("spans")).unwrap());
        let capture=capture::Capture::new(spans.clone(),"fixtureq","1").unwrap();
        tracing::subscriber::with_default(tracing_subscriber::registry().with(capture.clone()),|| {
            let store=Store::open_bounded(&path,budget.clone()).unwrap();
            for n in 0..200 {store.append(&[event(&format!("small.{n}"))]).unwrap();}
        });
        capture.flush().unwrap();
        let rooms=spans.export(0,0,1024).unwrap()["events"].as_array().unwrap().iter().filter(|r|r["observation"]["kind"]=="method"&&r["observation"]["operation"]=="fixtureq.operation.telemetry.checkpoint"&&r["observation"]["from"]=="wal-room").count();
        assert!(rooms<20,"{rooms} checkpoints for 200 appends");
        drop(capture);drop(spans);fs::remove_dir_all(dir).unwrap();
    }
    // taskq#1517: the bounded append's threshold checkpoint waited on a pinned Reader and stalled the service for seconds.
    #[test]
    fn pinned_reader_never_stalls_a_bounded_append_across_the_wal_threshold() {
        use tracing_subscriber::prelude::*;
        let dir=crate::scratch("telemetry-pinned-threshold");let path=dir.join("db");
        // The legacy 64 MiB budget: WAL share 3.7 MiB, so these appends cross the room threshold.
        let budget=Budget {lock:dir.join("installation.lock"),peers:vec![],total:64<<20,reserved:6<<20};
        let spans=Arc::new(Store::open(&dir.join("spans")).unwrap());
        let capture=capture::Capture::new(spans.clone(),"fixtureq","1").unwrap();
        tracing::subscriber::with_default(tracing_subscriber::registry().with(capture.clone()),|| {
            let store=Store::open_bounded(&path,budget.clone()).unwrap();store.append(&[event("first")]).unwrap();
            let reader=Reader::open(&path).unwrap();let before=reader.export(0,0,1024).unwrap();
            for batch in 0..16 {store.append(&(0..64).map(|i|large_event(&format!("pinned.{batch}.{i}"))).collect::<Vec<_>>()).unwrap();}
            assert!(store.storage().unwrap()["unfinished_checkpoints"].as_u64().unwrap()>0,"the WAL threshold was never crossed");
            assert_eq!(reader.export(0,0,1024).unwrap(),before,"the pinned snapshot changed");
            drop(reader);
            // The unfinished checkpoint is retried later and then completes, inside the WAL share.
            std::thread::sleep(CHECKPOINT_RETRY);store.append(&[event("released")]).unwrap();
            let storage=store.storage().unwrap();
            assert!(storage["wal_bytes"].as_u64().unwrap()<=budget.wal_bytes().unwrap(),"{storage}");
            assert_eq!(store.db.lock().unwrap().query_row("SELECT count(*) FROM events",[],|r|r.get::<_,i64>(0)).unwrap(),2+16*64);
        });
        capture.flush().unwrap();
        let rows=spans.export(0,0,1024).unwrap()["events"].as_array().unwrap().iter().map(|r|r["observation"].clone()).filter(|o|o["kind"]=="method").collect::<Vec<_>>();
        let seen=|operation:&str,from:&str,to:&str|rows.iter().any(|o|o["operation"]==format!("fixtureq.operation.{operation}")&&o["from"]==from&&o["to"]==to&&o["outcome"]=="ok"&&o["duration_us"].is_u64());
        assert!(seen("telemetry.open","first-open","scan"),"{rows:?}");
        assert!(seen("telemetry.checkpoint","wal-room","busy"),"{rows:?}");
        assert!(seen("telemetry.checkpoint","wal-room","complete"),"{rows:?}");
        drop(capture);drop(spans);fs::remove_dir_all(dir).unwrap();
    }
    // coreq#85: room-only checkpoints let the WAL file sit at its 64 MiB share and copied it under every writer lock.
    #[test]
    fn bounded_wal_file_returns_below_the_live_threshold_after_a_pinned_reader() {
        let dir=crate::scratch("telemetry-live-wal");let path=dir.join("db");
        let budget=Budget {lock:dir.join("installation.lock"),peers:vec![],total:INSTALLATION_BYTES,reserved:PEER_BYTES};
        let store=Store::open_bounded(&path,budget).unwrap();store.append(&[event("first")]).unwrap();
        let batch=|name:&str|(0..64).map(|i|large_event(&format!("{name}.{i}"))).collect::<Vec<_>>();
        let reader=Reader::open(&path).unwrap();reader.export(0,0,1).unwrap();
        for n in 0..48 {store.append(&batch(&format!("pinned.{n}"))).unwrap();}
        let storage=store.storage().unwrap();
        assert!(storage["wal_bytes"].as_u64().unwrap()>LIVE_WAL_BYTES,"{storage}");
        drop(reader);std::thread::sleep(CHECKPOINT_RETRY);
        for n in 0..48 {
            store.append(&batch(&format!("steady.{n}"))).unwrap();
            let storage=store.storage().unwrap();let wal=storage["wal_bytes"].as_u64().unwrap();
            assert!(wal<=LIVE_WAL_BYTES+(1<<20),"high-water WAL file kept: {storage}");
            assert!(storage["wal_live_bytes"].as_u64().unwrap()<=wal,"{storage}");
        }
        assert_eq!(store.db.lock().unwrap().query_row("SELECT count(*) FROM events",[],|r|r.get::<_,i64>(0)).unwrap(),1+96*64);
        drop(store);fs::remove_dir_all(dir).unwrap();
    }
    // taskq#1637: profile health ran Store::storage, which takes the writer lock, to read live WAL bytes.
    #[test]
    fn reader_storage_reports_live_wal_without_writer_lock_or_checkpoint() {
        let dir=crate::scratch("telemetry-reader-storage");let path=dir.join("db");let store=Store::open(&path).unwrap();
        store.append(&[event("live")]).unwrap();
        let before=store.storage().unwrap();assert!(before["wal_live_bytes"].as_u64().unwrap()>0,"{before}");
        let reader=Reader::open(&path).unwrap();
        // A held writer lock would block a reader that took it.
        let _write=crate::file_lock::lock(&store.write_lock).unwrap();
        let storage=reader.storage().unwrap();drop(_write);
        for key in ["database_bytes","wal_bytes","wal_live_bytes","shm_bytes"] {assert_eq!(storage[key],before[key],"{key}: {storage} {before}");}
        assert_eq!(store.storage().unwrap()["wal_live_bytes"],before["wal_live_bytes"],"reader checkpointed");
        drop(reader);drop(store);fs::remove_dir_all(dir).unwrap();
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
    /// Every fixture field maps to a CoreQ validation, every listed type occurs in an observation,
    /// and every observation appends and reads back unchanged.
    fn check_fixture(text: &str, name: &str) -> Res<()> {
        let fixture: Value = serde_json::from_str(text).map_err(err)?;
        fixture_fields(text)?;
        let observations = fixture["observations"].as_array().ok_or("no observations")?;
        let kind = |v: &Value| match v {Value::Null=>"null",Value::Bool(_)=>"boolean",Value::Number(_)=>"number",Value::String(_)=>"string",Value::Array(_)=>"array",Value::Object(_)=>"object"};
        for (field, types) in fixture["fields"].as_object().ok_or("no fields")? {
            for t in types.as_array().ok_or("no types")? {
                if !observations.iter().any(|o| o.get(field).is_some_and(|v| kind(v) == t)) {return Err(format!("{field} {} not exercised", t.as_str().unwrap_or("?")));}
            }
        }
        let dir = crate::scratch(name);let path = dir.join("db");
        let events = observations.iter().map(|o| Event::new(o.clone())).collect::<Res<Vec<_>>>()?;
        Store::open(&path)?.append(&events)?;
        let read = Store::open(&path)?.export(0, 0, 1024)?;
        let back: Vec<_> = read["events"].as_array().ok_or("no export")?.iter().map(|e| e["observation"].clone()).collect();
        fs::remove_dir_all(dir).map_err(err)?;
        if &back != observations {return Err("observations changed on read back".into());}
        Ok(())
    }
    #[test]
    fn pinned_taskq_fixture_fields_append_and_read_back_unchanged() {
        assert_eq!(crate::sha256_hex(TASKQ_FIELDS.as_bytes()), TASKQ_FIELDS_SHA256, "vendored fixture differs from the pinned release asset");
        check_fixture(TASKQ_FIELDS, "telemetry-taskq-fixture").unwrap();
    }
    #[test]
    fn taskq_fixture_field_that_coreq_drops_fails_the_check() {
        let mut fixture: Value = serde_json::from_str(TASKQ_FIELDS).unwrap();
        // A new string field maps without a code edit once its fixture is vendored...
        fixture["fields"]["new_state"] = json!(["string"]);
        assert!(fixture_fields(&fixture.to_string()).unwrap().contains(&("new_state".into(), Field::Text)));
        // ...but the built CoreQ drops it until then, and the check says so.
        let mut sample = fixture["observations"][0].clone();sample["new_state"] = json!("ready");
        fixture["observations"].as_array_mut().unwrap().push(sample);
        assert!(check_fixture(&fixture.to_string(), "telemetry-taskq-fixture-new").unwrap_err().contains("new_state"));
        // A type CoreQ cannot validate refuses rather than passing unchecked.
        fixture["fields"]["new_state"] = json!(["object"]);
        assert!(fixture_fields(&fixture.to_string()).unwrap_err().contains("new_state"));
        // A listed type that no observation exercises cannot prove the field survives.
        let mut fixture: Value = serde_json::from_str(TASKQ_FIELDS).unwrap();
        fixture["fields"]["open"] = json!(["boolean", "null"]);
        assert!(check_fixture(&fixture.to_string(), "telemetry-taskq-fixture-type").unwrap_err().contains("open null"));
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
        let writers=(0..4).map(|_|Store::open(&path).unwrap()).collect::<Vec<_>>();
        let start=std::sync::Barrier::new(4);
        std::thread::scope(|scope| { for writer in writers { let start=&start;scope.spawn(move || {
            start.wait();for _ in 0..25 { writer.append(&[event("method.call")]).unwrap(); }
        }); }});
        let store = Store::open(&path).unwrap();
        assert_eq!(store.export(0,0,1024).unwrap()["events"].as_array().unwrap().len(),100);
        drop(store); fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn independent_processes_commit_without_writer_starvation() {
        let dir=crate::scratch("telemetry-processes");let path=dir.join("db");
        let start=dir.join("start");let mut children=Vec::new();
        let until=Instant::now()+Duration::from_secs(20);
        let result=(||->Result<(),String> {
            // Open may refuse a busy initialization lock; this case measures concurrent append, not bootstrap.
            for n in 0..4 {
                let ready=dir.join(format!("{n}.ready"));
                children.push(Command::new(std::env::current_exe().map_err(err)?)
                    .args(["--exact","telemetry::tests::many_writer","--ignored","--nocapture"])
                    .env("COREQ_TELEMETRY_MULTIPROCESS_DB",&path).env("COREQ_TELEMETRY_WRITER_READY",&ready)
                    .env("COREQ_TELEMETRY_WRITER_START",&start).spawn().map_err(err)?);
                while !ready.try_exists().map_err(err)? {
                    if let Some(status)=children.last_mut().unwrap().try_wait().map_err(err)? {return Err(format!("writer {n} initialization failed: {status}"));}
                    if Instant::now()>=until {return Err(format!("writer {n} initialization deadline"));}
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
            fs::write(&start,"").map_err(err)?;
            for (n,child) in children.iter_mut().enumerate() {
                loop {
                    if let Some(status)=child.try_wait().map_err(err)? {
                        if !status.success() {return Err(format!("writer {n} append failed: {status}"));}break;
                    }
                    if Instant::now()>=until {return Err(format!("writer {n} append deadline"));}
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
            Ok(())
        })();
        for child in &mut children {
            if child.try_wait().unwrap().is_none() {child.kill().unwrap();}child.wait().unwrap();
        }
        assert!(result.is_ok(),"four native writers: {result:?}");
        let store=Store::open(&path).unwrap();assert_eq!(store.export(0,0,1024).unwrap()["events"].as_array().unwrap().len(),100);
        drop(store);fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    #[ignore="subprocess helper exercised by independent_processes_commit_without_writer_starvation"]
    fn many_writer() {
        let path=std::env::var_os("COREQ_TELEMETRY_MULTIPROCESS_DB").unwrap();let store=Store::open(Path::new(&path)).unwrap();
        fs::write(std::env::var_os("COREQ_TELEMETRY_WRITER_READY").unwrap(),"").unwrap();
        let start=PathBuf::from(std::env::var_os("COREQ_TELEMETRY_WRITER_START").unwrap());
        let until=Instant::now()+Duration::from_secs(20);
        while !start.try_exists().unwrap() {assert!(Instant::now()<until,"writer start deadline");std::thread::sleep(Duration::from_millis(5));}
        for _ in 0..25 {store.append(&[event("process.call")]).unwrap();}
    }

    // Busy bootstrap is an explicit refusal; already acknowledged observations remain recoverable.
    #[test]
    fn busy_initialization_preserves_committed_observations() {
        let dir=crate::scratch("telemetry-busy-init");let path=dir.join("db");
        let store=Store::open(&path).unwrap();let record=event("committed");
        store.append(&[record.clone()]).unwrap();drop(store);
        let before=fs::read(&path).unwrap();let mut lock=path.as_os_str().to_os_string();lock.push(".init.lock");
        let guard=crate::file_lock::try_lock(Path::new(&lock)).unwrap();
        assert!(Store::open(&path).is_err(),"busy bootstrap must never masquerade as an empty healthy store");
        assert_eq!(fs::read(&path).unwrap(),before);drop(guard);
        let store=Store::open(&path).unwrap();let rows=store.export(0,0,1024).unwrap();
        assert_eq!(rows["events"].as_array().unwrap().len(),1);assert_eq!(rows["events"][0]["id"],record.id());
        drop(store);fs::remove_dir_all(dir).unwrap();
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
