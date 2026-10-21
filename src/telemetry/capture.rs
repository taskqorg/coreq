//! One metadata-only tracing layer and bounded group-commit writer for every consumer.
use super::{Event, Store, clock, code};
use serde_json::{Map, Value, json};
use std::{
    collections::HashMap,
    io::Write,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, SyncSender, TrySendError},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
use tracing::{
    Id, Subscriber,
    field::{Field, Visit},
    span::Attributes,
};
use tracing_subscriber::{Layer, layer::Context, registry::LookupSpan};

static GLOBAL: OnceLock<Capture> = OnceLock::new();
/// The process sink, sealed after the orderly-shutdown flush.
static SINK: OnceLock<Arc<Mutex<Option<super::broker::Sink>>>> = OnceLock::new();
/// Observations the process bootstrap itself accepted: its writer election and owner open spans.
static BOOTSTRAP: AtomicU64 = AtomicU64::new(0);
const QUEUE_BYTES: u64 = 16 * 1024 * 1024;
const METHOD_TARGET: &str = "coreq::method";

pub(super) fn observing() -> bool {
    tracing::dispatcher::get_default(|d| !d.is::<tracing::subscriber::NoSubscriber>())
}

#[derive(Default)]
struct Diagnostics {
    active: AtomicBool,
    cache_dirty: AtomicBool,
    session: Mutex<Option<DiagnosticSession>>,
    dispatchers: Mutex<Vec<tracing::dispatcher::WeakDispatch>>,
}
struct DiagnosticSession {
    selector: String,
    until: Instant,
    limit: u64,
    admitted: u64,
}
impl DiagnosticSession {
    fn health(&self) -> Value {
        json!({"state":if self.admitted == self.limit {"limited"} else if Instant::now() >= self.until {"expired"} else {"active"},
            "selector":self.selector,"limit":self.limit,"admitted":self.admitted})
    }
}
impl Diagnostics {
    fn rebuild(&self) {
        let dispatch = self.dispatchers.lock().ok().and_then(|v| v.iter().find_map(|d| d.upgrade()));
        if let Some(dispatch) = dispatch {
            // tracing's single-subscriber fast path rebuilds from the current dispatcher.
            tracing::dispatcher::with_default(&dispatch, tracing::callsite::rebuild_interest_cache);
        }
    }
    fn matches(&self, name: &str, admit: bool) -> bool {
        if !self.active.load(Ordering::Relaxed) { return false; }
        let Ok(mut guard) = self.session.lock() else { return false; };
        let Some(s) = guard.as_mut() else { return false; };
        if Instant::now() >= s.until || s.admitted == s.limit {
            self.active.store(false, Ordering::Relaxed);
            self.cache_dirty.store(true, Ordering::Relaxed);
            return false;
        }
        let prefix=s.selector.trim_end_matches('.');
        let selected = s.selector == "*" || name == prefix
            || name.strip_prefix(prefix).is_some_and(|rest| rest.starts_with('.'));
        if selected && admit { s.admitted += 1; }
        if s.admitted == s.limit {
            self.active.store(false, Ordering::Relaxed);
            self.cache_dirty.store(true, Ordering::Relaxed);
        }
        selected
    }
    fn health(&self) -> Value {
        let Ok(guard) = self.session.lock() else { return json!({"state":"unknown"}) };
        let Some(s) = guard.as_ref() else { return json!({"state":"off"}) };
        let health = s.health();
        if health["state"] != "active" && self.active.swap(false, Ordering::Relaxed) {
            self.cache_dirty.store(true, Ordering::Relaxed);
        }
        drop(guard);
        // Rebuild from control/health code, never from a subscriber callback.
        if self.cache_dirty.swap(false, Ordering::Relaxed) { self.rebuild(); }
        health
    }
}

/// An observation link only: no operation payload, authority or delivery acknowledgment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceContext { trace: String, parent: String }
impl TraceContext {
    pub fn current() -> Option<Self> {
        tracing::Span::current().with_subscriber(|(id, dispatch)| {
            let registry = dispatch.downcast_ref::<tracing_subscriber::Registry>()?;
            let span = registry.span(id)?;
            span.scope().find_map(|span| {
                let extensions = span.extensions();
                let state = extensions.get::<State>()?;
                Some(Self { trace: state.trace.clone(), parent: state.span.clone() })
            })
        }).flatten()
    }
    pub fn encode(&self) -> String { format!("1 {} {}", self.trace, self.parent) }
    pub fn decode(text: &str) -> Result<Self, String> {
        if text.len() > 128 { return Err("telemetry invalid trace context".into()); }
        let mut fields = text.split(' ');
        match (fields.next(), fields.next(), fields.next(), fields.next()) {
            (Some("1"), Some(trace), Some(parent), None) if trace_id(trace) && trace_id(parent) =>
                Ok(Self { trace: trace.into(), parent: parent.into() }),
            _ => Err("telemetry invalid trace context".into()),
        }
    }
    pub fn span(&self) -> tracing::Span {
        if !observing() { return tracing::Span::none(); }
        tracing::info_span!(parent: None, "continuation", coreq_trace = self.trace.as_str(), coreq_parent = self.parent.as_str())
    }
}
fn trace_id(text: &str) -> bool {
    text.split_once('.').is_some_and(|(run, span)| super::hex(run, 32)
        && span.parse::<u64>().is_ok_and(|id| id != 0 && id.to_string() == span))
}

/// Carry the initiating dispatcher/link into one owned thread or scheduled synchronous callback.
pub fn bind<F,T>(action:F)->impl FnOnce()->T where F:FnOnce()->T {
    let dispatch=tracing::dispatcher::get_default(Clone::clone);
    let context=TraceContext::current();
    move ||tracing::dispatcher::with_default(&dispatch,||{
        let _context=context.as_ref().map(|context|context.span().entered());
        action()
    })
}

/// Replace stale inherited context with this execution's identities; never copy payload or authority.
pub fn inherit_context(command: &mut std::process::Command) {
    match TraceContext::current() {
        Some(link) => { command.env("COREQ_TRACE_CONTEXT", link.encode()); }
        None => { command.env_remove("COREQ_TRACE_CONTEXT"); }
    }
}

/// Process-owned flush guard; bootstrap belongs before all product execution.
pub struct Runtime { capture: Capture, continuation: Option<tracing::span::EnteredSpan> }
impl Runtime {
    pub fn install(path: &std::path::Path, product: &str, version: &str) -> Result<Self, String> {
        Self::install_with_budget(path,product,version,None)
    }
    pub fn install_bounded(path:&std::path::Path,product:&str,version:&str,budget:super::Budget)->Result<Self,String> {
        Self::install_with_budget(path,product,version,Some(budget))
    }
    fn install_with_budget(path:&std::path::Path,product:&str,version:&str,budget:Option<super::Budget>)->Result<Self,String> {
        use tracing_subscriber::prelude::*;
        if GLOBAL.get().is_some() { return Err("telemetry already initialized".into()); }
        // has_been_set() also turns true on any scoped default, including a failed install's election scope;
        // a fresh thread has no scoped default, so its default is the global one.
        if std::thread::spawn(observing).join().unwrap_or(true) { return Err("telemetry global subscriber unavailable".into()); }
        let sink=Arc::new(Mutex::new(None::<super::broker::Sink>));
        // Held until the sink exists: the writer waits for it, so the bootstrap's own spans are kept.
        let mut opening=sink.lock().map_err(|_|"telemetry writer poisoned")?;
        let capture = Capture::with_append({let sink=sink.clone();move |events|sink.lock().map_err(|_|"telemetry writer poisoned")?
            .as_mut().ok_or("telemetry writer stopped")?.append(events)}, product, version)?;
        let dispatch=tracing::Dispatch::new(tracing_subscriber::registry().with(capture.clone()));
        // Writer election and owner open are dated by spans like any later promotion.
        match tracing::dispatcher::with_default(&dispatch,||super::broker::open_with_budget(path,budget)) {
            Ok(opened)=>*opening=Some(opened),
            Err(error)=>{drop(opening);return Err(error);}
        }
        drop(opening);
        if tracing::dispatcher::set_global_default(dispatch).is_err() {
            // Lost race for the global subscriber: this collector commits nothing.
            if let Ok(mut opened)=sink.lock() {opened.take();}
            return Err("telemetry global subscriber unavailable".into());
        }
        GLOBAL.set(capture.clone()).map_err(|_| "telemetry already initialized")?;
        let _=SINK.set(sink);
        // qualify requires observations beyond the collector's own bootstrap spans.
        BOOTSTRAP.store(capture.writer.health.accepted.load(Ordering::Relaxed), Ordering::Relaxed);
        let continuation = std::env::var_os("COREQ_TRACE_CONTEXT").and_then(|value| {
            match value.to_str().and_then(|text| TraceContext::decode(text).ok()) {
                Some(link) => Some(link.span().entered()),
                None => {
                    capture.writer.health.lost.fetch_add(1, Ordering::Relaxed);
                    let _=writeln!(std::io::stderr(),"Telemetry: unknown (invalid process context)");
                    None
                }
            }
        });
        Ok(Self { capture, continuation })
    }
}
impl Drop for Runtime {
    fn drop(&mut self) { drop(self.continuation.take()); report_flush(&self.capture); }
}
/// Qualify the current local collector, never infer coverage or other hosts from this barrier.
pub fn qualify() -> Result<Value,String> {
    let capture=GLOBAL.get().ok_or("telemetry collector unavailable")?;
    capture.flush()?;
    let health=capture.health();
    if health["state"]!="collecting" {return Err("telemetry collection incomplete; observations lost".into());}
    if health["committed"].as_u64().unwrap_or(0)<=BOOTSTRAP.load(Ordering::Relaxed) {return Err("telemetry no committed observations".into());}
    Ok(health)
}
/// Same bounded selection for applications using the process bootstrap.
pub fn diagnose(selector: &str, period: Duration, limit: u64) -> Result<(), String> {
    GLOBAL.get().ok_or("telemetry collector unavailable")?.diagnose(selector, period, limit)
}

/// Admit product metadata to the current writer; success is not a durable receipt.
pub fn observe(value: Value) -> Result<(), String> {
    tracing::dispatcher::get_default(|dispatch| {
        dispatch.downcast_ref::<Capture>().cloned()
    }).ok_or("telemetry collector unavailable")?.observe(value)
}

/// Import an explicit execution link without replacing it with the receiving thread's scope.
/// Selected external method detail shares this process's existing diagnostic quota and expiry.
pub fn observe_linked(value: Value) -> Result<(), String> {
    tracing::dispatcher::get_default(|dispatch|dispatch.downcast_ref::<Capture>().cloned())
        .ok_or("telemetry collector unavailable")?.observe_linked(value)
}

/// Persist one command phase under the current explicit action; consumers decide when that phase is true.
pub fn checkpoint(phase: &str) -> Result<String, String> {
    CommandReceipt::current()?.checkpoint(phase)
}

/// Original command identity only; carrying this value never authorizes a business effect.
#[derive(Clone, Debug)]
pub struct CommandReceipt { metadata: Map<String, Value> }
impl CommandReceipt {
    pub fn current() -> Result<Self, String> {
        tracing::Span::current().with_subscriber(|(id, dispatch)| {
            let capture = dispatch.downcast_ref::<Capture>().ok_or("command receipt collector unavailable")?;
            let registry = dispatch.downcast_ref::<tracing_subscriber::Registry>().ok_or("command receipt registry unavailable")?;
            let span = registry.span(id).ok_or("command receipt action unavailable")?;
            let action = span.scope().find(|s| s.metadata().target() != METHOD_TARGET).ok_or("command receipt action unavailable")?;
            let ext = action.extensions();
            let state = ext.get::<State>().ok_or("command receipt identity unavailable")?;
            Self::from_value(json!({"wire":1,"product":&*capture.product,"version":&*capture.version,
                "operation":state.operation,"trace":state.trace,"span":state.span,"parent":state.parent,"started_ms":state.started_ms}))
        }).ok_or("command receipt action unavailable")?
    }
    pub fn encode(&self) -> String {
        let mut value = self.metadata.clone();
        value.insert("wire".into(), json!(1));
        Value::Object(value).to_string()
    }
    pub fn decode(text: &str) -> Result<Self, String> {
        if text.len() > 1024 { return Err("invalid command receipt descriptor".into()); }
        let value = serde_json::from_str(text).map_err(|_| "invalid command receipt descriptor")?;
        Self::from_value(value)
    }
    fn from_value(value: Value) -> Result<Self, String> {
        let invalid = "invalid command receipt descriptor";
        let mut metadata = value.as_object().cloned().ok_or(invalid)?;
        if metadata.len() != 8 || metadata.remove("wire") != Some(json!(1))
            || !["product", "version", "operation", "trace", "span", "parent", "started_ms"].iter().all(|key| metadata.contains_key(*key))
            || !["trace", "span"].iter().all(|key| metadata[*key].as_str().is_some_and(trace_id))
            || !(metadata["parent"].is_null() || metadata["parent"].as_str().is_some_and(trace_id))
            || !metadata["started_ms"].as_i64().is_some_and(|n| n >= 0) {
            return Err(invalid.into());
        }
        let receipt = Self { metadata };
        receipt.event("received").map_err(|_| invalid)?;
        if receipt.encode().len() > 1024 { return Err(invalid.into()); }
        Ok(receipt)
    }
    fn event(&self, phase: &str) -> Result<Event, String> {
        if !matches!(phase, "received"|"applied"|"observed"|"superseded"|"failed") { return Err("invalid command phase".into()); }
        let m = &self.metadata;
        Event::identified("command.receipt", &format!("{}.{phase}", m["span"].as_str().unwrap()), json!({
            "product":m["product"],"version":m["version"],"operation":m["operation"],
            "kind":format!("command.{phase}"),"source":"command","trace":m["trace"],"span":m["span"],"parent":m["parent"],
            "started_ms":m["started_ms"],"outcome":if phase=="failed" {"failed"} else {"ok"},
            "error":if phase=="failed" {"command-failed"} else {"none"}
        }))
    }
    /// Compute a phase's lookup key only; this does not persist or acknowledge that phase.
    pub fn event_id(&self, phase: &str) -> Result<String, String> {
        self.event(phase).map(|event|event.id().to_owned())
    }
    /// Commit under the original identity, independent of the observer's current span or version.
    pub fn checkpoint(&self, phase: &str) -> Result<String, String> {
        let event = self.event(phase)?;
        tracing::dispatcher::get_default(|dispatch| {
            let capture = dispatch.downcast_ref::<Capture>().ok_or("command receipt collector unavailable")?;
            if self.metadata["product"].as_str() != Some(&*capture.product) { return Err("command receipt product mismatch".into()); }
            capture.commit(&event)
        })?;
        Ok(event.id().into())
    }
}

/// Observe a dependency result before its caller can handle it; the adapter owns its deadline.
pub fn operation<T, E>(name: &'static str, action: impl FnOnce() -> Result<T, E>, classify: impl FnOnce(&E) -> &'static str) -> Result<T, E> {
    // A first call under NoSubscriber can cache `never` for an already registered scoped dispatcher.
    let span = if observing() { tracing::info_span!("operation", coreq_operation = name, coreq_result = "unclassified", coreq_error = tracing::field::Empty) }
        else { tracing::Span::none() };
    let _entered = span.enter();
    let result = action();
    if let Err(error) = &result {
        let class = classify(error);
        span.record("coreq_error", if code(class) && class != "none" { class } else { "returned-error" });
    }
    span.record("coreq_result", "observed");
    result
}

/// Repeated native check: `action` runs on every call and its result returns unchanged. `state` names the
/// observed result with a fixed code (`present`, `missing`, `denied`, ...). `subject` names the polled thing
/// (a PID, a path hash) and only separates fold keys; it is never stored. A changed code records one
/// `poll.change` row under this call's span; unchanged calls fold into one `poll` aggregate per operation,
/// project and subject each `POLL_INTERVAL_MS`, written early on a change and at process shutdown.
pub fn poll<T, E>(name: &'static str, subject: u64, action: impl FnOnce() -> Result<T, E>, state: impl FnOnce(&Result<T, E>) -> &'static str) -> Result<T, E> {
    let span = if observing() { tracing::info_span!("operation", coreq_operation = name, coreq_poll = true, coreq_subject = subject, coreq_result = "unclassified",
        coreq_error = tracing::field::Empty, coreq_state = tracing::field::Empty) } else { tracing::Span::none() };
    let _entered = span.enter();
    let result = action();
    let class = state(&result);
    let class = if code(class) && class != "none" { class } else if result.is_err() { "returned-error" } else { "ok" };
    if result.is_err() { span.record("coreq_error", class); }
    span.record("coreq_state", class);
    span.record("coreq_result", "observed");
    result
}

/// Owned background work keeps a causal link without delaying its caller's terminal observation.
pub fn detached_span(name: &'static str) -> tracing::Span {
    if !observing() { return tracing::Span::none(); }
    if let Some(link) = TraceContext::current() {
        tracing::info_span!(parent: None, "operation", coreq_operation=name, coreq_trace=link.trace.as_str(), coreq_parent=link.parent.as_str(),
            coreq_result="unclassified", coreq_error=tracing::field::Empty, process_id=tracing::field::Empty)
    } else {
        tracing::info_span!(parent: None, "operation", coreq_operation=name, coreq_result="unclassified", coreq_error=tracing::field::Empty, process_id=tracing::field::Empty)
    }
}
fn report_flush(capture: &Capture) {
    match capture.finish() {
        // Orderly shutdown: a sealed owner lets the next owner skip its full store scan.
        Ok(())=>if let Some(Ok(mut sink))=SINK.get().map(|sink|sink.lock()) && let Some(sink)=sink.as_mut() {sink.seal();},
        Err(error)=>{ let _=writeln!(std::io::stderr(),"Telemetry: unknown (flush: {})",super::failure_code(&error)); }
    }
}
/// An image replacement skips destructors, so flush while its open spans still exist.
pub fn before_exec() {
    if let Some(capture) = GLOBAL.get() { report_flush(capture); }
}
/// Same process exit semantics, with bounded collection flush before destructors are skipped.
pub fn exit(code: i32) -> ! {
    before_exec();
    std::process::exit(code)
}

enum Message {
    Record(Vec<Event>),
    Commit(Event, mpsc::Sender<Result<(), String>>),
    Flush(mpsc::Sender<Result<(), String>>),
    Stop,
}
#[derive(Default)]
struct Health {
    accepted: AtomicU64,
    committed: AtomicU64,
    lost: AtomicU64,
    failures: AtomicU64,
    batches: AtomicU64,
    append_us: AtomicU64,
    max_append_us: AtomicU64,
    inflight: AtomicU64,
    queued_bytes: AtomicU64,
    queued_events: AtomicU64,
}
struct Writer {
    sender: SyncSender<Message>,
    worker: Mutex<Option<JoinHandle<()>>>,
    health: Arc<Health>,
}
impl Drop for Writer {
    fn drop(&mut self) {
        let _ = self.sender.send(Message::Stop);
        if let Ok(worker) = self.worker.get_mut() {
            if let Some(thread) = worker.take() {
                let _ = thread.join();
            }
        }
    }
}
impl Writer {
    fn reserve(&self, bytes:u64, count:u64)->Result<(),&'static str> {
        self.health.queued_events.fetch_update(Ordering::Relaxed,Ordering::Relaxed,
            |n|n.checked_add(count).filter(|total|*total<=8192)).map_err(|_|"collection queue unavailable")?;
        if self.health.queued_bytes.fetch_update(Ordering::Relaxed,Ordering::Relaxed,
            |n|n.checked_add(bytes).filter(|total|*total<=QUEUE_BYTES)).is_err() {
            self.health.queued_events.fetch_sub(count,Ordering::Relaxed);
            return Err("collection byte budget exhausted");
        }
        Ok(())
    }
    fn release(&self, bytes:u64, count:u64) {
        self.health.queued_bytes.fetch_sub(bytes,Ordering::Relaxed);
        self.health.queued_events.fetch_sub(count,Ordering::Relaxed);
    }
    fn record(&self, event: Event) { let _ = self.enqueue(vec![event]); }
    fn enqueue(&self, events: Vec<Event>) -> Result<(), String> {
        let count=events.len() as u64;
        self.health.accepted.fetch_add(count, Ordering::Relaxed);
        let bytes = events.iter().map(|event|event.payload.len() as u64).sum();
        let failure=if let Err(error)=self.reserve(bytes,count) {
            Some(error)
        } else {
            match self.sender.try_send(Message::Record(events)) {
                Ok(()) => None,
                Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                    self.release(bytes,count);
                    Some("collection queue unavailable")
                }
            }
        };
        if let Some(error)=failure {
            if self.health.lost.fetch_add(count, Ordering::Relaxed)==0 {
                let _=writeln!(std::io::stderr(),"Telemetry: unknown ({error}; observations lost)");
            }
            return Err(format!("telemetry {error}"));
        }
        Ok(())
    }
}

/// Keep the handle until process shutdown; flush before acknowledging a qualified action.
#[derive(Clone)]
pub struct Capture {
    writer: Arc<Writer>,
    product: Arc<str>,
    version: Arc<str>,
    run: Arc<str>,
    diagnostics: Arc<Diagnostics>,
    // Registry ids are reused slab slots; this counter keeps span ids unique for the collector lifetime.
    spans: Arc<AtomicU64>,
    polls: Arc<Mutex<Polls>>,
    repeats: Arc<Mutex<Repeats>>,
}
impl Capture {
    pub fn new(store: Arc<Store>, product: &str, version: &str) -> Result<Self, String> {
        Self::with_append(move |events|store.append_group(events).map(|_|()),product,version)
    }
    fn with_append(mut append: impl FnMut(&[Event])->Result<(),String> + Send + 'static, product: &str, version: &str) -> Result<Self, String> {
        if !code(product) || !code(version) {
            return Err("telemetry invalid collector identity".into());
        }
        let seed = Event::new(
            json!({"product":product,"version":version,"operation":"collector.identity","kind":"identity","trace":"identity","span":"identity","started_ms":clock()?,"outcome":"ok","error":"none"}),
        )?;
        let (sender, receiver) = mpsc::sync_channel(8192);
        let health = Arc::new(Health::default());
        let state = health.clone();
        let worker = std::thread::Builder::new()
            .name("telemetry-writer".into())
            .spawn(move || {
                let mut pending = Vec::with_capacity(super::COMMIT_EVENTS);
                let mut pending_bytes=0;
                let mut last_error = None;
                let mut commit = |batch: &mut Vec<Event>, last_error: &mut Option<String>| -> Result<(), String> {
                    if batch.is_empty() {
                        return Ok(());
                    }
                    state.inflight.store(batch.len() as u64, Ordering::Relaxed);
                    let began = Instant::now();
                    // Contention is transient: retry the same immutable ids; the bounded queue absorbs arrivals meanwhile.
                    // ponytail: fixed 10 s retry window; past it the batch is lost explicitly. Persist a spool if longer maintenance must be survived.
                    let until = began + Duration::from_secs(10);
                    let mut result = append(batch);
                    while let Err(e) = &result {
                        if !matches!(super::failure_code(e), "budget-locked" | "sqlite-busy" | "transport-unavailable") || Instant::now() >= until { break; }
                        std::thread::sleep(Duration::from_millis(50));
                        result = append(batch);
                    }
                    match &result {
                        Ok(_) => {
                            state
                                .committed
                                .fetch_add(batch.len() as u64, Ordering::Relaxed);
                        }
                        Err(e) => {
                            state.failures.fetch_add(1, Ordering::Relaxed);
                            state.lost.fetch_add(batch.len() as u64, Ordering::Relaxed);
                            if last_error.is_none() {
                                let _=writeln!(std::io::stderr(),"Telemetry: unknown ({})",super::failure_code(&e));
                            }
                            *last_error = Some(e.clone());
                        }
                    }
                    let elapsed = began.elapsed().as_micros().min(u64::MAX as u128) as u64;
                    state.append_us.fetch_add(elapsed, Ordering::Relaxed);
                    state.max_append_us.fetch_max(elapsed, Ordering::Relaxed);
                    state.batches.fetch_add(1, Ordering::Relaxed);
                    state.inflight.store(0, Ordering::Relaxed);
                    batch.clear();
                    result
                };
                let mut deadline = Instant::now();
                loop {
                    let message = if pending.is_empty() {
                        receiver
                            .recv()
                            .map_err(|_| mpsc::RecvTimeoutError::Disconnected)
                    } else {
                        receiver.recv_timeout(deadline.saturating_duration_since(Instant::now()))
                    };
                    match message {
                        Ok(Message::Record(events)) => {
                            let bytes=events.iter().map(|event|event.payload.len()).sum::<usize>();
                            state.queued_bytes.fetch_sub(bytes as u64, Ordering::Relaxed);
                            state.queued_events.fetch_sub(events.len() as u64,Ordering::Relaxed);
                            if pending_bytes+bytes>super::COMMIT_BYTES || pending.len()+events.len()>super::COMMIT_EVENTS {
                                let _ = commit(&mut pending,&mut last_error);pending_bytes=0;
                            }
                            if pending.is_empty() {deadline = Instant::now() + Duration::from_millis(5);}
                            pending_bytes+=bytes;
                            pending.extend(events);
                            if pending.len() == super::COMMIT_EVENTS {
                                let _ = commit(&mut pending, &mut last_error);
                                pending_bytes=0;
                            }
                        }
                        Ok(Message::Commit(event, reply)) => {
                            state.queued_bytes.fetch_sub(event.payload.len() as u64, Ordering::Relaxed);
                            state.queued_events.fetch_sub(1,Ordering::Relaxed);
                            let _ = commit(&mut pending, &mut last_error);
                            pending_bytes=0;
                            let result=commit(&mut vec![event], &mut last_error);
                            let _ = reply.send(result);
                        }
                        Ok(Message::Flush(reply)) => {
                            let _ = commit(&mut pending, &mut last_error);
                            pending_bytes=0;
                            let _ = reply.send(last_error.clone().map_or(Ok(()), Err));
                        }
                        Ok(Message::Stop) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                            let _ = commit(&mut pending, &mut last_error);
                            break;
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            let _ = commit(&mut pending, &mut last_error);pending_bytes=0;
                        }
                    }
                }
            })
            .map_err(|e| format!("telemetry writer: {e}"))?;
        let writer = Arc::new(Writer { sender, worker: Mutex::new(Some(worker)), health });
        Ok(Self {
            writer: writer.clone(),
            product: product.into(),
            version: version.into(),
            run: seed.id().into(),
            diagnostics: Arc::new(Diagnostics::default()),
            spans: Arc::new(AtomicU64::new(1)),
            polls: Arc::new(Mutex::new(Polls::default())),
            repeats: Arc::new(Mutex::new(Repeats { writer: Some(writer), last: HashMap::new(), windows: HashMap::new() })),
        })
    }
    /// Enable only selected diagnostic sites; required operation evidence remains independent.
    pub fn diagnose(&self, selector: &str, period: Duration, limit: u64) -> Result<(), String> {
        if (selector != "*" && !code(selector)) || period.is_zero()
            || period > Duration::from_secs(60) || !(1..=256).contains(&limit) {
            return Err("telemetry invalid diagnostic limits".into());
        }
        let mut session = self.diagnostics.session.lock().map_err(|_| "telemetry diagnostic state unavailable")?;
        if session.as_ref().is_some_and(|s| Instant::now() < s.until && s.admitted < s.limit) {
            return Err("telemetry diagnostic session active".into());
        }
        if let Some(previous) = session.as_ref() { self.record_diagnostic_state(previous.health())?; }
        self.record(json!({"product":&*self.product,"version":&*self.version,"operation":"collector.diagnostic",
            "kind":"diagnostic","trace":&*self.run,"span":&*self.run,"started_ms":clock()?,
            "outcome":"ok","error":"none","source":if selector=="*" {"all"} else {selector},
            "duration_us":period.as_micros() as u64,"calls":limit}));
        *session = Some(DiagnosticSession { selector: selector.into(), until: Instant::now() + period, limit, admitted: 0 });
        self.diagnostics.active.store(true, Ordering::Relaxed);
        drop(session);
        self.diagnostics.rebuild();
        Ok(())
    }
    /// Persist partial repetition/poll windows before orderly shutdown, then cross the writer barrier.
    pub fn finish(&self) -> Result<(), String> {
        self.drain_polls();
        let rows = self.repeats.lock().map_err(|_| "telemetry repetition state unavailable")?.drain();
        for row in rows { self.record(row); }
        self.flush()
    }
    fn fold(&self, value: &Value, result: &str) -> bool {
        // Folding must not turn rejected metadata into a valid-looking aggregate.
        if super::validate(value, None).is_err() { return false; }
        let Ok(mut repeats) = self.repeats.lock() else {
            self.writer.health.lost.fetch_add(1, Ordering::Relaxed);
            return false;
        };
        let (folded, rows) = repeats.observe(value, result);
        drop(repeats);
        for row in rows { self.record(row); }
        folded
    }
    pub(crate) fn drain_polls(&self) {
        let rows = self.polls.lock().map(|mut polls| polls.drain()).unwrap_or_default();
        for row in rows { self.record(self.poll_row(row)); }
    }
    fn poll_row(&self, mut row: Value) -> Value {
        row["product"] = json!(&*self.product);
        row["version"] = json!(&*self.version);
        if row["trace"].is_null() { row["trace"] = json!(&*self.run); row["span"] = json!(&*self.run); }
        row
    }
    fn start_row(&self, state: &State, source: &str) -> Value {
        let mut metadata=Map::with_capacity(state.fields.len()+14);
        metadata.extend(state.fields.iter().map(|(k,v)|(k.clone(),v.clone())));
        let mut value = Value::Object(metadata);
        for (key, field) in [
            ("product", json!(&*self.product)),
            ("version", json!(&*self.version)),
            ("operation", json!(state.operation)),
            ("kind", json!("method.start")),
            ("source", json!(source)),
            ("trace", json!(state.trace)),
            ("span", json!(state.span)),
            ("parent", json!(state.parent)),
            ("started_ms", json!(state.started_ms)),
            ("duration_us", Value::Null),
            ("unknown", json!({"duration_us":"span-open"})),
            ("outcome", json!("unknown")),
            ("error", json!("span-open")),
        ] { value[key] = field; }
        value
    }
    fn operation(&self, name: &str) -> String {
        let operation = format!("{}.{}", self.product, name);
        if operation.len() <= 128 && code(&operation) {
            operation
        } else {
            format!("method.{}", crate::sha256_hex(operation.as_bytes()))
        }
    }
    fn observe(&self, value: Value) -> Result<(), String> {self.observe_metadata(value,true)}
    fn observe_linked(&self,value:Value)->Result<(),String>{
        Event::new(value.clone())?;
        if value["kind"]=="method"&&value["source"]=="diagnostic"&&!self.diagnostics.matches(value["operation"].as_str().ok_or("missing external method")?,true){return Ok(())}
        self.observe_metadata(value,false)
    }
    fn observe_metadata(&self, mut value: Value, inherit:bool) -> Result<(), String> {
        let result=(|| {
            if value["product"]!=self.product.as_ref() {return Err("telemetry foreign semantic product".into());}
            value["version"]=json!(&*self.version);
            if let Some(unknown)=value.get_mut("unknown").and_then(Value::as_object_mut) {unknown.remove("version");}
            if inherit&&let Some(link)=TraceContext::current() {
                value["trace"]=json!(link.trace);value["span"]=json!(link.parent);
                if let Some(unknown)=value.get_mut("unknown").and_then(Value::as_object_mut) {unknown.remove("trace");unknown.remove("span");}
            }
            let (repeating, project) = repeating_scope();
            if repeating && value.get("project").is_none() {
                if let Some(project)=project { value["project"]=project; }
            }
            let events = Event::split(value.clone())?;
            if matches!(value["kind"].as_str(), Some("method.aggregate"|"collection")) && repeating {
                if events.len()==1 && self.fold(&value, value["outcome"].as_str().unwrap_or("unknown")) { return Ok(Vec::new()); }
                // An inherited semantic exception must not point at a root that disappears into a fold.
                tracing::Span::current().with_subscriber(|(id,dispatch)| {
                    if let Some(span)=dispatch.downcast_ref::<tracing_subscriber::Registry>().and_then(|r|r.span(id)) {
                        for span in span.scope() {
                            let mut ext=span.extensions_mut();
                            if let Some(state)=ext.get_mut::<State>().filter(|s|s.repeating && !s.materialized) {
                                state.materialized=true;self.record(self.start_row(state,"action"));
                            }
                        }
                    }
                });
            }
            Ok(events)
        })();
        match result {
            Ok(events)=>self.writer.enqueue(events),
            Err(error)=>{self.writer.health.lost.fetch_add(1,Ordering::Relaxed);Err(error)}
        }
    }
    fn record(&self, value: Value) {
        match Event::new(value) {
            Ok(event) => self.writer.record(event),
            Err(_) => { self.writer.health.lost.fetch_add(1, Ordering::Relaxed); }
        }
    }
    fn record_diagnostic_state(&self, state: Value) -> Result<(), String> {
        self.record(json!({"product":&*self.product,"version":&*self.version,"operation":"collector.diagnostic.state",
            "kind":"diagnostic","trace":&*self.run,"span":&*self.run,"started_ms":clock()?,
            "outcome":"ok","error":"none","source":state["state"],
            "from":if state["selector"]=="*" {json!("all")} else {state["selector"].clone()},
            "calls":state["admitted"],"remaining":state["limit"].as_u64().unwrap_or(0).saturating_sub(state["admitted"].as_u64().unwrap_or(0))}));
        Ok(())
    }
    /// Acknowledge this immutable event only; timeout does not prove that the queued commit failed.
    pub fn commit(&self, event: &Event) -> Result<(), String> {
        let until=Instant::now()+Duration::from_secs(3);
        let bytes=event.payload.len() as u64;
        self.writer.reserve(bytes,1).map_err(|_|"telemetry required record queue full")?;
        let (sender,receiver)=mpsc::channel();
        if let Err(error)=self.writer.sender.try_send(Message::Commit(event.clone(),sender)) {
            self.writer.release(bytes,1);
            return Err(match error {TrySendError::Full(_)=>"telemetry required record queue full",TrySendError::Disconnected(_)=>"telemetry writer stopped"}.into());
        }
        self.writer.health.accepted.fetch_add(1,Ordering::Relaxed);
        receiver.recv_timeout(until.saturating_duration_since(Instant::now()))
            .map_err(|_| {
                let h=&self.writer.health;
                format!("telemetry required record unacknowledged; commit is unknown (accepted {} committed {} lost {} inflight {} queued {} append_us {} max_append_us {})",
                    h.accepted.load(Ordering::Relaxed),h.committed.load(Ordering::Relaxed),h.lost.load(Ordering::Relaxed),
                    h.inflight.load(Ordering::Relaxed),h.queued_events.load(Ordering::Relaxed),h.append_us.load(Ordering::Relaxed),h.max_append_us.load(Ordering::Relaxed))
            })?
    }

    pub fn flush(&self) -> Result<(), String> {
        let diagnostic = self.diagnostics.health();
        if diagnostic["state"] != "off" {
            self.record_diagnostic_state(diagnostic)?;
        }
        let (sender, receiver) = mpsc::channel();
        let until = Instant::now() + Duration::from_secs(3);
        let mut message = Message::Flush(sender);
        loop {
            match self.writer.sender.try_send(message) {
                Ok(()) => break,
                Err(TrySendError::Disconnected(_)) => return Err("telemetry writer stopped".into()),
                Err(TrySendError::Full(pending)) => {
                    if Instant::now() >= until { return Err("telemetry flush queue deadline".into()); }
                    message = pending;
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
        receiver.recv_timeout(until.saturating_duration_since(Instant::now()))
            .map_err(|_| format!("telemetry flush unacknowledged (accepted {} committed {} lost {} failed {} batches {} append_us {} max_append_us {} inflight {})",
                self.writer.health.accepted.load(Ordering::Relaxed),self.writer.health.committed.load(Ordering::Relaxed),
                self.writer.health.lost.load(Ordering::Relaxed),self.writer.health.failures.load(Ordering::Relaxed),
                self.writer.health.batches.load(Ordering::Relaxed),self.writer.health.append_us.load(Ordering::Relaxed),
                self.writer.health.max_append_us.load(Ordering::Relaxed),self.writer.health.inflight.load(Ordering::Relaxed)))??;
        if self.writer.health.lost.load(Ordering::Relaxed) != 0 {
            return Err("telemetry collection incomplete; observations lost".into());
        }
        Ok(())
    }
    pub fn health(&self) -> Value {
        let h = &self.writer.health;
        let lost = h.lost.load(Ordering::Relaxed);
        let failures = h.failures.load(Ordering::Relaxed);
        json!({"state":if lost==0&&failures==0 {"collecting"} else {"unknown"},"accepted":h.accepted.load(Ordering::Relaxed),"committed":h.committed.load(Ordering::Relaxed),"lost":lost,"failed_batches":failures,
            "append_batches":h.batches.load(Ordering::Relaxed),"append_us":h.append_us.load(Ordering::Relaxed),
            "max_append_us":h.max_append_us.load(Ordering::Relaxed),"inflight":h.inflight.load(Ordering::Relaxed),
            "queued_bytes":h.queued_bytes.load(Ordering::Relaxed),"queued_events":h.queued_events.load(Ordering::Relaxed),"queue_bytes_limit":QUEUE_BYTES,"diagnostics":self.diagnostics.health()})
    }
}
struct State {
    started_ms: i64,
    began: Instant,
    trace: String,
    span: String,
    parent: Option<String>,
    fields: Map<String, Value>,
    failed: bool,
    error_class: Option<String>,
    result_unknown: bool,
    refusal: Option<String>,
    operation: String,
    active_us: u64,
    entered: HashMap<std::thread::ThreadId, (usize, Instant)>,
    poll: bool,
    repeating: bool,
    materialized: bool,
    poll_state: Option<String>,
    poll_subject: u64,
}
#[derive(Default)]
struct Fields {
    values: Map<String, Value>,
    error: bool,
    error_class: Option<String>,
    result_unknown: Option<bool>,
    trace: Option<String>,
    parent: Option<String>,
    invalid_context: bool,
    refusal: Option<String>,
    operation: Option<String>,
    poll: bool,
    repeating: bool,
    poll_state: Option<String>,
    poll_subject: u64,
}
impl Visit for Fields {
    fn record_bool(&mut self, field: &Field, value: bool) {
        if field.name() == "coreq_poll" { self.poll = value; }
        if field.name() == "coreq_repeating" { self.repeating = value; }
    }
    fn record_debug(&mut self, field: &Field, _value: &dyn std::fmt::Debug) {
        if field.name() == "error" {
            self.error = true;
        }
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "coreq_error" {
            self.error = true;
            if code(value) && value != "none" { self.error_class = Some(value.into()); }
            return;
        }
        if field.name() == "coreq_operation" {
            if code(value) { self.operation = Some(value.into()); }
            return;
        }
        if field.name() == "coreq_state" {
            if code(value) { self.poll_state = Some(value.into()); }
            return;
        }
        if matches!(field.name(), "coreq_from" | "coreq_to") {
            if code(value) { self.values.insert(field.name()[6..].into(), json!(value)); }
            return;
        }
        if field.name() == "coreq_refusal" {
            if code(value) && value != "none" { self.refusal = Some(value.into()); }
            return;
        }
        if matches!(field.name(), "coreq_trace" | "coreq_parent") {
            if !trace_id(value) { self.invalid_context = true; }
            else if field.name() == "coreq_trace" { self.trace = Some(value.into()); }
            else { self.parent = Some(value.into()); }
            return;
        }
        if field.name() == "error" {
            self.error = true;
            return;
        }
        if field.name() == "coreq_result" {
            self.result_unknown = Some(value == "unclassified");
            return;
        }
        if [
            "project",
            "host",
            "runtime",
            "model",
            "project_spec",
            "shared_spec",
            "client_version",
        ]
        .contains(&field.name())
        {
            self.values.insert(field.name().into(), json!(value));
        }
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "coreq_subject" { self.poll_subject = value; return; }
        if super::NUMBERS.contains(&field.name())
            && !["started_ms", "duration_us"].contains(&field.name())
        {
            self.values.insert(field.name().into(), json!(value));
        }
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        if let Ok(n) = u64::try_from(value) {
            self.record_u64(field, n);
        }
    }
}
impl<S> Layer<S> for Capture
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_register_dispatch(&self, dispatch: &tracing::Dispatch) {
        let mut dispatchers = self.diagnostics.dispatchers.lock().unwrap_or_else(|e| e.into_inner());
        dispatchers.retain(|d| d.upgrade().is_some());
        dispatchers.push(dispatch.downgrade());
    }
    fn register_callsite(&self, metadata: &'static tracing::Metadata<'static>) -> tracing::subscriber::Interest {
        if metadata.target() == METHOD_TARGET {
            if self.diagnostics.active.load(Ordering::Relaxed) { tracing::subscriber::Interest::sometimes() }
            else { tracing::subscriber::Interest::never() }
        }
        else { tracing::subscriber::Interest::always() }
    }
    fn enabled(&self, metadata: &tracing::Metadata<'_>, _: Context<'_, S>) -> bool {
        metadata.target() != METHOD_TARGET || self.diagnostics.matches(metadata.name(), false)
    }
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        if attrs.metadata().target() == METHOD_TARGET && !self.diagnostics.matches(attrs.metadata().name(), true) { return; }
        let Some(span) = ctx.span(id) else { return };
        let repeating = span.parent().into_iter().flat_map(|p|p.scope())
            .any(|p|p.extensions().get::<State>().is_some_and(|s|s.repeating));
        // A child needs its parent's rows: an ancestor poll becomes an ordinary span.
        for ancestor in span.parent().into_iter().flat_map(|p| p.scope()) {
            let mut ext = ancestor.extensions_mut();
            if let Some(state) = ext.get_mut::<State>().filter(|s| s.poll && !s.repeating && !s.materialized) {
                state.materialized = true;
                let start = self.start_row(state, "action");
                drop(ext);
                self.record(start);
            }
        }
        let local = self.spans.fetch_add(1, Ordering::Relaxed);
        let (mut trace, mut parent_id, mut inherited) = span.parent()
            .and_then(|p| p.scope().find_map(|p| {
                let ext = p.extensions();
                ext.get::<State>().map(|s| {
                    (
                        s.trace.clone(),
                        Some(s.span.clone()),
                        s.fields
                            .iter()
                            .filter(|(k, _)| {
                                [
                                    "project",
                                    "host",
                                    "runtime",
                                    "task",
                                    "attempt",
                                    "model",
                                    "project_spec",
                                    "shared_spec",
                                    "client_version",
                                ]
                                .contains(&k.as_str())
                            })
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect(),
                    )
                })
            }))
            .unwrap_or_else(|| (format!("{}.{local}", self.run), None, Map::new()));
        let mut fields = Fields::default();
        attrs.record(&mut fields);
        if fields.invalid_context || fields.trace.is_some() != fields.parent.is_some() {
            self.writer.health.lost.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if let (Some(remote_trace), Some(remote_parent)) = (fields.trace, fields.parent) {
            trace = remote_trace;
            parent_id = Some(remote_parent);
            inherited.clear();
        }
        inherited.extend(fields.values);
        let Ok(started_ms) = clock() else {
            self.writer.health.lost.fetch_add(1, Ordering::Relaxed);
            return;
        };
        #[cfg(test)]
        let started_ms = tests::POLL_CLOCK.with(|clock| clock.get()).unwrap_or(started_ms);
        span.extensions_mut().insert(State {
            started_ms,
            began: Instant::now(),
            trace,
            span: format!("{}.{local}", self.run),
            parent: parent_id,
            fields: inherited,
            failed: fields.error,
            error_class: fields.error_class,
            result_unknown: fields.result_unknown.unwrap_or(false),
            refusal: fields.refusal,
            operation: self.operation(&fields.operation.map_or_else(||span.metadata().name().to_string(), |suffix|format!("{}.{suffix}",span.metadata().name()))),
            active_us: 0,
            entered: HashMap::new(),
            poll: fields.poll,
            repeating: (fields.repeating || repeating) && span.metadata().target() != METHOD_TARGET,
            materialized: false,
            poll_state: fields.poll_state,
            poll_subject: fields.poll_subject,
        });
        if fields.poll || fields.repeating || repeating && span.metadata().target() != METHOD_TARGET { return; }
        let ext = span.extensions();
        let state = ext.get::<State>().expect("inserted span state");
        self.record(self.start_row(state, if span.metadata().target() == METHOD_TARGET { "diagnostic" } else { "action" }));
    }
    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            if let Some(state) = span.extensions_mut().get_mut::<State>() {
                let entry = state
                    .entered
                    .entry(std::thread::current().id())
                    .or_insert_with(|| (0, Instant::now()));
                entry.0 += 1;
            }
        }
    }
    fn on_exit(&self, id: &Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            if let Some(state) = span.extensions_mut().get_mut::<State>() {
                let thread = std::thread::current().id();
                if let Some(entry) = state.entered.get_mut(&thread) {
                    entry.0 -= 1;
                    if entry.0 == 0 {
                        state.active_us = state.active_us.saturating_add(
                            entry.1.elapsed().as_micros().min(i64::MAX as u128) as u64,
                        );
                        state.entered.remove(&thread);
                    }
                }
            }
        }
    }
    fn on_record(&self, id: &Id, values: &tracing::span::Record<'_>, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            if let Some(state) = span.extensions_mut().get_mut::<State>() {
                let mut fields = Fields::default();
                values.record(&mut fields);
                state.fields.extend(fields.values);
                state.failed |= fields.error;
                if fields.error_class.is_some() { state.error_class = fields.error_class; }
                if fields.refusal.is_some() { state.refusal = fields.refusal; }
                if fields.poll_state.is_some() { state.poll_state = fields.poll_state; }
                if let Some(unknown) = fields.result_unknown {
                    state.result_unknown = unknown;
                }
            }
        }
    }
    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        if let Some(span) = ctx.event_span(event) {
            if let Some(state) = span.extensions_mut().get_mut::<State>() {
                let mut fields = Fields::default();
                event.record(&mut fields);
                state.failed |= fields.error;
                if fields.error_class.is_some() { state.error_class = fields.error_class; }
                if fields.refusal.is_some() { state.refusal = fields.refusal; }
                state.fields.extend(fields.values);
            }
        }
    }
    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let ext = span.extensions();
        let Some(state) = ext.get::<State>() else {
            return;
        };
        let failed = state.failed || std::thread::panicking();
        if state.poll && !state.repeating && !state.materialized && !std::thread::panicking() {
            let duration_us = state.began.elapsed().as_micros().min(i64::MAX as u128) as u64;
            let call = PollCall { operation: state.operation.clone(), project: state.fields.get("project").cloned(), subject: state.poll_subject,
                state: state.poll_state.clone().unwrap_or_else(|| "unknown".into()), failed,
                error: if failed { state.error_class.clone().unwrap_or_else(|| "returned-error".into()) } else { "none".into() },
                trace: state.trace.clone(), span: state.span.clone(), parent: state.parent.clone(), started_ms: state.started_ms, duration_us };
            drop(ext);
            let rows = self.polls.lock().map(|mut polls| polls.observe(call)).unwrap_or_else(|_| {
                self.writer.health.lost.fetch_add(1, Ordering::Relaxed); Vec::new()
            });
            for row in rows { self.record(self.poll_row(row)); }
            return;
        }
        let mut metadata=Map::with_capacity(state.fields.len()+13);
        metadata.extend(state.fields.iter().map(|(k,v)|(k.clone(),v.clone())));
        let mut value = Value::Object(metadata);
        for (k, v) in [
            ("product", json!(&*self.product)),
            ("version", json!(&*self.version)),
            ("operation", json!(state.operation)),
            ("kind", json!("method")),
            ("source", json!(if span.metadata().target() == METHOD_TARGET { "diagnostic" } else { "action" })),
            ("trace", json!(state.trace)),
            ("span", json!(state.span)),
            ("parent", json!(state.parent)),
            ("started_ms", json!(state.started_ms)),
            ("active_us", json!(state.active_us)),
            (
                "duration_us",
                json!(state.began.elapsed().as_micros().min(i64::MAX as u128) as u64),
            ),
            (
                "outcome",
                json!(if failed {
                    "failed"
                } else if state.refusal.is_some() {
                    "refused"
                } else if state.result_unknown {
                    "unknown"
                } else {
                    "ok"
                }),
            ),
            (
                "error",
                json!(if std::thread::panicking() {
                    "panic"
                } else if failed {
                    state.error_class.as_deref().unwrap_or("returned-error")
                } else if let Some(refusal) = &state.refusal {
                    refusal
                } else if state.result_unknown {
                    "result-unobserved"
                } else {
                    "none"
                }),
            ),
        ] {
            value[k] = v;
        }
        if state.repeating {
            if let Some(result)=&state.poll_state { value["to"]=json!(result); }
        }
        let result = state.poll_state.as_deref().unwrap_or_else(|| {
            if failed { state.error_class.as_deref().unwrap_or("failed") }
            else { value["outcome"].as_str().unwrap() }
        });
        if state.repeating && !state.materialized && self.fold(&value, result) { return; }
        if state.repeating && !state.materialized {
            self.record(self.start_row(state, "action"));
        }
        // Exceptions retain the actual ancestry, even when successful siblings were folded.
        drop(ext);
        for ancestor in span.parent().into_iter().flat_map(|p|p.scope()) {
            let mut ext=ancestor.extensions_mut();
            if let Some(state)=ext.get_mut::<State>().filter(|s|s.repeating && !s.materialized) {
                state.materialized=true;
                self.record(self.start_row(state,"action"));
            }
        }
        self.record(value);
    }
}

fn repeating_scope() -> (bool, Option<Value>) {
    tracing::Span::current().with_subscriber(|(id,dispatch)| {
        dispatch.downcast_ref::<tracing_subscriber::Registry>().and_then(|r|r.span(id))
            .and_then(|s|s.extensions().get::<State>().map(|s|(s.repeating,s.fields.get("project").cloned())))
    }).flatten().unwrap_or((false,None))
}

// Bounded capture-side windows reuse the existing immutable writer and its contention retry.
type RepeatKey = (String, Option<String>, String);
#[derive(Default)]
struct Repeats {
    writer: Option<Arc<Writer>>,
    last: HashMap<RepeatKey, String>,
    windows: HashMap<(RepeatKey, String), Value>,
}
impl Drop for Repeats {
    fn drop(&mut self) {
        // The final shared buffer owner drains once, including concurrent Capture clone retirement.
        for row in self.drain() {
            if let Some(writer)=&self.writer {
                match Event::new(row) {
                    Ok(event)=>writer.record(event),
                    Err(_)=>{writer.health.lost.fetch_add(1,Ordering::Relaxed);}
                }
            }
        }
    }
}
impl Repeats {
    fn drain(&mut self) -> Vec<Value> { self.windows.drain().map(|(_,v)|v).collect() }
    fn observe(&mut self, value: &Value, result: &str) -> (bool, Vec<Value>) {
        // Only fixed JS counters merge; arbitrary semantic summaries retain their validated rows.
        if value["counts"].as_object().is_some_and(|counts|counts.keys().any(|key|!matches!(key.as_str(),"calls"|"finished"|"failed"|"unknown"|"lost"|"pending"))) {return (false,Vec::new());}
        let key=(value["operation"].as_str().unwrap().to_owned(),value["project"].as_str().map(str::to_owned),value["kind"].as_str().unwrap().to_owned());
        let Some(at)=value["started_ms"].as_i64() else {return (false,Vec::new())};
        let bucket=at/POLL_INTERVAL_MS*POLL_INTERVAL_MS;
        let mut rows=Vec::new();
        self.windows.retain(|_,row| {
            if row["started_ms"].as_i64()!=Some(bucket) {rows.push(row.clone());false} else {true}
        });
        if !self.last.contains_key(&key) && self.last.len()>=POLL_KEYS || self.windows.len()>=POLL_KEYS {
            rows.extend(self.drain());self.last.clear();
        }
        let changed=self.last.insert(key.clone(),result.to_owned()).is_some_and(|old|old!=result);
        if changed || value["outcome"]!="ok" || value["source"]=="diagnostic" {return (false,rows);}
        let row=self.windows.entry((key,result.to_owned())).or_insert_with(|| {
            let mut row=json!({"product":value["product"],"version":value["version"],"operation":value["operation"],
                "kind":"method.aggregate","source":"repeating","from":result,"to":result,"trace":null,"span":null,
                "started_ms":bucket,"duration_us":0,"calls":0,"counts":{"failed":0},"outcome":"ok","error":"none",
                "unknown":{"trace":"aggregate","span":"aggregate"}});
            if !value["project"].is_null() {row["project"]=value["project"].clone();}
            row
        });
        let calls=value["calls"].as_u64().or_else(||value["counts"]["calls"].as_u64()).unwrap_or(1);
        row["calls"]=json!(row["calls"].as_u64().unwrap().saturating_add(calls));
        if let Some(counts)=value["counts"].as_object() {
            for (key,n) in counts {row["counts"][key]=json!(row["counts"][key].as_u64().unwrap_or(0).saturating_add(n.as_u64().unwrap()));}
        }
        if let (Some(total),Some(duration))=(row["duration_us"].as_u64(),value["duration_us"].as_u64()) {
            row["duration_us"]=json!(total.saturating_add(duration));
        } else {row["duration_us"]=Value::Null;row["unknown"]["duration_us"]=json!("not-measured");}
        (true,rows)
    }
}

/// Bound for unchanged poll calls folded into one row; a crash loses at most this window of counts.
const POLL_INTERVAL_MS: i64 = 15 * 60 * 1000;
// ponytail: whole-map drain at 256 keys; per-key eviction if consumers poll many distinct operations.
const POLL_KEYS: usize = 256;
struct PollCall { operation: String, project: Option<Value>, subject: u64, state: String, failed: bool, error: String,
    trace: String, span: String, parent: Option<String>, started_ms: i64, duration_us: u64 }
struct PollWindow { last: String, failed: bool, error: String, since_ms: i64, calls: u64, failures: u64, duration_us: u64 }
/// Per operation, project and subject: one change row per new result code, else one aggregate per interval.
type PollKey = (String, Option<String>, u64);
#[derive(Default)]
struct Polls { windows: HashMap<PollKey, PollWindow> }
impl Polls {
    fn observe(&mut self, call: PollCall) -> Vec<Value> {
        let key = (call.operation.clone(), call.project.as_ref().and_then(Value::as_str).map(str::to_owned), call.subject);
        let mut rows = Vec::new();
        if !self.windows.contains_key(&key) && self.windows.len() >= POLL_KEYS { rows.extend(self.drain()); self.windows.clear(); }
        match self.windows.get_mut(&key) {
            Some(window) if window.last == call.state => {
                if window.calls == 0 { window.since_ms = call.started_ms; }
                window.calls += 1; window.failures += u64::from(call.failed);
                window.duration_us = window.duration_us.saturating_add(call.duration_us);
                window.failed = call.failed; window.error = call.error;
                if call.started_ms.saturating_sub(window.since_ms) >= POLL_INTERVAL_MS { rows.extend(Self::aggregate(&key, window)); }
            }
            previous => {
                let from = previous.as_ref().map(|w| w.last.clone());
                if let Some(window) = previous { rows.extend(Self::aggregate(&key, window)); }
                let mut row = json!({"operation":call.operation,"kind":"poll.change","source":"poll","from":from,"to":call.state,
                    "trace":call.trace,"span":call.span,"parent":call.parent,"started_ms":call.started_ms,"duration_us":call.duration_us,
                    "calls":1,"outcome":if call.failed {"failed"} else {"ok"},"error":call.error});
                if from.is_none() { row["unknown"] = json!({"from":"first-poll"}); }
                if let Some(project) = call.project { row["project"] = project; }
                rows.push(row);
                self.windows.insert(key, PollWindow { last: call.state, failed: call.failed, error: call.error,
                    since_ms: call.started_ms, calls: 0, failures: 0, duration_us: 0 });
            }
        }
        rows
    }
    /// Unchanged calls since the last row; the window restarts empty.
    fn aggregate(key: &PollKey, window: &mut PollWindow) -> Option<Value> {
        if window.calls == 0 { return None; }
        let mut row = json!({"operation":key.0,"kind":"poll","source":"poll","from":window.last,"to":window.last,
            "trace":null,"span":null,"started_ms":window.since_ms,"duration_us":window.duration_us,"calls":window.calls,
            "counts":{"failed":window.failures},"outcome":if window.failed {"failed"} else {"ok"},"error":window.error});
        if let Some(project) = &key.1 { row["project"] = json!(project); }
        window.calls = 0; window.failures = 0; window.duration_us = 0;
        Some(row)
    }
    fn drain(&mut self) -> Vec<Value> {
        self.windows.iter_mut().filter_map(|(key, window)| Self::aggregate(key, window)).collect()
    }
}

/// Observe a typed return without formatting its error or wrapping the function in a closure.
pub fn returned_result<T, E>(value: Result<T, E>) -> Result<T, E> {
    if value.is_err() && observing() {
        tracing::error!(error = "returned-error");
    }
    value
}

/// A disabled internal method must not mark its enclosing action failed for a handled error.
pub fn returned_method_result<T, E>(span: &tracing::Span, value: Result<T, E>) -> Result<T, E> {
    // Use the span's owner, including older generated sites without a coreq_result field.
    span.with_subscriber(|(id,dispatch)| {
        let Some(registry)=dispatch.downcast_ref::<tracing_subscriber::Registry>() else {return};
        let Some(span)=registry.span(id) else {return};
        if let Some(state)=span.extensions_mut().get_mut::<State>() {
            state.failed |= value.is_err();
            state.result_unknown = false;
        }
    });
    value
}

/// Map generated source locations to the same original file and line, without exporting build-machine paths.
pub fn original_source(file: &str, generated_root: &str, source_base: &str) -> String {
    let file = file.replace('\\', "/");
    let root = generated_root.replace('\\', "/");
    file.strip_prefix(&(root + "/"))
        .map(|relative| format!("{source_base}/{relative}"))
        .unwrap_or(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    thread_local! { pub(super) static POLL_CLOCK: std::cell::Cell<Option<i64>> = const { std::cell::Cell::new(None) }; }

    #[test]
    fn semantic_groups_share_capture_context_and_read_only_pages() {
        use tracing_subscriber::prelude::*;
        let dir=crate::scratch("capture-semantic");let path=dir.join("db");
        let store=Arc::new(Store::open(&path).unwrap());
        let capture=Capture::new(store.clone(),"fixtureq","2").unwrap();
        let counts=(0..1800).map(|i|(format!("GET:issues.N.route{i:05}"),json!(u64::MAX))).collect::<Map<_,_>>();
        let value=json!({"product":"fixtureq","version":null,"operation":"requests.summary","kind":"requests",
            "trace":null,"span":null,"started_ms":1,"outcome":"ok","error":"none","counts":counts,
            "unknown":{"version":"not-recorded","trace":"not-recorded","span":"not-recorded"}});
        tracing::subscriber::with_default(tracing::subscriber::NoSubscriber::default(),||assert!(observe(value.clone()).is_err()));
        tracing::subscriber::with_default(tracing_subscriber::registry().with(capture.clone()),|| {
            let _action=tracing::info_span!("command").entered();
            observe(value.clone()).unwrap();
        });
        capture.flush().unwrap();
        let reader=super::super::Reader::open(&path).unwrap();
        let rows=reader.export_kinds(&["requests"],0,0,1024).unwrap();
        let parts=rows["events"].as_array().unwrap().iter().map(|row|row["observation"].clone()).collect::<Vec<_>>();
        assert!(parts.len()>1);let restored=super::super::assemble(&parts).unwrap();
        assert_eq!(restored["counts"],value["counts"]);assert_eq!(restored["version"],"2");
        assert!(restored["trace"].as_str().is_some_and(trace_id));assert!(restored["span"].as_str().is_some_and(trace_id));
        assert_eq!(capture.health()["lost"],0);drop(reader);drop(capture);drop(store);std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn semantic_queue_rejects_a_whole_group_and_counts_its_loss() {
        let (sender,_receiver)=mpsc::sync_channel(1);sender.send(Message::Stop).unwrap();
        let writer=Writer {sender,worker:Mutex::new(None),health:Arc::new(Health::default())};
        let event=Event::new(json!({"product":"fixtureq","version":"1","operation":"test","kind":"stage",
            "trace":"trace","span":"span","started_ms":1,"outcome":"ok","error":"none"})).unwrap();
        assert!(writer.enqueue(vec![event.clone(),event]).is_err());
        assert_eq!(writer.health.lost.load(Ordering::Relaxed),2);
        assert_eq!(writer.health.queued_bytes.load(Ordering::Relaxed),0);
        assert_eq!(writer.health.queued_events.load(Ordering::Relaxed),0);
        writer.health.queued_events.store(8191,Ordering::Relaxed);
        assert!(writer.reserve(1,2).is_err());
        assert_eq!(writer.health.queued_events.load(Ordering::Relaxed),8191);
        drop(_receiver);
    }

    // Risk: a WebView/remote callback is attributed to the receiver or bypasses the shared detail cap.
    #[test]
    fn linked_metadata_preserves_identity_and_shares_diagnostic_quota() {
        use tracing_subscriber::prelude::*;
        let rows=Arc::new(Mutex::new(Vec::<Value>::new()));let saved=rows.clone();
        let capture=Capture::with_append(move |events|{saved.lock().unwrap().extend(events.iter().map(|event|serde_json::from_str::<Value>(&event.payload).unwrap()));Ok(())},"fixtureq","1").unwrap();
        let subscriber=tracing_subscriber::registry().with(capture.clone());
        tracing::subscriber::with_default(subscriber,||{
            capture.diagnose("js.",Duration::from_secs(60),1).unwrap();
            let row=json!({"product":"fixtureq","version":"1","operation":"js.method","kind":"method","source":"diagnostic","trace":"external.1","span":"external.2","started_ms":1,"duration_us":1,"outcome":"ok","error":"none"});
            operation("native.receive",||{
                let origin=TraceContext::current().unwrap();
                let inherited=std::thread::spawn(bind(||TraceContext::current().unwrap().encode())).join().unwrap();
                let inherited=TraceContext::decode(&inherited).unwrap();assert_eq!(inherited.trace,origin.trace);
                observe_linked(row.clone())?;observe_linked(row)?;Ok::<_,String>(())},|_|"test").unwrap();
            let aggregate=json!({"product":"fixtureq","version":"1","operation":"js.aggregate","kind":"method.aggregate","source":"aggregate","trace":null,"span":null,"unknown":{"trace":"unlinked","span":"unlinked"},"started_ms":1,"outcome":"ok","error":"none"});
            operation("native.aggregate",||observe_linked(aggregate),|_|"test").unwrap();
        });
        capture.flush().unwrap();
        let rows=rows.lock().unwrap();let linked=rows.iter().filter(|row|row["operation"]=="js.method").collect::<Vec<_>>();
        assert_eq!(linked.len(),1);assert_eq!(linked[0]["trace"],"external.1");assert_eq!(linked[0]["span"],"external.2");
        assert!(rows.iter().find(|row|row["operation"]=="js.aggregate").unwrap()["trace"].is_null());
        assert_eq!(capture.health()["diagnostics"]["state"],"limited");assert_eq!(capture.health()["lost"],0);
    }

    // Chain: optional loss, required rejection, uncertain acknowledgment and retry remain distinct.
    #[test]
    fn required_commits_report_their_own_result_and_timeout_is_uncertain() {
        // Close the real pipe before any reporting; required store outcomes must still reach their caller.
        let Some(directory)=std::env::var_os("COREQ_CAPTURE_CLOSED_STDERR").map(std::path::PathBuf::from) else {
            let directory=std::env::temp_dir().join(format!("coreq-closed-stderr-{}",std::process::id()));
            std::fs::create_dir(&directory).unwrap();
            let mut child=std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact","telemetry::capture::tests::required_commits_report_their_own_result_and_timeout_is_uncertain","--nocapture"])
                .env("COREQ_CAPTURE_CLOSED_STDERR",&directory).env("COREQ_TRACE_CONTEXT","invalid-process-context")
                .stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).spawn().unwrap();
            drop(child.stderr.take());std::fs::write(directory.join("go"),"").unwrap();
            let until=Instant::now()+Duration::from_secs(15);
            while child.try_wait().unwrap().is_none()&&Instant::now()<until {std::thread::sleep(Duration::from_millis(10));}
            let expired=child.try_wait().unwrap().is_none();if expired {child.kill().unwrap();}
            let result=child.wait_with_output().unwrap();std::fs::remove_dir_all(directory).unwrap();
            assert!(!expired&&result.status.success(),"closed-stderr receipt chain: {result:?}");return;
        };
        let until=Instant::now()+Duration::from_secs(5);
        while !directory.join("go").exists() {assert!(Instant::now()<until,"closed-pipe fixture was not released");std::thread::sleep(Duration::from_millis(10));}
        let event=Event::new(json!({"product":"fixtureq","version":"1","operation":"resume","kind":"command.received",
            "trace":"trace","span":"span","started_ms":1,"outcome":"ok","error":"none"})).unwrap();
        let mut first=true;
        let capture=Capture::with_append(move |_| {
            if first {first=false;Err("injected store failure".into())} else {Ok(())}
        },"fixtureq","1").unwrap();
        capture.writer.record(event.clone());
        assert!(capture.flush().is_err());
        capture.commit(&event).unwrap();
        assert_eq!(capture.health()["lost"],1,"receipt success must not hide previous loss");
        assert!(capture.flush().is_err());
        drop(capture);
        let rejected=Capture::with_append(|_|Err("injected rejection".into()),"fixtureq","1").unwrap();
        assert_eq!(rejected.commit(&event).unwrap_err(),"injected rejection");drop(rejected);
        let (release,wait)=mpsc::channel();let mut first=true;
        let ids=Arc::new(Mutex::new(Vec::new()));let saved=ids.clone();
        let delayed=Capture::with_append(move |events| {
            if first {first=false;wait.recv_timeout(Duration::from_secs(10)).unwrap();}
            saved.lock().unwrap().extend(events.iter().map(|e|e.id().to_string()));Ok(())
        },"fixtureq","1").unwrap();
        let error=delayed.commit(&event).unwrap_err();
        assert!(error.contains("commit is unknown")&&error.contains("inflight 1")&&error.contains("max_append_us"),"{error}");
        release.send(()).unwrap();delayed.commit(&event).unwrap();
        assert_eq!(*ids.lock().unwrap(),[event.id(),event.id()],"uncertain retry must preserve immutable identity");
        assert_eq!(delayed.health()["queued_bytes"],0);
        let runtime=Runtime::install(&directory.join("db"),"fixtureq","1").unwrap();
        assert_eq!(runtime.capture.health()["lost"],1,"invalid inherited context must remain visible");
        assert!(qualify().is_err(),"a failed diagnostic channel must not make lost collection healthy");
        before_exec();drop(runtime);
    }

    #[test]
    fn required_commit_queue_rejection_admits_nothing() {
        let (sender,receiver)=mpsc::sync_channel(1);sender.send(Message::Stop).unwrap();
        let capture=Capture {writer:Arc::new(Writer {sender,worker:Mutex::new(None),health:Arc::new(Health::default())}),
            product:"fixtureq".into(),version:"1".into(),run:"fixture".into(),diagnostics:Arc::new(Diagnostics::default()),spans:Arc::default(),polls:Arc::default(),repeats:Arc::default()};
        let event=Event::new(json!({"product":"fixtureq","version":"1","operation":"resume","kind":"command.received",
            "trace":"trace","span":"span","started_ms":1,"outcome":"ok","error":"none"})).unwrap();
        assert_eq!(capture.commit(&event).unwrap_err(),"telemetry required record queue full");
        assert_eq!(capture.health()["accepted"],0);assert_eq!(capture.health()["queued_bytes"],0);
        drop(receiver);
        assert_eq!(capture.commit(&event).unwrap_err(),"telemetry writer stopped");
    }
    #[test]
    fn queued_observations_coalesce_without_exceeding_the_commit_byte_limit() {
        for large in [false,true] {
            let mut value=json!({"product":"fixtureq","version":"1.0.0","operation":"probe","kind":"operation",
                "trace":"trace","span":"span","started_ms":1,"outcome":"ok","error":"none"});
            if large {
                for key in super::super::TEXT.iter().filter(|key|!matches!(**key,"outcome"|"project")) {value[*key]=json!("x".repeat(128));}
                value["unknown"]=json!({});for key in super::super::NUMBERS.iter().filter(|key|**key!="started_ms") {value[*key]=Value::Null;value["unknown"][*key]=json!("x".repeat(128));}
            }
            let event=Event::new(value).unwrap();
            let groups=Arc::new(Mutex::new(Vec::new()));let observed=groups.clone();
            let (entered,ready)=mpsc::channel();let (release,wait)=mpsc::channel();let mut first=true;
            let capture=Capture::with_append(move |events| {
                if first {first=false;entered.send(()).unwrap();wait.recv_timeout(Duration::from_secs(5)).unwrap();}
                observed.lock().unwrap().push((events.len(),events.iter().map(|e|e.payload.len()).sum::<usize>()));Ok(())
            },"fixtureq","1.0.0").unwrap();
            capture.writer.record(event.clone());ready.recv_timeout(Duration::from_secs(3)).unwrap();
            let admitted = super::super::COMMIT_EVENTS.min(QUEUE_BYTES as usize / event.payload.len());
            for _ in 0..admitted {capture.writer.record(event.clone());}
            assert_eq!(capture.health()["queued_bytes"], (admitted * event.payload.len()) as u64);
            if large { capture.writer.record(event.clone()); }
            release.send(()).unwrap();assert_eq!(capture.flush().is_ok(), !large);
            let groups=groups.lock().unwrap();assert_eq!(groups.iter().map(|g|g.0).sum::<usize>(),admitted+1);
            assert!(groups.iter().all(|g|g.0<=super::super::COMMIT_EVENTS&&g.1<=super::super::COMMIT_BYTES));
            if !large {assert_eq!(groups.as_slice(),&[(1,event.payload.len()),(super::super::COMMIT_EVENTS,event.payload.len()*super::super::COMMIT_EVENTS)]);}
            else {assert!(admitted < super::super::COMMIT_EVENTS,"large events must obey the queue byte bound");}
            assert_eq!(capture.health()["lost"],u64::from(large));
            assert_eq!(capture.health()["queued_bytes"],0);
        }
    }
    #[test]
    fn append_health_measures_success_and_failure_without_losing_observations() {
        for succeeds in [true, false] {
            let capture=Capture::with_append(move |_| {
                std::thread::sleep(Duration::from_millis(2));
                if succeeds {Ok(())} else {Err("injected append failure".into())}
            },"fixtureq","1.0.0").unwrap();
            capture.record(json!({"product":"fixtureq","version":"1.0.0","operation":"probe","kind":"operation",
                "trace":"trace","span":"span","started_ms":1,"outcome":"ok","error":"none"}));
            assert_eq!(capture.flush().is_ok(),succeeds);
            let health=capture.health();
            assert_eq!(health["accepted"],1);assert_eq!(health["append_batches"],1);assert_eq!(health["inflight"],0);
            assert_eq!(health["committed"],u64::from(succeeds));assert_eq!(health["lost"],u64::from(!succeeds));
            assert!(health["append_us"].as_u64().unwrap()>=2000);
            assert_eq!(health["append_us"],health["max_append_us"]);
        }
    }

    #[test]
    fn full_queue_flush_has_a_total_deadline() {
        let (sender, receiver) = mpsc::sync_channel(1);
        sender.send(Message::Stop).unwrap();
        let capture = Capture {
            writer: Arc::new(Writer { sender, worker: Mutex::new(None), health: Arc::new(Health::default()) }),
            product: "fixtureq".into(), version: "1.0.0".into(), run: "fixture".into(),
            diagnostics: Arc::new(Diagnostics::default()), spans: Arc::default(), polls: Arc::default(), repeats: Arc::default(),
        };
        let began = Instant::now();
        let result = capture.flush();
        let elapsed = began.elapsed();
        drop(receiver);
        assert_eq!(result.unwrap_err(), "telemetry flush queue deadline");
        assert!(elapsed >= Duration::from_secs(3) && elapsed < Duration::from_secs(5));
    }

    fn rows(store: &Store) -> Vec<Value> {
        let mut out=Vec::new();let mut after=0;
        loop {
            let page=store.export(0,after,1024).unwrap();
            let events=page["events"].as_array().unwrap();
            if events.is_empty() {return out;}
            after=events.last().unwrap()["seq"].as_i64().unwrap();
            out.extend(events.iter().map(|r|r["observation"].clone()));
        }
    }

    // Defect: registry slot reuse gave sequential spans one span id. Protects A3 identity and parent links.
    #[test]
    fn span_ids_stay_unique_across_slot_reuse_nesting_threads_and_continuations() {
        let _disk=crate::bulk_disk();
        use tracing_subscriber::prelude::*;
        let dir=crate::scratch("capture-span-ids");let store=Arc::new(Store::open(&dir.join("db")).unwrap());
        let capture=Capture::new(store.clone(),"fixtureq","1").unwrap();
        let dispatch=tracing::Dispatch::new(tracing_subscriber::registry().with(capture.clone()));
        let receipt=tracing::dispatcher::with_default(&dispatch,|| {
            // 9000 spans emit 18000 rows; flush per 1000 so a slow writer never overruns the 8192-event queue.
            for i in 1..=9000 { { let _s=tracing::info_span!("sequential").entered(); } if i%1000==0 {capture.flush().unwrap();} }
            let outer=tracing::info_span!("outer").entered();
            let receipt=CommandReceipt::current().unwrap();
            let link=TraceContext::current().unwrap();
            { let _inner=tracing::info_span!("inner").entered(); }
            let threads:Vec<_>=(0..4).map(|_|std::thread::spawn(bind(||for _ in 0..25 { let _s=tracing::info_span!("concurrent").entered(); }))).collect();
            for thread in threads {thread.join().unwrap();}
            drop(outer);
            { let _c=link.span().entered(); let _child=tracing::info_span!("continued").entered(); }
            receipt
        });
        assert_eq!(receipt.checkpoint("received").is_err(),true,"no default collector outside the dispatcher");
        tracing::dispatcher::with_default(&dispatch,||{receipt.checkpoint("applied").unwrap();receipt.checkpoint("applied").unwrap();});
        capture.flush().unwrap();
        let rows=rows(&store);
        let ends:Vec<_>=rows.iter().filter(|r|r["kind"]=="method").collect();
        assert_eq!(ends.len(),9000+2+4+100+2);
        let ids:std::collections::HashSet<_>=ends.iter().map(|r|r["span"].as_str().unwrap()).collect();
        assert_eq!(ids.len(),ends.len(),"span ids repeated");
        let span=|name:&str|ends.iter().find(|r|r["operation"]==format!("fixtureq.{name}")).unwrap();
        assert_eq!(span("inner")["parent"],span("outer")["span"]);
        let continuations:Vec<_>=ends.iter().filter(|r|r["operation"]=="fixtureq.continuation").map(|r|r["span"].clone()).collect();
        assert_eq!(continuations.len(),5);
        assert!(ends.iter().filter(|r|r["operation"]=="fixtureq.continuation").all(|r|r["parent"]==span("outer")["span"]&&r["trace"]==span("outer")["trace"]));
        assert!(ends.iter().filter(|r|r["operation"]=="fixtureq.concurrent").all(|r|continuations.contains(&r["parent"])));
        assert!(continuations.contains(&span("continued")["parent"]));
        let applied:Vec<_>=rows.iter().filter(|r|r["kind"]=="command.applied").collect();
        assert_eq!(applied.len(),1,"receipt replay keeps one immutable row");assert_eq!(applied[0]["span"],span("outer")["span"]);
        drop(dispatch);drop(capture);drop(store);std::fs::remove_dir_all(dir).unwrap();
    }

    // Defect: every unchanged poll wrote start and terminal rows. Protects A1, A2 and A5.
    #[test]
    fn unchanged_polls_fold_into_changes_and_aggregates_without_changing_results() {
        use tracing_subscriber::prelude::*;
        let dir=crate::scratch("capture-polls");let store=Arc::new(Store::open(&dir.join("db")).unwrap());
        let capture=Capture::new(store.clone(),"fixtureq","1").unwrap();
        let states=["present","present","present","missing","missing","denied","present","present"];
        let mut runs=0;
        tracing::subscriber::with_default(tracing_subscriber::registry().with(capture.clone()),|| {
            let _pass=tracing::info_span!("pass",project="owner/repo").entered();
            for state in states {
                let result=poll("check",0,||{runs+=1;if state=="denied" {Err("denied")} else {Ok(state)}},|r|match r {Ok(s)=>s,Err(_)=>"denied"});
                assert_eq!(result,if state=="denied" {Err("denied")} else {Ok(state)},"poll changed the caller's result");
            }
            poll("nested",0,||{let _child=tracing::info_span!("child").entered();Ok::<_,()>(())},|_|"ok").unwrap();
            checkpoint("applied").unwrap();
        });
        assert_eq!(runs,states.len(),"native check skipped");
        let unobserved=tracing::subscriber::with_default(tracing::subscriber::NoSubscriber::default(),||poll("check",0,||Err::<(),_>("x"),|_|"failed"));
        assert_eq!(unobserved,Err("x"),"absent collection changed the result");
        capture.drain_polls();capture.flush().unwrap();
        let rows=rows(&store);
        let check:Vec<_>=rows.iter().filter(|r|r["operation"]=="fixtureq.operation.check").collect();
        assert!(check.iter().all(|r|r["kind"]=="poll.change"||r["kind"]=="poll"),"per-poll rows remain");
        let changes:Vec<_>=check.iter().filter(|r|r["kind"]=="poll.change").map(|r|(r["from"].clone(),r["to"].as_str().unwrap())).collect();
        assert_eq!(changes,[(Value::Null,"present"),(json!("present"),"missing"),(json!("missing"),"denied"),(json!("denied"),"present")]);
        assert_eq!(check.iter().map(|r|r["calls"].as_u64().unwrap()).sum::<u64>(),states.len() as u64,"poll counts lost");
        let denied=check.iter().find(|r|r["to"]=="denied").unwrap();assert_eq!(denied["outcome"],"failed");assert_eq!(denied["error"],"denied");
        assert!(check.iter().all(|r|r["project"]=="owner/repo"));
        let pass=rows.iter().find(|r|r["operation"]=="fixtureq.pass"&&r["kind"]=="method").unwrap();
        assert!(check.iter().filter(|r|r["kind"]=="poll.change").all(|r|r["parent"]==pass["span"]&&r["trace"]==pass["trace"]));
        let nested:Vec<_>=rows.iter().filter(|r|r["operation"]=="fixtureq.operation.nested").collect();
        assert_eq!(nested.iter().map(|r|r["kind"].as_str().unwrap()).collect::<Vec<_>>(),["method.start","method"],"a poll with a child keeps its rows");
        let child=rows.iter().find(|r|r["operation"]=="fixtureq.child"&&r["kind"]=="method").unwrap();assert_eq!(child["parent"],nested[0]["span"]);
        assert_eq!(rows.iter().filter(|r|r["kind"]=="command.applied").count(),1,"required receipt lost under polling");
        assert_eq!(capture.health()["lost"],0);
        drop(capture);drop(store);std::fs::remove_dir_all(dir).unwrap();
    }

    // #63 A1/A3: repeated roots must suppress steady rows without losing failure ancestry.
    #[test]
    fn repeating_roots_keep_exceptions_and_fold_steady_calls() {
        use tracing_subscriber::prelude::*;
        let dir=crate::scratch("repeating-roots");
        let store=Arc::new(Store::open(&dir.join("db")).unwrap());
        let capture=Capture::new(store.clone(),"fixtureq","1").unwrap();
        tracing::subscriber::with_default(tracing_subscriber::registry().with(capture.clone()),|| {
            for code in ["ready","ready","changed","changed"] {
                let _root=tracing::info_span!("refresh",coreq_repeating=true,project="owner/repo").entered();
                let _child=tracing::info_span!("read",coreq_state=code).entered();
            }
            {let _root=tracing::info_span!("refresh",coreq_repeating=true,project="owner/repo").entered();
                operation("failure",||Err::<(),_>(()),|_|"denied").unwrap_err();}
            {let _root=tracing::info_span!("semantic-refresh",coreq_repeating=true).entered();
                observe(json!({"product":"fixtureq","version":"1","operation":"js.failure","kind":"method.aggregate",
                    "trace":"external","span":"external","started_ms":1,"outcome":"failed","error":"javascript-error","counts":{"calls":1,"failed":1}})).unwrap();}
            {let _root=tracing::info_span!("unknown-refresh",coreq_repeating=true).entered();
                let _unknown=tracing::info_span!("unknown-child",coreq_result="unclassified").entered();}
            {let _outside=tracing::info_span!("outside").entered();}
        });
        capture.finish().unwrap();
        let rows=rows(&store);
        assert_eq!(rows.iter().filter(|r|r["source"]=="repeating"&&r["operation"]=="fixtureq.read").map(|r|r["calls"].as_u64().unwrap()).sum::<u64>(),3);
        assert_eq!(rows.iter().filter(|r|r["operation"]=="fixtureq.read"&&r["kind"]=="method").count(),1,"steady calls still write per-call rows");
        assert!(rows.iter().any(|r|r["operation"]=="fixtureq.read"&&r["kind"]=="method"&&r["to"]=="changed"));
        let failure=rows.iter().find(|r|r["outcome"]=="failed").unwrap();
        assert!(rows.iter().any(|r|r["kind"]=="method"&&r["span"]==failure["parent"]&&r["trace"]==failure["trace"]));
        assert_eq!(rows.iter().filter(|r|r["operation"]=="fixtureq.outside").count(),2);
        let semantic=rows.iter().find(|r|r["operation"]=="js.failure").unwrap();
        assert_eq!(semantic["outcome"],"failed");
        assert!(rows.iter().any(|r|r["operation"]=="fixtureq.semantic-refresh"&&r["kind"]=="method"&&r["span"]==semantic["span"]));
        assert!(rows.iter().any(|r|r["operation"]=="fixtureq.unknown-child"&&r["kind"]=="method"&&r["outcome"]=="unknown"));
        assert_eq!(capture.health()["lost"],0);
        drop(capture);drop(store);std::fs::remove_dir_all(dir).unwrap();
    }

    // #63 A2/A4/A5: actual capture -> validated JS ingestion -> bounded writer -> reopened store.
    // The unmarked control must fail the same physical growth gate; pruning cannot hide it.
    #[test]
    fn repeating_six_project_hour_fits_budget_and_preserves_shutdown_counts() {
        let _disk=crate::bulk_disk();
        use tracing_subscriber::prelude::*;
        struct ResetClock;
        impl Drop for ResetClock {fn drop(&mut self) {POLL_CLOCK.with(|clock|clock.set(None));}}
        let _reset=ResetClock;
        for folded in [false,true] {
            let dir=crate::scratch("repeating-hour");let path=dir.join("db");
            let budget=super::super::Budget {lock:dir.join("installation.lock"),peers:vec![],total:1<<30,reserved:64<<20};
            let store=Arc::new(Store::open_bounded(&path,budget.clone()).unwrap());
            store.reclaim().unwrap();let empty=store.storage().unwrap()["total_bytes"].as_u64().unwrap();
            let capture=Capture::new(store.clone(),"fixtureq","1").unwrap();
            let dispatch=tracing::Dispatch::new(tracing_subscriber::registry().with(capture.clone()));
            let start=clock().unwrap()/POLL_INTERVAL_MS*POLL_INTERVAL_MS;
            tracing::dispatcher::with_default(&dispatch,|| {
                for tick in 0..1800 {
                    let at=start+tick*2000;POLL_CLOCK.with(|c|c.set(Some(at)));
                    for project in 0..6 {
                        let project=format!("owner/project{project}");
                        if tick%30==0 {
                            let _pass=tracing::info_span!("pass",coreq_repeating=folded,project=project.as_str()).entered();
                            for _ in 0..4 {operation("board.read",||Ok::<_,()>(()),|_|"failed").unwrap();}
                        }
                        let _refresh=tracing::info_span!("refresh",coreq_repeating=folded,project=project.as_str()).entered();
                        operation("render",||Ok::<_,()>(()),|_|"failed").unwrap();
                        poll("identity",0,||Ok::<_,()>(()),|_|"present").unwrap();
                        poll("status",0,||Ok::<_,()>(()),|_|"ready").unwrap();
                        for kind in ["method.aggregate","collection"] {
                            let value=json!({"product":"fixtureq","version":"1","operation":if kind=="collection" {"js.collector.health"} else {"js.render"},
                                "kind":kind,"trace":"external","span":"external","started_ms":at,
                                "counts":if kind=="collection" {json!({"lost":0,"pending":0})} else {json!({"calls":3,"finished":3,"failed":0,"unknown":0})},
                                "outcome":"ok","error":"none"});
                            if tick%2==0 {observe(value).unwrap();} else {observe_linked(value).unwrap();}
                        }
                    }
                    // A5 measures growth, not disk latency: one Windows FULL append can
                    // outlast flush's 3s acknowledgment. Wait boundedly without replaying calls.
                    let until=Instant::now()+Duration::from_secs(30);
                    loop {
                        let result=capture.flush();
                        let health=capture.health();
                        assert_eq!(health["lost"],0,"{health}");
                        assert_eq!(health["failed_batches"],0,"{health}");
                        match result {
                            Ok(()) => break,
                            Err(error) if error.starts_with("telemetry flush unacknowledged (") && Instant::now()<until => continue,
                            Err(error) => panic!("hour fixture drain failed: {error}"),
                        }
                    }
                }
            });
            capture.flush().unwrap();
            let health=capture.health();
            assert_eq!(health["lost"],0);
            assert_eq!(health["failed_batches"],0);
            assert_eq!(health["accepted"],health["committed"]);
            drop(dispatch);drop(capture);drop(store); // last Capture drop must persist the partial window
            let store=Store::open_bounded(&path,budget).unwrap();store.reclaim().unwrap();
            let bytes=store.storage().unwrap()["total_bytes"].as_u64().unwrap()-empty;
            let limit=384*1024*1024/720;
            eprintln!("repeating hour folded={folded}: {bytes} bytes; limit={limit}");
            assert_eq!(bytes<=limit,folded,"hourly growth gate must reject unfolded capture");
            assert_eq!(store.storage().unwrap()["pruned_count"],0);
            if folded {
                let rows=rows(&store);
                assert!(rows.iter().all(|r|r["kind"]=="method.aggregate"));
                for project in 0..6 {
                    for (op,calls) in [("fixtureq.pass",60),("fixtureq.operation.board.read",240),("fixtureq.refresh",1800),
                        ("fixtureq.operation.render",1800),("fixtureq.operation.identity",1800),("fixtureq.operation.status",1800),("js.render",5400),("js.collector.health",1800)] {
                        let rows:Vec<_>=rows.iter().filter(|r|r["project"]==format!("owner/project{project}")&&r["operation"]==op).collect();
                        assert_eq!(rows.len(),4,"one aggregate per UTC window for {op}");
                        assert_eq!(rows.iter().map(|r|r["calls"].as_u64().unwrap()).sum::<u64>(),calls,"shutdown lost counts for {op}");
                        assert!(rows.iter().all(|r|r["counts"]["failed"]==0));
                        if op.starts_with("fixtureq.") {assert!(rows.iter().all(|r|r["duration_us"].is_u64()));}
                        else {assert!(rows.iter().all(|r|r["duration_us"].is_null()&&r["unknown"]["duration_us"]=="not-measured"));}
                    }
                }
            }
            drop(store);std::fs::remove_dir_all(dir).unwrap();
        }
    }

    // A3: a real budget-lock timeout must reach append before release; a reader's store mutex cannot mask it.
    // Retains exact batch ids, zero loss/failures, concurrent read and maintenance coverage.
    #[test]
    fn budget_lock_contention_delays_but_never_loses_a_batch() {
        let dir=crate::scratch("capture-budget-contention");
        let budget=super::super::Budget {lock:dir.join("installation.lock"),peers:vec![],total:8<<20,reserved:1<<20};
        let path=dir.join("db");
        let store=Arc::new(Store::open_bounded(&path,budget.clone()).unwrap());
        let peer=Store::open_bounded(&path,budget.clone()).unwrap();
        let (blocked_tx,blocked_rx)=mpsc::channel();
        let capture=Capture::with_append({let store=store.clone();let mut retry_ids=None;move |events| {
            let ids:Vec<_>=events.iter().map(|event|event.id.clone()).collect();
            if let Some(previous)=&retry_ids {assert_eq!(&ids,previous,"retry changed immutable batch ids");}
            let result=store.append_group(events).map(|_|());
            if result.as_ref().is_err_and(|e|super::super::failure_code(e)=="budget-locked") {
                retry_ids=Some(ids);blocked_tx.send(()).unwrap();
            } else if result.is_ok() {retry_ids=None;}
            result
        }},"fixtureq","1").unwrap();
        capture.flush().unwrap();
        let held=budget.acquire().unwrap();
        for i in 0..64 {capture.record(json!({"product":"fixtureq","version":"1","operation":"fixtureq.contended","kind":"method","trace":"t","span":format!("s{i}"),"started_ms":1,"outcome":"ok","error":"none"}));}
        blocked_rx.recv_timeout(Duration::from_secs(5)).expect("append must encounter real budget-locked before release");
        // Separate connection/mutex, like a different process. Start only after the proven append timeout.
        let reader=std::thread::spawn(move|| {
            peer.storage().unwrap();
            peer.reclaim().unwrap();
            peer.export(0,0,1024).unwrap();
        });
        drop(held);
        capture.flush().unwrap();reader.join().unwrap();
        let health=capture.health();
        assert_eq!(health["lost"],0);assert_eq!(health["failed_batches"],0);
        assert_eq!(health["accepted"],health["committed"]);
        let page=store.export_kinds(&["method"],0,0,1024).unwrap();
        assert_eq!(page["events"].as_array().unwrap().len(),64);
        drop(capture);drop(store);std::fs::remove_dir_all(dir).unwrap();
    }

    // A1/A2: the old model-only check missed Capture/adapter wiring. Exercise all 54,000 real calls,
    // changing only the span UTC clock; preserve return values, interval counts, disk and queue bounds.
    #[test]
    fn simulated_six_project_polling_fits_its_share_of_the_budget() {
        let _disk=crate::bulk_disk();
        use tracing_subscriber::prelude::*;
        struct ResetClock;
        impl Drop for ResetClock {fn drop(&mut self) {POLL_CLOCK.with(|clock|clock.set(None));}}
        let _reset=ResetClock;
        let dir=crate::scratch("capture-native-poll-hour");
        let budget=super::super::Budget {lock:dir.join("installation.lock"),peers:vec![],total:1<<30,reserved:64<<20};
        let store=Arc::new(Store::open_bounded(&dir.join("db"),budget).unwrap());
        let bytes=|| {store.reclaim().unwrap();store.storage().unwrap()["total_bytes"].as_u64().unwrap()};
        let empty=bytes();
        let capture=Capture::new(store.clone(),"fixtureq","1").unwrap();
        let dispatch=tracing::Dispatch::new(tracing_subscriber::registry().with(capture.clone()));
        let tool=crate::tool(&dir);
        // Reap a native child so the missing-PID adapter path is real on every target OS.
        let mut child=std::process::Command::new(std::env::current_exe().unwrap()).arg("--list")
            .stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).spawn().unwrap();
        let gone=child.id();assert!(child.wait().unwrap().success());
        let live=crate::process::identity(std::process::id()).unwrap().unwrap();
        let start=clock().unwrap();
        tracing::dispatcher::with_default(&dispatch,|| {
            let projects:Vec<_>=(0..6).map(|i|tracing::info_span!("project",project=format!("owner/project{i}"))).collect();
            for tick in 0..1800 {
                POLL_CLOCK.with(|clock|clock.set(Some(start+tick*2000)));
                for (i,project) in projects.iter().enumerate() {
                    let _entered=project.enter();
                    for _ in 0..2 {assert_eq!(crate::process::identity(live.pid).unwrap(),Some(live.clone()));}
                    assert_eq!(crate::process::identity(gone).unwrap(),None);
                    assert_eq!(crate::process::identity(0).unwrap_err().kind(),std::io::ErrorKind::InvalidInput);
                    let root=dir.join(format!("project{i}"));
                    let status=crate::status::common(&tool,"fixture",&format!("project{i}"),tick as u64,None,None);
                    crate::status::write(&tool,&root,&status).unwrap();
                    assert_eq!(crate::status::read(&tool,&root,60),Some(status),"status adapter skipped or changed the write");
                }
                // Bound admission even when mutation testing restores per-call rows.
                if tick%20==0 {capture.flush().unwrap();}
            }
        });
        capture.drain_polls();capture.flush().unwrap();
        let health=capture.health();assert_eq!(health["lost"],0);assert_eq!(health["failed_batches"],0);
        assert_eq!(health["accepted"],health["committed"]);
        let after=bytes()-empty;let limit=384*1024*1024/720;
        eprintln!("native six-project hour: 54000 calls, growth {after} bytes <= {limit}");
        assert!(after<=limit,"per-call telemetry exceeds hourly share: {after} > {limit}");
        let page=store.export(0,0,1024).unwrap();
        let rows:Vec<_>=page["events"].as_array().unwrap().iter().map(|r|&r["observation"])
            .filter(|r|r["operation"]=="fixtureq.operation.process.identity"||r["operation"]=="fixtureq.operation.status.write").collect();
        assert!(rows.iter().all(|r|r["kind"]=="poll"||r["kind"]=="poll.change"));
        assert_eq!(rows.iter().filter(|r|r["kind"]=="poll.change").count(),24);
        assert_eq!(rows.iter().filter(|r|r["kind"]=="poll").count(),96,"four interval aggregates per key");
        for i in 0..6 {
            let project:Vec<_>=rows.iter().filter(|r|r["project"]==format!("owner/project{i}")).collect();
            assert_eq!(project.iter().map(|r|r["calls"].as_u64().unwrap()).sum::<u64>(),9000);
            assert_eq!(project.iter().filter(|r|r["kind"]=="poll.change"&&r["outcome"]=="failed").count(),1);
        }
        assert_eq!(store.storage().unwrap()["pruned_count"],0,"pruning must not hide growth");
        drop(dispatch);drop(capture);drop(store);std::fs::remove_dir_all(dir).unwrap();
    }
}
