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
const QUEUE_BYTES: u64 = 16 * 1024 * 1024;
const METHOD_TARGET: &str = "coreq::method";

fn observing() -> bool {
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
        let mut sink=super::broker::open_with_budget(path,budget)?;
        let capture = Capture::with_append(move |events|sink.append(events), product, version)?;
        tracing::subscriber::set_global_default(tracing_subscriber::registry().with(capture.clone()))
            .map_err(|_| "telemetry global subscriber unavailable")?;
        GLOBAL.set(capture.clone()).map_err(|_| "telemetry already initialized")?;
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
    if health["committed"].as_u64().unwrap_or(0)==0 {return Err("telemetry no committed observations".into());}
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
    if let Err(error)=capture.flush() { let _=writeln!(std::io::stderr(),"Telemetry: unknown (flush: {})",super::failure_code(&error)); }
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
                    let result = append(batch);
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
        Ok(Self {
            writer: Arc::new(Writer {
                sender,
                worker: Mutex::new(Some(worker)),
                health,
            }),
            product: product.into(),
            version: version.into(),
            run: seed.id().into(),
            diagnostics: Arc::new(Diagnostics::default()),
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
            Event::split(value)
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
}
impl Visit for Fields {
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
            .unwrap_or_else(|| (format!("{}.{}", self.run, id.into_u64()), None, Map::new()));
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
        span.extensions_mut().insert(State {
            started_ms,
            began: Instant::now(),
            trace,
            span: format!("{}.{}", self.run, id.into_u64()),
            parent: parent_id,
            fields: inherited,
            failed: fields.error,
            error_class: fields.error_class,
            result_unknown: fields.result_unknown.unwrap_or(false),
            refusal: fields.refusal,
            operation: self.operation(&fields.operation.map_or_else(||span.metadata().name().to_string(), |suffix|format!("{}.{suffix}",span.metadata().name()))),
            active_us: 0,
            entered: HashMap::new(),
        });
        let ext = span.extensions();
        let state = ext.get::<State>().expect("inserted span state");
        let mut value = Value::Object(state.fields.clone());
        for (key, field) in [
            ("product", json!(&*self.product)),
            ("version", json!(&*self.version)),
            ("operation", json!(state.operation)),
            ("kind", json!("method.start")),
            ("source", json!(if span.metadata().target() == METHOD_TARGET { "diagnostic" } else { "action" })),
            ("trace", json!(state.trace)),
            ("span", json!(state.span)),
            ("parent", json!(state.parent)),
            ("started_ms", json!(state.started_ms)),
            ("duration_us", Value::Null),
            ("unknown", json!({"duration_us":"span-open"})),
            ("outcome", json!("unknown")),
            ("error", json!("span-open")),
        ] { value[key] = field; }
        self.record(value);

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
        let mut value = Value::Object(state.fields.clone());
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
        self.record(value);
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
            product:"fixtureq".into(),version:"1".into(),run:"fixture".into(),diagnostics:Arc::new(Diagnostics::default())};
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
            diagnostics: Arc::new(Diagnostics::default()),
        };
        let began = Instant::now();
        let result = capture.flush();
        let elapsed = began.elapsed();
        drop(receiver);
        assert_eq!(result.unwrap_err(), "telemetry flush queue deadline");
        assert!(elapsed >= Duration::from_secs(3) && elapsed < Duration::from_secs(5));
    }
}
