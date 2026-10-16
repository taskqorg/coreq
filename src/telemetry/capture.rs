//! One metadata-only tracing layer and bounded group-commit writer for every consumer.
use super::{Event, Store, clock, code};
use serde_json::{Map, Value, json};
use std::{
    collections::HashMap,
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
        let selected = s.selector == "*" || name == s.selector
            || name.strip_prefix(&s.selector).is_some_and(|rest| rest.starts_with('.'));
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
        tracing::info_span!(parent: None, "continuation", coreq_trace = self.trace.as_str(), coreq_parent = self.parent.as_str())
    }
}
fn trace_id(text: &str) -> bool {
    text.split_once('.').is_some_and(|(run, span)| super::hex(run, 32)
        && span.parse::<u64>().is_ok_and(|id| id != 0 && id.to_string() == span))
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
        use tracing_subscriber::prelude::*;
        if GLOBAL.get().is_some() { return Err("telemetry already initialized".into()); }
        let mut sink=super::broker::open(path)?;
        let capture = Capture::with_append(move |events|sink.append(events), product, version)?;
        tracing::subscriber::set_global_default(tracing_subscriber::registry().with(capture.clone()))
            .map_err(|_| "telemetry global subscriber unavailable")?;
        GLOBAL.set(capture.clone()).map_err(|_| "telemetry already initialized")?;
        let continuation = std::env::var_os("COREQ_TRACE_CONTEXT").and_then(|value| {
            match value.to_str().and_then(|text| TraceContext::decode(text).ok()) {
                Some(link) => Some(link.span().entered()),
                None => {
                    capture.writer.health.lost.fetch_add(1, Ordering::Relaxed);
                    eprintln!("Telemetry: unknown (invalid process context)");
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

/// Observe a dependency result before its caller can handle it; the adapter owns its deadline.
pub fn operation<T, E>(name: &'static str, action: impl FnOnce() -> Result<T, E>, classify: impl FnOnce(&E) -> &'static str) -> Result<T, E> {
    let span = tracing::info_span!("operation", coreq_operation = name, coreq_result = "unclassified", coreq_error = tracing::field::Empty);
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
    if let Some(link) = TraceContext::current() {
        tracing::info_span!(parent: None, "operation", coreq_operation=name, coreq_trace=link.trace.as_str(), coreq_parent=link.parent.as_str(),
            coreq_result="unclassified", coreq_error=tracing::field::Empty, process_id=tracing::field::Empty)
    } else {
        tracing::info_span!(parent: None, "operation", coreq_operation=name, coreq_result="unclassified", coreq_error=tracing::field::Empty, process_id=tracing::field::Empty)
    }
}
fn report_flush(capture: &Capture) {
    if let Err(error)=capture.flush() { eprintln!("Telemetry: unknown (flush: {})",super::failure_code(&error)); }
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
    Record(Event),
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
    fn record(&self, event: Event) {
        self.health.accepted.fetch_add(1, Ordering::Relaxed);
        let bytes = event.payload.len() as u64;
        if self.health.queued_bytes.fetch_update(Ordering::Relaxed, Ordering::Relaxed,
            |n| n.checked_add(bytes).filter(|total| *total <= QUEUE_BYTES)).is_err() {
            if self.health.lost.fetch_add(1, Ordering::Relaxed) == 0 {
                eprintln!("Telemetry: unknown (collection byte budget exhausted; observations lost)");
            }
            return;
        }
        match self.sender.try_send(Message::Record(event)) {
            Ok(()) => {}
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                self.health.queued_bytes.fetch_sub(bytes, Ordering::Relaxed);
                if self.health.lost.fetch_add(1, Ordering::Relaxed) == 0 {
                    eprintln!(
                        "Telemetry: unknown (collection queue unavailable; observations lost)"
                    );
                }
            }
        }
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
                let mut commit = |batch: &mut Vec<Event>, last_error: &mut Option<String>| {
                    if batch.is_empty() {
                        return;
                    }
                    state.inflight.store(batch.len() as u64, Ordering::Relaxed);
                    let began = Instant::now();
                    match append(batch) {
                        Ok(_) => {
                            state
                                .committed
                                .fetch_add(batch.len() as u64, Ordering::Relaxed);
                        }
                        Err(e) => {
                            state.failures.fetch_add(1, Ordering::Relaxed);
                            state.lost.fetch_add(batch.len() as u64, Ordering::Relaxed);
                            if last_error.is_none() {
                                eprintln!("Telemetry: unknown ({})",super::failure_code(&e));
                            }
                            *last_error = Some(e);
                        }
                    }
                    let elapsed = began.elapsed().as_micros().min(u64::MAX as u128) as u64;
                    state.append_us.fetch_add(elapsed, Ordering::Relaxed);
                    state.max_append_us.fetch_max(elapsed, Ordering::Relaxed);
                    state.batches.fetch_add(1, Ordering::Relaxed);
                    state.inflight.store(0, Ordering::Relaxed);
                    batch.clear();
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
                        Ok(Message::Record(event)) => {
                            state.queued_bytes.fetch_sub(event.payload.len() as u64, Ordering::Relaxed);
                            if pending_bytes+event.payload.len()>super::COMMIT_BYTES {
                                commit(&mut pending,&mut last_error);pending_bytes=0;
                            }
                            pending_bytes+=event.payload.len();
                            pending.push(event);
                            if pending.len() == 1 {
                                deadline = Instant::now() + Duration::from_millis(5);
                            }
                            if pending.len() == super::COMMIT_EVENTS {
                                commit(&mut pending, &mut last_error);
                                pending_bytes=0;
                            }
                        }
                        Ok(Message::Flush(reply)) => {
                            commit(&mut pending, &mut last_error);
                            pending_bytes=0;
                            let _ = reply.send(last_error.clone().map_or(Ok(()), Err));
                        }
                        Ok(Message::Stop) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                            commit(&mut pending, &mut last_error);
                            break;
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            commit(&mut pending, &mut last_error);pending_bytes=0;
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
    pub fn flush(&self) -> Result<(), String> {
        let diagnostic = self.diagnostics.health();
        if matches!(diagnostic["state"].as_str(), Some("limited" | "expired")) {
            tracing::callsite::rebuild_interest_cache();
        }
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
            "queued_bytes":h.queued_bytes.load(Ordering::Relaxed),"queue_bytes_limit":QUEUE_BYTES,"diagnostics":self.diagnostics.health()})
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
    if value.is_err() {
        tracing::error!(error = "returned-error");
    }
    value
}

/// A disabled internal method must not mark its enclosing action failed for a handled error.
pub fn returned_method_result<T, E>(span: &tracing::Span, value: Result<T, E>) -> Result<T, E> {
    if !span.is_disabled() {
        span.record("coreq_result", "observed");
        if value.is_err() { tracing::error!(parent: span, error = "returned-error"); }
    }
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
