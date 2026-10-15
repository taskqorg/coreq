//! One metadata-only tracing layer and bounded group-commit writer for every consumer.
use super::{Event, Store, clock, code};
use serde_json::{Map, Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
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
        match self.sender.try_send(Message::Record(event)) {
            Ok(()) => {}
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
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
}
impl Capture {
    pub fn new(store: Arc<Store>, product: &str, version: &str) -> Result<Self, String> {
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
                let mut pending = Vec::with_capacity(1024);
                let mut last_error = None;
                let commit = |batch: &mut Vec<Event>, last_error: &mut Option<String>| {
                    if batch.is_empty() {
                        return;
                    }
                    match store.append(batch) {
                        Ok(_) => {
                            state
                                .committed
                                .fetch_add(batch.len() as u64, Ordering::Relaxed);
                        }
                        Err(e) => {
                            state.failures.fetch_add(1, Ordering::Relaxed);
                            state.lost.fetch_add(batch.len() as u64, Ordering::Relaxed);
                            if last_error.is_none() {
                                eprintln!("Telemetry: unknown (durable collection failed)");
                            }
                            *last_error = Some(e);
                        }
                    }
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
                            pending.push(event);
                            if pending.len() == 1 {
                                deadline = Instant::now() + Duration::from_millis(5);
                            }
                            if pending.len() == 1024 {
                                commit(&mut pending, &mut last_error);
                            }
                        }
                        Ok(Message::Flush(reply)) => {
                            commit(&mut pending, &mut last_error);
                            let _ = reply.send(last_error.clone().map_or(Ok(()), Err));
                        }
                        Ok(Message::Stop) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                            commit(&mut pending, &mut last_error);
                            break;
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            commit(&mut pending, &mut last_error)
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
        })
    }
    pub fn flush(&self) -> Result<(), String> {
        let (sender, receiver) = mpsc::channel();
        self.writer
            .sender
            .send(Message::Flush(sender))
            .map_err(|_| "telemetry writer stopped")?;
        receiver
            .recv_timeout(Duration::from_secs(3))
            .map_err(|_| "telemetry flush unacknowledged")??;
        if self.writer.health.lost.load(Ordering::Relaxed) != 0 {
            return Err("telemetry collection incomplete; observations lost".into());
        }
        Ok(())
    }
    pub fn health(&self) -> Value {
        let h = &self.writer.health;
        let lost = h.lost.load(Ordering::Relaxed);
        let failures = h.failures.load(Ordering::Relaxed);
        json!({"state":if lost==0&&failures==0 {"collecting"} else {"unknown"},"accepted":h.accepted.load(Ordering::Relaxed),"committed":h.committed.load(Ordering::Relaxed),"lost":lost,"failed_batches":failures})
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
    result_unknown: bool,
    active_us: u64,
    entered: HashMap<std::thread::ThreadId, (usize, Instant)>,
}
#[derive(Default)]
struct Fields {
    values: Map<String, Value>,
    error: bool,
    result_unknown: Option<bool>,
}
impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, _value: &dyn std::fmt::Debug) {
        if field.name() == "error" {
            self.error = true;
        }
    }
    fn record_str(&mut self, field: &Field, value: &str) {
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
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let parent = span.parent();
        let (trace, parent_id, mut inherited) = parent
            .as_ref()
            .and_then(|p| {
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
            })
            .unwrap_or_else(|| (format!("{}.{}", self.run, id.into_u64()), None, Map::new()));
        let mut fields = Fields::default();
        attrs.record(&mut fields);
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
            failed: false,
            result_unknown: fields.result_unknown.unwrap_or(false),
            active_us: 0,
            entered: HashMap::new(),
        });
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
        let name = span.metadata().name();
        let operation = format!("{}.{}", self.product, name);
        let operation = if operation.len() <= 128 && code(&operation) {
            operation
        } else {
            format!("method.{}", crate::sha256_hex(operation.as_bytes()))
        };
        let failed = state.failed || std::thread::panicking();
        let mut value = Value::Object(state.fields.clone());
        for (k, v) in [
            ("product", json!(&*self.product)),
            ("version", json!(&*self.version)),
            ("operation", json!(operation)),
            ("kind", json!("method")),
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
                    "returned-error"
                } else if state.result_unknown {
                    "result-unobserved"
                } else {
                    "none"
                }),
            ),
        ] {
            value[k] = v;
        }
        match Event::new(value) {
            Ok(event) => self.writer.record(event),
            Err(_) => {
                self.writer.health.lost.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Observe a typed return without formatting its error or wrapping the function in a closure.
pub fn returned_result<T, E>(value: Result<T, E>) -> Result<T, E> {
    if value.is_err() {
        tracing::error!(error = "returned-error");
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
