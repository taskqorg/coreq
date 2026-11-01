//! App-server protocol state over caller-owned transport. UI, tool execution and approvals stay with the caller.
use super::*;
use std::sync::mpsc::{self, Receiver, SyncSender, RecvTimeoutError};

type WriteRequest=(Vec<u8>,mpsc::Sender<Res<()>>);

/// Shared native stdio transport. Dropping the handle does not kill an active session; callers own its lifecycle.
pub struct AppServer {
    child:Child,
    input:SyncSender<WriteRequest>,
    events:Receiver<Res<Value>>,
}

impl AppServer {
    pub fn start(base:Command,spec:&Spec,private_log:&Path)->Res<Self> {
        let mut options=File::options();options.create(true).append(true);
        #[cfg(unix)] {use std::os::unix::fs::OpenOptionsExt;options.mode(0o600);}
        let log=options.open(private_log).map_err(|e|format!("app-server log: {e}"))?;
        let mut diagnostics=log.try_clone().map_err(|e|format!("app-server log: {e}"))?;
        let mut command=app_server_command(base,spec)?;
        crate::telemetry::capture::inherit_context(&mut command);
        let mut child=command.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(log).spawn().map_err(|e|format!("app-server spawn: {e}"))?;
        let mut stdin=child.stdin.take().ok_or("app-server stdin unavailable")?;
        let stdout=child.stdout.take().ok_or("app-server stdout unavailable")?;
        let (input,writing)=mpsc::sync_channel::<WriteRequest>(1);
        std::thread::spawn(move || {
            while let Ok((bytes,receipt))=writing.recv() {
                let result=stdin.write_all(&bytes).and_then(|_|stdin.flush()).map_err(|e|format!("app-server write outcome unknown: {e}"));
                let failed=result.is_err();let _=receipt.send(result);
                if failed {break;}
            }
        });
        let (output,events)=mpsc::sync_channel(16);
        std::thread::spawn(move || {
            let mut reader=BufReader::new(stdout);
            loop {
                let mut line=String::new();
                let result=(&mut reader).take(8*1024*1024+1).read_line(&mut line);
                let event=match result {
                    Ok(0)=>break,
                    Ok(_) if line.len()>8*1024*1024 || !line.ends_with('\n')=>Err("app-server record incomplete or exceeds 8 MiB".into()),
                    Ok(_) if !line.trim_start().starts_with(['{','['])=>{
                        if let Err(e)=diagnostics.write_all(line.as_bytes()) {let _=output.send(Err(format!("app-server diagnostic log: {e}")));break;}
                        continue;
                    },
                    Ok(_)=>serde_json::from_str::<Value>(&line).map_err(|_|"app-server record invalid; native state unknown".into())
                        .and_then(|v|if v.is_object() {Ok(v)} else {Err("app-server record shape invalid; native state unknown".into())}),
                    Err(e)=>Err(format!("app-server read failed: {e}")),
                };
                let failed=event.is_err();
                if output.send(event).is_err() || failed {break;}
            }
        });
        Ok(Self {child,input,events})
    }

    pub fn id(&self)->u32 {self.child.id()}

    /// A timeout may follow partial native delivery. Retain the original request and reconcile; never resend it.
    pub fn send(&mut self,message:&Value,until:Instant)->Res<()> {
        let mut bytes=serde_json::to_vec(message).map_err(|e|e.to_string())?;
        if bytes.len()>=8*1024*1024 {return Err("app-server input exceeds 8 MiB; not submitted".into());}
        bytes.push(b'\n');
        let left=until.checked_duration_since(Instant::now()).ok_or("app-server send deadline; not submitted")?;
        let (receipt,done)=mpsc::channel();
        self.input.try_send((bytes,receipt)).map_err(|_|"app-server writer unavailable; not submitted")?;
        done.recv_timeout(left).map_err(|_|"app-server write acknowledgment unavailable; submission unknown")?
    }

    /// Timeout means no event yet. EOF/read error is unknown and never a completed turn.
    pub fn receive(&mut self,until:Instant)->Res<Option<Value>> {
        match self.events.recv_timeout(until.saturating_duration_since(Instant::now())) {
            Ok(event)=>event.map(Some),Err(RecvTimeoutError::Timeout)=>Ok(None),
            Err(RecvTimeoutError::Disconnected)=>Err("app-server stream ended; native turn outcome unknown".into()),
        }
    }

    pub fn poll(&mut self)->Res<Option<i32>> {self.child.try_wait().map(|s|s.map(|s|s.code().unwrap_or(-1))).map_err(|e|format!("app-server wait: {e}"))}

    /// Stop only this directly owned child. Consumer process-group/native-turn authority remains with the caller.
    pub fn kill(&mut self)->Res<()> {
        if self.poll()?.is_some() {return Ok(());}
        self.child.kill().map_err(|e|format!("app-server stop unconfirmed: {e}"))
    }
}

/// Caller-owned server transport: preserve all policy arguments and the account, without CLI `exec` flags.
pub fn app_server_command(mut base: Command, spec: &Spec) -> Res<Command> {
    if spec.client != Client::Codex {return Err("app-server requires Codex".into());}
    base.arg("app-server").args(&spec.args);
    Ok(environment(base,spec))
}

/// One app-server session. Feed every native response/notification to `observe`; forward unhandled server requests
/// to the caller's existing tool/approval policy. This state machine never approves a server request.
pub struct AppSession {
    spec: Spec,
    next: u64,
    pending: Option<(u64, &'static str)>,
    initialized: bool,
    ready: bool,
    thread: Option<String>,
    turn: Option<String>,
    active: bool,
    terminal: Option<String>,
    model: Option<String>,
    text: Option<String>,
    usage: Vec<Value>,
    limits: Option<Value>,
    latest_limits: Option<Value>,
    started_ms: i64,
    early: Vec<Value>,
    fault: Option<String>,
    scope: String,
    worked: bool,
    failure: Option<Value>,
}

impl AppSession {
    pub fn new(spec: &Spec) -> Res<Self> {
        if spec.client != Client::Codex || spec.resume && (!native::plain_id(&spec.session) || spec.session == "-") {
            return Err("app-server session invalid".into());
        }
        Ok(Self { spec: spec.clone(), next: 1, pending: None, initialized: false, ready: false,
            thread: None, turn: None, active: false, terminal: None, model: None, text: None, usage: vec![], limits: None, latest_limits:None, started_ms: 0,
            early:vec![],fault:None,scope:native::scope(spec),worked:false,failure:None })
    }

    /// Native catalogue/account methods share the same transport; caller handlers consume their raw responses.
    pub fn request(&mut self, method: &'static str, params: Value) -> Res<Value> {
        if self.pending.is_some() || self.fault.is_some() { return Err("app-server request pending or unknown; do not resubmit".into()); }
        if !params.is_object() { return Err("app-server params must be an object".into()); }
        let id = self.next; self.next=self.next.checked_add(1).ok_or("app-server request identity exhausted")?; self.pending = Some((id, method));
        Ok(json!({"id":id,"method":method,"params":params}))
    }

    pub fn initialize(&mut self, params: Value) -> Res<Value> {
        if self.initialized { return Err("app-server already initialized".into()); }
        self.request("initialize", params)
    }

    pub fn initialized(&self) -> Res<Value> {
        if !self.initialized { return Err("app-server initialization unconfirmed".into()); }
        Ok(json!({"method":"initialized"}))
    }

    /// Caller params, including model/provider/permissions/instructions/tools, pass through unchanged.
    pub fn open(&mut self, mut params: Value) -> Res<Value> {
        if !self.initialized || self.thread.is_some() { return Err("app-server thread not ready to open".into()); }
        if !params.is_object() { return Err("app-server params must be an object".into()); }
        if self.spec.resume {
            if params.get("threadId").is_some_and(|v| *v != self.spec.session) { return Err("app-server resume identity differs".into()); }
            params["threadId"] = json!(self.spec.session);
        }
        self.request(if self.spec.resume { "thread/resume" } else { "thread/start" }, params)
    }

    pub fn ready(&self) -> bool { self.ready && self.fault.is_none() }
    pub fn session(&self) -> Option<&str> { self.thread.as_deref() }
    pub fn turn_id(&self) -> Option<&str> { self.turn.as_deref() }

    /// Save the returned state before writing this request to the transport. A lost reply remains pending.
    pub fn start_turn(&mut self, mut params: Value) -> Res<Value> {
        if !self.ready() || self.active { return Err("app-server turn not ready; original submission retained".into()); }
        if !params.is_object() { return Err("app-server params must be an object".into()); }
        let thread = self.thread.as_deref().ok_or("app-server thread missing")?;
        if params.get("threadId").is_some_and(|v| v != thread) { return Err("app-server turn thread differs".into()); }
        params["threadId"] = json!(thread);
        let changed_model = params.get("model").is_some_and(|m| m.as_str() != self.model.as_deref());
        let request = self.request("turn/start", params)?;
        self.active = true; self.turn = None; self.terminal = None; self.text = None; self.usage.clear(); self.limits = self.latest_limits.clone(); self.started_ms = unix_ms();
        self.worked=false;self.failure=None;
        if changed_model {self.model=None;}
        Ok(request)
    }

    /// A request only. Matching terminal evidence is still required; no server process is killed.
    pub fn interrupt(&mut self) -> Res<Value> {
        if !self.active || self.terminal.is_some() { return Err("app-server turn is not active".into()); }
        let params = json!({"threadId":self.thread, "turnId":self.turn.as_ref().ok_or("app-server turn acceptance unknown")?});
        self.request("turn/interrupt", params)
    }

    pub fn observe(&mut self, message: &Value) -> Res<()> {
        if let Some(error)=&self.fault {return Err(error.clone());}
        let result=self.observe_inner(message);
        if let Err(error)=&result {self.fault=Some(error.clone());}
        result
    }

    fn observe_inner(&mut self, message: &Value) -> Res<()> {
        // Server requests belong to the caller, even when their id happens to equal a client request id.
        if message["method"].is_string() && message.get("id").is_some() { return Ok(()); }
        if let Some((id, method)) = self.pending {
            if message["id"].as_u64() == Some(id) {
                if !message["error"].is_null() {
                    if matches!(method,"model/list"|"account/rateLimits/read") {
                        if method=="account/rateLimits/read" {self.latest_limits=None;if self.active && self.terminal.is_none() {self.limits=None;}}
                        self.pending=None;return Ok(());
                    }
                    return Err(format!("app-server {method} refused; original session retained"));
                }
                let result = &message["result"];
                match method {
                    "initialize" => self.initialized = true,
                    "thread/start" | "thread/resume" => {
                        let thread = result["thread"]["id"].as_str().filter(|s| native::plain_id(s)).ok_or("app-server returned no thread")?;
                        if self.spec.resume && thread != self.spec.session { return Err("app-server resumed another session".into()); }
                        self.thread = Some(thread.into()); self.ready = true;
                        self.model = result["model"].as_str().filter(|s| model_code(s)).map(String::from);
                    }
                    "turn/start" => {
                        let turn = result["turn"]["id"].as_str().filter(|s| native::plain_id(s)).ok_or("app-server returned no turn")?;
                        if self.turn.as_deref().is_some_and(|old| old != turn) { return Err("app-server turn identity differs".into()); }
                        self.turn = Some(turn.into());
                    }
                    "account/rateLimits/read" => {
                        let limits=if result["rateLimitsByLimitId"].is_object() {&result["rateLimitsByLimitId"]["codex"]} else {&result["rateLimits"]};
                        self.latest_limits=limits.is_object().then(||limits.clone());
                        if self.active && self.terminal.is_none() {self.limits=self.latest_limits.clone();}
                    }
                    _ => {}
                }
                self.pending = None;
                if method == "turn/start" {
                    for event in std::mem::take(&mut self.early) {self.observe_inner(&event)?;}
                }
                return Ok(());
            }
        }
        let p = &message["params"];
        let method = message["method"].as_str().unwrap_or("");
        if method == "account/rateLimits/updated" {
            self.latest_limits=p["rateLimits"].is_object().then(||p["rateLimits"].clone());
            if self.active && self.terminal.is_none() {self.limits=self.latest_limits.clone();}
            return Ok(());
        }
        if !self.active { return Ok(()); }
        if p["threadId"].as_str() != self.thread.as_deref() { return Ok(()); }
        let id = p["turnId"].as_str().or(p["turn"]["id"].as_str());
        if self.turn.is_none() && self.pending.is_some_and(|(_, m)| m == "turn/start") {
            if self.early.len()>=256 || self.early.iter().map(|e|e.to_string().len()).sum::<usize>()+message.to_string().len()>8*1024*1024 {
                return Err("app-server early notifications exceed bound; reconcile original submission".into());
            }
            self.early.push(message.clone());return Ok(());
        }
        if self.turn.is_none() || id != self.turn.as_deref() { return Ok(()); }
        if self.terminal.is_none() && matches!(method,"item/started"|"item/completed")
            && p["item"]["type"].as_str().is_some_and(|s|s!="userMessage") {self.worked=true;}
        if method == "turn/completed" {
            let status = p["turn"]["status"].as_str().filter(|s| matches!(*s,"completed"|"failed"|"interrupted")).ok_or("app-server terminal status unknown")?;
            if self.terminal.as_deref().is_some_and(|old| old != status) { return Err("app-server terminal outcomes conflict".into()); }
            if self.terminal.is_none() && p["turn"]["error"].is_object() {self.failure=Some(p["turn"]["error"].clone());}
            self.terminal = Some(status.into());
        } else if self.terminal.is_none() {
            match method {
                "thread/tokenUsage/updated" => {
                    let last=&p["tokenUsage"]["last"];
                    self.worked|=["inputTokens","outputTokens"].iter().any(|k|last[k].as_u64().is_some_and(|n|n>0));
                    native::append_codex_usage(&mut self.usage,last,&p["tokenUsage"]["total"],true);
                }
                "item/completed" if p["item"]["type"] == "agentMessage" => self.text = p["item"]["text"].as_str().map(String::from),
                "model/rerouted" => self.model = p["toModel"].as_str().filter(|s| model_code(s)).map(String::from),
                "error" => self.failure=p["error"].is_object().then(||p["error"].clone()),
                _ => {}
            }
        }
        Ok(())
    }

    /// Native terminal report only. Transport EOF or process exit cannot manufacture a report.
    pub fn report(&self) -> Option<Report> {
        if self.fault.is_some() || self.turn.is_none() || self.pending.is_some_and(|(_,method)|method=="turn/start") {return None;}
        let status = self.terminal.as_deref()?;
        let mut events = self.usage.clone();
        if let Some(l) = self.limits.as_ref().filter(|l|l.is_object()) {
            let mut limits = l.clone();
            for key in ["primary", "secondary"] {
                if let Some(w) = l.get(key).filter(|w| w.is_object()) {
                    limits[key] = json!({"used_percent":w["usedPercent"],"resets_at":w["resetsAt"],"window_minutes":w["windowDurationMins"]});
                }
            }
            events.push(json!({"rate_limits":limits}));
        }
        let mut r = Report::read(Client::Codex, "", &events, None);
        r.session = self.thread.clone(); r.usage.model = self.model.clone(); r.text = self.text.clone(); r.started_ms = self.started_ms;
        (r.outcome, r.error) = match status { "completed" => ("ok","none"), "interrupted" => ("cancelled","interrupted"), _ => ("failed","turn-failed") };
        if status=="failed" {
            if let Some(error)=&self.failure {
                let event=json!({"type":"turn.failed","error":error});
                let evidence=[event.clone()];
                if !self.worked && limit_reached(&evidence).is_some() {
                    r.reached=Some(error["resetsAt"].as_i64().or_else(||r.limit.resets_at.and_then(|n|i64::try_from(n).ok())));
                    (r.outcome,r.error)=("refused","usage-limit");
                } else if auth_rejected(&event.to_string()).is_some() {(r.outcome,r.error)=("failed","auth-rejected");}
                else if transient_failure(&evidence).is_some() {(r.outcome,r.error)=("failed","model-capacity");}
            }
        }
        Some(r)
    }

    pub fn record(&self, ctx: &Context, store: &crate::telemetry::Store) -> Res<Option<Report>> {
        let Some(mut r) = self.report() else { return Ok(None) };
        let mut value = r.observation(Client::Codex, ctx);
        value["duration_us"] = Value::Null; value["unknown"]["duration_us"] = json!("not-reported");
        let key = crate::sha256_hex(format!("{}\0{}\0{}\0{}", self.spec.root.display(),self.scope, self.thread.as_deref().unwrap_or(""), self.turn.as_deref().unwrap_or("")).as_bytes());
        r.recorded = crate::telemetry::Event::identified("agent.app", &key, value).and_then(|e| store.append(&[e])).map(|_| ());
        Ok(Some(r))
    }

    /// Retire the terminal turn after the caller durably saved its outcome; the same server/thread can take another.
    pub fn finish_turn(&mut self) -> Res<()> {
        if self.fault.is_some() || self.terminal.is_none() || self.pending.is_some() { return Err("app-server terminal acknowledgment pending or unknown".into()); }
        self.active = false;
        Ok(())
    }

    /// Private restart state, never telemetry. Recovery performs no native request or permission change.
    pub fn checkpoint(&self) -> Value {
        json!({"schema":"coreq-app-session/1", "root":self.spec.root, "home":self.spec.home, "session":self.spec.session,
            "next":self.next, "pending":self.pending.map(|(id, method)| json!([id,method])), "initialized":self.initialized, "ready":self.ready,
            "thread":self.thread,"turn":self.turn,"active":self.active,"terminal":self.terminal,"model":self.model,"text":self.text,
            "usage":self.usage,"limits":self.limits,"latest_limits":self.latest_limits,"started_ms":self.started_ms,"early":self.early,"fault":self.fault,"scope":self.scope,"worked":self.worked,"failure":self.failure})
    }

    pub fn recover(spec: &Spec, v: &Value) -> Res<Self> {
        if v["schema"] != "coreq-app-session/1" || v["root"] != json!(spec.root) || v["home"] != json!(spec.home) || v["session"] != spec.session || v["scope"] != native::scope(spec) {
            return Err("app-server recovery scope differs".into());
        }
        let mut s = Self::new(spec)?;
        s.next = v["next"].as_u64().filter(|n| *n > 0).ok_or("app-server recovery request invalid")?;
        if !v["pending"].is_null() {
            let method = match v["pending"][1].as_str() { Some("initialize") => "initialize", Some("thread/start") => "thread/start", Some("thread/resume") => "thread/resume", Some("turn/start") => "turn/start", Some("turn/interrupt") => "turn/interrupt", _ => return Err("app-server recovery method invalid".into()) };
            s.pending = Some((v["pending"][0].as_u64().ok_or("app-server recovery id invalid")?,method));
        }
        s.initialized = v["initialized"].as_bool().ok_or("app-server recovery state invalid")?;
        s.ready = v["ready"].as_bool().ok_or("app-server recovery state invalid")?;
        s.active = v["active"].as_bool().ok_or("app-server recovery state invalid")?;
        for (field, target) in [("thread", &mut s.thread),("turn", &mut s.turn),("terminal", &mut s.terminal),("model", &mut s.model),("text", &mut s.text)] {
            *target = if v[field].is_null() { None } else { Some(v[field].as_str().ok_or("app-server recovery field invalid")?.into()) };
        }
        s.usage = v["usage"].as_array().ok_or("app-server recovery usage invalid")?.clone(); s.limits = (!v["limits"].is_null()).then(|| v["limits"].clone());
        s.latest_limits=(!v["latest_limits"].is_null()).then(||v["latest_limits"].clone());
        s.started_ms = v["started_ms"].as_i64().filter(|n| *n >= 0).ok_or("app-server recovery time invalid")?;
        s.early=v["early"].as_array().ok_or("app-server recovery early events missing")?.clone();
        s.fault=if v["fault"].is_null() {None} else {Some(v["fault"].as_str().ok_or("app-server recovery fault invalid")?.into())};
        s.worked=v["worked"].as_bool().ok_or("app-server recovery work boundary invalid")?;
        s.failure=if v["failure"].is_null() {None} else {Some(v["failure"].as_object().ok_or("app-server recovery failure invalid")?.clone().into())};
        if s.early.len()>256 || s.early.iter().map(|e|e.to_string().len()).sum::<usize>()>8*1024*1024
            || s.thread.as_deref().is_some_and(|s|!native::plain_id(s)) || s.turn.as_deref().is_some_and(|s|!native::plain_id(s))
            || s.terminal.as_deref().is_some_and(|s|!matches!(s,"completed"|"failed"|"interrupted"))
            || s.ready && s.thread.is_none() || s.terminal.is_some() && s.turn.is_none()
            || s.usage.iter().any(|v|v["type"]!="turn.completed" || !v["usage"].is_object()) || s.limits.as_ref().is_some_and(|v|!v.is_object())
            || s.latest_limits.as_ref().is_some_and(|v|!v.is_object()) {
            return Err("app-server recovery state inconsistent".into());
        }
        Ok(s)
    }
}
