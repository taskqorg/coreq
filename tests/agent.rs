//! coreq.md § Agent runtimes with fake clients: this binary is also the fake `claude` and `codex` when
//! `COREQ_FAKE_AGENT` names one, so start, resume, end detection and observations run on every OS without a real CLI.
use coreq::agent::{self, Client, Context, Spec, Turn};
use serde_json::{Value, json};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

fn main() {
    if let Ok(client) = std::env::var("COREQ_FAKE_AGENT") { return fake(&client); }
    let tests: &[(&str, fn())] = &[
        ("claude_starts_and_resumes_one_session", claude_starts_and_resumes_one_session),
        ("codex_resumes_its_thread_in_the_account_home", codex_resumes_its_thread_in_the_account_home),
        ("lingering_process_is_stopped_after_its_turn_ends", lingering_process_is_stopped_after_its_turn_ends),
        ("deadline_stops_a_turn_that_never_ends", deadline_stops_a_turn_that_never_ends),
        ("background_commands_preserve_native_mode", background_commands_preserve_native_mode),
        ("native_turns_bind_input_terminal_and_recovery", native_turns_bind_input_terminal_and_recovery),
        ("app_server_preserves_session_policy_and_counts_turns", app_server_preserves_session_policy_and_counts_turns),
        ("native_control_readiness_and_large_history_stay_owned", native_control_readiness_and_large_history_stay_owned),
        ("app_transport_preserves_native_tools_drafts_and_catalogue", app_transport_preserves_native_tools_drafts_and_catalogue),
    ];
    for (name, test) in tests {
        test();
        println!("test {name} ... ok");
    }
    println!("test result: ok. {} passed", tests.len());
}

fn append(path:&Path, events:&[Value]) {
    use std::io::Write;
    let mut f=std::fs::File::options().create(true).append(true).open(path).unwrap();
    for event in events {writeln!(f,"{event}").unwrap();}
}

// R48/R49: launcher readiness is separate from exact input and its native terminal; cold observation never replays.
// Risk: stale/foreign/partial native history releases owned work or charges a turn twice across restart.
fn native_turns_bind_input_terminal_and_recovery() {
    for client in [Client::Claude,Client::Codex] {
        let root=scratch(client.code());
        let spec=Spec {client,root:root.clone(),session:"native-session".into(),resume:false,args:vec![],home:Some(root.join("account")),env:vec![]};
        let path=root.join("native.jsonl");
        if client==Client::Codex {append(&path,&[json!({"type":"session_meta","payload":{"id":spec.session,"cwd":root}})]);}
        let mut turn=agent::NativeTurn::before(&spec,&path,"owned input").unwrap();
        let checkpoint=turn.checkpoint();
        let terminal=if client==Client::Claude {json!({"type":"system","subtype":"turn_duration","sessionId":spec.session,"durationMs":10})}
            else {json!({"type":"event_msg","payload":{"type":"task_complete","turn_id":"owned-turn"}})};
        append(&path,&[terminal.clone()]);
        assert!(turn.poll().unwrap().is_none(),"stale terminal without own input is not completion");
        assert!(!turn.accepted());
        let input=if client==Client::Claude {json!({"type":"user","sessionId":spec.session,"message":{"content":"owned input"}})}
            else {json!({"type":"event_msg","payload":{"type":"user_message","message":"owned input","turn_id":"owned-turn"}})};
        if client==Client::Codex {append(&path,&[json!({"type":"event_msg","payload":{"type":"task_started","turn_id":"owned-turn"}})]);}
        append(&path,&[input.clone()]);
        assert!(turn.poll().unwrap().is_none()); assert!(turn.accepted());
        if client==Client::Claude {
            append(&path,&[json!({"type":"system","subtype":"bridge_status","sessionId":spec.session,"url":"https://claude.ai/code/session_fixture123"}),
                json!({"type":"assistant","sessionId":spec.session,"message":{"content":[{"type":"tool_use","id":"tool1","name":"Read","input":{}}]}}),
                json!({"type":"user","sessionId":spec.session,"message":{"content":[{"type":"tool_result","tool_use_id":"tool1","content":"file content"}]}}),
                json!({"type":"assistant","sessionId":spec.session,"message":{"id":"m1","model":"fixture-model","content":[{"type":"text","text":"answer"}],
                "usage":{"input_tokens":3,"cache_read_input_tokens":20,"cache_creation_input_tokens":4,"output_tokens":5}}}),
                json!({"type":"rate_limit_event","rate_limit_info":{"utilization":0.25,"resetsAt":1793700000u64,"rateLimitType":"five_hour"}})]);
        } else {
            append(&path,&[json!({"type":"event_msg","payload":{"type":"token_count","turn_id":"foreign","info":{"last_token_usage":{"input_tokens":999,"cached_input_tokens":0,"output_tokens":999}}}})]);
        }
        append(&path,&[terminal]);
        let mut recovered=agent::NativeTurn::recover(&spec,&checkpoint).unwrap();
        let store=coreq::telemetry::Store::open(&root.join("telemetry.db")).unwrap();
        let ctx=Context {product:"fixtureq",version:"1.0.0",project:"other",task:Some((9,1)),route:None,account:"work"};
        let report=recovered.record(&ctx,&store).unwrap().unwrap(); report.recorded.unwrap();
        assert_eq!(report.outcome,"ok");
        let saved=recovered.checkpoint();
        recovered.record(&ctx,&store).unwrap().unwrap().recorded.unwrap();
        let mut cold=agent::NativeTurn::recover(&spec,&checkpoint).unwrap();
        cold.record(&ctx,&store).unwrap().unwrap().recorded.unwrap();
        assert_eq!(observations(&store).len(),1,"one durable observation even from a pre-ack checkpoint");
        if client==Client::Claude {
            assert_eq!(report.usage.input,Some(3)); assert_eq!(report.text.as_deref(),Some("answer"));
            assert_eq!(recovered.remote_url(),Some("https://claude.ai/code/session_fixture123"));
        } else {assert_eq!(report.usage.input,None,"foreign usage never becomes this turn's usage");assert_eq!(report.usage.unknown,Some("no-usage"));}
        let payload=observations(&store)[0].to_string();
        for private in ["owned input","native-session","native.jsonl","session_fixture123","answer"] {assert!(!payload.contains(private));}
        let changed=Spec {home:Some(root.join("foreign-account")),..spec.clone()};
        assert!(agent::NativeTurn::recover(&changed,&saved).is_err());
        let changed=Spec {resume:!spec.resume,..spec.clone()};assert!(agent::NativeTurn::recover(&changed,&saved).is_err());
        use std::io::Write;
        std::fs::File::options().append(true).open(&path).unwrap().write_all(b"{\"type\":").unwrap();
        assert!(recovered.poll().is_err(),"partial record cannot establish completion");
        std::fs::write(&path,"{}\n").unwrap();
        assert!(recovered.poll().is_err(),"truncated or rewritten history stays unknown");
        drop(store);std::fs::remove_dir_all(root).unwrap();
    }
}

// R47-R50: app-server protocol shares session/turn ownership while caller policy and server requests stay intact.
// Risk: wrong-thread completion, restored cumulative usage, policy rewriting, or recovery charges another turn.
fn app_server_preserves_session_policy_and_counts_turns() {
    let root=scratch("app-server");
    let mut spec=Spec {client:Client::Codex,root:root.clone(),session:"native-thread".into(),resume:true,args:vec![],home:Some(root.join("account")),env:vec![]};
    let mut app=agent::AppSession::new(&spec).unwrap();
    let init=app.initialize(json!({"clientInfo":{"name":"fixtureq","version":"1"},"capabilities":{"experimentalApi":true}})).unwrap();
    assert!(app.open(json!({})).is_err());
    app.observe(&json!({"id":init["id"],"result":{}})).unwrap();assert_eq!(app.initialized().unwrap(),json!({"method":"initialized"}));
    let policy=json!({"model":"fixture-model","modelProvider":"fixture-provider","effort":"high","sandbox":"read-only","approvalPolicy":"never","developerInstructions":"private rules"});
    let open=app.open(policy.clone()).unwrap();assert_eq!(open["method"],"thread/resume");
    for (k,v) in policy.as_object().unwrap() {assert_eq!(&open["params"][k],v);}
    let saved=app.checkpoint();
    assert!(agent::AppSession::recover(&Spec {resume:false,..spec.clone()},&saved).is_err(),"recovery cannot turn resume into start");
    app.observe(&json!({"id":open["id"],"result":{"thread":{"id":"foreign"}}})).unwrap_err();
    assert!(!app.ready());assert!(app.report().is_none());
    app=agent::AppSession::recover(&spec,&saved).unwrap();
    app.observe(&json!({"id":open["id"],"result":{"thread":{"id":"native-thread"},"model":"fixture-model"}})).unwrap();assert!(app.ready());
    for method in ["model/list","account/rateLimits/read"] {
        let optional=app.request(method,json!({})).unwrap();
        app.observe(&json!({"id":optional["id"],"error":{"code":-32601,"message":"invented unsupported optional read"}})).unwrap();assert!(app.ready());
    }
    let request=app.start_turn(json!({"input":[{"type":"text","text":"private input"}],"model":"fixture-model","effort":"high"})).unwrap();
    assert!(app.start_turn(json!({})).is_err(),"uncertain start is never replayed");
    app.observe(&json!({"method":"thread/tokenUsage/updated","params":{"threadId":"native-thread","turnId":"old","tokenUsage":{"total":{"inputTokens":999},"last":{"inputTokens":999}}}})).unwrap();
    for method in ["turn/started","turn/completed"] {
        app.observe(&json!({"method":method,"params":{"threadId":"native-thread","turn":{"id":"unrelated","status":"completed"}}})).unwrap();
    }
    assert!(app.report().is_none(),"same-thread notifications cannot bind a pending turn request");
    app.observe(&json!({"id":request["id"],"result":{"turn":{"id":"owned"}}})).unwrap();
    let before=app.checkpoint();let mut cold=agent::AppSession::recover(&spec,&before).unwrap();
    for current in [&mut app,&mut cold] {
        current.observe(&json!({"method":"turn/completed","params":{"threadId":"foreign","turn":{"id":"owned","status":"completed"}}})).unwrap();
        assert!(current.report().is_none());
        current.observe(&json!({"method":"item/tool/call","id":999,"params":{"tool":"caller-owned"}})).unwrap();
        let first=json!({"method":"thread/tokenUsage/updated","params":{"threadId":"native-thread","turnId":"owned","tokenUsage":{"total":{"inputTokens":130,"cachedInputTokens":100,"outputTokens":14},"last":{"inputTokens":30,"cachedInputTokens":20,"outputTokens":4}}}});
        current.observe(&first).unwrap();current.observe(&first).unwrap();
        current.observe(&json!({"method":"thread/tokenUsage/updated","params":{"threadId":"native-thread","turnId":"owned","tokenUsage":{"total":{"inputTokens":145,"cachedInputTokens":110,"outputTokens":16},"last":{"inputTokens":15,"cachedInputTokens":10,"outputTokens":2}}}})).unwrap();
        current.observe(&json!({"method":"account/rateLimits/updated","params":{"rateLimits":{"primary":{"usedPercent":25,"resetsAt":1793700000u64,"windowDurationMins":300}}}})).unwrap();
        current.observe(&json!({"method":"item/completed","params":{"threadId":"native-thread","turnId":"owned","item":{"type":"agentMessage","text":"private answer"}}})).unwrap();
        current.observe(&json!({"method":"turn/completed","params":{"threadId":"native-thread","turn":{"id":"owned","status":"completed"}}})).unwrap();
    }
    let report=app.report().unwrap(); assert_eq!(report.usage.input,Some(15)); assert_eq!(report.usage.output,Some(6));assert_eq!(report.text.as_deref(),Some("private answer"));
    assert_eq!(report.limit.used_percent,Some(25));
    let ctx=Context {product:"fixtureq",version:"1.0.0",project:"other",task:None,route:Some("manager"),account:"work"};
    let store=coreq::telemetry::Store::open(&root.join("telemetry.db")).unwrap();
    app.record(&ctx,&store).unwrap().unwrap().recorded.unwrap(); cold.record(&ctx,&store).unwrap().unwrap().recorded.unwrap();
    app.observe(&json!({"method":"account/rateLimits/updated","params":{"rateLimits":{"primary":{"usedPercent":99}}}})).unwrap();
    app.record(&ctx,&store).unwrap().unwrap().recorded.unwrap();
    assert_eq!(observations(&store).len(),1);
    let mut conflict=agent::AppSession::recover(&spec,&app.checkpoint()).unwrap();
    assert!(conflict.observe(&json!({"method":"turn/completed","params":{"threadId":"native-thread","turn":{"id":"owned","status":"interrupted"}}})).is_err());
    assert!(conflict.report().is_none());assert!(conflict.record(&ctx,&store).unwrap().is_none());
    assert!(agent::AppSession::recover(&spec,&conflict.checkpoint()).unwrap().report().is_none(),"conflict survives recovery");
    app.finish_turn().unwrap();let next=app.start_turn(json!({"input":[],"model":"another-model"})).unwrap();
    app.observe(&json!({"id":next["id"],"result":{"turn":{"id":"second"}}})).unwrap();
    let stop=app.interrupt().unwrap();assert_eq!(stop["params"],json!({"threadId":"native-thread","turnId":"second"}));
    assert!(app.report().is_none(),"interrupt request does not prove terminal");
    app.observe(&json!({"id":stop["id"],"result":{}})).unwrap();
    app.observe(&json!({"method":"account/rateLimits/updated","params":{"rateLimits":[]}})).unwrap();
    app.observe(&json!({"method":"turn/completed","params":{"threadId":"native-thread","turn":{"id":"second","status":"interrupted"}}})).unwrap();
    assert_eq!(app.report().unwrap().outcome,"cancelled");
    assert_eq!(app.report().unwrap().usage.model,None,"an override invalidates previous model attribution");
    for phase in [None,Some("item/started"),Some("item/completed")] {
        let worked=phase.is_some();
        app.finish_turn().unwrap();let req=app.start_turn(json!({"input":[]})).unwrap();
        let id=if worked {"limit-after-work"} else {"limit-before-work"};
        app.observe(&json!({"id":req["id"],"result":{"turn":{"id":id}}})).unwrap();
        if let Some(phase)=phase {app.observe(&json!({"method":phase,"params":{"threadId":"native-thread","turnId":id,"item":{"type":"commandExecution"}}})).unwrap();}
        app.observe(&json!({"method":"turn/completed","params":{"threadId":"native-thread","turn":{"id":id,"status":"failed","error":{"message":"usage limit reached","resetsAt":1793700000u64}}}})).unwrap();
        let report=app.report().unwrap();
        if worked {assert_eq!(report.reached,None);assert_eq!(report.outcome,"failed");}
        else {assert_eq!(report.reached,Some(Some(1793700000)));assert_eq!(report.outcome,"refused");}
    }
    spec.resume=false;spec.session="-".into();let mut new=agent::AppSession::new(&spec).unwrap();
    let req=new.initialize(json!({})).unwrap();new.observe(&json!({"id":req["id"],"result":{}})).unwrap();
    assert_eq!(new.open(json!({"dynamicTools":[{"name":"caller-tool"}]})).unwrap()["method"],"thread/start");
    drop(store);std::fs::remove_dir_all(root).unwrap();
}

// ---------- fake clients ----------

/// One line per event; the session store is `<cwd>/fake-sessions.json` (turn count per session).
fn fake(client: &str) {
    if std::env::var("COREQ_FAKE_APP_SERVER").is_ok() {return fake_app_server();}
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut input = String::new();
    std::io::stdin().read_to_string(&mut input).unwrap();
    if std::env::var("COREQ_FAKE_CONTROL").is_ok() {
        let record=json!({"args":args,"home":std::env::var(if client=="claude" {"CLAUDE_CONFIG_DIR"} else {"CODEX_HOME"}).ok()});
        std::fs::write("native-control.json",record.to_string()).unwrap();
        if args.first().is_some_and(|s|s=="agents") {println!("{}",std::env::var("FAKE_JOB_ROWS").unwrap());}
        return;
    }
    if std::env::var("COREQ_FAKE_BACKGROUND").is_ok() {
        assert_eq!(args.first().map(String::as_str), Some("--bg"), "background worker must not switch to -p");
        std::fs::write("background-command.json", json!({"args":args,"input":input,"home":std::env::var("CLAUDE_CONFIG_DIR").ok(),
            "inherited":std::env::var("CLAUDE_CODE_SESSION_ID").is_ok(),"extra":std::env::var("FAKE_EXTRA").ok()}).to_string()).unwrap();
        println!("backgrounded · c37eb75a");
        return;
    }
    let store = PathBuf::from("fake-sessions.json");
    let mut sessions: Value = std::fs::read_to_string(&store).ok().and_then(|t| serde_json::from_str(&t).ok()).unwrap_or(json!({}));
    let after = |flag: &str| args.iter().position(|a| a == flag).map(|i| args[i + 1].clone());
    let emit = |v: Value| println!("{v}");
    if std::env::var("COREQ_FAKE_HANG").is_ok() { std::thread::sleep(Duration::from_secs(60)); }
    let (session, resumed) = match client {
        "claude" => {
            assert_eq!(&args[..4], ["-p", "--output-format", "stream-json", "--verbose"], "claude print mode");
            match (after("--session-id"), after("--resume")) { (Some(s), None) => (s, false), (None, Some(s)) => (s, true), _ => panic!("claude session flag") }
        }
        _ => {
            assert_eq!(&args[..2], ["exec", "--json"], "codex exec");
            assert_eq!(args.last().map(String::as_str), Some("-"), "codex reads stdin");
            match after("resume") { Some(t) => (t, true), None => (format!("t-{}", std::process::id()), false) }
        }
    };
    if resumed && sessions.get(&session).is_none() { eprintln!("unknown session {session}"); std::process::exit(2); }
    if !resumed && sessions.get(&session).is_some() { eprintln!("session {session} already exists"); std::process::exit(2); }
    let turn = sessions[&session].as_u64().unwrap_or(0) + 1;
    sessions[&session] = json!(turn);
    std::fs::write(&store, sessions.to_string()).unwrap();
    // The record lets the test read argv, stdin and environment the child got.
    let record = json!({"args": args, "input": input, "home": std::env::var(if client == "claude" { "CLAUDE_CONFIG_DIR" } else { "CODEX_HOME" }).ok(),
        "inherited": std::env::var("CLAUDE_CODE_SESSION_ID").is_ok() || std::env::var("CODEX_THREAD_ID").is_ok(), "extra": std::env::var("FAKE_EXTRA").ok()});
    std::fs::write(format!("fake-turn-{turn}.json"), record.to_string()).unwrap();
    if client == "claude" {
        emit(json!({"type": "system", "subtype": "init", "session_id": session}));
        emit(json!({"type": "assistant", "message": {"id": format!("m{turn}"), "model": "claude-opus-5-5",
            "usage": {"input_tokens": 3, "cache_read_input_tokens": 100, "cache_creation_input_tokens": 20, "output_tokens": 7}}}));
        emit(json!({"type": "rate_limit_event", "rate_limit_info": {"status": "allowed", "resetsAt": 1792386000u64, "rateLimitType": "five_hour", "utilization": 0.25}}));
        emit(json!({"type": "result", "subtype": "success", "is_error": false, "result": format!("turn {turn}"), "session_id": session}));
    } else {
        if !resumed { emit(json!({"type": "thread.started", "thread_id": session})); }
        if let Ok(home) = std::env::var("CODEX_HOME") {
            let dir = Path::new(&home).join("sessions/2026/11/01");
            std::fs::create_dir_all(&dir).unwrap();
            let line = json!({"type": "event_msg", "payload": {"type": "token_count", "rate_limits": {"primary": {"used_percent": 30.0 + turn as f64, "window_minutes": 300, "resets_at": 1793700000u64}, "secondary": null}}});
            use std::io::Write;
            writeln!(std::fs::File::options().create(true).append(true).open(dir.join(format!("rollout-2026-11-01T00-00-00-{session}.jsonl"))).unwrap(), "{line}").unwrap();
        }
        emit(json!({"type": "turn.started"}));
        emit(json!({"type": "item.completed", "item": {"type": "agent_message", "text": format!("turn {turn}")}}));
        emit(json!({"type": "turn.completed", "usage": {"input_tokens": 50, "cached_input_tokens": 40, "output_tokens": 5}}));
    }
    if std::env::var("COREQ_FAKE_LINGER").is_ok() { std::thread::sleep(Duration::from_secs(60)); }
}

fn fake_app_server() {
    use std::io::BufRead;
    let args:Vec<_>=std::env::args().skip(1).collect();
    assert_eq!(args,vec!["app-server","--disable","apps"]);
    std::fs::write("app-transport.json",json!({"home":std::env::var("CODEX_HOME").ok(),"inherited":std::env::var("CODEX_THREAD_ID").is_ok()}).to_string()).unwrap();
    let mut turn=0;
    println!("fixture startup diagnostic");
    for line in std::io::stdin().lock().lines() {
        let value:Value=serde_json::from_str(&line.unwrap()).unwrap();
        let id=&value["id"];
        match value["method"].as_str() {
            Some("initialize")=>println!("{}",json!({"id":id,"result":{}})),
            Some("initialized")=>{},
            Some("model/list")=>println!("{}",json!({"id":id,"result":{"data":[{"model":"fixture-model","hidden":false}],"nextCursor":null}})),
            Some("thread/start")=>{
                assert_eq!(value["params"]["dynamicTools"][0]["name"],"caller-tool");
                assert_eq!(value["params"]["developerInstructions"],"private rules");
                std::fs::write("app-native-thread","native-thread").unwrap();
                println!("{}",json!({"id":id,"result":{"thread":{"id":"native-thread"},"model":"fixture-model"}}));
            },
            Some("thread/resume")=>{
                assert_eq!(value["params"]["threadId"],std::fs::read_to_string("app-native-thread").unwrap());
                println!("{}",json!({"id":id,"result":{"thread":{"id":"native-thread"},"model":"fixture-model"}}));
            },
            Some("turn/start")=>{
                turn+=1;
                println!("{}",json!({"id":id,"result":{"turn":{"id":format!("turn-{turn}")}}}));
                println!("{}",json!({"id":999,"method":"item/tool/call","params":{"threadId":"native-thread","turnId":format!("turn-{turn}"),"tool":"caller-tool","arguments":{"private":"tool input"}}}));
                println!("{}",json!({"method":"item/agentMessage/delta","params":{"threadId":"native-thread","turnId":format!("turn-{turn}"),"itemId":"draft","delta":"private draft"}}));
            },
            None if id==999=>{
                assert_eq!(value["result"]["success"],true,"only caller's handler can answer native tool requests");
                println!("{}",json!({"method":"item/completed","params":{"threadId":"native-thread","turnId":format!("turn-{turn}"),"item":{"type":"agentMessage","text":"final answer"}}}));
                println!("{}",json!({"method":"turn/completed","params":{"threadId":"native-thread","turn":{"id":format!("turn-{turn}"),"status":"completed"}}}));
            },
            _=>panic!("unexpected native protocol request"),
        }
    }
}

// R47-R50 and owner scope note: the native transport preserves dynamic tools, private drafts, catalogue and resume.
// Risk: migration forces exec/MCP, grants a server request, leaks draft text into telemetry, or kills the warm session.
fn app_transport_preserves_native_tools_drafts_and_catalogue() {
    let root=scratch("app-transport");
    let mut spec=Spec {client:Client::Codex,root:root.clone(),session:"-".into(),resume:false,
        args:vec!["--disable".into(),"apps".into()],home:Some(root.join("account")),env:vec![]};
    let store=coreq::telemetry::Store::open(&root.join("telemetry.db")).unwrap();
    let ctx=Context {product:"fixtureq",version:"1.0.0",project:"other",task:None,route:Some("manager"),account:"work"};
    for resumed in [false,true] {
        spec.resume=resumed;
        let mut base=fake_client(Client::Codex);base.env("COREQ_FAKE_APP_SERVER","1");
        let mut server=agent::AppServer::start(base,&spec,&root.join("private.log")).unwrap();
        let mut app=agent::AppSession::new(&spec).unwrap();
        let until=Instant::now()+Duration::from_secs(10);
        let init=app.initialize(json!({"clientInfo":{"name":"fixtureq","version":"1"}})).unwrap();server.send(&init,until).unwrap();
        app.observe(&server.receive(until).unwrap().unwrap()).unwrap();server.send(&app.initialized().unwrap(),until).unwrap();
        let catalogue=app.request("model/list",json!({"includeHidden":false,"limit":100})).unwrap();server.send(&catalogue,until).unwrap();
        let models=server.receive(until).unwrap().unwrap();app.observe(&models).unwrap();assert_eq!(models["result"]["data"][0]["model"],"fixture-model");
        let params=if resumed {json!({"approvalPolicy":"never","sandbox":"read-only"})}
            else {json!({"approvalPolicy":"never","sandbox":"read-only","dynamicTools":[{"name":"caller-tool","inputSchema":{"type":"object"}}],"developerInstructions":"private rules"})};
        let open=app.open(params).unwrap();server.send(&open,until).unwrap();app.observe(&server.receive(until).unwrap().unwrap()).unwrap();
        assert_eq!(app.session(),Some("native-thread"));
        let request=app.start_turn(json!({"input":[{"type":"text","text":"private request"}]})).unwrap();server.send(&request,until).unwrap();
        let mut drafts=vec![];let mut handled=0;
        while app.report().is_none() {
            let event=server.receive(until).unwrap().expect("native event before deadline");app.observe(&event).unwrap();
            if event["method"]=="item/tool/call" {
                assert_eq!(event["params"]["tool"],"caller-tool");handled+=1;
                assert!(app.report().is_none());
                server.send(&json!({"id":event["id"],"result":{"success":true,"contentItems":[{"type":"inputText","text":"caller tool result"}]}}),until).unwrap();
            }
            if event["method"]=="item/agentMessage/delta" {drafts.push(event["params"]["delta"].clone());}
        }
        assert_eq!(handled,1);assert_eq!(drafts,vec![json!("private draft")]);
        assert_eq!(app.report().unwrap().text.as_deref(),Some("final answer"));
        assert_eq!(server.poll().unwrap(),None,"native terminal leaves the shared server running");
        // Distinct routes are product-owned; this probe checks transport/report metadata without double-recording a fake reused turn ID.
        if !resumed {app.record(&ctx,&store).unwrap().unwrap().recorded.unwrap();}
        server.kill().unwrap();
        while server.poll().unwrap().is_none() {assert!(Instant::now()<until);std::thread::sleep(Duration::from_millis(10));}
        assert!(server.receive(until).is_err(),"EOF stays unknown rather than creating a terminal event");
        spec.session="native-thread".into();
    }
    let metadata=observations(&store)[0].to_string();
    for private in ["private draft","private request","tool input","caller tool result","final answer"] {assert!(!metadata.contains(private));}
    let env:Value=serde_json::from_str(&std::fs::read_to_string(root.join("app-transport.json")).unwrap()).unwrap();assert_eq!(env["home"],json!(spec.home));assert_eq!(env["inherited"],false);
    assert!(std::fs::read_to_string(root.join("private.log")).unwrap().contains("fixture startup diagnostic"));
    #[cfg(unix)] {use std::os::unix::fs::PermissionsExt;assert_eq!(std::fs::metadata(root.join("private.log")).unwrap().permissions().mode()&0o777,0o600);}
    drop(store);std::fs::remove_dir_all(root).unwrap();
}

// ---------- helpers ----------

fn scratch(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("coreq-agent-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn fake_client(client: Client) -> Command {
    let mut c = Command::new(std::env::current_exe().unwrap());
    c.env("COREQ_FAKE_AGENT", client.code()).env("CLAUDE_CODE_SESSION_ID", "parent").env("CODEX_THREAD_ID", "parent")
        .env_remove("COREQ_FAKE_LINGER").env_remove("COREQ_FAKE_HANG");
    c
}

fn record(root: &Path, turn: u64) -> Value { serde_json::from_str(&std::fs::read_to_string(root.join(format!("fake-turn-{turn}.json"))).unwrap()).unwrap() }

fn observations(store: &coreq::telemetry::Store) -> Vec<Value> {
    store.export(0, 0, 100).unwrap()["events"].as_array().unwrap().iter().map(|e| e["observation"].clone()).collect()
}

// R48/R49/R36: control acknowledgments, native readiness and large history progress cannot retire an execution.
// Risk: account/root confusion stops foreign work, or a bounded reader repeats input / accepts a stale terminal.
fn native_control_readiness_and_large_history_stay_owned() {
    let root=scratch("native-control");
    let mut spec=Spec {client:Client::Claude,root:root.clone(),session:"alias".into(),resume:false,args:vec![],home:Some(root.join("account")),env:vec![]};
    let row=json!({"id":"c37eb75a","kind":"background","cwd":root,"sessionId":"native-session","state":"working","pid":42});
    let mut base=fake_client(Client::Claude);base.env("COREQ_FAKE_CONTROL","1").env("FAKE_JOB_ROWS",json!([row]).to_string());
    let rows=agent::background_jobs(base,&spec,Duration::from_secs(5)).unwrap();
    let job=agent::BackgroundJob::read(&rows[0],&spec,"c37eb75a").unwrap();assert_eq!(job.session,"native-session");
    let control:Value=serde_json::from_str(&std::fs::read_to_string(root.join("native-control.json")).unwrap()).unwrap();
    assert_eq!(control["args"],json!(["agents","--json","--all","--cwd",root]));assert_eq!(control["home"],json!(spec.home));
    for field in ["id","kind","cwd","state","sessionId"] {
        let mut bad=row.clone();bad[field]=json!("foreign");
        assert!(agent::BackgroundJob::read(&bad,&Spec {resume:true,session:"native-session".into(),..spec.clone()},"c37eb75a").is_err(),"{field}");
    }
    let absent=root.join("not-created.jsonl");
    let history=root.join("native.jsonl");
    let mut turn=agent::NativeTurn::before(&spec,&absent,"owned").unwrap();
    turn.attach_background(&job,&history).unwrap();
    spec.session=job.session.clone();
    let saved=turn.checkpoint();
    let mut cold=agent::NativeTurn::recover(&spec,&saved).unwrap();
    let mut base=fake_client(Client::Claude);base.env("COREQ_FAKE_CONTROL","1");
    agent::background_stop(base,&spec,&job,Duration::from_secs(5)).unwrap();
    assert!(cold.poll().is_err(),"stop acknowledgment without native history never becomes completion");
    append(&history,&[json!({"type":"user","sessionId":spec.session,"message":{"content":"owned"}})]);
    // Multiple bounded passes, each individual native record below the record bound.
    let filler=json!({"type":"progress","data":"x".repeat(1024*1024)});
    append(&history,&vec![filler;9]);
    append(&history,&[json!({"type":"system","subtype":"turn_duration","sessionId":spec.session})]);
    assert!(cold.poll().unwrap().is_none(),"scan budget is pending, never an early success");
    assert!(cold.accepted());
    assert_eq!(cold.poll().unwrap().unwrap().outcome,"ok");
    let mut changed=spec.clone();changed.args.push("--model=foreign".into());assert!(agent::NativeTurn::recover(&changed,&saved).is_err());
    let replacement=root.join("replacement.jsonl");std::fs::copy(&history,&replacement).unwrap();
    std::fs::remove_file(&history).unwrap();std::fs::rename(&replacement,&history).unwrap();
    assert!(cold.poll().is_err(),"a copied transcript has another file identity");
    spec.client=Client::Codex;spec.resume=true;spec.session="native-thread".into();
    let mut base=fake_client(Client::Codex);base.env("COREQ_FAKE_CONTROL","1");
    agent::app_queue(base,&spec,"caller input reference",Duration::from_secs(5)).unwrap();
    let control:Value=serde_json::from_str(&std::fs::read_to_string(root.join("native-control.json")).unwrap()).unwrap();
    assert_eq!(control["args"],json!(["queue","--thread","native-thread","--message","caller input reference"]));assert_eq!(control["home"],json!(spec.home));
    let path=root.join("app.jsonl");
    append(&path,&[json!({"type":"session_meta","payload":{"id":spec.session,"cwd":root}})]);
    let mut app=agent::NativeTurn::before(&spec,&path,"caller input reference").unwrap();
    let usage=json!({"type":"event_msg","payload":{"type":"token_count","turn_id":"native-turn","info":{"last_token_usage":{"input_tokens":10,"cached_input_tokens":5,"output_tokens":2},"total_token_usage":{"input_tokens":110,"cached_input_tokens":55,"output_tokens":12}}}});
    append(&path,&[json!({"type":"event_msg","payload":{"type":"task_started","turn_id":"native-turn"}}),
        json!({"type":"event_msg","payload":{"type":"item_completed","thread_id":spec.session,"turn_id":"native-turn","item":{"type":"UserMessage","content":[{"type":"text","text":"caller input reference\n"}]}}}),
        usage.clone(),usage,
        json!({"type":"event_msg","payload":{"type":"agent_message","message":"app answer"}}),
        json!({"type":"event_msg","payload":{"type":"task_complete","turn_id":"native-turn"}})]);
    let report=app.poll().unwrap().unwrap();assert_eq!(report.usage.input,Some(5));assert_eq!(report.text.as_deref(),Some("app answer"));
    spec.session="unsafe & command".into();assert!(agent::app_queue(fake_client(Client::Codex),&spec,"x",Duration::from_secs(5)).is_err());
    std::fs::remove_dir_all(root).unwrap();
}

fn soon() -> Option<Instant> { Some(Instant::now() + Duration::from_secs(30)) }

// R47/R48: the native background path preserves exact start-only policy and resume argv.
// Risk: workers silently move to print mode or resume with changed saved permissions/rules.
fn background_commands_preserve_native_mode() {
    let root = scratch("background-command");
    let mut spec = Spec { client:Client::Claude, root:root.clone(), session:"native-session".into(), resume:false,
        args:vec!["--settings".into(),agent::claude_settings(Some("rules compact"), &[("FIXTURE_ATTEMPT","7")]).into(),"--name".into(),"fixture name".into(),
            "--remote-control".into(),"fixture name".into(),"--permission-mode".into(),"dontAsk".into()],
        home:Some(root.join("account")),env:vec![("FAKE_EXTRA".into(),"preserved".into())] };
    let mut base = fake_client(Client::Claude); base.env("COREQ_FAKE_BACKGROUND","1");
    let mut command = agent::background_command(base, &spec).unwrap();
    let out = coreq::process::exchange("fixture.launch", &mut command, b"read caller-owned input file", Duration::from_secs(5)).unwrap();
    assert!(out.status.success(), "native background start must be supported");
    let record:Value = serde_json::from_str(&std::fs::read_to_string(root.join("background-command.json")).unwrap()).unwrap();
    assert_eq!(record["args"][0],"--bg");
    assert_eq!(record["args"].as_array().unwrap().len(),1+spec.args.len());
    assert_eq!(record["input"],"read caller-owned input file");
    assert_eq!(record["home"],spec.home.as_ref().unwrap().to_str().unwrap());
    assert_eq!(record["inherited"],false); assert_eq!(record["extra"],"preserved");
    spec.resume = true;
    let mut base = fake_client(Client::Claude); base.env("COREQ_FAKE_BACKGROUND","1");
    agent::background_launch(base,&spec,"continue file reference",Duration::from_secs(5)).unwrap().unwrap();
    let record:Value = serde_json::from_str(&std::fs::read_to_string(root.join("background-command.json")).unwrap()).unwrap();
    assert_eq!(record["args"],json!(["--bg","--resume","native-session"]));
    assert!(!agent::background_command(fake_client(Client::Codex),&Spec {client:Client::Codex,..spec}).is_ok());
    std::fs::remove_dir_all(root).unwrap();
}

// ---------- tests ----------

// Chain: coreq.md R47-R50, task A2-A4: one Claude session per conversation, one process per turn, resumed by session
// ID; each turn appends one observation with tokens, model, limit and duration.
// Risk: a resume opens a second session, the parent's session leaks into the child, or a turn goes unobserved.
fn claude_starts_and_resumes_one_session() {
    let root = scratch("claude");
    let store = coreq::telemetry::Store::open(&root.join("telemetry.db")).unwrap();
    let log = root.join("logs").join("T1-1.log");
    let session = agent::new_session(Client::Claude);
    let settings = agent::claude_settings(Some("tool rules compact"), &[]);
    let mut spec = Spec { client: Client::Claude, root: root.clone(), session: session.clone(), resume: false,
        args: vec!["--permission-mode".into(), "auto".into(), "--settings".into(), settings.into()], home: None, env: vec![("FAKE_EXTRA".into(), "x".into())] };
    let ctx = Context { product: "fixtureq", version: "1.0.0", project: "taskqorg/coreq", task: Some((1, 1)), route: None, account: "default" };
    let first = agent::run(fake_client(Client::Claude), &spec, &log, "brief", soon(), &ctx, &store).unwrap();
    assert_eq!((first.outcome, first.session.as_deref(), first.text.as_deref(), first.exit), ("ok", Some(session.as_str()), Some("turn 1"), Some(0)));
    first.recorded.as_ref().unwrap();
    let r = record(&root, 1);
    assert_eq!((r["input"].as_str(), r["inherited"].as_bool(), r["extra"].as_str()), (Some("brief"), Some(false), Some("x")), "stdin brief, no parent session, caller env");
    spec.resume = true;
    let second = agent::run(fake_client(Client::Claude), &spec, &log, "next", soon(), &ctx, &store).unwrap();
    assert_eq!((second.outcome, second.text.as_deref()), ("ok", Some("turn 2")), "the resume continues the same session");
    assert!(record(&root, 2)["args"].as_array().unwrap().iter().any(|a| a == "--resume"));
    assert_eq!(second.usage.input, Some(3), "a resume reads only its own turn from the shared log");
    let seen = observations(&store);
    assert_eq!(seen.len(), 2, "one observation per turn");
    let o = &seen[1];
    assert_eq!((o["kind"].as_str(), o["runtime"].as_str(), o["model"].as_str(), o["task"].as_u64()), (Some("agent.turn"), Some("claude"), Some("claude-opus-5-5"), Some(1)));
    assert_eq!((o["input_tokens"].as_u64(), o["cache_read_tokens"].as_u64(), o["cache_write_tokens"].as_u64(), o["output_tokens"].as_u64()), (Some(3), Some(100), Some(20), Some(7)));
    assert_eq!((o["limit_used_percent"].as_u64(), o["limit_resets_at"].as_u64(), o["limit_window"].as_str()), (Some(25), Some(1792386000), Some("five_hour")));
    assert!(o["duration_us"].as_u64().is_some());
    drop(store);
    let _ = std::fs::remove_dir_all(root);
}

// Chain: coreq.md R48, R50, R51: Codex names its thread on start; the resume uses that thread and keeps the account's
// login home; the limit snapshot comes from the thread rollout in that home. Risk: a resume lands in another account
// or the snapshot reads another account's limits.
fn codex_resumes_its_thread_in_the_account_home() {
    let root = scratch("codex");
    let home = root.join("codex-home").join("fixtureq-accounts").join("work");
    let store = coreq::telemetry::Store::open(&root.join("telemetry.db")).unwrap();
    let log = root.join("T2-1.log");
    let mut spec = Spec { client: Client::Codex, root: root.clone(), session: agent::new_session(Client::Codex), resume: false,
        args: vec!["--sandbox".into(), "workspace-write".into()], home: Some(home.clone()), env: vec![] };
    let ctx = Context { product: "fixtureq", version: "1.0.0", project: "other", task: None, route: Some("manager"), account: "work" };
    let first = agent::run(fake_client(Client::Codex), &spec, &log, "hello", soon(), &ctx, &store).unwrap();
    let thread = first.session.clone().expect("thread.started names the thread");
    assert_eq!((first.outcome, first.text.as_deref()), ("ok", Some("turn 1")));
    assert_eq!(record(&root, 1)["home"].as_str().map(PathBuf::from), Some(home.clone()), "the account home reaches the child");
    let args = record(&root, 1)["args"].clone();
    assert_eq!(args, json!(["exec", "--json", "--sandbox", "workspace-write", "-C", root.to_string_lossy(), "-"]));
    spec.session = thread.clone();
    spec.resume = true;
    let second = agent::run(fake_client(Client::Codex), &spec, &log, "again", soon(), &ctx, &store).unwrap();
    assert_eq!((second.outcome, second.text.as_deref()), ("ok", Some("turn 2")));
    assert_eq!(record(&root, 2)["args"], json!(["exec", "--json", "--sandbox", "workspace-write", "resume", thread, "-"]));
    assert_eq!((second.limit.used_percent, second.limit.resets_at, second.limit.window.as_deref()), (Some(32), Some(1793700000), Some("300m")), "latest rollout snapshot");
    let o = &observations(&store)[1];
    assert_eq!((o["route"].as_str(), o["account"].as_str(), o["input_tokens"].as_u64(), o["cache_read_tokens"].as_u64()), (Some("manager"), Some("work"), Some(10), Some(40)));
    assert_eq!((o["model"].as_str(), o["unknown"]["model"].as_str(), o["unknown"]["task"].as_str()), (None, Some("not-reported"), Some("no-task")));
    drop(store);
    let _ = std::fs::remove_dir_all(root);
}

// Chain: TaskQ CliTurn end detection (unchanged): a turn-end event ends the turn even when the process lingers.
// Risk: a client that keeps running after its turn holds the caller's slot until the deadline.
fn lingering_process_is_stopped_after_its_turn_ends() {
    let root = scratch("linger");
    let spec = Spec { client: Client::Codex, root: root.clone(), session: "-".into(), resume: false, args: vec![], home: None, env: vec![] };
    let mut base = fake_client(Client::Codex);
    base.env("COREQ_FAKE_LINGER", "1");
    let started = Instant::now();
    let mut turn = Turn::start(base, &spec, &root.join("T3-1.log"), "x").unwrap();
    turn.wait(soon()).unwrap();
    assert!(started.elapsed() < Duration::from_secs(20), "stopped after its turn ended, not at the deadline");
    assert_eq!(turn.report().outcome, "ok", "the ended turn keeps its outcome");
    let _ = std::fs::remove_dir_all(root);
}

// Chain: coreq.md R49: a turn past its deadline is stopped and observed as cancelled/timeout, never ok.
// Risk: a hung client blocks its caller forever or reads as a finished turn.
fn deadline_stops_a_turn_that_never_ends() {
    let root = scratch("hang");
    let spec = Spec { client: Client::Claude, root: root.clone(), session: agent::new_session(Client::Claude), resume: false, args: vec![], home: None, env: vec![] };
    let mut base = fake_client(Client::Claude);
    base.env("COREQ_FAKE_HANG", "1");
    let mut turn = Turn::start(base, &spec, &root.join("T4-1.log"), "x").unwrap();
    turn.wait(Some(Instant::now() + Duration::from_millis(500))).unwrap();
    let r = turn.report();
    assert_eq!((r.outcome, r.error), ("cancelled", "timeout"));
    let ctx = Context { product: "fixtureq", version: "1.0.0", project: "other", task: Some((4, 1)), route: None, account: "default" };
    coreq::telemetry::Event::new(r.observation(Client::Claude, &ctx)).expect("a cancelled turn is a valid observation");
    let _ = std::fs::remove_dir_all(root);
}
