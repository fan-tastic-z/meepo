//! meepo-cli — command-line entry point.
//!
//! `meepo run [...] <prompt>` — one shot. `meepo chat [...]` — multi-turn REPL
//! with SQLite persistence. Output is streamed. The backend owns the tool loop
//! (executor injected at construction); the runner just consumes the stream.

use std::io::{self, BufRead, Write};
use std::sync::Arc;

use futures::stream::StreamExt;

use meepo_core::{
    AgentBackend, BackendSendInput, ChatMessage, Content, FakeBackend, Role, RuntimeEventStore,
    SessionEvent, StopReason,
};
use meepo_providers::OpenAiBackend;
use meepo_runtime::{messages_from_runtime_events, InvocationContext, RuntimeRunner, RunStatus, TurnEvent, DEFAULT_SYSTEM_PROMPT};
use meepo_storage::SqliteStore;
use meepo_sandbox::{MacosSeatbeltBackend, SandboxManager};
use meepo_tools::ToolRegistry;

const DEFAULT_OPENAI_MODEL: &str = "deepseek-v4-flash";

fn resolve_system(cli: &Cli) -> String {
    cli.system.clone().unwrap_or_else(|| DEFAULT_SYSTEM_PROMPT.to_string())
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let cli = parse_cli(&args);
    let session_id = cli.session.clone().unwrap_or_else(|| "cli-session".to_string());
    let sandbox = Arc::new({
        let mut sm = SandboxManager::new();
        #[cfg(target_os = "macos")]
        sm.register(Box::new(MacosSeatbeltBackend::new()));
        sm
    });
    let tools: Arc<ToolRegistry> = Arc::new({
        let mut t = ToolRegistry::new();
        for tool in meepo_tools::all_with_sandbox(sandbox.clone()) {
            t.register(tool);
        }
        t
    });

    match cli.mode {
        Mode::Chat => {
            let db = cli.db.clone().unwrap_or_else(default_db);
            run_chat(&session_id, cli, &tools, &db).await;
        }
        Mode::Run => {
            let prompt = match &cli.prompt {
                Some(p) => p.clone(),
                None => {
                    eprintln!("usage: meepo run [--provider fake|openai] [--model M] [--base-url U] [--system S] <prompt>");
                    std::process::exit(2);
                }
            };
            run_single_turn(&session_id, cli, &tools, &prompt).await;
        }
    }
}

fn default_db() -> String {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    format!("{home}/.meepo/runtime.sqlite")
}

async fn drive_turn(
    backend: &mut dyn AgentBackend,
    ctx: &InvocationContext,
    input: &BackendSendInput,
    previous_compact_summary: Option<&str>,
) -> (Vec<meepo_core::RuntimeEvent>, RunStatus, Option<String>) {
    let mut collected = Vec::new();
    let mut status = RunStatus::Failed;
    let mut compact_summary = None;
    let mut stream = Box::pin(RuntimeRunner::run_stream(backend, ctx, input, previous_compact_summary));
    while let Some(te) = stream.next().await {
        match te {
            TurnEvent::Event(re) => {
                print_event_live(&re);
                collected.push(re);
            }
            TurnEvent::Done { status: s, compact_summary: cs, .. } => {
                status = s;
                compact_summary = cs;
            }
        }
    }
    (collected, status, compact_summary)
}

fn print_event_live(re: &meepo_core::RuntimeEvent) {
    match (&re.role, &re.content) {
        (Role::Model, Some(Content::Text { text, .. })) => {
            print!("{text}");
            io::stdout().flush().ok();
        }
        (_, Some(Content::FunctionResponse { result, is_error, .. })) if !is_error.unwrap_or(false) => {
            eprintln!("[tool result] {result}");
        }
        (_, Some(Content::Error { message, .. })) => {
            eprintln!("[error] {message}");
        }
        _ => {}
    }
}

async fn run_single_turn(session_id: &str, cli: Cli, tools: &Arc<ToolRegistry>, prompt_display: &str) {
    let mut backend = build_backend(session_id, &cli, prompt_display, tools);
    let ctx = InvocationContext {
        session_id: session_id.into(), run_id: "r1".into(),
        invocation_id: "inv1".into(), turn_id: "t1".into(),
    };
    let input = BackendSendInput {
        turn_id: "t1".into(), run_id: Some("r1".into()),
        invocation_id: Some("inv1".into()), max_steps: None,
        messages: vec![ChatMessage::User { content: prompt_display.to_string() }],
        system_prompt: Some(resolve_system(&cli)),
        tools: vec![],
    };
    let (_events, status, _) = drive_turn(&mut *backend, &ctx, &input, None).await;
    println!();
    eprintln!("[provider: {}, turn status: {:?}]", cli.provider, status);
}

async fn run_chat(session_id: &str, cli: Cli, tools: &Arc<ToolRegistry>, db_path: &str) {
    let store = match SqliteStore::open(db_path) {
        Ok(s) => s,
        Err(e) => { eprintln!("open store {db_path}: {e}"); std::process::exit(2); }
    };
    let prior = store.read_session_runtime_events(session_id).await.unwrap_or_default();
    let mut messages = messages_from_runtime_events(&prior);
    if !messages.is_empty() {
        eprintln!("(resumed session '{session_id}': {} prior messages from {db_path})", messages.len());
    }
    let system_prompt = resolve_system(&cli);
    let stdin = io::stdin();
    let mut turn = 0u32;
    let mut compact_summary: Option<String> = None;
    println!("meepo chat (provider: {}, session: {session_id}, db: {db_path}). Ctrl-D to exit.", cli.provider);
    loop {
        print!("> "); io::stdout().flush().ok();
        let mut line = String::new();
        if stdin.lock().read_line(&mut line).unwrap_or(0) == 0 { break; }
        let line = line.trim_end_matches('\n').to_string();
        if line.is_empty() { continue; }
        turn += 1;
        messages.push(ChatMessage::User { content: line.clone() });
        let run_id = format!("r{turn}");
        let mut backend = build_backend(session_id, &cli, &line, tools);
        let ctx = InvocationContext {
            session_id: session_id.into(), run_id: run_id.clone(),
            invocation_id: format!("inv{turn}"), turn_id: format!("t{turn}"),
        };
        let input = BackendSendInput {
            turn_id: format!("t{turn}"), run_id: Some(run_id.clone()),
            invocation_id: Some(format!("inv{turn}")), max_steps: None,
            messages: messages.clone(), system_prompt: Some(system_prompt.clone()),
            tools: vec![],
        };
        let (turn_events, _status, new_summary) = drive_turn(&mut *backend, &ctx, &input, compact_summary.as_deref()).await;
        if let Some(s) = new_summary { compact_summary = Some(s); }
        println!();
        for ev in &turn_events {
            let _ = store.append_runtime_event(session_id, &run_id, ev.clone(), false).await;
        }
        let all = store.read_session_runtime_events(session_id).await.unwrap_or_default();
        messages = messages_from_runtime_events(&all);
    }
}

fn build_backend(session_id: &str, cli: &Cli, prompt: &str, tools: &Arc<ToolRegistry>) -> Box<dyn AgentBackend> {
    match cli.provider.as_str() {
        "openai" => {
            let key = match std::env::var("OPENAI_API_KEY") {
                Ok(k) => k,
                Err(_) => { eprintln!("OPENAI_API_KEY is not set"); std::process::exit(2); }
            };
            let model = cli.model.clone().unwrap_or_else(|| DEFAULT_OPENAI_MODEL.to_string());
            let mut b = OpenAiBackend::new(session_id, model, key);
            let base_url = cli.base_url.clone().or_else(|| std::env::var("OPENAI_BASE_URL").ok());
            if let Some(url) = base_url { b = b.with_base_url(url); }
            Box::new(b.with_executor(tools.clone()))
        }
        _ => Box::new(FakeBackend::new(session_id, fake_script(prompt))),
    }
}

fn fake_script(prompt: &str) -> Vec<SessionEvent> {
    vec![
        SessionEvent::TextComplete {
            id: "1".into(), turn_id: "t".into(), ts: 0, message_id: "m".into(),
            text: format!("meepo (fake backend): {prompt}"), provider_options: None,
        },
        SessionEvent::Complete {
            id: "2".into(), turn_id: "t".into(), ts: 1, stop_reason: StopReason::EndTurn,
        },
    ]
}

#[derive(Clone, Copy)]
enum Mode { Run, Chat }

struct Cli {
    mode: Mode, provider: String, prompt: Option<String>,
    model: Option<String>, base_url: Option<String>,
    session: Option<String>, db: Option<String>, system: Option<String>,
}

fn parse_cli(args: &[String]) -> Cli {
    let mut mode = Mode::Run;
    let mut provider = "fake".to_string();
    let mut model = None;
    let mut base_url = None;
    let mut session = None;
    let mut db = None;
    let mut system = None;
    let mut positional = Vec::new();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "run" => { mode = Mode::Run; i += 1; }
            "chat" => { mode = Mode::Chat; i += 1; }
            "--provider" if i + 1 < args.len() => { provider = args[i + 1].clone(); i += 2; }
            "--model" if i + 1 < args.len() => { model = Some(args[i + 1].clone()); i += 2; }
            "--base-url" if i + 1 < args.len() => { base_url = Some(args[i + 1].clone()); i += 2; }
            "--session" if i + 1 < args.len() => { session = Some(args[i + 1].clone()); i += 2; }
            "--db" if i + 1 < args.len() => { db = Some(args[i + 1].clone()); i += 2; }
            "--system" if i + 1 < args.len() => { system = Some(args[i + 1].clone()); i += 2; }
            s => { positional.push(s.to_string()); i += 1; }
        }
    }
    Cli { mode, provider, prompt: positional.into_iter().next(), model, base_url, session, db, system }
}
