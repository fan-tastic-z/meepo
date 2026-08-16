//! meepo-cli — command-line entry point.
//!
//! `meepo run [...] <prompt>` — one shot. `meepo chat [...]` — multi-turn REPL
//! with SQLite persistence. Output is streamed. The backend owns the tool loop
//! (executor injected at construction); the runner just consumes the stream.
//! Session lifecycle (recovery, persistence, status) is delegated to
//! SessionManager.

use std::io::{self, BufRead, Write};
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::StreamExt;

use meepo_core::{
    AgentBackend, ChatMessage, Content, DefaultPermissionGate, FakeBackend, PermissionAnswer,
    PermissionDecision, PermissionGate, PermissionMode, PermissionPrompter, PermissionRequest,
    Role, RuntimeEventStore, SessionEvent, StopReason,
};
use meepo_providers::AimuxBackend;
use meepo_runtime::{
    InvocationContext, RuntimeRunner, RunStatus, SessionManager, TurnEvent, DEFAULT_SYSTEM_PROMPT,
};
use meepo_sandbox::{MacosSeatbeltBackend, SandboxManager};
use meepo_storage::SqliteStore;
use meepo_headless::{run_task, DefaultSelfCheckGate, ModelSelfCheckGate, TaskDefinition, TaskRunStore};
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
        Mode::Headless => {
            let instruction = cli.prompt.clone().unwrap_or_else(|| {
                eprintln!("usage: meepo headless [--provider ...] [--max-attempts N] <instruction>");
                std::process::exit(2);
            });
            run_headless(&session_id, cli, &tools, &instruction).await;
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

// ── Streaming helpers (shared by run and chat) ──

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

async fn drive_turn_streaming(
    backend: &mut dyn AgentBackend,
    ctx: &InvocationContext,
    input: &meepo_core::BackendSendInput,
    previous_compact_summary: Option<&str>,
) -> (Vec<meepo_core::RuntimeEvent>, RunStatus) {
    let mut collected = Vec::new();
    let mut status = RunStatus::Failed;
    let mut stream = Box::pin(RuntimeRunner::run_stream(
        backend,
        ctx.clone(),
        input.clone(),
        previous_compact_summary.map(String::from),
        meepo_core::StopToken::never(),
    ));
    while let Some(te) = stream.next().await {
        match te {
            TurnEvent::Event(re) => {
                print_event_live(&re);
                collected.push(re);
            }
            TurnEvent::Done { status: s, .. } => {
                status = s;
            }
        }
    }
    (collected, status)
}

// ── Run (single turn, no persistence) ──

async fn run_single_turn(session_id: &str, cli: Cli, tools: &Arc<ToolRegistry>, prompt: &str) {
    let mut backend = build_backend(session_id, &cli, prompt, tools, None);
    let ctx = InvocationContext {
        session_id: session_id.into(), run_id: "r1".into(),
        invocation_id: "inv1".into(), turn_id: "t1".into(),
    };
    let input = meepo_core::BackendSendInput {
        turn_id: "t1".into(), run_id: Some("r1".into()),
        invocation_id: Some("inv1".into()), max_steps: None,
        messages: vec![ChatMessage::User { content: prompt.to_string() }],
        system_prompt: Some(resolve_system(&cli)),
        tools: vec![],
    };
    let (_events, status) = drive_turn_streaming(&mut *backend, &ctx, &input, None).await;
    println!();
    eprintln!("[provider: {}, turn status: {:?}]", cli.provider, status);
}

// ── Chat (multi-turn REPL, with SessionManager) ──

async fn run_chat(session_id: &str, cli: Cli, tools: &Arc<ToolRegistry>, db_path: &str) {
    let store = match SqliteStore::open(db_path) {
        Ok(s) => Arc::new(s),
        Err(e) => { eprintln!("open store {db_path}: {e}"); std::process::exit(2); }
    };

    // Resume via SessionManager (recovery + projection handled internally).
    let mut session = SessionManager::resume(session_id, &*store).await;

    if session.recovery_needed() {
        eprintln!("[recovery] ⚠️ Previous session had incomplete tool operations; orphaned calls dropped.");
    }
    if !session.messages().is_empty() {
        let health = if session.recovery_needed() { "repaired" } else { "healthy" };
        eprintln!(
            "(resumed session '{session_id}': {} prior messages from {db_path}, {health})",
            session.messages().len()
        );
    }

    let system_prompt = resolve_system(&cli);
    let stdin = io::stdin();
    println!("meepo chat (provider: {}, session: {session_id}, db: {db_path}). Ctrl-D to exit.", cli.provider);

    loop {
        print!("> "); io::stdout().flush().ok();
        let mut line = String::new();
        if stdin.lock().read_line(&mut line).unwrap_or(0) == 0 { break; }
        let line = line.trim_end_matches('\n').to_string();
        if line.is_empty() { continue; }

        // Build a fresh backend per turn (stateless; history lives in SessionManager).
        let mut backend = build_backend(session_id, &cli, &line, tools, Some(store.clone()));

        // Drive the turn through SessionManager, streaming each event live.
        let turn_result = session
            .send_turn_streaming(
                &mut *backend,
                &*store,
                line.clone(),
                Some(system_prompt.clone()),
                &[],
                print_event_live,
            )
            .await;

        println!();
        eprintln!("[turn status: {:?}]", turn_result.status);
    }
}

// ── Permission prompter (CLI stdin) ──

struct CliPrompter;

#[async_trait]
impl PermissionPrompter for CliPrompter {
    async fn ask(&self, request: &PermissionRequest) -> PermissionAnswer {
        eprintln!();
        eprintln!("── permission request ──");
        eprintln!("  tool : {}   category: {:?}", request.tool_name, request.category);
        eprintln!("  what : {}", request.summary);
        eprint!("allow? [y/N] ");
        io::stderr().flush().ok();
        let mut line = String::new();
        let n = io::stdin().lock().read_line(&mut line).unwrap_or(0);
        let allow = n > 0 && line.trim().eq_ignore_ascii_case("y");
        PermissionAnswer {
            decision: if allow { PermissionDecision::Allow } else { PermissionDecision::Deny },
            remember_for_turn: false,
        }
    }
}

// ── Headless (durable task) ──

async fn run_headless(session_id: &str, cli: Cli, tools: &Arc<ToolRegistry>, instruction: &str) {
    let db = cli.db.clone().unwrap_or_else(default_db);
    let store = match SqliteStore::open(&db) {
        Ok(s) => Arc::new(s),
        Err(e) => { eprintln!("open store {db}: {e}"); std::process::exit(2); }
    };
    let task = TaskDefinition {
        task_id: format!("task-{session_id}"),
        instruction: instruction.to_string(),
        workspace_dir: std::env::current_dir()
            .map(|p| p.display().to_string())
            .unwrap_or_default(),
    };
    let mut backend = build_backend(session_id, &cli, instruction, tools, Some(store.clone()));
    let max = cli.max_attempts.unwrap_or(3);
    let gate: Box<dyn meepo_headless::SelfCheckGate> = if cli.self_check {
        Box::new(ModelSelfCheckGate)
    } else {
        Box::new(DefaultSelfCheckGate)
    };
    let run = run_task(
        &format!("headless-{session_id}"),
        &mut *backend,
        &*store,
        &*store,
        &task,
        max,
        &*gate,
    )
    .await;
    eprintln!("[headless task: status={:?}, attempts={}]", run.status, run.attempt_count);
}

// ── Backend factory ──

fn build_backend(session_id: &str, cli: &Cli, prompt: &str, tools: &Arc<ToolRegistry>, store: Option<Arc<SqliteStore>>) -> Box<dyn AgentBackend + Send + Sync> {
    let gate: Option<Arc<dyn PermissionGate>> = if cli.permission_mode == PermissionMode::Bypass {
        None
    } else {
        Some(Arc::new(DefaultPermissionGate::new(
            cli.permission_mode,
            Arc::new(CliPrompter),
        )))
    };
    match cli.provider.as_str() {
        "aimux" | "openai" => {
            let key = match std::env::var("OPENAI_API_KEY") {
                Ok(k) => k,
                Err(_) => { eprintln!("OPENAI_API_KEY is not set"); std::process::exit(2); }
            };
            let model_name = cli.model.clone().unwrap_or_else(|| DEFAULT_OPENAI_MODEL.to_string());
            let base_url = cli.base_url.clone()
                .or_else(|| std::env::var("OPENAI_BASE_URL").ok())
                .unwrap_or_else(|| "https://api.openai.com/v1".to_string());
            let config = aimux_providers::openai::OpenAIConfig::new(&key)
                .with_base_url(&base_url);
            let provider = aimux_providers::openai::OpenAIProvider::new(config);
            let model = provider.model(&model_name);
            let mut backend = AimuxBackend::new(session_id, Box::new(model))
                .with_executor(tools.clone());
            if let Some(g) = &gate {
                backend = backend.with_permission_gate(g.clone());
            }
            if let Some(s) = &store {
                backend = backend.with_interaction_store(s.clone());
                backend = backend.with_tool_operation_store(s.clone());
            }
            Box::new(backend)
        }
        "anthropic" => {
            let base_url = cli.base_url.clone()
                .or_else(|| std::env::var("ANTHROPIC_BASE_URL").ok())
                .unwrap_or_else(|| "https://api.anthropic.com".to_string());
            // Bearer-token providers (e.g. an Anthropic-compatible endpoint via
            // ANTHROPIC_AUTH_TOKEN) coexist with native x-api-key (ANTHROPIC_API_KEY).
            let auth_token = std::env::var("ANTHROPIC_AUTH_TOKEN").ok();
            let api_key = std::env::var("ANTHROPIC_API_KEY").ok();
            if api_key.is_none() && auth_token.is_none() {
                eprintln!("ANTHROPIC_API_KEY or ANTHROPIC_AUTH_TOKEN is not set");
                std::process::exit(2);
            }
            let model_name = cli.model.clone().unwrap_or_else(|| "claude-sonnet-4-20250514".to_string());
            let config = aimux_providers::anthropic::AnthropicConfig {
                api_key: api_key.unwrap_or_default(),
                auth_token,
                base_url,
                api_version: "2023-06-01".to_string(),
                name: "anthropic".to_string(),
                headers: None,
                retry_config: Default::default(),
                body_overrides: None,
                api_key_source: None,
            };
            let provider = aimux_providers::anthropic::AnthropicProvider::new(config);
            let model = provider.model(&model_name);
            let mut backend = AimuxBackend::new(session_id, Box::new(model))
                .with_executor(tools.clone());
            if let Some(g) = &gate {
                backend = backend.with_permission_gate(g.clone());
            }
            if let Some(s) = &store {
                backend = backend.with_interaction_store(s.clone());
                backend = backend.with_tool_operation_store(s.clone());
            }
            Box::new(backend)
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

// ── CLI parsing ──

#[derive(Clone, Copy)]
enum Mode { Run, Chat, Headless }

struct Cli {
    mode: Mode, provider: String, prompt: Option<String>,
    model: Option<String>, base_url: Option<String>,
    session: Option<String>, db: Option<String>, system: Option<String>,
    permission_mode: PermissionMode,
    max_attempts: Option<u32>,
    self_check: bool,
}

fn parse_cli(args: &[String]) -> Cli {
    let mut mode = Mode::Run;
    let mut provider = "fake".to_string();
    let mut model = None;
    let mut base_url = None;
    let mut session = None;
    let mut db = None;
    let mut system = None;
    let mut permission_mode = PermissionMode::Ask;
    let mut max_attempts: Option<u32> = None;
    let mut self_check = false;
    let mut positional = Vec::new();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "run" => { mode = Mode::Run; i += 1; }
            "chat" => { mode = Mode::Chat; i += 1; }
            "headless" => { mode = Mode::Headless; i += 1; }
            "--provider" if i + 1 < args.len() => { provider = args[i + 1].clone(); i += 2; }
            "--model" if i + 1 < args.len() => { model = Some(args[i + 1].clone()); i += 2; }
            "--base-url" if i + 1 < args.len() => { base_url = Some(args[i + 1].clone()); i += 2; }
            "--session" if i + 1 < args.len() => { session = Some(args[i + 1].clone()); i += 2; }
            "--db" if i + 1 < args.len() => { db = Some(args[i + 1].clone()); i += 2; }
            "--system" if i + 1 < args.len() => { system = Some(args[i + 1].clone()); i += 2; }
            "--mode" if i + 1 < args.len() => {
                permission_mode = match args[i + 1].as_str() {
                    "explore" => PermissionMode::Explore,
                    "ask" => PermissionMode::Ask,
                    "execute" => PermissionMode::Execute,
                    "bypass" => PermissionMode::Bypass,
                    other => {
                        eprintln!("unknown --mode '{other}'; use explore|ask|execute|bypass");
                        std::process::exit(2);
                    }
                };
                i += 2;
            }
            "--self-check" => { self_check = true; i += 1; }
            "--max-attempts" if i + 1 < args.len() => {
                max_attempts = args[i + 1].parse().ok();
                i += 2;
            }
            s => { positional.push(s.to_string()); i += 1; }
        }
    }
    Cli {
        mode, provider, prompt: positional.into_iter().next(),
        model, base_url, session, db, system, permission_mode, max_attempts, self_check,
    }
}
