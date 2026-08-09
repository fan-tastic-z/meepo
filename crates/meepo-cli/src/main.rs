//! meepo-cli — command-line entry point.
//!
//! `meepo run [...] <prompt>`  — one shot, one turn.
//! `meepo chat [...]`         — multi-turn REPL (history accumulates across turns).
//!
//! `--provider fake|openai`, `--model M`, `--base-url U` apply to both.

use std::io::{self, BufRead, Write};

use meepo_core::{
    AgentBackend, BackendSendInput, ChatMessage, Content, FakeBackend, SessionEvent, StopReason,
};
use meepo_providers::OpenAiBackend;
use meepo_runtime::{InvocationContext, RuntimeRunner};
use meepo_tools::{ReadFile, ToolRegistry};

const DEFAULT_OPENAI_MODEL: &str = "gpt-4o-mini";

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let cli = parse_cli(&args);

    let session_id = "cli-session";
    let tools = {
        let mut t = ToolRegistry::new();
        t.register(Box::new(ReadFile));
        t
    };

    match cli.mode {
        Mode::Chat => run_chat(session_id, cli, &tools).await,
        Mode::Run => {
            let prompt = match &cli.prompt {
                Some(p) => p.clone(),
                None => {
                    eprintln!("usage: meepo run [--provider fake|openai] [--model M] [--base-url U] <prompt>");
                    std::process::exit(2);
                }
            };
            run_single_turn(session_id, cli, &tools, &prompt, vec![ChatMessage::User {
                content: prompt.clone(),
            }])
            .await;
        }
    }
}

async fn run_single_turn(
    session_id: &str,
    cli: Cli,
    tools: &ToolRegistry,
    prompt_display: &str,
    messages: Vec<ChatMessage>,
) {
    let mut backend = build_backend(session_id, &cli, prompt_display);
    let ctx = InvocationContext {
        session_id: session_id.into(),
        run_id: "r1".into(),
        invocation_id: "inv1".into(),
        turn_id: "t1".into(),
    };
    let input = BackendSendInput {
        turn_id: "t1".into(),
        run_id: Some("r1".into()),
        invocation_id: Some("inv1".into()),
        max_steps: None,
        messages,
        tools: tools.openai_functions(),
    };
    let result = RuntimeRunner::run(&mut *backend, &ctx, &input, tools).await;
    print_turn(&result.events);
    eprintln!(
        "[provider: {}, turn status: {:?}, {} events]",
        cli.provider,
        result.status,
        result.events.len()
    );
}

async fn run_chat(session_id: &str, cli: Cli, tools: &ToolRegistry) {
    let stdin = io::stdin();
    let mut messages: Vec<ChatMessage> = Vec::new();
    let mut turn = 0u32;
    println!("meepo chat (provider: {}). Ctrl-D to exit.", cli.provider);
    loop {
        print!("> ");
        io::stdout().flush().ok();
        let mut line = String::new();
        if stdin.lock().read_line(&mut line).unwrap_or(0) == 0 {
            break; // EOF (Ctrl-D)
        }
        let line = line.trim_end_matches('\n').to_string();
        if line.is_empty() {
            continue;
        }
        turn += 1;
        messages.push(ChatMessage::User { content: line.clone() });
        // Fresh backend per turn (stateless); history lives in `messages`.
        let mut backend = build_backend(session_id, &cli, &line);
        let ctx = InvocationContext {
            session_id: session_id.into(),
            run_id: format!("r{turn}"),
            invocation_id: format!("inv{turn}"),
            turn_id: format!("t{turn}"),
        };
        let input = BackendSendInput {
            turn_id: format!("t{turn}"),
            run_id: Some(format!("r{turn}")),
            invocation_id: Some(format!("inv{turn}")),
            max_steps: None,
            messages: messages.clone(),
            tools: tools.openai_functions(),
        };
        let result = RuntimeRunner::run(&mut *backend, &ctx, &input, tools).await;
        print_turn(&result.events);
        // Chain history for the next turn.
        messages = result.messages;
    }
}

fn print_turn(events: &[meepo_core::RuntimeEvent]) {
    for ev in events {
        match &ev.content {
            Some(Content::Text { text, .. }) => print!("{text}"),
            Some(Content::FunctionResponse { result, is_error, .. })
                if !is_error.unwrap_or(false) =>
            {
                eprintln!("[tool result] {result}");
            }
            _ => {}
        }
    }
    println!();
}

fn build_backend(session_id: &str, cli: &Cli, prompt: &str) -> Box<dyn AgentBackend> {
    match cli.provider.as_str() {
        "openai" => {
            let key = match std::env::var("OPENAI_API_KEY") {
                Ok(k) => k,
                Err(_) => {
                    eprintln!("OPENAI_API_KEY is not set");
                    std::process::exit(2);
                }
            };
            let model = cli.model.clone().unwrap_or_else(|| DEFAULT_OPENAI_MODEL.to_string());
            let mut b = OpenAiBackend::new(session_id, model, key);
            let base_url = cli.base_url.clone().or_else(|| std::env::var("OPENAI_BASE_URL").ok());
            if let Some(url) = base_url {
                b = b.with_base_url(url);
            }
            Box::new(b)
        }
        _ => Box::new(FakeBackend::new(session_id, fake_script(prompt))),
    }
}

fn fake_script(prompt: &str) -> Vec<SessionEvent> {
    vec![
        SessionEvent::TextComplete {
            id: "1".into(),
            turn_id: "t".into(),
            ts: 0,
            message_id: "m".into(),
            text: format!("meepo (fake backend): {prompt}"),
            provider_options: None,
        },
        SessionEvent::Complete {
            id: "2".into(),
            turn_id: "t".into(),
            ts: 1,
            stop_reason: StopReason::EndTurn,
        },
    ]
}

#[derive(Clone, Copy)]
enum Mode {
    Run,
    Chat,
}

struct Cli {
    mode: Mode,
    provider: String,
    prompt: Option<String>,
    model: Option<String>,
    base_url: Option<String>,
}

fn parse_cli(args: &[String]) -> Cli {
    let mut mode = Mode::Run;
    let mut provider = "fake".to_string();
    let mut model = None;
    let mut base_url = None;
    let mut positional: Vec<String> = Vec::new();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "run" => {
                mode = Mode::Run;
                i += 1;
            }
            "chat" => {
                mode = Mode::Chat;
                i += 1;
            }
            "--provider" if i + 1 < args.len() => {
                provider = args[i + 1].clone();
                i += 2;
            }
            "--model" if i + 1 < args.len() => {
                model = Some(args[i + 1].clone());
                i += 2;
            }
            "--base-url" if i + 1 < args.len() => {
                base_url = Some(args[i + 1].clone());
                i += 2;
            }
            s => {
                positional.push(s.to_string());
                i += 1;
            }
        }
    }
    Cli {
        mode,
        provider,
        prompt: positional.into_iter().next(),
        model,
        base_url,
    }
}
