//! meepo-cli — command-line entry point.
//!
//! `meepo run [--provider fake|openai] [--model M] [--base-url U] <prompt>`
//! drives one turn (with the tool step-loop) and prints the collected text.

use meepo_core::{AgentBackend, BackendSendInput, ChatMessage, Content, FakeBackend, SessionEvent, StopReason};
use meepo_providers::OpenAiBackend;
use meepo_runtime::{InvocationContext, RuntimeRunner};
use meepo_tools::{ReadFile, ToolRegistry};

const DEFAULT_OPENAI_MODEL: &str = "gpt-4o-mini";

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let cli = parse_cli(&args);
    let prompt = match cli.prompt {
        Some(p) => p,
        None => {
            eprintln!(
                "usage: meepo run [--provider fake|openai] [--model M] [--base-url U] <prompt>"
            );
            std::process::exit(2);
        }
    };

    let session_id = "cli-session";
    let mut backend: Box<dyn AgentBackend> = match cli.provider.as_str() {
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
            let base_url = cli
                .base_url
                .clone()
                .or_else(|| std::env::var("OPENAI_BASE_URL").ok());
            if let Some(url) = base_url {
                b = b.with_base_url(url);
            }
            Box::new(b)
        }
        _ => Box::new(FakeBackend::new(session_id, fake_script(&prompt))),
    };

    let mut tools = ToolRegistry::new();
    tools.register(Box::new(ReadFile));

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
        messages: vec![ChatMessage::User { content: prompt }],
        tools: tools.openai_functions(),
    };

    let result = RuntimeRunner::run(&mut *backend, &ctx, &input, &tools).await;

    for ev in &result.events {
        match &ev.content {
            Some(Content::Text { text, .. }) => print!("{text}"),
            Some(Content::FunctionResponse { result, is_error, .. }) if !is_error.unwrap_or(false) => {
                eprintln!("[tool result] {result}");
            }
            _ => {}
        }
    }
    println!();
    eprintln!(
        "[provider: {}, turn status: {:?}, {} events]",
        cli.provider,
        result.status,
        result.events.len()
    );
}

fn fake_script(prompt: &str) -> Vec<SessionEvent> {
    vec![
        SessionEvent::TextComplete {
            id: "1".into(),
            turn_id: "t1".into(),
            ts: 0,
            message_id: "m1".into(),
            text: format!("meepo (fake backend): {prompt}"),
            provider_options: None,
        },
        SessionEvent::Complete {
            id: "2".into(),
            turn_id: "t1".into(),
            ts: 1,
            stop_reason: StopReason::EndTurn,
        },
    ]
}

struct Cli {
    provider: String,
    prompt: Option<String>,
    model: Option<String>,
    base_url: Option<String>,
}

fn parse_cli(args: &[String]) -> Cli {
    let mut provider = "fake".to_string();
    let mut model = None;
    let mut base_url = None;
    let mut positional: Vec<String> = Vec::new();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "run" => i += 1,
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
        provider,
        prompt: positional.into_iter().next(),
        model,
        base_url,
    }
}
