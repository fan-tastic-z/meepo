//! meepo-cli — command-line entry point.
//!
//! Phase 1: a `run` subcommand that drives one turn through the runtime with a
//! fake backend and prints the reply. A real provider backend arrives later.

use meepo_core::{BackendSendInput, Content, FakeBackend, SessionEvent, StopReason};
use meepo_runtime::{InvocationContext, RuntimeRunner};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let prompt = match parse_prompt(&args) {
        Some(p) => p,
        None => {
            eprintln!("usage: meepo run <prompt>");
            std::process::exit(2);
        }
    };

    let session_id = "cli-session";
    // Walking-skeleton fake backend: echo the prompt back with a label. A real
    // provider backend replaces this script with a live model stream later.
    let reply = format!("meepo (fake backend): {prompt}");
    let script = vec![
        SessionEvent::TextComplete {
            id: "1".into(),
            turn_id: "t1".into(),
            ts: 0,
            message_id: "m1".into(),
            text: reply,
            provider_options: None,
        },
        SessionEvent::Complete {
            id: "2".into(),
            turn_id: "t1".into(),
            ts: 1,
            stop_reason: StopReason::EndTurn,
        },
    ];

    let mut backend = FakeBackend::new(session_id, script);
    let ctx = InvocationContext {
        session_id: session_id.into(),
        run_id: "r1".into(),
        invocation_id: "inv1".into(),
        turn_id: "t1".into(),
    };
    let input = BackendSendInput {
        turn_id: "t1".into(),
        text: prompt,
        run_id: Some("r1".into()),
        invocation_id: Some("inv1".into()),
        max_steps: None,
    };

    let result = RuntimeRunner::run(&mut backend, &ctx, &input).await;

    // Print every accepted text fragment in commit order.
    for ev in &result.events {
        if let Some(Content::Text { text, .. }) = &ev.content {
            print!("{text}");
        }
    }
    println!();
    eprintln!(
        "[turn status: {:?}, {} events collected]",
        result.status,
        result.events.len()
    );
}

/// Accept either `meepo <prompt>` or `meepo run <prompt>`.
fn parse_prompt(args: &[String]) -> Option<String> {
    match args {
        [p] => Some(p.clone()),
        [cmd, p] if cmd == "run" => Some(p.clone()),
        _ => None,
    }
}
