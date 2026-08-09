//! Live OpenAI smoke test. Ignored by default — run with:
//!   OPENAI_API_KEY=... [OPENAI_BASE_URL=...] cargo test -p meepo-providers -- --ignored --nocapture

use futures::stream::StreamExt;
use meepo_core::{AgentBackend, BackendSendInput, ChatMessage, SessionEvent};
use meepo_providers::OpenAiBackend;

#[tokio::test]
#[ignore]
async fn live_openai_streams_text() {
    let mut backend = OpenAiBackend::from_env("live", "gpt-4o-mini").unwrap();
    let input = BackendSendInput {
        turn_id: "t".into(),
        run_id: None,
        invocation_id: None,
        max_steps: None,
        messages: vec![ChatMessage::User {
            content: "Reply with exactly: pong".into(),
        }],
        tools: vec![],
    };
    let mut s = backend.send(&input);
    let mut text = String::new();
    let mut terminal = false;
    while let Some(ev) = s.next().await {
        match ev {
            SessionEvent::TextDelta { text: t, .. } => text.push_str(&t),
            SessionEvent::Complete { .. } => terminal = true,
            SessionEvent::Error { message, .. } => panic!("backend error: {message}"),
            _ => {}
        }
    }
    eprintln!("openai replied: {text}");
    assert!(!text.trim().is_empty(), "got no text");
    assert!(terminal, "stream did not produce a terminal event");
}
