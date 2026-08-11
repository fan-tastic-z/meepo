//! Context compaction behavior (rolling).

use meepo_core::{AssistantToolCall, ChatMessage, FakeBackend};
use meepo_runtime::compact_if_needed_with;
use serde_json::json;

fn user(text: &str) -> ChatMessage {
    ChatMessage::User { content: text.into() }
}
fn assistant(text: &str) -> ChatMessage {
    ChatMessage::Assistant { content: Some(text.into()), tool_calls: vec![], thinking: vec![] }
}
fn big_user(n: usize) -> ChatMessage {
    ChatMessage::User { content: "x".repeat(n) }
}

#[tokio::test]
async fn below_threshold_is_noop() {
    let backend = FakeBackend::new("s", vec![]);
    let msgs = vec![user("hi"), assistant("hello")];
    let result = compact_if_needed_with(&backend, &msgs, 10_000, 2, None).await;
    assert!(result.summary.is_none());
    assert_eq!(result.messages.len(), msgs.len());
}

#[tokio::test]
async fn above_threshold_folds_prefix_into_summary() {
    let backend = FakeBackend::new("s", vec![]);
    let msgs: Vec<ChatMessage> = (0..8).map(|_| big_user(1000)).collect();
    let result = compact_if_needed_with(&backend, &msgs, 5_000, 3, None).await;
    assert!(result.summary.is_some());
    assert_eq!(result.messages.len(), 4); // [summary] + 3 tail
    assert!(matches!(&result.messages[0], ChatMessage::User { content } if content.starts_with("[conversation summary]")));
}

#[tokio::test]
async fn rolling_uses_previous_summary() {
    let backend = FakeBackend::new("s", vec![]);
    let msgs: Vec<ChatMessage> = (0..8).map(|_| big_user(1000)).collect();
    // First compaction (no previous summary).
    let result1 = compact_if_needed_with(&backend, &msgs, 5_000, 3, None).await;
    assert!(result1.summary.is_some());
    // Second compaction with previous summary (rolling).
    let result2 = compact_if_needed_with(&backend, &msgs, 5_000, 3, result1.summary.as_deref()).await;
    assert!(result2.summary.is_some());
    // Both produce [summary] + tail, but rolling uses different summarizer input.
    assert_eq!(result2.messages.len(), 4);
}

#[tokio::test]
async fn does_not_start_tail_with_tool_message() {
    let backend = FakeBackend::new("s", vec![]);
    let mut msgs: Vec<ChatMessage> = (0..5).map(|_| big_user(1000)).collect();
    msgs.push(ChatMessage::Assistant {
        content: None,
        tool_calls: vec![AssistantToolCall { id: "c1".into(), name: "f".into(), args: json!({}) }],
        thinking: vec![],
    });
    msgs.push(ChatMessage::Tool { tool_call_id: "c1".into(), content: "result".into() });
    msgs.push(user("tail"));
    let result = compact_if_needed_with(&backend, &msgs, 5_000, 2, None).await;
    assert!(!matches!(result.messages.get(1), Some(ChatMessage::Tool { .. })));
}
