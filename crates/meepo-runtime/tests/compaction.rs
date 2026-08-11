//! Context compaction behavior.

use meepo_core::{AssistantToolCall, ChatMessage, FakeBackend};
use meepo_runtime::compact_if_needed_with;
use serde_json::json;

fn user(text: &str) -> ChatMessage {
    ChatMessage::User { content: text.into() }
}
fn assistant(text: &str) -> ChatMessage {
    ChatMessage::Assistant { content: Some(text.into()), tool_calls: vec![] }
}
fn big_user(n: usize) -> ChatMessage {
    ChatMessage::User { content: "x".repeat(n) }
}

#[tokio::test]
async fn below_threshold_is_noop() {
    let backend = FakeBackend::new("s", vec![]);
    let msgs = vec![user("hi"), assistant("hello")];
    let out = compact_if_needed_with(&backend, &msgs, 10_000, 2).await;
    assert_eq!(out.len(), msgs.len());
    assert_eq!(out, msgs);
}

#[tokio::test]
async fn above_threshold_folds_prefix_into_summary() {
    let backend = FakeBackend::new("s", vec![]);
    // 8 messages, total > threshold, keep_recent 3 → prefix 5, tail 3
    let msgs: Vec<ChatMessage> = (0..8).map(|_| big_user(1000)).collect();
    let out = compact_if_needed_with(&backend, &msgs, 5_000, 3).await;
    // [summary] + 3 tail
    assert_eq!(out.len(), 4);
    // first is the summary user message
    assert!(matches!(&out[0], ChatMessage::User { content } if content.starts_with("[conversation summary]")));
    // tail preserved
    assert!(matches!(&out[1], ChatMessage::User { .. }));
}

#[tokio::test]
async fn does_not_start_tail_with_tool_message() {
    // If the split point lands right before a Tool message, that Tool must be
    // moved into the prefix (a Tool message needs a preceding assistant
    // tool_calls).
    let backend = FakeBackend::new("s", vec![]);
    let mut msgs: Vec<ChatMessage> = (0..5).map(|_| big_user(1000)).collect();
    msgs.push(ChatMessage::Assistant {
        content: None,
        tool_calls: vec![AssistantToolCall { id: "c1".into(), name: "f".into(), args: json!({}) }],
    });
    msgs.push(ChatMessage::Tool { tool_call_id: "c1".into(), content: "result".into() });
    msgs.push(user("tail"));
    // keep_recent 2 would put Tool at tail[0] — it must move to prefix.
    let out = compact_if_needed_with(&backend, &msgs, 5_000, 2).await;
    // tail must NOT start with Tool
    assert!(!matches!(out.get(1), Some(ChatMessage::Tool { .. })));
}
