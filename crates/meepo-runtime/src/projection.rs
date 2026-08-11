//! Projection: rebuild ChatMessage history from a RuntimeEvent ledger.
//!
//! Used to resume a session: read the canonical events back and reconstruct
//! the conversation the model should see. Adjacent model events (text +
//! tool calls) collapse into one Assistant message so the OpenAI wire shape
//! stays valid (no two Assistant messages in a row before a Tool).

use meepo_core::{AssistantToolCall, ChatMessage, Content, Role, RuntimeEvent};
use serde_json::Value;

/// Project ledger events into conversation messages in order.
pub fn messages_from_runtime_events(events: &[RuntimeEvent]) -> Vec<ChatMessage> {
    let mut out: Vec<ChatMessage> = Vec::new();
    let mut pending_text: Option<String> = None;
    let mut pending_calls: Vec<AssistantToolCall> = Vec::new();

    for ev in events {
        match (ev.role, &ev.content) {
            (Role::User, Some(Content::Text { text, .. })) => {
                flush_assistant(&mut out, &mut pending_text, &mut pending_calls);
                out.push(ChatMessage::User { content: text.clone() });
            }
            (Role::Model, Some(Content::Text { text, .. })) => {
                match &mut pending_text {
                    Some(buf) => buf.push_str(text),
                    slot => *slot = Some(text.clone()),
                }
            }
            (Role::Model, Some(Content::FunctionCall { id, name, args, .. })) => {
                pending_calls.push(AssistantToolCall {
                    id: id.clone(),
                    name: name.clone(),
                    args: args.clone(),
                });
            }
            (Role::Tool, Some(Content::FunctionResponse { id, result, .. })) => {
                flush_assistant(&mut out, &mut pending_text, &mut pending_calls);
                out.push(ChatMessage::Tool {
                    tool_call_id: id.clone(),
                    content: value_to_string(result),
                });
            }
            _ => {}
        }
    }
    flush_assistant(&mut out, &mut pending_text, &mut pending_calls);
    out
}

fn flush_assistant(
    out: &mut Vec<ChatMessage>,
    text: &mut Option<String>,
    calls: &mut Vec<AssistantToolCall>,
) {
    if text.is_some() || !calls.is_empty() {
        out.push(ChatMessage::Assistant {
            content: text.take(),
            tool_calls: std::mem::take(calls),
            thinking: vec![],
        });
    }
}

fn value_to_string(v: &Value) -> String {
    v.as_str().map(String::from).unwrap_or_else(|| v.to_string())
}
