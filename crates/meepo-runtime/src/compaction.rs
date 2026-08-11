//! Context compaction: fold an older message prefix into a single summary.
//!
//! This is a **projection** — the store ledger is never mutated. Only the
//! working `messages` slice sent to the backend is replaced by
//! `[summary] + recent_tail` when its estimated size exceeds a threshold.
//! Inspired by maka's "compaction is a projection, not a mutation" principle
//! (simplified: no durable checkpoint, no digest, no rolling yet).

use meepo_core::{AgentBackend, ChatMessage};

/// Below this many characters of estimated message text, no compaction runs.
const DEFAULT_THRESHOLD_CHARS: usize = 16_000;
/// Keep at least this many recent messages as the raw tail.
const DEFAULT_KEEP_RECENT: usize = 6;

/// Compact `messages` if it exceeds the default threshold.
pub async fn compact_if_needed<B: AgentBackend + ?Sized>(
    backend: &B,
    messages: &[ChatMessage],
) -> Vec<ChatMessage> {
    compact_if_needed_with(backend, messages, DEFAULT_THRESHOLD_CHARS, DEFAULT_KEEP_RECENT).await
}

/// Compact with explicit thresholds. No-op when below threshold or too few
/// messages to split.
pub async fn compact_if_needed_with<B: AgentBackend + ?Sized>(
    backend: &B,
    messages: &[ChatMessage],
    threshold_chars: usize,
    keep_recent: usize,
) -> Vec<ChatMessage> {
    let total_chars: usize = messages.iter().map(message_char_len).sum();
    if total_chars <= threshold_chars || messages.len() <= keep_recent {
        return messages.to_vec();
    }

    let split = messages.len().saturating_sub(keep_recent);
    let mut prefix: Vec<ChatMessage> = messages[..split].to_vec();
    let mut tail: Vec<ChatMessage> = messages[split..].to_vec();

    // Never start the tail with a Tool message — it must follow an assistant
    // tool_calls. Move any leading Tool messages back into the prefix.
    while matches!(tail.first(), Some(ChatMessage::Tool { .. })) {
        prefix.push(tail.remove(0));
    }

    let summary = backend
        .compact_history(&prefix)
        .await
        .unwrap_or_else(|e| format!("[compaction failed: {e}]"));

    let mut result = Vec::with_capacity(tail.len() + 1);
    result.push(ChatMessage::User {
        content: format!("[conversation summary]\n{summary}"),
    });
    result.extend(tail);
    result
}

fn message_char_len(m: &ChatMessage) -> usize {
    match m {
        ChatMessage::User { content } => content.chars().count(),
        ChatMessage::Assistant { content, tool_calls } => {
            let base = content.as_deref().map(str::len).unwrap_or(0);
            base + tool_calls
                .iter()
                .map(|tc| tc.name.len() + tc.args.to_string().len())
                .sum::<usize>()
        }
        ChatMessage::Tool { content, .. } => content.chars().count(),
    }
}
