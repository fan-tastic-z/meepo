//! Context compaction: fold an older message prefix into a single summary,
//! with rolling support (incremental: previous summary + new fold).
//!
//! This is a **projection** — the store ledger is never mutated. Only the
//! working `messages` slice sent to the backend is replaced by
//! `[summary] + recent_tail` when its estimated size exceeds a threshold.

use meepo_core::{AgentBackend, ChatMessage};

const DEFAULT_THRESHOLD_CHARS: usize = 16_000;
const DEFAULT_KEEP_RECENT: usize = 6;

/// Result of a compaction attempt.
pub struct CompactResult {
    /// The (possibly compacted) messages to send to the backend.
    pub messages: Vec<ChatMessage>,
    /// The summary produced (None if no compaction ran). Store this and pass
    /// it as `previous_summary` on the next turn for rolling compaction.
    pub summary: Option<String>,
}

/// Compact if needed, using the default threshold and no previous summary.
pub async fn compact_if_needed<B: AgentBackend + ?Sized>(
    backend: &B,
    messages: &[ChatMessage],
) -> CompactResult {
    compact_if_needed_with(backend, messages, DEFAULT_THRESHOLD_CHARS, DEFAULT_KEEP_RECENT, None).await
}

/// Compact with explicit thresholds and optional rolling previous summary.
///
/// When `previous_summary` is provided, the summarizer receives the previous
/// summary + only the newly folded messages (rolling), instead of the entire
/// prefix from scratch.
pub async fn compact_if_needed_with<B: AgentBackend + ?Sized>(
    backend: &B,
    messages: &[ChatMessage],
    threshold_chars: usize,
    keep_recent: usize,
    previous_summary: Option<&str>,
) -> CompactResult {
    let total_chars: usize = messages.iter().map(message_char_len).sum();
    if total_chars <= threshold_chars || messages.len() <= keep_recent {
        return CompactResult {
            messages: messages.to_vec(),
            summary: None,
        };
    }

    let split = messages.len().saturating_sub(keep_recent);
    let mut prefix: Vec<ChatMessage> = messages[..split].to_vec();
    let mut tail: Vec<ChatMessage> = messages[split..].to_vec();

    // Never start the tail with a Tool message.
    while matches!(tail.first(), Some(ChatMessage::Tool { .. })) {
        prefix.push(tail.remove(0));
    }

    // Rolling: if we have a previous summary, feed (prev_summary + new fold)
    // to the summarizer instead of the entire prefix.
    let summary = if let Some(prev) = previous_summary {
        let mut roll_input = vec![ChatMessage::User {
            content: format!("[previous conversation summary]\n{prev}"),
        }];
        roll_input.extend(prefix.iter().cloned());
        backend.compact_history(&roll_input).await
    } else {
        backend.compact_history(&prefix).await
    }
    .unwrap_or_else(|e| format!("[compaction failed: {e}]"));

    let mut result = Vec::with_capacity(tail.len() + 1);
    result.push(ChatMessage::User {
        content: format!("[conversation summary]\n{summary}"),
    });
    result.extend(tail);

    CompactResult {
        messages: result,
        summary: Some(summary),
    }
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
