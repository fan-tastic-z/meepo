//! SessionEvent — the backend → consumer event-stream vocabulary.
//!
//! Backends normalize provider-native streams to this union; the runner maps
//! these into canonical RuntimeEvents. Phase 0/1 models text + terminal +
//! tool-call/result variants; thinking, permission, and usage variants arrive
//! later.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Reason a turn terminated normally.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    EndTurn,
    UserStop,
    Error,
    PlanHandoff,
    GraphYield,
    PermissionHandoff,
    StepLimit,
    MaxTokens,
    ContextBudgetExhausted,
}

/// Reason an in-flight turn was aborted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AbortReason {
    UserStop,
    Redirect,
    Timeout,
    Crash,
}

/// Backend stream vocabulary, discriminated by `type`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum SessionEvent {
    #[serde(rename = "text_delta", rename_all = "camelCase")]
    TextDelta {
        id: String,
        turn_id: String,
        ts: i64,
        message_id: String,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        start_offset: Option<u32>,
        text: String,
    },
    #[serde(rename = "text_complete", rename_all = "camelCase")]
    TextComplete {
        id: String,
        turn_id: String,
        ts: i64,
        message_id: String,
        text: String,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        provider_options: Option<Value>,
    },
    /// Model thinking delta (extended thinking / reasoning).
    #[serde(rename = "thinking_delta", rename_all = "camelCase")]
    ThinkingDelta {
        id: String,
        turn_id: String,
        ts: i64,
        message_id: String,
        text: String,
    },
    /// Complete thinking block with signature (for signed thinking replay).
    #[serde(rename = "thinking_complete", rename_all = "camelCase")]
    ThinkingComplete {
        id: String,
        turn_id: String,
        ts: i64,
        message_id: String,
        text: String,
        /// Anthropic signed thinking — MUST be re-sent on replay when present.
        #[serde(skip_serializing_if = "Option::is_none", default)]
        signature: Option<String>,
    },
    /// Model requested a tool call.
    #[serde(rename = "tool_call", rename_all = "camelCase")]
    ToolCall {
        id: String,
        turn_id: String,
        ts: i64,
        /// Provider tool-call id (matches the tool-result and the history entry).
        tool_call_id: String,
        tool_name: String,
        args: Value,
    },
    /// A tool's result, produced by the runner after execution.
    #[serde(rename = "tool_result", rename_all = "camelCase")]
    ToolResult {
        id: String,
        turn_id: String,
        ts: i64,
        tool_call_id: String,
        tool_name: String,
        content: String,
        is_error: bool,
    },
    #[serde(rename_all = "camelCase")]
    Complete {
        id: String,
        turn_id: String,
        ts: i64,
        stop_reason: StopReason,
    },
    #[serde(rename_all = "camelCase")]
    Error {
        id: String,
        turn_id: String,
        ts: i64,
        recoverable: bool,
        message: String,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        code: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        reason: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        details: Option<Value>,
    },
    #[serde(rename_all = "camelCase")]
    Abort {
        id: String,
        turn_id: String,
        ts: i64,
        reason: AbortReason,
    },
}
