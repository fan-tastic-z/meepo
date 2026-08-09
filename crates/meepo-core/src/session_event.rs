//! SessionEvent — the backend → consumer event-stream vocabulary.
//!
//! Backends normalize provider-native streams to this union; the runner maps
//! these into canonical RuntimeEvents. Phase 0 models the walking-skeleton
//! subset (text + the terminal states complete/error/abort); thinking, tool,
//! permission, and usage variants arrive in later phases.
//!
//! Every variant carries the BaseEvent fields (`id`, `turnId`, `ts`) inline.
//! Rust enums have no inheritance and we must keep the JSON flat for interop,
//! so the three fields are repeated per variant. Each struct variant also
//! carries its own `rename_all = "camelCase"` — a container-level rename_all
//! on an enum only renames variant names, not fields.

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

/// Walking-skeleton subset of the backend stream vocabulary, discriminated by
/// `type`. The full 25-variant union is filled in across later phases.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum SessionEvent {
    #[serde(rename = "text_delta", rename_all = "camelCase")]
    TextDelta {
        id: String,
        turn_id: String,
        ts: i64,
        message_id: String,
        /// Absolute UTF-16 offset for replay-safe streams; absent for
        /// append-only backends.
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
    #[serde(rename_all = "camelCase")]
    Complete {
        id: String,
        turn_id: String,
        ts: i64,
        stop_reason: StopReason,
        // Additional CompleteEvent fields (tokenUsage, contextBudget, ...)
        // are deferred.
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
        /// Stable machine-readable reason for routing.
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
