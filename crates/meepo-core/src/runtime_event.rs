//! Canonical runtime event contract — the single source of truth for the
//! agent runtime.
//!
//! Every execution fact (a user message, model text/thinking, a tool call or
//! result, a termination) is one append-only [`RuntimeEvent`]. Session state,
//! model context, the UI stream, crash recovery, and graph scheduling are all
//! *projections* over a log of these events — never independent sources of
//! truth.
//!
//! Phase 0 scope: identity quintet + role/author/status + the content union +
//! byte-exact canonical serialization. `actions`, `refs`, and `partial` are
//! opaque JSON here and gain typed shapes in later phases.

use serde::{Deserialize, Serialize};
use serde_json::Value;

// ===========================================================================
// Provenance enums. serde `rename_all` maps Rust PascalCase to the exact wire
// strings (lowercase, except `code_mode` which is snake_case).
// ===========================================================================

/// Conversation lane the event plays in model history (user/model/tool/system).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    User,
    Model,
    Tool,
    System,
}

/// Which subsystem authored the fact — orthogonal to [`Role`].
/// `agent` = model + flow orchestration; `tool` = tool execution; `system` =
/// runner/gate/recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Author {
    User,
    Host,
    Agent,
    Tool,
    System,
}

/// Lifecycle status an event asserts about its invocation/turn. Absent on
/// ordinary in-flight content events. Terminal values mark the last event of
/// an invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Streaming,
    Completed,
    Failed,
    Aborted,
    Cancelled,
}

impl Status {
    /// Terminal statuses (completed/failed/aborted/cancelled) end an invocation.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Aborted | Self::Cancelled
        )
    }
}

/// Execution surface that produced a fact. Absent on legacy ledgers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Origin {
    Provider,
    CodeMode,
}

/// Explicit provider-history policy. Absent means `visible`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ModelVisibility {
    Visible,
    Hidden,
}

// ===========================================================================
// Content — the model-facing payload, a 5-variant discriminated union.
//
// `#[serde(tag = "kind")]` yields the internal-tag shape {"kind":"text", ...}.
// NOTE: a container-level `rename_all` on an enum only renames *variant names*
// (the tag values), NOT the fields inside each variant. To get camelCase
// fields (providerOptions, isError, ...) each struct variant needs its own
// `rename_all = "camelCase"`. The two snake_case tags (`function_call`,
// `function_response`) are renamed explicitly on top of that.
// ===========================================================================

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Content {
    #[serde(rename_all = "camelCase")]
    Text {
        text: String,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        provider_options: Option<Value>,
        /// True when a user message is steered into a running turn mid-flight.
        #[serde(skip_serializing_if = "Option::is_none", default)]
        steering: Option<bool>,
        // `origin: TurnOrigin` is deferred — typed once we model TurnOrigin.
    },
    #[serde(rename_all = "camelCase")]
    Thinking {
        text: String,
        /// Signed thinking — MUST be re-sent verbatim on replay when present.
        #[serde(skip_serializing_if = "Option::is_none", default)]
        signature: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        provider_options: Option<Value>,
    },
    #[serde(rename = "function_call", rename_all = "camelCase")]
    FunctionCall {
        /// Matches the tool-call id the provider issued and the response.
        id: String,
        name: String,
        /// Opaque tool arguments (validated elsewhere against the tool schema).
        args: Value,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        provider_options: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        provider_executed: Option<bool>,
    },
    #[serde(rename = "function_response", rename_all = "camelCase")]
    FunctionResponse {
        /// Matches [`Content::FunctionCall`]'s id.
        id: String,
        name: String,
        result: Value,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        is_error: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        provider_executed: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        provider_output: Option<Value>,
    },
    #[serde(rename_all = "camelCase")]
    Error {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        code: Option<String>,
        /// Stable machine-readable reason for routing.
        #[serde(skip_serializing_if = "Option::is_none", default)]
        reason: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        details: Option<Value>,
    },
}

// ===========================================================================
// RuntimeEvent — the canonical fact envelope.
// ===========================================================================

/// The single internal runtime fact model. Not a UI event, not a trace row.
///
/// **Identity quintet — these must never collapse into one another:**
/// - `session_id`    — long-lived session
/// - `turn_id`       — one user-visible exchange
/// - `run_id`        — one agent run execution attempt
/// - `invocation_id` — one runner invocation boundary
/// - (`operation_id` lives in `actions`, deferred) — external idempotency key
///
/// `id`/`ts` are ordering. Recovery creates fresh run/invocation/turn ids; the
/// operation id is what survives across recovery as the external idempotency
/// attempt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeEvent {
    // --- identity ---
    pub session_id: String,
    pub invocation_id: String,
    pub run_id: String,
    pub turn_id: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub branch: Option<String>,

    // --- ordering ---
    /// Monotonic event id within the session ledger.
    pub id: String,
    /// Wall-clock millis when the event was committed.
    pub ts: i64,

    // --- provenance ---
    pub role: Role,
    pub author: Author,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub origin: Option<Origin>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub model_visibility: Option<ModelVisibility>,

    // --- lifecycle ---
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub status: Option<Status>,

    // --- payload ---
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub content: Option<Content>,

    // --- control / side-effect intent: tokenUsage, permission, endInvocation,
    //     toolDispatch, ... Typed as opaque JSON in Phase 0; gains a typed
    //     enum in a later phase. ---
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub actions: Option<Value>,

    // --- cross-event references; deferred. ---
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub refs: Option<Value>,

    /// True for non-terminal partial events.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub partial: Option<bool>,
}

impl RuntimeEvent {
    /// Canonical lossless JSON — the stable identity form for the event
    /// ledger.
    ///
    /// Object keys are alphabetically sorted (matching maka's stableJsonStringify
    /// keys.sort()). We sort explicitly rather than relying on serde_json's
    /// internal BTreeMap, because Cargo feature unification with aimux
    /// (which enables `preserve_order`) would otherwise break alphabetical
    /// ordering.
    pub fn to_canonical_json(&self) -> serde_json::Result<String> {
        let value = serde_json::to_value(self)?;
        let sorted = sort_value_keys(value);
        serde_json::to_string(&sorted)
    }
}

/// Recursively sort all object keys in a serde_json::Value to alphabetical
/// order, regardless of whether serde_json was compiled with preserve_order.
fn sort_value_keys(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut pairs: Vec<(String, Value)> = map
                .into_iter()
                .map(|(k, v)| (k, sort_value_keys(v)))
                .collect();
            pairs.sort_by(|a, b| a.0.cmp(&b.0));
            let mut sorted = serde_json::Map::new();
            for (k, v) in pairs {
                sorted.insert(k, v);
            }
            Value::Object(sorted)
        }
        Value::Array(arr) => Value::Array(arr.into_iter().map(sort_value_keys).collect()),
        other => other,
    }
}
