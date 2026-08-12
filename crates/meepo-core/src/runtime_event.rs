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
// Actions (control / side-effect intent) and Refs (ledger pointers).
//
// Typed counterparts of the canonical actions/refs envelopes. Fields the
// runtime does not yet populate are kept as opaque `Value` placeholders so the
// structure is complete and round-trips on the wire; they gain typed shapes in
// later phases. The fields the T1/T2 tool-boundary protocol needs —
// `toolDispatch`, `runtimeProtocol`, and `refs.operationId` — are typed now.
// ===========================================================================

/// The T1 tool-boundary protocol marker value. Its presence on a run's first
/// event tells recovery that tool calls go through the durable dispatch path.
pub const TOOL_BOUNDARY_PROTOCOL_V1: &str = "t1_after_preflight_v1";

/// What a later recovery phase may do with a tool side-effect boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRecoveryMode {
    ReplaySafe,
    Idempotent,
    Reconcile,
    Reattach,
    OutcomeUnknown,
    NeverAutoRetry,
}

/// Durable T1 tool-dispatch fact: the runtime crossed the boundary where a
/// tool side effect may have started. Presence does not assert completion.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolDispatch {
    pub protocol: String,
    pub operation_id: String,
    pub provider_tool_call_id: String,
    pub tool_name: String,
    pub canonical_args_hash: String,
    pub recovery_mode: ToolRecoveryMode,
}

/// Marker that the T1 tool-boundary protocol was active from the run's start.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolMarker {
    pub tool_boundary: String,
}

/// Permission decision attached to an event (audit; the canonical outcome
/// remains in the InteractionStore).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PermissionDecisionAction {
    pub request_id: String,
    pub decision: crate::interaction::PermissionDecision,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub remember_for_turn: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub tool_name: Option<String>,
}

/// Control / side-effect intent carried alongside content.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeEventActions {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub state_delta: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub artifact_delta: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub permission_request: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub permission_decision: Option<PermissionDecisionAction>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub permission_answer_accepted: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub permission_closure_accepted: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub user_question_request: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub user_question_answer_accepted: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub transfer_to_agent: Option<String>,
    /// Marks the event that closes the invocation.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub end_invocation: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub token_usage: Option<Value>,
    /// Durable, non-model-visible T1 tool-dispatch fact.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub tool_dispatch: Option<ToolDispatch>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub tool_recovery: Option<Value>,
    /// Protocols active from the first event of this run.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub runtime_protocol: Option<ProtocolMarker>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub continuation_start: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub workspace_fact: Option<Value>,
}

/// Links back to projection/ledger rows. Refs are diagnostics/audit pointers;
/// a missing ref never changes runtime behavior. `tool_call_id` doubles as the
/// matching key for function_call ↔ function_response.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeEventRefs {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub stored_message_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub trace_event_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub provider_event_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub source_message_digest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub provider_request_trace_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub artifact_id: Option<String>,
    /// Runtime-owned durable identity for one tool side-effect boundary.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub operation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub parent_tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub parent_operation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub step_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub source_invocation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub source_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub source_turn_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub source_runtime_event_high_water: Option<i64>,
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

    // --- control / side-effect intent (toolDispatch, runtimeProtocol,
    //     permissionDecision, endInvocation, ...). ---
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub actions: Option<RuntimeEventActions>,

    // --- cross-event references (operationId, toolCallId, ...). ---
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub refs: Option<RuntimeEventRefs>,

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

/// Canonical SHA-256 of `(tool_name, sorted-keys args)` — the digest a T1
/// dispatch fact carries so recovery can authenticate that the dispatched args
/// match the model's function_call args.
pub fn canonical_tool_args_hash(tool_name: &str, args: &Value) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(tool_name.as_bytes());
    hasher.update(b"\x1f");
    hasher.update(sort_value_keys(args.clone()).to_string().as_bytes());
    format!("sha256:{}", hex_lower(&hasher.finalize()))
}

/// Deterministic durable operation id for one tool side-effect boundary.
/// Derived from `(invocation_id, tool_call_id)` so the same provider call
/// replays to the same operation id across crash recovery.
pub fn operation_id(invocation_id: &str, tool_call_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(invocation_id.as_bytes());
    hasher.update(b"\x1f");
    hasher.update(tool_call_id.as_bytes());
    format!("op_{}", hex_lower(&hasher.finalize()))
}

fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_event() -> RuntimeEvent {
        RuntimeEvent {
            session_id: "s".into(),
            invocation_id: "inv".into(),
            run_id: "r".into(),
            turn_id: "t".into(),
            branch: None,
            id: "e1".into(),
            ts: 1,
            role: Role::System,
            author: Author::System,
            origin: None,
            model_visibility: None,
            status: None,
            content: None,
            actions: None,
            refs: None,
            partial: None,
        }
    }

    #[test]
    fn typed_actions_and_refs_round_trip_canonical() {
        let mut event = base_event();
        event.actions = Some(RuntimeEventActions {
            tool_dispatch: Some(ToolDispatch {
                protocol: TOOL_BOUNDARY_PROTOCOL_V1.to_string(),
                operation_id: "op1".into(),
                provider_tool_call_id: "call_1".into(),
                tool_name: "bash".into(),
                canonical_args_hash: "sha256:abc".into(),
                recovery_mode: ToolRecoveryMode::ReplaySafe,
            }),
            runtime_protocol: Some(ProtocolMarker {
                tool_boundary: TOOL_BOUNDARY_PROTOCOL_V1.to_string(),
            }),
            end_invocation: Some(true),
            ..Default::default()
        });
        event.refs = Some(RuntimeEventRefs {
            operation_id: Some("op1".into()),
            tool_call_id: Some("call_1".into()),
            ..Default::default()
        });
        let json = event.to_canonical_json().unwrap();
        // camelCase wire names present (typed fields serialize correctly).
        assert!(json.contains("\"toolDispatch\""));
        assert!(json.contains("\"operationId\""));
        assert!(json.contains("\"runtimeProtocol\""));
        assert!(json.contains("\"endInvocation\""));
        assert!(json.contains("\"t1_after_preflight_v1\""));
        assert!(json.contains("\"replay_safe\""));
        // Round-trip back to the typed form.
        let back: RuntimeEvent = serde_json::from_str(&json).unwrap();
        let actions = back.actions.expect("actions round-trip");
        assert_eq!(actions.tool_dispatch.unwrap().operation_id, "op1");
        assert_eq!(back.refs.unwrap().operation_id, Some("op1".into()));
    }

    #[test]
    fn none_actions_refs_omitted() {
        let event = base_event();
        let json = event.to_canonical_json().unwrap();
        assert!(!json.contains("actions"));
        assert!(!json.contains("refs"));
    }
}
