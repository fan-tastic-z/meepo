//! Recovery-bundle fact contracts — the durable `actions.toolRecovery`
//! payloads a recovery continuation appends to adjudicate an indeterminate
//! tool operation after a crash.
//!
//! A bundle is one `reconcile_result` fact (what a re-execution observed:
//! matches expected / prior state, diverged, unreadable) followed by one
//! terminal `recovery_decision` fact (completed — synthesized a successful
//! outcome; or parked — needs human action). The resolver consumes them via
//! `interpret_scanned_tool_recovery` to classify the operation.

use serde::{Deserialize, Serialize};

/// `reconcile_result` fact kind.
pub const TOOL_RECONCILE_RESULT_FACT_KIND: &str = "maka.tool.reconcile_result";
/// `recovery_decision` fact kind.
pub const TOOL_RECOVERY_DECISION_FACT_KIND: &str = "maka.tool.recovery_decision";
/// Envelope version.
pub const TOOL_RECOVERY_FACT_VERSION: u32 = 1;

/// What a re-execution of a tool operation observed about external state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolReconcileObservation {
    /// External state matches what the original dispatch would have produced.
    MatchesExpectedState,
    /// External state matches a prior known state (idempotent re-result).
    MatchesPriorState,
    /// External state diverged from any known state.
    Diverged,
    /// External state could not be read.
    Unreadable,
}

/// The `reconcile_result` payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolReconcileResultFact {
    pub protocol: String,
    pub operation_id: String,
    pub observation: ToolReconcileObservation,
    pub observation_schema: String,
    pub observation_digest: String,
}

/// Terminal disposition a `recovery_decision` asserts about the operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRecoveryDisposition {
    /// The operation is settled as complete (a successful outcome was synthesized).
    Completed,
    /// The operation is parked for human action.
    Parked,
}

/// The `recovery_decision` payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolRecoveryDecisionFact {
    pub protocol: String,
    pub operation_id: String,
    pub disposition: ToolRecoveryDisposition,
    pub reason_code: String,
    pub evidence_event_ids: Vec<String>,
    /// Required when disposition is Completed (the synthesized outcome event).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub outcome_event_id: Option<String>,
}

/// The `actions.toolRecovery` envelope, discriminated by `kind`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum ToolRecoveryFactEnvelope {
    #[serde(rename = "maka.tool.reconcile_result")]
    ReconcileResult {
        version: u32,
        payload: ToolReconcileResultFact,
    },
    #[serde(rename = "maka.tool.recovery_decision")]
    RecoveryDecision {
        version: u32,
        payload: ToolRecoveryDecisionFact,
    },
}

impl ToolRecoveryFactEnvelope {
    pub const fn kind_str(&self) -> &'static str {
        match self {
            Self::ReconcileResult { .. } => TOOL_RECONCILE_RESULT_FACT_KIND,
            Self::RecoveryDecision { .. } => TOOL_RECOVERY_DECISION_FACT_KIND,
        }
    }
}
