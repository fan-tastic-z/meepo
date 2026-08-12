//! Recovery resolver — classifies tool operations from a RuntimeEvent prefix
//! after a crash or interruption, and decides whether the prefix is safe to
//! replay or must be blocked.
//!
//! Consumes [`scan_tool_ledger`] (the single scanning authority) rather than
//! rebuilding its own call/response maps. Crash contract:
//! - A function_call WITH a matching function_response → completed → safe_replay
//! - A function_call WITHOUT a matching response → indeterminate → blocked
//! - Structural corruption (duplicate, orphan response, identity conflict) → blocked
//!
//! Phase scope: 3 states (completed / indeterminate / corruption). The two
//! remaining states — `parked` (recovery-bundle adjudication) and
//! `definitely_not_dispatched` (T1 dispatch protocol) — land once typed
//! actions and tool_dispatch events exist.

use meepo_core::{
    scan_tool_ledger, RuntimeEvent, ToolLedgerIssueCode, ToolLedgerScanOperation,
    ToolRecoveryDisposition, ToolRecoveryFactEnvelope,
};

/// Classification of a single tool operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolRecoveryStatus {
    /// Call has a matching response — safe to replay.
    Completed,
    /// Call exists but no response — the tool may or may not have been
    /// dispatched. Blocking is the fail-closed choice.
    Indeterminate,
    /// Structural corruption (orphan, duplicate, identity conflict).
    Corruption,
    /// Under the T1 protocol, a call with no dispatch fact — provably never
    /// executed. Safe to replay.
    DefinitelyNotDispatched,
    /// A recovery bundle adjudicated the operation as needing human action.
    Parked,
}

/// Reason for the classification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolRecoveryReason {
    MatchingResponse,
    DispatchWithoutResponse,
    NewProtocolBeforeDispatch,
    LegacyDispatchUnknown,
    OrphanResponse,
    OrphanDispatch,
    DuplicateCall,
    DuplicateDispatch,
    DuplicateResponse,
    IdentityConflict,
    DuplicateEventId,
    RecoveryBundleCompleted,
    RecoveryBundleParked,
    RecoveryFactCorruption,
    RecoveryFactWithoutOperation,
}

/// One tool operation's recovery decision.
#[derive(Debug, Clone)]
pub struct ToolRecoveryDecision {
    pub tool_call_id: String,
    pub tool_name: Option<String>,
    pub status: ToolRecoveryStatus,
    pub reason: ToolRecoveryReason,
}

/// Overall recovery resolution for a prefix of events.
#[derive(Debug, Clone)]
pub struct RecoveryResolution {
    /// Per-tool-operation decisions.
    pub decisions: Vec<ToolRecoveryDecision>,
    /// True if any structural corruption was found.
    pub has_corruption: bool,
    /// True if any indeterminate operation exists (no corruption).
    pub has_indeterminate: bool,
    /// The overall plan: safe_replay or blocked.
    pub plan: RecoveryPlan,
}

/// What to do with the prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryPlan {
    /// All tool operations are completed (or there are none). The prefix is
    /// safe to replay to a provider.
    SafeReplay,
    /// At least one tool operation is indeterminate or corrupt. The prefix
    /// must not be replayed without human or higher-phase intervention.
    Blocked,
}

/// Resolve recovery for a sequence of RuntimeEvents.
///
/// Pure: reads events via [`scan_tool_ledger`] and returns decisions. It does
/// not mutate the store or resume execution.
pub fn resolve_recovery(events: &[RuntimeEvent]) -> RecoveryResolution {
    let scan = scan_tool_ledger(events);
    // The T1 tool-boundary protocol is active when the run's first event
    // carries a runtimeProtocol marker. Under it, a call without a dispatch
    // fact is provably unexecuted; without it (legacy), the same shape is
    // indeterminate.
    let t1_protocol = events
        .first()
        .and_then(|e| e.actions.as_ref())
        .and_then(|a| a.runtime_protocol.as_ref())
        .is_some();
    let mut decisions = Vec::new();
    let mut has_indeterminate = false;

    for op in &scan.operations {
        let (status, reason) = classify_operation(op, t1_protocol);
        if status == ToolRecoveryStatus::Indeterminate {
            has_indeterminate = true;
        }
        decisions.push(ToolRecoveryDecision {
            tool_call_id: op.tool_call_id.clone(),
            tool_name: op.tool_name.clone(),
            status,
            reason,
        });
    }

    let has_corruption = scan.has_corruption;
    let has_parked = decisions.iter().any(|d| d.status == ToolRecoveryStatus::Parked);
    let plan = if has_corruption || has_indeterminate || has_parked {
        RecoveryPlan::Blocked
    } else {
        RecoveryPlan::SafeReplay
    };

    RecoveryResolution { decisions, has_corruption, has_indeterminate, plan }
}

fn classify_operation(
    op: &ToolLedgerScanOperation,
    t1_protocol: bool,
) -> (ToolRecoveryStatus, ToolRecoveryReason) {
    if let Some(issue) = op.issues.first() {
        return (ToolRecoveryStatus::Corruption, reason_from_issue(issue.code));
    }
    if op.call_event.is_none() {
        return (ToolRecoveryStatus::Corruption, ToolRecoveryReason::OrphanResponse);
    }
    // Default classification (without a recovery bundle).
    let default = if op.response_event.is_some() {
        (ToolRecoveryStatus::Completed, ToolRecoveryReason::MatchingResponse)
    } else {
        match (op.dispatch_event.is_some(), t1_protocol) {
            (true, _) => {
                (ToolRecoveryStatus::Indeterminate, ToolRecoveryReason::DispatchWithoutResponse)
            }
            (false, true) => (
                ToolRecoveryStatus::DefinitelyNotDispatched,
                ToolRecoveryReason::NewProtocolBeforeDispatch,
            ),
            (false, false) => {
                (ToolRecoveryStatus::Indeterminate, ToolRecoveryReason::LegacyDispatchUnknown)
            }
        }
    };
    // A recovery bundle overrides the default when present.
    if !op.reconcile_events.is_empty() || !op.decision_events.is_empty() {
        return match interpret_recovery_bundle(op) {
            RecoveryInterpretation::Valid {
                disposition: ToolRecoveryDisposition::Completed,
            } => (ToolRecoveryStatus::Completed, ToolRecoveryReason::RecoveryBundleCompleted),
            RecoveryInterpretation::Valid { .. } => {
                (ToolRecoveryStatus::Parked, ToolRecoveryReason::RecoveryBundleParked)
            }
            RecoveryInterpretation::Corruption => {
                (ToolRecoveryStatus::Corruption, ToolRecoveryReason::RecoveryFactCorruption)
            }
            RecoveryInterpretation::Absent => default,
        };
    }
    default
}

/// What a recovery bundle says about an operation.
enum RecoveryInterpretation {
    Absent,
    Valid { disposition: ToolRecoveryDisposition },
    Corruption,
}

/// Interpret the recovery tail of one operation. A canonical bundle is exactly
/// one reconcile_result fact + one terminal recovery_decision; the decision's
/// disposition settles the operation. Anything else is corruption. (Full
/// identity/outcome/evidence validation lands later; shape + disposition is
/// enough to drive the resolver.)
fn interpret_recovery_bundle(op: &ToolLedgerScanOperation) -> RecoveryInterpretation {
    if op.reconcile_events.is_empty() && op.decision_events.is_empty() {
        return RecoveryInterpretation::Absent;
    }
    if op.reconcile_events.len() != 1 || op.decision_events.len() != 1 {
        return RecoveryInterpretation::Corruption;
    }
    let Some(actions) = &op.decision_events[0].actions else {
        return RecoveryInterpretation::Corruption;
    };
    match &actions.tool_recovery {
        Some(ToolRecoveryFactEnvelope::RecoveryDecision { payload, .. }) => {
            RecoveryInterpretation::Valid { disposition: payload.disposition }
        }
        _ => RecoveryInterpretation::Corruption,
    }
}

fn reason_from_issue(code: ToolLedgerIssueCode) -> ToolRecoveryReason {
    match code {
        ToolLedgerIssueCode::DuplicateEventId => ToolRecoveryReason::DuplicateEventId,
        ToolLedgerIssueCode::DuplicateCall => ToolRecoveryReason::DuplicateCall,
        ToolLedgerIssueCode::DuplicateResponse => ToolRecoveryReason::DuplicateResponse,
        ToolLedgerIssueCode::DuplicateDispatch => ToolRecoveryReason::DuplicateDispatch,
        ToolLedgerIssueCode::OrphanResponse => ToolRecoveryReason::OrphanResponse,
        ToolLedgerIssueCode::OrphanDispatch => ToolRecoveryReason::OrphanDispatch,
        ToolLedgerIssueCode::IdentityConflict => ToolRecoveryReason::IdentityConflict,
        ToolLedgerIssueCode::RecoveryFactWithoutOperation => {
            ToolRecoveryReason::RecoveryFactWithoutOperation
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meepo_core::{
        Author, Content, ProtocolMarker, Role, RuntimeEvent, RuntimeEventActions,
        TOOL_BOUNDARY_PROTOCOL_V1, ToolDispatch, ToolRecoveryMode,
        ToolReconcileObservation, ToolReconcileResultFact, ToolRecoveryDecisionFact,
        ToolRecoveryDisposition, ToolRecoveryFactEnvelope,
    };

    fn make_event(id: &str, role: Role, content: Content) -> RuntimeEvent {
        RuntimeEvent {
            session_id: "s".into(),
            invocation_id: "inv".into(),
            run_id: "r".into(),
            turn_id: "t".into(),
            branch: None,
            id: id.into(),
            ts: 0,
            role,
            author: Author::Agent,
            origin: None,
            model_visibility: None,
            status: None,
            content: Some(content),
            actions: None,
            refs: None,
            partial: None,
        }
    }

    fn text_event(id: &str, text: &str) -> RuntimeEvent {
        make_event(id, Role::User, Content::Text {
            text: text.into(),
            provider_options: None,
            steering: None,
        })
    }

    fn call_event(id: &str, call_id: &str, name: &str) -> RuntimeEvent {
        make_event(id, Role::Model, Content::FunctionCall {
            id: call_id.into(),
            name: name.into(),
            args: serde_json::json!({}),
            provider_options: None,
            provider_executed: None,
        })
    }

    fn response_event(id: &str, call_id: &str) -> RuntimeEvent {
        make_event(id, Role::Tool, Content::FunctionResponse {
            id: call_id.into(),
            name: "read_file".into(),
            result: serde_json::json!("ok"),
            is_error: Some(false),
            provider_executed: None,
            provider_output: None,
        })
    }

    fn protocol_event(id: &str) -> RuntimeEvent {
        let mut e = text_event(id, "u");
        e.actions = Some(RuntimeEventActions {
            runtime_protocol: Some(ProtocolMarker {
                tool_boundary: TOOL_BOUNDARY_PROTOCOL_V1.to_string(),
            }),
            ..Default::default()
        });
        e
    }

    fn dispatch_event(id: &str, call_id: &str, name: &str) -> RuntimeEvent {
        let mut e = make_event(
            id,
            Role::System,
            Content::Error { message: "x".into(), code: None, reason: None, details: None },
        );
        e.author = Author::System;
        e.content = None;
        e.actions = Some(RuntimeEventActions {
            tool_dispatch: Some(ToolDispatch {
                protocol: TOOL_BOUNDARY_PROTOCOL_V1.to_string(),
                operation_id: format!("op_{call_id}"),
                provider_tool_call_id: call_id.into(),
                tool_name: name.into(),
                canonical_args_hash: "sha256:x".into(),
                recovery_mode: ToolRecoveryMode::ReplaySafe,
            }),
            ..Default::default()
        });
        e
    }

    fn reconcile_event(id: &str, op_id: &str, obs: ToolReconcileObservation) -> RuntimeEvent {
        let mut e = make_event(id, Role::System, Content::Error {
            message: "x".into(), code: None, reason: None, details: None,
        });
        e.author = Author::System;
        e.content = None;
        e.actions = Some(RuntimeEventActions {
            tool_recovery: Some(ToolRecoveryFactEnvelope::ReconcileResult {
                version: 1,
                payload: ToolReconcileResultFact {
                    protocol: "tool_reconcile_v1".into(),
                    operation_id: op_id.into(),
                    observation: obs,
                    observation_schema: "state_identity_v1".into(),
                    observation_digest: "sha256:x".into(),
                },
            }),
            ..Default::default()
        });
        e
    }

    fn decision_event(
        id: &str, op_id: &str, disp: ToolRecoveryDisposition, reason: &str,
    ) -> RuntimeEvent {
        let mut e = make_event(id, Role::System, Content::Error {
            message: "x".into(), code: None, reason: None, details: None,
        });
        e.author = Author::System;
        e.content = None;
        e.actions = Some(RuntimeEventActions {
            tool_recovery: Some(ToolRecoveryFactEnvelope::RecoveryDecision {
                version: 1,
                payload: ToolRecoveryDecisionFact {
                    protocol: "tool_recovery_v1".into(),
                    operation_id: op_id.into(),
                    disposition: disp,
                    reason_code: reason.into(),
                    evidence_event_ids: vec!["e1".into(), "e2".into(), "e3".into()],
                    outcome_event_id: None,
                },
            }),
            ..Default::default()
        });
        e
    }

    #[test]
    fn no_tools_is_safe_replay() {
        let events = vec![text_event("e1", "hello"), text_event("e2", "hi")];
        let resolution = resolve_recovery(&events);
        assert_eq!(resolution.plan, RecoveryPlan::SafeReplay);
        assert!(resolution.decisions.is_empty());
    }

    #[test]
    fn completed_call_with_response_is_safe() {
        let events = vec![
            call_event("e1", "call_1", "read_file"),
            response_event("e2", "call_1"),
        ];
        let resolution = resolve_recovery(&events);
        assert_eq!(resolution.plan, RecoveryPlan::SafeReplay);
        assert_eq!(resolution.decisions.len(), 1);
        assert_eq!(resolution.decisions[0].status, ToolRecoveryStatus::Completed);
    }

    #[test]
    fn call_without_response_is_blocked() {
        let events = vec![
            text_event("e1", "user"),
            call_event("e2", "call_1", "read_file"),
            // NO response — crash happened before tool finished
        ];
        let resolution = resolve_recovery(&events);
        assert_eq!(resolution.plan, RecoveryPlan::Blocked);
        assert!(resolution.has_indeterminate);
        assert!(!resolution.has_corruption);
        assert_eq!(resolution.decisions[0].status, ToolRecoveryStatus::Indeterminate);
    }

    #[test]
    fn orphan_response_is_corruption() {
        let events = vec![
            response_event("e1", "call_ghost"), // response with no matching call
        ];
        let resolution = resolve_recovery(&events);
        assert_eq!(resolution.plan, RecoveryPlan::Blocked);
        assert!(resolution.has_corruption);
    }

    #[test]
    fn duplicate_call_is_corruption() {
        let events = vec![
            call_event("e1", "call_1", "read_file"),
            call_event("e2", "call_1", "read_file"), // same id
        ];
        let resolution = resolve_recovery(&events);
        assert!(resolution.has_corruption);
    }

    #[test]
    fn mixed_completed_and_indeterminate_is_blocked() {
        let events = vec![
            call_event("e1", "call_1", "read_file"),
            response_event("e2", "call_1"),
            call_event("e3", "call_2", "bash"),
            // call_2 has no response
        ];
        let resolution = resolve_recovery(&events);
        assert_eq!(resolution.plan, RecoveryPlan::Blocked);
        assert!(resolution.has_indeterminate);
        assert!(!resolution.has_corruption);
        // call_1 is completed, call_2 is indeterminate
        assert_eq!(resolution.decisions.len(), 2);
    }

    #[test]
    fn real_world_dirty_store_scenario() {
        // Simulates the actual bug: 78 calls, 45 responses, 33 orphaned.
        let mut events = Vec::new();
        // 45 completed pairs
        for i in 0..45 {
            events.push(call_event(&format!("c{i}"), &format!("call_{i}"), "tool"));
            events.push(response_event(&format!("r{i}"), &format!("call_{i}")));
        }
        // 33 orphaned calls (no response)
        for i in 45..78 {
            events.push(call_event(&format!("c{i}"), &format!("call_{i}"), "tool"));
        }
        let resolution = resolve_recovery(&events);
        assert_eq!(resolution.plan, RecoveryPlan::Blocked);
        assert!(resolution.has_indeterminate);
        let indeterminate_count = resolution.decisions.iter()
            .filter(|d| d.status == ToolRecoveryStatus::Indeterminate)
            .count();
        assert_eq!(indeterminate_count, 33);
    }

    #[test]
    fn call_without_dispatch_under_t1_is_definitely_not_dispatched() {
        let events = vec![protocol_event("e0"), call_event("e1", "call_1", "read_file")];
        let r = resolve_recovery(&events);
        assert_eq!(r.decisions.len(), 1);
        assert_eq!(r.decisions[0].status, ToolRecoveryStatus::DefinitelyNotDispatched);
        assert!(!r.has_indeterminate);
        assert_eq!(r.plan, RecoveryPlan::SafeReplay);
    }

    #[test]
    fn call_without_dispatch_legacy_is_indeterminate() {
        let events = vec![text_event("e0", "u"), call_event("e1", "call_1", "read_file")];
        let r = resolve_recovery(&events);
        assert_eq!(r.decisions[0].status, ToolRecoveryStatus::Indeterminate);
        assert_eq!(r.plan, RecoveryPlan::Blocked);
    }

    #[test]
    fn dispatched_without_response_is_indeterminate_even_under_t1() {
        let events = vec![
            protocol_event("e0"),
            call_event("e1", "call_1", "bash"),
            dispatch_event("e2", "call_1", "bash"),
        ];
        let r = resolve_recovery(&events);
        assert_eq!(r.decisions[0].status, ToolRecoveryStatus::Indeterminate);
        assert_eq!(r.decisions[0].reason, ToolRecoveryReason::DispatchWithoutResponse);
        assert_eq!(r.plan, RecoveryPlan::Blocked);
    }

    #[test]
    fn recovery_bundle_parks_an_indeterminate_operation() {
        let events = vec![
            protocol_event("e0"),
            call_event("e1", "c1", "bash"),
            dispatch_event("e2", "c1", "bash"),
            reconcile_event("e3", "op_c1", ToolReconcileObservation::Diverged),
            decision_event("e4", "op_c1", ToolRecoveryDisposition::Parked, "reconcile_diverged"),
        ];
        let r = resolve_recovery(&events);
        assert_eq!(r.decisions[0].status, ToolRecoveryStatus::Parked);
        assert_eq!(r.decisions[0].reason, ToolRecoveryReason::RecoveryBundleParked);
        assert_eq!(r.plan, RecoveryPlan::Blocked);
    }

    #[test]
    fn recovery_bundle_completes_via_decision() {
        let events = vec![
            protocol_event("e0"),
            call_event("e1", "c1", "bash"),
            dispatch_event("e2", "c1", "bash"),
            reconcile_event("e3", "op_c1", ToolReconcileObservation::MatchesExpectedState),
            decision_event(
                "e4", "op_c1", ToolRecoveryDisposition::Completed, "reconcile_matches_expected_state",
            ),
        ];
        let r = resolve_recovery(&events);
        assert_eq!(r.decisions[0].status, ToolRecoveryStatus::Completed);
        assert_eq!(r.decisions[0].reason, ToolRecoveryReason::RecoveryBundleCompleted);
    }
}
