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
    // Reserved for the typed-actions / recovery-bundle phase:
    // Parked, DefinitelyNotDispatched,
}

/// Reason for the classification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolRecoveryReason {
    MatchingResponse,
    DispatchWithoutResponse,
    OrphanResponse,
    DuplicateCall,
    DuplicateResponse,
    IdentityConflict,
    DuplicateEventId,
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
    let mut decisions = Vec::new();
    let mut has_indeterminate = false;

    for op in &scan.operations {
        let (status, reason) = classify_operation(op);
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
    let plan = if has_corruption || has_indeterminate {
        RecoveryPlan::Blocked
    } else {
        RecoveryPlan::SafeReplay
    };

    RecoveryResolution { decisions, has_corruption, has_indeterminate, plan }
}

fn classify_operation(op: &ToolLedgerScanOperation) -> (ToolRecoveryStatus, ToolRecoveryReason) {
    if let Some(issue) = op.issues.first() {
        return (ToolRecoveryStatus::Corruption, reason_from_issue(issue.code));
    }
    if op.call_event.is_none() {
        // An operation without a call should always carry an issue (orphan
        // response); fail closed if we ever reach here without one.
        return (ToolRecoveryStatus::Corruption, ToolRecoveryReason::OrphanResponse);
    }
    if op.response_event.is_some() {
        (ToolRecoveryStatus::Completed, ToolRecoveryReason::MatchingResponse)
    } else {
        (ToolRecoveryStatus::Indeterminate, ToolRecoveryReason::DispatchWithoutResponse)
    }
}

fn reason_from_issue(code: ToolLedgerIssueCode) -> ToolRecoveryReason {
    match code {
        ToolLedgerIssueCode::DuplicateEventId => ToolRecoveryReason::DuplicateEventId,
        ToolLedgerIssueCode::DuplicateCall => ToolRecoveryReason::DuplicateCall,
        ToolLedgerIssueCode::DuplicateResponse => ToolRecoveryReason::DuplicateResponse,
        ToolLedgerIssueCode::OrphanResponse => ToolRecoveryReason::OrphanResponse,
        ToolLedgerIssueCode::IdentityConflict => ToolRecoveryReason::IdentityConflict,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meepo_core::{Author, Content, Role, RuntimeEvent};

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
}
