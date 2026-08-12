//! Tool ledger scanner — the single authority for interpreting tool-bearing
//! RuntimeEvents. See header in prior revision; this adds the reconcile_result
//! and recovery_decision lanes (the recovery-bundle tail).

use crate::recovery_fact::{ToolRecoveryFactEnvelope, TOOL_RECONCILE_RESULT_FACT_KIND};
use crate::{Content, Role, RuntimeEvent};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolLedgerLane {
    Ordinary,
    FunctionCall,
    ToolDispatch,
    FunctionResponse,
    /// actions.toolRecovery reconcile_result fact.
    ReconcileResult,
    /// actions.toolRecovery recovery_decision fact.
    RecoveryDecision,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolLedgerIssueCode {
    DuplicateEventId,
    DuplicateCall,
    DuplicateDispatch,
    DuplicateResponse,
    OrphanResponse,
    OrphanDispatch,
    IdentityConflict,
    /// A recovery fact (reconcile/decision) with no matching operation.
    RecoveryFactWithoutOperation,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolLedgerIssue {
    pub code: ToolLedgerIssueCode,
    pub event_id: String,
    pub tool_call_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ToolLedgerScanOperation {
    pub tool_call_id: String,
    pub tool_name: Option<String>,
    pub operation_id: Option<String>,
    pub call_event: Option<RuntimeEvent>,
    pub dispatch_event: Option<RuntimeEvent>,
    pub response_event: Option<RuntimeEvent>,
    pub reconcile_events: Vec<RuntimeEvent>,
    pub decision_events: Vec<RuntimeEvent>,
    pub issues: Vec<ToolLedgerIssue>,
}

#[derive(Debug, Clone, Default)]
pub struct ToolLedgerScanResult {
    pub operations: Vec<ToolLedgerScanOperation>,
    pub issues: Vec<ToolLedgerIssue>,
    pub has_corruption: bool,
}

pub fn scan_tool_ledger(events: &[RuntimeEvent]) -> ToolLedgerScanResult {
    let mut operations: Vec<ToolLedgerScanOperation> = Vec::new();
    let mut issues: Vec<ToolLedgerIssue> = Vec::new();
    let mut seen_event_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut by_call: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut by_operation: std::collections::HashMap<String, usize> = std::collections::HashMap::new();

    for event in events {
        if !seen_event_ids.insert(event.id.clone()) {
            push_issue(&mut operations, &mut issues, None, ToolLedgerIssue {
                code: ToolLedgerIssueCode::DuplicateEventId,
                event_id: event.id.clone(),
                tool_call_id: None,
            });
            continue;
        }

        match classify_lane(event) {
            ToolLedgerLane::FunctionCall => {
                let Some(Content::FunctionCall { id: call_id, name, .. }) = &event.content else { continue };
                let key = call_key(&event.invocation_id, call_id);
                if let Some(&idx) = by_call.get(&key) {
                    if operations[idx].call_event.is_some() {
                        push_issue(&mut operations, &mut issues, Some(idx), ToolLedgerIssue {
                            code: ToolLedgerIssueCode::DuplicateCall,
                            event_id: event.id.clone(),
                            tool_call_id: Some(call_id.clone()),
                        });
                    } else {
                        operations[idx].call_event = Some(event.clone());
                        push_issue(&mut operations, &mut issues, Some(idx), ToolLedgerIssue {
                            code: ToolLedgerIssueCode::IdentityConflict,
                            event_id: event.id.clone(),
                            tool_call_id: Some(call_id.clone()),
                        });
                    }
                    continue;
                }
                let idx = operations.len();
                operations.push(ToolLedgerScanOperation {
                    tool_call_id: call_id.clone(),
                    tool_name: Some(name.clone()),
                    operation_id: None,
                    call_event: Some(event.clone()),
                    dispatch_event: None,
                    response_event: None,
                    reconcile_events: Vec::new(),
                    decision_events: Vec::new(),
                    issues: Vec::new(),
                });
                by_call.insert(key, idx);
            }
            ToolLedgerLane::ToolDispatch => {
                let Some(actions) = &event.actions else { continue };
                let Some(dispatch) = &actions.tool_dispatch else { continue };
                let key = call_key(&event.invocation_id, &dispatch.provider_tool_call_id);
                if let Some(&idx) = by_call.get(&key) {
                    if operations[idx].dispatch_event.is_some() {
                        push_issue(&mut operations, &mut issues, Some(idx), ToolLedgerIssue {
                            code: ToolLedgerIssueCode::DuplicateDispatch,
                            event_id: event.id.clone(),
                            tool_call_id: Some(dispatch.provider_tool_call_id.clone()),
                        });
                    } else {
                        operations[idx].dispatch_event = Some(event.clone());
                        operations[idx].operation_id = Some(dispatch.operation_id.clone());
                        by_operation.insert(dispatch.operation_id.clone(), idx);
                    }
                } else {
                    let idx = operations.len();
                    operations.push(ToolLedgerScanOperation {
                        tool_call_id: dispatch.provider_tool_call_id.clone(),
                        tool_name: Some(dispatch.tool_name.clone()),
                        operation_id: Some(dispatch.operation_id.clone()),
                        call_event: None,
                        dispatch_event: Some(event.clone()),
                        response_event: None,
                        reconcile_events: Vec::new(),
                        decision_events: Vec::new(),
                        issues: Vec::new(),
                    });
                    by_call.insert(key, idx);
                    by_operation.insert(dispatch.operation_id.clone(), idx);
                    push_issue(&mut operations, &mut issues, Some(idx), ToolLedgerIssue {
                        code: ToolLedgerIssueCode::OrphanDispatch,
                        event_id: event.id.clone(),
                        tool_call_id: Some(dispatch.provider_tool_call_id.clone()),
                    });
                }
            }
            ToolLedgerLane::ReconcileResult | ToolLedgerLane::RecoveryDecision => {
                let Some(actions) = &event.actions else { continue };
                let Some(fact) = &actions.tool_recovery else { continue };
                let op_id = fact_operation_id(fact);
                if let Some(&idx) = op_id.as_deref().and_then(|id| by_operation.get(id)) {
                    match classify_lane(event) {
                        ToolLedgerLane::ReconcileResult => operations[idx].reconcile_events.push(event.clone()),
                        ToolLedgerLane::RecoveryDecision => operations[idx].decision_events.push(event.clone()),
                        _ => {}
                    }
                } else {
                    push_issue(&mut operations, &mut issues, None, ToolLedgerIssue {
                        code: ToolLedgerIssueCode::RecoveryFactWithoutOperation,
                        event_id: event.id.clone(),
                        tool_call_id: op_id.clone(),
                    });
                }
            }
            ToolLedgerLane::FunctionResponse => {
                let Some(Content::FunctionResponse { id: call_id, name, .. }) = &event.content else { continue };
                let key = call_key(&event.invocation_id, call_id);
                if let Some(&idx) = by_call.get(&key) {
                    if operations[idx].response_event.is_some() {
                        push_issue(&mut operations, &mut issues, Some(idx), ToolLedgerIssue {
                            code: ToolLedgerIssueCode::DuplicateResponse,
                            event_id: event.id.clone(),
                            tool_call_id: Some(call_id.clone()),
                        });
                        continue;
                    }
                    if operations[idx].tool_name.as_deref() != Some(name.as_str()) {
                        push_issue(&mut operations, &mut issues, Some(idx), ToolLedgerIssue {
                            code: ToolLedgerIssueCode::IdentityConflict,
                            event_id: event.id.clone(),
                            tool_call_id: Some(call_id.clone()),
                        });
                    }
                    operations[idx].response_event = Some(event.clone());
                } else {
                    let idx = operations.len();
                    operations.push(ToolLedgerScanOperation {
                        tool_call_id: call_id.clone(),
                        tool_name: Some(name.clone()),
                        operation_id: None,
                        call_event: None,
                        dispatch_event: None,
                        response_event: Some(event.clone()),
                        reconcile_events: Vec::new(),
                        decision_events: Vec::new(),
                        issues: Vec::new(),
                    });
                    by_call.insert(key, idx);
                    push_issue(&mut operations, &mut issues, Some(idx), ToolLedgerIssue {
                        code: ToolLedgerIssueCode::OrphanResponse,
                        event_id: event.id.clone(),
                        tool_call_id: Some(call_id.clone()),
                    });
                }
            }
            ToolLedgerLane::Ordinary => {}
        }
    }

    let has_corruption = !issues.is_empty();
    ToolLedgerScanResult { operations, issues, has_corruption }
}

fn classify_lane(event: &RuntimeEvent) -> ToolLedgerLane {
    match (&event.role, &event.content) {
        (Role::Model, Some(Content::FunctionCall { .. })) => ToolLedgerLane::FunctionCall,
        (Role::Tool, Some(Content::FunctionResponse { .. })) => ToolLedgerLane::FunctionResponse,
        (Role::System, None) => match event.actions.as_ref().and_then(|a| a.tool_recovery.as_ref()) {
            Some(f) if fact_kind(f) == TOOL_RECONCILE_RESULT_FACT_KIND => ToolLedgerLane::ReconcileResult,
            Some(_) => ToolLedgerLane::RecoveryDecision,
            None => {
                if event.actions.as_ref().and_then(|a| a.tool_dispatch.as_ref()).is_some() {
                    ToolLedgerLane::ToolDispatch
                } else {
                    ToolLedgerLane::Ordinary
                }
            }
        },
        _ => ToolLedgerLane::Ordinary,
    }
}

fn fact_kind(fact: &ToolRecoveryFactEnvelope) -> &'static str {
    fact.kind_str()
}

fn fact_operation_id(fact: &ToolRecoveryFactEnvelope) -> Option<String> {
    match fact {
        ToolRecoveryFactEnvelope::ReconcileResult { payload, .. } => Some(payload.operation_id.clone()),
        ToolRecoveryFactEnvelope::RecoveryDecision { payload, .. } => Some(payload.operation_id.clone()),
    }
}

fn call_key(invocation_id: &str, tool_call_id: &str) -> String {
    format!("{invocation_id}\x1f{tool_call_id}")
}

fn push_issue(
    operations: &mut [ToolLedgerScanOperation],
    issues: &mut Vec<ToolLedgerIssue>,
    op_idx: Option<usize>,
    issue: ToolLedgerIssue,
) {
    issues.push(issue.clone());
    if let Some(i) = op_idx {
        operations[i].issues.push(issue);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Author, Content, Role, RuntimeEvent, RuntimeEventActions, TOOL_BOUNDARY_PROTOCOL_V1,
        ToolDispatch, ToolRecoveryMode,
    };
    use crate::recovery_fact::{
        ToolReconcileObservation, ToolReconcileResultFact, ToolRecoveryDecisionFact,
        ToolRecoveryDisposition, ToolRecoveryFactEnvelope,
    };

    fn ev(id: &str, role: Role, content: Option<Content>) -> RuntimeEvent {
        RuntimeEvent {
            session_id: "s".into(), invocation_id: "inv".into(), run_id: "r".into(),
            turn_id: "t".into(), branch: None, id: id.into(), ts: 0, role,
            author: Author::Agent, origin: None, model_visibility: None, status: None,
            content, actions: None, refs: None, partial: None,
        }
    }
    fn call(id: &str, call_id: &str, name: &str) -> RuntimeEvent {
        ev(id, Role::Model, Some(Content::FunctionCall {
            id: call_id.into(), name: name.into(), args: serde_json::json!({}),
            provider_options: None, provider_executed: None,
        }))
    }
    fn resp(id: &str, call_id: &str, name: &str) -> RuntimeEvent {
        ev(id, Role::Tool, Some(Content::FunctionResponse {
            id: call_id.into(), name: name.into(), result: serde_json::json!("ok"),
            is_error: Some(false), provider_executed: None, provider_output: None,
        }))
    }
    fn dispatch(id: &str, call_id: &str, name: &str, op_id: &str) -> RuntimeEvent {
        let mut e = ev(id, Role::System, None);
        e.author = Author::System;
        e.actions = Some(RuntimeEventActions {
            tool_dispatch: Some(ToolDispatch {
                protocol: TOOL_BOUNDARY_PROTOCOL_V1.to_string(), operation_id: op_id.into(),
                provider_tool_call_id: call_id.into(), tool_name: name.into(),
                canonical_args_hash: "sha256:x".into(), recovery_mode: ToolRecoveryMode::ReplaySafe,
            }),
            ..Default::default()
        });
        e
    }
    fn reconcile(id: &str, op_id: &str, obs: ToolReconcileObservation) -> RuntimeEvent {
        let mut e = ev(id, Role::System, None);
        e.author = Author::System;
        e.actions = Some(RuntimeEventActions {
            tool_recovery: Some(ToolRecoveryFactEnvelope::ReconcileResult {
                version: 1,
                payload: ToolReconcileResultFact {
                    protocol: "tool_reconcile_v1".into(), operation_id: op_id.into(),
                    observation: obs, observation_schema: "state_identity_v1".into(),
                    observation_digest: "sha256:x".into(),
                },
            }),
            ..Default::default()
        });
        e
    }
    fn decision(id: &str, op_id: &str, disp: ToolRecoveryDisposition, reason: &str) -> RuntimeEvent {
        let mut e = ev(id, Role::System, None);
        e.author = Author::System;
        e.actions = Some(RuntimeEventActions {
            tool_recovery: Some(ToolRecoveryFactEnvelope::RecoveryDecision {
                version: 1,
                payload: ToolRecoveryDecisionFact {
                    protocol: "tool_recovery_v1".into(), operation_id: op_id.into(),
                    disposition: disp, reason_code: reason.into(),
                    evidence_event_ids: vec!["c1".into(), "d1".into(), "rec".into()],
                    outcome_event_id: None,
                },
            }),
            ..Default::default()
        });
        e
    }

    #[test]
    fn dispatch_binds_to_call() {
        let s = scan_tool_ledger(&[call("c1", "tc1", "read"), dispatch("d1", "tc1", "read", "op1"), resp("r1", "tc1", "read")]);
        assert_eq!(s.operations.len(), 1);
        assert!(s.issues.is_empty());
        assert!(s.operations[0].dispatch_event.is_some());
    }
    #[test]
    fn orphan_dispatch_flagged() {
        let s = scan_tool_ledger(&[dispatch("d1", "ghost", "read", "op1")]);
        assert!(s.issues.iter().any(|i| i.code == ToolLedgerIssueCode::OrphanDispatch));
    }
    #[test]
    fn duplicate_dispatch_flagged() {
        let s = scan_tool_ledger(&[call("c1", "tc1", "read"), dispatch("d1", "tc1", "read", "op1"), dispatch("d2", "tc1", "read", "op1")]);
        assert!(s.issues.iter().any(|i| i.code == ToolLedgerIssueCode::DuplicateDispatch));
    }
    #[test]
    fn recovery_facts_bind_to_operation() {
        let s = scan_tool_ledger(&[
            call("c1", "tc1", "bash"),
            dispatch("d1", "tc1", "bash", "op1"),
            reconcile("rec", "op1", ToolReconcileObservation::Diverged),
            decision("dec", "op1", ToolRecoveryDisposition::Parked, "reconcile_diverged"),
        ]);
        assert_eq!(s.operations.len(), 1);
        assert!(s.issues.is_empty(), "bound recovery facts are not issues");
        assert_eq!(s.operations[0].reconcile_events.len(), 1);
        assert_eq!(s.operations[0].decision_events.len(), 1);
    }
    #[test]
    fn recovery_fact_without_operation_flagged() {
        let s = scan_tool_ledger(&[decision("dec", "orphan_op", ToolRecoveryDisposition::Parked, "reconcile_diverged")]);
        assert!(s.issues.iter().any(|i| i.code == ToolLedgerIssueCode::RecoveryFactWithoutOperation));
    }
    #[test]
    fn orphan_response_flagged() {
        let s = scan_tool_ledger(&[resp("r1", "ghost", "read")]);
        assert!(s.issues.iter().any(|i| i.code == ToolLedgerIssueCode::OrphanResponse));
    }
}
