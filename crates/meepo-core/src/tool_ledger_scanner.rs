//! Tool ledger scanner — the single authority for interpreting tool-bearing
//! RuntimeEvents.
//!
//! Scans an immutable event prefix once, in physical ledger order, grouping
//! `function_call` / `tool_dispatch` / `function_response` facts into per-call
//! operations and flagging structural issues (duplicate ids, duplicate
//! calls/dispatches/responses, orphan responses/dispatches, identity
//! conflicts). The recovery resolver and the projection rebuild both consume
//! [`scan_tool_ledger`] — neither rebuilds these maps.

use crate::{Content, Role, RuntimeEvent};

/// Which tool-ledger lane an event occupies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolLedgerLane {
    /// Non-tool conversation fact (text, thinking, terminal, ...).
    Ordinary,
    /// Model-issued tool call.
    FunctionCall,
    /// Durable T1 dispatch fact (actions.toolDispatch) — the runtime is about
    /// to execute the tool. Recovery uses it to decide definitely_not_dispatched.
    ToolDispatch,
    /// Tool result returned to the model.
    FunctionResponse,
    // Reserved: ReconcileResult, RecoveryDecision.
}

/// Structural issue found while grouping the ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolLedgerIssueCode {
    /// An event id appeared more than once in the prefix.
    DuplicateEventId,
    /// Two function_call events share a tool-call id within one invocation.
    DuplicateCall,
    /// Two tool_dispatch facts for the same operation.
    DuplicateDispatch,
    /// Two function_responses share a tool-call id within one invocation.
    DuplicateResponse,
    /// A function_response with no matching function_call.
    OrphanResponse,
    /// A tool_dispatch with no matching function_call.
    OrphanDispatch,
    /// A tool-call id used with differing tool names or execution identity.
    IdentityConflict,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolLedgerIssue {
    pub code: ToolLedgerIssueCode,
    pub event_id: String,
    pub tool_call_id: Option<String>,
}

/// One tool operation reconstructed from the ledger: a call, its dispatch fact
/// and its response (each optional), plus the structural issues attached to it.
#[derive(Debug, Clone)]
pub struct ToolLedgerScanOperation {
    pub tool_call_id: String,
    pub tool_name: Option<String>,
    /// Runtime-owned durable operation id (from the T1 dispatch fact, if any).
    pub operation_id: Option<String>,
    pub call_event: Option<RuntimeEvent>,
    pub dispatch_event: Option<RuntimeEvent>,
    pub response_event: Option<RuntimeEvent>,
    pub issues: Vec<ToolLedgerIssue>,
}

/// Result of scanning an event prefix.
#[derive(Debug, Clone, Default)]
pub struct ToolLedgerScanResult {
    pub operations: Vec<ToolLedgerScanOperation>,
    pub issues: Vec<ToolLedgerIssue>,
    pub has_corruption: bool,
}

/// Scan an immutable event prefix once, grouping `function_call` /
/// `tool_dispatch` / `function_response` facts into operations keyed by
/// `(invocation_id, tool_call_id)`. The sole authority for duplicate / orphan /
/// identity interpretation — recovery and projection both call this.
///
/// A call without a response is **not** an issue here: whether it is safe to
/// replay is a recovery decision (indeterminate), reported by the resolver.
/// Structural issues (duplicates, orphans, identity conflicts) are corruption —
/// the ledger itself is inconsistent.
pub fn scan_tool_ledger(events: &[RuntimeEvent]) -> ToolLedgerScanResult {
    let mut operations: Vec<ToolLedgerScanOperation> = Vec::new();
    let mut issues: Vec<ToolLedgerIssue> = Vec::new();
    let mut seen_event_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    // Keyed by (invocation_id, tool_call_id) → index into `operations`.
    let mut by_call: std::collections::HashMap<String, usize> = std::collections::HashMap::new();

    for event in events {
        if !seen_event_ids.insert(event.id.clone()) {
            push_issue(
                &mut operations,
                &mut issues,
                None,
                ToolLedgerIssue {
                    code: ToolLedgerIssueCode::DuplicateEventId,
                    event_id: event.id.clone(),
                    tool_call_id: None,
                },
            );
            continue;
        }

        match classify_lane(event) {
            ToolLedgerLane::FunctionCall => {
                let Some(Content::FunctionCall { id: call_id, name, .. }) = &event.content else {
                    continue;
                };
                let key = call_key(&event.invocation_id, call_id);
                if let Some(&idx) = by_call.get(&key) {
                    if operations[idx].call_event.is_some() {
                        push_issue(
                            &mut operations,
                            &mut issues,
                            Some(idx),
                            ToolLedgerIssue {
                                code: ToolLedgerIssueCode::DuplicateCall,
                                event_id: event.id.clone(),
                                tool_call_id: Some(call_id.clone()),
                            },
                        );
                    } else {
                        // Operation exists from an earlier orphan response — a
                        // call binding now means the pair disagrees on identity.
                        operations[idx].call_event = Some(event.clone());
                        push_issue(
                            &mut operations,
                            &mut issues,
                            Some(idx),
                            ToolLedgerIssue {
                                code: ToolLedgerIssueCode::IdentityConflict,
                                event_id: event.id.clone(),
                                tool_call_id: Some(call_id.clone()),
                            },
                        );
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
                        push_issue(
                            &mut operations,
                            &mut issues,
                            Some(idx),
                            ToolLedgerIssue {
                                code: ToolLedgerIssueCode::DuplicateDispatch,
                                event_id: event.id.clone(),
                                tool_call_id: Some(dispatch.provider_tool_call_id.clone()),
                            },
                        );
                    } else {
                        operations[idx].dispatch_event = Some(event.clone());
                        operations[idx].operation_id = Some(dispatch.operation_id.clone());
                    }
                } else {
                    // Orphan dispatch — no matching call. Carry it on a new
                    // operation and flag the structural issue.
                    let idx = operations.len();
                    operations.push(ToolLedgerScanOperation {
                        tool_call_id: dispatch.provider_tool_call_id.clone(),
                        tool_name: Some(dispatch.tool_name.clone()),
                        operation_id: Some(dispatch.operation_id.clone()),
                        call_event: None,
                        dispatch_event: Some(event.clone()),
                        response_event: None,
                        issues: Vec::new(),
                    });
                    by_call.insert(key, idx);
                    push_issue(
                        &mut operations,
                        &mut issues,
                        Some(idx),
                        ToolLedgerIssue {
                            code: ToolLedgerIssueCode::OrphanDispatch,
                            event_id: event.id.clone(),
                            tool_call_id: Some(dispatch.provider_tool_call_id.clone()),
                        },
                    );
                }
            }
            ToolLedgerLane::FunctionResponse => {
                let Some(Content::FunctionResponse { id: call_id, name, .. }) = &event.content else {
                    continue;
                };
                let key = call_key(&event.invocation_id, call_id);
                if let Some(&idx) = by_call.get(&key) {
                    if operations[idx].response_event.is_some() {
                        push_issue(
                            &mut operations,
                            &mut issues,
                            Some(idx),
                            ToolLedgerIssue {
                                code: ToolLedgerIssueCode::DuplicateResponse,
                                event_id: event.id.clone(),
                                tool_call_id: Some(call_id.clone()),
                            },
                        );
                        continue;
                    }
                    if operations[idx].tool_name.as_deref() != Some(name.as_str()) {
                        push_issue(
                            &mut operations,
                            &mut issues,
                            Some(idx),
                            ToolLedgerIssue {
                                code: ToolLedgerIssueCode::IdentityConflict,
                                event_id: event.id.clone(),
                                tool_call_id: Some(call_id.clone()),
                            },
                        );
                    }
                    operations[idx].response_event = Some(event.clone());
                } else {
                    // Orphan response — no matching call.
                    let idx = operations.len();
                    operations.push(ToolLedgerScanOperation {
                        tool_call_id: call_id.clone(),
                        tool_name: Some(name.clone()),
                        operation_id: None,
                        call_event: None,
                        dispatch_event: None,
                        response_event: Some(event.clone()),
                        issues: Vec::new(),
                    });
                    by_call.insert(key, idx);
                    push_issue(
                        &mut operations,
                        &mut issues,
                        Some(idx),
                        ToolLedgerIssue {
                            code: ToolLedgerIssueCode::OrphanResponse,
                            event_id: event.id.clone(),
                            tool_call_id: Some(call_id.clone()),
                        },
                    );
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
        (Role::System, None) => {
            if event.actions.as_ref().and_then(|a| a.tool_dispatch.as_ref()).is_some() {
                ToolLedgerLane::ToolDispatch
            } else {
                ToolLedgerLane::Ordinary
            }
        }
        _ => ToolLedgerLane::Ordinary,
    }
}

fn call_key(invocation_id: &str, tool_call_id: &str) -> String {
    // Unit separator (\x1f) keeps invocation id and tool call id distinct even
    // if either contains the other's content.
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

    fn ev(id: &str, role: Role, content: Option<Content>) -> RuntimeEvent {
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
            content,
            actions: None,
            refs: None,
            partial: None,
        }
    }

    fn call(id: &str, call_id: &str, name: &str) -> RuntimeEvent {
        ev(
            id,
            Role::Model,
            Some(Content::FunctionCall {
                id: call_id.into(),
                name: name.into(),
                args: serde_json::json!({}),
                provider_options: None,
                provider_executed: None,
            }),
        )
    }

    fn resp(id: &str, call_id: &str, name: &str) -> RuntimeEvent {
        ev(
            id,
            Role::Tool,
            Some(Content::FunctionResponse {
                id: call_id.into(),
                name: name.into(),
                result: serde_json::json!("ok"),
                is_error: Some(false),
                provider_executed: None,
                provider_output: None,
            }),
        )
    }

    fn dispatch(id: &str, call_id: &str, name: &str, op_id: &str) -> RuntimeEvent {
        let mut e = ev(id, Role::System, None);
        e.author = Author::System;
        e.actions = Some(RuntimeEventActions {
            tool_dispatch: Some(ToolDispatch {
                protocol: TOOL_BOUNDARY_PROTOCOL_V1.to_string(),
                operation_id: op_id.into(),
                provider_tool_call_id: call_id.into(),
                tool_name: name.into(),
                canonical_args_hash: "sha256:x".into(),
                recovery_mode: ToolRecoveryMode::ReplaySafe,
            }),
            ..Default::default()
        });
        e
    }

    #[test]
    fn paired_call_response_one_operation() {
        let s = scan_tool_ledger(&[call("c1", "tc1", "read"), resp("r1", "tc1", "read")]);
        assert_eq!(s.operations.len(), 1);
        assert!(s.issues.is_empty());
        assert!(!s.has_corruption);
        assert!(s.operations[0].call_event.is_some());
        assert!(s.operations[0].response_event.is_some());
    }

    #[test]
    fn duplicate_event_id_flagged() {
        let s = scan_tool_ledger(&[call("c1", "tc1", "read"), call("c1", "tc2", "read")]);
        assert!(s.issues.iter().any(|i| i.code == ToolLedgerIssueCode::DuplicateEventId));
        assert!(s.has_corruption);
    }

    #[test]
    fn duplicate_call_flagged() {
        let s = scan_tool_ledger(&[call("c1", "tc1", "read"), call("c2", "tc1", "read")]);
        assert!(s.issues.iter().any(|i| i.code == ToolLedgerIssueCode::DuplicateCall));
        assert!(s.has_corruption);
    }

    #[test]
    fn duplicate_response_flagged() {
        let s = scan_tool_ledger(&[resp("r1", "tc1", "read"), resp("r2", "tc1", "read")]);
        assert!(s.issues.iter().any(|i| i.code == ToolLedgerIssueCode::DuplicateResponse));
    }

    #[test]
    fn orphan_response_flagged() {
        let s = scan_tool_ledger(&[resp("r1", "ghost", "read")]);
        assert!(s.issues.iter().any(|i| i.code == ToolLedgerIssueCode::OrphanResponse));
        assert!(s.has_corruption);
    }

    #[test]
    fn identity_conflict_on_name_mismatch() {
        let s = scan_tool_ledger(&[call("c1", "tc1", "read"), resp("r1", "tc1", "write")]);
        assert!(s.issues.iter().any(|i| i.code == ToolLedgerIssueCode::IdentityConflict));
    }

    #[test]
    fn call_without_response_is_not_structural_issue() {
        // Indeterminate, not corruption — the resolver decides.
        let s = scan_tool_ledger(&[call("c1", "tc1", "read")]);
        assert!(!s.has_corruption);
        assert_eq!(s.operations.len(), 1);
        assert!(s.operations[0].response_event.is_none());
    }

    #[test]
    fn ordinary_events_ignored() {
        let text = ev("t1", Role::User, Some(Content::Text {
            text: "hi".into(),
            provider_options: None,
            steering: None,
        }));
        let s = scan_tool_ledger(&[text]);
        assert!(s.operations.is_empty());
        assert!(!s.has_corruption);
    }

    #[test]
    fn dispatch_binds_to_call() {
        let s = scan_tool_ledger(&[
            call("c1", "tc1", "read"),
            dispatch("d1", "tc1", "read", "op1"),
            resp("r1", "tc1", "read"),
        ]);
        assert_eq!(s.operations.len(), 1);
        assert!(s.issues.is_empty(), "no issues for a bound dispatch");
        assert!(s.operations[0].dispatch_event.is_some());
        assert_eq!(s.operations[0].operation_id.as_deref(), Some("op1"));
    }

    #[test]
    fn orphan_dispatch_flagged() {
        let s = scan_tool_ledger(&[dispatch("d1", "ghost", "read", "op1")]);
        assert!(s.issues.iter().any(|i| i.code == ToolLedgerIssueCode::OrphanDispatch));
        assert!(s.has_corruption);
    }

    #[test]
    fn duplicate_dispatch_flagged() {
        let s = scan_tool_ledger(&[
            call("c1", "tc1", "read"),
            dispatch("d1", "tc1", "read", "op1"),
            dispatch("d2", "tc1", "read", "op1"),
        ]);
        assert!(s.issues.iter().any(|i| i.code == ToolLedgerIssueCode::DuplicateDispatch));
    }
}
