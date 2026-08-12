//! Recovery resolver — classifies tool operations from a RuntimeEvent prefix
//! after a crash or interruption, and decides whether the prefix is safe to
//! replay or must be blocked.
//!
//! Phase 0 crash contract (mirrors maka's runtime-resume-phase0):
//! - A function_call WITH a matching function_response → completed → safe_replay
//! - A function_call WITHOUT a matching response → indeterminate → blocked
//! - An orphan function_response (no matching call) → corruption → blocked
//! - Duplicate calls or responses → corruption → blocked
//! - No tool operations → safe_replay

use std::collections::HashMap;

use meepo_core::{Content, RuntimeEvent};

/// Classification of a single tool operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolRecoveryStatus {
    /// Call has a matching response — safe to replay.
    Completed,
    /// Call exists but no response — the tool may or may not have been
    /// dispatched. Blocking is the fail-closed choice.
    Indeterminate,
    /// Structural corruption (orphan, duplicate, conflict).
    Corruption,
}

/// Reason for the classification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolRecoveryReason {
    MatchingResponse,
    DispatchWithoutResponse,
    OrphanResponse,
    OrphanCall,
    DuplicateCall,
    DuplicateResponse,
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
    /// True if any corruption was found.
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
/// This is a pure function — it reads events and returns decisions. It does
/// not mutate the store or resume execution.
pub fn resolve_recovery(events: &[RuntimeEvent]) -> RecoveryResolution {
    // Collect all function_call and function_response events.
    let mut calls: HashMap<String, &RuntimeEvent> = HashMap::new();
    let mut responses: HashMap<String, &RuntimeEvent> = HashMap::new();
    let mut decisions: Vec<ToolRecoveryDecision> = Vec::new();
    let mut has_corruption = false;
    let mut has_indeterminate = false;

    // First pass: detect duplicates and build lookup maps.
    for ev in events {
        if let Some(content) = &ev.content {
            match content {
                Content::FunctionCall { id, name, .. } => {
                    if calls.contains_key(id) {
                        decisions.push(ToolRecoveryDecision {
                            tool_call_id: id.clone(),
                            tool_name: Some(name.clone()),
                            status: ToolRecoveryStatus::Corruption,
                            reason: ToolRecoveryReason::DuplicateCall,
                        });
                        has_corruption = true;
                    } else {
                        calls.insert(id.clone(), ev);
                    }
                }
                Content::FunctionResponse { id, .. } => {
                    if responses.contains_key(id) {
                        decisions.push(ToolRecoveryDecision {
                            tool_call_id: id.clone(),
                            tool_name: None,
                            status: ToolRecoveryStatus::Corruption,
                            reason: ToolRecoveryReason::DuplicateResponse,
                        });
                        has_corruption = true;
                    } else {
                        responses.insert(id.clone(), ev);
                    }
                }
                _ => {}
            }
        }
    }

    // Second pass: classify each call.
    for (call_id, call_ev) in &calls {
        let tool_name = call_ev
            .content
            .as_ref()
            .and_then(|c| match c {
                Content::FunctionCall { name, .. } => Some(name.clone()),
                _ => None,
            });

        if let Some(_response_ev) = responses.get(call_id) {
            decisions.push(ToolRecoveryDecision {
                tool_call_id: call_id.clone(),
                tool_name,
                status: ToolRecoveryStatus::Completed,
                reason: ToolRecoveryReason::MatchingResponse,
            });
        } else {
            decisions.push(ToolRecoveryDecision {
                tool_call_id: call_id.clone(),
                tool_name,
                status: ToolRecoveryStatus::Indeterminate,
                reason: ToolRecoveryReason::DispatchWithoutResponse,
            });
            has_indeterminate = true;
        }
    }

    // Third pass: orphan responses (response without a matching call).
    for (resp_id, _) in &responses {
        if !calls.contains_key(resp_id) {
            decisions.push(ToolRecoveryDecision {
                tool_call_id: resp_id.clone(),
                tool_name: None,
                status: ToolRecoveryStatus::Corruption,
                reason: ToolRecoveryReason::OrphanResponse,
            });
            has_corruption = true;
        }
    }

    let plan = if has_corruption || has_indeterminate {
        RecoveryPlan::Blocked
    } else {
        RecoveryPlan::SafeReplay
    };

    RecoveryResolution {
        decisions,
        has_corruption,
        has_indeterminate,
        plan,
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
