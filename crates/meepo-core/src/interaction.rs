//! Interaction contracts and the permission gate.
//!
//! When a tool call's policy is Prompt, the runtime builds a
//! [`PermissionRequest`] and asks a [`PermissionPrompter`] (the host — CLI
//! today, desktop/TUI later); the answer resolves to Allow/Deny. Question and
//! sandbox-boundary request kinds are reserved for later phases. The gate is
//! the single execution-boundary check the tool loop calls before dispatching
//! a tool, and returns a [`PermissionResolution`] carrying both the verdict
//! and the request — so the caller can persist a canonical outcome.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::permission::{
    classify_tool_use, policy_decision, PermissionMode, PolicyDecision, ToolCategory,
};
use crate::store::StoreResult;

/// Why the user is being asked — drives the prompt wording.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionReason {
    ShellDangerous,
    FileWrite,
    FsDestructive,
    GitDestructive,
    Privileged,
    Custom,
}

impl PermissionReason {
    pub fn for_category(category: ToolCategory) -> Self {
        match category {
            ToolCategory::ShellUnsafe => PermissionReason::ShellDangerous,
            ToolCategory::FileWrite => PermissionReason::FileWrite,
            ToolCategory::FsDestructive => PermissionReason::FsDestructive,
            ToolCategory::GitDestructive => PermissionReason::GitDestructive,
            ToolCategory::Privileged => PermissionReason::Privileged,
            _ => PermissionReason::Custom,
        }
    }
}

/// One permission request carried by the runtime to the host.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PermissionRequest {
    pub tool_call_id: String,
    pub tool_name: String,
    pub category: ToolCategory,
    pub reason: PermissionReason,
    /// A short human-facing summary of what the tool will do (the bash command,
    /// the target path, ...).
    pub summary: String,
    /// Whether an "allow + remember for the rest of this turn" choice is valid.
    pub remember_for_turn_allowed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PermissionDecision {
    Allow,
    Deny,
}

/// The host's answer to a permission request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PermissionAnswer {
    pub decision: PermissionDecision,
    /// Only meaningful when decision is Allow AND the request allowed it.
    pub remember_for_turn: bool,
}

/// Canonical outcome persisted for audit / replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PermissionOutcome {
    pub tool_call_id: String,
    pub decision: PermissionDecision,
    pub remember_for_turn: bool,
    pub committed_at: i64,
}

/// Verdict the tool loop acts on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionVerdict {
    Allow,
    Deny,
}

/// What [`PermissionGate::check`] returns: the verdict plus the request that
/// produced it, so the caller can persist a canonical outcome.
#[derive(Debug, Clone)]
pub struct PermissionResolution {
    pub verdict: PermissionVerdict,
    pub request: PermissionRequest,
}

/// The host side of a permission prompt: given a request, return an answer.
#[async_trait]
pub trait PermissionPrompter: Send + Sync {
    async fn ask(&self, request: &PermissionRequest) -> PermissionAnswer;
}

/// The execution-boundary check the tool loop calls before dispatching a tool.
#[async_trait]
pub trait PermissionGate: Send + Sync {
    async fn check(
        &self,
        tool_call_id: &str,
        tool_name: &str,
        args: &Value,
    ) -> PermissionResolution;
}

/// Default gate: classify the call, apply the mode ceiling, and ask the
/// prompter only when the policy is Prompt. Always constructs the request so
/// the caller can persist it regardless of the path taken.
pub struct DefaultPermissionGate {
    mode: PermissionMode,
    prompter: Arc<dyn PermissionPrompter>,
}

impl DefaultPermissionGate {
    pub fn new(mode: PermissionMode, prompter: Arc<dyn PermissionPrompter>) -> Self {
        Self { mode, prompter }
    }
}

#[async_trait]
impl PermissionGate for DefaultPermissionGate {
    async fn check(
        &self,
        tool_call_id: &str,
        tool_name: &str,
        args: &Value,
    ) -> PermissionResolution {
        let category = classify_tool_use(tool_name, args);
        let request = PermissionRequest {
            tool_call_id: tool_call_id.to_string(),
            tool_name: tool_name.to_string(),
            category,
            reason: PermissionReason::for_category(category),
            summary: summarize_call(tool_name, args),
            remember_for_turn_allowed: true,
        };
        let verdict = match policy_decision(self.mode, category) {
            PolicyDecision::Allow => PermissionVerdict::Allow,
            PolicyDecision::Block => PermissionVerdict::Deny,
            PolicyDecision::Prompt => {
                let answer = self.prompter.ask(&request).await;
                match answer.decision {
                    PermissionDecision::Allow => PermissionVerdict::Allow,
                    PermissionDecision::Deny => PermissionVerdict::Deny,
                }
            }
        };
        PermissionResolution { verdict, request }
    }
}

/// Persists canonical permission requests + outcomes. The two-table shape
/// (request row, outcome row FK→request) is byte-aligned with the upstream
/// `core_interaction_requests` / `core_interaction_outcomes` schema so a
/// database written by either side is readable by the other.
#[async_trait]
pub trait InteractionStore: Send + Sync {
    /// Record one permission request and its outcome atomically. `request_id`
    /// is the primary key (idempotent on re-write). `created_at` is the
    /// caller's clock (runtime event counter / wall ts).
    async fn record_permission(
        &self,
        session_id: &str,
        run_id: &str,
        turn_id: &str,
        request_id: &str,
        created_at: i64,
        request_json: &str,
        outcome_json: &str,
    ) -> StoreResult<()>;
}

/// Build a short human-facing summary of what the tool will do.
fn summarize_call(tool_name: &str, args: &Value) -> String {
    if let Some(cmd) = args.get("command").and_then(Value::as_str) {
        return cmd.to_string();
    }
    if let Some(path) = args.get("path").and_then(Value::as_str) {
        return format!("{tool_name} {path}");
    }
    tool_name.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct StubPrompter {
        answer: PermissionDecision,
        asked: Mutex<usize>,
    }

    #[async_trait]
    impl PermissionPrompter for StubPrompter {
        async fn ask(&self, _request: &PermissionRequest) -> PermissionAnswer {
            *self.asked.lock().unwrap() += 1;
            PermissionAnswer { decision: self.answer, remember_for_turn: false }
        }
    }

    #[tokio::test]
    async fn bypass_allows_without_prompting() {
        let p = Arc::new(StubPrompter { answer: PermissionDecision::Deny, asked: Mutex::new(0) });
        let gate = DefaultPermissionGate::new(PermissionMode::Bypass, p.clone());
        let r = gate.check("c1", "bash", &serde_json::json!({"command":"rm -rf /"})).await;
        assert_eq!(r.verdict, PermissionVerdict::Allow);
        assert_eq!(*p.asked.lock().unwrap(), 0, "bypass must not prompt");
    }

    #[tokio::test]
    async fn explore_blocks_write_without_prompting() {
        let p = Arc::new(StubPrompter { answer: PermissionDecision::Allow, asked: Mutex::new(0) });
        let gate = DefaultPermissionGate::new(PermissionMode::Explore, p.clone());
        let r = gate.check("c1", "write_file", &serde_json::json!({"path":"/x"})).await;
        assert_eq!(r.verdict, PermissionVerdict::Deny);
        assert_eq!(*p.asked.lock().unwrap(), 0, "explore blocks silently");
    }

    #[tokio::test]
    async fn ask_prompts_for_shell_and_respects_allow() {
        let p = Arc::new(StubPrompter { answer: PermissionDecision::Allow, asked: Mutex::new(0) });
        let gate = DefaultPermissionGate::new(PermissionMode::Ask, p.clone());
        let r = gate.check("c1", "bash", &serde_json::json!({"command":"ls"})).await;
        assert_eq!(r.verdict, PermissionVerdict::Allow);
        assert_eq!(*p.asked.lock().unwrap(), 1, "ask mode must prompt for shell");
        assert_eq!(r.request.summary, "ls");
    }

    #[tokio::test]
    async fn ask_denied_when_prompter_denies() {
        let p = Arc::new(StubPrompter { answer: PermissionDecision::Deny, asked: Mutex::new(0) });
        let gate = DefaultPermissionGate::new(PermissionMode::Ask, p.clone());
        let r = gate.check("c1", "bash", &serde_json::json!({"command":"ls"})).await;
        assert_eq!(r.verdict, PermissionVerdict::Deny);
    }

    #[tokio::test]
    async fn read_never_prompts() {
        let p = Arc::new(StubPrompter { answer: PermissionDecision::Deny, asked: Mutex::new(0) });
        let gate = DefaultPermissionGate::new(PermissionMode::Explore, p.clone());
        let r = gate.check("c1", "read_file", &serde_json::json!({"path":"/x"})).await;
        assert_eq!(r.verdict, PermissionVerdict::Allow);
        assert_eq!(*p.asked.lock().unwrap(), 0);
    }
}
