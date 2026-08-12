//! SessionManager — owns the lifecycle of an agent session.
//!
//! Encapsulates what was previously ad-hoc in CLI: session status transitions,
//! turn admission (no concurrent turns), recovery scanning on resume, event
//! persistence, and compaction summary management.
//!
//! Architecture (mirrors maka):
//!   CLI → SessionManager → RuntimeRunner → Backend
//!            ↓
//!       SqliteStore (persist events)
//!       RecoveryResolver (scan on resume)
//!       Compaction (rolling summary)

use meepo_core::{
    AgentBackend, BackendSendInput, ChatMessage, RuntimeEvent, RuntimeEventStore,
};

use crate::map_session_event::InvocationContext;
use crate::recovery::{resolve_recovery, RecoveryPlan};
use crate::runner::{RuntimeRunner, RunStatus};

/// Session lifecycle status (mirrors maka's SESSION_STATUSES).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionStatus {
    /// No turn in progress, waiting for user input.
    WaitingForUser,
    /// A turn is currently executing.
    Running,
    /// Turn completed normally.
    Done,
    /// Session was aborted (error or user stop).
    Aborted,
}

/// Result of a single turn.
#[derive(Debug, Clone)]
pub struct TurnResult {
    pub status: RunStatus,
    pub messages: Vec<ChatMessage>,
    /// Events produced during this turn (for persistence).
    pub events: Vec<RuntimeEvent>,
    /// Compact summary from this turn (for rolling compaction on the next turn).
    pub compact_summary: Option<String>,
}

/// Manages a single agent session's lifecycle.
pub struct SessionManager {
    session_id: String,
    status: SessionStatus,
    messages: Vec<ChatMessage>,
    compact_summary: Option<String>,
    /// Track whether recovery was needed on resume.
    recovery_needed: bool,
}

impl SessionManager {
    /// Resume a session from a store: read events, run recovery, rebuild messages.
    pub async fn resume<S: RuntimeEventStore + ?Sized>(
        session_id: impl Into<String>,
        store: &S,
    ) -> Self {
        let session_id = session_id.into();
        let events = store.read_session_runtime_events(&session_id).await.unwrap_or_default();

        // Recovery scan.
        let recovery = resolve_recovery(&events);
        let recovery_needed = recovery.plan == RecoveryPlan::Blocked;

        // Rebuild conversation messages (projection drops orphaned calls).
        let messages = crate::projection::messages_from_runtime_events(&events);

        Self {
            session_id,
            status: SessionStatus::WaitingForUser,
            messages,
            compact_summary: None,
            recovery_needed,
        }
    }

    /// Create a fresh session with no history.
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            status: SessionStatus::WaitingForUser,
            messages: Vec::new(),
            compact_summary: None,
            recovery_needed: false,
        }
    }

    /// Execute one turn: send user message → run agent loop → persist events.
    pub async fn send_turn<B, S>(
        &mut self,
        backend: &mut B,
        store: &S,
        user_message: String,
        system_prompt: Option<String>,
        tools: &[serde_json::Value],
    ) -> TurnResult
    where
        B: AgentBackend + ?Sized,
        S: RuntimeEventStore + ?Sized,
    {
        // Admission: can't start a turn while one is running.
        if self.status == SessionStatus::Running {
            return TurnResult {
                status: RunStatus::Failed,
                messages: self.messages.clone(),
                events: Vec::new(),
                compact_summary: self.compact_summary.clone(),
            };
        }
        self.status = SessionStatus::Running;

        // Push user message.
        self.messages.push(ChatMessage::User { content: user_message.clone() });

        // Build turn identity.
        let turn_id = format!("turn-{}", self.messages.len());
        let run_id = format!("run-{}", self.messages.len());
        let invocation_id = format!("inv-{}", self.messages.len());

        let ctx = InvocationContext {
            session_id: self.session_id.clone(),
            run_id: run_id.clone(),
            invocation_id: invocation_id.clone(),
            turn_id: turn_id.clone(),
        };

        let input = BackendSendInput {
            turn_id: turn_id.clone(),
            run_id: Some(run_id.clone()),
            invocation_id: Some(invocation_id.clone()),
            max_steps: None,
            messages: self.messages.clone(),
            system_prompt,
            tools: tools.to_vec(),
        };

        // Run the turn (runner handles compaction internally).
        let result = RuntimeRunner::run(backend, &ctx, &input).await;

        // Persist events.
        for ev in &result.events {
            let _ = store
                .append_runtime_event(&self.session_id, &run_id, ev.clone(), false)
                .await;
        }

        // Update session messages from result.
        self.messages = result.messages.clone();
        // compact_summary comes from TurnEvent::Done but run() doesn't expose it.
        // For now, compaction state is managed by the runner internally.
        // A future refactor will expose it through RunResult.

        // Transition status.
        self.status = match result.status {
            RunStatus::Completed => SessionStatus::Done,
            RunStatus::Failed | RunStatus::Aborted => SessionStatus::Aborted,
        };
        // Allow next turn after completion.
        if self.status == SessionStatus::Done {
            self.status = SessionStatus::WaitingForUser;
        }

        TurnResult {
            status: result.status,
            messages: result.messages,
            events: result.events,
            compact_summary: None,
        }
    }

    pub fn status(&self) -> SessionStatus {
        self.status
    }

    pub fn messages(&self) -> &[ChatMessage] {
        &self.messages
    }

    pub fn recovery_needed(&self) -> bool {
        self.recovery_needed
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }
}
