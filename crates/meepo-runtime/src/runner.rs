//! RuntimeRunner — the invocation shell.
//!
//! Consumes an [`AgentBackend`]'s session-event stream, maps each event to a
//! canonical RuntimeEvent, and enforces the per-invocation terminal invariant:
//! exactly one terminal event is accepted; if the stream ends without one, a
//! `missing_terminal_event` failure is synthesized.
//!
//! The runner does NOT persist — it returns the collected events and the
//! terminal fact. The orchestration layer above owns the ledger writes (this
//! mirrors the upstream layering and keeps the runner testable with fakes).

use futures::stream::StreamExt;

use meepo_core::{Author, BackendSendInput, Content, AgentBackend, Role, RuntimeEvent, Status};

use crate::map_session_event::{map_session_event, InvocationContext};

/// Coarse outcome of an invocation, derived from the terminal event's status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStatus {
    Completed,
    Failed,
    Aborted,
}

/// What [`RuntimeRunner::run`] returns.
#[derive(Debug, Clone)]
pub struct RunResult {
    /// Every accepted event in order, including the terminal one.
    pub events: Vec<RuntimeEvent>,
    /// The single accepted terminal event.
    pub terminal: RuntimeEvent,
    /// Coarse status derived from `terminal`.
    pub status: RunStatus,
}

/// Stateless invocation shell. All state lives in the backend and the caller.
pub struct RuntimeRunner;

impl RuntimeRunner {
    /// Drive one invocation: consume the backend stream, map to canonical
    /// events, stop at the first terminal, synthesize a failure if none.
    pub async fn run<B>(backend: &mut B, ctx: &InvocationContext, input: &BackendSendInput) -> RunResult
    where
        B: AgentBackend + ?Sized,
    {
        let mut events: Vec<RuntimeEvent> = Vec::new();
        let mut terminal: Option<RuntimeEvent> = None;

        let mut stream = backend.send(input);
        while let Some(session_event) = stream.next().await {
            // Once a terminal is accepted, drain the rest (walking skeleton
            // breaks immediately, so this only guards against a re-entrant
            // terminal within the same stream chunk).
            if terminal.is_some() {
                continue;
            }
            let runtime_event = map_session_event(&session_event, ctx);
            let is_terminal = runtime_event.status.is_some_and(|s| s.is_terminal());
            if is_terminal {
                terminal = Some(runtime_event.clone());
                events.push(runtime_event);
                break;
            }
            events.push(runtime_event);
        }

        let terminal = terminal.unwrap_or_else(|| {
            // The stream ended without a terminal event — synthesize a failure
            // so the invariant (exactly one terminal per invocation) holds.
            let missing = missing_terminal_event(ctx);
            events.push(missing.clone());
            missing
        });

        let status = match terminal.status {
            Some(Status::Completed) => RunStatus::Completed,
            Some(Status::Aborted) => RunStatus::Aborted,
            _ => RunStatus::Failed,
        };

        RunResult { events, terminal, status }
    }
}

fn missing_terminal_event(ctx: &InvocationContext) -> RuntimeEvent {
    RuntimeEvent {
        session_id: ctx.session_id.clone(),
        invocation_id: ctx.invocation_id.clone(),
        run_id: ctx.run_id.clone(),
        turn_id: ctx.turn_id.clone(),
        branch: None,
        id: format!("{}-missing-terminal", ctx.invocation_id),
        ts: 0,
        role: Role::System,
        author: Author::System,
        origin: None,
        model_visibility: None,
        status: Some(Status::Failed),
        content: Some(Content::Error {
            message: "backend stream ended without a terminal event".into(),
            code: Some("missing_terminal_event".into()),
            reason: Some("missing_terminal_event".into()),
            details: None,
        }),
        actions: None,
        refs: None,
        partial: None,
    }
}
