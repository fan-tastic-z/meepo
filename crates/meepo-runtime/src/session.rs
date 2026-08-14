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

use std::pin::Pin;
use std::task::{Context, Poll};

use meepo_core::{
    AgentBackend, BackendSendInput, ChatMessage, RuntimeEvent, RuntimeEventStore, StopToken,
};

use futures::stream::Stream;
use futures::StreamExt;

use crate::map_session_event::InvocationContext;
use crate::recovery::{resolve_recovery, RecoveryPlan};
use crate::runner::{RuntimeRunner, RunStatus, TurnEvent};

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

/// A turn admitted and running but not yet driven to completion or finalized.
///
/// The caller drives this stream (collecting events until the `Done` event),
/// then calls [`SessionManager::finalize_turn`] to persist and transition
/// status. It borrows the backend for the stream's lifetime but NOT the
/// session, so `finalize_turn` may run while a handle is still held.
pub struct TurnStream<'a> {
    stream: Pin<Box<dyn Stream<Item = TurnEvent> + 'a>>,
    /// The run id this turn writes its events under.
    pub run_id: String,
}

impl<'a> Stream for TurnStream<'a> {
    type Item = TurnEvent;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<TurnEvent>> {
        self.get_mut().stream.as_mut().poll_next(cx)
    }
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

    /// Execute one turn, streaming each RuntimeEvent to `on_event` as it is
    /// produced (for live terminal/UI output). Handles admission, persistence,
    /// the terminal-durability boundary, and the rolling-compaction summary.
    ///
    /// This is the embedded path: it drives the turn to completion in one
    /// await, passing a never-cancelled stop token. The host path uses
    /// [`start_turn_streaming`](Self::start_turn_streaming) + a real stop token.
    pub async fn send_turn_streaming<B, S, F>(
        &mut self,
        backend: &mut B,
        store: &S,
        user_message: String,
        system_prompt: Option<String>,
        tools: &[serde_json::Value],
        mut on_event: F,
    ) -> TurnResult
    where
        B: AgentBackend + ?Sized,
        S: RuntimeEventStore + ?Sized,
        F: FnMut(&RuntimeEvent),
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

        let mut ts = self.start_turn_streaming(
            backend,
            user_message,
            system_prompt,
            tools,
            StopToken::never(),
        );
        let run_id = ts.run_id.clone();
        let mut events = Vec::new();
        let mut terminal = None;
        let mut status = RunStatus::Failed;
        let mut messages = Vec::new();
        let mut compact_summary = None;
        while let Some(te) = ts.next().await {
            match te {
                TurnEvent::Event(re) => {
                    on_event(&re);
                    events.push(re);
                }
                TurnEvent::Done {
                    terminal: t,
                    status: st,
                    messages: m,
                    compact_summary: c,
                } => {
                    terminal = Some(t);
                    status = st;
                    messages = m;
                    compact_summary = c;
                }
            }
        }
        let terminal = terminal.expect("turn ended without a Done event");
        self.finalize_turn(store, &run_id, terminal, events, messages, compact_summary, status)
            .await
    }

    /// Admit a turn and start it running without driving to completion. The
    /// caller drives the returned [`TurnStream`] and then finalizes. Does NOT
    /// re-check admission — the caller (the embedded wrapper or the host
    /// admission gate) must ensure no concurrent turn. `stop` lets the host
    /// cancel the run.
    pub fn start_turn_streaming<'a, B>(
        &mut self,
        backend: &'a mut B,
        user_message: String,
        system_prompt: Option<String>,
        tools: &[serde_json::Value],
        stop: StopToken,
    ) -> TurnStream<'a>
    where
        B: AgentBackend + ?Sized,
    {
        self.status = SessionStatus::Running;
        self.messages.push(ChatMessage::User { content: user_message });

        let turn_id = format!("turn-{}", self.messages.len());
        let run_id = format!("run-{}", self.messages.len());
        let invocation_id = format!("inv-{}", self.messages.len());

        let ctx = InvocationContext {
            session_id: self.session_id.clone(),
            run_id: run_id.clone(),
            invocation_id,
            turn_id,
        };
        let input = BackendSendInput {
            turn_id: ctx.turn_id.clone(),
            run_id: Some(run_id.clone()),
            invocation_id: Some(ctx.invocation_id.clone()),
            max_steps: None,
            messages: self.messages.clone(),
            system_prompt,
            tools: tools.to_vec(),
        };
        let prev_summary = self.compact_summary.clone();
        let stream = RuntimeRunner::run_stream(backend, ctx, input, prev_summary, stop);
        TurnStream { stream: Box::pin(stream), run_id }
    }

    /// Persist a completed turn's events, make the terminal durable, and
    /// transition session status. Called after a [`TurnStream`] is driven to
    /// its `Done` event.
    pub async fn finalize_turn<S>(
        &mut self,
        store: &S,
        run_id: &str,
        terminal: RuntimeEvent,
        events: Vec<RuntimeEvent>,
        messages: Vec<ChatMessage>,
        compact_summary: Option<String>,
        status: RunStatus,
    ) -> TurnResult
    where
        S: RuntimeEventStore + ?Sized,
    {
        // Persist events.
        for ev in &events {
            let _ = store
                .append_runtime_event(&self.session_id, run_id, ev.clone(), false)
                .await;
        }
        // Crash-safety boundary: make the terminal event durable (idempotent).
        let _ = store
            .ensure_terminal_runtime_event_durable(&self.session_id, run_id, terminal.clone())
            .await;

        // Carry conversation messages and the rolling compaction summary.
        self.messages = messages.clone();
        self.compact_summary = compact_summary.clone();

        // Transition status.
        self.status = match status {
            RunStatus::Completed => SessionStatus::Done,
            RunStatus::Failed | RunStatus::Aborted => SessionStatus::Aborted,
        };
        if self.status == SessionStatus::Done {
            self.status = SessionStatus::WaitingForUser;
        }

        TurnResult {
            status,
            messages,
            events,
            compact_summary,
        }
    }

    /// Execute one turn without streaming — equivalent to `send_turn_streaming`
    /// with a no-op event callback.
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
        self.send_turn_streaming(backend, store, user_message, system_prompt, tools, |_| {}).await
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
