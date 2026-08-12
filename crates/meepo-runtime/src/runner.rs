//! RuntimeRunner — the invocation shell.
//!
//! After the architecture alignment, the runner only consumes the backend
//! stream (which internally drives the tool loop) and maps SessionEvents to
//! canonical RuntimeEvents. It does NOT do step loops or tool execution —
//! those live inside the backend (like maka's sendWithinScope).

use futures::stream::{Stream, StreamExt};

use meepo_core::{
    AgentBackend, AssistantToolCall, BackendSendInput, ChatMessage, RuntimeEvent, SessionEvent,
    Status,
};

use crate::map_session_event::{map_session_event, InvocationContext};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStatus {
    Completed,
    Failed,
    Aborted,
}

#[derive(Debug, Clone)]
pub struct RunResult {
    pub events: Vec<RuntimeEvent>,
    pub terminal: RuntimeEvent,
    pub status: RunStatus,
    pub messages: Vec<ChatMessage>,
    /// Summary produced by compaction this turn (None if compaction did not
    /// run). The caller persists this and passes it back as
    /// `previous_compact_summary` on the next turn for rolling compaction.
    pub compact_summary: Option<String>,
}

#[derive(Debug, Clone)]
pub enum TurnEvent {
    Event(RuntimeEvent),
    Done {
        terminal: RuntimeEvent,
        status: RunStatus,
        messages: Vec<ChatMessage>,
        /// Summary from compaction (if it ran). Pass to the next turn for rolling.
        compact_summary: Option<String>,
    },
}

pub struct RuntimeRunner;

impl RuntimeRunner {
    pub fn run_stream<'a, B>(
        backend: &'a mut B,
        ctx: &'a InvocationContext,
        input: &'a BackendSendInput,
        previous_compact_summary: Option<&'a str>,
    ) -> impl Stream<Item = TurnEvent> + 'a
    where
        B: AgentBackend + ?Sized,
    {
        async_stream::stream! {
            let mut messages = input.messages.clone();

            // Record the user turn in the ledger.
            if let Some(ChatMessage::User { content }) = input.messages.last() {
                yield TurnEvent::Event(user_event(ctx, content));
            }

            // Compaction (once, before send — a projection; the store is not touched).
            // Uses rolling: previous turn's summary + newly folded messages.
            let compact_result =
                crate::compaction::compact_if_needed_rolling(&*backend, &messages, previous_compact_summary).await;
            messages = compact_result.messages;
            let compact_summary = compact_result.summary;

            // Build the send input with compacted messages.
            let send_input = BackendSendInput {
                messages: messages.clone(),
                ..input.clone()
            };

            // Consume the backend stream — the backend drives the internal
            // tool loop and emits all SessionEvents (text, tool_call,
            // tool_result, terminal).
            let mut stream = Box::pin(backend.send(&send_input));
            let mut terminal_se: Option<SessionEvent> = None;

            let mut pending_text = String::new();
            while let Some(se) = stream.next().await {
                match &se {
                    SessionEvent::TextComplete { text, .. } => {
                        pending_text = text.clone();
                    }
                    SessionEvent::TextDelta { text, .. } => {
                        pending_text.push_str(text);
                    }
                    SessionEvent::ThinkingDelta { .. } | SessionEvent::ThinkingComplete { .. } => {
                        // Thinking events are mapped to RuntimeEvents but don't
                        // accumulate into the conversation messages here — the
                        // projection layer handles thinking replay from the ledger.
                    }
                    SessionEvent::ToolCall { tool_call_id, tool_name, args, .. } => {
                        let content = if pending_text.is_empty() {
                            None
                        } else {
                            Some(std::mem::take(&mut pending_text))
                        };
                        messages.push(ChatMessage::Assistant {
                            content,
                            tool_calls: vec![AssistantToolCall {
                                id: tool_call_id.clone(),
                                name: tool_name.clone(),
                                args: args.clone(),
                            }],
                            thinking: vec![],
                        });
                    }
                    SessionEvent::ToolResult { tool_call_id, content, .. } => {
                        messages.push(ChatMessage::Tool {
                            tool_call_id: tool_call_id.clone(),
                            content: content.clone(),
                        });
                    }
                    SessionEvent::Complete { .. }
                    | SessionEvent::Error { .. }
                    | SessionEvent::Abort { .. } => {
                        if !pending_text.is_empty() {
                            messages.push(ChatMessage::Assistant {
                                content: Some(std::mem::take(&mut pending_text)),
                                tool_calls: vec![],
                                thinking: vec![],
                            });
                        }
                        terminal_se = Some(se.clone());
                    }
                }
                yield TurnEvent::Event(map_session_event(&se, ctx));
            }

            let terminal_se = terminal_se.unwrap_or_else(|| SessionEvent::Error {
                id: format!("{}-missing-terminal", input.turn_id),
                turn_id: input.turn_id.clone(),
                ts: 0,
                recoverable: false,
                message: "backend stream ended without a terminal event".into(),
                code: Some("missing_terminal_event".into()),
                reason: Some("missing_terminal_event".into()),
                details: None,
            });
            let terminal = map_session_event(&terminal_se, ctx);
            let status = run_status(&terminal);
            yield TurnEvent::Done { terminal, status, messages, compact_summary };
        }
    }

    pub async fn run<B>(
        backend: &mut B,
        ctx: &InvocationContext,
        input: &BackendSendInput,
        previous_compact_summary: Option<&str>,
    ) -> RunResult
    where
        B: AgentBackend + ?Sized,
    {
        let mut events = Vec::new();
        let mut terminal = None;
        let mut status = RunStatus::Failed;
        let mut messages = Vec::new();
        let mut compact_summary = None;
        let mut s = Box::pin(Self::run_stream(
            backend,
            ctx,
            input,
            previous_compact_summary,
        ));
        while let Some(te) = s.next().await {
            match te {
                TurnEvent::Event(re) => events.push(re),
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
        RunResult {
            events,
            terminal: terminal.expect("turn ended without Done"),
            status,
            messages,
            compact_summary,
        }
    }
}

fn user_event(ctx: &InvocationContext, content: &str) -> RuntimeEvent {
    RuntimeEvent {
        session_id: ctx.session_id.clone(),
        invocation_id: ctx.invocation_id.clone(),
        run_id: ctx.run_id.clone(),
        turn_id: ctx.turn_id.clone(),
        branch: None,
        id: format!("{}-user", ctx.invocation_id),
        ts: 0,
        role: meepo_core::Role::User,
        author: meepo_core::Author::User,
        origin: None,
        model_visibility: None,
        status: None,
        content: Some(meepo_core::Content::Text {
            text: content.to_string(),
            provider_options: None,
            steering: None,
        }),
        actions: None,
        refs: None,
        partial: None,
    }
}

fn run_status(terminal: &RuntimeEvent) -> RunStatus {
    match terminal.status {
        Some(Status::Completed) => RunStatus::Completed,
        Some(Status::Aborted) => RunStatus::Aborted,
        _ => RunStatus::Failed,
    }
}
