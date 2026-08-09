//! RuntimeRunner — the invocation shell with a tool step-loop and history.
//!
//! Each run: emit the initial user RuntimeEvent (so the ledger records what
//! the user said), then drive steps. Each step consumes one `backend.send`;
//! the step's events are mapped to canonical RuntimeEvents, the assistant text
//! + tool calls are recorded in history, tool calls are executed, and the loop
//! continues until a terminal event, a tool-less stream end (synthesized
//! failure), or the step budget (step_limit).

use futures::stream::StreamExt;

use meepo_core::{
    AgentBackend, AssistantToolCall, BackendSendInput, ChatMessage, Content, Role, RuntimeEvent,
    SessionEvent, Status, StopReason,
};
use meepo_tools::ToolRegistry;

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
    /// Full conversation history after the run (input + this turn's additions).
    pub messages: Vec<ChatMessage>,
}

pub struct RuntimeRunner;

impl RuntimeRunner {
    pub async fn run<B>(
        backend: &mut B,
        ctx: &InvocationContext,
        input: &BackendSendInput,
        tools: &ToolRegistry,
    ) -> RunResult
    where
        B: AgentBackend + ?Sized,
    {
        let mut messages = input.messages.clone();
        let mut events: Vec<RuntimeEvent> = Vec::new();
        let max_steps = input.max_steps.unwrap_or(50);
        let mut terminal_se: Option<SessionEvent> = None;

        // Record the user turn in the ledger (the last message, if this is a
        // user turn) — without it, a resumed session would not see what the
        // user asked.
        if let Some(ChatMessage::User { content }) = input.messages.last() {
            events.push(user_event(ctx, content));
        }

        for _step in 0..max_steps {
            let step_input = BackendSendInput {
                turn_id: input.turn_id.clone(),
                run_id: input.run_id.clone(),
                invocation_id: input.invocation_id.clone(),
                max_steps: input.max_steps,
                messages: messages.clone(),
                tools: tools.openai_functions(),
            };
            let (step_events, term) = consume_step(backend, &step_input).await;
            let tool_calls = extract_tool_calls(&step_events);
            let step_text = extract_assistant_text(&step_events);

            // Map this step's backend events to canonical facts.
            for se in &step_events {
                events.push(map_session_event(se, ctx));
            }

            // Record the step's assistant output (text + tool calls) in history.
            if !step_text.is_empty() || !tool_calls.is_empty() {
                let assistant_calls: Vec<AssistantToolCall> = tool_calls
                    .iter()
                    .map(|(id, name, args)| AssistantToolCall {
                        id: id.clone(),
                        name: name.clone(),
                        args: args.clone(),
                    })
                    .collect();
                messages.push(ChatMessage::Assistant {
                    content: if step_text.is_empty() { None } else { Some(step_text) },
                    tool_calls: assistant_calls,
                });
            }

            if let Some(t) = term {
                terminal_se = Some(t);
                break;
            }
            if tool_calls.is_empty() {
                let missing = missing_terminal_se(&input.turn_id, events.len() as i64);
                events.push(map_session_event(&missing, ctx));
                terminal_se = Some(missing);
                break;
            }

            // Execute each tool, emit a ToolResult, append a Tool message.
            for (tc_id, tc_name, tc_args) in &tool_calls {
                let (content, is_error) = match tools.execute(tc_name, tc_args).await {
                    Ok(c) => (c, false),
                    Err(e) => (e.to_string(), true),
                };
                let tr = SessionEvent::ToolResult {
                    id: format!("tool-result-{tc_id}"),
                    turn_id: input.turn_id.clone(),
                    ts: events.len() as i64,
                    tool_call_id: tc_id.clone(),
                    tool_name: tc_name.clone(),
                    content: content.clone(),
                    is_error,
                };
                events.push(map_session_event(&tr, ctx));
                messages.push(ChatMessage::Tool {
                    tool_call_id: tc_id.clone(),
                    content,
                });
            }
        }

        let terminal_se = terminal_se.unwrap_or_else(|| {
            let sl = SessionEvent::Complete {
                id: format!("{}-step-limit", input.turn_id),
                turn_id: input.turn_id.clone(),
                ts: events.len() as i64,
                stop_reason: StopReason::StepLimit,
            };
            events.push(map_session_event(&sl, ctx));
            sl
        });

        let terminal = map_session_event(&terminal_se, ctx);
        let status = run_status(&terminal);
        RunResult { events, terminal, status, messages }
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
        role: Role::User,
        author: meepo_core::Author::User,
        origin: None,
        model_visibility: None,
        status: None,
        content: Some(Content::Text {
            text: content.to_string(),
            provider_options: None,
            steering: None,
        }),
        actions: None,
        refs: None,
        partial: None,
    }
}

async fn consume_step<B: AgentBackend + ?Sized>(
    backend: &mut B,
    input: &BackendSendInput,
) -> (Vec<SessionEvent>, Option<SessionEvent>) {
    let mut events = Vec::new();
    let mut term = None;
    let mut stream = backend.send(input);
    while let Some(se) = stream.next().await {
        let is_term = matches!(
            &se,
            SessionEvent::Complete { .. } | SessionEvent::Error { .. } | SessionEvent::Abort { .. }
        );
        events.push(se.clone());
        if is_term {
            term = Some(se);
            break;
        }
    }
    (events, term)
}

fn extract_tool_calls(events: &[SessionEvent]) -> Vec<(String, String, serde_json::Value)> {
    events
        .iter()
        .filter_map(|se| match se {
            SessionEvent::ToolCall { tool_call_id, tool_name, args, .. } => {
                Some((tool_call_id.clone(), tool_name.clone(), args.clone()))
            }
            _ => None,
        })
        .collect()
}

fn extract_assistant_text(events: &[SessionEvent]) -> String {
    let mut deltas = String::new();
    for se in events {
        match se {
            SessionEvent::TextComplete { text, .. } => return text.clone(),
            SessionEvent::TextDelta { text, .. } => deltas.push_str(text),
            _ => {}
        }
    }
    deltas
}

fn missing_terminal_se(turn_id: &str, ts: i64) -> SessionEvent {
    SessionEvent::Error {
        id: format!("{turn_id}-missing-terminal"),
        turn_id: turn_id.to_string(),
        ts,
        recoverable: false,
        message: "backend stream ended without a terminal event".into(),
        code: Some("missing_terminal_event".into()),
        reason: Some("missing_terminal_event".into()),
        details: None,
    }
}

fn run_status(terminal: &RuntimeEvent) -> RunStatus {
    match terminal.status {
        Some(Status::Completed) => RunStatus::Completed,
        Some(Status::Aborted) => RunStatus::Aborted,
        _ => RunStatus::Failed,
    }
}
