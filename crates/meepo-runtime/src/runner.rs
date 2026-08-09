//! RuntimeRunner — the invocation shell with a tool step-loop and history.
//!
//! Each step: drive one `backend.send`, collect events. The step's assistant
//! text and tool calls are recorded as one Assistant message in the history;
//! tool calls are executed via the [`ToolRegistry`] and their results appended
//! as Tool messages; then the loop continues. The loop ends on a terminal
//! event, a stream that ends without a tool call (synthesized failure), or the
//! step budget (step_limit).
//!
//! [`RunResult::messages`] is the full history after the run, so a caller can
//! chain another turn (multi-turn conversation).

use futures::stream::StreamExt;

use meepo_core::{
    AgentBackend, AssistantToolCall, BackendSendInput, ChatMessage, RuntimeEvent, SessionEvent,
    Status, StopReason,
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
    /// Pass this as the next turn's `BackendSendInput.messages` for multi-turn.
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
        let mut all_session: Vec<SessionEvent> = Vec::new();
        let max_steps = input.max_steps.unwrap_or(50);
        let mut terminal_se: Option<SessionEvent> = None;

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
            all_session.extend(step_events);

            // Record this step's assistant output (text + tool calls) in history.
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
                let missing = missing_terminal_se(&input.turn_id, next_ts(&all_session));
                all_session.push(missing.clone());
                terminal_se = Some(missing);
                break;
            }

            // Execute each tool, emit a ToolResult, append a Tool message.
            for (tc_id, tc_name, tc_args) in &tool_calls {
                let (content, is_error) = match tools.execute(tc_name, tc_args).await {
                    Ok(c) => (c, false),
                    Err(e) => (e.to_string(), true),
                };
                all_session.push(SessionEvent::ToolResult {
                    id: format!("tool-result-{tc_id}"),
                    turn_id: input.turn_id.clone(),
                    ts: next_ts(&all_session),
                    tool_call_id: tc_id.clone(),
                    tool_name: tc_name.clone(),
                    content: content.clone(),
                    is_error,
                });
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
                ts: next_ts(&all_session),
                stop_reason: StopReason::StepLimit,
            };
            all_session.push(sl.clone());
            sl
        });

        let events: Vec<RuntimeEvent> =
            all_session.iter().map(|se| map_session_event(se, ctx)).collect();
        let terminal = map_session_event(&terminal_se, ctx);
        let status = run_status(&terminal);
        RunResult { events, terminal, status, messages }
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

/// Assistant text for one step: prefer a TextComplete, else concatenate deltas.
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

fn next_ts(events: &[SessionEvent]) -> i64 {
    events.len() as i64
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
