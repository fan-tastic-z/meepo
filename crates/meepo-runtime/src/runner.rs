//! RuntimeRunner — the invocation shell with a tool step-loop and history.
//!
//! [`RuntimeRunner::run_stream`] yields events as they happen (so a caller can
//! render text incrementally); [`RuntimeRunner::run`] is the collecting
//! convenience wrapper used by tests.

use futures::stream::{Stream, StreamExt};

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
    pub messages: Vec<ChatMessage>,
}

/// One streamed item from a turn.
#[derive(Debug, Clone)]
pub enum TurnEvent {
    /// A canonical fact, yielded as soon as it is produced.
    Event(RuntimeEvent),
    /// The turn has terminated.
    Done {
        terminal: RuntimeEvent,
        status: RunStatus,
        messages: Vec<ChatMessage>,
    },
}

pub struct RuntimeRunner;

impl RuntimeRunner {
    /// Stream the turn: events are yielded live, then a single `Done`.
    pub fn run_stream<'a, B>(
        backend: &'a mut B,
        ctx: &'a InvocationContext,
        input: &'a BackendSendInput,
        tools: &'a ToolRegistry,
    ) -> impl Stream<Item = TurnEvent> + 'a
    where
        B: AgentBackend + ?Sized,
    {
        async_stream::stream! {
            let mut messages = input.messages.clone();
            let max_steps = input.max_steps.unwrap_or(50);
            let mut terminal_se: Option<SessionEvent> = None;
            let mut seq: i64 = 0;

            // Record the user turn in the ledger.
            if let Some(ChatMessage::User { content }) = input.messages.last() {
                yield TurnEvent::Event(user_event(ctx, content));
            }

            'outer: for _step in 0..max_steps {
                let step_input = BackendSendInput {
                    turn_id: input.turn_id.clone(),
                    run_id: input.run_id.clone(),
                    invocation_id: input.invocation_id.clone(),
                    max_steps: input.max_steps,
                    messages: messages.clone(),
                    system_prompt: input.system_prompt.clone(),
                    tools: tools.openai_functions(),
                };
                let mut tool_calls: Vec<(String, String, serde_json::Value)> = Vec::new();
                let mut step_text = String::new();
                let mut step_terminal: Option<SessionEvent> = None;

                let mut stream = backend.send(&step_input);
                while let Some(se) = stream.next().await {
                    let is_term = matches!(
                        &se,
                        SessionEvent::Complete { .. }
                            | SessionEvent::Error { .. }
                            | SessionEvent::Abort { .. }
                    );
                    match &se {
                        SessionEvent::ToolCall { tool_call_id, tool_name, args, .. } => {
                            tool_calls.push((tool_call_id.clone(), tool_name.clone(), args.clone()));
                        }
                        SessionEvent::TextComplete { text, .. } => step_text = text.clone(),
                        SessionEvent::TextDelta { text, .. } if step_text.is_empty() => {
                            step_text.push_str(text);
                        }
                        _ => {}
                    }
                    yield TurnEvent::Event(map_session_event(&se, ctx));
                    if is_term {
                        step_terminal = Some(se);
                        break;
                    }
                }

                // Record this step's assistant output.
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

                if let Some(t) = step_terminal {
                    terminal_se = Some(t);
                    break 'outer;
                }
                if tool_calls.is_empty() {
                    seq += 1;
                    let missing = missing_terminal_se(&input.turn_id, seq);
                    yield TurnEvent::Event(map_session_event(&missing, ctx));
                    terminal_se = Some(missing);
                    break 'outer;
                }

                // Execute tools.
                for (tc_id, tc_name, tc_args) in &tool_calls {
                    let (content, is_error) = match tools.execute(tc_name, tc_args).await {
                        Ok(c) => (c, false),
                        Err(e) => (e.to_string(), true),
                    };
                    let content = truncate_tool_result(content);
                    seq += 1;
                    let tr = SessionEvent::ToolResult {
                        id: format!("tool-result-{tc_id}"),
                        turn_id: input.turn_id.clone(),
                        ts: seq,
                        tool_call_id: tc_id.clone(),
                        tool_name: tc_name.clone(),
                        content: content.clone(),
                        is_error,
                    };
                    yield TurnEvent::Event(map_session_event(&tr, ctx));
                    messages.push(ChatMessage::Tool {
                        tool_call_id: tc_id.clone(),
                        content,
                    });
                }
            }

            let (terminal, status) = match terminal_se {
                Some(t) => {
                    let te = map_session_event(&t, ctx);
                    let status = run_status(&te);
                    (te, status)
                }
                None => {
                    seq += 1;
                    let sl = SessionEvent::Complete {
                        id: format!("{}-step-limit", input.turn_id),
                        turn_id: input.turn_id.clone(),
                        ts: seq,
                        stop_reason: StopReason::StepLimit,
                    };
                    let te = map_session_event(&sl, ctx);
                    yield TurnEvent::Event(te.clone());
                    let status = run_status(&te);
                    (te, status)
                }
            };
            yield TurnEvent::Done { terminal, status, messages };
        }
    }

    /// Collecting wrapper: gather all events + the terminal into a RunResult.
    pub async fn run<B>(
        backend: &mut B,
        ctx: &InvocationContext,
        input: &BackendSendInput,
        tools: &ToolRegistry,
    ) -> RunResult
    where
        B: AgentBackend + ?Sized,
    {
        let mut events = Vec::new();
        let mut terminal = None;
        let mut status = RunStatus::Failed;
        let mut messages = Vec::new();
        let mut s = Box::pin(Self::run_stream(backend, ctx, input, tools));
        while let Some(te) = s.next().await {
            match te {
                TurnEvent::Event(re) => events.push(re),
                TurnEvent::Done { terminal: t, status: st, messages: m } => {
                    terminal = Some(t);
                    status = st;
                    messages = m;
                }
            }
        }
        RunResult {
            events,
            terminal: terminal.expect("turn ended without Done"),
            status,
            messages,
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

/// Cap a tool result so a single huge output (e.g. `cargo test` log) cannot
/// blow the context window. Mirrors maka's TOOL_OUTPUT_DELTA_MAX_CHARS idea.
fn truncate_tool_result(content: String) -> String {
    const MAX_CHARS: usize = 8_000;
    if content.chars().count() <= MAX_CHARS {
        return content;
    }
    let mut head: String = content.chars().take(MAX_CHARS).collect();
    head.push_str("\n…[truncated by meepo; ");
    head.push_str(&content.chars().count().to_string());
    head.push_str(" chars total]");
    head
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
