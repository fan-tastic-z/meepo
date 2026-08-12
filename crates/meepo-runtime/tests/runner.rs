//! RuntimeRunner behavior after architecture alignment: the runner just
//! consumes the backend stream and maps events. Tool execution lives in the
//! backend now, so FakeBackend scripts include pre-orchestrated ToolResults.

use meepo_core::{
    BackendSendInput, ChatMessage, Content, FakeBackend, Role, SessionEvent, Status, StopReason,
};
use meepo_runtime::{InvocationContext, RunStatus, RuntimeRunner};
use serde_json::json;

fn ctx() -> InvocationContext {
    InvocationContext {
        session_id: "s".into(), run_id: "r".into(),
        invocation_id: "inv".into(), turn_id: "t".into(),
    }
}

fn user_input(prompt: &str) -> BackendSendInput {
    BackendSendInput {
        turn_id: "t".into(), run_id: Some("r".into()),
        invocation_id: Some("inv".into()), max_steps: None,
        messages: vec![ChatMessage::User { content: prompt.into() }],
        system_prompt: None, tools: vec![],
    }
}

#[tokio::test]
async fn single_text_turn_completes() {
    let script = vec![
        SessionEvent::TextComplete {
            id: "1".into(), turn_id: "t".into(), ts: 0, message_id: "m".into(),
            text: "hi".into(), provider_options: None,
        },
        SessionEvent::Complete {
            id: "2".into(), turn_id: "t".into(), ts: 1, stop_reason: StopReason::EndTurn,
        },
    ];
    let mut backend = FakeBackend::new("s", script);
    let result = RuntimeRunner::run(&mut backend, &ctx(), &user_input("hi"), None).await;
    assert_eq!(result.status, RunStatus::Completed);
    assert!(result.events.iter().any(|e| matches!(e.content, Some(Content::Text { .. }))));
}

#[tokio::test]
async fn tool_loop_events_are_mapped() {
    // FakeBackend script with pre-orchestrated ToolCall + ToolResult (the
    // runner no longer executes tools — the backend does).
    let script = vec![
        SessionEvent::ToolCall {
            id: "1".into(), turn_id: "t".into(), ts: 0,
            tool_call_id: "call_00".into(), tool_name: "read_file".into(),
            args: json!({ "path": "/tmp/x" }),
        },
        SessionEvent::ToolResult {
            id: "2".into(), turn_id: "t".into(), ts: 1,
            tool_call_id: "call_00".into(), tool_name: "read_file".into(),
            content: "42".into(), is_error: false,
        },
        SessionEvent::TextComplete {
            id: "3".into(), turn_id: "t".into(), ts: 2, message_id: "m".into(),
            text: "the answer is 42".into(), provider_options: None,
        },
        SessionEvent::Complete {
            id: "4".into(), turn_id: "t".into(), ts: 3, stop_reason: StopReason::EndTurn,
        },
    ];
    let mut backend = FakeBackend::new("s", script);
    let result = RuntimeRunner::run(&mut backend, &ctx(), &user_input("read it"), None).await;
    assert_eq!(result.status, RunStatus::Completed);
    assert!(result.events.iter().any(|e| matches!(
        &e.content, Some(Content::FunctionCall { name, .. }) if name == "read_file"
    )));
    assert!(result.events.iter().any(|e| matches!(
        &e.content, Some(Content::FunctionResponse { result, .. }) if result == "42"
    )));
}

#[tokio::test]
async fn user_message_is_recorded_in_events() {
    let script = vec![SessionEvent::Complete {
        id: "1".into(), turn_id: "t".into(), ts: 0, stop_reason: StopReason::EndTurn,
    }];
    let mut backend = FakeBackend::new("s", script);
    let result = RuntimeRunner::run(&mut backend, &ctx(), &user_input("hello"), None).await;
    let has_user = result.events.iter().any(|ev| {
        ev.role == Role::User
            && matches!(&ev.content, Some(Content::Text { text, .. }) if text == "hello")
    });
    assert!(has_user, "user turn must be in the event ledger");
}

#[tokio::test]
async fn missing_terminal_synthesized() {
    let script = vec![SessionEvent::TextComplete {
        id: "1".into(), turn_id: "t".into(), ts: 0, message_id: "m".into(),
        text: "partial".into(), provider_options: None,
    }];
    let mut backend = FakeBackend::new("s", script);
    let result = RuntimeRunner::run(&mut backend, &ctx(), &user_input("x"), None).await;
    assert_eq!(result.status, RunStatus::Failed);
}

#[tokio::test]
async fn multi_turn_chains_history_through_messages() {
    let steps = vec![
        vec![
            SessionEvent::TextComplete {
                id: "1".into(), turn_id: "t1".into(), ts: 0, message_id: "m".into(),
                text: "reply one".into(), provider_options: None,
            },
            SessionEvent::Complete {
                id: "2".into(), turn_id: "t1".into(), ts: 1, stop_reason: StopReason::EndTurn,
            },
        ],
        vec![
            SessionEvent::TextComplete {
                id: "3".into(), turn_id: "t2".into(), ts: 0, message_id: "m".into(),
                text: "reply two".into(), provider_options: None,
            },
            SessionEvent::Complete {
                id: "4".into(), turn_id: "t2".into(), ts: 1, stop_reason: StopReason::EndTurn,
            },
        ],
    ];
    let mut backend = FakeBackend::new_stepped("s", steps);

    // Turn 1
    let mut messages = vec![ChatMessage::User { content: "hello".into() }];
    let r1 = RuntimeRunner::run(&mut backend, &ctx(), &BackendSendInput {
        turn_id: "t1".into(), run_id: Some("r1".into()),
        invocation_id: Some("inv1".into()), max_steps: None,
        messages: messages.clone(), system_prompt: None, tools: vec![],
    }, None).await;
    messages = r1.messages.clone();
    assert_eq!(messages.len(), 2);
    assert!(matches!(messages[1], ChatMessage::Assistant { ref content, .. } if content.as_deref() == Some("reply one")));

    // Turn 2
    messages.push(ChatMessage::User { content: "again".into() });
    let r2 = RuntimeRunner::run(&mut backend, &ctx(), &BackendSendInput {
        turn_id: "t2".into(), run_id: Some("r2".into()),
        invocation_id: Some("inv2".into()), max_steps: None,
        messages: messages.clone(), system_prompt: None, tools: vec![],
    }, None).await;
    assert_eq!(r2.messages.len(), 4);
    assert!(matches!(r2.messages[3], ChatMessage::Assistant { ref content, .. } if content.as_deref() == Some("reply two")));
}

#[tokio::test]
async fn runner_output_persists_to_store() {
    let script = vec![
        SessionEvent::TextComplete {
            id: "1".into(), turn_id: "t".into(), ts: 0, message_id: "m".into(),
            text: "hello".into(), provider_options: None,
        },
        SessionEvent::Complete {
            id: "2".into(), turn_id: "t".into(), ts: 1, stop_reason: StopReason::EndTurn,
        },
    ];
    let mut backend = FakeBackend::new("s", script);
    let result = RuntimeRunner::run(&mut backend, &ctx(), &user_input("hi"), None).await;

    use meepo_core::RuntimeEventStore;
    let store = meepo_core::InMemoryRuntimeEventStore::new();
    for ev in &result.events {
        store.append_runtime_event("s", "r", ev.clone(), false).await.unwrap();
    }
    store.ensure_terminal_runtime_event_durable("s", "r", result.terminal.clone()).await.unwrap();
    let read_back = store.read_runtime_events("s", "r").await.unwrap();
    assert!(read_back.iter().any(|e| e.status == Some(Status::Completed)));
}

// ── Rolling compaction threading (断点① regression) ──
//
// Previous bug: SessionManager → run() hardcoded previous_compact_summary =
// None, so every turn re-summarized from scratch and the rolling summary was
// lost. This pins the runner-layer contract: when a previous summary is
// supplied, compaction receives it as one extra leading message.

use std::sync::{Arc, Mutex};
use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use meepo_core::{AgentBackend, BackendKind, BackendResult, BackendStopMode, BackendStopReason};

/// Test backend that records every `compact_history` invocation's input.
struct RecordingBackend {
    session_id: String,
    steps: Mutex<Vec<Vec<SessionEvent>>>,
    calls: Mutex<usize>,
    compact_inputs: Arc<Mutex<Vec<Vec<ChatMessage>>>>,
}

impl RecordingBackend {
    fn new_stepped(
        session_id: &str,
        steps: Vec<Vec<SessionEvent>>,
        compact_inputs: Arc<Mutex<Vec<Vec<ChatMessage>>>>,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            steps: Mutex::new(steps),
            calls: Mutex::new(0),
            compact_inputs,
        }
    }
}

#[async_trait]
impl AgentBackend for RecordingBackend {
    fn kind(&self) -> BackendKind {
        BackendKind::Fake
    }
    fn session_id(&self) -> &str {
        &self.session_id
    }
    fn send<'a>(&'a mut self, _input: &'a BackendSendInput) -> BoxStream<'a, SessionEvent> {
        let step = {
            let mut calls = self.calls.lock().unwrap();
            let idx = *calls;
            *calls += 1;
            self.steps.lock().unwrap().get(idx).cloned().unwrap_or_default()
        };
        futures::stream::iter(step).boxed()
    }
    async fn stop(&mut self, _r: BackendStopReason, _m: Option<BackendStopMode>) -> BackendResult<()> {
        Ok(())
    }
    async fn dispose(&mut self) -> BackendResult<()> {
        Ok(())
    }
    async fn compact_history(&self, messages: &[ChatMessage]) -> BackendResult<String> {
        self.compact_inputs.lock().unwrap().push(messages.to_vec());
        Ok(format!("[recording compact of {} messages]", messages.len()))
    }
}

fn big_input() -> BackendSendInput {
    // 15 messages, total > 16k chars, len > keep_recent(6) → forces compaction.
    let big = "x".repeat(20_000);
    let mut messages = vec![ChatMessage::User { content: big }];
    for i in 0..7 {
        messages.push(ChatMessage::Assistant {
            content: Some(format!("a{i}")),
            tool_calls: vec![],
            thinking: vec![],
        });
        messages.push(ChatMessage::User { content: format!("u{i}") });
    }
    BackendSendInput {
        turn_id: "t".into(),
        run_id: Some("r".into()),
        invocation_id: Some("inv".into()),
        max_steps: None,
        messages,
        system_prompt: None,
        tools: vec![],
    }
}

#[tokio::test]
async fn run_passes_previous_summary_to_compaction() {
    let compact_inputs = Arc::new(Mutex::new(Vec::new()));
    let script = vec![SessionEvent::Complete {
        id: "1".into(),
        turn_id: "t".into(),
        ts: 0,
        stop_reason: StopReason::EndTurn,
    }];
    let mut backend = RecordingBackend::new_stepped("s", vec![script.clone(), script], compact_inputs.clone());

    // Turn 1 — no previous summary: compaction sees the raw prefix.
    let r1 = RuntimeRunner::run(&mut backend, &ctx(), &big_input(), None).await;
    assert!(r1.compact_summary.is_some(), "turn 1 must compact");

    // Turn 2 — previous summary threaded: compaction sees one extra leading msg.
    let r2 = RuntimeRunner::run(&mut backend, &ctx(), &big_input(), Some("PREV")).await;
    assert!(r2.compact_summary.is_some());

    let inputs = compact_inputs.lock().unwrap();
    assert_eq!(inputs.len(), 2, "compaction invoked once per turn");
    assert_eq!(
        inputs[1].len(),
        inputs[0].len() + 1,
        "rolling compaction must receive the previous summary as an extra leading message"
    );
}
