//! RuntimeRunner with the tool step-loop: tool execution, history threading,
//! missing-terminal synthesis, step limit, and the runner→store path.

use meepo_core::{
    AssistantToolCall, BackendSendInput, ChatMessage, Content, FakeBackend,
    InMemoryRuntimeEventStore, Role, RuntimeEventStore, SessionEvent, Status, StopReason,
};
use meepo_runtime::{InvocationContext, RunStatus, RuntimeRunner};
use meepo_tools::{ReadFile, ToolRegistry};
use serde_json::json;

fn ctx() -> InvocationContext {
    InvocationContext {
        session_id: "s".into(),
        run_id: "r".into(),
        invocation_id: "inv".into(),
        turn_id: "t".into(),
    }
}

fn user_input(prompt: &str) -> BackendSendInput {
    BackendSendInput {
        turn_id: "t".into(),
        run_id: Some("r".into()),
        invocation_id: Some("inv".into()),
        max_steps: None,
        messages: vec![ChatMessage::User { content: prompt.into() }],
        system_prompt: None,
        tools: vec![],
    }
}

fn tools() -> ToolRegistry {
    let mut t = ToolRegistry::new();
    t.register(Box::new(ReadFile));
    t
}

#[tokio::test]
async fn single_text_turn_completes() {
    let script = vec![
        SessionEvent::TextComplete {
            id: "1".into(),
            turn_id: "t".into(),
            ts: 0,
            message_id: "m".into(),
            text: "hi".into(),
            provider_options: None,
        },
        SessionEvent::Complete {
            id: "2".into(),
            turn_id: "t".into(),
            ts: 1,
            stop_reason: StopReason::EndTurn,
        },
    ];
    let mut backend = FakeBackend::new("s", script);
    let result = RuntimeRunner::run(&mut backend, &ctx(), &user_input("hi"), &tools()).await;
    assert_eq!(result.status, RunStatus::Completed);
    assert!(result.events.iter().any(|e| matches!(
        e.content,
        Some(Content::Text { .. })
    )));
}

#[tokio::test]
async fn tool_loop_executes_and_continues() {
    // Step 0: model calls read_file. Step 1: model gives a final answer.
    let path = std::env::temp_dir().join(format!("meepo-runner-{}.txt", std::process::id()));
    std::fs::write(&path, "42").unwrap();

    let steps = vec![
        vec![SessionEvent::ToolCall {
            id: "1".into(),
            turn_id: "t".into(),
            ts: 0,
            tool_call_id: "call_1".into(),
            tool_name: "read_file".into(),
            args: json!({ "path": path }),
        }],
        vec![
            SessionEvent::TextComplete {
                id: "2".into(),
                turn_id: "t".into(),
                ts: 0,
                message_id: "m".into(),
                text: "the answer is 42".into(),
                provider_options: None,
            },
            SessionEvent::Complete {
                id: "3".into(),
                turn_id: "t".into(),
                ts: 1,
                stop_reason: StopReason::EndTurn,
            },
        ],
    ];
    let mut backend = FakeBackend::new_stepped("s", steps);
    let result = RuntimeRunner::run(&mut backend, &ctx(), &user_input("read it"), &tools()).await;

    assert_eq!(result.status, RunStatus::Completed);
    // The ledger contains the tool call, the tool result, the final text, and the terminal.
    let has_call = result.events.iter().any(|e| matches!(
        &e.content,
        Some(Content::FunctionCall { name, .. }) if name == "read_file"
    ));
    let has_result = result.events.iter().any(|e| matches!(
        &e.content,
        Some(Content::FunctionResponse { result, .. }) if result == "42"
    ));
    assert!(has_call, "tool call mapped to RuntimeEvent");
    assert!(has_result, "tool result mapped to RuntimeEvent with content 42");
    // Backend was called twice (step 0 + step 1).
}

#[tokio::test]
async fn missing_terminal_synthesized_when_no_tool_no_terminal() {
    let script = vec![SessionEvent::TextComplete {
        id: "1".into(),
        turn_id: "t".into(),
        ts: 0,
        message_id: "m".into(),
        text: "partial".into(),
        provider_options: None,
    }];
    let mut backend = FakeBackend::new("s", script);
    let result = RuntimeRunner::run(&mut backend, &ctx(), &user_input("x"), &tools()).await;
    assert_eq!(result.status, RunStatus::Failed);
}

#[tokio::test]
async fn step_limit_when_tool_loops_forever() {
    // Every step emits a tool call -> never terminates -> hits the step budget.
    let forever = vec![SessionEvent::ToolCall {
        id: "1".into(),
        turn_id: "t".into(),
        ts: 0,
        tool_call_id: "call_1".into(),
        tool_name: "read_file".into(),
        args: json!({ "path": "/dev/null" }),
    }];
    let steps: Vec<Vec<SessionEvent>> = (0..100).map(|_| forever.clone()).collect();
    let mut backend = FakeBackend::new_stepped("s", steps);

    let mut input = user_input("loop");
    input.max_steps = Some(3);
    let result = RuntimeRunner::run(&mut backend, &ctx(), &input, &tools()).await;
    // Step limit yields a Completed (stop_reason step_limit) terminal.
    assert_eq!(result.status, RunStatus::Completed);
    assert_eq!(result.terminal.status, Some(Status::Completed));
}

#[tokio::test]
async fn multi_turn_chains_history_through_messages() {
    // Two turns on the same stepped backend. Turn 1 -> reply1; turn 2 should
    // see turn 1's user+assistant in its input messages and produce reply2.
    let steps = vec![
        vec![
            SessionEvent::TextComplete {
                id: "1".into(),
                turn_id: "t1".into(),
                ts: 0,
                message_id: "m".into(),
                text: "reply one".into(),
                provider_options: None,
            },
            SessionEvent::Complete {
                id: "2".into(),
                turn_id: "t1".into(),
                ts: 1,
                stop_reason: StopReason::EndTurn,
            },
        ],
        vec![
            SessionEvent::TextComplete {
                id: "3".into(),
                turn_id: "t2".into(),
                ts: 0,
                message_id: "m".into(),
                text: "reply two".into(),
                provider_options: None,
            },
            SessionEvent::Complete {
                id: "4".into(),
                turn_id: "t2".into(),
                ts: 1,
                stop_reason: StopReason::EndTurn,
            },
        ],
    ];
    let mut backend = FakeBackend::new_stepped("s", steps);

    // Turn 1
    let mut messages = vec![ChatMessage::User { content: "hello".into() }];
    let r1 = RuntimeRunner::run(
        &mut backend,
        &InvocationContext {
            session_id: "s".into(),
            run_id: "r1".into(),
            invocation_id: "inv1".into(),
            turn_id: "t1".into(),
        },
        &BackendSendInput {
            turn_id: "t1".into(),
            run_id: Some("r1".into()),
            invocation_id: Some("inv1".into()),
            max_steps: None,
            messages: messages.clone(),
            system_prompt: None,
            tools: vec![],
        },
        &tools(),
    )
    .await;
    // History after turn 1: user + assistant reply.
    messages = r1.messages.clone();
    assert_eq!(messages.len(), 2);
    assert!(matches!(messages[1], ChatMessage::Assistant { ref content, .. } if content.as_deref() == Some("reply one")));

    // Turn 2 chains the history.
    messages.push(ChatMessage::User { content: "again".into() });
    let r2 = RuntimeRunner::run(
        &mut backend,
        &InvocationContext {
            session_id: "s".into(),
            run_id: "r2".into(),
            invocation_id: "inv2".into(),
            turn_id: "t2".into(),
        },
        &BackendSendInput {
            turn_id: "t2".into(),
            run_id: Some("r2".into()),
            invocation_id: Some("inv2".into()),
            max_steps: None,
            messages: messages.clone(),
            system_prompt: None,
            tools: vec![],
        },
        &tools(),
    )
    .await;
    // After turn 2: user1, assistant1, user2, assistant2.
    assert_eq!(r2.messages.len(), 4);
    assert!(matches!(r2.messages[3], ChatMessage::Assistant { ref content, .. } if content.as_deref() == Some("reply two")));
}

#[tokio::test]
async fn user_message_is_recorded_in_events() {
    // The ledger must contain the user's turn, or a resumed session loses it.
    let script = vec![SessionEvent::Complete {
        id: "1".into(),
        turn_id: "t".into(),
        ts: 0,
        stop_reason: StopReason::EndTurn,
    }];
    let mut backend = FakeBackend::new("s", script);
    let result = RuntimeRunner::run(&mut backend, &ctx(), &user_input("hello"), &tools()).await;
    let has_user = result.events.iter().any(|ev| {
        ev.role == Role::User
            && matches!(&ev.content, Some(Content::Text { text, .. }) if text == "hello")
    });
    assert!(has_user, "user turn must be in the event ledger");
}

#[tokio::test]
async fn runner_output_persists_to_store() {
    let script = vec![
        SessionEvent::TextComplete {
            id: "1".into(),
            turn_id: "t".into(),
            ts: 0,
            message_id: "m".into(),
            text: "hello".into(),
            provider_options: None,
        },
        SessionEvent::Complete {
            id: "2".into(),
            turn_id: "t".into(),
            ts: 1,
            stop_reason: StopReason::EndTurn,
        },
    ];
    let mut backend = FakeBackend::new("s", script);
    let result = RuntimeRunner::run(&mut backend, &ctx(), &user_input("hi"), &tools()).await;

    let store = InMemoryRuntimeEventStore::new();
    for ev in &result.events {
        store
            .append_runtime_event("s", "r", ev.clone(), false)
            .await
            .unwrap();
    }
    store
        .ensure_terminal_runtime_event_durable("s", "r", result.terminal.clone())
        .await
        .unwrap();
    let read_back = store.read_runtime_events("s", "r").await.unwrap();
    assert!(read_back.iter().any(|e| e.status == Some(Status::Completed)));
    // silence unused import warning if AssistantToolCall ever drops out
    let _ = AssistantToolCall { id: String::new(), name: String::new(), args: json!(null) };
}
