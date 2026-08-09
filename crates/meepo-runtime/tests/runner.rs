//! RuntimeRunner behavior: event collection, terminal invariant, missing
//! terminal synthesis, and the runner → store persistence path.

use meepo_core::{
    BackendSendInput, FakeBackend, InMemoryRuntimeEventStore, RuntimeEventStore, SessionEvent,
    Status, StopReason,
};
use meepo_runtime::{InvocationContext, RunStatus, RuntimeRunner};

fn ctx() -> InvocationContext {
    InvocationContext {
        session_id: "s".into(),
        run_id: "r".into(),
        invocation_id: "inv".into(),
        turn_id: "t".into(),
    }
}

fn input() -> BackendSendInput {
    BackendSendInput {
        turn_id: "t".into(),
        text: "hi".into(),
        run_id: None,
        invocation_id: None,
        max_steps: None,
    }
}

fn delta(id: &str, ts: i64, text: &str) -> SessionEvent {
    SessionEvent::TextDelta {
        id: id.into(),
        turn_id: "t".into(),
        ts,
        message_id: "m".into(),
        start_offset: None,
        text: text.into(),
    }
}

#[tokio::test]
async fn collects_events_and_accepts_terminal() {
    let script = vec![
        delta("1", 0, "he"),
        delta("2", 1, "llo"),
        SessionEvent::Complete {
            id: "3".into(),
            turn_id: "t".into(),
            ts: 2,
            stop_reason: StopReason::EndTurn,
        },
    ];
    let mut backend = FakeBackend::new("s", script);
    let result = RuntimeRunner::run(&mut backend, &ctx(), &input()).await;
    assert_eq!(result.events.len(), 3);
    assert_eq!(result.status, RunStatus::Completed);
    assert_eq!(result.terminal.status, Some(Status::Completed));
    // Deltas map to partial facts.
    assert_eq!(result.events[0].partial, Some(true));
}

#[tokio::test]
async fn synthesizes_missing_terminal_as_failure() {
    let script = vec![delta("1", 0, "hi")];
    let mut backend = FakeBackend::new("s", script);
    let result = RuntimeRunner::run(&mut backend, &ctx(), &input()).await;
    assert_eq!(result.status, RunStatus::Failed);
    assert_eq!(result.terminal.status, Some(Status::Failed));
    // delta + synthesized terminal
    assert_eq!(result.events.len(), 2);
}

#[tokio::test]
async fn maps_error_event_to_failed_terminal() {
    let script = vec![SessionEvent::Error {
        id: "1".into(),
        turn_id: "t".into(),
        ts: 0,
        recoverable: false,
        message: "boom".into(),
        code: None,
        reason: None,
        details: None,
    }];
    let mut backend = FakeBackend::new("s", script);
    let result = RuntimeRunner::run(&mut backend, &ctx(), &input()).await;
    assert_eq!(result.status, RunStatus::Failed);
}

#[tokio::test]
async fn ignores_events_after_terminal() {
    let script = vec![
        SessionEvent::Complete {
            id: "1".into(),
            turn_id: "t".into(),
            ts: 0,
            stop_reason: StopReason::EndTurn,
        },
        delta("2", 1, "late"),
    ];
    let mut backend = FakeBackend::new("s", script);
    let result = RuntimeRunner::run(&mut backend, &ctx(), &input()).await;
    assert_eq!(result.events.len(), 1, "events after terminal are dropped");
}

#[tokio::test]
async fn runner_output_persists_to_store() {
    // The runner does not write the store; the driver (here, the test) does.
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
    let result = RuntimeRunner::run(&mut backend, &ctx(), &input()).await;

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
    assert_eq!(read_back.len(), 2);
    assert!(read_back
        .iter()
        .any(|e| e.status == Some(Status::Completed)));
}
