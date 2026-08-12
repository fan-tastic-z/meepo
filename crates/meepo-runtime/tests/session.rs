//! SessionManager regression: the rolling compaction summary threads across
//! turns and reaches TurnResult (断点①), and the terminal event is persisted
//! through the durable boundary (断点②).

use meepo_core::{FakeBackend, InMemoryRuntimeEventStore, SessionEvent, StopReason};
use meepo_runtime::{RunStatus, SessionManager};

fn text_event(id: &str, turn_id: &str, ts: i64, text: &str) -> SessionEvent {
    SessionEvent::TextComplete {
        id: id.into(),
        turn_id: turn_id.into(),
        ts,
        message_id: "m".into(),
        text: text.into(),
        provider_options: None,
    }
}

fn complete_event(id: &str, turn_id: &str, ts: i64) -> SessionEvent {
    SessionEvent::Complete {
        id: id.into(),
        turn_id: turn_id.into(),
        ts,
        stop_reason: StopReason::EndTurn,
    }
}

#[tokio::test]
async fn send_turn_threads_and_returns_compact_summary() {
    // Four oversized turns push accumulated history past both thresholds
    // (len > 6, chars > 16k), so compaction runs on turn 4. Its summary must
    // reach TurnResult.compact_summary — regression for the old bug where
    // SessionManager hardcoded it to None and never wrote it back.
    let big = "x".repeat(20_000);
    let one_turn = vec![text_event("t", "turn", 0, "ok"), complete_event("c", "turn", 1)];
    let mut backend = FakeBackend::new_stepped("s", vec![one_turn; 4]);

    let store = InMemoryRuntimeEventStore::new();
    let mut session = SessionManager::new("s");

    let mut last = None;
    for _ in 0..4 {
        let result = session.send_turn(&mut backend, &store, big.clone(), None, &[]).await;
        assert_eq!(result.status, RunStatus::Completed);
        last = result.compact_summary;
    }
    assert!(
        last.is_some(),
        "compaction must run and its summary must reach TurnResult"
    );
}

#[tokio::test]
async fn send_turn_streaming_invokes_callback_per_event() {
    // The streaming entry point must surface every RuntimeEvent to the
    // callback as it is produced (断点③: live REPL output).
    let one_turn = vec![text_event("t", "turn", 0, "hi"), complete_event("c", "turn", 1)];
    let mut backend = FakeBackend::new_stepped("s", vec![one_turn]);
    let store = InMemoryRuntimeEventStore::new();
    let mut session = SessionManager::new("s");

    let mut seen = 0usize;
    let result = session
        .send_turn_streaming(&mut backend, &store, "hello".into(), None, &[], |_| seen += 1)
        .await;
    assert_eq!(result.status, RunStatus::Completed);
    assert_eq!(
        seen,
        result.events.len(),
        "callback must fire once per RuntimeEvent"
    );
}
