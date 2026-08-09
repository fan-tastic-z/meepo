//! RuntimeEventStore in-memory behavior: append order, terminal idempotency,
//! cross-run read order, durability reporting.

use meepo_core::{
    Author, Content, Durability, InMemoryRuntimeEventStore, Role, RuntimeEvent,
    RuntimeEventStore, Status,
};

fn text_event(
    session: &str,
    run: &str,
    id: &str,
    status: Option<Status>,
    text: &str,
) -> RuntimeEvent {
    RuntimeEvent {
        session_id: session.into(),
        invocation_id: "inv".into(),
        run_id: run.into(),
        turn_id: "t".into(),
        branch: None,
        id: id.into(),
        ts: 0,
        role: Role::Model,
        author: Author::Agent,
        origin: None,
        model_visibility: None,
        status,
        content: Some(Content::Text {
            text: text.into(),
            provider_options: None,
            steering: None,
        }),
        actions: None,
        refs: None,
        partial: None,
    }
}

#[tokio::test]
async fn append_and_read_back_in_commit_order() {
    let store = InMemoryRuntimeEventStore::new();
    store
        .append_runtime_event("s", "r", text_event("s", "r", "e1", None, "a"), false)
        .await
        .unwrap();
    store
        .append_runtime_event("s", "r", text_event("s", "r", "e2", None, "b"), false)
        .await
        .unwrap();
    let got = store.read_runtime_events("s", "r").await.unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].id, "e1");
    assert_eq!(got[1].id, "e2");
}

#[tokio::test]
async fn terminal_event_is_idempotent() {
    let store = InMemoryRuntimeEventStore::new();
    store
        .ensure_terminal_runtime_event_durable(
            "s",
            "r",
            text_event("s", "r", "e1", Some(Status::Completed), "done"),
        )
        .await
        .unwrap();
    // A second terminal for the same run must be a no-op (terminal invariant).
    store
        .ensure_terminal_runtime_event_durable(
            "s",
            "r",
            text_event("s", "r", "e2", Some(Status::Completed), "done2"),
        )
        .await
        .unwrap();
    let got = store.read_runtime_events("s", "r").await.unwrap();
    assert_eq!(got.len(), 1, "second terminal must be dropped");
    assert_eq!(got[0].id, "e1");
}

#[tokio::test]
async fn session_read_spans_runs_ordered_by_event_id() {
    let store = InMemoryRuntimeEventStore::new();
    // Insert out of id order across two runs.
    store
        .append_runtime_event("s", "r1", text_event("s", "r1", "e2", None, "b"), false)
        .await
        .unwrap();
    store
        .append_runtime_event("s", "r2", text_event("s", "r2", "e1", None, "a"), false)
        .await
        .unwrap();
    let got = store.read_session_runtime_events("s").await.unwrap();
    let ids: Vec<&str> = got.iter().map(|e| e.id.as_str()).collect();
    assert_eq!(ids, vec!["e1", "e2"]);
}

#[tokio::test]
async fn reports_configured_durability() {
    let store = InMemoryRuntimeEventStore::with_durability(Durability::Canonical);
    assert_eq!(store.durability(), Durability::Canonical);
}

#[tokio::test]
async fn missing_run_reads_as_empty() {
    let store = InMemoryRuntimeEventStore::new();
    let got = store.read_runtime_events("nope", "nope").await.unwrap();
    assert!(got.is_empty());
}
