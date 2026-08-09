//! SqliteStore persistence: cross-connection durability, terminal idempotency,
//! session-spanning reads.

use meepo_core::{Author, Content, Durability, Role, RuntimeEvent, RuntimeEventStore, Status};
use meepo_storage::SqliteStore;

fn text_event(id: &str, status: Option<Status>, text: &str) -> RuntimeEvent {
    RuntimeEvent {
        session_id: "s".into(),
        invocation_id: "inv".into(),
        run_id: "r".into(),
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
async fn in_memory_append_and_read_in_order() {
    let store = SqliteStore::in_memory().unwrap();
    store
        .append_runtime_event("s", "r", text_event("e1", None, "a"), false)
        .await
        .unwrap();
    store
        .append_runtime_event("s", "r", text_event("e2", None, "b"), false)
        .await
        .unwrap();
    let got = store.read_runtime_events("s", "r").await.unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].id, "e1");
    assert_eq!(got[1].id, "e2");
}

#[tokio::test]
async fn persists_across_connections() {
    let path = std::env::temp_dir().join(format!("meepo-storage-{}.sqlite", std::process::id()));
    let _ = std::fs::remove_file(&path);
    // First connection writes.
    {
        let store = SqliteStore::open(&path).unwrap();
        store
            .append_runtime_event("s", "r", text_event("e1", None, "first"), false)
            .await
            .unwrap();
    }
    // A fresh connection reads it back — true persistence.
    {
        let store = SqliteStore::open(&path).unwrap();
        let got = store.read_runtime_events("s", "r").await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].id, "e1");
        assert_eq!(
            got[0].content,
            Some(Content::Text {
                text: "first".into(),
                provider_options: None,
                steering: None,
            })
        );
    }
    std::fs::remove_file(&path).ok();
}

#[tokio::test]
async fn terminal_is_idempotent_and_reports_canonical() {
    let store = SqliteStore::in_memory().unwrap();
    assert_eq!(store.durability(), Durability::Canonical);
    store
        .ensure_terminal_runtime_event_durable(
            "s",
            "r",
            text_event("e1", Some(Status::Completed), "done"),
        )
        .await
        .unwrap();
    store
        .ensure_terminal_runtime_event_durable(
            "s",
            "r",
            text_event("e2", Some(Status::Completed), "done2"),
        )
        .await
        .unwrap();
    let got = store.read_runtime_events("s", "r").await.unwrap();
    assert_eq!(got.len(), 1, "second terminal is a no-op");
    assert_eq!(got[0].status, Some(Status::Completed));
}

#[tokio::test]
async fn session_read_spans_runs_ordered_by_seq() {
    let store = SqliteStore::in_memory().unwrap();
    // Insert r1 first then r2; session read must interleave by seq.
    store
        .append_runtime_event("s", "r1", text_event("e1", None, "a"), false)
        .await
        .unwrap();
    store
        .append_runtime_event("s", "r2", text_event("e2", None, "b"), false)
        .await
        .unwrap();
    let got = store.read_session_runtime_events("s").await.unwrap();
    let ids: Vec<&str> = got.iter().map(|e| e.id.as_str()).collect();
    assert_eq!(ids, vec!["e1", "e2"]);
}
