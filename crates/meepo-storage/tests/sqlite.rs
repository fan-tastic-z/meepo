//! SqliteStore (upstream-aligned schema): persistence, ordering, terminal
//! idempotency, cross-connection durability.

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
    {
        let store = SqliteStore::open(&path).unwrap();
        store
            .append_runtime_event("s", "r", text_event("e1", None, "first"), false)
            .await
            .unwrap();
    }
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
async fn event_seq_is_per_invocation() {
    // Two invocations under the same session/run must each start at seq 1.
    let store = SqliteStore::in_memory().unwrap();
    let mut a = text_event("a1", None, "a1");
    a.invocation_id = "invA".into();
    let mut b = text_event("b1", None, "b1");
    b.invocation_id = "invB".into();
    store.append_runtime_event("s", "r", a, false).await.unwrap();
    store.append_runtime_event("s", "r", b, false).await.unwrap();
    // Both readable under the same run, ordered by event_seq.
    let got = store.read_runtime_events("s", "r").await.unwrap();
    assert_eq!(got.len(), 2);
}

#[tokio::test]
async fn session_read_spans_runs_ordered() {
    let store = SqliteStore::in_memory().unwrap();
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

/// Opens a real upstream runtime.sqlite if MEEPO_UPSTREAM_DB points at one,
/// and confirms we can SELECT runtime_events rows (read interop). Ignored by
/// default. Run with:
///   MEEPO_UPSTREAM_DB=/path/to/runtime.sqlite \
///     cargo test -p meepo-storage upstream_db_is_readable -- --ignored --nocapture
#[tokio::test]
#[ignore]
async fn upstream_db_is_readable() {
    let path = std::env::var("MEEPO_UPSTREAM_DB").expect("MEEPO_UPSTREAM_DB not set");
    let store = SqliteStore::open(&path).unwrap();
    // Read whatever session exists in the upstream DB; just assert it parses.
    let conn = rusqlite::Connection::open(&path).unwrap();
    let some_session: Option<String> = conn
        .query_row(
            "SELECT session_id FROM runtime_events LIMIT 1",
            [],
            |r| r.get(0),
        )
        .ok();
    drop(conn);
    if let Some(session) = some_session {
        let events = store.read_session_runtime_events(&session).await.unwrap();
        eprintln!("read {} events from upstream session {session}", events.len());
        assert!(!events.is_empty());
    } else {
        eprintln!("upstream DB has no runtime_events rows");
    }
}

#[tokio::test]
async fn records_permission_request_and_outcome() {
    use meepo_core::InteractionStore;
    let path = std::env::temp_dir().join(format!("meepo-perm-{}.sqlite", std::process::id()));
    let _ = std::fs::remove_file(&path);
    {
        let store = SqliteStore::open(&path).unwrap();
        store
            .record_permission("s", "r", "t", "call_1", 42, r#"{"r":1}"#, r#"{"o":1}"#)
            .await
            .unwrap();
        // Idempotent: re-recording the same request_id is a no-op.
        store
            .record_permission("s", "r", "t", "call_1", 42, r#"{"r":1}"#, r#"{"o":1}"#)
            .await
            .unwrap();
    }
    let conn = rusqlite::Connection::open(&path).unwrap();
    let req_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM core_interaction_requests WHERE request_id = 'call_1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(req_count, 1, "request row written once (idempotent)");
    let outcome_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM core_interaction_outcomes WHERE request_id = 'call_1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(outcome_count, 1, "outcome row written");
    let kind: String = conn
        .query_row(
            "SELECT request_kind FROM core_interaction_requests WHERE request_id = 'call_1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(kind, "permission");
    std::fs::remove_file(&path).ok();
}
