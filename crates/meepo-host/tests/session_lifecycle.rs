//! Phase 9a: session catalog lifecycle — create, list, optimistic-concurrency
//! updates, archive guard on turns, read marker, removal.

use std::sync::Arc;

use meepo_core::{FakeBackend, SessionEvent, StopReason};
use meepo_storage::SqliteStore;
use serde_json::json;

use meepo_host::server::{BackendFactory, Composition, TurnCoordinator};
use meepo_host::{handlers, transport, Dispatcher, HostClient, HostKernel, SessionContinuityCoordinator};

fn factory() -> BackendFactory {
    Arc::new(|_s| {
        Box::new(FakeBackend::new(
            "s",
            vec![
                SessionEvent::TextComplete {
                    id: "1".into(),
                    turn_id: "t".into(),
                    ts: 0,
                    message_id: "m".into(),
                    text: "ok".into(),
                    provider_options: None,
                },
                SessionEvent::Complete {
                    id: "2".into(),
                    turn_id: "t".into(),
                    ts: 1,
                    stop_reason: StopReason::EndTurn,
                },
            ],
        ))
    })
}

#[tokio::test]
async fn session_lifecycle_and_archived_guard() {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("h.sock");
    let listener = transport::bind(&sock).unwrap();

    let store = Arc::new(SqliteStore::in_memory().unwrap());
    let composition = Arc::new(Composition::new(store.clone(), factory(), None));
    let continuity = Arc::new(SessionContinuityCoordinator::new());
    let turns = Arc::new(TurnCoordinator::new(composition, continuity.clone()));

    let mut dispatcher = Dispatcher::new();
    handlers::host::register(&mut dispatcher);
    handlers::session::register(&mut dispatcher, store);
    handlers::turn::register(&mut dispatcher, turns);
    let kernel = HostKernel::new("epoch-sess", dispatcher, continuity);
    let serve = tokio::spawn(async move {
        kernel.serve(listener).await;
    });

    let (mut client, _) = HostClient::connect(&sock).await.expect("connect");

    // create
    let rec = client
        .request("session.create", json!({"sessionId": "s1", "name": "main", "labels": ["a"]}))
        .await
        .expect("create");
    assert_eq!(rec["revision"], json!(1));
    assert_eq!(rec["name"], json!("main"));

    // catalog query
    let page = client.request("session.catalog.query", json!({})).await.expect("catalog");
    assert_eq!(page["kind"], json!("page"));
    assert_eq!(page["items"].as_array().unwrap().len(), 1);

    // metadata update at the right revision commits (revision bumps to 2)
    let up = client
        .request(
            "session.metadata.update",
            json!({"sessionId": "s1", "expectedRevision": 1, "patch": {"name": "renamed"}}),
        )
        .await
        .expect("metadata update");
    assert_eq!(up["kind"], json!("committed"));
    assert_eq!(up["session"]["name"], json!("renamed"));
    assert_eq!(up["session"]["revision"], json!(2));

    // a stale revision reports revision_conflict (result union, not an error)
    let stale = client
        .request(
            "session.metadata.update",
            json!({"sessionId": "s1", "expectedRevision": 1, "patch": {"name": "x"}}),
        )
        .await
        .expect("stale update resolves");
    assert_eq!(stale["kind"], json!("revision_conflict"));

    // archive guards turns
    client
        .request("session.lifecycle.set", json!({"sessionId": "s1", "lifecycle": "archived"}))
        .await
        .expect("archive");
    let archived_turn = client.request("turn.start", json!({"sessionId": "s1", "content": "hi"})).await;
    assert!(archived_turn.is_err(), "turn.start on an archived session must fail");

    // unarchive; a turn runs
    client
        .request("session.lifecycle.set", json!({"sessionId": "s1", "lifecycle": "active"}))
        .await
        .expect("unarchive");
    let started = client
        .request("turn.start", json!({"sessionId": "s1", "content": "hi"}))
        .await
        .expect("turn.start after unarchive");
    assert_eq!(started["turnId"], json!("turn-1"));

    // read marker
    let rm = client
        .request("session.read_marker.set", json!({"sessionId": "s1", "readThroughMessageId": "m9"}))
        .await
        .expect("read marker");
    assert_eq!(rm["kind"], json!("committed"));
    assert_eq!(rm["session"]["readMarker"], json!("m9"));

    // remove; later turns report NotFound
    let removed = client
        .request("session.remove", json!({"sessionId": "s1"}))
        .await
        .expect("remove");
    assert_eq!(removed["removed"], json!(true));
    let gone = client.request("turn.start", json!({"sessionId": "s1", "content": "x"})).await;
    assert!(gone.is_err(), "turn.start on a removed session must fail");

    drop(client);
    serve.abort();
}
