//! Phase 7: chat through the host — subscribe, start a turn, and receive the
//! streamed text delta + the terminal projection on the subscription.

use std::sync::Arc;
use std::time::Duration;

use meepo_core::{FakeBackend, SessionEvent, StopReason};
use meepo_storage::SqliteStore;

use meepo_host::server::{BackendFactory, Composition, TurnCoordinator};
use meepo_host::{handlers, transport, Dispatcher, HostClient, HostKernel, SessionContinuityCoordinator};

fn fake_factory() -> BackendFactory {
    Arc::new(|_session: &str| {
        Box::new(FakeBackend::new(
            "s",
            vec![
                SessionEvent::TextComplete {
                    id: "1".into(),
                    turn_id: "t".into(),
                    ts: 0,
                    message_id: "m".into(),
                    text: "hello from the host".into(),
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

fn inspect(frame: &serde_json::Value, saw_delta: &mut bool, saw_projection: &mut bool) {
    match frame["kind"].as_str() {
        Some("subscription.session_delta") => {
            *saw_delta = true;
            assert_eq!(frame["text"], serde_json::json!("hello from the host"));
            assert_eq!(frame["sequence"], serde_json::json!(1));
        }
        Some("subscription.session_projection") => {
            *saw_projection = true;
            assert_eq!(frame["snapshot"]["rootTurn"]["status"], serde_json::json!("completed"));
        }
        _ => {}
    }
}

#[tokio::test]
async fn turn_start_streams_to_subscribed_client() {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("h.sock");
    let listener = transport::bind(&sock).unwrap();

    let store = Arc::new(SqliteStore::in_memory().unwrap());
    let composition = Arc::new(Composition::new(store.clone(), fake_factory(), None));
    let continuity = Arc::new(SessionContinuityCoordinator::new());
    let turns = Arc::new(TurnCoordinator::new(composition, continuity.clone()));

    let mut dispatcher = Dispatcher::new();
    handlers::host::register(&mut dispatcher);
    handlers::session::register(&mut dispatcher, store);
    handlers::turn::register(&mut dispatcher, turns);
    let kernel = HostKernel::new("epoch-turn", dispatcher, continuity);
    let serve = tokio::spawn(async move {
        kernel.serve(listener).await;
    });

    let (mut client, _) = HostClient::connect(&sock).await.expect("connect + handshake");

    // Sessions must exist in the catalog before a turn can start.
    let created = client
        .request("session.create", serde_json::json!({"sessionId": "s1", "name": "test"}))
        .await
        .expect("session.create");
    assert_eq!(created["revision"], serde_json::json!(1));

    // Subscribe before starting the turn.
    let opened = client
        .request("subscription.open", serde_json::json!({"sessionId": "s1"}))
        .await
        .expect("subscription.open");
    assert_eq!(opened["nextSequence"], serde_json::json!(1));
    assert_eq!(opened["snapshot"]["sessionId"], serde_json::json!("s1"));

    // Start the turn; the response carries the admitted identity.
    let started = client
        .request("turn.start", serde_json::json!({"sessionId": "s1", "content": "hi"}))
        .await
        .expect("turn.start");
    assert_eq!(started["turnId"], serde_json::json!("turn-1"));
    assert_eq!(started["status"], serde_json::json!("running"));

    // Collect streamed frames (order vs the response is racy: check the
    // skipped-frames buffer first, then read live).
    let mut saw_delta = false;
    let mut saw_projection = false;
    loop {
        for frame in client.take_streamed() {
            inspect(&frame, &mut saw_delta, &mut saw_projection);
        }
        if saw_delta && saw_projection {
            break;
        }
        match tokio::time::timeout(Duration::from_secs(2), client.next_streamed()).await {
            Ok(Some(frame)) => inspect(&frame, &mut saw_delta, &mut saw_projection),
            _ => break,
        }
    }
    assert!(saw_delta, "client must receive the text delta");
    assert!(saw_projection, "client must receive the terminal projection");

    drop(client);
    serve.abort();
}
