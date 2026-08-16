//! Phase 10: resume — after a turn completes, `turn.resume.query` reports the
//! durable high-water; `turn.resume.start` validates it, records a
//! continuation claim, and starts a continuation turn with the source content.

use std::sync::Arc;
use std::time::Duration;

use meepo_core::{FakeBackend, SessionEvent, StopReason};
use meepo_storage::SqliteStore;
use serde_json::json;

use meepo_host::server::{BackendFactory, Composition, TurnCoordinator};
use meepo_host::{handlers, transport, Dispatcher, HostClient, HostKernel, SessionContinuityCoordinator};

fn factory() -> BackendFactory {
    Arc::new(|s| {
        Box::new(FakeBackend::new(
            s,
            vec![
                SessionEvent::TextComplete {
                    id: "1".into(),
                    turn_id: "t".into(),
                    ts: 0,
                    message_id: "m".into(),
                    text: "did the work".into(),
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
async fn resume_query_and_continuation() {
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
    let kernel = HostKernel::new("epoch-resume", dispatcher, continuity);
    let serve = tokio::spawn(async move {
        kernel.serve(listener).await;
    });

    let (mut client, _) = HostClient::connect(&sock).await.expect("connect");
    client
        .request("session.create", json!({"sessionId": "s1"}))
        .await
        .expect("session.create");
    let started = client
        .request("turn.start", json!({"sessionId": "s1", "content": "do the work"}))
        .await
        .expect("turn.start");
    assert_eq!(started["runId"], json!("run-1"));

    // Wait for the run to be durably committed (high-water > 0), then query.
    let plan = loop {
        let p = client
            .request("turn.resume.query", json!({"sessionId": "s1"}))
            .await
            .expect("resume.query");
        if p["disposition"] == json!("ready") {
            break p;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(plan["sourceRunId"], json!("run-1"));
    let high_water = plan["sourceRuntimeEventHighWater"].as_i64().expect("high water");
    assert!(high_water > 0, "the source run committed events");

    // A drift-check: a stale high-water parks.
    let drifted = client
        .request(
            "turn.resume.start",
            json!({"sessionId": "s1", "sourceRuntimeEventHighWater": high_water + 999}),
        )
        .await
        .expect("drifted start resolves");
    assert_eq!(drifted["kind"], json!("parked"));
    assert_eq!(drifted["reason"], json!("source_run_changed"));

    // The correct high-water starts a continuation. Turn ids derive from the
    // message count: turn-1 completed with [user, assistant] (len 2), so the
    // continuation is turn-3/run-3.
    let resumed = client
        .request(
            "turn.resume.start",
            json!({"sessionId": "s1", "sourceRuntimeEventHighWater": high_water}),
        )
        .await
        .expect("resume.start");
    assert_eq!(resumed["kind"], json!("started"), "got {resumed}");
    assert_eq!(resumed["turnId"], json!("turn-3"));
    assert_eq!(resumed["runId"], json!("run-3"));
    assert_eq!(resumed["sourceRunId"], json!("run-1"));

    drop(client);
    serve.abort();
}

#[tokio::test]
async fn resume_query_parks_without_a_source_run() {
    let store = Arc::new(SqliteStore::in_memory().unwrap());
    let composition = Arc::new(Composition::new(store.clone(), factory(), None));
    let coordinator = TurnCoordinator::new(composition, Arc::new(SessionContinuityCoordinator::new()));
    let plan = coordinator.resume_query("never-started").await.expect("query");
    match plan {
        meepo_host::server::ResumePlan::Parked { reason } => {
            assert_eq!(reason, "resume_candidate_missing");
        }
        other => panic!("expected parked, got {other:?}"),
    }
}
