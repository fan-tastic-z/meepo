//! Phase 9b: asynchronous permission over the wire — a prompt surfaces as a
//! pending interaction, the client answers, and the blocked turn resumes.

use std::sync::Arc;
use std::time::Duration;

use meepo_core::{PermissionDecision, PermissionReason, PermissionRequest, ToolCategory};
use meepo_storage::SqliteStore;
use serde_json::json;

use meepo_host::server::{InteractionContext, InteractionHub, TurnCoordinator, Composition};
use meepo_host::{handlers, transport, Dispatcher, HostClient, HostKernel, SessionContinuityCoordinator};

#[tokio::test]
async fn permission_prompt_round_trips_over_the_wire() {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("h.sock");
    let listener = transport::bind(&sock).unwrap();

    let store = Arc::new(SqliteStore::in_memory().unwrap());
    // A turn coordinator is not needed for this test, but the composition is.
    let factory: meepo_host::server::BackendFactory = Arc::new(|s| {
        Box::new(meepo_core::FakeBackend::new(s, vec![]))
    });
    let composition = Arc::new(Composition::new(store.clone(), factory, None));
    let continuity = Arc::new(SessionContinuityCoordinator::new());
    let _turns = TurnCoordinator::new(composition, continuity.clone());
    let hub = Arc::new(InteractionHub::new(store.clone(), continuity.clone()));

    let mut dispatcher = Dispatcher::new();
    handlers::host::register(&mut dispatcher);
    handlers::session::register(&mut dispatcher, store);
    handlers::interaction::register(&mut dispatcher, hub.clone());
    let kernel = HostKernel::new("epoch-int", dispatcher, continuity);
    let serve = tokio::spawn(async move {
        kernel.serve(listener).await;
    });

    let (mut client, _) = HostClient::connect(&sock).await.expect("connect");
    client
        .request("session.create", json!({"sessionId": "s1"}))
        .await
        .expect("session.create");
    client
        .request("subscription.open", json!({"sessionId": "s1"}))
        .await
        .expect("subscribe");

    // A backend gate asks through the hub (blocked turn).
    let ask_hub = hub.clone();
    let ask_handle = tokio::spawn(async move {
        let request = PermissionRequest {
            tool_call_id: "tc-1".into(),
            tool_name: "bash".into(),
            category: ToolCategory::ShellUnsafe,
            reason: PermissionReason::ShellDangerous,
            summary: "cargo test".into(),
            remember_for_turn_allowed: true,
        };
        ask_hub
            .ask(
                &InteractionContext {
                    session_id: "s1".into(),
                    run_id: "run-1".into(),
                    turn_id: "turn-1".into(),
                },
                "epoch-int",
                &request,
            )
            .await
    });

    // The client sees the pending interaction.
    let pending = loop {
        let q = client
            .request("interaction.query", json!({"sessionId": "s1"}))
            .await
            .expect("interaction.query");
        let p = q["pending"].as_array().cloned().unwrap_or_default();
        if !p.is_empty() {
            break p;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(pending[0]["toolName"], json!("bash"));
    assert_eq!(pending[0]["summary"], json!("cargo test"));
    let interaction_id = pending[0]["interactionId"].as_str().unwrap().to_string();

    // The continuity snapshot carries it too.
    let mut saw_pending_snapshot = false;
    for _ in 0..20 {
        for frame in client.take_streamed() {
            if frame["kind"] == json!("subscription.session_projection")
                && frame["snapshot"]["interactionsPending"].as_array().is_some_and(|a| !a.is_empty())
            {
                saw_pending_snapshot = true;
            }
        }
        if saw_pending_snapshot {
            break;
        }
        match tokio::time::timeout(Duration::from_millis(200), client.next_streamed()).await {
            Ok(Some(frame)) => {
                if frame["kind"] == json!("subscription.session_projection")
                    && frame["snapshot"]["interactionsPending"].as_array().is_some_and(|a| !a.is_empty())
                {
                    saw_pending_snapshot = true;
                    break;
                }
            }
            _ => break,
        }
    }
    assert!(saw_pending_snapshot, "snapshot must surface the pending interaction");

    // Answer allow; the blocked ask resolves with Allow.
    client
        .request(
            "interaction.answer",
            json!({"interactionId": interaction_id, "answer": {"decision": "allow", "rememberForTurn": false}}),
        )
        .await
        .expect("interaction.answer");
    let answer = tokio::time::timeout(Duration::from_secs(2), ask_handle)
        .await
        .expect("ask resolves")
        .expect("task ok");
    assert_eq!(answer.decision, PermissionDecision::Allow);

    // Pending is cleared.
    let cleared = client
        .request("interaction.query", json!({"sessionId": "s1"}))
        .await
        .expect("interaction.query after answer");
    assert!(cleared["pending"].as_array().unwrap().is_empty());

    drop(client);
    serve.abort();
}
