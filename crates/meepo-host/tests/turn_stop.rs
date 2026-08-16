//! Phase 8: turn.stop cancels the active turn — the client observes an abort
//! terminal projection on the subscription, and the session is released.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use meepo_core::{
    AbortReason, AgentBackend, BackendKind, BackendResult, BackendSendInput, BackendStopMode,
    BackendStopReason, ChatMessage, SessionEvent, StopToken,
};
use meepo_storage::SqliteStore;

use meepo_host::server::{BackendFactory, Composition, TurnCoordinator};
use meepo_host::{handlers, transport, Dispatcher, HostClient, HostKernel, SessionContinuityCoordinator};

/// A backend that blocks until cancelled, then yields one Abort terminal.
struct StopBackend {
    session_id: String,
}

#[async_trait]
impl AgentBackend for StopBackend {
    fn kind(&self) -> BackendKind {
        BackendKind::Fake
    }
    fn session_id(&self) -> &str {
        &self.session_id
    }
    fn send<'a>(&'a mut self, _input: &'a BackendSendInput) -> BoxStream<'a, SessionEvent> {
        unimplemented!("send not used; send_cancellable handles cancellation")
    }
    fn send_cancellable<'a>(
        &'a mut self,
        _input: &'a BackendSendInput,
        stop: StopToken,
    ) -> BoxStream<'a, SessionEvent> {
        let turn_id = self.session_id.clone();
        futures::stream::once(async move {
            stop.cancelled().await;
            SessionEvent::Abort {
                id: "abort".into(),
                turn_id,
                ts: 0,
                reason: AbortReason::UserStop,
            }
        })
        .boxed()
    }
    async fn stop(&mut self, _: BackendStopReason, _: Option<BackendStopMode>) -> BackendResult<()> {
        Ok(())
    }
    async fn dispose(&mut self) -> BackendResult<()> {
        Ok(())
    }
    async fn compact_history(&self, _m: &[ChatMessage]) -> BackendResult<String> {
        Ok("c".into())
    }
}

#[tokio::test]
async fn turn_stop_cancels_and_releases_the_session() {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("h.sock");
    let listener = transport::bind(&sock).unwrap();

    let factory: BackendFactory = Arc::new(|_s| Box::new(StopBackend { session_id: "s".into() }));
    let store = Arc::new(SqliteStore::in_memory().unwrap());
    let composition = Arc::new(Composition::new(store.clone(), factory, None));
    let continuity = Arc::new(SessionContinuityCoordinator::new());
    let turns = Arc::new(TurnCoordinator::new(composition, continuity.clone()));

    let mut dispatcher = Dispatcher::new();
    handlers::host::register(&mut dispatcher);
    handlers::session::register(&mut dispatcher, store);
    handlers::turn::register(&mut dispatcher, turns);
    let kernel = HostKernel::new("epoch-stop", dispatcher, continuity);
    let serve = tokio::spawn(async move {
        kernel.serve(listener).await;
    });

    let (mut client, _) = HostClient::connect(&sock).await.expect("connect");
    client
        .request("session.create", serde_json::json!({"sessionId": "s1"}))
        .await
        .expect("session.create");
    client
        .request("subscription.open", serde_json::json!({"sessionId": "s1"}))
        .await
        .expect("subscribe");
    let started = client
        .request("turn.start", serde_json::json!({"sessionId": "s1", "content": "long task"}))
        .await
        .expect("turn.start");
    assert_eq!(started["status"], serde_json::json!("running"));

    // Stop the turn; the subscribed client must observe the abort terminal.
    let stopped = client
        .request("turn.stop", serde_json::json!({"sessionId": "s1"}))
        .await
        .expect("turn.stop");
    assert_eq!(stopped["status"], serde_json::json!("request_stop"));

    let mut saw_aborted = false;
    loop {
        for frame in client.take_streamed() {
            if frame["kind"] == serde_json::json!("subscription.session_projection")
                && frame["snapshot"]["rootTurn"]["status"] == serde_json::json!("aborted")
            {
                saw_aborted = true;
            }
        }
        if saw_aborted {
            break;
        }
        match tokio::time::timeout(Duration::from_secs(2), client.next_streamed()).await {
            Ok(Some(frame)) => {
                if frame["kind"] == serde_json::json!("subscription.session_projection")
                    && frame["snapshot"]["rootTurn"]["status"] == serde_json::json!("aborted")
                {
                    saw_aborted = true;
                }
            }
            _ => break,
        }
    }
    assert!(saw_aborted, "client must observe the aborted projection");

    // The session is released: a later turn.stop reports no active turn.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let second = client.request("turn.stop", serde_json::json!({"sessionId": "s1"})).await;
    assert!(second.is_err(), "no active turn after the drain settled");

    drop(client);
    serve.abort();
}
