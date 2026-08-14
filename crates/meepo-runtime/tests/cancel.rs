//! Phase 3: a turn started via `start_turn_streaming` and then cancelled must
//! end with an abort terminal (`RunStatus::Aborted`), preserving the
//! one-terminal-per-run invariant the recovery layer depends on.

use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use meepo_core::{
    AbortReason, AgentBackend, BackendKind, BackendResult, BackendSendInput, BackendStopMode,
    BackendStopReason, ChatMessage, SessionEvent, StopToken,
};
use meepo_runtime::{RunStatus, SessionManager, TurnEvent};
use tokio_util::sync::CancellationToken;

/// A backend that blocks until its stop token fires, then yields one Abort
/// terminal — the shape a real backend takes when it observes `turn.stop`.
struct CancellableBackend {
    session_id: String,
}

#[async_trait]
impl AgentBackend for CancellableBackend {
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
        async_stream::stream! {
            // Block until cancelled, then yield the abort terminal.
            stop.cancelled().await;
            yield SessionEvent::Abort {
                id: "abort".into(),
                turn_id,
                ts: 0,
                reason: AbortReason::UserStop,
            };
        }
        .boxed()
    }
    async fn stop(
        &mut self,
        _: BackendStopReason,
        _: Option<BackendStopMode>,
    ) -> BackendResult<()> {
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
async fn cancelled_turn_ends_aborted() {
    let store = meepo_core::InMemoryRuntimeEventStore::new();
    let mut session = SessionManager::new("s1");
    let ct = CancellationToken::new();
    let mut backend = CancellableBackend { session_id: "s1".into() };

    let mut ts = session.start_turn_streaming(
        &mut backend,
        "do thing".into(),
        None,
        &[],
        StopToken::from_token(ct.clone()),
    );
    let run_id = ts.run_id.clone();

    // Cancel after the turn has started.
    ct.cancel();

    let mut events = Vec::new();
    let mut terminal = None;
    let mut status = RunStatus::Failed;
    let mut messages = Vec::new();
    let mut compact_summary = None;
    while let Some(te) = ts.next().await {
        match te {
            TurnEvent::Event(re) => events.push(re),
            TurnEvent::Done {
                terminal: t,
                status: s,
                messages: m,
                compact_summary: c,
            } => {
                terminal = Some(t);
                status = s;
                messages = m;
                compact_summary = c;
            }
        }
    }
    let terminal = terminal.expect("turn ended without a Done event");
    let result = session
        .finalize_turn(&store, &run_id, terminal, events, messages, compact_summary, status)
        .await;

    assert_eq!(result.status, RunStatus::Aborted, "a cancelled turn must be Aborted");
    // The session itself reflects the abort.
    assert_eq!(session.status(), meepo_runtime::SessionStatus::Aborted);
}
