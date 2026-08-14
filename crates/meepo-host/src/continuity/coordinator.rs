//! [`SessionContinuityCoordinator`] — owns the per-session canonical snapshot
//! and fans `subscription.*` frames out to attached subscribers as runtime
//! events arrive.
//!
//! Phase 6 wires the coordinator and the text/terminal event mapping; phase 7
//! connects it to a live turn's event stream (tool lifecycle → `session_event`).

use std::collections::HashMap;
use std::sync::Arc;

use meepo_core::{Content, Role, RuntimeEvent};
use tokio::sync::{mpsc, Mutex};

use super::frames::{DeltaKind, SubscriptionFrame};
use super::snapshot::SessionContinuitySnapshot;

/// Per-subscriber outbound queue depth. A subscriber that falls a full queue
/// behind is dropped (slow consumer).
const SUB_QUEUE: usize = 32;

pub struct SessionContinuityCoordinator {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    sessions: HashMap<String, SessionState>,
}

struct SessionState {
    snapshot: SessionContinuitySnapshot,
    subscribers: Vec<Subscriber>,
}

struct Subscriber {
    subscription_id: String,
    /// Sequence number the next frame to THIS subscriber will carry.
    next_sequence: u64,
    tx: mpsc::Sender<SubscriptionFrame>,
}

/// The result of `subscription.open`: the canonical snapshot plus the receiver
/// the caller drains to forward streamed frames to its client.
pub struct OpenedSubscription {
    pub subscription_id: String,
    /// Sequence the first streamed frame will carry.
    pub next_sequence: u64,
    pub snapshot: SessionContinuitySnapshot,
    pub frames: mpsc::Receiver<SubscriptionFrame>,
}

impl Default for SessionContinuityCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionContinuityCoordinator {
    pub fn new() -> Self {
        Self { inner: Arc::new(Mutex::new(Inner { sessions: HashMap::new() })) }
    }

    /// Open a subscription: allocate an id, register the subscriber, return the
    /// current snapshot + the stream receiver.
    pub async fn open_subscription(&self, session_id: &str) -> OpenedSubscription {
        let (tx, rx) = mpsc::channel(SUB_QUEUE);
        let subscription_id = uuid::Uuid::new_v4().to_string();
        let mut inner = self.inner.lock().await;
        let state = inner
            .sessions
            .entry(session_id.to_string())
            .or_insert_with(|| SessionState {
                snapshot: SessionContinuitySnapshot::fresh(session_id),
                subscribers: Vec::new(),
            });
        let next_sequence = 1;
        state.subscribers.push(Subscriber { subscription_id: subscription_id.clone(), next_sequence, tx });
        let snapshot = state.snapshot.clone();
        OpenedSubscription { subscription_id, next_sequence, snapshot, frames: rx }
    }

    /// Explicitly close a subscription (delivers `closed{session_removed}`
    /// when the queue has room, then removes the subscriber).
    pub async fn close_subscription(&self, subscription_id: &str, host_epoch: &str) {
        let mut inner = self.inner.lock().await;
        for state in inner.sessions.values_mut() {
            if let Some(idx) = state.subscribers.iter().position(|s| s.subscription_id == subscription_id) {
                let _ = state.subscribers[idx].tx.try_send(SubscriptionFrame::closed(
                    host_epoch,
                    subscription_id,
                    super::frames::ClosedReason::SessionRemoved,
                ));
                state.subscribers.swap_remove(idx);
            }
        }
    }

    /// Map a runtime event to subscription frame(s) and fan out to subscribers.
    pub async fn accept_runtime_event(
        &self,
        session_id: &str,
        run_id: &str,
        host_epoch: &str,
        event: &RuntimeEvent,
    ) {
        let mut inner = self.inner.lock().await;
        let Some(state) = inner.sessions.get_mut(session_id) else {
            return;
        };

        if event.status.is_some() {
            // Canonical terminal transition → bump revision + session_projection.
            state.snapshot.projection_revision += 1;
            let status_str = serde_json::to_value(&event.status)
                .ok()
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_default();
            state.snapshot.root_turn = Some(serde_json::json!({
                "turnId": event.turn_id,
                "runId": run_id,
                "status": status_str,
            }));
            let snapshot_val = serde_json::to_value(&state.snapshot).expect("snapshot serializes");
            fan_out(state, host_epoch, |sid, seq| SubscriptionFrame::SessionProjection {
                host_epoch: host_epoch.to_string(),
                subscription_id: sid.to_string(),
                sequence: seq,
                snapshot: snapshot_val.clone(),
            });
        } else if is_assistant_text(event) {
            let text = text_of(event);
            fan_out(state, host_epoch, |sid, seq| SubscriptionFrame::SessionDelta {
                host_epoch: host_epoch.to_string(),
                subscription_id: sid.to_string(),
                sequence: seq,
                turn_id: event.turn_id.clone(),
                run_id: run_id.to_string(),
                message_id: event.id.clone(),
                delta_kind: DeltaKind::Text,
                start_offset: 0,
                text: text.clone(),
            });
        }
        // Tool call/result + dispatch facts map to session_event in phase 7.
    }
}

/// Build + send a frame to every subscriber (per-subscriber sequence). A full
/// queue drops the subscriber (slow consumer) — the queue is full so the
/// `closed` frame cannot ride it; the client observes the stream end.
fn fan_out<F>(state: &mut SessionState, host_epoch: &str, build: F)
where
    F: Fn(&str, u64) -> SubscriptionFrame,
{
    let mut to_drop: Vec<String> = Vec::new();
    for sub in state.subscribers.iter_mut() {
        let seq = sub.next_sequence;
        sub.next_sequence += 1;
        let frame = build(&sub.subscription_id, seq);
        if sub.tx.try_send(frame).is_err() {
            to_drop.push(sub.subscription_id.clone());
        }
    }
    for sid in to_drop {
        if let Some(idx) = state.subscribers.iter().position(|s| s.subscription_id == sid) {
            // Best-effort close notice; on a full queue this is lost (stream ends).
            let _ = state.subscribers[idx].tx.try_send(SubscriptionFrame::closed(
                host_epoch,
                &sid,
                super::frames::ClosedReason::SlowConsumer,
            ));
            state.subscribers.swap_remove(idx);
        }
    }
}

fn is_assistant_text(ev: &RuntimeEvent) -> bool {
    ev.role == Role::Model
        && ev.status.is_none()
        && matches!(ev.content, Some(Content::Text { .. }))
}

fn text_of(ev: &RuntimeEvent) -> String {
    match &ev.content {
        Some(Content::Text { text, .. }) => text.clone(),
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meepo_core::{Author, Status};
    use meepo_core::Content;

    fn ev(text: &str, status: Option<Status>) -> RuntimeEvent {
        RuntimeEvent {
            session_id: "s1".into(),
            invocation_id: "inv".into(),
            run_id: "run-1".into(),
            turn_id: "turn-1".into(),
            branch: None,
            id: "e1".into(),
            ts: 0,
            role: Role::Model,
            author: Author::Agent,
            origin: None,
            model_visibility: None,
            status,
            content: Some(Content::Text { text: text.into(), provider_options: None, steering: None }),
            actions: None,
            refs: None,
            partial: None,
        }
    }

    #[tokio::test]
    async fn open_returns_snapshot_and_next_sequence() {
        let coord = SessionContinuityCoordinator::new();
        let opened = coord.open_subscription("s1").await;
        assert_eq!(opened.next_sequence, 1);
        assert_eq!(opened.snapshot.session_id, "s1");
    }

    #[tokio::test]
    async fn text_event_emits_delta_with_sequence() {
        let coord = SessionContinuityCoordinator::new();
        let mut opened = coord.open_subscription("s1").await;
        let sid = opened.subscription_id.clone();
        coord.accept_runtime_event("s1", "run-1", "epoch-1", &ev("hi", None)).await;
        match opened.frames.recv().await.unwrap() {
            SubscriptionFrame::SessionDelta { subscription_id, sequence, text, delta_kind, .. } => {
                assert_eq!(subscription_id, sid);
                assert_eq!(sequence, 1);
                assert_eq!(text, "hi");
                assert_eq!(delta_kind, DeltaKind::Text);
            }
            other => panic!("expected delta, got {}", other.kind()),
        }
    }

    #[tokio::test]
    async fn terminal_event_emits_projection_with_bumped_revision() {
        let coord = SessionContinuityCoordinator::new();
        let mut opened = coord.open_subscription("s1").await;
        coord.accept_runtime_event("s1", "run-1", "epoch-1", &ev("done", Some(Status::Completed))).await;
        match opened.frames.recv().await.unwrap() {
            SubscriptionFrame::SessionProjection { snapshot, sequence, .. } => {
                assert_eq!(sequence, 1);
                assert_eq!(snapshot["projectionRevision"], 1);
                assert_eq!(snapshot["rootTurn"]["status"], "completed");
            }
            other => panic!("expected projection, got {}", other.kind()),
        }
    }

    #[tokio::test]
    async fn slow_consumer_is_dropped() {
        let coord = SessionContinuityCoordinator::new();
        let mut opened = coord.open_subscription("s1").await;
        // Flood far past the queue cap without draining.
        for i in 0..60 {
            coord.accept_runtime_event("s1", "run-1", "epoch", &ev(&format!("t{i}"), None)).await;
        }
        let mut count = 0;
        while opened.frames.recv().await.is_some() {
            count += 1;
            if count > 100 {
                panic!("too many frames");
            }
        }
        assert!(count <= SUB_QUEUE, "buffered {count}, cap {SUB_QUEUE}");
    }

    #[tokio::test]
    async fn explicit_close_ends_the_stream() {
        let coord = SessionContinuityCoordinator::new();
        let mut opened = coord.open_subscription("s1").await;
        let sid = opened.subscription_id.clone();
        coord.close_subscription(&sid, "epoch").await;
        match opened.frames.recv().await.unwrap() {
            SubscriptionFrame::Closed { reason, .. } => {
                assert_eq!(reason, super::super::frames::ClosedReason::SessionRemoved);
            }
            other => panic!("expected closed, got {}", other.kind()),
        }
    }
}
