//! [`TurnCoordinator`] — the key path: admit a turn durably, run it on a
//! detached task, pump its runtime events into continuity (subscription
//! frames), and finalize (persist + durable terminal).
//!
//! One turn per session at a time: `turn.start` on a session with an active
//! turn returns `session_busy`. The drain task holds the backend for the
//! stream's lifetime (start_turn_streaming's TurnStream borrows it); the
//! SessionManager lock is only held across admit-start and finalize.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use meepo_core::StopToken;
use meepo_runtime::{RunStatus, SessionManager, TurnEvent};
use meepo_storage::RootAdmissionRecord;
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;

use crate::continuity::SessionContinuityCoordinator;
use crate::server::composition::Composition;

#[derive(Debug, thiserror::Error)]
pub enum TurnError {
    #[error("session busy: a turn is already running")]
    SessionBusy,
    #[error("not found: {0}")]
    NotFound(String),
    #[error("session poisoned: admission chain marked poisoned")]
    SessionPoisoned,
    #[error("storage: {0}")]
    Storage(String),
}

/// The `turn.start` result: the admitted turn identity.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnStarted {
    pub session_id: String,
    pub turn_id: String,
    pub run_id: String,
    pub status: String,
}

/// The `turn.stop` result.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnStopped {
    pub session_id: String,
    pub status: String,
}

struct ActiveTurn {
    #[allow(dead_code)] // phase 8 (turn.stop) cancels through this handle
    stop: CancellationToken,
}

pub struct TurnCoordinator {
    composition: Arc<Composition>,
    continuity: Arc<SessionContinuityCoordinator>,
    active: Arc<AsyncMutex<HashMap<String, ActiveTurn>>>,
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl TurnCoordinator {
    pub fn new(
        composition: Arc<Composition>,
        continuity: Arc<SessionContinuityCoordinator>,
    ) -> Self {
        Self {
            composition,
            continuity,
            active: Arc::new(AsyncMutex::new(HashMap::new())),
        }
    }

    /// Start a turn. Returns immediately with the admitted identity; the run
    /// itself drains on a detached task (admitted → running).
    pub async fn start_turn(
        &self,
        session_id: &str,
        host_epoch: &str,
        content: String,
    ) -> Result<TurnStarted, TurnError> {
        // One turn per session at a time.
        if self.active.lock().await.contains_key(session_id) {
            return Err(TurnError::SessionBusy);
        }

        // Durable root admission: extend the chain from the current tip. Turn
        // identity is deterministic (turn-N/run-N from the message count).
        let store = self.composition.store();
        let session: Arc<AsyncMutex<SessionManager>> = self.composition.session(session_id).await;
        let n = session.lock().await.messages().len() + 1;
        let turn_id = format!("turn-{n}");
        let run_id = format!("run-{n}");
        let tip = store
            .read_admission_tip(session_id)
            .await
            .map_err(|e| TurnError::Storage(e.to_string()))?;
        if tip.as_ref().is_some_and(|t| t.poisoned) {
            return Err(TurnError::SessionPoisoned);
        }
        let rec = RootAdmissionRecord {
            session_id: session_id.into(),
            turn_id: turn_id.clone(),
            run_id: run_id.clone(),
            previous_root_turn_id: tip.map(|t| t.turn_id),
            identity_json: serde_json::to_string(&serde_json::json!({
                "turnId": turn_id,
                "runId": run_id,
                "content": content,
            }))
            .unwrap_or_default(),
            admitted_at: now_secs(),
            poisoned: false,
        };
        store
            .extend_admission_chain(&rec)
            .await
            .map_err(|e| TurnError::Storage(e.to_string()))?;

        // Register active, then drain on a detached task.
        let stop = CancellationToken::new();
        self.active
            .lock()
            .await
            .insert(session_id.to_string(), ActiveTurn { stop: stop.clone() });

        let composition = self.composition.clone();
        let continuity = self.continuity.clone();
        let active_map = self.active.clone();
        let sid = session_id.to_string();
        let epoch = host_epoch.to_string();
        let mut backend = composition.build_backend(session_id);
        let system = composition.system_prompt().map(str::to_string);
        tokio::spawn(async move {
            let stop_token = StopToken::from_token(stop);
            let mut turn = {
                let mut s = session.lock().await;
                s.start_turn_streaming(&mut *backend, content, system, &[], stop_token)
            };
            let run_id = turn.run_id.clone();
            let mut events = Vec::new();
            let mut terminal = None;
            let mut status = RunStatus::Failed;
            let mut messages = Vec::new();
            let mut compact_summary = None;
            while let Some(te) = turn.next().await {
                match te {
                    TurnEvent::Event(re) => {
                        continuity.accept_runtime_event(&sid, &run_id, &epoch, &re).await;
                        events.push(re);
                    }
                    TurnEvent::Done {
                        terminal: t,
                        status: st,
                        messages: m,
                        compact_summary: c,
                    } => {
                        terminal = Some(t);
                        status = st;
                        messages = m;
                        compact_summary = c;
                    }
                }
            }
            if let Some(terminal) = terminal {
                let mut s = session.lock().await;
                s.finalize_turn(
                    &**composition.store(),
                    &run_id,
                    terminal,
                    events,
                    messages,
                    compact_summary,
                    status,
                )
                .await;
            }
            active_map.lock().await.remove(&sid);
        });

        Ok(TurnStarted {
            session_id: session_id.into(),
            turn_id,
            run_id,
            status: "running".into(),
        })
    }

    /// Stop the active turn of `session_id`: cancel its stop token. The drain
    /// task observes the cancellation, ends the run with an abort terminal
    /// (the one-terminal-per-run invariant holds), finalizes, and releases the
    /// session.
    pub async fn stop_turn(&self, session_id: &str) -> Result<TurnStopped, TurnError> {
        let active = self.active.lock().await;
        match active.get(session_id) {
            Some(t) => {
                t.stop.cancel();
                Ok(TurnStopped { session_id: session_id.into(), status: "request_stop".into() })
            }
            None => Err(TurnError::NotFound(format!(
                "no active turn for session '{session_id}'"
            ))),
        }
    }
}
