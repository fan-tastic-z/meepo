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
    #[error("session archived: {0}")]
    SessionArchived(String),
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

/// `turn.resume.query` result: a continuation is possible, or why not.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase", tag = "disposition")]
pub enum ResumePlan {
    #[serde(rename_all = "camelCase")]
    Ready {
        source_run_id: String,
        source_turn_id: Option<String>,
        /// How many RuntimeEvents of the source run are durably committed —
        /// the resume cursor the continuation replays after.
        source_runtime_event_high_water: i64,
    },
    #[serde(rename_all = "camelCase")]
    Parked {
        reason: String,
    },
}

/// `turn.resume.start` result: the continuation started, or parked.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum ResumeStart {
    #[serde(rename_all = "camelCase")]
    Started {
        #[serde(flatten)]
        turn: TurnStarted,
        source_run_id: String,
        source_runtime_event_high_water: i64,
    },
    #[serde(rename_all = "camelCase")]
    Parked {
        reason: String,
    },
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

    /// Start a turn (the `turn.start` op). Returns immediately with the
    /// admitted identity; the run drains on a detached task.
    pub async fn start_turn(
        &self,
        session_id: &str,
        host_epoch: &str,
        content: String,
    ) -> Result<TurnStarted, TurnError> {
        self.begin_turn(session_id, host_epoch, content).await
    }

    /// The shared admit-and-drain path for `turn.start` and resume continuations.
    async fn begin_turn(
        &self,
        session_id: &str,
        host_epoch: &str,
        content: String,
    ) -> Result<TurnStarted, TurnError> {
        // One turn per session at a time.
        if self.active.lock().await.contains_key(session_id) {
            return Err(TurnError::SessionBusy);
        }

        // The session must exist in the catalog and not be archived.
        match self.composition.store().get_session(session_id).await {
            Ok(Some(rec)) if !rec.is_archived => {}
            Ok(Some(_)) => {
                return Err(TurnError::SessionArchived(session_id.to_string()));
            }
            Ok(None) => {
                return Err(TurnError::NotFound(format!(
                    "session '{session_id}' does not exist (session.create it first)"
                )));
            }
            Err(e) => return Err(TurnError::Storage(e.to_string())),
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
                "invocationId": format!("inv-{n}"),
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

    /// `turn.resume.query`: whether a continuation of the session's latest run
    /// is possible, keyed on the durable RuntimeEvent high-water mark.
    pub async fn resume_query(&self, session_id: &str) -> Result<ResumePlan, TurnError> {
        let store = self.composition.store();
        let tip = store
            .read_admission_tip(session_id)
            .await
            .map_err(|e| TurnError::Storage(e.to_string()))?;
        let Some(tip) = tip else {
            return Ok(ResumePlan::Parked { reason: "resume_candidate_missing".into() });
        };
        if tip.poisoned {
            return Ok(ResumePlan::Parked { reason: "safety_check_failed".into() });
        }
        let high_water = store
            .read_runtime_event_high_water(session_id, &tip.run_id)
            .await
            .map_err(|e| TurnError::Storage(e.to_string()))?;
        if high_water == 0 {
            return Ok(ResumePlan::Parked { reason: "source_run_unreadable".into() });
        }
        Ok(ResumePlan::Ready {
            source_run_id: tip.run_id,
            source_turn_id: Some(tip.turn_id),
            source_runtime_event_high_water: high_water,
        })
    }

    /// `turn.resume.start`: validate the caller's high-water against the
    /// durable source run, record a continuation claim, and start a
    /// continuation turn carrying the source admission's original content.
    pub async fn resume_start(
        &self,
        session_id: &str,
        host_epoch: &str,
        source_high_water: i64,
    ) -> Result<ResumeStart, TurnError> {
        // Resolve + drift-check the source run.
        let plan = self.resume_query(session_id).await?;
        let ResumePlan::Ready {
            source_run_id,
            source_turn_id,
            source_runtime_event_high_water: current_hw,
        } = plan
        else {
            return Ok(match plan {
                ResumePlan::Parked { reason } => ResumeStart::Parked { reason },
                _ => unreachable!("plan is Ready or Parked"),
            });
        };
        if current_hw != source_high_water {
            return Ok(ResumeStart::Parked { reason: "source_run_changed".into() });
        }

        // The original content (the continuation re-runs it from the boundary).
        let store = self.composition.store();
        let tip = store
            .read_admission_tip(session_id)
            .await
            .map_err(|e| TurnError::Storage(e.to_string()))?
            .ok_or_else(|| TurnError::NotFound("admission tip vanished".into()))?;
        let identity: serde_json::Value =
            serde_json::from_str(&tip.identity_json).unwrap_or(serde_json::json!({}));
        let content = identity
            .get("content")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        // Deterministic continuation identity (n = messages + 1).
        let session = self.composition.session(session_id).await;
        let n2 = session.lock().await.messages().len() + 1;
        let target_turn = format!("turn-{n2}");
        let target_run = format!("run-{n2}");
        let target_invocation = format!("inv-{n2}");

        // Content digest over the durable source prefix.
        use meepo_core::RuntimeEventStore;
        let events = store
            .read_session_runtime_events(session_id)
            .await
            .map_err(|e| TurnError::Storage(e.to_string()))?;
        let mut prefix = String::new();
        for ev in events.iter().filter(|e| e.run_id == source_run_id) {
            prefix.push_str(&serde_json::to_string(ev).unwrap_or_default());
        }
        use sha2::{Digest, Sha256};
        let source_prefix_digest = format!("{:x}", Sha256::digest(prefix.as_bytes()));
        let boundary_digest = format!("{:x}", Sha256::digest(
            format!("{source_prefix_digest}:{target_turn}:{target_run}:{source_high_water}").as_bytes(),
        ));
        let claim = meepo_storage::ContinuationClaim {
            claim_id: uuid::Uuid::new_v4().to_string(),
            source_session_id: session_id.to_string(),
            source_invocation_id: identity
                .get("invocationId")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            source_run_id: source_run_id.clone(),
            source_turn_id: source_turn_id.unwrap_or_default(),
            source_event_high_water: source_high_water,
            source_prefix_digest,
            boundary_digest,
            boundary_json: serde_json::json!({
                "sourceRunId": source_run_id,
                "sourceHighWater": source_high_water,
                "targetTurnId": target_turn,
            })
            .to_string(),
            provider_projection_version: 1,
            provider_replay_digest: format!("{:x}", Sha256::digest(prefix.as_bytes())),
            target_session_id: session_id.to_string(),
            target_invocation_id: target_invocation,
            target_run_id: target_run.clone(),
            target_turn_id: target_turn.clone(),
            target_run_header_json: serde_json::json!({
                "turnId": target_turn,
                "runId": target_run,
            })
            .to_string(),
            claimed_at: now_secs(),
            start_event_id: None,
            start_kind: None,
            protocol_version: 1,
        };
        store
            .record_continuation_claim(&claim)
            .await
            .map_err(|e| TurnError::Storage(e.to_string()))?;

        // Run the continuation on the shared admit-and-drain path.
        match self.begin_turn(session_id, host_epoch, content).await {
            Ok(turn) => Ok(ResumeStart::Started {
                turn,
                source_run_id,
                source_runtime_event_high_water: source_high_water,
            }),
            Err(TurnError::SessionBusy) => {
                Ok(ResumeStart::Parked { reason: "session_busy".into() })
            }
            Err(e) => Err(e),
        }
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
