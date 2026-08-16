//! Interaction hub — asynchronous permission prompts over the wire.
//!
//! A backend's permission gate asks through the hub: the request becomes a
//! pending interaction (visible via `interaction.query` and the continuity
//! snapshot's `interactionsPending`), the turn blocks on the answer, and
//! `interaction.answer` resolves it (persisting the canonical request+outcome
//! pair) so the turn continues.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use meepo_core::{InteractionStore, PermissionAnswer, PermissionDecision, PermissionPrompter, PermissionRequest};
use meepo_storage::SqliteStore;
use tokio::sync::{oneshot, Mutex};

use crate::continuity::SessionContinuityCoordinator;

/// Where the prompt arose (for persistence + snapshot routing).
#[derive(Debug, Clone)]
pub struct InteractionContext {
    pub session_id: String,
    pub run_id: String,
    pub turn_id: String,
}

struct PendingInteraction {
    ctx: InteractionContext,
    request_json: String,
    created_at: i64,
    view: serde_json::Value,
    answer_tx: oneshot::Sender<PermissionAnswer>,
}

#[derive(Debug, thiserror::Error)]
pub enum AnswerError {
    #[error("no pending interaction '{0}'")]
    NotFound(String),
}

pub struct InteractionHub {
    store: Arc<SqliteStore>,
    continuity: Arc<SessionContinuityCoordinator>,
    pending: Mutex<HashMap<String, PendingInteraction>>,
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl InteractionHub {
    pub fn new(store: Arc<SqliteStore>, continuity: Arc<SessionContinuityCoordinator>) -> Self {
        Self { store, continuity, pending: Mutex::new(HashMap::new()) }
    }

    /// Register a prompt and block until it is answered. The request surfaces
    /// in `interaction.query` and the continuity snapshot while pending.
    pub async fn ask(
        &self,
        ctx: &InteractionContext,
        host_epoch: &str,
        request: &PermissionRequest,
    ) -> PermissionAnswer {
        let request_id = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        let request_json = serde_json::to_string(request).unwrap_or_default();
        let view = serde_json::json!({
            "interactionId": request_id,
            "sessionId": ctx.session_id,
            "toolName": request.tool_name,
            "category": serde_json::to_value(&request.category).unwrap_or(serde_json::Value::Null),
            "summary": request.summary,
        });
        let created_at = now_secs();
        {
            let mut pending = self.pending.lock().await;
            pending.insert(
                request_id.clone(),
                PendingInteraction {
                    ctx: ctx.clone(),
                    request_json,
                    created_at,
                    view: view.clone(),
                    answer_tx: tx,
                },
            );
        }
        self.publish_pending(&ctx.session_id, host_epoch).await;

        match rx.await {
            Ok(answer) => answer,
            Err(_) => {
                // The answerer vanished (host shutdown): deny to unblock.
                PermissionAnswer { decision: PermissionDecision::Deny, remember_for_turn: false }
            }
        }
    }

    /// Answer a pending interaction: persist the canonical request+outcome,
    /// clear it from the snapshot, and unblock the turn.
    pub async fn answer(
        &self,
        interaction_id: &str,
        host_epoch: &str,
        answer: PermissionAnswer,
    ) -> Result<(), AnswerError> {
        let entry = {
            let mut pending = self.pending.lock().await;
            pending.remove(interaction_id)
        };
        let Some(entry) = entry else {
            return Err(AnswerError::NotFound(interaction_id.to_string()));
        };
        let outcome_json = serde_json::to_string(&answer).unwrap_or_default();
        let _ = self
            .store
            .record_permission(
                &entry.ctx.session_id,
                &entry.ctx.run_id,
                &entry.ctx.turn_id,
                interaction_id,
                entry.created_at,
                &entry.request_json,
                &outcome_json,
            )
            .await;
        let _ = entry.answer_tx.send(answer);
        self.publish_pending(&entry.ctx.session_id, host_epoch).await;
        Ok(())
    }

    /// The pending interactions of a session (client-facing views).
    pub async fn pending_for_session(&self, session_id: &str) -> Vec<serde_json::Value> {
        let pending = self.pending.lock().await;
        pending
            .values()
            .filter(|p| p.ctx.session_id == session_id)
            .map(|p| p.view.clone())
            .collect()
    }

    async fn publish_pending(&self, session_id: &str, host_epoch: &str) {
        let views = self.pending_for_session(session_id).await;
        self.continuity.set_pending_interactions(session_id, host_epoch, views).await;
    }
}

/// A [`PermissionPrompter`] backed by the hub — wire into a backend's gate so
/// prompts become wire interactions instead of terminal stdin.
pub struct HubPrompter {
    hub: Arc<InteractionHub>,
    ctx: InteractionContext,
    host_epoch: String,
}

impl HubPrompter {
    pub fn new(hub: Arc<InteractionHub>, ctx: InteractionContext, host_epoch: String) -> Self {
        Self { hub, ctx, host_epoch }
    }
}

#[async_trait]
impl PermissionPrompter for HubPrompter {
    async fn ask(&self, request: &PermissionRequest) -> PermissionAnswer {
        self.hub.ask(&self.ctx, &self.host_epoch, request).await
    }
}
