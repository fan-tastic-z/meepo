//! Interaction ops: `interaction.query` lists a session's pending permission
//! prompts; `interaction.answer` resolves one (persisting the canonical
//! request+outcome pair) and unblocks the waiting turn.

use std::sync::Arc;

use meepo_core::{PermissionAnswer, PermissionDecision};
use serde_json::json;

use crate::protocol::OpErrorCode;
use crate::server::dispatcher::{handler, Dispatcher, Outcome};
use crate::server::interaction::{AnswerError, InteractionHub};

fn str_field(input: &serde_json::Value, key: &str) -> String {
    input.get(key).and_then(|v| v.as_str()).unwrap_or_default().to_string()
}

pub fn register(dispatcher: &mut Dispatcher, hub: Arc<InteractionHub>) {
    let query_hub = hub.clone();
    dispatcher.register("interaction.query", handler(move |input, _ctx| {
        let hub = query_hub.clone();
        async move {
            let session_id = str_field(&input, "sessionId");
            if session_id.is_empty() {
                return Outcome::Err {
                    code: OpErrorCode::InvalidRequest,
                    message: "sessionId is required".into(),
                };
            }
            let pending = hub.pending_for_session(&session_id).await;
            Outcome::Ok(json!({ "sessionId": session_id, "pending": pending }))
        }
    }));

    dispatcher.register("interaction.answer", handler(move |input, ctx| {
        let hub = hub.clone();
        async move {
            let interaction_id = str_field(&input, "interactionId");
            if interaction_id.is_empty() {
                return Outcome::Err {
                    code: OpErrorCode::InvalidRequest,
                    message: "interactionId is required".into(),
                };
            }
            let decision = match input.pointer("/answer/decision").and_then(|v| v.as_str()) {
                Some("allow") => PermissionDecision::Allow,
                Some("deny") => PermissionDecision::Deny,
                _ => {
                    return Outcome::Err {
                        code: OpErrorCode::InvalidRequest,
                        message: "answer.decision must be 'allow' or 'deny'".into(),
                    }
                }
            };
            let remember_for_turn = input
                .pointer("/answer/rememberForTurn")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let answer = PermissionAnswer { decision, remember_for_turn };
            match hub.answer(&interaction_id, &ctx.host_epoch, answer).await {
                Ok(()) => Outcome::Ok(json!({ "resolved": true })),
                Err(AnswerError::NotFound(m)) => {
                    Outcome::Err { code: OpErrorCode::AlreadyResolved, message: m }
                }
            }
        }
    }));
}
