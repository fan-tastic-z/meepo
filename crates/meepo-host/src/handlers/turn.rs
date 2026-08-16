//! `turn.start` — admit a turn for a session. The response returns once the
//! turn is admitted/running; assistant output streams over a subscription.

use std::sync::Arc;

use crate::protocol::OpErrorCode;
use crate::server::dispatcher::{handler, Dispatcher, Outcome};
use crate::server::turn::{TurnCoordinator, TurnError};

pub fn register(dispatcher: &mut Dispatcher, coordinator: Arc<TurnCoordinator>) {
    dispatcher.register(
        "turn.start",
        handler(move |input, ctx| {
            let coordinator = coordinator.clone();
            async move {
                let session_id = input
                    .get("sessionId")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let content = input
                    .get("content")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                if session_id.is_empty() || content.is_empty() {
                    return Outcome::Err {
                        code: OpErrorCode::InvalidRequest,
                        message: "sessionId and content are required".into(),
                    };
                }
                match coordinator.start_turn(&session_id, &ctx.host_epoch, content).await {
                    Ok(started) => {
                        Outcome::Ok(serde_json::to_value(&started).expect("started serializes"))
                    }
                    Err(TurnError::SessionBusy) => Outcome::Err {
                        code: OpErrorCode::SessionBusy,
                        message: "a turn is already running for this session".into(),
                    },
                    Err(TurnError::SessionPoisoned) => Outcome::Err {
                        code: OpErrorCode::OperationConflict,
                        message: "session admission chain is poisoned".into(),
                    },
                    Err(TurnError::Storage(e)) => {
                        Outcome::Err { code: OpErrorCode::PersistenceFailed, message: e }
                    }
                }
            }
        }),
    );
}
