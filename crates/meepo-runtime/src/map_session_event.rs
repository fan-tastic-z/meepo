//! SessionEvent → RuntimeEvent mapping (the walking-skeleton subset).
//!
//! Each backend SessionEvent becomes one canonical RuntimeEvent. Text deltas
//! are marked `partial`; terminal events (complete/error/abort) carry a
//! terminal `status`. `refs.providerEventId` links the fact back to the
//! provider's message id for replay alignment.

use meepo_core::{Author, Content, Role, RuntimeEvent, SessionEvent, Status};
use serde_json::{json, Value};

/// Identity spine threaded through every event of one invocation.
#[derive(Debug, Clone)]
pub struct InvocationContext {
    pub session_id: String,
    pub run_id: String,
    pub invocation_id: String,
    pub turn_id: String,
}

/// Map one backend stream event into a canonical runtime fact.
pub fn map_session_event(event: &SessionEvent, ctx: &InvocationContext) -> RuntimeEvent {
    let mut ev = skeleton(ctx, event);
    match event {
        SessionEvent::TextDelta {
            text, message_id, ..
        } => {
            ev.partial = Some(true);
            ev.content = Some(Content::Text {
                text: text.clone(),
                provider_options: None,
                steering: None,
            });
            ev.refs = Some(json!({ "providerEventId": message_id }));
        }
        SessionEvent::TextComplete {
            text,
            message_id,
            provider_options,
            ..
        } => {
            ev.content = Some(Content::Text {
                text: text.clone(),
                provider_options: provider_options.clone(),
                steering: None,
            });
            ev.refs = Some(json!({ "providerEventId": message_id }));
        }
        SessionEvent::Complete { stop_reason, .. } => {
            ev.status = Some(Status::Completed);
            ev.actions = Some(json!({
                "endInvocation": { "stopReason": serde_json::to_value(*stop_reason).unwrap() }
            }));
        }
        SessionEvent::Error {
            message,
            code,
            reason,
            ..
        } => {
            ev.role = Role::System;
            ev.author = Author::System;
            ev.status = Some(Status::Failed);
            ev.content = Some(Content::Error {
                message: message.clone(),
                code: code.clone(),
                reason: reason.clone(),
                details: None,
            });
        }
        SessionEvent::Abort { reason, .. } => {
            ev.role = Role::System;
            ev.author = Author::System;
            ev.status = Some(Status::Aborted);
            ev.actions = Some(json!({
                "endInvocation": { "abortReason": serde_json::to_value(*reason).unwrap() }
            }));
        }
    }
    ev
}

/// Build the identity/ordering spine shared by every mapped event. Role/author
/// default to model/agent (the common case for backend content) and are
/// overridden by terminal error/abort below.
fn skeleton(ctx: &InvocationContext, event: &SessionEvent) -> RuntimeEvent {
    let (id, turn_id, ts) = base(event);
    RuntimeEvent {
        session_id: ctx.session_id.clone(),
        invocation_id: ctx.invocation_id.clone(),
        run_id: ctx.run_id.clone(),
        turn_id: turn_id.to_string(),
        branch: None,
        id: id.to_string(),
        ts,
        role: Role::Model,
        author: Author::Agent,
        origin: None,
        model_visibility: None,
        status: None,
        content: None,
        actions: None,
        refs: None,
        partial: None,
    }
}

/// Extract the BaseEvent fields (id, turnId, ts) shared by every variant.
fn base(event: &SessionEvent) -> (&str, &str, i64) {
    match event {
        SessionEvent::TextDelta { id, turn_id, ts, .. }
        | SessionEvent::TextComplete { id, turn_id, ts, .. }
        | SessionEvent::Complete { id, turn_id, ts, .. }
        | SessionEvent::Error { id, turn_id, ts, .. }
        | SessionEvent::Abort { id, turn_id, ts, .. } => (id.as_str(), turn_id.as_str(), *ts),
    }
}

// Reference Value so the `json!` macro's output type is unambiguous.
#[allow(dead_code)]
type _Json = Value;
