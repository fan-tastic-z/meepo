//! SessionEvent → RuntimeEvent mapping.

use meepo_core::{
    Author, Content, Role, RuntimeEvent, RuntimeEventActions, RuntimeEventRefs, SessionEvent,
    Status, TOOL_BOUNDARY_PROTOCOL_V1, ToolDispatch,
};
use serde_json::Value;

/// Identity spine threaded through every event of one invocation.
#[derive(Debug, Clone)]
pub struct InvocationContext {
    pub session_id: String,
    pub run_id: String,
    pub invocation_id: String,
    pub turn_id: String,
}

pub fn map_session_event(event: &SessionEvent, ctx: &InvocationContext) -> RuntimeEvent {
    let mut ev = skeleton(ctx, event);
    match event {
        SessionEvent::TextDelta { text, message_id, .. } => {
            ev.partial = Some(true);
            ev.content = Some(Content::Text {
                text: text.clone(),
                provider_options: None,
                steering: None,
            });
            ev.refs = Some(RuntimeEventRefs {
                provider_event_id: Some(message_id.clone()),
                ..Default::default()
            });
        }
        SessionEvent::TextComplete { text, message_id, provider_options, .. } => {
            ev.content = Some(Content::Text {
                text: text.clone(),
                provider_options: provider_options.clone(),
                steering: None,
            });
            ev.refs = Some(RuntimeEventRefs {
                provider_event_id: Some(message_id.clone()),
                ..Default::default()
            });
        }
        SessionEvent::ThinkingDelta { text, message_id, .. } => {
            ev.partial = Some(true);
            ev.content = Some(Content::Thinking {
                text: text.clone(),
                signature: None,
                provider_options: None,
            });
            ev.refs = Some(RuntimeEventRefs {
                provider_event_id: Some(message_id.clone()),
                ..Default::default()
            });
        }
        SessionEvent::ThinkingComplete { text, signature, message_id, .. } => {
            ev.content = Some(Content::Thinking {
                text: text.clone(),
                signature: signature.clone(),
                provider_options: None,
            });
            ev.refs = Some(RuntimeEventRefs {
                provider_event_id: Some(message_id.clone()),
                ..Default::default()
            });
        }
        SessionEvent::ToolCall { tool_call_id, tool_name, args, .. } => {
            ev.content = Some(Content::FunctionCall {
                id: tool_call_id.clone(),
                name: tool_name.clone(),
                args: args.clone(),
                provider_options: None,
                provider_executed: None,
            });
        }
        SessionEvent::ToolDispatch {
            operation_id,
            tool_call_id,
            tool_name,
            canonical_args_hash,
            recovery_mode,
            ..
        } => {
            ev.role = Role::System;
            ev.author = Author::System;
            ev.content = None;
            ev.actions = Some(RuntimeEventActions {
                tool_dispatch: Some(ToolDispatch {
                    protocol: TOOL_BOUNDARY_PROTOCOL_V1.to_string(),
                    operation_id: operation_id.clone(),
                    provider_tool_call_id: tool_call_id.clone(),
                    tool_name: tool_name.clone(),
                    canonical_args_hash: canonical_args_hash.clone(),
                    recovery_mode: *recovery_mode,
                }),
                ..Default::default()
            });
            ev.refs = Some(RuntimeEventRefs {
                operation_id: Some(operation_id.clone()),
                tool_call_id: Some(tool_call_id.clone()),
                ..Default::default()
            });
        }
        SessionEvent::ToolResult { tool_call_id, tool_name, content, is_error, .. } => {
            ev.role = Role::Tool;
            ev.author = Author::Tool;
            ev.content = Some(Content::FunctionResponse {
                id: tool_call_id.clone(),
                name: tool_name.clone(),
                result: Value::String(content.clone()),
                is_error: Some(*is_error),
                provider_executed: None,
                provider_output: None,
            });
        }
        SessionEvent::Complete { .. } => {
            ev.status = Some(Status::Completed);
            // status already says Completed; endInvocation marks the closing
            // event. (stopReason is not carried yet — it lands in
            // tokenUsage.rawFinishReason once token accounting is typed.)
            ev.actions = Some(RuntimeEventActions {
                end_invocation: Some(true),
                ..Default::default()
            });
        }
        SessionEvent::Error { message, code, reason, .. } => {
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
        SessionEvent::Abort { .. } => {
            ev.role = Role::System;
            ev.author = Author::System;
            ev.status = Some(Status::Aborted);
            ev.actions = Some(RuntimeEventActions {
                end_invocation: Some(true),
                ..Default::default()
            });
        }
    }
    ev
}

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

fn base(event: &SessionEvent) -> (&str, &str, i64) {
    match event {
        SessionEvent::TextDelta { id, turn_id, ts, .. }
        | SessionEvent::TextComplete { id, turn_id, ts, .. }
        | SessionEvent::ThinkingDelta { id, turn_id, ts, .. }
        | SessionEvent::ThinkingComplete { id, turn_id, ts, .. }
        | SessionEvent::ToolCall { id, turn_id, ts, .. }
        | SessionEvent::ToolResult { id, turn_id, ts, .. }
        | SessionEvent::ToolDispatch { id, turn_id, ts, .. }
        | SessionEvent::Complete { id, turn_id, ts, .. }
        | SessionEvent::Error { id, turn_id, ts, .. }
        | SessionEvent::Abort { id, turn_id, ts, .. } => (id.as_str(), turn_id.as_str(), *ts),
    }
}
