//! Projection: RuntimeEvent ledger -> ChatMessage history.

use meepo_core::{Author, ChatMessage, Content, Role, RuntimeEvent, Status};
use meepo_runtime::messages_from_runtime_events;
use serde_json::json;

fn text(role: Role, author: Author, text: &str) -> RuntimeEvent {
    RuntimeEvent {
        session_id: "s".into(),
        invocation_id: "inv".into(),
        run_id: "r".into(),
        turn_id: "t".into(),
        branch: None,
        id: text.into(),
        ts: 0,
        role,
        author,
        origin: None,
        model_visibility: None,
        status: None,
        content: Some(Content::Text {
            text: text.into(),
            provider_options: None,
            steering: None,
        }),
        actions: None,
        refs: None,
        partial: None,
    }
}

fn call(id: &str, name: &str, args: serde_json::Value) -> RuntimeEvent {
    RuntimeEvent {
        content: Some(Content::FunctionCall {
            id: id.into(),
            name: name.into(),
            args,
            provider_options: None,
            provider_executed: None,
        }),
        role: Role::Model,
        ..text(Role::Model, Author::Agent, id)
    }
}

fn response(id: &str, result: &str) -> RuntimeEvent {
    RuntimeEvent {
        content: Some(Content::FunctionResponse {
            id: id.into(),
            name: "read_file".into(),
            result: serde_json::Value::String(result.into()),
            is_error: None,
            provider_executed: None,
            provider_output: None,
        }),
        role: Role::Tool,
        author: Author::Tool,
        ..text(Role::Tool, Author::Tool, id)
    }
}

#[test]
fn collapses_user_assistant_pairs() {
    let events = vec![
        text(Role::User, Author::User, "hello"),
        text(Role::Model, Author::Agent, "hi there"),
    ];
    let msgs = messages_from_runtime_events(&events);
    assert_eq!(msgs.len(), 2);
    assert!(matches!(msgs[0], ChatMessage::User { .. }));
    assert!(matches!(msgs[1], ChatMessage::Assistant { .. }));
}

#[test]
fn merges_model_text_and_tool_call_into_one_assistant() {
    let events = vec![
        text(Role::User, Author::User, "read it"),
        text(Role::Model, Author::Agent, "sure"),
        call("c1", "read_file", json!({"path": "/x"})),
        response("c1", "contents"),
        text(Role::Model, Author::Agent, "done"),
    ];
    let msgs = messages_from_runtime_events(&events);
    // user, assistant(text+tool_call), tool, assistant(text)
    assert_eq!(msgs.len(), 4);
    assert!(matches!(msgs[1], ChatMessage::Assistant { ref tool_calls, .. } if tool_calls.len() == 1));
    assert!(matches!(msgs[2], ChatMessage::Tool { .. }));
    assert!(matches!(msgs[3], ChatMessage::Assistant { .. }));
}

#[test]
fn ignores_terminal_and_non_content_events() {
    let mut term = text(Role::Model, Author::System, "term");
    term.content = None;
    term.status = Some(Status::Completed);
    let events = vec![text(Role::User, Author::User, "hi"), term];
    let msgs = messages_from_runtime_events(&events);
    assert_eq!(msgs.len(), 1, "terminal event has no text content -> dropped");
}
