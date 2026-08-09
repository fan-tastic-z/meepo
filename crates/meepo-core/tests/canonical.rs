//! Canonical serialization round-trip + canonical-form invariants.

use meepo_core::{Author, Content, Role, RuntimeEvent, Status};
use serde_json::Value;

fn sample_user_event() -> RuntimeEvent {
    RuntimeEvent {
        session_id: "s1".into(),
        invocation_id: "inv1".into(),
        run_id: "r1".into(),
        turn_id: "t1".into(),
        branch: None,
        id: "e1".into(),
        ts: 1_700_000_000_000,
        role: Role::User,
        author: Author::User,
        origin: None,
        model_visibility: None,
        status: None,
        content: Some(Content::Text {
            text: "hello meepo".into(),
            provider_options: None,
            steering: None,
        }),
        actions: None,
        refs: None,
        partial: None,
    }
}

#[test]
fn roundtrips_through_canonical_json() {
    let ev = sample_user_event();
    let json = ev.to_canonical_json().unwrap();
    let back: RuntimeEvent = serde_json::from_str(&json).unwrap();
    assert_eq!(ev, back);
}

#[test]
fn field_names_are_camelcase() {
    // Wire fields are camelCase; Rust snake_case names must NOT leak.
    let json = sample_user_event().to_canonical_json().unwrap();
    assert!(json.contains(r#""sessionId""#), "expected camelCase: {json}");
    assert!(json.contains(r#""invocationId""#));
    assert!(json.contains(r#""runId""#));
    assert!(json.contains(r#""turnId""#));
    assert!(!json.contains(r#""session_id""#));
    assert!(!json.contains(r#""run_id""#));
}

#[test]
fn object_keys_are_alphabetical() {
    // BTreeMap gives alphabetical ordering — the stable canonical form.
    let json = sample_user_event().to_canonical_json().unwrap();
    let obj: Value = serde_json::from_str(&json).unwrap();
    let keys: Vec<&str> = obj.as_object().unwrap().keys().map(|s| s.as_str()).collect();
    let mut sorted = keys.clone();
    sorted.sort_unstable();
    assert_eq!(keys, sorted, "keys must be alphabetical: {json}");
}

#[test]
fn absent_optional_fields_are_omitted() {
    let json = sample_user_event().to_canonical_json().unwrap();
    // Absent Options are dropped entirely, not serialized as null.
    assert!(!json.contains(r#""branch""#));
    assert!(!json.contains(r#""status""#));
    assert!(!json.contains(r#""actions""#));
    assert!(!json.contains(r#""origin""#));
}

#[test]
fn content_function_call_uses_snake_case_kind() {
    let ev = RuntimeEvent {
        role: Role::Model,
        author: Author::Agent,
        content: Some(Content::FunctionCall {
            id: "call_1".into(),
            name: "read_file".into(),
            args: serde_json::json!({"path": "/tmp/x"}),
            provider_options: None,
            provider_executed: None,
        }),
        ..sample_user_event()
    };
    let json = ev.to_canonical_json().unwrap();
    assert!(json.contains(r#""kind":"function_call""#), "{json}");
    // camelCase field on a snake_case-tagged variant:
    assert!(!json.contains(r#""providerExecuted""#));
}

#[test]
fn terminal_statuses_detected() {
    assert!(Status::Completed.is_terminal());
    assert!(Status::Failed.is_terminal());
    assert!(Status::Aborted.is_terminal());
    assert!(!Status::Streaming.is_terminal());
}
