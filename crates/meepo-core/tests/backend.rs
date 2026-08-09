//! AgentBackend / FakeBackend stream behavior.

use futures::stream::StreamExt;

use meepo_core::{
    AgentBackend, BackendKind, BackendSendInput, ChatMessage, FakeBackend, SessionEvent, StopReason,
};

fn input() -> BackendSendInput {
    BackendSendInput {
        turn_id: "t1".into(),
        messages: vec![ChatMessage::User { content: "hi".into() }],
        run_id: Some("r1".into()),
        invocation_id: Some("inv1".into()),
        max_steps: None,
    }
}

#[tokio::test]
async fn fake_backend_replays_script_in_order() {
    let script = vec![
        SessionEvent::TextDelta {
            id: "1".into(),
            turn_id: "t1".into(),
            ts: 0,
            message_id: "m".into(),
            start_offset: None,
            text: "hel".into(),
        },
        SessionEvent::TextDelta {
            id: "2".into(),
            turn_id: "t1".into(),
            ts: 1,
            message_id: "m".into(),
            start_offset: None,
            text: "lo".into(),
        },
        SessionEvent::Complete {
            id: "3".into(),
            turn_id: "t1".into(),
            ts: 2,
            stop_reason: StopReason::EndTurn,
        },
    ];
    let mut backend = FakeBackend::new("s1", script);
    assert_eq!(backend.kind(), BackendKind::Fake);
    assert_eq!(backend.session_id(), "s1");

    let inp = input();
    let mut stream = backend.send(&inp);
    let mut got = Vec::new();
    while let Some(ev) = stream.next().await {
        got.push(ev);
    }
    assert_eq!(got.len(), 3);
    assert!(matches!(got.last(), Some(SessionEvent::Complete { .. })));
}

#[tokio::test]
async fn session_event_text_delta_roundtrips_with_snake_case_type() {
    let ev = SessionEvent::TextDelta {
        id: "1".into(),
        turn_id: "t1".into(),
        ts: 0,
        message_id: "m".into(),
        start_offset: None,
        text: "hi".into(),
    };
    let json = serde_json::to_string(&ev).unwrap();
    assert!(json.contains(r#""type":"text_delta""#), "{json}");
    assert!(json.contains(r#""messageId""#), "camelCase field: {json}");
    assert!(!json.contains(r#""startOffset""#), "absent Option omitted: {json}");
    let back: SessionEvent = serde_json::from_str(&json).unwrap();
    assert_eq!(ev, back);
}

#[tokio::test]
async fn stop_and_dispose_are_ok_on_fake() {
    let mut backend = FakeBackend::new("s1", vec![]);
    backend
        .stop(meepo_core::BackendStopReason::UserStop, None)
        .await
        .unwrap();
    backend.dispose().await.unwrap();
}
