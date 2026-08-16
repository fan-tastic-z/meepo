//! End-to-end: spawn the real `meepo-host` binary (fake provider), wait for
//! its registration, connect a client, and drive a full chat turn through the
//! daemon — the same path `meepo chat` now takes.

use std::process::{Command, Stdio};
use std::time::Duration;

use meepo_host::server::read_registration;
use serde_json::json;

#[tokio::test]
async fn real_daemon_serves_a_chat_turn() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();

    let mut child = Command::new(env!("CARGO_BIN_EXE_meepo-host"))
        .args([
            "--root",
            root.to_str().unwrap(),
            "--provider",
            "fake",
            "--idle-grace-ms",
            "60000",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn meepo-host");

    // Wait for the daemon to win the flock and publish its registration.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while read_registration(&root).is_none() {
        assert!(std::time::Instant::now() < deadline, "daemon never registered");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // connect_or_spawn takes the fast path (an owner is registered).
    let (mut client, host_epoch) = meepo_host::client::connect_or_spawn(&root)
        .await
        .expect("connect to the daemon");
    assert!(!host_epoch.is_empty());

    client
        .request("session.create", json!({"sessionId": "s1"}))
        .await
        .expect("session.create");
    client
        .request("subscription.open", json!({"sessionId": "s1"}))
        .await
        .expect("subscription.open");
    let started = client
        .request("turn.start", json!({"sessionId": "s1", "content": "hello"}))
        .await
        .expect("turn.start");
    assert_eq!(started["turnId"], json!("turn-1"));

    // The daemon streams the fake backend's delta + terminal projection.
    let mut saw_delta = false;
    let mut saw_completed = false;
    loop {
        for frame in client.take_streamed() {
            inspect(&frame, &mut saw_delta, &mut saw_completed);
        }
        if saw_delta && saw_completed {
            break;
        }
        match tokio::time::timeout(Duration::from_secs(3), client.next_streamed()).await {
            Ok(Some(frame)) => inspect(&frame, &mut saw_delta, &mut saw_completed),
            _ => break,
        }
    }
    assert!(saw_delta, "daemon must stream the text delta");
    assert!(saw_completed, "daemon must stream the terminal projection");

    drop(client);
    let _ = child.kill();
    let _ = child.wait();
}

fn inspect(frame: &serde_json::Value, saw_delta: &mut bool, saw_completed: &mut bool) {
    match frame["kind"].as_str() {
        Some("subscription.session_delta") => {
            *saw_delta = true;
            assert_eq!(frame["text"], json!("meepo (fake backend)"));
        }
        Some("subscription.session_projection") => {
            if frame["snapshot"]["rootTurn"]["status"] == json!("completed") {
                *saw_completed = true;
            }
        }
        _ => {}
    }
}
