//! Phase 5 lifecycle: the owned host acquires the flock, publishes
//! registration, self-shuts-down when truly idle, and loses to an existing
//! owner.

use std::time::Duration;

use meepo_host::server::{Ownership, ServeOutcome};
use meepo_host::{handlers, transport, Dispatcher, HostKernel};
use tempfile::tempdir;
use tokio_util::sync::CancellationToken;

fn kernel() -> HostKernel {
    let mut dispatcher = Dispatcher::new();
    handlers::host::register(&mut dispatcher);
    HostKernel::new(
        uuid::Uuid::new_v4().to_string(),
        dispatcher,
        std::sync::Arc::new(meepo_host::SessionContinuityCoordinator::new()),
    )
}

#[tokio::test]
async fn idle_kernel_self_shuts_down() {
    let dir = tempdir().unwrap();
    let sock = dir.path().join("h.sock");
    let listener = transport::bind(&sock).unwrap();
    let kernel = kernel();
    let outcome = tokio::time::timeout(
        Duration::from_secs(3),
        kernel.serve_owned(listener, dir.path(), sock.to_str().unwrap(), Duration::from_millis(100), CancellationToken::new()),
    )
    .await
    .expect("serve_owned did not return in time (idle shutdown failed)")
    .expect("serve_owned io error");
    assert_eq!(outcome, ServeOutcome::Done);
    // Cleanup removed the registration.
    assert!(
        meepo_host::server::read_registration(dir.path()).is_none(),
        "registration must be removed on clean shutdown"
    );
}

#[tokio::test]
async fn serve_owned_loses_to_existing_owner() {
    let dir = tempdir().unwrap();
    let _owner = Ownership::try_acquire(dir.path()).unwrap().expect("pre-acquire");
    let sock = dir.path().join("h.sock");
    let listener = transport::bind(&sock).unwrap();
    let kernel = kernel();
    let outcome = kernel
        .serve_owned(listener, dir.path(), sock.to_str().unwrap(), Duration::from_secs(30), CancellationToken::new())
        .await
        .expect("serve_owned io error");
    assert_eq!(outcome, ServeOutcome::Loser);
}

#[tokio::test]
async fn owned_host_serves_a_client() {
    // With a live connection the host must NOT idle-shutdown; after the
    // client drops it does, then cleanup runs.
    let dir = tempdir().unwrap();
    let sock = dir.path().join("h.sock");
    let listener = transport::bind(&sock).unwrap();
    let kernel = kernel();
    let sock_str = sock.to_str().unwrap().to_string();
    let serve = tokio::spawn(async move {
        kernel
            .serve_owned(listener, dir.path(), &sock_str, Duration::from_millis(100), CancellationToken::new())
            .await
            .unwrap()
    });

    // Keep a connection open: the host stays up past the idle grace.
    let (mut client, _) = meepo_host::HostClient::connect(&sock).await.unwrap();
    let status = client.host_status().await.unwrap();
    assert_eq!(status["state"], serde_json::json!("ready"));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!serve.is_finished(), "host must stay up while a connection is open");

    drop(client);
    // After the client leaves, the host idles out and shuts down.
    let outcome = tokio::time::timeout(Duration::from_secs(3), serve)
        .await
        .expect("host did not idle-shutdown after client dropped")
        .unwrap();
    assert_eq!(outcome, ServeOutcome::Done);
}
