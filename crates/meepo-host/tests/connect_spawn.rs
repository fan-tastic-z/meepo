//! Phase 5 client discovery: connect-or-spawn launches a candidate (an
//! in-process kernel here) and connects once it publishes its registration.

use std::time::Duration;

use meepo_host::client::connect_or_spawn_with;
use meepo_host::{handlers, transport, Dispatcher, HostKernel};
use tempfile::tempdir;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn connect_or_spawn_launches_then_connects() {
    let dir = tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let result = connect_or_spawn_with(&root, Duration::from_secs(5), |r| async move {
        let sock = r.join("h.sock");
        let listener = transport::bind(&sock).unwrap();
        let mut dispatcher = Dispatcher::new();
        handlers::host::register(&mut dispatcher);
        let kernel = HostKernel::new(
            "epoch-cos",
            dispatcher,
            std::sync::Arc::new(meepo_host::SessionContinuityCoordinator::new()),
        );
        tokio::spawn(async move {
            let _ = kernel
                .serve_owned(listener, &r, sock.to_str().unwrap(), Duration::from_secs(30), CancellationToken::new())
                .await;
        });
    })
    .await;

    let (mut client, epoch) = result.expect("connect_or_spawn resolves");
    assert_eq!(epoch, "epoch-cos");
    let status = client.host_status().await.expect("host.status");
    assert_eq!(status["state"], serde_json::json!("ready"));
}
