//! Phase 4 integration: boot a HostKernel on a tempdir socket, then a HostClient
//! handshakes and round-trips `host.status`.

use meepo_host::{handlers, transport, Dispatcher, HostClient, HostKernel};
use serde_json::json;

#[tokio::test]
async fn host_status_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("h.sock");
    let listener = transport::bind(&sock).unwrap();

    let mut dispatcher = Dispatcher::new();
    handlers::host::register(&mut dispatcher);
    let kernel = HostKernel::new("epoch-test", dispatcher);
    let serve = tokio::spawn(async move {
        kernel.serve(listener).await;
    });

    let (mut client, host_epoch) = HostClient::connect(&sock).await.expect("connect + handshake");
    assert_eq!(host_epoch, "epoch-test");

    let status = client.host_status().await.expect("host.status");
    assert_eq!(status["hostEpoch"], json!("epoch-test"));
    assert_eq!(status["state"], json!("ready"));

    // An unregistered op resolves to operation_unavailable.
    let unknown = client.request("nope.op", json!({})).await;
    assert!(unknown.is_err(), "unknown op must error, got {unknown:?}");

    drop(client);
    serve.abort();
}
