//! Example: exercise the tool_operations store API end to end.
//!
//! Run: cargo run -p meepo-storage --example tool_ops
//! Then inspect the table directly with sqlite3 (path printed at the end).

use meepo_storage::{SqliteStore, ToolOperation};

#[tokio::main]
async fn main() {
    let path = std::env::temp_dir().join("meepo-tool-ops-example.sqlite");
    let _ = std::fs::remove_file(&path);
    let store = SqliteStore::open(&path).expect("open store");

    // 1. A dispatch fact opened an operation — record it.
    let op = ToolOperation {
        operation_id: "op_demo".into(),
        invocation_id: "inv1".into(),
        run_id: "run1".into(),
        turn_id: "turn1".into(),
        provider_tool_call_id: "call_demo".into(),
        tool_name: "bash".into(),
        canonical_args_hash: "sha256:demo".into(),
        recovery_mode: "replay_safe".into(),
        current_state: "dispatched".into(),
        call_event_id: "e_call".into(),
        result_event_id: None,
        dispatch_event_id: Some("e_dispatch".into()),
        version: 1,
    };
    store.record_tool_operation(&op).await.unwrap();
    println!("recorded: op_demo (state=dispatched, v1)");

    // 2. The tool result lands — upsert advances state + version.
    let mut advanced = op.clone();
    advanced.current_state = "completed".into();
    advanced.result_event_id = Some("e_result".into());
    advanced.version = 2;
    store.record_tool_operation(&advanced).await.unwrap();
    println!("upserted: op_demo (state=completed, v2)");

    // 3. Read it back.
    let read = store.read_tool_operation("op_demo").await.unwrap().unwrap();
    println!(
        "read back: {} tool={} state={} result_event_id={:?} v{}",
        read.operation_id,
        read.tool_name,
        read.current_state,
        read.result_event_id,
        read.version
    );

    // 4. Unknown id reads as None.
    let missing = store.read_tool_operation("op_missing").await.unwrap();
    println!("read missing: {:?}", missing);

    println!("\nInspect the table directly:");
    println!(
        "  sqlite3 -header {} \"SELECT operation_id, tool_name, current_state, version FROM tool_operations\"",
        path.display()
    );
}
