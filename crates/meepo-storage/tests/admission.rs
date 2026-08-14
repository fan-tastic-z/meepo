//! Phase 3: the durable root-turn admission chain (extend / read-tip /
//! recover / poison) and the RuntimeEvent high-water query.

use meepo_storage::{RootAdmissionRecord, SqliteStore};

fn rec(turn: &str, run: &str, prev: Option<&str>, at: i64) -> RootAdmissionRecord {
    RootAdmissionRecord {
        session_id: "s1".into(),
        turn_id: turn.into(),
        run_id: run.into(),
        previous_root_turn_id: prev.map(str::to_string),
        identity_json: "{}".into(),
        admitted_at: at,
        poisoned: false,
    }
}

#[tokio::test]
async fn admission_chain_extend_tip_recover_poison() {
    let store = SqliteStore::in_memory().unwrap();
    store.extend_admission_chain(&rec("turn-1", "run-1", None, 1)).await.unwrap();
    store
        .extend_admission_chain(&rec("turn-2", "run-2", Some("turn-1"), 2))
        .await
        .unwrap();

    // Tip is the most-recently admitted turn.
    let tip = store.read_admission_tip("s1").await.unwrap().unwrap();
    assert_eq!(tip.turn_id, "turn-2");

    // Full chain in admission order, with the chain link intact.
    let chain = store.recover_admission_chain("s1").await.unwrap();
    assert_eq!(chain.len(), 2);
    assert_eq!(chain[0].turn_id, "turn-1");
    assert_eq!(chain[1].previous_root_turn_id.as_deref(), Some("turn-1"));

    // Poisoning flips the whole session's admissions.
    store.mark_admission_poisoned("s1").await.unwrap();
    let tip = store.read_admission_tip("s1").await.unwrap().unwrap();
    assert!(tip.poisoned);

    // Unknown session → None / empty.
    assert!(store.read_admission_tip("nope").await.unwrap().is_none());
    assert!(store.recover_admission_chain("nope").await.unwrap().is_empty());
}

#[tokio::test]
async fn high_water_is_zero_for_an_empty_run() {
    let store = SqliteStore::in_memory().unwrap();
    assert_eq!(store.read_runtime_event_high_water("s1", "run-1").await.unwrap(), 0);
    assert_eq!(store.read_runtime_event_high_water("s1", "run-2").await.unwrap(), 0);
}
