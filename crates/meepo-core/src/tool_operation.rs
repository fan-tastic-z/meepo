//! Tool operation — the durable side-effect boundary a T1 dispatch fact opens.
//!
//! One operation per tool call; its `current_state` advances `dispatched` →
//! `completed` as the runtime persists the call, the dispatch fact, and the
//! result. The recovery continuation (Phase 3-4) reads these rows to decide
//! whether to re-execute, reconcile, or park an interrupted operation.

use async_trait::async_trait;

use crate::store::StoreResult;

/// One durable tool side-effect boundary (a row in `tool_operations`).
#[derive(Debug, Clone, PartialEq)]
pub struct ToolOperation {
    pub operation_id: String,
    pub invocation_id: String,
    pub run_id: String,
    pub turn_id: String,
    pub provider_tool_call_id: String,
    pub tool_name: String,
    pub canonical_args_hash: String,
    pub recovery_mode: String,
    pub current_state: String,
    pub call_event_id: String,
    pub result_event_id: Option<String>,
    pub dispatch_event_id: Option<String>,
    pub version: i64,
}

/// Persists tool-operation rows. The runtime writes one row when it dispatches
/// a tool call and upserts it when the result lands.
#[async_trait]
pub trait ToolOperationStore: Send + Sync {
    /// Upsert one tool operation row (`operation_id` is the primary key).
    async fn record_tool_operation(&self, op: &ToolOperation) -> StoreResult<()>;
    /// Read one tool operation by its operation id.
    async fn read_tool_operation(
        &self,
        operation_id: &str,
    ) -> StoreResult<Option<ToolOperation>>;
}
