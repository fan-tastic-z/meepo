//! Meepo core — the pure contract layer.

pub mod backend;
pub mod interaction;
pub mod permission;
pub mod recovery_fact;
pub mod runtime_event;
pub mod session_event;
pub mod store;
pub mod tool_ledger_scanner;
pub mod tool_operation;

pub use backend::{
    AgentBackend, AssistantToolCall, BackendKind, BackendResult, BackendSendInput,
    BackendStopMode, BackendStopReason, ChatMessage, FakeBackend, StopToken, ThinkingBlock,
    ToolExecutor,
};
pub use permission::{
    builtin_tool_category, categorize_bash, classify_tool_use, policy_decision, PermissionMode,
    PolicyDecision, ToolCategory,
};
pub use interaction::{
    DefaultPermissionGate, InteractionStore, PermissionAnswer, PermissionDecision, PermissionGate,
    PermissionOutcome, PermissionPrompter, PermissionReason, PermissionRequest, PermissionResolution,
    PermissionVerdict,
};
pub use runtime_event::{
    Author, Content, ModelVisibility, Origin, PermissionDecisionAction, ProtocolMarker, Role,
    RuntimeEvent, RuntimeEventActions, RuntimeEventRefs, Status, TOOL_BOUNDARY_PROTOCOL_V1,
    ToolDispatch, ToolRecoveryMode, canonical_tool_args_hash, operation_id,
};
pub use recovery_fact::{
    ToolReconcileObservation, ToolReconcileResultFact, ToolRecoveryDecisionFact,
    ToolRecoveryDisposition, ToolRecoveryFactEnvelope, TOOL_RECONCILE_RESULT_FACT_KIND,
    TOOL_RECOVERY_DECISION_FACT_KIND,
};
pub use session_event::{AbortReason, SessionEvent, StopReason};
pub use store::{Durability, InMemoryRuntimeEventStore, RuntimeEventStore, StoreResult};
pub use tool_ledger_scanner::{
    scan_tool_ledger, ToolLedgerIssue, ToolLedgerIssueCode, ToolLedgerLane,
    ToolLedgerScanOperation, ToolLedgerScanResult,
};
pub use tool_operation::{ToolOperation, ToolOperationStore};
