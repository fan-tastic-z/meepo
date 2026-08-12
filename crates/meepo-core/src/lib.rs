//! Meepo core — the pure contract layer.

pub mod backend;
pub mod interaction;
pub mod permission;
pub mod runtime_event;
pub mod session_event;
pub mod store;
pub mod tool_ledger_scanner;

pub use backend::{
    AgentBackend, AssistantToolCall, BackendKind, BackendResult, BackendSendInput,
    BackendStopMode, BackendStopReason, ChatMessage, FakeBackend, ThinkingBlock, ToolExecutor,
};
pub use permission::{
    builtin_tool_category, categorize_bash, classify_tool_use, policy_decision, PermissionMode,
    PolicyDecision, ToolCategory,
};
pub use interaction::{
    DefaultPermissionGate, PermissionAnswer, PermissionDecision, PermissionGate, PermissionOutcome,
    PermissionPrompter, PermissionReason, PermissionRequest, PermissionVerdict,
};
pub use runtime_event::{
    Author, Content, ModelVisibility, Origin, Role, RuntimeEvent, Status,
};
pub use session_event::{AbortReason, SessionEvent, StopReason};
pub use store::{Durability, InMemoryRuntimeEventStore, RuntimeEventStore, StoreResult};
pub use tool_ledger_scanner::{
    scan_tool_ledger, ToolLedgerIssue, ToolLedgerIssueCode, ToolLedgerLane,
    ToolLedgerScanOperation, ToolLedgerScanResult,
};
