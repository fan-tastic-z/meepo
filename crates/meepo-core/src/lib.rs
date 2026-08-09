//! Meepo core — the pure contract layer.

pub mod backend;
pub mod runtime_event;
pub mod session_event;
pub mod store;

pub use backend::{
    AgentBackend, AssistantToolCall, BackendKind, BackendResult, BackendSendInput,
    BackendStopMode, BackendStopReason, ChatMessage, FakeBackend,
};
pub use runtime_event::{
    Author, Content, ModelVisibility, Origin, Role, RuntimeEvent, Status,
};
pub use session_event::{AbortReason, SessionEvent, StopReason};
pub use store::{Durability, InMemoryRuntimeEventStore, RuntimeEventStore, StoreResult};
