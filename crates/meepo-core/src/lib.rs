//! Meepo core — the pure contract layer.
//!
//! Types, codecs, and port traits only, plus in-memory test doubles. No
//! runtime, no real I/O. Every other crate depends on this one.
//!
//! Design principle: **log-first, projection-driven**. The canonical fact is
//! [`RuntimeEvent`] (append-only); sessions, model context, the UI stream,
//! crash recovery, and graph scheduling are all projections over a sequence of
//! these events — never independent sources of truth.
//!
//! Phase 0 scope: the [`RuntimeEvent`] envelope (identity quintet, role/author/
//! status, content union, canonical serialization), the [`RuntimeEventStore`]
//! ledger port with an in-memory implementation, and the [`AgentBackend`]
//! execution port with a [`FakeBackend`] double and the walking-skeleton
//! [`SessionEvent`] stream vocabulary.

pub mod backend;
pub mod runtime_event;
pub mod session_event;
pub mod store;

pub use backend::{
    AgentBackend, BackendKind, BackendResult, BackendSendInput, BackendStopMode,
    BackendStopReason, FakeBackend,
};
pub use runtime_event::{
    Author, Content, ModelVisibility, Origin, Role, RuntimeEvent, Status,
};
pub use session_event::{AbortReason, SessionEvent, StopReason};
pub use store::{
    Durability, InMemoryRuntimeEventStore, RuntimeEventStore, StoreResult,
};
