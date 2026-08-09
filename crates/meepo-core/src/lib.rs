//! Meepo core — the pure contract layer.
//!
//! Types, codecs, and port traits only. No I/O beyond in-memory test doubles,
//! no runtime. Every other crate depends on this one.
//!
//! Design principle: **log-first, projection-driven**. The canonical fact is
//! [`RuntimeEvent`] (append-only); sessions, model context, the UI stream,
//! crash recovery, and graph scheduling are all projections over a sequence of
//! these events — never independent sources of truth.
//!
//! Phase 0 scope so far: the [`RuntimeEvent`] envelope (identity quintet,
//! role/author/status, content union, canonical serialization) and the
//! [`RuntimeEventStore`] ledger port with an in-memory implementation.

pub mod runtime_event;
pub mod store;

pub use runtime_event::{
    Author, Content, ModelVisibility, Origin, Role, RuntimeEvent, Status,
};
pub use store::{
    Durability, InMemoryRuntimeEventStore, RuntimeEventStore, StoreResult,
};
