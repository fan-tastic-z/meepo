//! Meepo core — the pure contract layer.
//!
//! Types, codecs, and port traits only. No I/O, no async, no storage. Every
//! other crate depends on this one.
//!
//! Design principle: **log-first, projection-driven**. The canonical fact is
//! [`RuntimeEvent`] (append-only); sessions, model context, the UI stream,
//! crash recovery, and graph scheduling are all projections over a sequence of
//! these events — never independent sources of truth.
//!
//! Phase 0 scope: the [`RuntimeEvent`] envelope, its identity quintet, the
//! role/author/status enums, the content union, and byte-exact canonical
//! serialization. `actions`, `refs`, and `partial` are opaque JSON for now and
//! gain typed shapes in later phases.

pub mod runtime_event;

pub use runtime_event::{
    Author, Content, ModelVisibility, Origin, Role, RuntimeEvent, Status,
};
