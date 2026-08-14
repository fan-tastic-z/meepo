//! Session continuity — the streaming projection layer.
//!
//! A subscription (`subscription.open`) returns the canonical
//! [`SessionContinuitySnapshot`] plus a monotonic sequence, then the host
//! pushes `subscription.*` frames as the session's runtime events arrive.
//! Streaming is keyed by `subscriptionId` + `sequence` + `hostEpoch`, NOT by
//! the originating request. (Phase 6 wires the coordinator + frame types;
//! phase 7 connects it to a live turn.)

pub mod coordinator;
pub mod frames;
pub mod snapshot;

pub use coordinator::{OpenedSubscription, SessionContinuityCoordinator};
pub use frames::{ClosedReason, DeltaKind, SubscriptionFrame};
pub use snapshot::{MessageQueueProjection, SessionContinuitySnapshot, MAX_SNAPSHOT_BYTES};
