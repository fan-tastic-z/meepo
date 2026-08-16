//! Turn coordination — admits turns durably and drains them into continuity.

pub mod coordinator;

pub use coordinator::{TurnCoordinator, TurnError, TurnStarted};
