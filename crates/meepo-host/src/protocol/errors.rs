//! Three-layer error model.
//!
//! - [`ProtocolError`]: framing/decoding faults — fatal, the connection is
//!   torn down.
//! - [`TransportError`]: local connection-state faults — never serialized.
//! - [`OpErrorCode`] / [`OperationError`]: operation-level failures carried
//!   inside an `ok:false` response envelope as `{ code, message }`. Each op
//!   declares the closed subset it may return; the dispatcher rejects codes
//!   an op did not declare.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Fatal framing/decoding fault. Any variant tears the connection down.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProtocolError {
    #[error("invalid frame")]
    InvalidFrame,
    #[error("frame too large")]
    FrameTooLarge,
    #[error("invalid utf-8")]
    InvalidUtf8,
    #[error("invalid json: {0}")]
    InvalidJson(String),
}

/// Local transport-state fault. Never serialized across the wire.
#[derive(Debug, Error)]
pub enum TransportError {
    #[error("connection closed")]
    Closed,
    #[error("read eof")]
    ReadEof,
    #[error("write zero")]
    WriteZero,
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// Operation-level error code, serialized as `snake_case` wire strings. Every
/// operation must be able to return [`OpErrorCode::InternalFailure`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpErrorCode {
    HostNotReady,
    HostDraining,
    OperationUnavailable,
    NotFound,
    SessionArchived,
    SessionBusy,
    OperationConflict,
    CapabilityUnavailable,
    InvalidRequest,
    ProjectionIncomplete,
    PersistenceFailed,
    CommitOutcomeUnknown,
    AlreadyResolved,
    OutcomeUnknown,
    InternalFailure,
    RevisionConflict,
}

/// Operation error payload carried in an `ok:false` response: `{ code, message }`.
/// `message` is capped at [`MAX_ERROR_MESSAGE_CHARS`] on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationError {
    pub code: OpErrorCode,
    pub message: String,
}

/// Maximum chars in an operation error `message`.
pub const MAX_ERROR_MESSAGE_CHARS: usize = 1024;

impl OperationError {
    pub fn new(code: OpErrorCode, message: impl Into<String>) -> Self {
        let mut message = message.into();
        if message.chars().count() > MAX_ERROR_MESSAGE_CHARS {
            let end: String = message.chars().take(MAX_ERROR_MESSAGE_CHARS).collect();
            message = end;
        }
        Self { code, message }
    }
}
