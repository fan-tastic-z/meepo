//! Wire protocol for the host socket.
//!
//! One JSON object per frame, LF-delimited over a Unix domain socket. Frames
//! are closed-schema (unknown keys are rejected) and `kind`-discriminated,
//! with request/response frames carrying no `kind` and serving as the routing
//! fallback. See [`codec`] for framing, [`envelope`] for frame routing, and
//! [`handshake`] for the connection negotiation.

pub mod codec;
pub mod envelope;
pub mod errors;
pub mod handshake;
pub mod ops;
pub mod versions;

pub use codec::{CodecError, LfCodec};
pub use envelope::{
    decode_client_frame, decode_host_frame, ClientFrame, HostFrame, RequestFrame, ResponseFrame,
};
pub use errors::{OpErrorCode, OperationError, ProtocolError, TransportError};
pub use handshake::{
    negotiate_protocol, validate_protocol_range, AcceptedFrame, DrainingFrame, HandshakeKind,
    HandshakeResult, HandshakeSurface, HelloFrame, IncompatibleFrame, LifecycleState,
};
pub use ops::{is_spine_op, OperationSpec, OpName};
pub use versions::{
    COMPATIBILITY_EPOCH, MAX_FRAME_BYTES, MAX_IN_FLIGHT_DOMAIN_REQUESTS, PROTOCOL_MAX,
    PROTOCOL_MIN, REGISTRATION_SCHEMA_VERSION,
};
