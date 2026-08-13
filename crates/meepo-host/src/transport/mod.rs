//! Framed transport and bounded outbound writer.

pub mod framed;
pub mod outbound;

pub use framed::{bind, connect, FramedConn, FrameRead, FrameWrite};
pub use outbound::{
    BoundedSerialOutboundWriter, OutboundQueueError, MAX_QUEUED_BYTES, MAX_QUEUED_FRAMES,
};
