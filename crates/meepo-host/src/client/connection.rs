//! Host client: connect to a listening socket, complete the `hello`/
//! `accepted` handshake, then drive typed request/response turns.

use std::path::Path;

use futures::sink::SinkExt;
use futures::StreamExt;
use serde_json::Value;

use crate::protocol::{
    decode_host_frame, AcceptedFrame, HandshakeSurface, HostFrame, HelloFrame, ResponseFrame,
    COMPATIBILITY_EPOCH, PROTOCOL_MAX, PROTOCOL_MIN,
};
use crate::transport::{self, FrameRead, FrameWrite};

/// Errors from the client side: transport io, framing/decode faults, or an
/// operation-level `{code, message}` returned by the host.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("host closed the connection")]
    Closed,
    #[error("framing fault: {0:?}")]
    Framing(#[from] crate::protocol::CodecError),
    #[error("protocol fault: {0}")]
    Protocol(#[from] crate::protocol::ProtocolError),
    #[error("handshake rejected: {0}")]
    Handshake(String),
    #[error("operation {operation} failed ({code:?}): {message}")]
    Operation {
        operation: String,
        code: crate::protocol::OpErrorCode,
        message: String,
    },
}

pub struct HostClient {
    write: FrameWrite,
    read: FrameRead,
    /// Subscription frames skipped while awaiting responses.
    streamed: Vec<Value>,
}

impl HostClient {
    /// Connect to `path`, handshake, and return the client + the host epoch.
    pub async fn connect(path: impl AsRef<Path>) -> Result<(Self, String), ClientError> {
        let mut conn = transport::connect(path).await?;
        let hello = HelloFrame::new(
            uuid::Uuid::new_v4().to_string(),
            HandshakeSurface::Run,
            PROTOCOL_MIN,
            PROTOCOL_MAX,
            COMPATIBILITY_EPOCH,
        );
        let hello_value = serde_json::to_value(&hello).expect("hello serializes");
        conn.write.send(hello_value).await?;

        let frame = match conn.read.next().await {
            Some(Ok(v)) => v,
            Some(Err(e)) => return Err(ClientError::Framing(e)),
            None => return Err(ClientError::Closed),
        };
        let host_epoch = match decode_host_frame(frame)? {
            HostFrame::Accepted(AcceptedFrame { host_epoch, .. }) => host_epoch,
            HostFrame::Draining(_) => return Err(ClientError::Handshake("host draining".into())),
            HostFrame::Incompatible(_) => {
                return Err(ClientError::Handshake("protocol incompatible".into()))
            }
            _ => return Err(ClientError::Handshake("unexpected handshake reply".into())),
        };
        Ok((Self { write: conn.write, read: conn.read, streamed: Vec::new() }, host_epoch))
    }

    /// Send one request and await its response (skipping non-response frames
    /// such as subscription pushes until the matching response arrives).
    pub async fn request(&mut self, operation: &str, input: Value) -> Result<Value, ClientError> {
        let request_id = uuid::Uuid::new_v4().to_string();
        let req = crate::protocol::RequestFrame {
            request_id: request_id.clone(),
            operation: operation.to_string(),
            input,
        };
        let req_value = serde_json::to_value(&req).expect("request serializes");
        self.write.send(req_value).await?;

        loop {
            let frame = match self.read.next().await {
                Some(Ok(v)) => v,
                Some(Err(e)) => return Err(ClientError::Framing(e)),
                None => return Err(ClientError::Closed),
            };
            let decoded = match decode_host_frame(frame) {
                Ok(d) => d,
                Err(e) => return Err(e.into()),
            };
            match decoded {
                HostFrame::Response(ResponseFrame { request_id: rid, ok, result, error, .. }) => {
                    if rid == request_id {
                        return if ok {
                            result.ok_or_else(|| {
                                ClientError::Handshake("ok response without result".into())
                            })
                        } else {
                            let err = error.unwrap_or_else(|| {
                                crate::protocol::OperationError::new(
                                    crate::protocol::OpErrorCode::InternalFailure,
                                    "no error payload",
                                )
                            });
                            Err(ClientError::Operation {
                                operation: operation.to_string(),
                                code: err.code,
                                message: err.message,
                            })
                        };
                    }
                }
                HostFrame::Subscription(sub) => self.streamed.push(sub),
                _ => {}
            }
        }
    }

    /// Drain subscription frames collected while awaiting responses.
    pub fn take_streamed(&mut self) -> Vec<Value> {
        std::mem::take(&mut self.streamed)
    }

    /// Read the next streamed (subscription) frame from the host, skipping any
    /// interleaved responses. Returns None when the connection closes.
    pub async fn next_streamed(&mut self) -> Option<Value> {
        loop {
            match self.read.next().await? {
                Ok(v) => {
                    if let Ok(HostFrame::Subscription(sub)) = decode_host_frame(v) {
                        return Some(sub);
                    }
                    // skip responses / other frames
                }
                Err(_) => return None,
            }
        }
    }

    /// Convenience: `host.status` → the lifecycle state + epoch object.
    pub async fn host_status(&mut self) -> Result<Value, ClientError> {
        self.request("host.status", serde_json::json!({})).await
    }
}
