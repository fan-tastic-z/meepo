//! Per-connection serving loop: handshake then request/response dispatch.
//!
//! The first client frame must be `hello`; the host replies with exactly one
//! handshake result (`accepted` | `incompatible` | `draining`). On `accepted`,
//! subsequent client frames are requests dispatched through [`Dispatcher`];
//! each yields one response frame, written FIFO through the bounded outbound
//! writer.

use std::sync::Arc;

use futures::sink::SinkExt;
use futures::StreamExt;
use serde::Serialize;
use serde_json::Value;

use crate::protocol::{
    decode_client_frame, negotiate_protocol, AcceptedFrame, ClientFrame, COMPATIBILITY_EPOCH,
    DrainingFrame, IncompatibleFrame, LifecycleState, PROTOCOL_MAX, PROTOCOL_MIN,
};
use crate::protocol::handshake::{HandshakeKind, ReplacementPolicy};
use crate::protocol::{OperationError, ResponseFrame};
use crate::server::dispatcher::{Dispatcher, OpContext, Outcome};
use crate::transport::{BoundedSerialOutboundWriter, FramedConn};

fn frame_value<T: Serialize>(frame: &T) -> Value {
    serde_json::to_value(frame).expect("frame serializes")
}

/// Serve one accepted connection to completion (until the client disconnects).
pub async fn serve_connection(mut conn: FramedConn, ctx: OpContext, dispatcher: Arc<Dispatcher>) {
    // ── Handshake ──
    let first = match conn.read.next().await {
        Some(Ok(v)) => v,
        _ => return, // disconnected or framing fault
    };
    let hello = match decode_client_frame(first) {
        Ok(ClientFrame::Hello(h)) => h,
        _ => return, // first frame must be hello
    };

    let selected = match negotiate_protocol(
        hello.protocol_min,
        hello.protocol_max,
        PROTOCOL_MIN,
        PROTOCOL_MAX,
    ) {
        Ok(p) => p,
        Err(_) => {
            let inc = IncompatibleFrame {
                kind: HandshakeKind::Incompatible,
                host_epoch: ctx.host_epoch.clone(),
                protocol_min: PROTOCOL_MIN,
                protocol_max: PROTOCOL_MAX,
                compatibility_epoch: COMPATIBILITY_EPOCH,
                state: ctx.lifecycle,
                replacement: ReplacementPolicy::BlockedByResidency,
            };
            let _ = conn.write.send(frame_value(&inc)).await;
            return;
        }
    };

    if ctx.lifecycle == LifecycleState::Draining {
        let _ = conn.write.send(frame_value(&DrainingFrame::new(&ctx.host_epoch))).await;
        return;
    }

    let connection_id = uuid::Uuid::new_v4().to_string();
    let accepted = AcceptedFrame::new(
        &ctx.host_epoch,
        &connection_id,
        selected,
        COMPATIBILITY_EPOCH,
        ctx.lifecycle,
    );
    if conn.write.send(frame_value(&accepted)).await.is_err() {
        return;
    }

    // ── Request loop ──
    let outbound = Arc::new(BoundedSerialOutboundWriter::new(conn.write));
    while let Some(frame_result) = conn.read.next().await {
        let v = match frame_result {
            Ok(v) => v,
            Err(_) => break, // framing fault: drop the connection
        };
        let req = match decode_client_frame(v) {
            Ok(ClientFrame::Request(r)) => r,
            _ => continue, // a second hello or stray frame: ignore
        };
        let outcome = dispatcher.dispatch(&req.operation, req.input, ctx.clone()).await;
        let response = match outcome {
            Outcome::Ok(val) => ResponseFrame::ok(&req.request_id, &req.operation, val),
            Outcome::Err { code, message } => {
                ResponseFrame::err(&req.request_id, &req.operation, OperationError::new(code, message))
            }
        };
        if outbound.enqueue(frame_value(&response)).await.is_err() {
            break; // outbound closed (slow consumer / sink gone)
        }
    }
    outbound.close().await;
}
