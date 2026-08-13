//! Frame envelopes and `kind`-routing.
//!
//! The socket carries two frame vocabularies:
//! - [`ClientFrame`]: `hello` | `request` (client-capability is deferred).
//! - [`HostFrame`]:   `accepted` | `incompatible` | `draining` | `response` |
//!                    `subscription.*` | `configuration.changed` |
//!                    `session.catalog.changed`.
//!
//! Discrimination is by a `kind` string field when present; `request`/
//! `response` frames carry no `kind` and are the fallback. Each concrete frame
//! is a closed schema (`deny_unknown_fields`); the router reads `kind`, then
//! parses the [`serde_json::Value`] into the specific struct. The router
//! itself is intentionally not `deny_unknown_fields` (it must read `kind`).
//!
//! `serde_json` built without `preserve_order` yields alphabetical object keys
//! on the wire; load-bearing digests elsewhere canonicalize explicitly.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::errors::{OperationError, ProtocolError};
use super::handshake::{AcceptedFrame, DrainingFrame, HandshakeKind, IncompatibleFrame};

/// Client→host request frame. Closed schema; `input` is op-specific JSON.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RequestFrame {
    pub request_id: String,
    pub operation: String,
    pub input: Value,
}

/// Host→client response frame. Closed schema; exactly one of `result`/`error`
/// is present depending on `ok`. Validate with [`ResponseFrame::validate`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ResponseFrame {
    pub request_id: String,
    pub operation: String,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<OperationError>,
}

impl ResponseFrame {
    pub fn ok(request_id: impl Into<String>, operation: impl Into<String>, result: Value) -> Self {
        Self {
            request_id: request_id.into(),
            operation: operation.into(),
            ok: true,
            result: Some(result),
            error: None,
        }
    }

    pub fn err(
        request_id: impl Into<String>,
        operation: impl Into<String>,
        error: OperationError,
    ) -> Self {
        Self {
            request_id: request_id.into(),
            operation: operation.into(),
            ok: false,
            result: None,
            error: Some(error),
        }
    }

    /// Enforce the `ok`⇔payload invariant. `ok:true` must carry no error;
    /// `ok:false` must carry an error and no result.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.ok {
            if self.error.is_some() {
                return Err(ProtocolError::InvalidFrame);
            }
        } else if self.error.is_none() || self.result.is_some() {
            return Err(ProtocolError::InvalidFrame);
        }
        Ok(())
    }
}

/// A fully-decoded client→host frame.
#[derive(Debug, Clone, PartialEq)]
pub enum ClientFrame {
    Hello(super::handshake::HelloFrame),
    Request(RequestFrame),
}

/// A fully-decoded host→client frame. Subscription/config/catalog payloads are
/// carried as raw [`Value`] here; the continuity layer (phase 6) parses them.
#[derive(Debug, Clone, PartialEq)]
pub enum HostFrame {
    Accepted(AcceptedFrame),
    Incompatible(IncompatibleFrame),
    Draining(DrainingFrame),
    Response(ResponseFrame),
    Subscription(Value),
    ConfigChanged(Value),
    CatalogChanged(Value),
}

fn kind_of(v: &Value) -> Option<&str> {
    v.get("kind").and_then(|k| k.as_str())
}

fn from_value<T: for<'de> Deserialize<'de>>(v: Value) -> Result<T, ProtocolError> {
    serde_json::from_value(v).map_err(|e| ProtocolError::InvalidJson(e.to_string()))
}

/// Decode a client→host frame. Unknown client `kind`s (e.g. client-capability,
/// deferred) are rejected as `InvalidFrame`; a frame with no `kind` must be a
/// request.
pub fn decode_client_frame(v: Value) -> Result<ClientFrame, ProtocolError> {
    if !v.is_object() {
        return Err(ProtocolError::InvalidFrame);
    }
    match kind_of(&v) {
        Some("hello") => {
            let f: super::handshake::HelloFrame = from_value(v)?;
            if f.kind != HandshakeKind::Hello {
                return Err(ProtocolError::InvalidFrame);
            }
            Ok(ClientFrame::Hello(f))
        }
        Some(_) => Err(ProtocolError::InvalidFrame),
        None => {
            if v.get("requestId").is_none() {
                return Err(ProtocolError::InvalidFrame);
            }
            Ok(ClientFrame::Request(from_value(v)?))
        }
    }
}

/// Decode a host→client frame, enforcing the response payload invariant.
pub fn decode_host_frame(v: Value) -> Result<HostFrame, ProtocolError> {
    if !v.is_object() {
        return Err(ProtocolError::InvalidFrame);
    }
    match kind_of(&v) {
        Some("accepted") => Ok(HostFrame::Accepted(from_value(v)?)),
        Some("incompatible") => Ok(HostFrame::Incompatible(from_value(v)?)),
        Some("draining") => Ok(HostFrame::Draining(from_value(v)?)),
        Some(k) if k.starts_with("subscription.") => Ok(HostFrame::Subscription(v)),
        Some("configuration.changed") => Ok(HostFrame::ConfigChanged(v)),
        Some("session.catalog.changed") => Ok(HostFrame::CatalogChanged(v)),
        Some(_) => Err(ProtocolError::InvalidFrame),
        None => {
            if v.get("requestId").is_none() {
                return Err(ProtocolError::InvalidFrame);
            }
            let f: ResponseFrame = from_value(v)?;
            f.validate()?;
            Ok(HostFrame::Response(f))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::errors::OpErrorCode;
    use super::super::handshake::{HandshakeSurface, LifecycleState};
    use serde_json::json;

    #[test]
    fn decodes_hello() {
        let v = json!({
            "kind": "hello", "clientInstanceId": "c1", "surface": "tui",
            "protocolMin": 0, "protocolMax": 0
        });
        let f = decode_client_frame(v).unwrap();
        match f {
            ClientFrame::Hello(h) => assert_eq!(h.surface, HandshakeSurface::Tui),
            other => panic!("expected Hello, got {other:?}"),
        }
    }

    #[test]
    fn decodes_request_without_kind() {
        let v = json!({"requestId": "r1", "operation": "host.status", "input": {}});
        let f = decode_client_frame(v).unwrap();
        match f {
            ClientFrame::Request(r) => assert_eq!(r.operation, "host.status"),
            other => panic!("expected Request, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unknown_client_kind() {
        let v = json!({"kind": "client.capability.replace"});
        assert_eq!(decode_client_frame(v), Err(ProtocolError::InvalidFrame));
    }

    #[test]
    fn rejects_hello_with_unknown_key() {
        let v = json!({
            "kind": "hello", "clientInstanceId": "c1", "surface": "tui",
            "protocolMin": 0, "protocolMax": 0, "extra": 1
        });
        assert!(matches!(decode_client_frame(v), Err(ProtocolError::InvalidJson(_))));
    }

    #[test]
    fn round_trips_ok_response() {
        let r = ResponseFrame::ok("r1", "host.status", json!({"state": "ready"}));
        let v = serde_json::to_value(&r).unwrap();
        // canonical: ok true, result present, no error key
        let obj = v.as_object().unwrap();
        assert!(!obj.contains_key("error"));
        let back = decode_host_frame(v).unwrap();
        match back {
            HostFrame::Response(rf) => {
                assert!(rf.ok);
                assert_eq!(rf.result, Some(json!({"state": "ready"})));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    #[test]
    fn round_trips_err_response() {
        let r = ResponseFrame::err("r1", "turn.start", OperationError::new(OpErrorCode::NotFound, "no session"));
        let v = serde_json::to_value(&r).unwrap();
        let back = decode_host_frame(v).unwrap();
        match back {
            HostFrame::Response(rf) => {
                assert!(!rf.ok);
                assert_eq!(rf.error.unwrap().code, OpErrorCode::NotFound);
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    #[test]
    fn rejects_ok_with_error_payload() {
        let v = json!({
            "requestId": "r1", "operation": "x", "ok": true,
            "error": {"code": "internal_failure", "message": "bad"}
        });
        assert_eq!(decode_host_frame(v), Err(ProtocolError::InvalidFrame));
    }

    #[test]
    fn rejects_err_without_error_payload() {
        let v = json!({"requestId": "r1", "operation": "x", "ok": false});
        assert_eq!(decode_host_frame(v), Err(ProtocolError::InvalidFrame));
    }

    #[test]
    fn decodes_accepted_and_subscription_routing() {
        let accepted = json!({
            "kind": "accepted", "hostEpoch": "h1", "connectionId": "c1",
            "selectedProtocol": 0, "compatibilityEpoch": 10, "state": "ready"
        });
        match decode_host_frame(accepted).unwrap() {
            HostFrame::Accepted(a) => assert_eq!(a.state, LifecycleState::Ready),
            other => panic!("expected Accepted, got {other:?}"),
        }
        let sub = json!({"kind": "subscription.session_delta", "subscriptionId": "s1", "sequence": 1});
        assert!(matches!(decode_host_frame(sub).unwrap(), HostFrame::Subscription(_)));
    }

    #[test]
    fn request_rejects_unknown_key() {
        let v = json!({"requestId": "r1", "operation": "x", "input": {}, "stray": true});
        assert!(matches!(decode_client_frame(v), Err(ProtocolError::InvalidJson(_))));
    }
}
