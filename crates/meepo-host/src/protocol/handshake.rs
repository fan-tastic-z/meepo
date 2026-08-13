//! Connection handshake: hello/handshake-result frames and protocol-range
//! negotiation.
//!
//! One round trip: the client sends a [`HelloFrame`], the host replies with
//! exactly one [`HandshakeResult`] (`accepted` | `incompatible` | `draining`).
//! The selected protocol is `min(client_max, host_max)`, valid iff it is `>=
//! max(client_min, host_min)`. The compatibility epoch is an INDEPENDENT axis
//! from the protocol version; a missing epoch decodes as `0`.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The client surface identifying itself in `hello`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HandshakeSurface {
    Desktop,
    Tui,
    Run,
    Activation,
    Bot,
    Inspect,
}

/// Tag for a handshake-direction frame: `hello` (client) or `accepted` /
/// `incompatible` / `draining` (host).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HandshakeKind {
    Hello,
    Accepted,
    Incompatible,
    Draining,
}

/// Host lifecycle state, carried in `accepted.state` and `registration.state`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleState {
    Starting,
    Recovering,
    Ready,
    Draining,
}

/// Why a client could not be accepted onto this host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplacementPolicy {
    BlockedByResidency,
    WaitForIdleExit,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum HandshakeError {
    #[error("invalid protocol range: min {min} > max {max}")]
    InvalidRange { min: u32, max: u32 },
    #[error("no protocol overlap")]
    NoOverlap,
}

/// Client→host opening frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct HelloFrame {
    pub kind: HandshakeKind,
    pub client_instance_id: String,
    pub surface: HandshakeSurface,
    pub protocol_min: u32,
    pub protocol_max: u32,
    #[serde(default)]
    pub compatibility_epoch: u32,
}

impl HelloFrame {
    pub fn new(
        client_instance_id: impl Into<String>,
        surface: HandshakeSurface,
        protocol_min: u32,
        protocol_max: u32,
        compatibility_epoch: u32,
    ) -> Self {
        Self {
            kind: HandshakeKind::Hello,
            client_instance_id: client_instance_id.into(),
            surface,
            protocol_min,
            protocol_max,
            compatibility_epoch,
        }
    }
}

/// Host→client acceptance frame. `state` must not be `Draining`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AcceptedFrame {
    pub kind: HandshakeKind,
    pub host_epoch: String,
    pub connection_id: String,
    pub selected_protocol: u32,
    #[serde(default)]
    pub compatibility_epoch: u32,
    pub state: LifecycleState,
}

impl AcceptedFrame {
    pub fn new(
        host_epoch: impl Into<String>,
        connection_id: impl Into<String>,
        selected_protocol: u32,
        compatibility_epoch: u32,
        state: LifecycleState,
    ) -> Self {
        Self {
            kind: HandshakeKind::Accepted,
            host_epoch: host_epoch.into(),
            connection_id: connection_id.into(),
            selected_protocol,
            compatibility_epoch,
            state,
        }
    }
}

/// Host→client incompatibility frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct IncompatibleFrame {
    pub kind: HandshakeKind,
    pub host_epoch: String,
    pub protocol_min: u32,
    pub protocol_max: u32,
    #[serde(default)]
    pub compatibility_epoch: u32,
    pub state: LifecycleState,
    pub replacement: ReplacementPolicy,
}

/// Host→client draining frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DrainingFrame {
    pub kind: HandshakeKind,
    pub host_epoch: String,
}

impl DrainingFrame {
    pub fn new(host_epoch: impl Into<String>) -> Self {
        Self { kind: HandshakeKind::Draining, host_epoch: host_epoch.into() }
    }
}

/// The host's single handshake reply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandshakeResult {
    Accepted(AcceptedFrame),
    Incompatible(IncompatibleFrame),
    Draining(DrainingFrame),
}

/// Validate a `[min, max]` protocol range (non-negative by `u32` construction).
pub fn validate_protocol_range(min: u32, max: u32) -> Result<(), HandshakeError> {
    if min > max {
        Err(HandshakeError::InvalidRange { min, max })
    } else {
        Ok(())
    }
}

/// Pick the selected protocol version, or fail if the ranges do not overlap.
/// `selected = min(client_max, host_max)`, valid iff `>= max(client_min, host_min)`.
pub fn negotiate_protocol(
    client_min: u32,
    client_max: u32,
    host_min: u32,
    host_max: u32,
) -> Result<u32, HandshakeError> {
    validate_protocol_range(client_min, client_max)?;
    validate_protocol_range(host_min, host_max)?;
    let selected = client_max.min(host_max);
    let floor = client_min.max(host_min);
    if selected >= floor {
        Ok(selected)
    } else {
        Err(HandshakeError::NoOverlap)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_range_ok() {
        assert!(validate_protocol_range(0, 0).is_ok());
        assert!(validate_protocol_range(3, 7).is_ok());
    }

    #[test]
    fn inverted_range_rejected() {
        assert!(matches!(
            validate_protocol_range(5, 2),
            Err(HandshakeError::InvalidRange { min: 5, max: 2 })
        ));
    }

    #[test]
    fn negotiates_overlap_at_client_max() {
        // client 2..5, host 3..7 → min(5,7)=5 >= max(2,3)=3
        assert_eq!(negotiate_protocol(2, 5, 3, 7), Ok(5));
    }

    #[test]
    fn negotiates_overlap_at_host_max() {
        // client 4..9, host 3..6 → min(9,6)=6 >= max(4,3)=4
        assert_eq!(negotiate_protocol(4, 9, 3, 6), Ok(6));
    }

    #[test]
    fn no_overlap_rejected() {
        // client 0..1, host 5..7 → min(1,7)=1 < max(0,5)=5
        assert_eq!(negotiate_protocol(0, 1, 5, 7), Err(HandshakeError::NoOverlap));
    }

    #[test]
    fn rejects_invalid_input_range_first() {
        assert!(matches!(
            negotiate_protocol(9, 2, 0, 0),
            Err(HandshakeError::InvalidRange { .. })
        ));
    }

    #[test]
    fn hello_round_trips_and_defaults_epoch() {
        let v = serde_json::json!({
            "kind": "hello",
            "clientInstanceId": "c1",
            "surface": "tui",
            "protocolMin": 0,
            "protocolMax": 0
        });
        let f: HelloFrame = serde_json::from_value(v).unwrap();
        assert_eq!(f.kind, HandshakeKind::Hello);
        assert_eq!(f.compatibility_epoch, 0, "missing epoch defaults to 0");
        // re-serialize is canonical (alphabetical keys)
        let s = serde_json::to_string(&f).unwrap();
        assert_eq!(s, serde_json::to_string(&HelloFrame::new("c1", HandshakeSurface::Tui, 0, 0, 0)).unwrap());
    }

    #[test]
    fn accepted_rejects_unknown_key() {
        let v = serde_json::json!({
            "kind": "accepted", "hostEpoch": "h1", "connectionId": "x1",
            "selectedProtocol": 0, "compatibilityEpoch": 10, "state": "ready",
            "stray": true
        });
        assert!(serde_json::from_value::<AcceptedFrame>(v).is_err());
    }
}
