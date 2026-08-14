//! Subscription frame vocabulary. Each frame carries `kind`, `hostEpoch`,
//! `subscriptionId`, and a monotonic `sequence`.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Kind of an incremental assistant delta.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeltaKind {
    Text,
    Thinking,
}

/// Why a subscription was closed by the host.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ClosedReason {
    SlowConsumer,
    SessionRemoved,
}

/// One frame pushed on an open subscription. Discriminated by `kind`; the
/// runtime fields are camelCase on the wire.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind")]
pub enum SubscriptionFrame {
    #[serde(rename = "subscription.session_projection", rename_all = "camelCase")]
    SessionProjection {
        host_epoch: String,
        subscription_id: String,
        sequence: u64,
        snapshot: Value,
    },

    #[serde(rename = "subscription.session_delta", rename_all = "camelCase")]
    SessionDelta {
        host_epoch: String,
        subscription_id: String,
        sequence: u64,
        turn_id: String,
        run_id: String,
        message_id: String,
        delta_kind: DeltaKind,
        start_offset: u64,
        text: String,
    },

    #[serde(rename = "subscription.session_event", rename_all = "camelCase")]
    SessionEvent {
        host_epoch: String,
        subscription_id: String,
        sequence: u64,
        /// Tool-lifecycle payload; phase 7 fills the typed shape.
        payload: Value,
    },

    #[serde(rename = "subscription.session_domain_changed", rename_all = "camelCase")]
    SessionDomainChanged {
        host_epoch: String,
        subscription_id: String,
        sequence: u64,
    },

    #[serde(rename = "subscription.closed", rename_all = "camelCase")]
    Closed {
        host_epoch: String,
        subscription_id: String,
        reason: ClosedReason,
    },
}

impl SubscriptionFrame {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::SessionProjection { .. } => "subscription.session_projection",
            Self::SessionDelta { .. } => "subscription.session_delta",
            Self::SessionEvent { .. } => "subscription.session_event",
            Self::SessionDomainChanged { .. } => "subscription.session_domain_changed",
            Self::Closed { .. } => "subscription.closed",
        }
    }

    pub fn closed(host_epoch: &str, subscription_id: &str, reason: ClosedReason) -> Self {
        Self::Closed {
            host_epoch: host_epoch.to_string(),
            subscription_id: subscription_id.to_string(),
            reason,
        }
    }
}
