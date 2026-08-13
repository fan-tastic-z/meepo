//! Protocol versioning and hard wire limits.
//!
//! Three independent version axes (a missing epoch decodes as `0` everywhere):
//! - [`REGISTRATION_SCHEMA_VERSION`]: the `registration.json` discovery record.
//! - [`PROTOCOL_MIN`] / [`PROTOCOL_MAX`]: the wire protocol range; negotiation
//!   picks `min(client_max, host_max)`, valid iff `>= max(client_min, host_min)`.
//! - [`COMPATIBILITY_EPOCH`]: an independent axis letting a client retire a
//!   stale same-protocol host whose closed schema has drifted.

/// `registration.json` discovery-record schema version.
pub const REGISTRATION_SCHEMA_VERSION: u32 = 1;
/// Lowest wire protocol this peer speaks.
pub const PROTOCOL_MIN: u32 = 0;
/// Highest wire protocol this peer speaks.
pub const PROTOCOL_MAX: u32 = 0;
/// Closed-schema compatibility epoch.
pub const COMPATIBILITY_EPOCH: u32 = 10;

/// Maximum bytes in one frame, delimiter included. Oversized ⇒ fatal teardown.
pub const MAX_FRAME_BYTES: usize = 96 * 1024;
/// Maximum simultaneously in-flight domain (non-`host.status`) requests.
pub const MAX_IN_FLIGHT_DOMAIN_REQUESTS: usize = 64;
