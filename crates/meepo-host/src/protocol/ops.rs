//! Operation registry: op names, their specs, and the closed error-code
//! whitelist each declares. Every op must be able to return
//! [`OpErrorCode::InternalFailure`]; the dispatcher (phase 4) rejects any code
//! an op did not declare.
//!
//! Only the spine op set is enumerated here. Domain extensions (artifact,
//! memory, oauth, web-search, ...) reserve handler-map slots later.

use super::errors::OpErrorCode as E;

/// A wire operation key, e.g. `host.status`, `turn.start`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OpName(pub &'static str);

// ── bootstrap ops (callable before `ready`) ──
pub const HOST_STATUS: OpName = OpName("host.status");
pub const HOST_DIAGNOSTICS_QUERY: OpName = OpName("host.diagnostics.query");

// ── turn lifecycle ──
pub const TURN_START: OpName = OpName("turn.start");
pub const TURN_QUERY: OpName = OpName("turn.query");
pub const TURN_STOP: OpName = OpName("turn.stop");
pub const TURN_REGENERATE: OpName = OpName("turn.regenerate");
pub const TURN_RESUME_QUERY: OpName = OpName("turn.resume.query");
pub const TURN_RESUME_START: OpName = OpName("turn.resume.start");

// ── subscription ──
pub const SUBSCRIPTION_OPEN: OpName = OpName("subscription.open");
pub const SUBSCRIPTION_CLOSE: OpName = OpName("subscription.close");

// ── message steering ──
pub const TURN_MESSAGE_SUBMIT: OpName = OpName("turn.message.submit");
pub const QUEUE_RETRACT: OpName = OpName("queue.retract");
pub const TURN_INTERRUPT: OpName = OpName("turn.interrupt");

// ── session lifecycle ──
pub const SESSION_CREATE: OpName = OpName("session.create");
pub const SESSION_CATALOG_QUERY: OpName = OpName("session.catalog.query");
pub const SESSION_METADATA_UPDATE: OpName = OpName("session.metadata.update");
pub const SESSION_CONFIGURATION_UPDATE: OpName = OpName("session.configuration.update");
pub const SESSION_CWD_RELOCATE: OpName = OpName("session.cwd.relocate");
pub const SESSION_READ_MARKER_SET: OpName = OpName("session.read_marker.set");
pub const SESSION_LIFECYCLE_SET: OpName = OpName("session.lifecycle.set");
pub const SESSION_REMOVE: OpName = OpName("session.remove");

// ── interaction (permission) ──
pub const INTERACTION_QUERY: OpName = OpName("interaction.query");
pub const INTERACTION_ANSWER: OpName = OpName("interaction.answer");

/// One operation's declared shape. The handler map is checked for
/// completeness at startup against this table.
#[derive(Debug, Clone, Copy)]
pub struct OperationSpec {
    pub name: OpName,
    pub allowed_error_codes: &'static [E],
}

const fn ife(codes: &'static [E]) -> &'static [E] {
    codes
}

/// The full spine op set with its declared error-code whitelist.
pub const SPINE_SPECS: &[OperationSpec] = &[
    OperationSpec { name: HOST_STATUS, allowed_error_codes: ife(&[E::InternalFailure]) },
    OperationSpec { name: HOST_DIAGNOSTICS_QUERY, allowed_error_codes: ife(&[E::InternalFailure]) },
    OperationSpec {
        name: TURN_START,
        allowed_error_codes: ife(&[
            E::HostNotReady, E::NotFound, E::SessionArchived, E::SessionBusy,
            E::OperationConflict, E::InvalidRequest, E::PersistenceFailed, E::InternalFailure,
        ]),
    },
    OperationSpec {
        name: TURN_QUERY,
        allowed_error_codes: ife(&[E::NotFound, E::InternalFailure]),
    },
    OperationSpec {
        name: TURN_STOP,
        allowed_error_codes: ife(&[E::NotFound, E::SessionBusy, E::OperationConflict, E::InternalFailure]),
    },
    OperationSpec {
        name: TURN_REGENERATE,
        allowed_error_codes: ife(&[E::NotFound, E::SessionBusy, E::OperationConflict, E::InternalFailure]),
    },
    OperationSpec {
        name: TURN_RESUME_QUERY,
        allowed_error_codes: ife(&[E::NotFound, E::InternalFailure]),
    },
    OperationSpec {
        name: TURN_RESUME_START,
        allowed_error_codes: ife(&[
            E::NotFound, E::SessionBusy, E::OperationConflict, E::InvalidRequest, E::InternalFailure,
        ]),
    },
    OperationSpec {
        name: SUBSCRIPTION_OPEN,
        allowed_error_codes: ife(&[E::NotFound, E::InternalFailure]),
    },
    OperationSpec {
        name: SUBSCRIPTION_CLOSE,
        allowed_error_codes: ife(&[E::NotFound, E::InternalFailure]),
    },
    OperationSpec {
        name: TURN_MESSAGE_SUBMIT,
        allowed_error_codes: ife(&[
            E::HostNotReady, E::NotFound, E::SessionBusy, E::OperationConflict,
            E::InvalidRequest, E::InternalFailure,
        ]),
    },
    OperationSpec {
        name: QUEUE_RETRACT,
        allowed_error_codes: ife(&[E::NotFound, E::OperationConflict, E::InternalFailure]),
    },
    OperationSpec {
        name: TURN_INTERRUPT,
        allowed_error_codes: ife(&[E::NotFound, E::SessionBusy, E::OperationConflict, E::InternalFailure]),
    },
    OperationSpec {
        name: SESSION_CREATE,
        allowed_error_codes: ife(&[E::InvalidRequest, E::PersistenceFailed, E::InternalFailure]),
    },
    OperationSpec {
        name: SESSION_CATALOG_QUERY,
        allowed_error_codes: ife(&[E::InvalidRequest, E::InternalFailure]),
    },
    OperationSpec {
        name: SESSION_METADATA_UPDATE,
        allowed_error_codes: ife(&[E::NotFound, E::RevisionConflict, E::InternalFailure]),
    },
    OperationSpec {
        name: SESSION_CONFIGURATION_UPDATE,
        allowed_error_codes: ife(&[E::NotFound, E::RevisionConflict, E::InternalFailure]),
    },
    OperationSpec {
        name: SESSION_CWD_RELOCATE,
        allowed_error_codes: ife(&[E::NotFound, E::RevisionConflict, E::InternalFailure]),
    },
    OperationSpec {
        name: SESSION_READ_MARKER_SET,
        allowed_error_codes: ife(&[E::NotFound, E::InternalFailure]),
    },
    OperationSpec {
        name: SESSION_LIFECYCLE_SET,
        allowed_error_codes: ife(&[E::NotFound, E::OperationConflict, E::InternalFailure]),
    },
    OperationSpec {
        name: SESSION_REMOVE,
        allowed_error_codes: ife(&[E::NotFound, E::OperationConflict, E::InternalFailure]),
    },
    OperationSpec {
        name: INTERACTION_QUERY,
        allowed_error_codes: ife(&[E::NotFound, E::InternalFailure]),
    },
    OperationSpec {
        name: INTERACTION_ANSWER,
        allowed_error_codes: ife(&[E::NotFound, E::AlreadyResolved, E::InternalFailure]),
    },
];

/// Whether `name` is part of the spine op set.
pub fn is_spine_op(name: &str) -> bool {
    SPINE_SPECS.iter().any(|s| s.name.0 == name)
}

/// Look up the declared error-code whitelist for an op, if it is a spine op.
pub fn spec_for(name: &str) -> Option<&'static OperationSpec> {
    SPINE_SPECS.iter().find(|s| s.name.0 == name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_op_declares_internal_failure() {
        for spec in SPINE_SPECS {
            assert!(
                spec.allowed_error_codes.contains(&E::InternalFailure),
                "op {} must declare internal_failure",
                spec.name.0,
            );
        }
    }

    #[test]
    fn spine_op_names_are_unique() {
        let n = SPINE_SPECS.len();
        let unique = SPINE_SPECS.iter().map(|s| s.name.0).collect::<std::collections::HashSet<_>>().len();
        assert_eq!(n, unique, "duplicate op names in SPINE_SPECS");
    }

    #[test]
    fn recognizes_spine_ops() {
        assert!(is_spine_op("host.status"));
        assert!(is_spine_op("turn.start"));
        assert!(!is_spine_op("oauth.exchange"));
    }
}
