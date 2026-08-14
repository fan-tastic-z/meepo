//! Per-root ownership (advisory flock) + discovery (registration.json).
//!
//! Exactly one host process owns each storage root: an exclusive advisory
//! lock on `<root>/control/owner.lock`, released automatically when the owning
//! process dies. The registration record (`registration.json` in the same dir)
//! advertises the socket endpoint + host epoch so a client can find and
//! connect to the owner. The lock and the registration are distinct — the lock
//! is the ownership invariant; the record is discovery.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use serde::{Deserialize, Serialize};

use crate::protocol::{
    LifecycleState, COMPATIBILITY_EPOCH, PROTOCOL_MAX, PROTOCOL_MIN, REGISTRATION_SCHEMA_VERSION,
};

const REGISTRATION_KIND: &str = "meepo-runtime-host";
const REGISTRATION_FILE: &str = "registration.json";
const OWNER_LOCK_FILE: &str = "owner.lock";
const CONTROL_DIR: &str = "control";

/// The discovery record, written atomically into the control dir.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct HostRegistration {
    pub kind: String,
    pub schema_version: u32,
    pub root_id: String,
    pub host_epoch: String,
    pub endpoint: String,
    pub protocol_min: u32,
    pub protocol_max: u32,
    pub compatibility_epoch: u32,
    pub state: LifecycleState,
    pub pid: u32,
    pub created_at: i64,
}

impl HostRegistration {
    pub fn new(
        root_id: impl Into<String>,
        host_epoch: impl Into<String>,
        endpoint: impl Into<String>,
        state: LifecycleState,
    ) -> Self {
        Self {
            kind: REGISTRATION_KIND.into(),
            schema_version: REGISTRATION_SCHEMA_VERSION,
            root_id: root_id.into(),
            host_epoch: host_epoch.into(),
            endpoint: endpoint.into(),
            protocol_min: PROTOCOL_MIN,
            protocol_max: PROTOCOL_MAX,
            compatibility_epoch: COMPATIBILITY_EPOCH,
            state,
            pid: std::process::id(),
            created_at: now_secs(),
        }
    }
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// A stable root id from a root path (canonical path string). Used in the
/// registration and validated against `--expected-root-id`.
pub fn root_id_of(root: &Path) -> String {
    root.canonicalize()
        .unwrap_or_else(|_| root.to_path_buf())
        .to_string_lossy()
        .into_owned()
}

/// `<root>/control`.
pub fn control_dir(root: &Path) -> PathBuf {
    root.join(CONTROL_DIR)
}

/// `<root>/control/registration.json`.
pub fn registration_path(root: &Path) -> PathBuf {
    control_dir(root).join(REGISTRATION_FILE)
}

fn owner_lock_path(root: &Path) -> PathBuf {
    control_dir(root).join(OWNER_LOCK_FILE)
}

/// Read the registration record, if present and well-formed.
pub fn read_registration(root: &Path) -> Option<HostRegistration> {
    let bytes = fs::read(registration_path(root)).ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Write the registration record atomically (temp file + fsync + rename).
pub fn write_registration(root: &Path, reg: &HostRegistration) -> std::io::Result<()> {
    let dir = control_dir(root);
    fs::create_dir_all(&dir)?;
    let bytes = serde_json::to_vec(reg).expect("registration serializes");
    let tmp = dir.join(format!("{REGISTRATION_FILE}.tmp.{pid}", pid = std::process::id()));
    {
        let mut f = File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, registration_path(root))?;
    Ok(())
}

/// Remove the registration record, but only if `host_epoch` still owns it
/// (a restarted host must not delete a newer owner's record).
pub fn remove_registration(root: &Path, host_epoch: &str) -> std::io::Result<()> {
    if let Some(reg) = read_registration(root) {
        if reg.host_epoch == host_epoch {
            let _ = fs::remove_file(registration_path(root));
        }
    }
    Ok(())
}

/// Exclusive ownership of a storage root, held for the owner's lifetime.
/// Dropping releases the lock.
pub struct Ownership {
    lock: Option<File>,
}

impl Ownership {
    /// Try to acquire exclusive ownership. Returns `None` if another process
    /// already holds it (the loser must exit). Re-acquiring from the same
    /// process is not supported (would deadlock); each owner acquires once.
    pub fn try_acquire(root: &Path) -> std::io::Result<Option<Self>> {
        fs::create_dir_all(control_dir(root))?;
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(owner_lock_path(root))?;
        // fs2: try_lock_exclusive returns Ok(()) on acquisition, Err on contention.
        match file.try_lock_exclusive() {
            Ok(()) => Ok(Some(Self { lock: Some(file) })),
            Err(_) => Ok(None),
        }
    }
}

impl Drop for Ownership {
    fn drop(&mut self) {
        if let Some(f) = self.lock.take() {
            let _ = f.unlock();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let reg = HostRegistration::new("/root/id", "epoch-1", "/root/host.sock", LifecycleState::Ready);
        write_registration(dir.path(), &reg).unwrap();
        let back = read_registration(dir.path()).expect("registration present");
        assert_eq!(back, reg);
        assert_eq!(back.kind, "meepo-runtime-host");
        // closed schema: unknown key rejected on read.
        let dir2 = tempfile::tempdir().unwrap();
        fs::create_dir_all(control_dir(dir2.path())).unwrap();
        fs::write(
            registration_path(dir2.path()),
            serde_json::to_string(&reg).unwrap().replace("\"pid\":", "\"stray\":1,\"pid\":"),
        )
        .unwrap();
        assert!(read_registration(dir2.path()).is_none(), "unknown key must reject");
    }

    #[test]
    fn flock_second_acquirer_loses() {
        let dir = tempfile::tempdir().unwrap();
        let first = Ownership::try_acquire(dir.path()).unwrap().expect("first acquires");
        let second = Ownership::try_acquire(dir.path()).unwrap();
        assert!(second.is_none(), "second acquirer must lose while first holds the lock");
        drop(first);
        // After release, a new acquirer wins.
        let third = Ownership::try_acquire(dir.path()).unwrap();
        assert!(third.is_some(), "lock is released on drop");
    }

    #[test]
    fn remove_registration_only_when_epoch_matches() {
        let dir = tempfile::tempdir().unwrap();
        write_registration(
            dir.path(),
            &HostRegistration::new("/r", "epoch-1", "/s", LifecycleState::Ready),
        )
        .unwrap();
        // A stale epoch does not remove it.
        remove_registration(dir.path(), "epoch-other").unwrap();
        assert!(read_registration(dir.path()).is_some());
        // The owning epoch removes it.
        remove_registration(dir.path(), "epoch-1").unwrap();
        assert!(read_registration(dir.path()).is_none());
    }
}
