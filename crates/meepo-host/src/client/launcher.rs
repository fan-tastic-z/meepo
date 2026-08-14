//! Host candidate launcher — spawn a detached `meepo-host` daemon for a root.

use std::path::Path;
use std::process::{Command, Stdio};

/// Spawn a detached host candidate: `meepo-host --root <root>`. The child runs
/// independently (on Unix it is orphaned to init when the launcher exits).
pub fn spawn_candidate(executable: &Path, root: &Path) -> std::io::Result<()> {
    Command::new(executable)
        .arg("--root")
        .arg(root)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map(|_| ())
}
