//! CLI smoke test: `meepo run <prompt>` echoes via the fake backend.

use assert_cmd::Command;

#[test]
fn run_subcommand_echoes_prompt() {
    let output = Command::cargo_bin("meepo-cli")
        .unwrap()
        .args(["run", "hello"])
        .ok()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("meepo (fake backend): hello"),
        "stdout was: {stdout}"
    );
}

#[test]
fn bare_prompt_also_works() {
    let output = Command::cargo_bin("meepo-cli")
        .unwrap()
        .arg("hi")
        .ok()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("meepo (fake backend): hi"), "stdout was: {stdout}");
}

#[test]
fn no_args_exits_nonzero() {
    let status = Command::cargo_bin("meepo-cli")
        .unwrap()
        .ok()
        .err()
        .map(|e| e.as_output().map(|o| o.status.code()));
    // assert_cmd's .ok() returns Err when the command exits non-zero.
    assert!(status.is_some(), "expected non-zero exit for no args");
}
