//! Tool / ToolRegistry behavior: OpenAI function shape, execution, dispatch.

use meepo_tools::{Bash, Edit, ReadFile, Tool, ToolError, ToolRegistry, WriteFile};
use serde_json::json;

fn temp_path(suffix: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "meepo-tools-{}-{suffix}.txt",
        std::process::id()
    ))
}

#[test]
fn read_file_openai_function_shape() {
    let tool = ReadFile;
    let f = tool.openai_function();
    assert_eq!(f["type"], "function");
    assert_eq!(f["function"]["name"], "read_file");
    assert_eq!(f["function"]["parameters"]["properties"]["path"]["type"], "string");
}

#[tokio::test]
async fn read_file_executes() {
    let path = temp_path("exec");
    std::fs::write(&path, "hello from file").unwrap();
    let tool = ReadFile;
    let out = tool.execute(&json!({ "path": path })).await.unwrap();
    assert_eq!(out, "hello from file");
}

#[tokio::test]
async fn read_file_missing_path_is_bad_args() {
    let tool = ReadFile;
    let err = tool.execute(&json!({})).await.unwrap_err();
    assert!(matches!(err, ToolError::BadArgs(_)));
}

#[tokio::test]
async fn registry_dispatches_and_lists() {
    let path = temp_path("reg");
    std::fs::write(&path, "registry works").unwrap();

    let mut reg = ToolRegistry::new();
    reg.register(Box::new(ReadFile));

    let funcs = reg.openai_functions();
    assert!(funcs.iter().any(|f| f["function"]["name"] == "read_file"));

    let out = reg
        .execute("read_file", &json!({ "path": path }))
        .await
        .unwrap();
    assert_eq!(out, "registry works");

    let err = reg.execute("nope", &json!({})).await.unwrap_err();
    assert!(matches!(err, ToolError::NotFound(_)));
}

#[tokio::test]
async fn write_file_creates_and_overwrites() {
    let path = temp_path("write");
    let tool = WriteFile;
    tool.execute(&json!({ "path": path, "content": "first" }))
        .await
        .unwrap();
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "first");
    tool.execute(&json!({ "path": path, "content": "second" }))
        .await
        .unwrap();
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "second");
}

#[tokio::test]
async fn edit_replaces_unique_occurrence() {
    let path = temp_path("edit");
    std::fs::write(&path, "foo bar baz").unwrap();
    let tool = Edit;
    tool.execute(&json!({ "path": path, "old_string": "bar", "new_string": "QUX" }))
        .await
        .unwrap();
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "foo QUX baz");
}

#[tokio::test]
async fn edit_errors_when_not_unique() {
    let path = temp_path("edit-multi");
    std::fs::write(&path, "a a a").unwrap();
    let tool = Edit;
    let err = tool
        .execute(&json!({ "path": path, "old_string": "a", "new_string": "b" }))
        .await
        .unwrap_err();
    assert!(matches!(err, ToolError::Other(_)));
}

#[tokio::test]
async fn bash_runs_command() {
    let tool = Bash;
    let out = tool
        .execute(&json!({ "command": "echo hello-meepo" }))
        .await
        .unwrap();
    assert!(out.contains("hello-meepo"), "{out}");
    assert!(out.contains("exit 0"), "{out}");
}

#[tokio::test]
async fn all_registers_four_tools() {
    let mut reg = ToolRegistry::new();
    for t in meepo_tools::all() {
        reg.register(t);
    }
    let names = reg.names();
    for expected in ["read_file", "write_file", "edit", "bash"] {
        assert!(names.contains(&expected), "missing {expected}: {names:?}");
    }
}
