//! Tool / ToolRegistry behavior: OpenAI function shape, execution, dispatch.

use meepo_tools::{ReadFile, Tool, ToolError, ToolRegistry};
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
