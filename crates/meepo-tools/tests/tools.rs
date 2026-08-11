//! Tool / ToolRegistry behavior: OpenAI function shape, execution, dispatch.

use meepo_tools::{all, Bash, Edit, Glob, Grep, ReadFile, Tool, ToolError, ToolRegistry, WriteFile};
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
    let tool = Bash::default();
    let out = tool
        .execute(&json!({ "command": "echo hello-meepo" }))
        .await
        .unwrap();
    assert!(out.contains("hello-meepo"), "{out}");
    assert!(out.contains("exit 0"), "{out}");
}

#[tokio::test]
async fn all_registers_six_tools() {
    let mut reg = ToolRegistry::new();
    for t in all() {
        reg.register(t);
    }
    let names = reg.names();
    for expected in ["read_file", "write_file", "edit", "bash", "glob", "grep"] {
        assert!(names.contains(&expected), "missing {expected}: {names:?}");
    }
}

fn temp_dir(prefix: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("meepo-{prefix}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test]
async fn glob_finds_files_by_pattern() {
    let dir = temp_dir("glob");
    std::fs::write(dir.join("a.rs"), "x").unwrap();
    std::fs::write(dir.join("b.txt"), "y").unwrap();
    std::fs::create_dir_all(dir.join("sub")).unwrap();
    std::fs::write(dir.join("sub").join("c.rs"), "z").unwrap();

    let out = Glob.execute(&json!({ "pattern": "**/*.rs", "path": dir })).await.unwrap();
    assert!(out.contains("a.rs"), "{out}");
    assert!(out.contains("c.rs"), "{out}");
    assert!(!out.contains("b.txt"), "{out}");
}

#[tokio::test]
async fn grep_searches_contents() {
    let dir = temp_dir("grep");
    std::fs::write(dir.join("a.txt"), "alpha\nbeta\ngamma\n").unwrap();
    std::fs::write(dir.join("b.txt"), "delta\n").unwrap();

    let out = Grep.execute(&json!({ "pattern": "beta|delta", "path": dir })).await.unwrap();
    assert!(out.contains(":2: beta"), "{out}");
    assert!(out.contains(":1: delta"), "{out}");
}

#[tokio::test]
async fn grep_respects_include_filter() {
    let dir = temp_dir("grep-inc");
    std::fs::write(dir.join("a.rs"), "needle\n").unwrap();
    std::fs::write(dir.join("a.txt"), "needle\n").unwrap();

    let out = Grep.execute(&json!({ "pattern": "needle", "path": dir, "include": "*.rs" })).await.unwrap();
    assert!(out.contains("a.rs"), "{out}");
    assert!(!out.contains("a.txt"), "{out}");
}
