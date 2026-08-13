//! AimuxBackend — unified multi-provider backend via aimux.
//!
//! Wraps any `aimux_core::LanguageModel` (325+ providers) and drives an
//! internal agentLoop: stream_text → consume StreamPart → map to SessionEvent
//! → execute tools via ToolExecutor → loop → terminal.
//!
//! This replaces the hand-written OpenAiBackend + AnthropicBackend with a
//! single ~200-line adapter, mirroring how maka uses the Vercel AI SDK.

use std::sync::Arc;

use aimux_core::content::ContentPart;
use aimux_core::generate::{stream_text, GenerateTextOptions};
use aimux_core::language_model::LanguageModel;
use aimux_core::message::{MessageContent, ModelMessage, ModelPrompt, Role};
use aimux_core::stream_part::StreamPart;
use aimux_core::tool::{FunctionTool, Tool as AimuxTool};
use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use serde_json::Value;

use meepo_core::{
    AgentBackend, AssistantToolCall, BackendKind, BackendResult, BackendSendInput,
    BackendStopMode, BackendStopReason, ChatMessage, InteractionStore, PermissionDecision,
    PermissionGate, PermissionOutcome, PermissionVerdict, SessionEvent, StopReason, ToolExecutor,
    ToolOperation, ToolOperationStore, ToolRecoveryMode, canonical_tool_args_hash, operation_id,
};

/// Tool results above this many chars are archived to disk and replaced with
/// a summary in the model-visible context. This is "active tool result pruning"
/// (maka Chapter 2): the ledger stores the summary, the full result is in an
/// archive file for audit. The model never sees the full raw output — only
/// the preview + archive reference.
const ARCHIVE_THRESHOLD_CHARS: usize = 4_000;

pub struct AimuxBackend {
    session_id: String,
    model: Box<dyn LanguageModel>,
    counter: u64,
    executor: Option<Arc<dyn ToolExecutor>>,
    gate: Option<Arc<dyn PermissionGate>>,
    store: Option<Arc<dyn InteractionStore>>,
    op_store: Option<Arc<dyn ToolOperationStore>>,
}

impl AimuxBackend {
    pub fn new(session_id: impl Into<String>, model: Box<dyn LanguageModel>) -> Self {
        Self {
            session_id: session_id.into(),
            model,
            counter: 0,
            executor: None,
            gate: None,
            store: None,
            op_store: None,
        }
    }

    pub fn with_executor(mut self, executor: Arc<dyn ToolExecutor>) -> Self {
        self.executor = Some(executor);
        self
    }

    pub fn with_permission_gate(mut self, gate: Arc<dyn PermissionGate>) -> Self {
        self.gate = Some(gate);
        self
    }

    pub fn with_interaction_store(mut self, store: Arc<dyn InteractionStore>) -> Self {
        self.store = Some(store);
        self
    }

    pub fn with_tool_operation_store(mut self, store: Arc<dyn ToolOperationStore>) -> Self {
        self.op_store = Some(store);
        self
    }
}

#[async_trait]
impl AgentBackend for AimuxBackend {
    fn kind(&self) -> BackendKind {
        BackendKind::AiSdk
    }

    fn session_id(&self) -> &str {
        &self.session_id
    }

    fn send<'a>(&'a mut self, input: &'a BackendSendInput) -> BoxStream<'a, SessionEvent> {
        let session_id = self.session_id.clone();
        let turn_id = input.turn_id.clone();
        let system_prompt = input.system_prompt.clone();
        let max_steps = input.max_steps.unwrap_or(50);
        let executor = self.executor.clone();
        let gate = self.gate.clone();
        let store = self.store.clone();
        let op_store = self.op_store.clone();
        let run_id = input.run_id.clone().unwrap_or_default();
        let invocation_id = input.invocation_id.clone().unwrap_or_default();
        let mut messages = input.messages.clone();
        let mut counter = self.counter;
        self.counter = self.counter.saturating_add(1_000_000);

        let stream = async_stream::stream! {
            let tools: Vec<AimuxTool> = executor
                .as_ref()
                .map(|e| executor_to_aimux_tools(&**e))
                .unwrap_or_default();

            let mut step = 0u32;
            loop {
                step += 1;
                if step > max_steps {
                    counter += 1;
                    yield done_event(&turn_id, counter, StopReason::StepLimit);
                    return;
                }

                // Build aimux prompt + options.
                let prompt = ModelPrompt::Messages(chat_to_aimux_messages(&messages));
                let options = GenerateTextOptions {
                    max_output_tokens: Some(8192),
                    tools: if tools.is_empty() { None } else { Some(tools.clone()) },
                    instructions: system_prompt.clone(),
                    ..Default::default()
                };

                // Stream via aimux.
                let result = match stream_text(&*self.model, prompt, options).await {
                    Ok(r) => r,
                    Err(e) => {
                        counter += 1;
                        yield err_event(&turn_id, counter, format!("stream_text: {e}"));
                        return;
                    }
                };

                // Consume StreamPart stream.
                let mut tool_calls: Vec<(String, String, Value)> = Vec::new();
                let mut step_text = String::new();
                let mut thinking_text = String::new();
                let mut thinking_blocks: Vec<meepo_core::ThinkingBlock> = Vec::new();
                let mut stream = result.stream;
                while let Some(part_result) = stream.next().await {
                    match part_result {
                        Ok(StreamPart::TextDelta { delta, id, .. }) => {
                            step_text.push_str(&delta);
                            counter += 1;
                            yield SessionEvent::TextDelta {
                                id: format!("aimux-{counter}"),
                                turn_id: turn_id.clone(),
                                ts: counter as i64,
                                message_id: id,
                                start_offset: None,
                                text: delta,
                            };
                        }
                        Ok(StreamPart::ReasoningStart { .. }) => {
                            thinking_text.clear();
                        }
                        Ok(StreamPart::ReasoningDelta { delta, id, .. }) => {
                            thinking_text.push_str(&delta);
                            counter += 1;
                            yield SessionEvent::ThinkingDelta {
                                id: format!("aimux-{counter}"),
                                turn_id: turn_id.clone(),
                                ts: counter as i64,
                                message_id: id,
                                text: delta,
                            };
                        }
                        Ok(StreamPart::ReasoningEnd { id, provider_metadata, .. }) => {
                            let signature = provider_metadata
                                .as_ref()
                                .and_then(|m| m.get("anthropic"))
                                .and_then(|a| a.get("signature"))
                                .and_then(|s| s.as_str())
                                .map(|s| s.to_string());
                            thinking_blocks.push(meepo_core::ThinkingBlock {
                                text: thinking_text.clone(),
                                signature: signature.clone(),
                            });
                            counter += 1;
                            yield SessionEvent::ThinkingComplete {
                                id: format!("aimux-{counter}"),
                                turn_id: turn_id.clone(),
                                ts: counter as i64,
                                message_id: id,
                                text: thinking_text.clone(),
                                signature,
                            };
                        }
                        Ok(StreamPart::ToolCall { tool_call_id, tool_name, input, .. }) => {
                            tool_calls.push((tool_call_id, tool_name, input));
                        }
                        Ok(StreamPart::Error { error }) => {
                            counter += 1;
                            yield err_event(&turn_id, counter, format!("{error}"));
                            return;
                        }
                        Err(e) => {
                            counter += 1;
                            yield err_event(&turn_id, counter, format!("{e}"));
                            return;
                        }
                        _ => {}
                    }
                }

                // Tool execution or terminal.
                if !tool_calls.is_empty() {
                    let calls: Vec<AssistantToolCall> = tool_calls
                        .iter()
                        .map(|(id, name, args)| AssistantToolCall {
                            id: id.clone(),
                            name: name.clone(),
                            args: args.clone(),
                        })
                        .collect();
                    // Preserve accumulated text and thinking from this step.
                    let content = if step_text.is_empty() { None } else { Some(std::mem::take(&mut step_text)) };
                    let thinking = std::mem::take(&mut thinking_blocks);
                    messages.push(ChatMessage::Assistant { content, tool_calls: calls, thinking });

                    for (tc_id, tc_name, tc_args) in &tool_calls {
                        counter += 1;
                        yield SessionEvent::ToolCall {
                            id: tc_id.clone(),
                            turn_id: turn_id.clone(),
                            ts: counter as i64,
                            tool_call_id: tc_id.clone(),
                            tool_name: tc_name.clone(),
                            args: tc_args.clone(),
                        };

                        // T1 dispatch fact: the runtime is about to execute
                        // this call. Emitted before dispatch so a crash between
                        // here and the result leaves a durable "may have
                        // started" marker for recovery.
                        let op_id = operation_id(&invocation_id, tc_id);
                        let args_hash = canonical_tool_args_hash(tc_name, tc_args);
                        counter += 1;
                        let dispatch_id = format!("aimux-{counter}");
                        yield SessionEvent::ToolDispatch {
                            id: dispatch_id.clone(),
                            turn_id: turn_id.clone(),
                            ts: counter as i64,
                            operation_id: op_id.clone(),
                            tool_call_id: tc_id.clone(),
                            tool_name: tc_name.clone(),
                            canonical_args_hash: args_hash.clone(),
                            recovery_mode: ToolRecoveryMode::ReplaySafe,
                        };

                        // Persist the operation row at the dispatch boundary.
                        if let Some(op_store) = &op_store {
                            let _ = op_store
                                .record_tool_operation(&ToolOperation {
                                    operation_id: op_id.clone(),
                                    invocation_id: invocation_id.clone(),
                                    run_id: run_id.clone(),
                                    turn_id: turn_id.clone(),
                                    provider_tool_call_id: tc_id.clone(),
                                    tool_name: tc_name.clone(),
                                    canonical_args_hash: args_hash.clone(),
                                    recovery_mode: "replay_safe".into(),
                                    current_state: "dispatched".into(),
                                    call_event_id: tc_id.clone(),
                                    result_event_id: None,
                                    dispatch_event_id: Some(dispatch_id.clone()),
                                    version: 1,
                                })
                                .await;
                        }

                        let (content, is_error) = dispatch_tool_call(
                            &gate, &store, &executor, &session_id, &run_id, &turn_id,
                            counter as i64, tc_id, tc_name, tc_args,
                        )
                        .await;

                        counter += 1;
                        let result_id = format!("aimux-{counter}");
                        yield SessionEvent::ToolResult {
                            id: result_id.clone(),
                            turn_id: turn_id.clone(),
                            ts: counter as i64,
                            tool_call_id: tc_id.clone(),
                            tool_name: tc_name.clone(),
                            content: content.clone(),
                            is_error,
                        };

                        // Advance the operation to completed.
                        if let Some(op_store) = &op_store {
                            let _ = op_store
                                .record_tool_operation(&ToolOperation {
                                    operation_id: op_id.clone(),
                                    invocation_id: invocation_id.clone(),
                                    run_id: run_id.clone(),
                                    turn_id: turn_id.clone(),
                                    provider_tool_call_id: tc_id.clone(),
                                    tool_name: tc_name.clone(),
                                    canonical_args_hash: args_hash.clone(),
                                    recovery_mode: "replay_safe".into(),
                                    current_state: "completed".into(),
                                    call_event_id: tc_id.clone(),
                                    result_event_id: Some(result_id),
                                    dispatch_event_id: Some(dispatch_id.clone()),
                                    version: 2,
                                })
                                .await;
                        }

                        messages.push(ChatMessage::Tool {
                            tool_call_id: tc_id.clone(),
                            content,
                        });
                    }
                    // continue agentLoop
                } else {
                    counter += 1;
                    yield done_event(&turn_id, counter, StopReason::EndTurn);
                    return;
                }
            }
        };

        stream.boxed()
    }

    async fn stop(
        &mut self,
        _reason: BackendStopReason,
        _mode: Option<BackendStopMode>,
    ) -> BackendResult<()> {
        Ok(())
    }

    async fn dispose(&mut self) -> BackendResult<()> {
        Ok(())
    }

    async fn compact_history(&self, messages: &[ChatMessage]) -> BackendResult<String> {
        let prompt = ModelPrompt::Messages(chat_to_aimux_messages(messages));
        let options = GenerateTextOptions {
            max_output_tokens: Some(4096),
            instructions: Some(
                "Summarize the following earlier conversation for continuation. Output ONLY plain text prose. Keep: the goal, work done, key decisions, exact paths/commands/results/errors, and next step. Be concise.".into(),
            ),
            ..Default::default()
        };
        let result = stream_text(&*self.model, prompt, options).await?;
        let mut summary = String::new();
        let mut stream = result.stream;
        while let Some(part) = stream.next().await {
            if let Ok(StreamPart::TextDelta { delta, .. }) = part {
                summary.push_str(&delta);
            }
        }
        Ok(if summary.is_empty() {
            "[compact: empty]".into()
        } else {
            summary
        })
    }
}

// ── conversion helpers ──

fn chat_to_aimux_messages(messages: &[ChatMessage]) -> Vec<ModelMessage> {
    messages
        .iter()
        .map(|m| match m {
            ChatMessage::User { content } => ModelMessage::text(Role::User, content.clone()),
            ChatMessage::Assistant { content, tool_calls, thinking } => {
                let mut parts = Vec::new();
                // Signed thinking blocks first (Anthropic requires this order).
                for tb in thinking {
                    parts.push(ContentPart::Reasoning {
                        text: tb.text.clone(),
                        signature: tb.signature.clone(),
                        provider_options: None,
                    });
                }
                if let Some(c) = content {
                    parts.push(ContentPart::Text {
                        text: c.clone(),
                        provider_options: None,
                    });
                }
                for tc in tool_calls {
                    parts.push(ContentPart::ToolCall {
                        tool_call_id: tc.id.clone(),
                        tool_name: tc.name.clone(),
                        input: tc.args.clone(),
                        thought_signature: None,
                        provider_options: None,
                    });
                }
                ModelMessage {
                    role: Role::Assistant,
                    content: MessageContent::Parts(parts),
                }
            }
            ChatMessage::Tool { tool_call_id, content } => ModelMessage {
                role: Role::Tool,
                content: MessageContent::Parts(vec![ContentPart::ToolResult {
                    tool_call_id: tool_call_id.clone(),
                    result: Value::String(content.clone()),
                    tool_name: None,
                    is_error: None,
                    preliminary: None,
                    dynamic: None,
                    provider_options: None,
                }]),
            },
        })
        .collect()
}

fn executor_to_aimux_tools(executor: &dyn ToolExecutor) -> Vec<AimuxTool> {
    executor
        .openai_functions()
        .into_iter()
        .map(|t| {
            let name = t["function"]["name"].as_str().unwrap_or("").to_string();
            let desc = t["function"]["description"].as_str().unwrap_or("").to_string();
            let schema = t["function"]["parameters"].clone();
            AimuxTool::Function(FunctionTool::new(name, schema).with_description(desc))
        })
        .collect()
}

/// Prune a tool result: if it exceeds the archive threshold, write the full
/// result to a file and return a preview + archive reference. Otherwise return
/// as-is. This is "active tool result pruning" (maka Chapter 2):
///
/// - The ledger stores the pruned summary (with archive path).
/// - The model sees only the preview + reference, not the full raw output.
/// - The full result is on disk for audit / re-projection.
///
/// The archive lives under `{tmpdir}/meepo-archives/{session_id}/{tool_call_id}.txt`.
fn prune_result(session_id: &str, tool_call_id: &str, content: String) -> String {
    let char_count = content.chars().count();
    if char_count <= ARCHIVE_THRESHOLD_CHARS {
        return content;
    }
    // Archive the full result to disk.
    let archive_dir = std::env::temp_dir()
        .join("meepo-archives")
        .join(session_id);
    let _ = std::fs::create_dir_all(&archive_dir);
    let archive_path = archive_dir.join(format!("{tool_call_id}.txt"));
    let _ = std::fs::write(&archive_path, &content);

    // Build a summary: preview + archive reference.
    let preview: String = content.chars().take(ARCHIVE_THRESHOLD_CHARS).collect();
    format!(
        "{preview}\n\n…[{char_count} chars total. Full result archived to {path}]",
        path = archive_path.display()
    )
}

fn done_event(turn_id: &str, counter: u64, stop_reason: StopReason) -> SessionEvent {
    SessionEvent::Complete {
        id: format!("aimux-{counter}"),
        turn_id: turn_id.to_string(),
        ts: counter as i64,
        stop_reason,
    }
}

fn err_event(turn_id: &str, counter: u64, message: String) -> SessionEvent {
    SessionEvent::Error {
        id: format!("aimux-{counter}-err"),
        turn_id: turn_id.to_string(),
        ts: counter as i64,
        recoverable: false,
        message,
        code: None,
        reason: None,
        details: None,
    }
}

/// Resolve one tool call: ask the permission gate (if any), persist the
/// canonical request + outcome (if a store is configured), then dispatch to
/// the executor. A denied call is NOT dispatched — it returns an error result
/// so the model learns the tool was refused.
async fn dispatch_tool_call(
    gate: &Option<Arc<dyn PermissionGate>>,
    store: &Option<Arc<dyn InteractionStore>>,
    executor: &Option<Arc<dyn ToolExecutor>>,
    session_id: &str,
    run_id: &str,
    turn_id: &str,
    created_at: i64,
    tool_call_id: &str,
    tool_name: &str,
    args: &Value,
) -> (String, bool) {
    let verdict = if let Some(gate) = gate {
        let resolution = gate.check(tool_call_id, tool_name, args).await;
        if let Some(store) = store {
            let outcome = PermissionOutcome {
                tool_call_id: tool_call_id.to_string(),
                decision: match resolution.verdict {
                    PermissionVerdict::Allow => PermissionDecision::Allow,
                    PermissionVerdict::Deny => PermissionDecision::Deny,
                },
                remember_for_turn: false,
                committed_at: created_at,
            };
            if let (Ok(request_json), Ok(outcome_json)) =
                (serde_json::to_string(&resolution.request), serde_json::to_string(&outcome))
            {
                let _ = store
                    .record_permission(
                        session_id, run_id, turn_id, tool_call_id, created_at,
                        &request_json, &outcome_json,
                    )
                    .await;
            }
        }
        resolution.verdict
    } else {
        PermissionVerdict::Allow
    };

    if matches!(verdict, PermissionVerdict::Deny) {
        return (
            format!("[permission denied: tool {tool_name} was not approved]"),
            true,
        );
    }
    match executor {
        Some(exec) => match exec.execute(tool_name, args).await {
            Ok(c) => (prune_result(session_id, tool_call_id, c), false),
            Err(e) => (prune_result(session_id, tool_call_id, e), true),
        },
        None => ("[no executor]".to_string(), true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meepo_core::{
        PermissionReason, PermissionRequest, PermissionResolution, StoreResult, ToolCategory,
    };
    use std::sync::Mutex;

    struct StubGate {
        verdict: PermissionVerdict,
    }

    #[async_trait]
    impl meepo_core::PermissionGate for StubGate {
        async fn check(&self, id: &str, name: &str, _args: &Value) -> PermissionResolution {
            PermissionResolution {
                verdict: self.verdict,
                request: PermissionRequest {
                    tool_call_id: id.into(),
                    tool_name: name.into(),
                    category: ToolCategory::ShellUnsafe,
                    reason: PermissionReason::ShellDangerous,
                    summary: "x".into(),
                    remember_for_turn_allowed: true,
                },
            }
        }
    }

    struct StubExecutor {
        called: Mutex<usize>,
    }

    #[async_trait]
    impl ToolExecutor for StubExecutor {
        async fn execute(&self, _name: &str, _args: &Value) -> Result<String, String> {
            *self.called.lock().unwrap() += 1;
            Ok("ok".into())
        }
        fn openai_functions(&self) -> Vec<Value> {
            vec![]
        }
    }

    struct StubStore {
        outcomes: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl meepo_core::InteractionStore for StubStore {
        async fn record_permission(
            &self,
            _session_id: &str,
            _run_id: &str,
            _turn_id: &str,
            _request_id: &str,
            _created_at: i64,
            _request_json: &str,
            outcome_json: &str,
        ) -> StoreResult<()> {
            self.outcomes.lock().unwrap().push(outcome_json.into());
            Ok(())
        }
    }

    #[tokio::test]
    async fn deny_does_not_dispatch_and_records_denied_outcome() {
        let gate: Arc<dyn PermissionGate> = Arc::new(StubGate { verdict: PermissionVerdict::Deny });
        let exec = Arc::new(StubExecutor { called: Mutex::new(0) });
        let store = Arc::new(StubStore { outcomes: Mutex::new(vec![]) });
        let (content, is_error) = dispatch_tool_call(
            &Some(gate), &Some(store.clone()), &Some(exec.clone()),
            "s", "r", "t", 1, "c1", "bash", &serde_json::json!({}),
        )
        .await;
        assert!(is_error);
        assert!(content.contains("permission denied"));
        assert_eq!(*exec.called.lock().unwrap(), 0, "denied call must not execute");
        let outs = store.outcomes.lock().unwrap();
        assert_eq!(outs.len(), 1);
        assert!(outs[0].contains("deny"), "outcome recorded as denied");
    }

    #[tokio::test]
    async fn allow_dispatches_and_records_allowed_outcome() {
        let gate: Arc<dyn PermissionGate> = Arc::new(StubGate { verdict: PermissionVerdict::Allow });
        let exec = Arc::new(StubExecutor { called: Mutex::new(0) });
        let store = Arc::new(StubStore { outcomes: Mutex::new(vec![]) });
        let (content, is_error) = dispatch_tool_call(
            &Some(gate), &Some(store.clone()), &Some(exec.clone()),
            "s", "r", "t", 1, "c1", "bash", &serde_json::json!({}),
        )
        .await;
        assert!(!is_error);
        assert_eq!(content, "ok");
        assert_eq!(*exec.called.lock().unwrap(), 1, "allowed call executes once");
        let outs = store.outcomes.lock().unwrap();
        assert_eq!(outs.len(), 1);
        assert!(outs[0].contains("allow"), "outcome recorded as allowed");
    }

    #[tokio::test]
    async fn no_gate_allows_without_recording() {
        let exec = Arc::new(StubExecutor { called: Mutex::new(0) });
        let (content, is_error) = dispatch_tool_call(
            &None, &None, &Some(exec.clone()),
            "s", "r", "t", 1, "c1", "bash", &serde_json::json!({}),
        )
        .await;
        assert!(!is_error);
        assert_eq!(content, "ok");
        assert_eq!(*exec.called.lock().unwrap(), 1);
    }
}
