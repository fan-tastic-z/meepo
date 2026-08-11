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
    BackendStopMode, BackendStopReason, ChatMessage, SessionEvent, StopReason, ToolExecutor,
};

const MAX_TOOL_RESULT_CHARS: usize = 8_000;

pub struct AimuxBackend {
    session_id: String,
    model: Box<dyn LanguageModel>,
    counter: u64,
    executor: Option<Arc<dyn ToolExecutor>>,
}

impl AimuxBackend {
    pub fn new(session_id: impl Into<String>, model: Box<dyn LanguageModel>) -> Self {
        Self {
            session_id: session_id.into(),
            model,
            counter: 0,
            executor: None,
        }
    }

    pub fn with_executor(mut self, executor: Arc<dyn ToolExecutor>) -> Self {
        self.executor = Some(executor);
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
        let turn_id = input.turn_id.clone();
        let system_prompt = input.system_prompt.clone();
        let max_steps = input.max_steps.unwrap_or(50);
        let executor = self.executor.clone();
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
                let mut thinking_text = String::new();
                let mut thinking_msg_id = String::new();
                let mut stream = result.stream;
                while let Some(part_result) = stream.next().await {
                    match part_result {
                        Ok(StreamPart::TextDelta { delta, id, .. }) => {
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
                        Ok(StreamPart::ReasoningStart { id, .. }) => {
                            thinking_text.clear();
                            thinking_msg_id = id;
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
                            // Extract signature from provider_metadata (Anthropic stores it there).
                            let signature = provider_metadata
                                .as_ref()
                                .and_then(|m| m.get("anthropic"))
                                .and_then(|a| a.get("signature"))
                                .and_then(|s| s.as_str())
                                .map(|s| s.to_string());
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
                    messages.push(ChatMessage::Assistant { content: None, tool_calls: calls, thinking: vec![] });

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

                        let content = if let Some(ref exec) = executor {
                            match exec.execute(tc_name, tc_args).await {
                                Ok(c) => truncate(c),
                                Err(e) => truncate(e),
                            }
                        } else {
                            "[no executor]".to_string()
                        };

                        counter += 1;
                        yield SessionEvent::ToolResult {
                            id: format!("aimux-{counter}"),
                            turn_id: turn_id.clone(),
                            ts: counter as i64,
                            tool_call_id: tc_id.clone(),
                            tool_name: tc_name.clone(),
                            content: content.clone(),
                            is_error: false,
                        };
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

fn truncate(content: String) -> String {
    if content.chars().count() <= MAX_TOOL_RESULT_CHARS {
        return content;
    }
    let mut head: String = content.chars().take(MAX_TOOL_RESULT_CHARS).collect();
    head.push_str("\n…[truncated]");
    head
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
