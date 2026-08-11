//! Anthropic Messages API backend — streaming + function calling.
//!
//! Implements [`AgentBackend`] against the Anthropic Messages API with
//! `stream: true`. Parses the Anthropic SSE event-stream format
//! (event:content_block_delta + data:json) and maps to SessionEvents.
//!
//! Key differences from OpenAI:
//! - Endpoint: /v1/messages (not /chat/completions)
//! - Headers: x-api-key + anthropic-version (not Authorization Bearer)
//! - Messages: content blocks array (not string + tool_calls)
//! - SSE: event: type + data: json pairs (not data: only)
//! - Tool results: role:user + type:tool_result (not role:tool)

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use serde_json::{json, Value};

use meepo_core::{
    AgentBackend, AssistantToolCall, BackendKind, BackendResult, BackendSendInput,
    BackendStopMode, BackendStopReason, ChatMessage, SessionEvent, StopReason, ToolExecutor,
};

const MAX_TOOL_RESULT_CHARS: usize = 8_000;
const ANTHROPIC_VERSION: &str = "2023-06-01";

pub struct AnthropicBackend {
    session_id: String,
    api_key: String,
    model: String,
    base_url: String,
    http: reqwest::Client,
    counter: u64,
    executor: Option<Arc<dyn ToolExecutor>>,
}

impl AnthropicBackend {
    pub fn new(
        session_id: impl Into<String>,
        model: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            api_key: api_key.into(),
            model: model.into(),
            base_url: "https://api.anthropic.com".to_string(),
            http: reqwest::Client::new(),
            counter: 0,
            executor: None,
        }
    }

    pub fn from_env(
        session_id: impl Into<String>,
        model: impl Into<String>,
    ) -> Result<Self, String> {
        let key = std::env::var("ANTHROPIC_API_KEY")
            .map_err(|_| "ANTHROPIC_API_KEY is not set".to_string())?;
        let mut backend = Self::new(session_id, model, key);
        if let Ok(base) = std::env::var("ANTHROPIC_BASE_URL") {
            backend = backend.with_base_url(base);
        }
        Ok(backend)
    }

    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
        self
    }

    pub fn with_executor(mut self, executor: Arc<dyn ToolExecutor>) -> Self {
        self.executor = Some(executor);
        self
    }
}

#[async_trait]
impl AgentBackend for AnthropicBackend {
    fn kind(&self) -> BackendKind {
        BackendKind::AiSdk
    }

    fn session_id(&self) -> &str {
        &self.session_id
    }

    fn send<'a>(&'a mut self, input: &'a BackendSendInput) -> BoxStream<'a, SessionEvent> {
        let model = self.model.clone();
        let key = self.api_key.clone();
        let base_url = self.base_url.clone();
        let http = self.http.clone();
        let executor = self.executor.clone();
        let mut messages = input.messages.clone();
        let system_prompt = input.system_prompt.clone();
        let turn_id = input.turn_id.clone();
        let max_steps = input.max_steps.unwrap_or(50);
        let tool_defs = executor
            .as_ref()
            .map(|e| e.openai_functions())
            .unwrap_or_default();
        let mut counter = self.counter;
        self.counter = self.counter.saturating_add(1_000_000);

        let stream = async_stream::stream! {
            let mut step = 0u32;
            loop {
                step += 1;
                if step > max_steps {
                    counter += 1;
                    yield done_event(&turn_id, counter, StopReason::StepLimit);
                    return;
                }

                // --- Build Anthropic Messages request ---
                let anthropic_messages = messages_to_anthropic(&messages);
                if std::env::var("MEPEO_DEBUG").is_ok() {
                    eprintln!("[meepo] anthropic messages:\n{}", serde_json::to_string_pretty(&anthropic_messages).unwrap_or_default());
                }

                // Convert OpenAI-style tool defs to Anthropic format.
                let anthropic_tools: Vec<Value> = tool_defs.iter().map(|t| {
                    json!({
                        "name": t["function"]["name"],
                        "description": t["function"]["description"],
                        "input_schema": t["function"]["parameters"]
                    })
                }).collect();

                let mut body = json!({
                    "model": model,
                    "max_tokens": 8192,
                    "messages": anthropic_messages,
                    "stream": true,
                });
                if let Some(ref sys) = system_prompt {
                    body["system"] = json!(sys);
                }
                if !anthropic_tools.is_empty() {
                    body["tools"] = json!(anthropic_tools);
                }

                // --- Send request ---
                let resp = match http
                    .post(format!("{base_url}/v1/messages"))
                    .header("x-api-key", &key)
                    .header("anthropic-version", ANTHROPIC_VERSION)
                    .header("content-type", "application/json")
                    .json(&body)
                    .send()
                    .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        counter += 1;
                        yield err_event(&turn_id, counter, format!("request failed: {e}"));
                        return;
                    }
                };
                if !resp.status().is_success() {
                    let status = resp.status();
                    let text = resp.text().await.unwrap_or_default();
                    counter += 1;
                    yield err_event(&turn_id, counter, format!("anthropic {status}: {text}"));
                    return;
                }

                // --- Consume Anthropic SSE stream ---
                // Format: "event: <type>\ndata: <json>\n\n"
                let mut byte_stream = resp.bytes_stream();
                let mut buf = String::new();
                let mut current_event = String::new();
                let mut tool_use_blocks: BTreeMap<usize, ToolUseAccum> = BTreeMap::new();
                let mut finish_reason: Option<String> = None;

                'outer: while let Some(chunk) = byte_stream.next().await {
                    let chunk = match chunk {
                        Ok(c) => c,
                        Err(e) => {
                            counter += 1;
                            yield err_event(&turn_id, counter, format!("stream read: {e}"));
                            return;
                        }
                    };
                    buf.push_str(&String::from_utf8_lossy(chunk.as_ref()));

                    // Process complete SSE events (separated by \n\n).
                    while let Some(pos) = buf.find("\n\n") {
                        let raw_event = buf[..pos].to_string();
                        buf.drain(..=pos + 1);

                        // Parse event lines.
                        for line in raw_event.lines() {
                            if let Some(ev) = line.strip_prefix("event: ") {
                                current_event = ev.trim().to_string();
                            } else if let Some(data) = line.strip_prefix("data: ") {
                                let v: Value = match serde_json::from_str(data) {
                                    Ok(v) => v,
                                    Err(_) => continue,
                                };
                                let data_type = v["type"].as_str().unwrap_or("");

                                match data_type {
                                    "content_block_start" => {
                                        let index = v["index"].as_u64().unwrap_or(0) as usize;
                                        let block_type = v["content_block"]["type"].as_str().unwrap_or("");
                                        if block_type == "tool_use" {
                                            tool_use_blocks.insert(index, ToolUseAccum {
                                                id: v["content_block"]["id"].as_str().unwrap_or("").to_string(),
                                                name: v["content_block"]["name"].as_str().unwrap_or("").to_string(),
                                                input_json: String::new(),
                                            });
                                        }
                                    }
                                    "content_block_delta" => {
                                        let index = v["index"].as_u64().unwrap_or(0) as usize;
                                        let delta_type = v["delta"]["type"].as_str().unwrap_or("");
                                        match delta_type {
                                            "text_delta" => {
                                                if let Some(text) = v["delta"]["text"].as_str() {
                                                    counter += 1;
                                                    yield SessionEvent::TextDelta {
                                                        id: format!("ant-{counter}"),
                                                        turn_id: turn_id.clone(),
                                                        ts: counter as i64,
                                                        message_id: "ant-message".to_string(),
                                                        start_offset: None,
                                                        text: text.to_string(),
                                                    };
                                                }
                                            }
                                            "input_json_delta" => {
                                                if let Some(partial) = v["delta"]["partial_json"].as_str() {
                                                    if let Some(block) = tool_use_blocks.get_mut(&index) {
                                                        block.input_json.push_str(partial);
                                                    }
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                    "message_delta" => {
                                        if let Some(reason) = v["delta"]["stop_reason"].as_str() {
                                            finish_reason = Some(reason.to_string());
                                        }
                                    }
                                    "message_stop" => {
                                        break 'outer;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }

                // --- Step end: tool calls or terminal ---
                if finish_reason.as_deref() == Some("tool_use") || !tool_use_blocks.is_empty() {
                    let calls: Vec<AssistantToolCall> = tool_use_blocks.values().map(|a| {
                        AssistantToolCall {
                            id: a.id.clone(),
                            name: a.name.clone(),
                            args: serde_json::from_str(&a.input_json).unwrap_or(Value::Null),
                        }
                    }).collect();
                    messages.push(ChatMessage::Assistant {
                        content: None,
                        tool_calls: calls,
                    });

                    for accum in tool_use_blocks.values() {
                        let tc_id = accum.id.clone();
                        let tc_name = accum.name.clone();
                        let tc_args: Value = serde_json::from_str(&accum.input_json).unwrap_or(Value::Null);

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
                            match exec.execute(&tc_name, &tc_args).await {
                                Ok(c) => truncate(c),
                                Err(e) => truncate(e),
                            }
                        } else {
                            "[no tool executor]".to_string()
                        };

                        counter += 1;
                        yield SessionEvent::ToolResult {
                            id: format!("ant-{counter}"),
                            turn_id: turn_id.clone(),
                            ts: counter as i64,
                            tool_call_id: tc_id.clone(),
                            tool_name: tc_name.clone(),
                            content: content.clone(),
                            is_error: false,
                        };
                        messages.push(ChatMessage::Tool {
                            tool_call_id: tc_id,
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
        // Use the Anthropic Messages API (non-streaming) for summarization.
        let anthropic_messages = messages_to_anthropic(messages);
        let body = json!({
            "model": self.model,
            "max_tokens": 4096,
            "system": "Summarize the following earlier conversation for continuation. Output ONLY plain text prose. Do NOT output tool calls or suggested actions. Keep: the goal, work done, key decisions, exact paths/commands/results/errors, and next step. Be concise.",
            "messages": anthropic_messages,
        });
        let resp = self.http
            .post(format!("{}/v1/messages", self.base_url))
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", ANTHROPIC_VERSION)
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await?;
        let v: Value = resp.json().await?;
        // Anthropic response: content[0].text
        Ok(v["content"][0]["text"]
            .as_str()
            .unwrap_or("[compact: empty response]")
            .to_string())
    }
}

// ── helpers ──

#[derive(Default)]
struct ToolUseAccum {
    id: String,
    name: String,
    input_json: String,
}

/// Convert ChatMessages to Anthropic Messages API format.
/// Anthropic uses content blocks arrays (not string + tool_calls).
fn messages_to_anthropic(messages: &[ChatMessage]) -> Vec<Value> {
    messages
        .iter()
        .map(|m| match m {
            ChatMessage::User { content } => {
                json!({"role": "user", "content": [{"type": "text", "text": content}]})
            }
            ChatMessage::Assistant { content, tool_calls } => {
                let mut blocks = Vec::new();
                if let Some(c) = content {
                    blocks.push(json!({"type": "text", "text": c}));
                }
                for tc in tool_calls {
                    blocks.push(json!({
                        "type": "tool_use",
                        "id": tc.id,
                        "name": tc.name,
                        "input": tc.args,
                    }));
                }
                json!({"role": "assistant", "content": blocks})
            }
            // Anthropic: tool results go as role:user + type:tool_result.
            ChatMessage::Tool { tool_call_id, content } => {
                json!({
                    "role": "user",
                    "content": [{
                        "type": "tool_result",
                        "tool_use_id": tool_call_id,
                        "content": content,
                    }],
                })
            }
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
        id: format!("ant-{counter}"),
        turn_id: turn_id.to_string(),
        ts: counter as i64,
        stop_reason,
    }
}

fn err_event(turn_id: &str, counter: u64, message: String) -> SessionEvent {
    SessionEvent::Error {
        id: format!("ant-{counter}-err"),
        turn_id: turn_id.to_string(),
        ts: counter as i64,
        recoverable: false,
        message,
        code: None,
        reason: None,
        details: None,
    }
}
