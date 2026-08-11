//! OpenAI Chat Completions backend — streaming text + function calling,
//! with an internal agent tool loop (mirrors maka's sendWithinScope).
//!
//! `send()` runs an `agentLoop`: each iteration makes one streaming provider
//! request, consumes the SSE stream (text deltas + tool-call accumulation),
//! then either executes the tool calls via the injected `ToolExecutor` and
//! loops, or terminates with `Complete`.

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

pub struct OpenAiBackend {
    session_id: String,
    api_key: String,
    model: String,
    base_url: String,
    http: reqwest::Client,
    counter: u64,
    executor: Option<Arc<dyn ToolExecutor>>,
}

impl OpenAiBackend {
    pub fn new(
        session_id: impl Into<String>,
        model: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            api_key: api_key.into(),
            model: model.into(),
            base_url: "https://api.openai.com/v1".to_string(),
            http: reqwest::Client::new(),
            counter: 0,
            executor: None,
        }
    }

    pub fn from_env(
        session_id: impl Into<String>,
        model: impl Into<String>,
    ) -> Result<Self, String> {
        let key = std::env::var("OPENAI_API_KEY")
            .map_err(|_| "OPENAI_API_KEY is not set".to_string())?;
        let mut backend = Self::new(session_id, model, key);
        if let Ok(base) = std::env::var("OPENAI_BASE_URL") {
            backend = backend.with_base_url(base);
        }
        Ok(backend)
    }

    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
        self
    }

    /// Inject a tool executor so `send()` can drive an internal tool loop.
    pub fn with_executor(mut self, executor: Arc<dyn ToolExecutor>) -> Self {
        self.executor = Some(executor);
        self
    }
}

#[async_trait]
impl AgentBackend for OpenAiBackend {
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

                // --- Build provider request ---
                let mut openai_messages = Vec::new();
                if let Some(sys) = &system_prompt {
                    openai_messages.push(json!({ "role": "system", "content": sys }));
                }
                openai_messages.extend(messages_to_openai(&messages));
                if std::env::var("MEPEO_DEBUG").is_ok() {
                    eprintln!(
                        "[meepo] request messages:\n{}",
                        serde_json::to_string_pretty(&openai_messages).unwrap_or_default()
                    );
                }
                let mut body = json!({ "model": model, "messages": openai_messages, "stream": true });
                if !tool_defs.is_empty() {
                    body["tools"] = json!(tool_defs);
                    body["tool_choice"] = json!("auto");
                }

                // --- Send request ---
                let resp = match http
                    .post(format!("{base_url}/chat/completions"))
                    .bearer_auth(&key)
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
                    yield err_event(&turn_id, counter, format!("openai {status}: {text}"));
                    return;
                }

                // --- Consume SSE stream ---
                let mut byte_stream = resp.bytes_stream();
                let mut buf = String::new();
                let mut tool_calls: BTreeMap<u32, ToolCallAccum> = BTreeMap::new();
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
                    while let Some(nl) = buf.find('\n') {
                        let line: String = buf[..nl].trim_end_matches('\r').to_string();
                        buf.drain(..=nl);
                        let data = match line.strip_prefix("data: ") {
                            Some(d) => d,
                            None => continue,
                        };
                        if data.trim() == "[DONE]" {
                            break 'outer;
                        }
                        let Some(parsed) = parse_chunk(data) else {
                            continue;
                        };
                        if let Some(text) = parsed.content {
                            counter += 1;
                            yield SessionEvent::TextDelta {
                                id: format!("oai-{counter}"),
                                turn_id: turn_id.clone(),
                                ts: counter as i64,
                                message_id: "oai-message".to_string(),
                                start_offset: None,
                                text,
                            };
                        }
                        for tcd in parsed.tool_call_deltas {
                            let entry = tool_calls.entry(tcd.index).or_default();
                            if let Some(id) = tcd.id {
                                entry.id = Some(id);
                            }
                            if let Some(name) = tcd.name {
                                entry.name = Some(name);
                            }
                            if let Some(args) = tcd.arguments {
                                entry.args.push_str(&args);
                            }
                        }
                        if let Some(fr) = parsed.finish_reason {
                            finish_reason = Some(fr);
                        }
                    }
                }

                // --- Step end: tool calls or terminal ---
                if finish_reason.as_deref() == Some("tool_calls") || !tool_calls.is_empty() {
                    // Record assistant tool_calls in history.
                    let calls: Vec<AssistantToolCall> = tool_calls
                        .values()
                        .map(|a| AssistantToolCall {
                            id: a.id.clone().unwrap_or_default(),
                            name: a.name.clone().unwrap_or_default(),
                            args: serde_json::from_str(&a.args).unwrap_or(Value::Null),
                        })
                        .collect();
                    messages.push(ChatMessage::Assistant {
                        content: None,
                        tool_calls: calls,
                    });

                    // Execute each tool (parallel in maka; sequential here for now).
                    for accum in tool_calls.values() {
                        let tc_id = accum.id.clone().unwrap_or_default();
                        let tc_name = accum.name.clone().unwrap_or_default();
                        let tc_args: Value =
                            serde_json::from_str(&accum.args).unwrap_or(Value::Null);

                        counter += 1;
                        yield SessionEvent::ToolCall {
                            id: accum
                                .id
                                .clone()
                                .unwrap_or_else(|| format!("oai-{counter}")),
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
                            id: format!("oai-{counter}"),
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
                    // continue agentLoop — next step carries tool results.
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
        let mut req = vec![json!({
            "role": "system",
            "content": "Summarize the following earlier conversation for continuation. Output ONLY plain text prose. Do NOT output tool calls, function calls, DSML tags, code blocks, or suggested actions. Keep: the goal, work done, key decisions, exact paths/commands/results/errors, and the next step. Be concise."
        })];
        req.extend(messages_to_openai(messages));
        let body = json!({ "model": self.model, "messages": req });
        let resp = self
            .http
            .post(format!("{}/chat/completions", self.base_url))
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await?;
        let v: Value = resp.json().await?;
        Ok(v["choices"][0]["message"]["content"]
            .as_str()
            .unwrap_or("[compact: empty response]")
            .to_string())
    }
}

// ── helpers ──

#[derive(Default)]
struct ToolCallAccum {
    id: Option<String>,
    name: Option<String>,
    args: String,
}

struct Chunk {
    content: Option<String>,
    tool_call_deltas: Vec<ToolCallDelta>,
    finish_reason: Option<String>,
}

struct ToolCallDelta {
    index: u32,
    id: Option<String>,
    name: Option<String>,
    arguments: Option<String>,
}

fn parse_chunk(data: &str) -> Option<Chunk> {
    let v: Value = serde_json::from_str(data).ok()?;
    let choice = &v["choices"][0];
    let delta = &choice["delta"];
    let content = delta["content"]
        .as_str()
        .filter(|s| !s.is_empty())
        .map(String::from);
    let mut tool_call_deltas = Vec::new();
    if let Some(tcs) = delta["tool_calls"].as_array() {
        for tc in tcs {
            tool_call_deltas.push(ToolCallDelta {
                index: tc["index"].as_u64().unwrap_or(0) as u32,
                id: tc["id"].as_str().map(String::from),
                name: tc["function"]["name"].as_str().map(String::from),
                arguments: tc["function"]["arguments"].as_str().map(String::from),
            });
        }
    }
    let finish_reason = choice["finish_reason"].as_str().map(String::from);
    if content.is_none() && tool_call_deltas.is_empty() && finish_reason.is_none() {
        return None;
    }
    Some(Chunk { content, tool_call_deltas, finish_reason })
}

fn messages_to_openai(messages: &[ChatMessage]) -> Vec<Value> {
    messages
        .iter()
        .map(|m| match m {
            ChatMessage::User { content } => json!({ "role": "user", "content": content }),
            ChatMessage::Assistant { content, tool_calls } => {
                let mut v = json!({ "role": "assistant" });
                if let Some(c) = content {
                    v["content"] = json!(c);
                }
                if !tool_calls.is_empty() {
                    v["tool_calls"] = json!(tool_calls.iter().map(|tc| json!({
                        "id": tc.id,
                        "type": "function",
                        "function": { "name": tc.name, "arguments": serde_json::to_string(&tc.args).unwrap_or_default() }
                    })).collect::<Vec<_>>());
                }
                v
            }
            ChatMessage::Tool { tool_call_id, content } => {
                json!({ "role": "tool", "tool_call_id": tool_call_id, "content": content })
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
        id: format!("oai-{counter}"),
        turn_id: turn_id.to_string(),
        ts: counter as i64,
        stop_reason,
    }
}

fn err_event(turn_id: &str, counter: u64, message: String) -> SessionEvent {
    SessionEvent::Error {
        id: format!("oai-{counter}-err"),
        turn_id: turn_id.to_string(),
        ts: counter as i64,
        recoverable: false,
        message,
        code: None,
        reason: None,
        details: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_text_delta_chunk() {
        let data = r#"{"choices":[{"delta":{"content":"hi"}}]}"#;
        let c = parse_chunk(data).unwrap();
        assert_eq!(c.content.as_deref(), Some("hi"));
    }

    #[test]
    fn parses_tool_call_first_delta() {
        let data = r#"{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"read_file","arguments":""}}]}}]}"#;
        let c = parse_chunk(data).unwrap();
        assert_eq!(c.tool_call_deltas[0].id.as_deref(), Some("call_1"));
    }

    #[test]
    fn parses_finish_reason() {
        let data = r#"{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}"#;
        assert_eq!(parse_chunk(data).unwrap().finish_reason.as_deref(), Some("tool_calls"));
    }

    #[test]
    fn ignores_empty_chunk() {
        assert!(parse_chunk(r#"{"choices":[{"delta":{}}]}"#).is_none());
    }

    #[test]
    fn messages_to_openai_emits_valid_tool_pair() {
        use meepo_core::{AssistantToolCall, ChatMessage};
        let msgs = vec![
            ChatMessage::User { content: "do it".into() },
            ChatMessage::Assistant {
                content: None,
                tool_calls: vec![AssistantToolCall {
                    id: "call_00".into(),
                    name: "bash".into(),
                    args: json!({ "command": "echo hi" }),
                }],
            },
            ChatMessage::Tool { tool_call_id: "call_00".into(), content: "hi".into() },
        ];
        let out = messages_to_openai(&msgs);
        assert_eq!(out[1]["tool_calls"][0]["function"]["arguments"].is_string(), true);
        assert_eq!(out[2]["tool_call_id"], "call_00");
    }
}
