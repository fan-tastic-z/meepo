//! OpenAI Chat Completions backend — streaming text + function calling.
//!
//! Implements [`AgentBackend`] against chat/completions with `stream: true`.
//! Text deltas map to [`SessionEvent::TextDelta`]; tool-call deltas are
//! accumulated by index and emitted as [`SessionEvent::ToolCall`] when the
//! stream finishes with tool calls. A `stop` finish (or a stream with no tool
//! calls) terminates with [`SessionEvent::Complete`].

use std::collections::BTreeMap;

use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use serde_json::{json, Value};

use meepo_core::{
    AgentBackend, BackendKind, BackendResult, BackendSendInput, BackendStopMode,
    BackendStopReason, ChatMessage, SessionEvent, StopReason,
};

pub struct OpenAiBackend {
    session_id: String,
    api_key: String,
    model: String,
    base_url: String,
    http: reqwest::Client,
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
        let messages = input.messages.clone();
        let tools = input.tools.clone();
        let turn_id = input.turn_id.clone();

        let stream = async_stream::stream! {
            let mut body = json!({
                "model": model,
                "messages": messages_to_openai(&messages),
                "stream": true,
            });
            if !tools.is_empty() {
                body["tools"] = json!(tools);
                body["tool_choice"] = json!("auto");
            }

            let resp = match http
                .post(format!("{base_url}/chat/completions"))
                .bearer_auth(&key)
                .json(&body)
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    yield error_event(&turn_id, format!("request failed: {e}"));
                    return;
                }
            };
            if !resp.status().is_success() {
                let status = resp.status();
                let text = resp.text().await.unwrap_or_default();
                yield error_event(&turn_id, format!("openai {status}: {text}"));
                return;
            }

            let mut byte_stream = resp.bytes_stream();
            let mut buf = String::new();
            let mut counter: u64 = 0;
            let mut tool_calls: BTreeMap<u32, ToolCallAccum> = BTreeMap::new();
            let mut finish_reason: Option<String> = None;

            'outer: while let Some(chunk) = byte_stream.next().await {
                let chunk = match chunk {
                    Ok(c) => c,
                    Err(e) => {
                        yield error_event(&turn_id, format!("stream read: {e}"));
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

            // Terminal: tool calls win if any; otherwise completion.
            if finish_reason.as_deref() == Some("tool_calls") || !tool_calls.is_empty() {
                for (index, accum) in tool_calls {
                    counter += 1;
                    let args: Value = serde_json::from_str(&accum.args).unwrap_or(Value::Null);
                    yield SessionEvent::ToolCall {
                        id: format!("oai-tc-{index}"),
                        turn_id: turn_id.clone(),
                        ts: counter as i64,
                        tool_call_id: accum.id.unwrap_or_default(),
                        tool_name: accum.name.unwrap_or_default(),
                        args,
                    };
                }
            } else {
                yield complete_event(&turn_id, counter);
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
}

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

/// Parse one SSE `data:` payload (excluding `[DONE]`).
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
    Some(Chunk {
        content,
        tool_call_deltas,
        finish_reason,
    })
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

fn error_event(turn_id: &str, message: String) -> SessionEvent {
    SessionEvent::Error {
        id: format!("oai-err-{turn_id}"),
        turn_id: turn_id.to_string(),
        ts: 0,
        recoverable: false,
        message,
        code: None,
        reason: None,
        details: None,
    }
}

fn complete_event(turn_id: &str, counter: u64) -> SessionEvent {
    SessionEvent::Complete {
        id: format!("oai-done-{counter}"),
        turn_id: turn_id.to_string(),
        ts: counter as i64,
        stop_reason: StopReason::EndTurn,
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
        assert!(c.tool_call_deltas.is_empty());
    }

    #[test]
    fn parses_tool_call_first_delta() {
        let data = r#"{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"read_file","arguments":""}}]}}]}"#;
        let c = parse_chunk(data).unwrap();
        assert_eq!(c.tool_call_deltas.len(), 1);
        let tc = &c.tool_call_deltas[0];
        assert_eq!(tc.index, 0);
        assert_eq!(tc.id.as_deref(), Some("call_1"));
        assert_eq!(tc.name.as_deref(), Some("read_file"));
    }

    #[test]
    fn parses_tool_call_argument_fragment() {
        let data = r#"{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"pa"}}]}}]}"#;
        let c = parse_chunk(data).unwrap();
        assert_eq!(c.tool_call_deltas[0].arguments.as_deref(), Some("{\"pa"));
        assert!(c.tool_call_deltas[0].id.is_none());
    }

    #[test]
    fn parses_finish_reason() {
        let data = r#"{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}"#;
        let c = parse_chunk(data).unwrap();
        assert_eq!(c.finish_reason.as_deref(), Some("tool_calls"));
    }

    #[test]
    fn ignores_empty_chunk() {
        let data = r#"{"choices":[{"delta":{}}]}"#;
        assert!(parse_chunk(data).is_none());
    }
}
