//! OpenAI Chat Completions backend — streaming text (walking skeleton).
//!
//! Implements [`AgentBackend`] by calling chat/completions with `stream: true`,
//! mapping SSE deltas into [`SessionEvent::TextDelta`] and terminating with
//! [`SessionEvent::Complete`] (or [`SessionEvent::Error`]). No function calling
//! yet (the request omits `tools`); that arrives in a later phase.

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
        let turn_id = input.turn_id.clone();

        let stream = async_stream::stream! {
            let body = json!({
                "model": model,
                "messages": messages_to_openai(&messages),
                "stream": true,
            });
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
            let mut saw_done = false;

            while let Some(chunk) = byte_stream.next().await {
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
                    match parse_sse_line(&line) {
                        Some(Sse::Delta(text)) => {
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
                        Some(Sse::Done) => {
                            saw_done = true;
                            yield complete_event(&turn_id, counter);
                            return;
                        }
                        Some(Sse::Other) | None => {}
                    }
                }
            }
            if !saw_done {
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

enum Sse {
    Delta(String),
    Done,
    Other,
}

fn parse_sse_line(line: &str) -> Option<Sse> {
    let data = line.strip_prefix("data: ")?;
    if data.trim() == "[DONE]" {
        return Some(Sse::Done);
    }
    let v: Value = serde_json::from_str(data).ok()?;
    match v["choices"][0]["delta"]["content"].as_str() {
        Some(t) if !t.is_empty() => Some(Sse::Delta(t.to_string())),
        _ => Some(Sse::Other),
    }
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
    fn parses_delta_content() {
        let line = r#"data: {"choices":[{"delta":{"content":"hello"}}]}"#;
        assert!(matches!(parse_sse_line(line), Some(Sse::Delta(t)) if t == "hello"));
    }

    #[test]
    fn parses_done_sentinel() {
        assert!(matches!(parse_sse_line("data: [DONE]"), Some(Sse::Done)));
    }

    #[test]
    fn ignores_comments_and_empty_deltas() {
        assert!(parse_sse_line(": heartbeat").is_none());
        assert!(matches!(
            parse_sse_line(r#"data: {"choices":[{"delta":{}}]}"#),
            Some(Sse::Other)
        ));
        assert!(parse_sse_line("event: ping").is_none());
    }

    #[test]
    fn renders_chat_messages_to_openai_shape() {
        let msgs = vec![
            ChatMessage::User { content: "hi".into() },
            ChatMessage::Assistant {
                content: Some("thinking".into()),
                tool_calls: vec![AssistantToolCallLite {
                    id: "call_1".into(),
                    name: "read_file".into(),
                    args: json!({"path": "/x"}),
                }
                .to_assistant()],
            },
        ];
        let out = messages_to_openai(&msgs);
        assert_eq!(out[0]["role"], "user");
        assert_eq!(out[1]["role"], "assistant");
        assert_eq!(out[1]["tool_calls"][0]["function"]["name"], "read_file");
    }

    // Helper kept local so the test doesn't depend on meepo-core's exact name.
    struct AssistantToolCallLite {
        id: String,
        name: String,
        args: Value,
    }
    impl AssistantToolCallLite {
        fn to_assistant(self) -> meepo_core::AssistantToolCall {
            meepo_core::AssistantToolCall {
                id: self.id,
                name: self.name,
                args: self.args,
            }
        }
    }
}
