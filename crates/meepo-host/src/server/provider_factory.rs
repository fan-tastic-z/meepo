//! The daemon's backend factory — builds a concrete [`AgentBackend`] per turn
//! from provider configuration (mirroring the CLI's embedded construction):
//! aimux OpenAI/Anthropic providers from env keys, or a scripted fake fallback
//! (so a keyless daemon still boots). Permission prompts route through the
//! [`InteractionHub`] so the CLI answers them over the wire.

use std::sync::Arc;

use meepo_core::{
    AgentBackend, PermissionGate, PermissionMode, PermissionPrompter, SessionEvent, StopReason,
};
use meepo_providers::AimuxBackend;
use meepo_storage::SqliteStore;

use crate::server::interaction::{HubPrompter, InteractionContext, InteractionHub};
use crate::server::BackendFactory;

pub const DEFAULT_OPENAI_MODEL: &str = "deepseek-v4-flash";
pub const DEFAULT_ANTHROPIC_MODEL: &str = "claude-sonnet-4-20250514";

/// Build the daemon's backend factory.
///
/// `provider`: "fake" | "openai" | "anthropic". Missing API keys fall back to
/// the fake backend (with a warning) so a keyless daemon stays bootable.
pub fn daemon_backend_factory(
    provider: &str,
    model: Option<String>,
    base_url: Option<String>,
    mode: PermissionMode,
    hub: Arc<InteractionHub>,
    host_epoch: String,
    store: Arc<SqliteStore>,
) -> BackendFactory {
    let provider = provider.to_string();
    Arc::new(move |session_id: &str| {
        let mut sm = meepo_sandbox::SandboxManager::new();
        #[cfg(target_os = "macos")]
        sm.register(Box::new(meepo_sandbox::MacosSeatbeltBackend::new()));
        let tools: Arc<meepo_tools::ToolRegistry> = {
            let mut t = meepo_tools::ToolRegistry::new();
            for tool in meepo_tools::all_with_sandbox(Arc::new(sm)) {
                t.register(tool);
            }
            Arc::new(t)
        };

        let gate: Option<Arc<dyn PermissionGate>> = if mode == PermissionMode::Bypass {
            None
        } else {
            let prompter: Arc<dyn PermissionPrompter> = Arc::new(HubPrompter::new(
                hub.clone(),
                InteractionContext {
                    session_id: session_id.to_string(),
                    // The factory does not know the per-turn ids; the canonical
                    // interaction record is session-scoped (run/turn empty).
                    run_id: String::new(),
                    turn_id: String::new(),
                },
                host_epoch.clone(),
            ));
            Some(Arc::new(meepo_core::DefaultPermissionGate::new(mode, prompter)))
        };

        let mut backend: AimuxBackend = match provider.as_str() {
            "openai" => match std::env::var("OPENAI_API_KEY") {
                Ok(key) => {
                    let model_name = model.clone().unwrap_or_else(|| DEFAULT_OPENAI_MODEL.into());
                    let url = base_url
                        .clone()
                        .or_else(|| std::env::var("OPENAI_BASE_URL").ok())
                        .unwrap_or_else(|| "https://api.openai.com/v1".into());
                    let config = aimux_providers::openai::OpenAIConfig::new(&key).with_base_url(&url);
                    let provider = aimux_providers::openai::OpenAIProvider::new(config);
                    AimuxBackend::new(session_id, Box::new(provider.model(&model_name)))
                }
                Err(_) => {
                    eprintln!("[meepo-host] OPENAI_API_KEY not set; falling back to the fake backend");
                    return fake_backend(session_id);
                }
            },
            "anthropic" => {
                let auth_token = std::env::var("ANTHROPIC_AUTH_TOKEN").ok();
                let api_key = std::env::var("ANTHROPIC_API_KEY").ok();
                if api_key.is_none() && auth_token.is_none() {
                    eprintln!(
                        "[meepo-host] ANTHROPIC_API_KEY/AUTH_TOKEN not set; falling back to the fake backend"
                    );
                    return fake_backend(session_id);
                }
                let url = base_url
                    .clone()
                    .or_else(|| std::env::var("ANTHROPIC_BASE_URL").ok())
                    .unwrap_or_else(|| "https://api.anthropic.com".into());
                let config = aimux_providers::anthropic::AnthropicConfig {
                    api_key: api_key.unwrap_or_default(),
                    auth_token,
                    base_url: url,
                    api_version: "2023-06-01".into(),
                    name: "anthropic".into(),
                    headers: None,
                    retry_config: Default::default(),
                    body_overrides: None,
                    api_key_source: None,
                };
                let provider = aimux_providers::anthropic::AnthropicProvider::new(config);
                let model_name = model.clone().unwrap_or_else(|| DEFAULT_ANTHROPIC_MODEL.into());
                AimuxBackend::new(session_id, Box::new(provider.model(&model_name)))
            }
            _ => return fake_backend(session_id),
        };
        backend = backend.with_executor(tools);
        if let Some(g) = &gate {
            backend = backend.with_permission_gate(g.clone());
        }
        backend = backend.with_interaction_store(store.clone());
        backend = backend.with_tool_operation_store(store.clone());
        Box::new(backend)
    })
}

fn fake_backend(session_id: &str) -> Box<dyn AgentBackend + Send + Sync> {
    Box::new(meepo_core::FakeBackend::new(
        session_id,
        vec![
            SessionEvent::TextComplete {
                id: "1".into(),
                turn_id: "t".into(),
                ts: 0,
                message_id: "m".into(),
                text: "meepo (fake backend)".into(),
                provider_options: None,
            },
            SessionEvent::Complete {
                id: "2".into(),
                turn_id: "t".into(),
                ts: 1,
                stop_reason: StopReason::EndTurn,
            },
        ],
    ))
}
