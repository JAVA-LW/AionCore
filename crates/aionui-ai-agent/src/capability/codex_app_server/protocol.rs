use std::collections::HashMap;
use std::path::PathBuf;

use async_trait::async_trait;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexThreadRuntime {
    pub active_turn_id: Option<String>,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct CodexPendingRequest {
    pub request_id: Value,
    pub method: String,
    pub params: Value,
}

#[derive(Debug, Clone)]
pub enum CodexAppServerEvent {
    Connected { codex_home: PathBuf, user_agent: String },
    Disconnected { reason: String },
    Notification { method: String, params: Value },
    ServerRequest(CodexPendingRequest),
}

#[derive(Debug, Clone, Default)]
pub struct CodexAppServerSnapshot {
    pub connected: bool,
    pub codex_home: Option<PathBuf>,
    pub user_agent: Option<String>,
    pub threads: HashMap<String, Value>,
    pub runtimes: HashMap<String, CodexThreadRuntime>,
    pub pending_requests: HashMap<String, CodexPendingRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexSendReceipt {
    pub turn_id: String,
    pub steered: bool,
}

#[derive(Debug, thiserror::Error, Clone, PartialEq)]
pub enum CodexAppServerError {
    #[error("Codex app-server is unavailable: {0}")]
    Unavailable(String),
    #[error("Codex app-server request timed out")]
    Timeout,
    #[error("Codex app-server RPC {code}: {message}")]
    Rpc {
        code: i64,
        message: String,
        data: Option<Value>,
    },
    #[error("Invalid Codex app-server response: {0}")]
    InvalidResponse(String),
    #[error("Codex turn cannot currently accept direct input")]
    ActiveTurnNotSteerable,
}

impl CodexAppServerError {
    pub fn is_turn_race(&self) -> bool {
        matches!(self, Self::Rpc { message, .. } if message.contains("no active turn") || message.contains("expected active turn id"))
    }

    pub fn is_active_turn_not_steerable(&self) -> bool {
        match self {
            Self::ActiveTurnNotSteerable => true,
            Self::Rpc { message, data, .. } => {
                message.contains("cannot steer a review turn")
                    || message.contains("cannot steer a compact turn")
                    || data
                        .as_ref()
                        .and_then(|value| value.get("codexErrorInfo"))
                        .and_then(|value| value.get("type"))
                        .and_then(Value::as_str)
                        == Some("activeTurnNotSteerable")
            }
            _ => false,
        }
    }
}

#[async_trait]
pub trait ICodexAppServerGateway: Send + Sync {
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<CodexAppServerEvent>;
    async fn snapshot(&self) -> CodexAppServerSnapshot;
    async fn list_threads(&self, archived: bool) -> Result<Vec<Value>, CodexAppServerError>;
    async fn list_loaded_threads(&self) -> Result<Vec<String>, CodexAppServerError>;
    async fn read_thread(&self, thread_id: &str, include_turns: bool) -> Result<Value, CodexAppServerError>;
    async fn resume_thread(&self, thread_id: &str) -> Result<Value, CodexAppServerError>;
    async fn start_thread(&self, cwd: &str) -> Result<Value, CodexAppServerError>;
    async fn send_input(
        &self,
        thread_id: &str,
        client_message_id: &str,
        text: &str,
    ) -> Result<CodexSendReceipt, CodexAppServerError>;
    async fn interrupt(&self, thread_id: &str, turn_id: &str) -> Result<(), CodexAppServerError>;
    async fn respond_to_server_request(&self, request_id: Value, result: Value) -> Result<(), CodexAppServerError>;
}
