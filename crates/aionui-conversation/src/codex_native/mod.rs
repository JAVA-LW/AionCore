mod projection;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use aionui_ai_agent::capability::codex_app_server::{
    CodexAppServerError, CodexAppServerEvent, CodexPendingRequest, ICodexAppServerGateway,
};
use aionui_api_types::WebSocketMessage;
use aionui_common::{Confirmation, ConfirmationOption, ErrorChain, generate_short_id, now_ms};
use aionui_db::{
    CodexThreadBindingRow, CodexWatchedWorkspaceRow, ConversationRowUpdate, ICodexNativeRepository,
    IConversationRepository,
};
use aionui_realtime::EventBroadcaster;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use crate::ConversationError;
use crate::runtime_state::ConversationRuntimeStateService;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexNativeSendOutcome {
    pub msg_id: String,
    pub turn_id: String,
    pub steered: bool,
    pub pending_delivery: bool,
}

#[derive(Debug, Clone)]
struct PendingDelivery {
    msg_id: String,
    content: String,
}

pub struct CodexNativeRuntime {
    gateway: Arc<dyn ICodexAppServerGateway>,
    codex_repo: Arc<dyn ICodexNativeRepository>,
    conversation_repo: Arc<dyn IConversationRepository>,
    broadcaster: Arc<dyn EventBroadcaster>,
    runtime_state: Arc<ConversationRuntimeStateService>,
    start_lock: Mutex<()>,
    text_buffers: Mutex<HashMap<(String, String), String>>,
    thinking_buffers: Mutex<HashMap<(String, String), String>>,
    command_output_buffers: Mutex<HashMap<(String, String), String>>,
    pending_deliveries: Mutex<HashMap<String, Vec<PendingDelivery>>>,
}

impl CodexNativeRuntime {
    pub fn new(
        gateway: Arc<dyn ICodexAppServerGateway>,
        codex_repo: Arc<dyn ICodexNativeRepository>,
        conversation_repo: Arc<dyn IConversationRepository>,
        broadcaster: Arc<dyn EventBroadcaster>,
        runtime_state: Arc<ConversationRuntimeStateService>,
    ) -> Arc<Self> {
        Arc::new(Self {
            gateway,
            codex_repo,
            conversation_repo,
            broadcaster,
            runtime_state,
            start_lock: Mutex::new(()),
            text_buffers: Mutex::new(HashMap::new()),
            thinking_buffers: Mutex::new(HashMap::new()),
            command_output_buffers: Mutex::new(HashMap::new()),
            pending_deliveries: Mutex::new(HashMap::new()),
        })
    }

    pub fn start(self: &Arc<Self>) {
        let receiver = self.gateway.subscribe();
        let runtime = self.clone();
        tokio::spawn(async move {
            runtime.run_event_loop(receiver).await;
        });
    }

    pub async fn register_workspace(
        &self,
        user_id: &str,
        root_path: &str,
        recursive: bool,
    ) -> Result<CodexWatchedWorkspaceRow, ConversationError> {
        let root_path = canonical_path(root_path);
        let now = now_ms();
        let row = CodexWatchedWorkspaceRow {
            id: generate_short_id(),
            user_id: user_id.to_owned(),
            root_path,
            recursive,
            enabled: true,
            created_at: now,
            updated_at: now,
        };
        let saved = self.codex_repo.upsert_workspace(&row).await?;
        if let Err(error) = self.reconcile().await {
            warn!(error = %ErrorChain(&error), "Codex workspace registered but initial reconciliation failed");
        }
        Ok(saved)
    }

    pub async fn send(
        &self,
        conversation: &aionui_db::models::ConversationRow,
        msg_id: String,
        content: String,
    ) -> Result<CodexNativeSendOutcome, ConversationError> {
        let binding = self.ensure_conversation_binding(conversation).await?;
        if binding.live_state != "live" {
            self.gateway
                .resume_thread(&binding.thread_id)
                .await
                .map_err(codex_error)?;
            self.codex_repo
                .update_binding_live_state(&binding.conversation_id, "live", now_ms())
                .await?;
        }

        match self.gateway.send_input(&binding.thread_id, &msg_id, &content).await {
            Ok(receipt) => Ok(CodexNativeSendOutcome {
                msg_id,
                turn_id: receipt.turn_id,
                steered: receipt.steered,
                pending_delivery: false,
            }),
            Err(CodexAppServerError::ActiveTurnNotSteerable) => {
                let snapshot = self.gateway.snapshot().await;
                let turn_id = snapshot
                    .runtimes
                    .get(&binding.thread_id)
                    .and_then(|runtime| runtime.active_turn_id.clone())
                    .ok_or_else(|| ConversationError::Busy {
                        reason: "Codex is running a non-steerable internal turn".into(),
                    })?;
                self.pending_deliveries
                    .lock()
                    .await
                    .entry(conversation.id.clone())
                    .or_default()
                    .push(PendingDelivery {
                        msg_id: msg_id.clone(),
                        content,
                    });
                info!(
                    conversation_id = %conversation.id,
                    turn_id,
                    "Codex input retained until the non-steerable turn completes"
                );
                Ok(CodexNativeSendOutcome {
                    msg_id,
                    turn_id,
                    steered: false,
                    pending_delivery: true,
                })
            }
            Err(error) => Err(codex_error(error)),
        }
    }

    pub async fn interrupt(&self, conversation_id: &str, turn_id: &str) -> Result<(), ConversationError> {
        let binding = self
            .codex_repo
            .get_binding_for_conversation(conversation_id)
            .await?
            .ok_or_else(|| ConversationError::ActiveAgentNotFound {
                conversation_id: conversation_id.to_owned(),
            })?;
        self.gateway
            .interrupt(&binding.thread_id, turn_id)
            .await
            .map_err(codex_error)
    }

    pub async fn pending_confirmation_count(&self, conversation_id: &str) -> usize {
        self.list_confirmations(conversation_id)
            .await
            .map_or(0, |items| items.len())
    }

    pub async fn list_confirmations(&self, conversation_id: &str) -> Result<Vec<Confirmation>, ConversationError> {
        let Some(binding) = self.codex_repo.get_binding_for_conversation(conversation_id).await? else {
            return Ok(Vec::new());
        };
        let snapshot = self.gateway.snapshot().await;
        Ok(snapshot
            .pending_requests
            .values()
            .filter(|request| request.params.get("threadId").and_then(Value::as_str) == Some(&binding.thread_id))
            .filter_map(pending_request_to_confirmation)
            .collect())
    }

    pub async fn confirm(
        &self,
        conversation_id: &str,
        call_id: &str,
        data: &Value,
        always_allow: bool,
    ) -> Result<Option<String>, ConversationError> {
        let binding = self
            .codex_repo
            .get_binding_for_conversation(conversation_id)
            .await?
            .ok_or_else(|| ConversationError::ActiveAgentNotFound {
                conversation_id: conversation_id.to_owned(),
            })?;
        let snapshot = self.gateway.snapshot().await;
        let request = snapshot
            .pending_requests
            .values()
            .find(|request| {
                request_key(&request.request_id) == call_id
                    && request.params.get("threadId").and_then(Value::as_str) == Some(&binding.thread_id)
            })
            .cloned()
            .ok_or_else(|| ConversationError::BadRequest {
                reason: "Codex approval request is no longer pending".into(),
            })?;

        let selected = data
            .get("value")
            .and_then(Value::as_str)
            .or_else(|| data.as_str())
            .unwrap_or("cancel");
        let decision = match (selected, always_allow) {
            (_, true) | ("proceed_always", _) => "acceptForSession",
            ("proceed_once", _) | ("accept", _) => "accept",
            ("decline", _) => "decline",
            _ => "cancel",
        };
        let confirmation_id = pending_request_to_confirmation(&request).map(|confirmation| confirmation.id);
        self.gateway
            .respond_to_server_request(request.request_id, json!({"decision": decision}))
            .await
            .map_err(codex_error)?;
        Ok(confirmation_id)
    }

    async fn ensure_conversation_binding(
        &self,
        conversation: &aionui_db::models::ConversationRow,
    ) -> Result<CodexThreadBindingRow, ConversationError> {
        if let Some(binding) = self.codex_repo.get_binding_for_conversation(&conversation.id).await? {
            return Ok(binding);
        }

        let _guard = self.start_lock.lock().await;
        if let Some(binding) = self.codex_repo.get_binding_for_conversation(&conversation.id).await? {
            return Ok(binding);
        }
        let extra: Value = serde_json::from_str(&conversation.extra)
            .map_err(|error| ConversationError::internal(format!("invalid conversation extra: {error}")))?;
        let cwd = extra
            .get("workspace")
            .and_then(Value::as_str)
            .ok_or_else(|| ConversationError::BadRequest {
                reason: "GPT Codex conversation requires a workspace".into(),
            })?;
        let thread = self.gateway.start_thread(cwd).await.map_err(codex_error)?;
        let codex_home = self.codex_home().await?;
        let binding = binding_from_thread(&codex_home, &thread, &conversation.user_id, &conversation.id, "live")?;
        let saved = self.codex_repo.upsert_binding(&binding).await?;

        let mut next_extra = extra;
        next_extra["codex_thread_id"] = Value::String(saved.thread_id.clone());
        next_extra["codex_source"] = Value::String(saved.source.clone());
        next_extra["codex_live_state"] = Value::String("live".into());
        self.conversation_repo
            .update(
                &conversation.id,
                &ConversationRowUpdate {
                    extra: Some(next_extra.to_string()),
                    updated_at: Some(now_ms()),
                    ..Default::default()
                },
            )
            .await?;
        Ok(saved)
    }

    async fn codex_home(&self) -> Result<String, ConversationError> {
        self.gateway
            .snapshot()
            .await
            .codex_home
            .map(|path| path.to_string_lossy().into_owned())
            .ok_or_else(|| ConversationError::Busy {
                reason: "Codex app-server is not connected".into(),
            })
    }

    async fn run_event_loop(self: Arc<Self>, mut receiver: tokio::sync::broadcast::Receiver<CodexAppServerEvent>) {
        if self.gateway.snapshot().await.connected
            && let Err(error) = self.reconcile().await
        {
            error!(error = %ErrorChain(&error), "Initial Codex thread reconciliation failed");
        }
        loop {
            match receiver.recv().await {
                Ok(CodexAppServerEvent::Connected { .. }) => {
                    if let Err(error) = self.reconcile().await {
                        error!(error = %ErrorChain(&error), "Codex thread reconciliation failed");
                    }
                }
                Ok(CodexAppServerEvent::Disconnected { reason }) => {
                    warn!(reason, "Codex native projection paused");
                    if let Err(error) = self.mark_bindings_stored_only().await {
                        warn!(error = %ErrorChain(&error), "Failed to mark Codex bindings stored-only");
                    }
                }
                Ok(CodexAppServerEvent::Notification { method, params }) => {
                    if let Err(error) = self.handle_notification(&method, &params).await {
                        warn!(method, error = %ErrorChain(&error), "Codex notification projection failed");
                    }
                }
                Ok(CodexAppServerEvent::ServerRequest(request)) => {
                    if let Err(error) = self.handle_server_request(&request).await {
                        warn!(method = request.method, error = %ErrorChain(&error), "Codex server request projection failed");
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(
                        skipped,
                        "Codex event projection lagged; reconciling from source of truth"
                    );
                    if let Err(error) = self.reconcile().await {
                        warn!(error = %ErrorChain(&error), "Codex lag recovery failed");
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    }

    async fn reconcile(&self) -> Result<(), ConversationError> {
        let codex_home = self.codex_home().await?;
        let loaded: std::collections::HashSet<String> = self
            .gateway
            .list_loaded_threads()
            .await
            .map_err(codex_error)?
            .into_iter()
            .collect();
        let mut threads = self.gateway.list_threads(false).await.map_err(codex_error)?;
        threads.extend(self.gateway.list_threads(true).await.map_err(codex_error)?);
        for thread in threads {
            let bindings = self.ensure_bindings_for_thread(&codex_home, &thread).await?;
            if bindings.is_empty() {
                continue;
            }
            let Some(thread_id) = thread.get("id").and_then(Value::as_str) else {
                continue;
            };
            let full_thread = self
                .gateway
                .read_thread(thread_id, true)
                .await
                .unwrap_or_else(|_| thread.clone());
            for binding in &bindings {
                self.project_thread_history(binding, &full_thread).await?;
            }
            if loaded.contains(thread_id) {
                let resumed = self.gateway.resume_thread(thread_id).await.map_err(codex_error)?;
                for binding in &bindings {
                    self.codex_repo
                        .update_binding_live_state(&binding.conversation_id, "live", now_ms())
                        .await?;
                    self.project_thread_history(binding, &resumed).await?;
                }
            }
        }
        debug!("Codex thread catalog reconciled");
        Ok(())
    }

    async fn ensure_bindings_for_thread(
        &self,
        codex_home: &str,
        thread: &Value,
    ) -> Result<Vec<CodexThreadBindingRow>, ConversationError> {
        let thread_id = thread
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| ConversationError::internal("Codex thread omitted id"))?;
        let cwd = thread
            .get("cwd")
            .and_then(Value::as_str)
            .ok_or_else(|| ConversationError::internal("Codex thread omitted cwd"))?;
        let mut bindings = self.codex_repo.list_bindings_for_thread(codex_home, thread_id).await?;
        let watches = self.codex_repo.list_enabled_workspaces().await?;
        for watch in watches {
            if bindings.iter().any(|binding| binding.user_id == watch.user_id)
                || !workspace_matches(&watch.root_path, cwd, watch.recursive)
            {
                continue;
            }
            let binding = self.create_discovered_conversation(codex_home, thread, &watch).await?;
            bindings.push(binding);
        }
        Ok(bindings)
    }

    async fn create_discovered_conversation(
        &self,
        codex_home: &str,
        thread: &Value,
        watch: &CodexWatchedWorkspaceRow,
    ) -> Result<CodexThreadBindingRow, ConversationError> {
        let id = generate_short_id();
        let now = now_ms();
        let cwd = thread.get("cwd").and_then(Value::as_str).unwrap_or(&watch.root_path);
        let source = source_name(thread.get("source"));
        let status = thread.pointer("/status/type").and_then(Value::as_str);
        let live_state = if status == Some("notLoaded") {
            "stored_only"
        } else {
            "live"
        };
        let name = thread
            .get("name")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .or_else(|| thread.get("preview").and_then(Value::as_str))
            .map(short_title)
            .unwrap_or_else(|| "GPT Codex".into());
        let thread_id = thread.get("id").and_then(Value::as_str).unwrap_or_default();
        let extra = json!({
            "workspace": cwd,
            "custom_workspace": true,
            "backend": "codex-native",
            "agent_id": "c0d3a55e",
            "agent_source": "builtin",
            "codex_thread_id": thread_id,
            "codex_source": source,
            "codex_live_state": live_state,
            "codex_watched_root": watch.root_path,
        });
        let row = aionui_db::models::ConversationRow {
            id: id.clone(),
            user_id: watch.user_id.clone(),
            name,
            r#type: "codex-app-server".into(),
            extra: extra.to_string(),
            model: None,
            status: Some(
                if status == Some("active") {
                    "running"
                } else {
                    "finished"
                }
                .into(),
            ),
            source: Some("aionui".into()),
            channel_chat_id: None,
            pinned: false,
            pinned_at: None,
            created_at: thread
                .get("createdAt")
                .and_then(Value::as_i64)
                .map_or(now, |value| value * 1000),
            updated_at: thread
                .get("updatedAt")
                .and_then(Value::as_i64)
                .map_or(now, |value| value * 1000),
        };
        self.conversation_repo.create(&row).await?;
        let binding = binding_from_thread(codex_home, thread, &watch.user_id, &id, live_state)?;
        let saved = self.codex_repo.upsert_binding(&binding).await?;
        self.broadcaster.broadcast(WebSocketMessage::new(
            "conversation.listChanged",
            json!({"conversation_id": id, "action": "created", "source": "aionui"}),
        ));
        info!(
            conversation_id = %saved.conversation_id,
            thread_id = %saved.thread_id,
            cwd = %saved.cwd,
            source = %saved.source,
            "Codex thread projected as AionUI conversation"
        );
        Ok(saved)
    }

    async fn mark_bindings_stored_only(&self) -> Result<(), ConversationError> {
        for binding in self.codex_repo.list_all_bindings().await? {
            self.codex_repo
                .update_binding_live_state(&binding.conversation_id, "stored_only", now_ms())
                .await?;
            if let Some(turn_id) = self.runtime_state.active_turn_id_for(&binding.conversation_id) {
                self.runtime_state
                    .clear_external_turn(&binding.conversation_id, &turn_id);
            }
            if let Some(conversation) = self.conversation_repo.get(&binding.conversation_id).await? {
                let mut extra = serde_json::from_str::<Value>(&conversation.extra).unwrap_or_else(|_| json!({}));
                extra["codex_live_state"] = Value::String("stored_only".into());
                self.conversation_repo
                    .update(
                        &binding.conversation_id,
                        &ConversationRowUpdate {
                            extra: Some(extra.to_string()),
                            status: Some("finished".into()),
                            updated_at: Some(now_ms()),
                            ..Default::default()
                        },
                    )
                    .await?;
            }
        }
        Ok(())
    }

    async fn deliver_pending(&self, conversation_id: &str, thread_id: &str) {
        let pending = self
            .pending_deliveries
            .lock()
            .await
            .remove(conversation_id)
            .unwrap_or_default();
        for delivery in pending {
            if let Err(error) = self
                .gateway
                .send_input(thread_id, &delivery.msg_id, &delivery.content)
                .await
            {
                warn!(conversation_id, error = %error, "Deferred Codex input delivery failed");
            }
        }
    }
}

fn pending_request_to_confirmation(request: &CodexPendingRequest) -> Option<Confirmation> {
    if !matches!(
        request.method.as_str(),
        "item/commandExecution/requestApproval" | "item/fileChange/requestApproval"
    ) {
        return None;
    }
    let call_id = request_key(&request.request_id);
    let is_command = request.method == "item/commandExecution/requestApproval";
    let description = request
        .params
        .get(if is_command { "command" } else { "reason" })
        .and_then(Value::as_str)
        .or_else(|| request.params.get("reason").and_then(Value::as_str))
        .unwrap_or(if is_command {
            "Codex requests permission to run a command"
        } else {
            "Codex requests permission to change files"
        })
        .to_owned();
    Some(Confirmation {
        id: call_id.clone(),
        call_id,
        title: None,
        action: Some(if is_command { "exec" } else { "edit" }.into()),
        description,
        command_type: Some(if is_command { "execute" } else { "edit" }.into()),
        options: vec![
            ConfirmationOption {
                label: "messages.confirmation.yesAllowOnce".into(),
                value: json!("proceed_once"),
                params: None,
            },
            ConfirmationOption {
                label: "messages.confirmation.yesAllowAlways".into(),
                value: json!("proceed_always"),
                params: None,
            },
            ConfirmationOption {
                label: "messages.confirmation.no".into(),
                value: json!("cancel"),
                params: None,
            },
        ],
    })
}

fn binding_from_thread(
    codex_home: &str,
    thread: &Value,
    user_id: &str,
    conversation_id: &str,
    live_state: &str,
) -> Result<CodexThreadBindingRow, ConversationError> {
    let now = now_ms();
    Ok(CodexThreadBindingRow {
        codex_home: codex_home.to_owned(),
        thread_id: thread
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| ConversationError::internal("Codex thread omitted id"))?
            .to_owned(),
        user_id: user_id.to_owned(),
        conversation_id: conversation_id.to_owned(),
        cwd: thread
            .get("cwd")
            .and_then(Value::as_str)
            .ok_or_else(|| ConversationError::internal("Codex thread omitted cwd"))?
            .to_owned(),
        source: source_name(thread.get("source")),
        live_state: live_state.to_owned(),
        created_at: thread
            .get("createdAt")
            .and_then(Value::as_i64)
            .map_or(now, |value| value * 1000),
        updated_at: now,
    })
}

fn source_name(source: Option<&Value>) -> String {
    source
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            source
                .and_then(|value| value.get("type"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "unknown".into())
}

fn short_title(value: &str) -> String {
    let title: String = value.trim().chars().take(80).collect();
    if title.is_empty() { "GPT Codex".into() } else { title }
}

fn canonical_path(value: &str) -> String {
    std::fs::canonicalize(value)
        .unwrap_or_else(|_| PathBuf::from(value))
        .to_string_lossy()
        .into_owned()
}

pub(crate) fn workspace_matches(root: &str, cwd: &str, recursive: bool) -> bool {
    let root = PathBuf::from(canonical_path(root));
    let cwd = PathBuf::from(canonical_path(cwd));
    cwd == root || (recursive && cwd.starts_with(&root))
}

fn request_key(id: &Value) -> String {
    serde_json::to_string(id).unwrap_or_else(|_| "null".into())
}

fn codex_error(error: CodexAppServerError) -> ConversationError {
    match error {
        CodexAppServerError::ActiveTurnNotSteerable => ConversationError::Busy {
            reason: "Codex is running a non-steerable internal turn".into(),
        },
        CodexAppServerError::Unavailable(reason) => ConversationError::Busy { reason },
        CodexAppServerError::Timeout => ConversationError::Busy {
            reason: "Codex app-server request timed out".into(),
        },
        other => ConversationError::internal(other.to_string()),
    }
}

#[cfg(test)]
#[path = "codex_native_tests.rs"]
mod tests;
