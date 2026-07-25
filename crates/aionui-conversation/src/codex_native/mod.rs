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
use tracing::{info, warn};

use crate::ConversationError;
use crate::runtime_state::ConversationRuntimeStateService;

const RECENT_FINISHED_THREADS_PER_WORKSPACE: usize = 5;
const THREAD_CATALOG_PAGE_SIZE: u32 = 50;
const THREAD_CATALOG_MAX_PAGES: usize = 20;

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
    discovery_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    history_sync_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
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
            discovery_locks: Mutex::new(HashMap::new()),
            history_sync_locks: Mutex::new(HashMap::new()),
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

    pub async fn list_models(&self) -> Result<Vec<Value>, ConversationError> {
        self.gateway.list_models().await.map_err(codex_error)
    }

    pub async fn binding_for_conversation(
        &self,
        conversation_id: &str,
    ) -> Result<Option<CodexThreadBindingRow>, ConversationError> {
        Ok(self.codex_repo.get_binding_for_conversation(conversation_id).await?)
    }

    pub async fn register_workspace(
        self: &Arc<Self>,
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
        self.schedule_workspace_discovery(saved.clone());
        Ok(saved)
    }

    pub async fn list_workspaces_for_user(
        &self,
        user_id: &str,
    ) -> Result<Vec<CodexWatchedWorkspaceRow>, ConversationError> {
        Ok(self
            .codex_repo
            .list_enabled_workspaces()
            .await?
            .into_iter()
            .filter(|workspace| workspace.user_id == user_id)
            .collect())
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
        let model = extra.get("codex_model").and_then(Value::as_str);
        let reasoning_effort = extra.get("codex_reasoning_effort").and_then(Value::as_str);
        let thread = self
            .gateway
            .start_thread(cwd, model, reasoning_effort)
            .await
            .map_err(codex_error)?;
        let codex_home = self.codex_home().await?;
        let binding = binding_from_thread(&codex_home, &thread, &conversation.user_id, &conversation.id, "live")?;
        let saved = self.codex_repo.upsert_binding(&binding).await?;

        let mut next_extra = extra;
        next_extra["codex_thread_id"] = Value::String(saved.thread_id.clone());
        next_extra["codex_source"] = Value::String(saved.source.clone());
        next_extra["codex_live_state"] = Value::String("live".into());
        next_extra["codex_thread_role"] = Value::String("root".into());
        next_extra["codex_subagent_total_count"] = Value::Number(0.into());
        next_extra["codex_subagent_running_count"] = Value::Number(0.into());
        next_extra["codex_subagent_completed_count"] = Value::Number(0.into());
        if let Some(can_accept_direct_input) = thread.get("canAcceptDirectInput").and_then(Value::as_bool) {
            next_extra["codex_can_accept_direct_input"] = Value::Bool(can_accept_direct_input);
        }
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
        if self.gateway.snapshot().await.connected {
            self.schedule_all_workspace_discoveries();
        }
        loop {
            match receiver.recv().await {
                Ok(CodexAppServerEvent::Connected { .. }) => {
                    self.schedule_all_workspace_discoveries();
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
                    warn!(skipped, "Codex event projection lagged; refreshing lightweight catalog");
                    self.schedule_all_workspace_discoveries();
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    }

    fn schedule_all_workspace_discoveries(self: &Arc<Self>) {
        let runtime = Arc::clone(self);
        tokio::spawn(async move {
            match runtime.codex_repo.list_enabled_workspaces().await {
                Ok(workspaces) => {
                    for workspace in workspaces {
                        runtime.schedule_workspace_discovery(workspace);
                    }
                }
                Err(error) => warn!(error = %ErrorChain(&error), "Failed to list Codex watched workspaces"),
            }
        });
    }

    fn schedule_workspace_discovery(self: &Arc<Self>, workspace: CodexWatchedWorkspaceRow) {
        let runtime = Arc::clone(self);
        tokio::spawn(async move {
            let root_path = workspace.root_path.clone();
            let user_id = workspace.user_id.clone();
            if let Err(error) = runtime.discover_workspace(&workspace).await {
                warn!(
                    user_id,
                    root_path,
                    error = %ErrorChain(&error),
                    "Codex workspace discovery failed"
                );
            }
        });
    }

    async fn discover_workspace(&self, workspace: &CodexWatchedWorkspaceRow) -> Result<(), ConversationError> {
        let lock_key = format!("{}:{}", workspace.user_id, workspace.root_path);
        let discovery_lock = {
            let mut locks = self.discovery_locks.lock().await;
            locks
                .entry(lock_key)
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let _guard = discovery_lock.lock().await;
        let codex_home = self.codex_home().await?;
        info!(
            user_id = workspace.user_id,
            root_path = workspace.root_path,
            "Codex workspace discovery started"
        );

        let mut seen = std::collections::HashSet::new();
        let mut active_thread_ids = Vec::new();
        let mut root_thread_ids = std::collections::HashSet::new();
        match self.gateway.list_loaded_threads().await {
            Ok(thread_ids) => {
                for thread_id in thread_ids {
                    let Ok(thread) = self.gateway.read_thread(&thread_id, false).await else {
                        continue;
                    };
                    if !thread_matches_workspace(&thread, workspace) {
                        continue;
                    }
                    if let Some(parent_thread_id) = thread_parent_id(&thread) {
                        root_thread_ids.insert(parent_thread_id.to_owned());
                    } else {
                        root_thread_ids.insert(thread_id.clone());
                    }
                    seen.insert(thread_id.clone());
                    if let Some(binding) = self
                        .project_thread_for_workspace(&codex_home, &thread, workspace)
                        .await?
                    {
                        self.sync_binding_thread_state(&binding, &thread).await?;
                    }
                    if thread_is_active(&thread) {
                        active_thread_ids.push(thread_id);
                    }
                }
            }
            Err(error) => warn!(
                root_path = workspace.root_path,
                error = %error,
                "Codex loaded-thread snapshot failed; continuing with recent catalog"
            ),
        }

        // A running child can be loaded before its parent is present in the
        // app-server snapshot. Materialize those parents before the recent
        // catalog so child projections can be linked immediately.
        for root_thread_id in root_thread_ids.clone() {
            if seen.contains(&root_thread_id) {
                continue;
            }
            let Ok(thread) = self.gateway.read_thread(&root_thread_id, false).await else {
                continue;
            };
            if !thread_matches_workspace(&thread, workspace) {
                continue;
            }
            seen.insert(root_thread_id.clone());
            if let Some(binding) = self
                .project_thread_for_workspace(&codex_home, &thread, workspace)
                .await?
            {
                self.sync_binding_thread_state(&binding, &thread).await?;
            }
        }

        let mut recent_finished = 0usize;
        let mut scanned_pages = 0usize;
        'catalog: for archived in [false, true] {
            let mut cursor: Option<String> = None;
            for _ in 0..THREAD_CATALOG_MAX_PAGES {
                let page = match self
                    .gateway
                    .list_thread_page(cursor.as_deref(), THREAD_CATALOG_PAGE_SIZE, archived)
                    .await
                {
                    Ok(page) => page,
                    Err(error) => {
                        warn!(
                            root_path = workspace.root_path,
                            archived,
                            error = %error,
                            "Codex recent-thread catalog page failed"
                        );
                        break 'catalog;
                    }
                };
                scanned_pages += 1;
                for thread in page.threads {
                    let Some(thread_id) = thread.get("id").and_then(Value::as_str) else {
                        continue;
                    };
                    if !thread_matches_workspace(&thread, workspace) {
                        continue;
                    }
                    // Finished sub-agents do not consume the workspace's five
                    // root-task slots. They are loaded below as descendants of
                    // the roots that are actually visible in the sidebar.
                    if thread_is_subagent(&thread) {
                        continue;
                    }
                    if !seen.insert(thread_id.to_owned()) {
                        continue;
                    }
                    root_thread_ids.insert(thread_id.to_owned());
                    if let Some(binding) = self
                        .project_thread_for_workspace(&codex_home, &thread, workspace)
                        .await?
                    {
                        self.sync_binding_thread_state(&binding, &thread).await?;
                    }
                    if thread_is_active(&thread) {
                        active_thread_ids.push(thread_id.to_owned());
                    } else {
                        recent_finished += 1;
                        if recent_finished >= RECENT_FINISHED_THREADS_PER_WORKSPACE {
                            break 'catalog;
                        }
                    }
                }
                cursor = page.next_cursor;
                if cursor.is_none() {
                    break;
                }
            }
        }

        // Fetch only lightweight descendant summaries for the visible roots.
        // Message histories remain cursor-paged and are not touched here.
        for root_thread_id in root_thread_ids.clone() {
            for archived in [false, true] {
                let mut cursor: Option<String> = None;
                for _ in 0..THREAD_CATALOG_MAX_PAGES {
                    let page = match self
                        .gateway
                        .list_thread_descendants(&root_thread_id, cursor.as_deref(), THREAD_CATALOG_PAGE_SIZE, archived)
                        .await
                    {
                        Ok(page) => page,
                        Err(error) => {
                            warn!(
                                root_thread_id,
                                archived,
                                error = %error,
                                "Codex descendant catalog page failed"
                            );
                            break;
                        }
                    };
                    for thread in page.threads {
                        let Some(thread_id) = thread.get("id").and_then(Value::as_str) else {
                            continue;
                        };
                        if !seen.insert(thread_id.to_owned()) || !thread_matches_workspace(&thread, workspace) {
                            continue;
                        }
                        if let Some(binding) = self
                            .project_thread_for_workspace(&codex_home, &thread, workspace)
                            .await?
                        {
                            self.sync_binding_thread_state(&binding, &thread).await?;
                        }
                    }
                    cursor = page.next_cursor;
                    if cursor.is_none() {
                        break;
                    }
                }
            }
        }

        active_thread_ids.sort_unstable();
        active_thread_ids.dedup();
        for thread_id in &active_thread_ids {
            match self.gateway.resume_thread(thread_id).await {
                Ok(thread) => {
                    if let Some(binding) = self
                        .project_thread_for_workspace(&codex_home, &thread, workspace)
                        .await?
                    {
                        self.sync_binding_thread_state(&binding, &thread).await?;
                    }
                }
                Err(error) => warn!(
                    thread_id,
                    root_path = workspace.root_path,
                    error = %error,
                    "Codex active thread resume failed"
                ),
            }
        }

        info!(
            user_id = workspace.user_id,
            root_path = workspace.root_path,
            active_threads = active_thread_ids.len(),
            recent_finished,
            scanned_pages,
            "Codex workspace discovery completed"
        );
        Ok(())
    }

    async fn project_thread_for_workspace(
        &self,
        codex_home: &str,
        thread: &Value,
        workspace: &CodexWatchedWorkspaceRow,
    ) -> Result<Option<CodexThreadBindingRow>, ConversationError> {
        let _guard = self.start_lock.lock().await;
        self.ensure_binding_for_workspace(codex_home, thread, workspace).await
    }

    async fn sync_binding_thread_state(
        &self,
        binding: &CodexThreadBindingRow,
        thread: &Value,
    ) -> Result<(), ConversationError> {
        let status = thread.pointer("/status/type").and_then(Value::as_str);
        let active_turn_id = thread
            .get("turns")
            .and_then(Value::as_array)
            .and_then(|turns| {
                turns
                    .iter()
                    .find(|turn| turn.get("status").and_then(Value::as_str) == Some("inProgress"))
            })
            .and_then(|turn| turn.get("id"))
            .and_then(Value::as_str);
        if let Some(turn_id) = active_turn_id {
            self.runtime_state.set_external_turn(&binding.conversation_id, turn_id);
        } else if status != Some("active")
            && let Some(turn_id) = self.runtime_state.active_turn_id_for(&binding.conversation_id)
        {
            self.runtime_state
                .clear_external_turn(&binding.conversation_id, &turn_id);
        }

        let live_state = if status == Some("notLoaded") {
            "stored_only"
        } else {
            "live"
        };
        self.codex_repo
            .update_binding_live_state(&binding.conversation_id, live_state, now_ms())
            .await?;
        if let Some(conversation) = self.conversation_repo.get(&binding.conversation_id).await? {
            let mut extra = serde_json::from_str::<Value>(&conversation.extra).unwrap_or_else(|_| json!({}));
            let parent_conversation_id = extra
                .get("codex_parent_conversation_id")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let live_state_changed = extra.get("codex_live_state").and_then(Value::as_str) != Some(live_state);
            extra["codex_live_state"] = Value::String(live_state.into());
            let source_updated_at = if status == Some("active") {
                now_ms()
            } else {
                thread
                    .get("updatedAt")
                    .and_then(Value::as_i64)
                    .map_or(conversation.updated_at, |value| value * 1000)
            };
            let conversation_status = match status {
                Some("active") => Some("running".to_owned()),
                Some("idle" | "notLoaded") => Some("finished".to_owned()),
                _ => None,
            };
            let status_changed = conversation_status
                .as_deref()
                .is_some_and(|next| conversation.status.as_deref() != Some(next));
            self.conversation_repo
                .update(
                    &binding.conversation_id,
                    &ConversationRowUpdate {
                        extra: Some(extra.to_string()),
                        status: conversation_status,
                        updated_at: Some(source_updated_at),
                        ..Default::default()
                    },
                )
                .await?;
            if live_state_changed || status_changed {
                self.broadcast_conversation_updated(&binding.conversation_id);
            }
            if let Some(parent_conversation_id) = parent_conversation_id {
                self.recompute_subagent_summary(&parent_conversation_id).await?;
            }
        }
        Ok(())
    }

    fn broadcast_conversation_updated(&self, conversation_id: &str) {
        self.broadcaster.broadcast(WebSocketMessage::new(
            "conversation.listChanged",
            json!({
                "conversation_id": conversation_id,
                "action": "updated",
                "source": "aionui",
            }),
        ));
    }

    async fn ensure_bindings_for_thread(
        &self,
        codex_home: &str,
        thread: &Value,
    ) -> Result<Vec<CodexThreadBindingRow>, ConversationError> {
        let mut bindings = Vec::new();
        let watches = self.codex_repo.list_enabled_workspaces().await?;
        for watch in watches {
            if let Some(binding) = self.ensure_binding_for_workspace(codex_home, thread, &watch).await? {
                bindings.push(binding);
            }
        }
        Ok(bindings)
    }

    async fn ensure_binding_for_workspace(
        &self,
        codex_home: &str,
        thread: &Value,
        workspace: &CodexWatchedWorkspaceRow,
    ) -> Result<Option<CodexThreadBindingRow>, ConversationError> {
        let thread_id = thread
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| ConversationError::internal("Codex thread omitted id"))?;
        let cwd = thread
            .get("cwd")
            .and_then(Value::as_str)
            .ok_or_else(|| ConversationError::internal("Codex thread omitted cwd"))?;
        let bindings = self.codex_repo.list_bindings_for_thread(codex_home, thread_id).await?;
        if let Some(binding) = bindings
            .into_iter()
            .find(|binding| binding.user_id == workspace.user_id)
        {
            self.sync_conversation_thread_metadata(&binding, thread).await?;
            return Ok(Some(binding));
        }
        if !workspace_matches(&workspace.root_path, cwd, workspace.recursive) {
            return Ok(None);
        }
        Ok(Some(
            self.create_discovered_conversation(codex_home, thread, workspace)
                .await?,
        ))
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
        let metadata = thread_metadata(thread);
        let parent_conversation_id = if let Some(parent_thread_id) = metadata.parent_thread_id.as_deref() {
            self.codex_repo
                .list_bindings_for_thread(codex_home, parent_thread_id)
                .await?
                .into_iter()
                .find(|binding| binding.user_id == watch.user_id)
                .map(|binding| binding.conversation_id)
        } else {
            None
        };
        let status = thread.pointer("/status/type").and_then(Value::as_str);
        let live_state = if status == Some("notLoaded") {
            "stored_only"
        } else {
            "live"
        };
        let name = thread_display_title(thread, &metadata);
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
            "codex_thread_role": metadata.role,
            "codex_parent_thread_id": metadata.parent_thread_id,
            "codex_parent_conversation_id": parent_conversation_id,
            "codex_subagent_kind": metadata.kind,
            "codex_agent_path": metadata.agent_path,
            "codex_agent_nickname": metadata.agent_nickname,
            "codex_agent_role": metadata.agent_role,
            "codex_agent_depth": metadata.depth,
            "codex_can_accept_direct_input": metadata.can_accept_direct_input,
            "codex_auto_title": true,
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
        if metadata.role == "root" {
            self.relink_subagents(&saved).await?;
        } else if let Some(parent_conversation_id) = parent_conversation_id.as_deref() {
            self.recompute_subagent_summary(parent_conversation_id).await?;
        }
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

    async fn sync_conversation_thread_metadata(
        &self,
        binding: &CodexThreadBindingRow,
        thread: &Value,
    ) -> Result<(), ConversationError> {
        let Some(conversation) = self.conversation_repo.get(&binding.conversation_id).await? else {
            return Ok(());
        };
        let metadata = thread_metadata(thread);
        let parent_conversation_id = if let Some(parent_thread_id) = metadata.parent_thread_id.as_deref() {
            self.codex_repo
                .list_bindings_for_thread(&binding.codex_home, parent_thread_id)
                .await?
                .into_iter()
                .find(|candidate| candidate.user_id == binding.user_id)
                .map(|candidate| candidate.conversation_id)
        } else {
            None
        };
        let source = source_name(thread.get("source"));
        if source != binding.source {
            let mut refreshed_binding = binding.clone();
            refreshed_binding.source = source.clone();
            refreshed_binding.updated_at = now_ms();
            self.codex_repo.upsert_binding(&refreshed_binding).await?;
        }
        let mut extra = serde_json::from_str::<Value>(&conversation.extra).unwrap_or_else(|_| json!({}));
        let previous = extra.clone();
        extra["codex_source"] = Value::String(source);
        extra["codex_thread_role"] = Value::String(metadata.role.clone());
        set_optional_string(
            &mut extra,
            "codex_parent_thread_id",
            metadata.parent_thread_id.as_deref(),
        );
        set_optional_string(
            &mut extra,
            "codex_parent_conversation_id",
            parent_conversation_id.as_deref(),
        );
        set_optional_string(&mut extra, "codex_subagent_kind", metadata.kind.as_deref());
        set_optional_string(&mut extra, "codex_agent_path", metadata.agent_path.as_deref());
        set_optional_string(&mut extra, "codex_agent_nickname", metadata.agent_nickname.as_deref());
        set_optional_string(&mut extra, "codex_agent_role", metadata.agent_role.as_deref());
        if let Some(depth) = metadata.depth {
            extra["codex_agent_depth"] = Value::Number(depth.into());
        }
        if let Some(can_accept_direct_input) = metadata.can_accept_direct_input {
            extra["codex_can_accept_direct_input"] = Value::Bool(can_accept_direct_input);
        }
        let auto_title = extra
            .get("codex_auto_title")
            .and_then(Value::as_bool)
            .unwrap_or_else(|| conversation.name == "GPT Codex" || conversation.name == "GPT Codex 子智能体");
        let next_name = auto_title.then(|| thread_display_title(thread, &metadata));
        if auto_title {
            extra["codex_auto_title"] = Value::Bool(true);
        }
        let name_changed = next_name.as_deref().is_some_and(|name| name != conversation.name);
        if extra != previous || name_changed {
            self.conversation_repo
                .update(
                    &binding.conversation_id,
                    &ConversationRowUpdate {
                        name: next_name.filter(|name| name != &conversation.name),
                        extra: Some(extra.to_string()),
                        ..Default::default()
                    },
                )
                .await?;
            self.broadcast_conversation_updated(&binding.conversation_id);
        }
        if metadata.role == "root" {
            self.relink_subagents(binding).await?;
        } else if let Some(parent_conversation_id) = parent_conversation_id.as_deref() {
            self.recompute_subagent_summary(parent_conversation_id).await?;
        }
        Ok(())
    }

    async fn relink_subagents(&self, parent: &CodexThreadBindingRow) -> Result<(), ConversationError> {
        for child in self.codex_repo.list_all_bindings().await? {
            if child.user_id != parent.user_id || child.codex_home != parent.codex_home {
                continue;
            }
            let Some(conversation) = self.conversation_repo.get(&child.conversation_id).await? else {
                continue;
            };
            let mut extra = serde_json::from_str::<Value>(&conversation.extra).unwrap_or_else(|_| json!({}));
            if extra.get("codex_parent_thread_id").and_then(Value::as_str) != Some(parent.thread_id.as_str())
                || extra.get("codex_parent_conversation_id").and_then(Value::as_str)
                    == Some(parent.conversation_id.as_str())
            {
                continue;
            }
            extra["codex_parent_conversation_id"] = Value::String(parent.conversation_id.clone());
            self.conversation_repo
                .update(
                    &child.conversation_id,
                    &ConversationRowUpdate {
                        extra: Some(extra.to_string()),
                        ..Default::default()
                    },
                )
                .await?;
            self.broadcast_conversation_updated(&child.conversation_id);
        }
        self.recompute_subagent_summary(&parent.conversation_id).await
    }

    async fn recompute_subagent_summary(&self, parent_conversation_id: &str) -> Result<(), ConversationError> {
        let Some(parent) = self.conversation_repo.get(parent_conversation_id).await? else {
            return Ok(());
        };
        let mut total = 0_i64;
        let mut running = 0_i64;
        let mut latest_child_at = parent.updated_at;
        for child in self
            .conversation_repo
            .list_codex_subagents(&parent.user_id, parent_conversation_id)
            .await?
        {
            total += 1;
            if child.status.as_deref() == Some("running") {
                running += 1;
            }
            latest_child_at = latest_child_at.max(child.updated_at);
        }
        let mut extra = serde_json::from_str::<Value>(&parent.extra).unwrap_or_else(|_| json!({}));
        let changed = extra.get("codex_subagent_total_count").and_then(Value::as_i64) != Some(total)
            || extra.get("codex_subagent_running_count").and_then(Value::as_i64) != Some(running);
        if changed {
            extra["codex_subagent_total_count"] = Value::Number(total.into());
            extra["codex_subagent_running_count"] = Value::Number(running.into());
            extra["codex_subagent_completed_count"] = Value::Number((total - running).into());
            self.conversation_repo
                .update(
                    parent_conversation_id,
                    &ConversationRowUpdate {
                        extra: Some(extra.to_string()),
                        updated_at: Some(latest_child_at),
                        ..Default::default()
                    },
                )
                .await?;
            self.broadcast_conversation_updated(parent_conversation_id);
        }
        Ok(())
    }

    async fn mark_bindings_stored_only(&self) -> Result<(), ConversationError> {
        for binding in self.codex_repo.list_all_bindings().await? {
            self.codex_repo
                .update_binding_live_state(&binding.conversation_id, "stored_only", now_ms())
                .await?;
            if let Some(conversation) = self.conversation_repo.get(&binding.conversation_id).await? {
                let mut extra = serde_json::from_str::<Value>(&conversation.extra).unwrap_or_else(|_| json!({}));
                extra["codex_live_state"] = Value::String("stored_only".into());
                self.conversation_repo
                    .update(
                        &binding.conversation_id,
                        &ConversationRowUpdate {
                            extra: Some(extra.to_string()),
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
        history_cursor: None,
        history_complete: false,
        history_next_created_at: None,
        created_at: thread
            .get("createdAt")
            .and_then(Value::as_i64)
            .map_or(now, |value| value * 1000),
        updated_at: now,
    })
}

#[derive(Debug, Clone)]
struct CodexThreadMetadata {
    role: String,
    parent_thread_id: Option<String>,
    kind: Option<String>,
    agent_path: Option<String>,
    agent_nickname: Option<String>,
    agent_role: Option<String>,
    depth: Option<i64>,
    can_accept_direct_input: Option<bool>,
}

fn thread_metadata(thread: &Value) -> CodexThreadMetadata {
    let subagent = thread
        .pointer("/source/subAgent")
        .or_else(|| thread.pointer("/source/subagent"));
    let (kind, details) = subagent
        .and_then(Value::as_object)
        .and_then(|object| object.iter().next())
        .map(|(kind, details)| (Some(kind.replace('_', "-")), Some(details)))
        .unwrap_or((None, None));
    let parent_thread_id = thread
        .get("parentThreadId")
        .or_else(|| thread.get("parent_thread_id"))
        .and_then(Value::as_str)
        .or_else(|| {
            details
                .and_then(|value| value.get("parentThreadId").or_else(|| value.get("parent_thread_id")))
                .and_then(Value::as_str)
        })
        .map(str::to_owned);
    let thread_source = thread
        .get("threadSource")
        .or_else(|| thread.get("thread_source"))
        .and_then(Value::as_str);
    let is_subagent = parent_thread_id.is_some() || thread_source == Some("subagent") || subagent.is_some();
    let detail_string = |camel: &str, snake: &str| {
        details
            .and_then(|value| value.get(camel).or_else(|| value.get(snake)))
            .and_then(Value::as_str)
            .map(str::to_owned)
    };
    CodexThreadMetadata {
        role: if is_subagent { "subagent" } else { "root" }.into(),
        parent_thread_id,
        kind,
        agent_path: thread
            .get("agentPath")
            .or_else(|| thread.get("agent_path"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| detail_string("agentPath", "agent_path")),
        agent_nickname: thread
            .get("agentNickname")
            .or_else(|| thread.get("agent_nickname"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| detail_string("agentNickname", "agent_nickname")),
        agent_role: thread
            .get("agentRole")
            .or_else(|| thread.get("agent_role"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| detail_string("agentRole", "agent_role")),
        depth: details.and_then(|value| value.get("depth")).and_then(Value::as_i64),
        can_accept_direct_input: thread
            .get("canAcceptDirectInput")
            .or_else(|| thread.get("can_accept_direct_input"))
            .and_then(Value::as_bool),
    }
}

fn thread_parent_id(thread: &Value) -> Option<&str> {
    thread
        .get("parentThreadId")
        .or_else(|| thread.get("parent_thread_id"))
        .and_then(Value::as_str)
        .or_else(|| {
            thread
                .pointer("/source/subAgent/thread_spawn/parent_thread_id")
                .or_else(|| thread.pointer("/source/subagent/thread_spawn/parent_thread_id"))
                .or_else(|| thread.pointer("/source/subAgent/threadSpawn/parentThreadId"))
                .and_then(Value::as_str)
        })
}

fn thread_is_subagent(thread: &Value) -> bool {
    thread_parent_id(thread).is_some()
        || thread
            .get("threadSource")
            .or_else(|| thread.get("thread_source"))
            .and_then(Value::as_str)
            == Some("subagent")
        || thread.pointer("/source/subAgent").is_some()
        || thread.pointer("/source/subagent").is_some()
}

fn thread_display_title(thread: &Value, metadata: &CodexThreadMetadata) -> String {
    if let Some(name) = thread
        .get("name")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
    {
        return short_title(name);
    }
    if let Some(agent_path) = metadata.agent_path.as_deref()
        && let Some(segment) = agent_path.rsplit('/').find(|segment| !segment.is_empty())
    {
        let mut title = segment.replace(['_', '-'], " ");
        if let Some(first) = title.get_mut(0..1) {
            first.make_ascii_uppercase();
        }
        return short_title(&title);
    }
    if let Some(agent_role) = metadata.agent_role.as_deref().filter(|value| !value.trim().is_empty()) {
        return short_title(agent_role);
    }
    if let Some(agent_nickname) = metadata
        .agent_nickname
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        return short_title(agent_nickname);
    }
    if let Some(preview) = thread
        .get("preview")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
    {
        return short_title(preview);
    }
    if metadata.role == "subagent" {
        "GPT Codex 子智能体".into()
    } else {
        "GPT Codex".into()
    }
}

fn set_optional_string(target: &mut Value, key: &str, value: Option<&str>) {
    match value {
        Some(value) => target[key] = Value::String(value.to_owned()),
        None => {
            if let Some(object) = target.as_object_mut() {
                object.remove(key);
            }
        }
    }
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
        .or_else(|| {
            source.and_then(Value::as_object).and_then(|object| {
                object.keys().next().map(|key| {
                    if key.eq_ignore_ascii_case("subagent") || key.eq_ignore_ascii_case("subAgent") {
                        "subagent".to_owned()
                    } else {
                        key.clone()
                    }
                })
            })
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

fn thread_matches_workspace(thread: &Value, workspace: &CodexWatchedWorkspaceRow) -> bool {
    thread
        .get("cwd")
        .and_then(Value::as_str)
        .is_some_and(|cwd| workspace_matches(&workspace.root_path, cwd, workspace.recursive))
}

fn thread_is_active(thread: &Value) -> bool {
    thread.pointer("/status/type").and_then(Value::as_str) == Some("active")
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
