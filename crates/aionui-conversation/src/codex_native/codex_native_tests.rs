use std::collections::HashMap;

use aionui_ai_agent::capability::codex_app_server::{CodexAppServerSnapshot, CodexSendReceipt, CodexThreadRuntime};
use aionui_db::{
    ICodexNativeRepository, IConversationRepository, SqliteCodexNativeRepository, SqliteConversationRepository,
    init_database_memory,
};
use aionui_realtime::BroadcastEventBus;
use async_trait::async_trait;
use tokio::sync::{RwLock, broadcast};

use super::*;

#[test]
fn workspace_match_is_component_aware() {
    assert!(workspace_matches("/tmp/project", "/tmp/project", true));
    assert!(workspace_matches("/tmp/project", "/tmp/project/crate", true));
    assert!(!workspace_matches("/tmp/project", "/tmp/project-other", true));
    assert!(!workspace_matches("/tmp/project", "/tmp/project/crate", false));
}

#[test]
fn command_approval_maps_to_existing_confirmation_contract() {
    let request = CodexPendingRequest {
        request_id: json!(7),
        method: "item/commandExecution/requestApproval".into(),
        params: json!({"threadId": "t1", "command": "cargo test"}),
    };
    let confirmation = pending_request_to_confirmation(&request).unwrap();
    assert_eq!(confirmation.call_id, "7");
    assert_eq!(confirmation.action.as_deref(), Some("exec"));
    assert_eq!(confirmation.options[0].value, json!("proceed_once"));
}

#[tokio::test]
async fn discovery_is_workspace_scoped_idempotent_and_streams_command_output() {
    let db = init_database_memory().await.unwrap();
    let codex_repo: Arc<dyn ICodexNativeRepository> = Arc::new(SqliteCodexNativeRepository::new(db.pool().clone()));
    let conversation_repo: Arc<dyn IConversationRepository> =
        Arc::new(SqliteConversationRepository::new(db.pool().clone()));
    codex_repo
        .upsert_workspace(&CodexWatchedWorkspaceRow {
            id: "watch-1".into(),
            user_id: "system_default_user".into(),
            root_path: "/home/taichuy/git/1flowbase".into(),
            recursive: true,
            enabled: true,
            created_at: 1,
            updated_at: 1,
        })
        .await
        .unwrap();

    let target_thread = thread("thread-target", "/home/taichuy/git/1flowbase/api");
    let other_thread = thread("thread-other", "/home/taichuy/git/1flowbase-other");
    let gateway = Arc::new(FakeGateway::new(vec![target_thread.clone(), other_thread.clone()]));
    let event_bus = Arc::new(BroadcastEventBus::new(32));
    let mut events = event_bus.subscribe();
    let runtime = CodexNativeRuntime::new(
        gateway,
        codex_repo.clone(),
        conversation_repo.clone(),
        event_bus,
        Arc::new(ConversationRuntimeStateService::default()),
    );

    runtime
        .handle_notification("thread/started", &json!({"thread": target_thread}))
        .await
        .unwrap();
    runtime
        .handle_notification("thread/started", &json!({"thread": other_thread}))
        .await
        .unwrap();
    runtime
        .handle_notification(
            "thread/started",
            &json!({"thread": thread("thread-target", "/home/taichuy/git/1flowbase/api")}),
        )
        .await
        .unwrap();

    let bindings = codex_repo.list_all_bindings().await.unwrap();
    assert_eq!(bindings.len(), 1);
    let binding = &bindings[0];
    assert_eq!(binding.thread_id, "thread-target");
    assert_eq!(binding.cwd, "/home/taichuy/git/1flowbase/api");

    while events.try_recv().is_ok() {}
    runtime
        .handle_notification(
            "turn/started",
            &json!({
                "threadId": "thread-target",
                "turn": {"id": "turn-1", "status": "inProgress"}
            }),
        )
        .await
        .unwrap();
    let event_names = std::iter::from_fn(|| events.try_recv().ok())
        .map(|event| event.name)
        .collect::<Vec<_>>();
    assert!(event_names.iter().any(|name| name == "message.stream"));
    assert!(event_names.iter().any(|name| name == "conversation.listChanged"));

    runtime
        .handle_notification(
            "item/started",
            &json!({
                "threadId": "thread-target",
                "turnId": "turn-1",
                "item": {
                    "type": "commandExecution",
                    "id": "command-1",
                    "command": "cargo test",
                    "cwd": "/home/taichuy/git/1flowbase/api",
                    "status": "inProgress"
                }
            }),
        )
        .await
        .unwrap();
    runtime
        .handle_notification(
            "item/commandExecution/outputDelta",
            &json!({
                "threadId": "thread-target",
                "turnId": "turn-1",
                "itemId": "command-1",
                "delta": "test result: ok"
            }),
        )
        .await
        .unwrap();

    let message = conversation_repo
        .get_message(&binding.conversation_id, "command-1")
        .await
        .unwrap()
        .unwrap();
    let content: Value = serde_json::from_str(&message.content).unwrap();
    assert_eq!(content.get("output").and_then(Value::as_str), Some("test result: ok"));
    assert_eq!(message.status.as_deref(), Some("work"));

    runtime
        .handle_notification(
            "thread/status/changed",
            &json!({
                "threadId": "thread-target",
                "status": {"type": "idle"}
            }),
        )
        .await
        .unwrap();
    let conversation = conversation_repo.get(&binding.conversation_id).await.unwrap().unwrap();
    assert_eq!(conversation.status.as_deref(), Some("finished"));
}

#[tokio::test]
async fn legacy_history_falls_back_to_turn_summaries() {
    let db = init_database_memory().await.unwrap();
    let codex_repo: Arc<dyn ICodexNativeRepository> = Arc::new(SqliteCodexNativeRepository::new(db.pool().clone()));
    let conversation_repo: Arc<dyn IConversationRepository> =
        Arc::new(SqliteConversationRepository::new(db.pool().clone()));
    codex_repo
        .upsert_workspace(&CodexWatchedWorkspaceRow {
            id: "watch-legacy".into(),
            user_id: "system_default_user".into(),
            root_path: "/home/taichuy/git/legacy".into(),
            recursive: true,
            enabled: true,
            created_at: 1,
            updated_at: 1,
        })
        .await
        .unwrap();

    let legacy_thread = thread("thread-legacy", "/home/taichuy/git/legacy");
    let gateway = Arc::new(FakeGateway::new(vec![legacy_thread.clone()]).with_legacy_turns(
        aionui_ai_agent::capability::codex_app_server::CodexThreadTurnsPage {
            entries: vec![
                json!({"turnId": "turn-legacy", "item": {"type": "agentMessage", "id": "agent-1", "text": "done"}}),
                json!({"turnId": "turn-legacy", "item": {"type": "userMessage", "id": "user-1", "content": [{"type": "text", "text": "start"}]}}),
            ],
            next_cursor: None,
        },
    ));
    let runtime = CodexNativeRuntime::new(
        gateway,
        codex_repo.clone(),
        conversation_repo.clone(),
        Arc::new(BroadcastEventBus::new(32)),
        Arc::new(ConversationRuntimeStateService::default()),
    );
    runtime
        .handle_notification("thread/started", &json!({"thread": legacy_thread}))
        .await
        .unwrap();
    let binding = codex_repo.list_all_bindings().await.unwrap().pop().unwrap();

    runtime
        .sync_history_page(&binding.conversation_id, None, 20)
        .await
        .unwrap();

    assert!(
        conversation_repo
            .get_message(&binding.conversation_id, "user-1")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        conversation_repo
            .get_message(&binding.conversation_id, "agent-1")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        codex_repo
            .get_binding_for_conversation(&binding.conversation_id)
            .await
            .unwrap()
            .unwrap()
            .history_complete
    );
}

#[tokio::test]
async fn workspace_discovery_projects_only_five_recent_finished_threads() {
    let db = init_database_memory().await.unwrap();
    let codex_repo: Arc<dyn ICodexNativeRepository> = Arc::new(SqliteCodexNativeRepository::new(db.pool().clone()));
    let conversation_repo: Arc<dyn IConversationRepository> =
        Arc::new(SqliteConversationRepository::new(db.pool().clone()));
    let workspace = CodexWatchedWorkspaceRow {
        id: "watch-recent".into(),
        user_id: "system_default_user".into(),
        root_path: "/home/taichuy/git/recent".into(),
        recursive: true,
        enabled: true,
        created_at: 1,
        updated_at: 1,
    };
    let threads = (0..8)
        .map(|index| idle_thread(&format!("thread-{index}"), "/home/taichuy/git/recent"))
        .collect();
    let runtime = CodexNativeRuntime::new(
        Arc::new(FakeGateway::new(threads)),
        codex_repo.clone(),
        conversation_repo,
        Arc::new(BroadcastEventBus::new(32)),
        Arc::new(ConversationRuntimeStateService::default()),
    );

    runtime.discover_workspace(&workspace).await.unwrap();

    assert_eq!(codex_repo.list_all_bindings().await.unwrap().len(), 5);
}

fn thread(id: &str, cwd: &str) -> Value {
    json!({
        "id": id,
        "cwd": cwd,
        "source": "cli",
        "status": {"type": "active", "activeFlags": []},
        "createdAt": 1,
        "updatedAt": 1,
        "turns": []
    })
}

fn idle_thread(id: &str, cwd: &str) -> Value {
    let mut thread = thread(id, cwd);
    thread["status"] = json!({"type": "idle"});
    thread
}

struct FakeGateway {
    events: broadcast::Sender<CodexAppServerEvent>,
    snapshot: RwLock<CodexAppServerSnapshot>,
    legacy_turns: Option<aionui_ai_agent::capability::codex_app_server::CodexThreadTurnsPage>,
}

impl FakeGateway {
    fn new(threads: Vec<Value>) -> Self {
        let (events, _) = broadcast::channel(16);
        Self {
            events,
            snapshot: RwLock::new(CodexAppServerSnapshot {
                connected: true,
                codex_home: Some(PathBuf::from("/home/taichuy/.codex")),
                threads: threads
                    .into_iter()
                    .map(|thread| (thread["id"].as_str().unwrap().to_owned(), thread))
                    .collect(),
                runtimes: HashMap::from([(
                    "thread-target".into(),
                    CodexThreadRuntime {
                        active_turn_id: Some("turn-1".into()),
                        status: "active".into(),
                    },
                )]),
                ..Default::default()
            }),
            legacy_turns: None,
        }
    }

    fn with_legacy_turns(mut self, page: aionui_ai_agent::capability::codex_app_server::CodexThreadTurnsPage) -> Self {
        self.legacy_turns = Some(page);
        self
    }
}

#[async_trait]
impl ICodexAppServerGateway for FakeGateway {
    fn subscribe(&self) -> broadcast::Receiver<CodexAppServerEvent> {
        self.events.subscribe()
    }

    async fn snapshot(&self) -> CodexAppServerSnapshot {
        self.snapshot.read().await.clone()
    }

    async fn list_models(&self) -> Result<Vec<Value>, CodexAppServerError> {
        Ok(Vec::new())
    }

    async fn list_thread_page(
        &self,
        cursor: Option<&str>,
        _limit: u32,
        _archived: bool,
    ) -> Result<aionui_ai_agent::capability::codex_app_server::CodexThreadPage, CodexAppServerError> {
        if cursor.is_some() {
            return Ok(Default::default());
        }
        Ok(aionui_ai_agent::capability::codex_app_server::CodexThreadPage {
            threads: self.snapshot.read().await.threads.values().cloned().collect(),
            next_cursor: None,
        })
    }

    async fn list_loaded_threads(&self) -> Result<Vec<String>, CodexAppServerError> {
        Ok(vec!["thread-target".into()])
    }

    async fn read_thread(&self, thread_id: &str, _include_turns: bool) -> Result<Value, CodexAppServerError> {
        self.snapshot
            .read()
            .await
            .threads
            .get(thread_id)
            .cloned()
            .ok_or_else(|| CodexAppServerError::InvalidResponse("missing fake thread".into()))
    }

    async fn resume_thread(&self, thread_id: &str) -> Result<Value, CodexAppServerError> {
        self.read_thread(thread_id, true).await
    }

    async fn list_thread_items(
        &self,
        _thread_id: &str,
        _cursor: Option<&str>,
        _limit: u32,
    ) -> Result<aionui_ai_agent::capability::codex_app_server::CodexThreadItemsPage, CodexAppServerError> {
        if self.legacy_turns.is_some() {
            return Err(CodexAppServerError::Rpc {
                code: -32601,
                message: "thread/items/list is not supported yet".into(),
                data: None,
            });
        }
        Ok(Default::default())
    }

    async fn list_thread_turns_summary(
        &self,
        _thread_id: &str,
        _cursor: Option<&str>,
        _limit: u32,
    ) -> Result<aionui_ai_agent::capability::codex_app_server::CodexThreadTurnsPage, CodexAppServerError> {
        Ok(self.legacy_turns.clone().unwrap_or_default())
    }

    async fn start_thread(
        &self,
        _cwd: &str,
        _model: Option<&str>,
        _reasoning_effort: Option<&str>,
    ) -> Result<Value, CodexAppServerError> {
        Err(CodexAppServerError::Unavailable("not used by this test".into()))
    }

    async fn send_input(
        &self,
        _thread_id: &str,
        _client_message_id: &str,
        _text: &str,
    ) -> Result<CodexSendReceipt, CodexAppServerError> {
        Err(CodexAppServerError::Unavailable("not used by this test".into()))
    }

    async fn interrupt(&self, _thread_id: &str, _turn_id: &str) -> Result<(), CodexAppServerError> {
        Ok(())
    }

    async fn respond_to_server_request(&self, _request_id: Value, _result: Value) -> Result<(), CodexAppServerError> {
        Ok(())
    }
}
