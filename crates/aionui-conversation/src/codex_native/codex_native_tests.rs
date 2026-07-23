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
    let runtime = CodexNativeRuntime::new(
        gateway,
        codex_repo.clone(),
        conversation_repo.clone(),
        Arc::new(BroadcastEventBus::new(32)),
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

struct FakeGateway {
    events: broadcast::Sender<CodexAppServerEvent>,
    snapshot: RwLock<CodexAppServerSnapshot>,
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
        }
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

    async fn list_threads(&self, _archived: bool) -> Result<Vec<Value>, CodexAppServerError> {
        Ok(self.snapshot.read().await.threads.values().cloned().collect())
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

    async fn start_thread(&self, _cwd: &str) -> Result<Value, CodexAppServerError> {
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
