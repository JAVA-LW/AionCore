use aionui_db::{
    CodexThreadBindingRow, CodexWatchedWorkspaceRow, ICodexNativeRepository, IConversationRepository,
    SqliteCodexNativeRepository, SqliteConversationRepository, init_database_memory, models::ConversationRow,
};

const USER_ID: &str = "system_default_user";

#[tokio::test]
async fn workspace_upsert_preserves_identity_and_updates_policy() {
    let db = init_database_memory().await.unwrap();
    let repo = SqliteCodexNativeRepository::new(db.pool().clone());
    let original = workspace("watch-1", "/home/taichuy/git/1flowbase", true, true, 10);
    assert_eq!(repo.upsert_workspace(&original).await.unwrap(), original);

    let replacement = workspace("watch-replacement", "/home/taichuy/git/1flowbase", false, false, 20);
    let saved = repo.upsert_workspace(&replacement).await.unwrap();
    assert_eq!(
        saved,
        CodexWatchedWorkspaceRow {
            id: "watch-1".into(),
            created_at: 10,
            ..replacement
        }
    );
    assert!(repo.list_enabled_workspaces().await.unwrap().is_empty());
}

#[tokio::test]
async fn thread_binding_upsert_is_idempotent_and_tracks_live_state() {
    let db = init_database_memory().await.unwrap();
    let conversation_repo = SqliteConversationRepository::new(db.pool().clone());
    conversation_repo.create(&conversation("conversation-1")).await.unwrap();
    let repo = SqliteCodexNativeRepository::new(db.pool().clone());

    let original = binding("conversation-1", "live", "/home/taichuy/git/1flowbase", 10);
    assert_eq!(repo.upsert_binding(&original).await.unwrap(), original);

    let replacement = binding(
        "ignored-conversation-id",
        "stored_only",
        "/home/taichuy/git/1flowbase/api",
        20,
    );
    let saved = repo.upsert_binding(&replacement).await.unwrap();
    assert_eq!(
        saved,
        CodexThreadBindingRow {
            conversation_id: "conversation-1".into(),
            created_at: 10,
            ..replacement
        }
    );
    assert_eq!(
        repo.list_bindings_for_thread("/home/taichuy/.codex", "thread-1")
            .await
            .unwrap(),
        vec![saved.clone()]
    );

    repo.update_binding_live_state("conversation-1", "live", 30)
        .await
        .unwrap();
    assert_eq!(
        repo.get_binding_for_conversation("conversation-1").await.unwrap(),
        Some(CodexThreadBindingRow {
            live_state: "live".into(),
            updated_at: 30,
            ..saved.clone()
        })
    );

    repo.update_binding_history_state("conversation-1", Some("next-page"), false, 29, 40)
        .await
        .unwrap();
    assert_eq!(
        repo.get_binding_for_conversation("conversation-1").await.unwrap(),
        Some(CodexThreadBindingRow {
            live_state: "live".into(),
            history_cursor: Some("next-page".into()),
            history_next_created_at: Some(29),
            updated_at: 40,
            ..saved
        })
    );
}

fn workspace(id: &str, root_path: &str, recursive: bool, enabled: bool, timestamp: i64) -> CodexWatchedWorkspaceRow {
    CodexWatchedWorkspaceRow {
        id: id.into(),
        user_id: USER_ID.into(),
        root_path: root_path.into(),
        recursive,
        enabled,
        created_at: timestamp,
        updated_at: timestamp,
    }
}

fn binding(conversation_id: &str, live_state: &str, cwd: &str, timestamp: i64) -> CodexThreadBindingRow {
    CodexThreadBindingRow {
        codex_home: "/home/taichuy/.codex".into(),
        thread_id: "thread-1".into(),
        user_id: USER_ID.into(),
        conversation_id: conversation_id.into(),
        cwd: cwd.into(),
        source: "cli".into(),
        live_state: live_state.into(),
        history_cursor: None,
        history_complete: false,
        history_next_created_at: None,
        created_at: timestamp,
        updated_at: timestamp,
    }
}

fn conversation(id: &str) -> ConversationRow {
    ConversationRow {
        id: id.into(),
        user_id: USER_ID.into(),
        name: "Codex task".into(),
        r#type: "codex-app-server".into(),
        extra: r#"{"workspace":"/home/taichuy/git/1flowbase"}"#.into(),
        model: None,
        status: Some("pending".into()),
        source: Some("aionui".into()),
        channel_chat_id: None,
        pinned: false,
        pinned_at: None,
        created_at: 1,
        updated_at: 1,
    }
}
