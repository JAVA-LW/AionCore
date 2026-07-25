use crate::error::DbError;
use crate::models::{CodexThreadBindingRow, CodexWatchedWorkspaceRow};

#[async_trait::async_trait]
pub trait ICodexNativeRepository: Send + Sync {
    async fn list_enabled_workspaces(&self) -> Result<Vec<CodexWatchedWorkspaceRow>, DbError>;
    async fn upsert_workspace(&self, row: &CodexWatchedWorkspaceRow) -> Result<CodexWatchedWorkspaceRow, DbError>;
    async fn list_bindings_for_thread(
        &self,
        codex_home: &str,
        thread_id: &str,
    ) -> Result<Vec<CodexThreadBindingRow>, DbError>;
    async fn list_all_bindings(&self) -> Result<Vec<CodexThreadBindingRow>, DbError>;
    async fn get_binding_for_conversation(
        &self,
        conversation_id: &str,
    ) -> Result<Option<CodexThreadBindingRow>, DbError>;
    async fn upsert_binding(&self, row: &CodexThreadBindingRow) -> Result<CodexThreadBindingRow, DbError>;
    async fn update_binding_live_state(
        &self,
        conversation_id: &str,
        live_state: &str,
        updated_at: aionui_common::TimestampMs,
    ) -> Result<(), DbError>;
    async fn update_binding_history_state(
        &self,
        conversation_id: &str,
        history_cursor: Option<&str>,
        history_complete: bool,
        history_next_created_at: aionui_common::TimestampMs,
        updated_at: aionui_common::TimestampMs,
    ) -> Result<(), DbError>;
}
