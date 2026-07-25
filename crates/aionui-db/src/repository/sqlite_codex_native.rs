use sqlx::SqlitePool;

use crate::error::DbError;
use crate::models::{CodexThreadBindingRow, CodexWatchedWorkspaceRow};
use crate::repository::ICodexNativeRepository;

#[derive(Clone, Debug)]
pub struct SqliteCodexNativeRepository {
    pool: SqlitePool,
}

impl SqliteCodexNativeRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl ICodexNativeRepository for SqliteCodexNativeRepository {
    async fn list_enabled_workspaces(&self) -> Result<Vec<CodexWatchedWorkspaceRow>, DbError> {
        Ok(sqlx::query_as::<_, CodexWatchedWorkspaceRow>(
            "SELECT * FROM codex_watched_workspaces WHERE enabled = 1 ORDER BY updated_at DESC",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    async fn upsert_workspace(&self, row: &CodexWatchedWorkspaceRow) -> Result<CodexWatchedWorkspaceRow, DbError> {
        sqlx::query(
            "INSERT INTO codex_watched_workspaces
             (id, user_id, root_path, recursive, enabled, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(user_id, root_path) DO UPDATE SET
                recursive = excluded.recursive,
                enabled = excluded.enabled,
                updated_at = excluded.updated_at",
        )
        .bind(&row.id)
        .bind(&row.user_id)
        .bind(&row.root_path)
        .bind(row.recursive)
        .bind(row.enabled)
        .bind(row.created_at)
        .bind(row.updated_at)
        .execute(&self.pool)
        .await?;

        Ok(sqlx::query_as::<_, CodexWatchedWorkspaceRow>(
            "SELECT * FROM codex_watched_workspaces WHERE user_id = ? AND root_path = ?",
        )
        .bind(&row.user_id)
        .bind(&row.root_path)
        .fetch_one(&self.pool)
        .await?)
    }

    async fn list_bindings_for_thread(
        &self,
        codex_home: &str,
        thread_id: &str,
    ) -> Result<Vec<CodexThreadBindingRow>, DbError> {
        Ok(sqlx::query_as::<_, CodexThreadBindingRow>(
            "SELECT * FROM codex_thread_bindings WHERE codex_home = ? AND thread_id = ?",
        )
        .bind(codex_home)
        .bind(thread_id)
        .fetch_all(&self.pool)
        .await?)
    }

    async fn list_all_bindings(&self) -> Result<Vec<CodexThreadBindingRow>, DbError> {
        Ok(
            sqlx::query_as::<_, CodexThreadBindingRow>("SELECT * FROM codex_thread_bindings ORDER BY updated_at DESC")
                .fetch_all(&self.pool)
                .await?,
        )
    }

    async fn get_binding_for_conversation(
        &self,
        conversation_id: &str,
    ) -> Result<Option<CodexThreadBindingRow>, DbError> {
        Ok(
            sqlx::query_as::<_, CodexThreadBindingRow>("SELECT * FROM codex_thread_bindings WHERE conversation_id = ?")
                .bind(conversation_id)
                .fetch_optional(&self.pool)
                .await?,
        )
    }

    async fn upsert_binding(&self, row: &CodexThreadBindingRow) -> Result<CodexThreadBindingRow, DbError> {
        sqlx::query(
            "INSERT INTO codex_thread_bindings
             (codex_home, thread_id, user_id, conversation_id, cwd, source, live_state, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(user_id, codex_home, thread_id) DO UPDATE SET
                cwd = excluded.cwd,
                source = excluded.source,
                live_state = excluded.live_state,
                updated_at = excluded.updated_at",
        )
        .bind(&row.codex_home)
        .bind(&row.thread_id)
        .bind(&row.user_id)
        .bind(&row.conversation_id)
        .bind(&row.cwd)
        .bind(&row.source)
        .bind(&row.live_state)
        .bind(row.created_at)
        .bind(row.updated_at)
        .execute(&self.pool)
        .await?;

        Ok(sqlx::query_as::<_, CodexThreadBindingRow>(
            "SELECT * FROM codex_thread_bindings WHERE user_id = ? AND codex_home = ? AND thread_id = ?",
        )
        .bind(&row.user_id)
        .bind(&row.codex_home)
        .bind(&row.thread_id)
        .fetch_one(&self.pool)
        .await?)
    }

    async fn update_binding_live_state(
        &self,
        conversation_id: &str,
        live_state: &str,
        updated_at: aionui_common::TimestampMs,
    ) -> Result<(), DbError> {
        sqlx::query("UPDATE codex_thread_bindings SET live_state = ?, updated_at = ? WHERE conversation_id = ?")
            .bind(live_state)
            .bind(updated_at)
            .bind(conversation_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn update_binding_history_state(
        &self,
        conversation_id: &str,
        history_cursor: Option<&str>,
        history_complete: bool,
        history_next_created_at: aionui_common::TimestampMs,
        updated_at: aionui_common::TimestampMs,
    ) -> Result<(), DbError> {
        sqlx::query(
            "UPDATE codex_thread_bindings
             SET history_cursor = ?, history_complete = ?, history_next_created_at = ?, updated_at = ?
             WHERE conversation_id = ?",
        )
        .bind(history_cursor)
        .bind(history_complete)
        .bind(history_next_created_at)
        .bind(updated_at)
        .bind(conversation_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
