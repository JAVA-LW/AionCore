use aionui_common::TimestampMs;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, sqlx::FromRow)]
pub struct CodexWatchedWorkspaceRow {
    pub id: String,
    pub user_id: String,
    pub root_path: String,
    pub recursive: bool,
    pub enabled: bool,
    pub created_at: TimestampMs,
    pub updated_at: TimestampMs,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, sqlx::FromRow)]
pub struct CodexThreadBindingRow {
    pub codex_home: String,
    pub thread_id: String,
    pub user_id: String,
    pub conversation_id: String,
    pub cwd: String,
    pub source: String,
    pub live_state: String,
    pub history_cursor: Option<String>,
    pub history_complete: bool,
    pub history_next_created_at: Option<TimestampMs>,
    pub created_at: TimestampMs,
    pub updated_at: TimestampMs,
}
