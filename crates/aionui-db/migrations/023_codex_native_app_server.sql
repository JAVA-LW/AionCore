-- Native Codex app-server integration. Watched workspaces determine which
-- external Codex threads are projected into AionUI conversations.

CREATE TABLE IF NOT EXISTS codex_watched_workspaces (
    id          TEXT PRIMARY KEY NOT NULL,
    user_id     TEXT NOT NULL,
    root_path   TEXT NOT NULL,
    recursive   INTEGER NOT NULL DEFAULT 1,
    enabled     INTEGER NOT NULL DEFAULT 1,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL,
    UNIQUE(user_id, root_path)
);
CREATE INDEX IF NOT EXISTS idx_codex_watched_workspaces_enabled
    ON codex_watched_workspaces(enabled, user_id);

CREATE TABLE IF NOT EXISTS codex_thread_bindings (
    codex_home      TEXT NOT NULL,
    thread_id       TEXT NOT NULL,
    user_id         TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    cwd             TEXT NOT NULL,
    source          TEXT NOT NULL DEFAULT 'unknown',
    live_state      TEXT NOT NULL DEFAULT 'stored_only',
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL,
    PRIMARY KEY(user_id, codex_home, thread_id),
    UNIQUE(conversation_id),
    FOREIGN KEY(conversation_id) REFERENCES conversations(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_codex_thread_bindings_thread
    ON codex_thread_bindings(codex_home, thread_id);
CREATE INDEX IF NOT EXISTS idx_codex_thread_bindings_conversation
    ON codex_thread_bindings(conversation_id);

INSERT INTO agent_metadata
    (id, icon, name, description, backend, agent_type, agent_source, agent_source_info,
     enabled, command, args, env, native_skills_dirs, behavior_policy, sort_order,
     created_at, updated_at)
VALUES
    ('c0d3a55e', '/api/assets/logos/tools/coding/codex.svg', 'GPT Codex',
     'Native Codex app-server conversations and watched workspace threads.',
     'codex-native', 'codex-app-server', 'builtin', '{"binary_name":"codex"}',
     1, 'codex', '[]', '[]', '[".codex/skills"]',
     '{"supports_side_question":true,"supports_team":false}', 3105,
     unixepoch('now','subsec')*1000, unixepoch('now','subsec')*1000)
ON CONFLICT(id) DO UPDATE SET
    icon = excluded.icon,
    name = excluded.name,
    description = excluded.description,
    backend = excluded.backend,
    agent_type = excluded.agent_type,
    agent_source = excluded.agent_source,
    agent_source_info = excluded.agent_source_info,
    command = excluded.command,
    args = excluded.args,
    env = excluded.env,
    native_skills_dirs = excluded.native_skills_dirs,
    behavior_policy = excluded.behavior_policy,
    sort_order = excluded.sort_order,
    updated_at = excluded.updated_at;
