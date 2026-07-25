-- Persist app-server history pagination state per projected Codex thread.
-- The opaque cursor always points to the next older source page.

ALTER TABLE codex_thread_bindings ADD COLUMN history_cursor TEXT;
ALTER TABLE codex_thread_bindings ADD COLUMN history_complete INTEGER NOT NULL DEFAULT 0;
ALTER TABLE codex_thread_bindings ADD COLUMN history_next_created_at INTEGER;
