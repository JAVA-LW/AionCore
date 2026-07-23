use aionui_ai_agent::capability::codex_app_server::CodexPendingRequest;
use aionui_api_types::WebSocketMessage;
use aionui_common::{ErrorChain, now_ms};
use aionui_db::{CodexThreadBindingRow, ConversationRowUpdate};
use serde_json::{Value, json};
use tracing::{debug, warn};

use super::{CodexNativeRuntime, pending_request_to_confirmation};
use crate::ConversationError;

impl CodexNativeRuntime {
    pub(super) async fn handle_notification(&self, method: &str, params: &Value) -> Result<(), ConversationError> {
        if method == "thread/started" {
            let Some(thread) = params.get("thread") else {
                return Ok(());
            };
            let _guard = self.start_lock.lock().await;
            let codex_home = self.codex_home().await?;
            let bindings = self.ensure_bindings_for_thread(&codex_home, thread).await?;
            for binding in bindings {
                self.project_thread_history(&binding, thread).await?;
            }
            return Ok(());
        }

        let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
            return Ok(());
        };
        let codex_home = self.codex_home().await?;
        let mut bindings = self.codex_repo.list_bindings_for_thread(&codex_home, thread_id).await?;
        if bindings.is_empty()
            && let Some(thread) = self.gateway.snapshot().await.threads.get(thread_id).cloned()
        {
            bindings = self.ensure_bindings_for_thread(&codex_home, &thread).await?;
        }
        if bindings.is_empty() {
            return Ok(());
        }

        match method {
            "turn/started" => {
                let Some(turn_id) = params.pointer("/turn/id").and_then(Value::as_str) else {
                    return Ok(());
                };
                for binding in &bindings {
                    self.runtime_state.set_external_turn(&binding.conversation_id, turn_id);
                    self.conversation_repo
                        .update(
                            &binding.conversation_id,
                            &ConversationRowUpdate {
                                status: Some("running".into()),
                                updated_at: Some(now_ms()),
                                ..Default::default()
                            },
                        )
                        .await?;
                    self.broadcast_stream(&binding.conversation_id, turn_id, turn_id, "start", json!({}), None);
                }
            }
            "turn/completed" => {
                let Some(turn_id) = params.pointer("/turn/id").and_then(Value::as_str) else {
                    return Ok(());
                };
                for binding in &bindings {
                    self.runtime_state
                        .clear_external_turn(&binding.conversation_id, turn_id);
                    self.conversation_repo
                        .update(
                            &binding.conversation_id,
                            &ConversationRowUpdate {
                                status: Some("finished".into()),
                                updated_at: Some(now_ms()),
                                ..Default::default()
                            },
                        )
                        .await?;
                    self.broadcast_stream(&binding.conversation_id, turn_id, turn_id, "finish", json!({}), None);
                    self.broadcaster.broadcast(WebSocketMessage::new(
                        "turn.completed",
                        json!({
                            "conversation_id": binding.conversation_id,
                            "session_id": binding.conversation_id,
                            "turn_id": turn_id,
                            "status": "finished",
                            "canSendMessage": true,
                        }),
                    ));
                    self.deliver_pending(&binding.conversation_id, thread_id).await;
                }
            }
            "item/agentMessage/delta" => {
                let Some(item_id) = params.get("itemId").and_then(Value::as_str) else {
                    return Ok(());
                };
                let delta = params.get("delta").and_then(Value::as_str).unwrap_or_default();
                let turn_id = params.get("turnId").and_then(Value::as_str).unwrap_or_default();
                for binding in &bindings {
                    let key = (binding.conversation_id.clone(), item_id.to_owned());
                    let text = {
                        let mut buffers = self.text_buffers.lock().await;
                        let buffer = buffers.entry(key).or_default();
                        buffer.push_str(delta);
                        buffer.clone()
                    };
                    self.persist_text(binding, item_id, &text, "left", "work", now_ms())
                        .await?;
                    self.broadcast_stream(
                        &binding.conversation_id,
                        item_id,
                        turn_id,
                        "content",
                        json!({"content": delta}),
                        Some(("left", "work")),
                    );
                }
            }
            "item/reasoning/summaryTextDelta" | "item/reasoning/textDelta" => {
                let Some(item_id) = params.get("itemId").and_then(Value::as_str) else {
                    return Ok(());
                };
                let delta = params.get("delta").and_then(Value::as_str).unwrap_or_default();
                let turn_id = params.get("turnId").and_then(Value::as_str).unwrap_or_default();
                for binding in &bindings {
                    let key = (binding.conversation_id.clone(), item_id.to_owned());
                    let text = {
                        let mut buffers = self.thinking_buffers.lock().await;
                        let buffer = buffers.entry(key).or_default();
                        buffer.push_str(delta);
                        buffer.clone()
                    };
                    self.persist_structured(
                        binding,
                        &format!("thinking-{item_id}"),
                        item_id,
                        "thinking",
                        json!({"content": text, "status": "thinking"}),
                        "left",
                        "work",
                        now_ms(),
                    )
                    .await?;
                    self.broadcast_stream(
                        &binding.conversation_id,
                        item_id,
                        turn_id,
                        "thinking",
                        json!({"content": delta, "status": "thinking"}),
                        None,
                    );
                }
            }
            "item/commandExecution/outputDelta" => {
                let Some(item_id) = params.get("itemId").and_then(Value::as_str) else {
                    return Ok(());
                };
                let delta = params.get("delta").and_then(Value::as_str).unwrap_or_default();
                let turn_id = params.get("turnId").and_then(Value::as_str).unwrap_or_default();
                for binding in &bindings {
                    let key = (binding.conversation_id.clone(), item_id.to_owned());
                    let output = {
                        let mut buffers = self.command_output_buffers.lock().await;
                        let buffer = buffers.entry(key).or_default();
                        buffer.push_str(delta);
                        buffer.clone()
                    };
                    let data = json!({
                        "call_id": item_id,
                        "name": "shell",
                        "status": "running",
                        "output": output,
                    });
                    self.persist_structured(
                        binding,
                        item_id,
                        item_id,
                        "tool_call",
                        data.clone(),
                        "left",
                        "work",
                        now_ms(),
                    )
                    .await?;
                    self.broadcast_stream(&binding.conversation_id, item_id, turn_id, "tool_call", data, None);
                }
            }
            "item/started" => {
                if let Some(item) = params.get("item") {
                    let turn_id = params.get("turnId").and_then(Value::as_str).unwrap_or_default();
                    let created_at = params.get("startedAtMs").and_then(Value::as_i64).unwrap_or_else(now_ms);
                    for binding in &bindings {
                        self.project_item(binding, turn_id, item, created_at, true, false)
                            .await?;
                    }
                }
            }
            "item/completed" => {
                if let Some(item) = params.get("item") {
                    let turn_id = params.get("turnId").and_then(Value::as_str).unwrap_or_default();
                    let created_at = params
                        .get("completedAtMs")
                        .and_then(Value::as_i64)
                        .unwrap_or_else(now_ms);
                    for binding in &bindings {
                        self.project_item(binding, turn_id, item, created_at, true, true)
                            .await?;
                    }
                }
            }
            "error" => {
                let turn_id = params.get("turnId").and_then(Value::as_str).unwrap_or_default();
                let message = params
                    .pointer("/error/message")
                    .and_then(Value::as_str)
                    .unwrap_or("Codex turn failed");
                for binding in &bindings {
                    self.broadcast_stream(
                        &binding.conversation_id,
                        turn_id,
                        turn_id,
                        "error",
                        json!({"message": message}),
                        None,
                    );
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub(super) async fn handle_server_request(&self, request: &CodexPendingRequest) -> Result<(), ConversationError> {
        let Some(thread_id) = request.params.get("threadId").and_then(Value::as_str) else {
            return Ok(());
        };
        let codex_home = self.codex_home().await?;
        let mut bindings = self.codex_repo.list_bindings_for_thread(&codex_home, thread_id).await?;
        if bindings.is_empty()
            && let Some(thread) = self.gateway.snapshot().await.threads.get(thread_id).cloned()
        {
            bindings = self.ensure_bindings_for_thread(&codex_home, &thread).await?;
        }
        let Some(confirmation) = pending_request_to_confirmation(request) else {
            debug!(
                method = request.method,
                "Codex server request is not represented by the confirmation UI"
            );
            return Ok(());
        };
        let data = serde_json::to_value(&confirmation)
            .map_err(|error| ConversationError::internal(format!("serialize Codex confirmation: {error}")))?;
        let turn_id = request.params.get("turnId").and_then(Value::as_str).unwrap_or_default();
        for binding in bindings {
            self.broadcast_stream(
                &binding.conversation_id,
                &confirmation.call_id,
                turn_id,
                "permission",
                data.clone(),
                None,
            );
        }
        Ok(())
    }

    pub(super) async fn project_thread_history(
        &self,
        binding: &CodexThreadBindingRow,
        thread: &Value,
    ) -> Result<(), ConversationError> {
        let Some(turns) = thread.get("turns").and_then(Value::as_array) else {
            return Ok(());
        };
        for turn in turns {
            let turn_id = turn.get("id").and_then(Value::as_str).unwrap_or_default();
            let started_at = turn
                .get("startedAt")
                .and_then(Value::as_i64)
                .map_or_else(now_ms, |value| value * 1000);
            if turn.get("status").and_then(Value::as_str) == Some("inProgress") {
                self.runtime_state.set_external_turn(&binding.conversation_id, turn_id);
            }
            if let Some(items) = turn.get("items").and_then(Value::as_array) {
                for (index, item) in items.iter().enumerate() {
                    self.project_item(binding, turn_id, item, started_at + index as i64, false, true)
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn project_item(
        &self,
        binding: &CodexThreadBindingRow,
        turn_id: &str,
        item: &Value,
        created_at: i64,
        broadcast: bool,
        completed: bool,
    ) -> Result<(), ConversationError> {
        let item_type = item.get("type").and_then(Value::as_str).unwrap_or_default();
        let item_id = item.get("id").and_then(Value::as_str).unwrap_or_default();
        if item_id.is_empty() {
            return Ok(());
        }
        match item_type {
            "userMessage" if completed => {
                let text = user_message_text(item);
                let msg_id = item.get("clientId").and_then(Value::as_str).unwrap_or(item_id);
                self.persist_text(binding, msg_id, &text, "right", "finish", created_at)
                    .await?;
                if broadcast {
                    self.broadcaster.broadcast(WebSocketMessage::new(
                        "message.userCreated",
                        json!({
                            "conversation_id": binding.conversation_id,
                            "msg_id": msg_id,
                            "content": text,
                            "position": "right",
                            "status": "finish",
                            "hidden": false,
                            "created_at": created_at,
                        }),
                    ));
                }
            }
            "agentMessage" if completed => {
                let text = item.get("text").and_then(Value::as_str).unwrap_or_default();
                let had_stream = self
                    .text_buffers
                    .lock()
                    .await
                    .remove(&(binding.conversation_id.clone(), item_id.to_owned()))
                    .is_some();
                self.persist_text(binding, item_id, text, "left", "finish", created_at)
                    .await?;
                if broadcast && !had_stream && !text.is_empty() {
                    self.broadcast_stream(
                        &binding.conversation_id,
                        item_id,
                        turn_id,
                        "content",
                        json!({"content": text}),
                        Some(("left", "finish")),
                    );
                }
            }
            "reasoning" if completed => {
                let text = item
                    .get("summary")
                    .and_then(Value::as_array)
                    .map(|parts| parts.iter().filter_map(Value::as_str).collect::<Vec<_>>().join("\n"))
                    .unwrap_or_default();
                self.thinking_buffers
                    .lock()
                    .await
                    .remove(&(binding.conversation_id.clone(), item_id.to_owned()));
                if !text.is_empty() {
                    self.persist_structured(
                        binding,
                        &format!("thinking-{item_id}"),
                        item_id,
                        "thinking",
                        json!({"content": text, "status": "done"}),
                        "left",
                        "finish",
                        created_at,
                    )
                    .await?;
                    if broadcast {
                        self.broadcast_stream(
                            &binding.conversation_id,
                            item_id,
                            turn_id,
                            "thinking",
                            json!({"content": "", "status": "done"}),
                            None,
                        );
                    }
                }
            }
            "plan" if completed => {
                let text = item.get("text").and_then(Value::as_str).unwrap_or_default();
                if !text.is_empty() {
                    self.persist_text(binding, item_id, text, "left", "finish", created_at)
                        .await?;
                    if broadcast {
                        self.broadcast_stream(
                            &binding.conversation_id,
                            item_id,
                            turn_id,
                            "content",
                            json!({"content": text}),
                            Some(("left", "finish")),
                        );
                    }
                }
            }
            "commandExecution" => {
                let key = (binding.conversation_id.clone(), item_id.to_owned());
                self.command_output_buffers.lock().await.remove(&key);
                let status = if completed {
                    if item.get("status").and_then(Value::as_str) == Some("failed") {
                        "error"
                    } else {
                        "completed"
                    }
                } else {
                    "running"
                };
                let data = json!({
                    "call_id": item_id,
                    "name": "shell",
                    "args": {
                        "command": item.get("command").cloned().unwrap_or(Value::Null),
                        "cwd": item.get("cwd").cloned().unwrap_or(Value::Null),
                    },
                    "status": status,
                    "output": item.get("aggregatedOutput").cloned().unwrap_or(Value::Null),
                    "description": item.get("command").cloned().unwrap_or(Value::Null),
                });
                self.persist_structured(
                    binding,
                    item_id,
                    item_id,
                    "tool_call",
                    data.clone(),
                    "left",
                    if completed { "finish" } else { "work" },
                    created_at,
                )
                .await?;
                if broadcast {
                    self.broadcast_stream(&binding.conversation_id, item_id, turn_id, "tool_call", data, None);
                }
            }
            "fileChange" => {
                let status = if completed {
                    if item.get("status").and_then(Value::as_str) == Some("failed") {
                        "error"
                    } else {
                        "completed"
                    }
                } else {
                    "running"
                };
                let data = json!({
                    "call_id": item_id,
                    "name": "apply_patch",
                    "args": item.get("changes").cloned().unwrap_or_else(|| json!([])),
                    "status": status,
                    "description": "File changes",
                });
                self.persist_structured(
                    binding,
                    item_id,
                    item_id,
                    "tool_call",
                    data.clone(),
                    "left",
                    if completed { "finish" } else { "work" },
                    created_at,
                )
                .await?;
                if broadcast {
                    self.broadcast_stream(&binding.conversation_id, item_id, turn_id, "tool_call", data, None);
                }
            }
            "mcpToolCall" => {
                let status = if completed { "completed" } else { "running" };
                let data = json!({
                    "call_id": item_id,
                    "name": item.get("tool").cloned().unwrap_or_else(|| json!("mcp")),
                    "args": item.get("arguments").cloned().unwrap_or_else(|| json!({})),
                    "status": status,
                    "description": item.get("server").cloned().unwrap_or(Value::Null),
                });
                self.persist_structured(
                    binding,
                    item_id,
                    item_id,
                    "tool_call",
                    data.clone(),
                    "left",
                    if completed { "finish" } else { "work" },
                    created_at,
                )
                .await?;
                if broadcast {
                    self.broadcast_stream(&binding.conversation_id, item_id, turn_id, "tool_call", data, None);
                }
            }
            _ => {}
        }
        Ok(())
    }

    async fn persist_text(
        &self,
        binding: &CodexThreadBindingRow,
        msg_id: &str,
        text: &str,
        position: &str,
        status: &str,
        created_at: i64,
    ) -> Result<(), ConversationError> {
        self.persist_structured(
            binding,
            msg_id,
            msg_id,
            "text",
            json!({"content": text}),
            position,
            status,
            created_at,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn persist_structured(
        &self,
        binding: &CodexThreadBindingRow,
        id: &str,
        msg_id: &str,
        message_type: &str,
        content: Value,
        position: &str,
        status: &str,
        created_at: i64,
    ) -> Result<(), ConversationError> {
        let row = aionui_db::models::MessageRow {
            id: id.to_owned(),
            conversation_id: binding.conversation_id.clone(),
            msg_id: Some(msg_id.to_owned()),
            r#type: message_type.to_owned(),
            content: content.to_string(),
            position: Some(position.to_owned()),
            status: Some(status.to_owned()),
            hidden: false,
            created_at,
        };
        if let Err(error) = self.conversation_repo.upsert_message(&row).await {
            warn!(
                conversation_id = %binding.conversation_id,
                thread_id = %binding.thread_id,
                item_id = id,
                error = %ErrorChain(&error),
                "Failed to persist projected Codex item"
            );
            return Err(error.into());
        }
        Ok(())
    }

    fn broadcast_stream(
        &self,
        conversation_id: &str,
        msg_id: &str,
        turn_id: &str,
        message_type: &str,
        data: Value,
        display: Option<(&str, &str)>,
    ) {
        let mut payload = json!({
            "conversation_id": conversation_id,
            "msg_id": msg_id,
            "turn_id": turn_id,
            "type": message_type,
            "data": data,
            "hidden": false,
            "created_at": now_ms(),
        });
        if let Some((position, status)) = display {
            payload["position"] = Value::String(position.into());
            payload["status"] = Value::String(status.into());
        }
        self.broadcaster
            .broadcast(WebSocketMessage::new("message.stream", payload));
    }
}

fn user_message_text(item: &Value) -> String {
    item.get("content")
        .and_then(Value::as_array)
        .map(|inputs| {
            inputs
                .iter()
                .filter_map(|input| match input.get("type").and_then(Value::as_str) {
                    Some("text") => input.get("text").and_then(Value::as_str).map(str::to_owned),
                    Some("localImage") => input
                        .get("path")
                        .and_then(Value::as_str)
                        .map(|path| format!("[Image: {path}]")),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}
