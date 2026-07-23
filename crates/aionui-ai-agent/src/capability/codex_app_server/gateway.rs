use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::sync::{RwLock, broadcast, mpsc, oneshot};

#[cfg(unix)]
use futures_util::{SinkExt, StreamExt};
#[cfg(unix)]
use std::collections::HashMap;
#[cfg(unix)]
use std::process::Stdio;
#[cfg(unix)]
use tokio::net::UnixStream;
#[cfg(unix)]
use tokio::process::Child;
#[cfg(unix)]
use tokio_tungstenite::WebSocketStream;
#[cfg(unix)]
use tokio_tungstenite::tungstenite::Message;
#[cfg(unix)]
use tracing::{info, warn};

use super::protocol::{
    CodexAppServerError, CodexAppServerEvent, CodexAppServerSnapshot, CodexPendingRequest, CodexSendReceipt,
    CodexThreadRuntime, ICodexAppServerGateway,
};

const RPC_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(unix)]
const RECONNECT_DELAY: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
pub struct CodexAppServerConfig {
    pub codex_home: PathBuf,
    pub codex_binary: PathBuf,
}

impl Default for CodexAppServerConfig {
    fn default() -> Self {
        Self {
            codex_home: default_codex_home(),
            codex_binary: PathBuf::from("codex"),
        }
    }
}

enum GatewayCommand {
    Rpc {
        method: String,
        params: Value,
        response: oneshot::Sender<Result<Value, CodexAppServerError>>,
    },
    Respond {
        request_id: Value,
        result: Value,
        response: oneshot::Sender<Result<(), CodexAppServerError>>,
    },
}

pub struct CodexAppServerGateway {
    command_tx: mpsc::Sender<GatewayCommand>,
    event_tx: broadcast::Sender<CodexAppServerEvent>,
    snapshot: Arc<RwLock<CodexAppServerSnapshot>>,
}

impl CodexAppServerGateway {
    pub fn start(config: CodexAppServerConfig) -> Arc<Self> {
        let (command_tx, command_rx) = mpsc::channel(256);
        let (event_tx, _) = broadcast::channel(1024);
        let snapshot = Arc::new(RwLock::new(CodexAppServerSnapshot::default()));
        let gateway = Arc::new(Self {
            command_tx,
            event_tx: event_tx.clone(),
            snapshot: snapshot.clone(),
        });

        #[cfg(unix)]
        tokio::spawn(run_supervisor(config, command_rx, event_tx, snapshot));
        #[cfg(not(unix))]
        tokio::spawn(run_unsupported(command_rx, event_tx));
        gateway
    }

    async fn rpc(&self, method: &str, params: Value) -> Result<Value, CodexAppServerError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(GatewayCommand::Rpc {
                method: method.to_owned(),
                params,
                response: response_tx,
            })
            .await
            .map_err(|_| CodexAppServerError::Unavailable("gateway task stopped".into()))?;

        tokio::time::timeout(RPC_TIMEOUT, response_rx)
            .await
            .map_err(|_| CodexAppServerError::Timeout)?
            .map_err(|_| CodexAppServerError::Unavailable("gateway response channel closed".into()))?
    }

    async fn active_turn_id(&self, thread_id: &str) -> Option<String> {
        self.snapshot
            .read()
            .await
            .runtimes
            .get(thread_id)
            .and_then(|runtime| runtime.active_turn_id.clone())
    }

    async fn turn_start(
        &self,
        thread_id: &str,
        client_message_id: &str,
        text: &str,
    ) -> Result<CodexSendReceipt, CodexAppServerError> {
        let response = self
            .rpc(
                "turn/start",
                json!({
                    "threadId": thread_id,
                    "clientUserMessageId": client_message_id,
                    "input": [{"type": "text", "text": text}],
                }),
            )
            .await?;
        let turn_id = response
            .pointer("/turn/id")
            .and_then(Value::as_str)
            .ok_or_else(|| CodexAppServerError::InvalidResponse("turn/start omitted turn.id".into()))?
            .to_owned();
        self.snapshot.write().await.runtimes.insert(
            thread_id.to_owned(),
            CodexThreadRuntime {
                active_turn_id: Some(turn_id.clone()),
                status: "active".into(),
            },
        );
        Ok(CodexSendReceipt {
            turn_id,
            steered: false,
        })
    }
}

#[async_trait]
impl ICodexAppServerGateway for CodexAppServerGateway {
    fn subscribe(&self) -> broadcast::Receiver<CodexAppServerEvent> {
        self.event_tx.subscribe()
    }

    async fn snapshot(&self) -> CodexAppServerSnapshot {
        self.snapshot.read().await.clone()
    }

    async fn list_threads(&self, archived: bool) -> Result<Vec<Value>, CodexAppServerError> {
        let mut cursor: Option<String> = None;
        let mut threads = Vec::new();
        loop {
            let response = self
                .rpc(
                    "thread/list",
                    json!({
                        "cursor": cursor,
                        "limit": 100,
                        "sortKey": "updated_at",
                        "sortDirection": "desc",
                        "sourceKinds": ["cli", "vscode", "exec", "appServer", "subAgent", "subAgentReview", "subAgentCompact", "subAgentThreadSpawn", "subAgentOther", "unknown"],
                        "archived": archived,
                    }),
                )
                .await?;
            let data = response
                .get("data")
                .and_then(Value::as_array)
                .ok_or_else(|| CodexAppServerError::InvalidResponse("thread/list omitted data".into()))?;
            threads.extend(data.iter().cloned());
            cursor = response.get("nextCursor").and_then(Value::as_str).map(str::to_owned);
            if cursor.is_none() {
                break;
            }
        }
        Ok(threads)
    }

    async fn list_loaded_threads(&self) -> Result<Vec<String>, CodexAppServerError> {
        let response = self.rpc("thread/loaded/list", json!({})).await?;
        Ok(response
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| CodexAppServerError::InvalidResponse("thread/loaded/list omitted data".into()))?
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect())
    }

    async fn read_thread(&self, thread_id: &str, include_turns: bool) -> Result<Value, CodexAppServerError> {
        let response = self
            .rpc(
                "thread/read",
                json!({"threadId": thread_id, "includeTurns": include_turns}),
            )
            .await?;
        let thread = response
            .get("thread")
            .cloned()
            .ok_or_else(|| CodexAppServerError::InvalidResponse("thread/read omitted thread".into()))?;
        self.record_thread_snapshot(&thread).await;
        Ok(thread)
    }

    async fn resume_thread(&self, thread_id: &str) -> Result<Value, CodexAppServerError> {
        let response = self.rpc("thread/resume", json!({"threadId": thread_id})).await?;
        let thread = response
            .get("thread")
            .cloned()
            .ok_or_else(|| CodexAppServerError::InvalidResponse("thread/resume omitted thread".into()))?;
        self.record_thread_snapshot(&thread).await;
        Ok(thread)
    }

    async fn start_thread(&self, cwd: &str) -> Result<Value, CodexAppServerError> {
        let response = self.rpc("thread/start", json!({"cwd": cwd})).await?;
        let thread = response
            .get("thread")
            .cloned()
            .ok_or_else(|| CodexAppServerError::InvalidResponse("thread/start omitted thread".into()))?;
        self.record_thread_snapshot(&thread).await;
        Ok(thread)
    }

    async fn send_input(
        &self,
        thread_id: &str,
        client_message_id: &str,
        text: &str,
    ) -> Result<CodexSendReceipt, CodexAppServerError> {
        let Some(active_turn_id) = self.active_turn_id(thread_id).await else {
            return self.turn_start(thread_id, client_message_id, text).await;
        };

        let steer = self
            .rpc(
                "turn/steer",
                json!({
                    "threadId": thread_id,
                    "clientUserMessageId": client_message_id,
                    "input": [{"type": "text", "text": text}],
                    "expectedTurnId": active_turn_id,
                }),
            )
            .await;
        match steer {
            Ok(response) => {
                let turn_id = response
                    .get("turnId")
                    .and_then(Value::as_str)
                    .ok_or_else(|| CodexAppServerError::InvalidResponse("turn/steer omitted turnId".into()))?
                    .to_owned();
                Ok(CodexSendReceipt { turn_id, steered: true })
            }
            Err(error) if error.is_active_turn_not_steerable() => Err(CodexAppServerError::ActiveTurnNotSteerable),
            Err(error) if error.is_turn_race() => self.turn_start(thread_id, client_message_id, text).await,
            Err(error) => Err(error),
        }
    }

    async fn interrupt(&self, thread_id: &str, turn_id: &str) -> Result<(), CodexAppServerError> {
        self.rpc("turn/interrupt", json!({"threadId": thread_id, "turnId": turn_id}))
            .await?;
        Ok(())
    }

    async fn respond_to_server_request(&self, request_id: Value, result: Value) -> Result<(), CodexAppServerError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(GatewayCommand::Respond {
                request_id,
                result,
                response: response_tx,
            })
            .await
            .map_err(|_| CodexAppServerError::Unavailable("gateway task stopped".into()))?;
        tokio::time::timeout(RPC_TIMEOUT, response_rx)
            .await
            .map_err(|_| CodexAppServerError::Timeout)?
            .map_err(|_| CodexAppServerError::Unavailable("gateway response channel closed".into()))?
    }
}

impl CodexAppServerGateway {
    async fn record_thread_snapshot(&self, thread: &Value) {
        let Some(thread_id) = thread.get("id").and_then(Value::as_str) else {
            return;
        };
        let status = thread_status_name(thread.get("status"));
        let reported_active_turn = thread
            .get("turns")
            .and_then(Value::as_array)
            .and_then(|turns| {
                turns
                    .iter()
                    .rev()
                    .find(|turn| turn.get("status").and_then(Value::as_str) == Some("inProgress"))
            })
            .and_then(|turn| turn.get("id"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut snapshot = self.snapshot.write().await;
        let active_turn_id = if status == "active" {
            reported_active_turn.or_else(|| {
                snapshot
                    .runtimes
                    .get(thread_id)
                    .and_then(|runtime| runtime.active_turn_id.clone())
            })
        } else {
            None
        };
        snapshot.threads.insert(thread_id.to_owned(), thread.clone());
        snapshot
            .runtimes
            .insert(thread_id.to_owned(), CodexThreadRuntime { active_turn_id, status });
    }
}

#[cfg(not(unix))]
async fn run_unsupported(
    mut command_rx: mpsc::Receiver<GatewayCommand>,
    event_tx: broadcast::Sender<CodexAppServerEvent>,
) {
    let message = "native Codex daemon control requires a Unix platform".to_owned();
    let _ = event_tx.send(CodexAppServerEvent::Disconnected {
        reason: message.clone(),
    });
    while let Some(command) = command_rx.recv().await {
        let error = CodexAppServerError::Unavailable(message.clone());
        match command {
            GatewayCommand::Rpc { response, .. } => {
                let _ = response.send(Err(error));
            }
            GatewayCommand::Respond { response, .. } => {
                let _ = response.send(Err(error));
            }
        }
    }
}

#[cfg(unix)]
struct ConnectedSocket {
    socket: WebSocketStream<UnixStream>,
    _child: Option<Child>,
}

#[cfg(unix)]
type PendingRpc = oneshot::Sender<Result<Value, CodexAppServerError>>;

#[cfg(unix)]
async fn run_supervisor(
    config: CodexAppServerConfig,
    mut command_rx: mpsc::Receiver<GatewayCommand>,
    event_tx: broadcast::Sender<CodexAppServerEvent>,
    snapshot: Arc<RwLock<CodexAppServerSnapshot>>,
) {
    loop {
        match connect_or_launch(&config).await {
            Ok(mut connection) => match initialize(&mut connection.socket).await {
                Ok((user_agent, codex_home)) => {
                    {
                        let mut state = snapshot.write().await;
                        state.connected = true;
                        state.codex_home = Some(codex_home.clone());
                        state.user_agent = Some(user_agent.clone());
                    }
                    info!(codex_home = %codex_home.display(), user_agent, "Codex app-server connected");
                    let _ = event_tx.send(CodexAppServerEvent::Connected { codex_home, user_agent });
                    if let Err(error) =
                        run_connection(&mut connection.socket, &mut command_rx, &event_tx, &snapshot).await
                    {
                        warn!(error = %error, "Codex app-server connection ended");
                    }
                }
                Err(error) => warn!(error = %error, "Codex app-server initialize failed"),
            },
            Err(error) => warn!(error = %error, "Codex app-server connection failed"),
        }

        {
            let mut state = snapshot.write().await;
            state.connected = false;
            state.runtimes.clear();
        }
        let _ = event_tx.send(CodexAppServerEvent::Disconnected {
            reason: "connection lost".into(),
        });
        tokio::time::sleep(RECONNECT_DELAY).await;
    }
}

#[cfg(unix)]
async fn run_connection(
    socket: &mut WebSocketStream<UnixStream>,
    command_rx: &mut mpsc::Receiver<GatewayCommand>,
    event_tx: &broadcast::Sender<CodexAppServerEvent>,
    snapshot: &Arc<RwLock<CodexAppServerSnapshot>>,
) -> Result<(), CodexAppServerError> {
    let mut next_id = 1_i64;
    let mut pending: HashMap<i64, PendingRpc> = HashMap::new();

    loop {
        tokio::select! {
            command = command_rx.recv() => {
                let Some(command) = command else {
                    return Err(CodexAppServerError::Unavailable("gateway command channel closed".into()));
                };
                match command {
                    GatewayCommand::Rpc { method, params, response } => {
                        let id = next_id;
                        next_id += 1;
                        let payload = json!({"id": id, "method": method, "params": params});
                        socket.send(Message::Text(payload.to_string().into())).await
                            .map_err(|error| CodexAppServerError::Unavailable(error.to_string()))?;
                        pending.insert(id, response);
                    }
                    GatewayCommand::Respond { request_id, result, response } => {
                        let key = request_key(&request_id);
                        let payload = json!({"id": request_id, "result": result});
                        let outcome = socket.send(Message::Text(payload.to_string().into())).await
                            .map_err(|error| CodexAppServerError::Unavailable(error.to_string()));
                        if outcome.is_ok() {
                            snapshot.write().await.pending_requests.remove(&key);
                        }
                        let _ = response.send(outcome.map(|_| ()));
                    }
                }
            }
            incoming = socket.next() => {
                let Some(incoming) = incoming else {
                    return Err(CodexAppServerError::Unavailable("websocket stream closed".into()));
                };
                let message = incoming.map_err(|error| CodexAppServerError::Unavailable(error.to_string()))?;
                let Message::Text(text) = message else {
                    continue;
                };
                let envelope: Value = serde_json::from_str(&text)
                    .map_err(|error| CodexAppServerError::InvalidResponse(error.to_string()))?;
                if let Some(id) = envelope.get("id").and_then(Value::as_i64)
                    && (envelope.get("result").is_some() || envelope.get("error").is_some())
                {
                    if let Some(response) = pending.remove(&id) {
                        let _ = response.send(parse_rpc_response(envelope));
                    }
                    continue;
                }
                if envelope.get("id").is_some() && envelope.get("method").is_some() {
                    let request = CodexPendingRequest {
                        request_id: envelope.get("id").cloned().unwrap_or(Value::Null),
                        method: envelope.get("method").and_then(Value::as_str).unwrap_or_default().to_owned(),
                        params: envelope.get("params").cloned().unwrap_or_else(|| json!({})),
                    };
                    snapshot.write().await.pending_requests.insert(request_key(&request.request_id), request.clone());
                    let _ = event_tx.send(CodexAppServerEvent::ServerRequest(request));
                    continue;
                }
                if let Some(method) = envelope.get("method").and_then(Value::as_str) {
                    let params = envelope.get("params").cloned().unwrap_or_else(|| json!({}));
                    apply_notification(snapshot, method, &params).await;
                    let _ = event_tx.send(CodexAppServerEvent::Notification {
                        method: method.to_owned(),
                        params,
                    });
                }
            }
        }
    }
}

#[cfg(unix)]
async fn initialize(socket: &mut WebSocketStream<UnixStream>) -> Result<(String, PathBuf), CodexAppServerError> {
    socket
        .send(Message::Text(
            json!({
                "id": 0,
                "method": "initialize",
                "params": {
                    "clientInfo": {
                        "name": "aionui",
                        "title": "AionUI",
                        "version": env!("CARGO_PKG_VERSION"),
                    },
                    "capabilities": {
                        "experimentalApi": true,
                        "mcpServerOpenaiFormElicitation": false,
                    },
                },
            })
            .to_string()
            .into(),
        ))
        .await
        .map_err(|error| CodexAppServerError::Unavailable(error.to_string()))?;

    let response = loop {
        let incoming = socket
            .next()
            .await
            .ok_or_else(|| CodexAppServerError::Unavailable("initialize stream closed".into()))?
            .map_err(|error| CodexAppServerError::Unavailable(error.to_string()))?;
        let Message::Text(text) = incoming else {
            continue;
        };
        let value: Value =
            serde_json::from_str(&text).map_err(|error| CodexAppServerError::InvalidResponse(error.to_string()))?;
        if value.get("id").and_then(Value::as_i64) == Some(0) {
            break parse_rpc_response(value)?;
        }
    };

    socket
        .send(Message::Text(json!({"method": "initialized"}).to_string().into()))
        .await
        .map_err(|error| CodexAppServerError::Unavailable(error.to_string()))?;

    let user_agent = response
        .get("userAgent")
        .and_then(Value::as_str)
        .ok_or_else(|| CodexAppServerError::InvalidResponse("initialize omitted userAgent".into()))?
        .to_owned();
    let codex_home = response
        .get("codexHome")
        .and_then(Value::as_str)
        .ok_or_else(|| CodexAppServerError::InvalidResponse("initialize omitted codexHome".into()))?;
    Ok((user_agent, PathBuf::from(codex_home)))
}

#[cfg(unix)]
async fn connect_or_launch(config: &CodexAppServerConfig) -> Result<ConnectedSocket, CodexAppServerError> {
    let socket_path = control_socket_path(&config.codex_home);
    if let Ok(socket) = connect_socket(&socket_path).await {
        return Ok(ConnectedSocket { socket, _child: None });
    }

    std::fs::create_dir_all(socket_path.parent().unwrap_or(&config.codex_home))
        .map_err(|error| CodexAppServerError::Unavailable(error.to_string()))?;
    let mut builder = aionui_runtime::Builder::new(&config.codex_binary);
    builder
        .args(["app-server", "--listen", "unix://"])
        .env("CODEX_HOME", &config.codex_home)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let child = builder
        .spawn()
        .map_err(|error| CodexAppServerError::Unavailable(format!("failed to launch codex app-server: {error}")))?;

    for _ in 0..50 {
        if let Ok(socket) = connect_socket(&socket_path).await {
            return Ok(ConnectedSocket {
                socket,
                _child: Some(child),
            });
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(CodexAppServerError::Unavailable(format!(
        "timed out waiting for {}",
        socket_path.display()
    )))
}

#[cfg(unix)]
async fn connect_socket(path: &Path) -> Result<WebSocketStream<UnixStream>, CodexAppServerError> {
    let stream = UnixStream::connect(path)
        .await
        .map_err(|error| CodexAppServerError::Unavailable(error.to_string()))?;
    let (socket, _) = tokio_tungstenite::client_async("ws://localhost/", stream)
        .await
        .map_err(|error| CodexAppServerError::Unavailable(error.to_string()))?;
    Ok(socket)
}

#[cfg(unix)]
async fn apply_notification(snapshot: &Arc<RwLock<CodexAppServerSnapshot>>, method: &str, params: &Value) {
    let mut state = snapshot.write().await;
    match method {
        "thread/started" => {
            if let Some(thread) = params.get("thread")
                && let Some(thread_id) = thread.get("id").and_then(Value::as_str)
            {
                state.threads.insert(thread_id.to_owned(), thread.clone());
                state
                    .runtimes
                    .entry(thread_id.to_owned())
                    .or_insert(CodexThreadRuntime {
                        active_turn_id: None,
                        status: thread_status_name(thread.get("status")),
                    });
            }
        }
        "thread/status/changed" => {
            if let Some(thread_id) = params.get("threadId").and_then(Value::as_str) {
                state
                    .runtimes
                    .entry(thread_id.to_owned())
                    .or_insert(CodexThreadRuntime {
                        active_turn_id: None,
                        status: "unknown".into(),
                    })
                    .status = thread_status_name(params.get("status"));
            }
        }
        "turn/started" => {
            if let (Some(thread_id), Some(turn_id)) = (
                params.get("threadId").and_then(Value::as_str),
                params.pointer("/turn/id").and_then(Value::as_str),
            ) {
                state.runtimes.insert(
                    thread_id.to_owned(),
                    CodexThreadRuntime {
                        active_turn_id: Some(turn_id.to_owned()),
                        status: "active".into(),
                    },
                );
            }
        }
        "turn/completed" => {
            if let Some(thread_id) = params.get("threadId").and_then(Value::as_str) {
                state.runtimes.insert(
                    thread_id.to_owned(),
                    CodexThreadRuntime {
                        active_turn_id: None,
                        status: "idle".into(),
                    },
                );
            }
        }
        "serverRequest/resolved" => {
            if let Some(request_id) = params.get("requestId") {
                state.pending_requests.remove(&request_key(request_id));
            }
        }
        _ => {}
    }
}

fn parse_rpc_response(envelope: Value) -> Result<Value, CodexAppServerError> {
    if let Some(error) = envelope.get("error") {
        return Err(CodexAppServerError::Rpc {
            code: error.get("code").and_then(Value::as_i64).unwrap_or(-32_000),
            message: error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown Codex app-server error")
                .to_owned(),
            data: error.get("data").cloned(),
        });
    }
    envelope
        .get("result")
        .cloned()
        .ok_or_else(|| CodexAppServerError::InvalidResponse("RPC response omitted result".into()))
}

fn thread_status_name(status: Option<&Value>) -> String {
    status
        .and_then(|value| value.get("type"))
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_owned()
}

fn request_key(id: &Value) -> String {
    serde_json::to_string(id).unwrap_or_else(|_| "null".into())
}

fn control_socket_path(codex_home: &Path) -> PathBuf {
    codex_home.join("app-server-control").join("app-server-control.sock")
}

fn default_codex_home() -> PathBuf {
    std::env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".codex")))
        .unwrap_or_else(|| PathBuf::from(".codex"))
}

#[cfg(test)]
#[path = "gateway_tests.rs"]
mod tests;
