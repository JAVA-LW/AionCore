use super::*;

#[test]
fn control_socket_uses_codex_home() {
    assert_eq!(
        control_socket_path(Path::new("/tmp/codex-home")),
        PathBuf::from("/tmp/codex-home/app-server-control/app-server-control.sock")
    );
}

#[test]
fn turn_race_errors_are_classified_for_transparent_retry() {
    let error = CodexAppServerError::Rpc {
        code: -32600,
        message: "expected active turn id `a` but found `b`".into(),
        data: None,
    };
    assert!(error.is_turn_race());
}

#[cfg(unix)]
#[tokio::test]
async fn notification_updates_active_turn_snapshot() {
    let snapshot = Arc::new(RwLock::new(CodexAppServerSnapshot::default()));
    apply_notification(
        &snapshot,
        "turn/started",
        &json!({"threadId": "thread-1", "turn": {"id": "turn-1"}}),
    )
    .await;
    assert_eq!(
        snapshot
            .read()
            .await
            .runtimes
            .get("thread-1")
            .and_then(|runtime| runtime.active_turn_id.as_deref()),
        Some("turn-1")
    );
}

#[cfg(unix)]
#[tokio::test]
async fn unix_gateway_initializes_steers_starts_and_answers_approval() {
    use tokio::net::UnixListener;
    use tokio_tungstenite::accept_async;

    let codex_home = tempfile::tempdir().unwrap();
    let socket_path = control_socket_path(codex_home.path());
    std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();
    let listener = UnixListener::bind(&socket_path).unwrap();
    let expected_home = codex_home.path().to_string_lossy().into_owned();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();

        let initialize = next_json(&mut socket).await;
        assert_eq!(initialize.get("method").and_then(Value::as_str), Some("initialize"));
        send_json(
            &mut socket,
            json!({
                "id": 0,
                "result": {"userAgent": "mock-codex", "codexHome": expected_home}
            }),
        )
        .await;
        let initialized = next_json(&mut socket).await;
        assert_eq!(initialized.get("method").and_then(Value::as_str), Some("initialized"));

        let read = next_json(&mut socket).await;
        assert_eq!(read.get("method").and_then(Value::as_str), Some("thread/read"));
        send_json(
            &mut socket,
            json!({
                "id": read["id"],
                "result": {
                    "thread": {
                        "id": "thread-1",
                        "cwd": "/tmp/project",
                        "status": {"type": "active", "activeFlags": []},
                        "turns": [{"id": "turn-1", "status": "inProgress", "items": []}]
                    }
                }
            }),
        )
        .await;

        let steer = next_json(&mut socket).await;
        assert_eq!(steer.get("method").and_then(Value::as_str), Some("turn/steer"));
        assert_eq!(
            steer.pointer("/params/expectedTurnId").and_then(Value::as_str),
            Some("turn-1")
        );
        assert_eq!(
            steer.pointer("/params/clientUserMessageId").and_then(Value::as_str),
            Some("msg-steer")
        );
        send_json(&mut socket, json!({"id": steer["id"], "result": {"turnId": "turn-1"}})).await;

        send_json(
            &mut socket,
            json!({"method": "turn/completed", "params": {"threadId": "thread-1", "turn": {"id": "turn-1"}}}),
        )
        .await;

        let start = next_json(&mut socket).await;
        assert_eq!(start.get("method").and_then(Value::as_str), Some("turn/start"));
        assert_eq!(
            start.pointer("/params/clientUserMessageId").and_then(Value::as_str),
            Some("msg-start")
        );
        send_json(
            &mut socket,
            json!({"id": start["id"], "result": {"turn": {"id": "turn-2"}}}),
        )
        .await;

        send_json(
            &mut socket,
            json!({
                "id": "approval-1",
                "method": "item/commandExecution/requestApproval",
                "params": {"threadId": "thread-1", "turnId": "turn-2", "command": "cargo test"}
            }),
        )
        .await;
        let approval = next_json(&mut socket).await;
        assert_eq!(approval.get("id").and_then(Value::as_str), Some("approval-1"));
        assert_eq!(
            approval.pointer("/result/decision").and_then(Value::as_str),
            Some("accept")
        );
    });

    let gateway = CodexAppServerGateway::start(CodexAppServerConfig {
        codex_home: codex_home.path().to_path_buf(),
        codex_binary: PathBuf::from("unused-in-test"),
    });
    wait_until_connected(&gateway).await;
    gateway.read_thread("thread-1", true).await.unwrap();
    wait_for_active_turn(&gateway, "thread-1", Some("turn-1")).await;

    assert_eq!(
        gateway
            .send_input("thread-1", "msg-steer", "fix the test")
            .await
            .unwrap(),
        CodexSendReceipt {
            turn_id: "turn-1".into(),
            steered: true,
        }
    );
    wait_for_active_turn(&gateway, "thread-1", None).await;
    let mut events = gateway.subscribe();
    assert_eq!(
        gateway.send_input("thread-1", "msg-start", "continue").await.unwrap(),
        CodexSendReceipt {
            turn_id: "turn-2".into(),
            steered: false,
        }
    );

    let request = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(CodexAppServerEvent::ServerRequest(request)) = events.recv().await {
                break request;
            }
        }
    })
    .await
    .unwrap();
    gateway
        .respond_to_server_request(request.request_id, json!({"decision": "accept"}))
        .await
        .unwrap();

    server.await.unwrap();
}

#[cfg(unix)]
async fn next_json(socket: &mut WebSocketStream<UnixStream>) -> Value {
    loop {
        let message = socket.next().await.unwrap().unwrap();
        if let Message::Text(text) = message {
            return serde_json::from_str(&text).unwrap();
        }
    }
}

#[cfg(unix)]
async fn send_json(socket: &mut WebSocketStream<UnixStream>, value: Value) {
    socket.send(Message::Text(value.to_string().into())).await.unwrap();
}

#[cfg(unix)]
async fn wait_until_connected(gateway: &CodexAppServerGateway) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if gateway.snapshot().await.connected {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[cfg(unix)]
async fn wait_for_active_turn(gateway: &CodexAppServerGateway, thread_id: &str, expected: Option<&str>) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let current = gateway
                .snapshot()
                .await
                .runtimes
                .get(thread_id)
                .and_then(|runtime| runtime.active_turn_id.clone());
            if current.as_deref() == expected {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
