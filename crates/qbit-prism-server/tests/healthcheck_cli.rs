//! Exercise the actual operator command without signing keys, worker identity,
//! or a database. Each subprocess receives an isolated environment.
use serde_json::{json, Value};
use std::{process::Output, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    process::Command,
    time::timeout,
};

fn command(port: u16, highdiff: bool) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_qbit-prism-server"));
    command.arg("healthcheck").kill_on_drop(true);
    for (name, _) in
        std::env::vars().filter(|(name, _)| name.starts_with("PRISM_") || name.starts_with("QBIT_"))
    {
        command.env_remove(name);
    }
    command
        .env("PRISM_RUNTIME_WORKERS", "2")
        .env("PRISM_AUDIT_PORT", "0")
        .env("PRISM_STRATUM_BIND", "0.0.0.0")
        .env(
            "PRISM_STRATUM_PORT",
            if highdiff {
                "0".into()
            } else {
                port.to_string()
            },
        );
    if highdiff {
        command
            .env("PRISM_STRATUM_HIGHDIFF_BIND", "127.0.0.1")
            .env("PRISM_STRATUM_HIGHDIFF_PORT", port.to_string());
    }
    command
}

async fn probe(reply: Vec<u8>, highdiff: bool) -> Output {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut reader = BufReader::new(socket);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        let request: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(
            request,
            json!({"id":1,"method":"mining.get_health","params":[]})
        );
        reader.get_mut().write_all(&reply).await.unwrap();
        reader.get_mut().shutdown().await.unwrap();
    });
    let output = timeout(Duration::from_secs(5), command(port, highdiff).output())
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
    output
}

#[tokio::test]
async fn audit_disabled_healthcheck_uses_only_readiness_on_primary_or_highdiff() {
    for highdiff in [false, true] {
        let output = probe(
            b"{\"id\":1,\"result\":{\"ready\":true},\"error\":null}\n".to_vec(),
            highdiff,
        )
        .await;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[tokio::test]
async fn invalid_or_unready_stratum_responses_fail_the_actual_health_command() {
    for reply in [
        b"{\"id\":1,\"result\":{\"ready\":false},\"error\":null}\n".to_vec(),
        b"{\"id\":2,\"result\":{\"ready\":true},\"error\":null}\n".to_vec(),
        b"{\"id\":1,\"result\":{\"ready\":true},\"error\":[20,\"failed\",null]}\n".to_vec(),
        b"not JSON\n".to_vec(),
        b"{\"id\":1,\"result\":{\"ready\":true},\"error\":null}".to_vec(),
        Vec::new(),
        vec![b'x'; 4097],
    ] {
        let output = probe(reply, false).await;
        assert!(
            !output.status.success(),
            "malformed/unready response passed healthcheck"
        );
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let output = timeout(Duration::from_secs(5), command(port, false).output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        !output.status.success(),
        "closed Stratum listener was healthy"
    );
}

/// The healthcheck subcommand against one canned `/healthz` answer.
async fn http_probe(status: &str, body: Value) -> Output {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let status = status.to_owned();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            let mut byte = [0u8; 1];
            socket.read_exact(&mut byte).await.unwrap();
            request.push(byte[0]);
        }
        assert!(request.starts_with(b"GET /healthz "));
        let body = body.to_string();
        let reply = format!(
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        socket.write_all(reply.as_bytes()).await.unwrap();
        socket.shutdown().await.unwrap();
    });
    let mut command = command(0, false);
    command
        .arg("--url")
        .arg(format!("http://127.0.0.1:{port}/healthz"));
    let output = timeout(Duration::from_secs(5), command.output())
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
    output
}

fn dual_writer_health(caught_up: bool, writer_path: &str) -> Value {
    json!({
        "ok": false,
        "status": if caught_up { "writer-not-local" } else { "own-log-behind" },
        "dual_writer": {"node_index": 1, "carry_owner": false, "own_log_caught_up": caught_up,
            "peer_sync": {"peer_reachable": false, "own_log_caught_up": caught_up, "per_table": {}},
            "writer_path": writer_path},
        "admission": {"admitting": false, "state": "starting", "reason": null, "grace_seconds": 10},
    })
}

/// 3.1 (the D5 cutover decision): a dual-writer frontend's healthcheck is
/// liveness, so one still catching up on its own log is healthy and one
/// writing to the wrong database is not; a single writer's is 3.0's.
#[tokio::test]
async fn a_dual_writer_body_is_held_to_liveness_and_a_single_writer_body_to_readiness() {
    let output = http_probe(
        "503 Service Unavailable",
        dual_writer_health(false, "local"),
    )
    .await;
    assert!(
        output.status.success(),
        "a catching-up dual-writer frontend failed its healthcheck: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = http_probe(
        "503 Service Unavailable",
        dual_writer_health(true, "remote"),
    )
    .await;
    assert!(!output.status.success(), "a remote writer passed");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("writer_path"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = http_probe(
        "503 Service Unavailable",
        json!({"ok": false, "status": "unavailable",
            "admission": {"admitting": true, "state": "grace"}}),
    )
    .await;
    assert!(!output.status.success(), "a not-ready single writer passed");
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("PRISM is unhealthy (HTTP 503 Service Unavailable)"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = http_probe("200 OK", json!({"ok": true, "status": "ok"})).await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// A dual-writer frontend's Stratum listeners refuse connections while it
/// does not admit miners, so its healthcheck never falls back to them.
#[tokio::test]
async fn a_dual_writer_healthcheck_without_the_operator_listener_refuses_to_probe_stratum() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let mut command = command(port, false);
    command.env("PRISM_DUAL_WRITER", "1");
    let output = timeout(Duration::from_secs(5), command.output())
        .await
        .unwrap()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("PRISM_AUDIT_PORT"), "{stderr}");
    assert!(
        timeout(Duration::from_millis(200), listener.accept())
            .await
            .is_err(),
        "the healthcheck dialled Stratum"
    );
}
