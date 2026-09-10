//! Exercise the actual operator command without signing keys, worker identity,
//! or a database. Each subprocess receives an isolated environment.
use serde_json::{json, Value};
use std::{process::Output, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
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
