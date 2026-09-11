//! Lazy subscription allocation through the production Stratum listener.
#[path = "support/stratum_admission.rs"]
mod support;
use qbit_prism_server::stratum::StratumConfig;
use serde_json::{json, Value};
use std::sync::{atomic::Ordering, Arc};
use support::{Backend, Client, Server};

fn subscribe(id: u64) -> Value {
    json!({"id":id,"method":"mining.subscribe","params":[]})
}
fn assert_backend_error(response: &Value) {
    assert!(response["result"].is_null());
    assert_eq!(response["error"][2]["reason_id"], "backend-rpc-unavailable");
}

#[tokio::test]
async fn connect_health_configure_and_authorize_without_subscribe_allocate_nothing() {
    let server = Server::start(StratumConfig::default(), Arc::new(Backend::default())).await;
    let silent = Client::connect(&server).await;
    server.connections(1).await;
    drop(silent);
    server.connections(0).await;
    assert_eq!(server.backend.allocation_calls.load(Ordering::SeqCst), 0);

    let mut client = Client::connect(&server).await;
    for request in [
        json!({"id":1,"method":"mining.get_health","params":[]}),
        json!({"id":2,"method":"mining.configure","params":[["version-rolling"],{}]}),
        json!({"id":3,"method":"mining.authorize","params":["original.worker","x"]}),
    ] {
        let response = client.request(request).await;
        assert!(response["error"].is_null(), "{response}");
    }
    assert_eq!(server.backend.authorize_calls.load(Ordering::SeqCst), 1);
    assert_eq!(server.backend.allocation_calls.load(Ordering::SeqCst), 0);
    assert_eq!(server.backend.build_calls.load(Ordering::SeqCst), 0);
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn malformed_subscribe_does_not_allocate_then_pipelined_subscribes_reuse_identity() {
    let server = Server::start(StratumConfig::default(), Arc::new(Backend::default())).await;
    let mut client = Client::connect(&server).await;
    let invalid = client
        .request(json!({"id":1,"method":"mining.subscribe","params":{}}))
        .await;
    assert_eq!(invalid["error"][2]["reason_id"], "malformed-submit");
    assert_eq!(server.backend.allocation_calls.load(Ordering::SeqCst), 0);
    client.send(subscribe(2)).await;
    client.send(subscribe(3)).await;
    let first = client.read().await;
    let second = client.read().await;
    assert_eq!(first["id"], 2);
    assert_eq!(second["id"], 3);
    assert_eq!(first["result"], json!([[], "00000001", 8]));
    assert_eq!(second["result"], first["result"]);
    assert_eq!(server.backend.allocation_calls.load(Ordering::SeqCst), 1);
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn failed_allocation_leaves_authorized_session_unsubscribed_and_retryable() {
    let backend = Arc::new(Backend::default());
    backend.fail_once.store(true, Ordering::SeqCst);
    let server = Server::start(StratumConfig::default(), backend).await;
    let mut client = Client::connect(&server).await;
    let authorized = client
        .request(json!({"id":1,"method":"mining.authorize","params":["original.worker","x"]}))
        .await;
    assert_eq!(authorized["result"], true);
    assert_backend_error(&client.request(subscribe(2)).await);
    let response = client
        .request(json!({"id":3,"method":"mining.get_health","params":[]}))
        .await;
    assert!(response["error"].is_null());
    assert_eq!(server.backend.build_calls.load(Ordering::SeqCst), 0);
    let accepted = client.request(subscribe(4)).await;
    assert_eq!(accepted["result"], json!([[], "00000001", 8]));
    let repeat = client.request(subscribe(5)).await;
    assert_eq!(repeat["result"], accepted["result"]);
    assert_eq!(server.backend.allocation_calls.load(Ordering::SeqCst), 2);
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn timed_out_consumed_allocation_returns_error_then_reconnect_uses_fresh_id() {
    let backend = Arc::new(Backend::default());
    backend.stall_once.store(true, Ordering::SeqCst);
    let server = Server::start(
        StratumConfig {
            initial_job_timeout_seconds: 0.1,
            ..Default::default()
        },
        backend,
    )
    .await;
    let mut client = Client::connect(&server).await;
    client.send(subscribe(1)).await;
    server.backend.allocation_started.notified().await;
    let failed = client.read().await;
    assert_backend_error(&failed);
    assert_eq!(failed["error"][1], "session allocation timed out");
    assert_eq!(server.backend.ids.load(Ordering::SeqCst), 1);
    server.backend.allocation_release.notify_one();
    // Allocation consumed the initial-job lifetime. Preserve its existing
    // timer cutoff; recovery uses a new connection rather than extending it.
    server.connections(0).await;
    drop(client);
    let mut client = Client::connect(&server).await;
    let accepted = client.request(subscribe(2)).await;
    assert_eq!(accepted["result"], json!([[], "00000002", 8]));
    assert_eq!(
        client.request(subscribe(3)).await["result"],
        accepted["result"]
    );
    assert_eq!(server.backend.allocation_calls.load(Ordering::SeqCst), 2);
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn reset_connection_errors_are_structured_tracing_events() {
    use std::{io::Write, sync::Mutex};
    #[derive(Clone)]
    struct Capture(Arc<Mutex<Vec<u8>>>);
    impl Write for Capture {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
        type Writer = Self;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }
    let captured = Arc::new(Mutex::new(Vec::new()));
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .json()
            .with_writer(Capture(captured.clone()))
            .finish(),
    )
    .unwrap();
    let server = Server::start(StratumConfig::default(), Arc::new(Backend::default())).await;
    let mut client = Client::connect(&server).await;
    // A completed request proves the listener is reading this established socket.
    client
        .request(json!({"id":1,"method":"mining.get_health","params":[]}))
        .await;
    client.0.get_ref().set_zero_linger().unwrap();
    drop(client);
    server.connections(0).await;
    server.stop().await;
    let output = String::from_utf8(captured.lock().unwrap().clone()).unwrap();
    let event = output
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|value| value["fields"]["message"] == "Stratum connection ended")
        .expect("reset connection was not reported through tracing");
    assert!(event["fields"]["error"]
        .as_str()
        .is_some_and(|error| !error.is_empty()));
}
