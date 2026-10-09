//! #759: qbitd closes a kept-alive connection once it has been idle for its
//! `-rpcservertimeout` (30 s by default). A request written into that close is
//! never read and fails after the connection existed, so `submitblock` would
//! record an unknown outcome and never offer the block again. The client
//! therefore drops a pooled connection before the node's idle close
//! ([`POOL_IDLE_TIMEOUT`]), and sends each relay call on a new connection. The
//! mock node answers a request on a reused connection only within its idle
//! limit, and drops a later one unanswered, as the race does. Each case runs
//! the real client against a real loopback socket.
use qbit_prism_server::rpc::{Rpc, RpcNotSentError, POOL_IDLE_TIMEOUT};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

/// The mock node's idle close.
const NODE_IDLE_LIMIT: Duration = Duration::from_millis(400);
/// How long a client waits between its two calls: past the node's limit.
const PAUSE: Duration = Duration::from_millis(700);

fn rpc(port: u16, pool_idle_timeout: Duration) -> Rpc {
    Rpc::with_pool_idle_timeout(
        format!("http://127.0.0.1:{port}/"),
        "user".into(),
        "password".into(),
        Duration::from_secs(5),
        pool_idle_timeout,
    )
    .unwrap()
}

/// Read one HTTP request, headers and `Content-Length` body, and return the
/// body, or `None` once the client has closed the connection.
async fn read_request(stream: &mut BufReader<TcpStream>) -> Option<Vec<u8>> {
    let mut length = 0;
    loop {
        let mut line = String::new();
        if stream.read_line(&mut line).await.ok()? == 0 {
            return None;
        }
        if let Some((name, value)) = line.trim_end().split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                length = value.trim().parse().ok()?;
            }
        }
        if line == "\r\n" {
            break;
        }
    }
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await.ok()?;
    Some(body)
}

/// Serve one connection: answer each request with the result 7 and the
/// request's id, until a request comes more than [`NODE_IDLE_LIMIT`] after the
/// previous answer. That one meets the node's idle close: it is read but never
/// answered, and the connection closes.
async fn serve(stream: TcpStream) {
    let mut stream = BufReader::new(stream);
    let mut answered: Option<Instant> = None;
    while let Some(body) = read_request(&mut stream).await {
        if answered.is_some_and(|at| at.elapsed() > NODE_IDLE_LIMIT) {
            return;
        }
        let request: Value = serde_json::from_slice(&body).unwrap();
        let reply = json!({"result": 7, "error": null, "id": request["id"]}).to_string();
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{reply}",
            reply.len()
        );
        if stream
            .get_mut()
            .write_all(response.as_bytes())
            .await
            .is_err()
        {
            return;
        }
        answered = Some(Instant::now());
    }
}

/// A node with [`NODE_IDLE_LIMIT`] as its idle close; returns its port and
/// the count of connections it has accepted.
async fn mock_node() -> (u16, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let connections = Arc::new(AtomicUsize::new(0));
    let accepted = connections.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            accepted.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(serve(stream));
        }
    });
    (port, connections)
}

#[test]
fn rpc_new_keeps_an_idle_connection_for_the_pool_idle_bound() {
    let rpc = Rpc::new(
        "http://127.0.0.1:1/".into(),
        "user".into(),
        "password".into(),
        Duration::from_secs(5),
    )
    .unwrap();
    assert_eq!(rpc.pool_idle_timeout(), POOL_IDLE_TIMEOUT);
}

/// With the bound below the node's idle close, the connection is dropped
/// before the node would close it. The next call opens a new connection, and
/// the node answers it.
#[tokio::test]
async fn a_call_after_the_nodes_idle_close_uses_a_new_connection() {
    let (port, connections) = mock_node().await;
    let rpc = rpc(port, NODE_IDLE_LIMIT / 2);
    assert_eq!(rpc.call("getblockcount", json!([])).await.unwrap(), 7);
    tokio::time::sleep(PAUSE).await;
    assert_eq!(rpc.call("getblockcount", json!([])).await.unwrap(), 7);
    assert_eq!(connections.load(Ordering::SeqCst), 2);
}

/// A relay call never reuses a pooled connection, whatever the bound: each
/// opens a new one, which the node answers even after it has closed every
/// idle connection the client kept.
#[tokio::test]
async fn a_relay_call_never_reuses_a_pooled_connection() {
    let (port, connections) = mock_node().await;
    let rpc = rpc(port, Duration::from_secs(60));
    assert_eq!(rpc.call("getblockcount", json!([])).await.unwrap(), 7);
    assert_eq!(rpc.call("submitblock", json!(["00"])).await.unwrap(), 7);
    tokio::time::sleep(PAUSE).await;
    assert_eq!(rpc.call("submitblock", json!(["00"])).await.unwrap(), 7);
    assert_eq!(connections.load(Ordering::SeqCst), 3);
}

/// The race the bound prevents. A client that keeps the idle connection past
/// the node's idle close reuses it, and its request meets the close. The
/// failure comes after the connection existed, so it is not
/// `RpcNotSentError`: on `submitblock` the outcome would be unknown.
#[tokio::test]
async fn a_connection_kept_past_the_nodes_idle_close_fails_the_call() {
    let (port, connections) = mock_node().await;
    let rpc = rpc(port, Duration::from_secs(60));
    assert_eq!(rpc.call("getblockcount", json!([])).await.unwrap(), 7);
    tokio::time::sleep(PAUSE).await;
    let error = rpc
        .call("getblockcount", json!([]))
        .await
        .expect_err("the reused connection must fail");
    assert!(
        error.downcast_ref::<RpcNotSentError>().is_none(),
        "{error:#}"
    );
    assert!(
        error
            .to_string()
            .contains("qbit RPC getblockcount transport failed"),
        "{error:#}"
    );
    assert_eq!(connections.load(Ordering::SeqCst), 1);
}
