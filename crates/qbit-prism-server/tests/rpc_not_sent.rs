//! #522: which `submitblock` transport failures prove that the request never
//! reached the node. Only a call whose connection was never established is
//! [`RpcNotSentError`], one whose connect outlasted half its deadline included
//! (#762); every failure after the connection existed, whether or not the
//! peer read the request, stays an ordinary error that the offer path records
//! as an unknown outcome. Each case runs the real client against a real
//! socket on the loopback interface, so a change in reqwest's or hyper's
//! error kinds fails here rather than in production.
use qbit_prism_server::rpc::{Rpc, RpcNotSentError, RpcRelayRefused, RpcReplyError, RELAY_METHODS};
use serde_json::json;
use std::time::{Duration, Instant};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpSocket, TcpStream},
};

/// The deadline [`submit`] gives its call.
const SUBMIT_DEADLINE: Duration = Duration::from_millis(500);

fn rpc(port: u16) -> Rpc {
    Rpc::new(
        format!("http://127.0.0.1:{port}/"),
        "user".into(),
        "password".into(),
        Duration::from_secs(5),
    )
    .unwrap()
}

async fn submit(rpc: &Rpc) -> anyhow::Error {
    rpc.call_timeout("submitblock", json!(["00"]), Some(SUBMIT_DEADLINE))
        .await
        .expect_err("the call must fail")
}

fn not_sent(error: &anyhow::Error) -> Option<&RpcNotSentError> {
    error.downcast_ref::<RpcNotSentError>()
}

/// A port with nothing listening on it.
fn closed_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Read one HTTP request, headers and `Content-Length` body, and return how
/// many bytes it had.
async fn read_request(stream: &mut BufReader<TcpStream>) -> usize {
    read_call(stream).await.0
}

/// [`read_request`], and the JSON-RPC call its body holds.
async fn read_call(stream: &mut BufReader<TcpStream>) -> (usize, serde_json::Value) {
    let mut length = 0;
    let mut total = 0;
    loop {
        let mut line = String::new();
        let read = stream.read_line(&mut line).await.unwrap();
        assert!(read > 0, "the client closed before its request ended");
        total += read;
        if let Some((name, value)) = line.trim_end().split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                length = value.trim().parse().unwrap();
            }
        }
        if line == "\r\n" {
            break;
        }
    }
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await.unwrap();
    (total + length, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
async fn a_refused_connection_is_not_sent() {
    let error = submit(&rpc(closed_port())).await;
    let not_sent =
        not_sent(&error).unwrap_or_else(|| panic!("refused was not proven unsent: {error:#}"));
    assert_eq!(not_sent.method, "submitblock");
    assert_eq!(not_sent.cause, "connection refused");
    // Callers that match the transport wording keep matching it.
    assert!(
        error
            .to_string()
            .contains("qbit RPC submitblock transport failed"),
        "{error}"
    );
}

/// The node answered one call on a kept-alive connection and then went
/// away: the pooled connection is closed and the listener is gone. The next
/// call needs a new connection, which is refused, so it is still not sent.
#[tokio::test]
async fn a_refusal_after_the_pooled_connection_closed_is_not_sent() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let rpc = rpc(port);
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = BufReader::new(stream);
        read_request(&mut stream).await;
        let body = r#"{"result":7,"error":null,"id":1}"#;
        stream
            .get_mut()
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        // The listener and the connection both close here.
    });
    assert_eq!(rpc.call("getblockcount", json!([])).await.unwrap(), 7);
    server.await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let error = submit(&rpc).await;
    assert!(not_sent(&error).is_some(), "{error:#}");
}

/// The peer read the whole request and reset the connection without a reply:
/// the node may have acted on it, so the outcome is unknown.
#[tokio::test]
async fn a_reset_after_the_request_was_read_may_have_been_sent() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = BufReader::new(stream);
        let read = read_request(&mut stream).await;
        let stream = stream.into_inner();
        stream.set_zero_linger().unwrap();
        drop(stream);
        read
    });
    let error = submit(&rpc(port)).await;
    assert!(server.await.unwrap() > 0, "the peer never read the request");
    assert!(
        not_sent(&error).is_none(),
        "a reset after the write was treated as unsent: {error:#}"
    );
    assert_eq!(error.to_string(), "qbit RPC submitblock transport failed");
}

/// The connection was established and closed at once. Whether the peer's
/// kernel took the request bytes is not knowable, so it is not proven unsent.
#[tokio::test]
async fn a_connection_closed_right_after_it_was_accepted_may_have_been_sent() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        drop(stream);
    });
    let error = submit(&rpc(port)).await;
    server.await.unwrap();
    assert!(not_sent(&error).is_none(), "{error:#}");
}

/// The peer read the request and never answered: the request deadline
/// expires with the outcome unknown.
#[tokio::test]
async fn a_timeout_after_the_request_was_read_may_have_been_sent() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (read_tx, read_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = BufReader::new(stream);
        let _ = read_tx.send(read_request(&mut stream).await);
        tokio::time::sleep(Duration::from_secs(5)).await;
    });
    let error = submit(&rpc(port)).await;
    assert!(read_rx.await.unwrap() > 0);
    server.abort();
    assert!(not_sent(&error).is_none(), "{error:#}");
}

/// A peer that accepts every connection and reads what the client sends, but
/// never answers: an `https` client's TLS handshake, which is part of its
/// connect, never completes. Each connection's bytes arrive on the receiver
/// once the client has closed it.
async fn handshake_stall() -> (u16, tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (sent_tx, sent_rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let sent_tx = sent_tx.clone();
            tokio::spawn(async move {
                let mut sent = Vec::new();
                let _ = stream.read_to_end(&mut sent).await;
                let _ = sent_tx.send(sent);
            });
        }
    });
    (port, sent_rx)
}

/// #762: the node accepts the TCP connection and never completes the TLS
/// handshake, so the connect outlasts the call's deadline. It fails at half
/// the deadline as a connect error, and the call is proven unsent within its
/// deadline; the peer saw only the start of a handshake. Before #762 the
/// request timeout fired during the connect, the outcome was unknown, and the
/// block was never offered again. A read keeps the plain request timeout.
#[tokio::test]
async fn a_submitblock_whose_connect_outlasts_its_deadline_is_not_sent() {
    let (port, mut sent) = handshake_stall().await;
    let rpc = Rpc::new(
        format!("https://127.0.0.1:{port}/"),
        "user".into(),
        "password".into(),
        Duration::from_secs(5),
    )
    .unwrap();
    let started = Instant::now();
    let error = submit(&rpc).await;
    let elapsed = started.elapsed();
    let not_sent = not_sent(&error).unwrap_or_else(|| {
        panic!("a connect cut short by the deadline was not proven unsent: {error:#}")
    });
    assert_eq!(not_sent.method, "submitblock");
    assert_eq!(not_sent.cause, "timed out");
    assert!(
        elapsed >= SUBMIT_DEADLINE / 2 && elapsed < SUBMIT_DEADLINE,
        "the connect did not end at half the deadline: {elapsed:?}"
    );
    let handshake = sent.recv().await.unwrap();
    assert_eq!(handshake.first(), Some(&0x16), "no TLS handshake record");

    let error = rpc
        .call_timeout("getblockcount", json!([]), Some(SUBMIT_DEADLINE))
        .await
        .expect_err("the read must fail");
    assert!(
        error.downcast_ref::<RpcNotSentError>().is_none(),
        "{error:#}"
    );
    assert_eq!(error.to_string(), "qbit RPC getblockcount transport failed");
}

/// Fill the accept queue of a `listen(0)` listener: one connection queued and
/// never accepted, after which Linux drops every SYN to it. A probe whose
/// connect does not finish proves the queue full; a probe that connects raced
/// the queued connection's handshake, and another probe follows.
async fn fill_accept_queue(address: std::net::SocketAddr) -> Vec<TcpStream> {
    let mut queued = vec![TcpStream::connect(address).await.unwrap()];
    for _ in 0..10 {
        let probe = TcpStream::connect(address);
        match tokio::time::timeout(Duration::from_millis(200), probe).await {
            Err(_) => return queued,
            Ok(probe) => queued.push(probe.unwrap()),
        }
    }
    panic!("the accept queue never filled: this kernel does not drop SYNs to a full queue");
}

/// #762's other side. A `submitblock` finds the client's one connection busy,
/// so it starts its own connect, which cannot finish: the node's accept queue
/// is full. The busy connection then returns to the pool and the request goes
/// out on it, while that connect still runs and later fails at the connect
/// deadline. The node holds the request past the call's deadline: the
/// outcome stays unknown, never unsent. A connect that never finished does
/// not prove that the request was not written.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_submitblock_written_to_a_pooled_connection_while_its_own_connect_runs_may_have_been_sent(
) {
    let deadline = Duration::from_secs(1);
    let socket = TcpSocket::new_v4().unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(0).unwrap();
    let address = listener.local_addr().unwrap();
    let rpc = rpc(address.port());
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let (read_tx, mut read_rx) = tokio::sync::mpsc::unbounded_channel();
    // The node accepts one connection. It answers the first call on it once
    // released, then reads the next and never answers; it accepts no other.
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = BufReader::new(stream);
        let (_, call) = read_call(&mut stream).await;
        read_tx.send(call["method"].clone()).unwrap();
        release_rx.await.unwrap();
        let reply = json!({"result": 7, "error": null, "id": call["id"]}).to_string();
        stream
            .get_mut()
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{reply}",
                    reply.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let (_, call) = read_call(&mut stream).await;
        read_tx.send(call["method"].clone()).unwrap();
        std::future::pending::<()>().await;
        drop(listener);
    });
    let first = tokio::spawn({
        let rpc = rpc.clone();
        async move { rpc.call("getblockcount", json!([])).await }
    });
    assert_eq!(read_rx.recv().await.unwrap(), "getblockcount");
    let _queued = fill_accept_queue(address).await;
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let second = tokio::spawn(async move {
        started_tx.send(()).unwrap();
        let started = Instant::now();
        let result = rpc
            .call_timeout("submitblock", json!(["00"]), Some(deadline))
            .await;
        (result, started.elapsed())
    });
    // One runtime thread: `second` ran until its call waited, with its own
    // connect started, before this task resumes.
    started_rx.await.unwrap();
    release_tx.send(()).unwrap();
    assert_eq!(first.await.unwrap().unwrap(), 7);
    assert_eq!(
        read_rx.recv().await.unwrap(),
        "submitblock",
        "the request did not go out on the pooled connection"
    );
    let (result, elapsed) = second.await.unwrap();
    server.abort();
    let error = result.expect_err("the node never answered");
    assert!(
        not_sent(&error).is_none(),
        "a request written to a pooled connection was taken for unsent: {error:#}"
    );
    assert_eq!(error.to_string(), "qbit RPC submitblock transport failed");
    assert!(elapsed >= deadline, "{elapsed:?}");
}

/// Any HTTP reply, an error status or a JSON-RPC error, proves the request
/// arrived; neither is ever treated as unsent.
#[tokio::test]
async fn http_and_node_errors_after_the_send_are_not_unsent() {
    for (status, body) in [
        ("500 Internal Server Error", "busy".to_owned()),
        (
            "200 OK",
            r#"{"result":null,"error":{"code":-1,"message":"no"},"id":1}"#.to_owned(),
        ),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let reply = body.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = BufReader::new(stream);
            read_request(&mut stream).await;
            stream
                .get_mut()
                .write_all(
                    format!(
                        "HTTP/1.1 {status}\r\ncontent-length: {}\r\n\r\n{reply}",
                        reply.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });
        let error = submit(&rpc(port)).await;
        server.await.unwrap();
        assert!(not_sent(&error).is_none(), "{status}: {error:#}");
        if status.starts_with("200") {
            assert!(error.downcast_ref::<RpcReplyError>().is_some(), "{error:#}");
        }
    }
}

/// #291: a client built `without_relay`, and every wallet client made from
/// it, refuses each relaying call before it is sent and still sends every
/// other call. The node records each method it receives, so "never sent" is
/// read off the socket; an ordinary client's `submitblock` does reach it.
#[tokio::test]
async fn a_client_without_relay_sends_no_relaying_call_and_every_other_call() {
    use axum::{extract::State, Json, Router};
    use std::sync::{Arc, Mutex};

    async fn record(
        State(seen): State<Arc<Mutex<Vec<String>>>>,
        Json(request): Json<serde_json::Value>,
    ) -> Json<serde_json::Value> {
        let method = request["method"].as_str().unwrap_or_default().to_owned();
        seen.lock().unwrap().push(method);
        Json(json!({"result": 7, "error": null, "id": request["id"]}))
    }
    let seen = Arc::new(Mutex::new(Vec::new()));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let app = Router::new().fallback(record).with_state(seen.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let held = rpc(port).without_relay();
    for client in [held.clone(), held.wallet("sponsor").unwrap()] {
        for method in RELAY_METHODS {
            let error = client
                .call(method, json!([]))
                .await
                .expect_err("a relaying call was sent");
            let refused = error
                .downcast_ref::<RpcRelayRefused>()
                .unwrap_or_else(|| panic!("{method}: {error:#}"));
            assert_eq!(refused.method, *method);
        }
        assert_eq!(client.call("getblockcount", json!([])).await.unwrap(), 7);
    }
    assert_eq!(
        rpc(port).call("submitblock", json!(["00"])).await.unwrap(),
        7
    );
    server.abort();
    assert_eq!(
        *seen.lock().unwrap(),
        ["getblockcount", "getblockcount", "submitblock"]
    );
}
