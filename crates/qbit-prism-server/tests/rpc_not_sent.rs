//! #522: which `submitblock` transport failures prove that the request never
//! reached the node. Only a call whose connection was never established is
//! [`RpcNotSentError`]; every failure after the connection existed, whether
//! or not the peer read the request, stays an ordinary error that the offer
//! path records as an unknown outcome. Each case runs the real client against
//! a real socket on the loopback interface, so a change in reqwest's or
//! hyper's error kinds fails here rather than in production.
use qbit_prism_server::rpc::{Rpc, RpcNotSentError, RpcReplyError};
use serde_json::json;
use std::time::Duration;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

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
    rpc.call_timeout(
        "submitblock",
        json!(["00"]),
        Some(Duration::from_millis(500)),
    )
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
    total + length
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
