//! General node reads and ambiguous block submissions have independent budgets.
use axum::{
    body::{Body, Bytes},
    extract::{OriginalUri, State},
    http::header::CONTENT_TYPE,
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use futures_util::{stream, StreamExt};
use qbit_prism_server::{config::Config, rpc::Rpc};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    convert::Infallible,
    process::Command,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[test]
fn configuration_keeps_general_and_submit_deadlines_independent() {
    const EXPECTED: &str = "PRISM_TEST_EXPECTED_DEADLINES";
    if let Ok(expected) = std::env::var(EXPECTED) {
        let expected: Value = serde_json::from_str(&expected).unwrap();
        match Config::from_env() {
            Ok(config) => {
                assert!(expected["error"].is_null(), "invalid timeout was accepted");
                assert_eq!(
                    config.rpc_timeout.as_millis(),
                    expected["general_ms"].as_u64().unwrap() as u128
                );
                assert_eq!(
                    config.block_submit_timeout.as_millis(),
                    expected["submit_ms"].as_u64().unwrap() as u128
                );
            }
            Err(error) => {
                let message = expected["error"].as_str().expect("valid timeouts rejected");
                assert!(error.to_string().contains(message), "{error}");
            }
        }
        return;
    }
    type Overrides<'a> = &'a [(&'a str, &'a str)];
    let cases: &[(Overrides<'_>, Value)] = &[
        (&[], json!({"general_ms":15000,"submit_ms":1000})),
        (
            &[("PRISM_BLOCK_SUBMIT_RPC_TIMEOUT_SECONDS", "0.125")],
            json!({"general_ms":15000,"submit_ms":125}),
        ),
        (
            &[("PRISM_RPC_TIMEOUT_SECONDS", "2.5")],
            json!({"general_ms":2500,"submit_ms":1000}),
        ),
        (
            &[
                ("PRISM_RPC_TIMEOUT_SECONDS", "3.75"),
                ("PRISM_BLOCK_SUBMIT_RPC_TIMEOUT_SECONDS", "0.25"),
            ],
            json!({"general_ms":3750,"submit_ms":250}),
        ),
        (
            &[("PRISM_RPC_TIMEOUT_SECONDS", "0")],
            json!({"error":"PRISM_RPC_TIMEOUT_SECONDS"}),
        ),
        (
            &[("PRISM_BLOCK_SUBMIT_RPC_TIMEOUT_SECONDS", "-1")],
            json!({"error":"PRISM_BLOCK_SUBMIT_RPC_TIMEOUT_SECONDS"}),
        ),
    ];
    for (settings, expected) in cases {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "configuration_keeps_general_and_submit_deadlines_independent",
                "--nocapture",
            ])
            .env_clear()
            .env(
                "PRISM_DATABASE_URL",
                "postgresql://operator@127.0.0.1:1/offline",
            )
            .env("PRISM_ALLOW_TEST_SIGNING_SEEDS", "1")
            .env("PRISM_RUNTIME_WORKERS", "2")
            .env("QBIT_CHAIN", "regtest")
            .env(EXPECTED, expected.to_string())
            .envs(settings.iter().copied())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{settings:?}: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[derive(Default)]
struct Observed {
    calls: Mutex<BTreeMap<String, usize>>,
    paths: Mutex<Vec<String>>,
}
struct Node {
    url: String,
    observed: Arc<Observed>,
    task: tokio::task::JoinHandle<()>,
}
impl Node {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let observed = Arc::new(Observed::default());
        let app = Router::new()
            .route("/", post(reply))
            .fallback(reply)
            .with_state(observed.clone());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            url,
            observed,
            task,
        }
    }
    fn rpc(&self, timeout: Duration) -> Rpc {
        Rpc::new(
            self.url.clone(),
            "rpc-user".into(),
            "test-only-password".into(),
            timeout,
        )
        .unwrap()
    }
    fn count(&self, method: &str) -> usize {
        self.observed
            .calls
            .lock()
            .unwrap()
            .get(method)
            .copied()
            .unwrap_or(0)
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn reply(
    State(observed): State<Arc<Observed>>,
    OriginalUri(uri): OriginalUri,
    Json(request): Json<Value>,
) -> Response {
    observed.paths.lock().unwrap().push(uri.path().to_string());
    let method = request["method"].as_str().unwrap();
    *observed
        .calls
        .lock()
        .unwrap()
        .entry(method.into())
        .or_default() += 1;
    if method == "getrawtransaction" {
        // Headers and a valid JSON prefix arrive immediately. The RPC deadline
        // must cover reading the remaining response, not just receiving headers.
        let prefix = Bytes::from(format!(
            "{{\"id\":{},\"error\":null,\"result\":",
            request["id"]
        ));
        let body = stream::once(async { Ok::<_, Infallible>(prefix) }).chain(stream::once(async {
            tokio::time::sleep(Duration::from_millis(650)).await;
            Ok::<_, Infallible>(Bytes::from_static(b"true}"))
        }));
        return (
            [(CONTENT_TYPE, "application/json")],
            Body::from_stream(body),
        )
            .into_response();
    }
    let result = match method {
        "getblocktemplate" => {
            tokio::time::sleep(Duration::from_millis(350)).await;
            json!({"height":101,"bits":"207fffff"})
        }
        "submitblock" => {
            assert_eq!(request["params"], json!(["candidate-bytes"]));
            tokio::time::sleep(Duration::from_millis(450)).await;
            Value::Null
        }
        "getblockcount" => json!(100),
        _ => panic!("unexpected RPC {method}"),
    };
    Json(json!({"id":request["id"],"result":result,"error":null})).into_response()
}

#[tokio::test]
async fn ordinary_read_can_outlive_submit_deadline_and_submit_is_never_retried() {
    let node = Node::start().await;
    let rpc = node.rpc(Duration::from_secs(2));
    let submit_timeout = Duration::from_millis(100);
    let started = Instant::now();
    let template = rpc.call("getblocktemplate", json!([{}])).await.unwrap();
    assert_eq!(template["height"], 101);
    assert!(started.elapsed() > submit_timeout);
    let started = Instant::now();
    let error = rpc
        .call_timeout(
            "submitblock",
            json!(["candidate-bytes"]),
            Some(submit_timeout),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("submitblock"));
    assert!(
        started.elapsed() < Duration::from_millis(600),
        "submitblock used the general deadline"
    );
    // Give the original request time to finish server-side. A timed-out POST
    // must remain ambiguous, without any hidden second submission.
    tokio::time::sleep(Duration::from_millis(450)).await;
    assert_eq!(node.count("submitblock"), 1);
    assert_eq!(node.count("getblocktemplate"), 1);
    assert_eq!(rpc.call("getblockcount", json!([])).await.unwrap(), 100);
}

#[tokio::test]
async fn general_deadline_covers_streamed_body_and_releases_the_client() {
    let node = Node::start().await;
    let rpc = node.rpc(Duration::from_millis(150));
    let started = Instant::now();
    let error = rpc
        .call("getrawtransaction", json!(["txid"]))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("returned invalid JSON (HTTP 200"),
        "response headers should arrive before the body stalls: {error}"
    );
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "response body escaped the general deadline"
    );
    assert_eq!(node.count("getrawtransaction"), 1);
    assert_eq!(rpc.call("getblockcount", json!([])).await.unwrap(), 100);
}

#[tokio::test]
async fn wallet_selection_preserves_proxy_prefix_and_replaces_existing_wallet() {
    let node = Node::start().await;
    for (path, wallet, expected) in [
        (
            "proxy/qbit/",
            "fee wallet",
            "/proxy/qbit/wallet/fee%20wallet",
        ),
        ("proxy/qbit/wallet/old", "new", "/proxy/qbit/wallet/new"),
        ("", "fee wallet", "/wallet/fee%20wallet"),
    ] {
        let rpc = Rpc::new(
            format!("{}{path}", node.url),
            "rpc-user".into(),
            "test-only-password".into(),
            Duration::from_secs(2),
        )
        .unwrap();
        assert_eq!(
            rpc.wallet(wallet)
                .unwrap()
                .call("getblockcount", json!([]))
                .await
                .unwrap(),
            100
        );
        assert_eq!(
            node.observed.paths.lock().unwrap().last().unwrap(),
            expected
        );
    }
    assert_eq!(node.count("getblockcount"), 3);
}
