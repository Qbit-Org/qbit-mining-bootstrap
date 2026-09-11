//! Exercise actual HTTP failures through coordinator authorization. Environment
//! configuration is isolated in a child process; no parallel test mutates env.
use anyhow::{bail, ensure, Context, Result};
use qbit_prism_server::{config::Config, coordinator::Coordinator, stratum::MiningBackend};
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::Mutex,
    task::{JoinHandle, JoinSet},
};

const FALLBACK: &str = "configured-fallback";

enum Reply {
    Result(Value),
    RpcError,
    InvalidJson,
    Disconnect,
    Delay,
}

#[derive(Default)]
struct NodeState {
    replies: HashMap<String, Reply>,
    addresses: Vec<String>,
}

struct Node {
    state: Arc<Mutex<NodeState>>,
    url: String,
    task: JoinHandle<()>,
}

impl Drop for Node {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn valid_address(address: &str) -> Value {
    let program = if address == FALLBACK { "22" } else { "11" };
    json!({"isvalid":true,"scriptPubKey":format!("5220{}", program.repeat(32))})
}

impl Node {
    async fn open() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/", listener.local_addr()?);
        let state = Arc::new(Mutex::new(NodeState::default()));
        let shared = state.clone();
        let task = tokio::spawn(async move {
            let mut requests = JoinSet::new();
            loop {
                tokio::select! {
                    connection = listener.accept() => {
                        let (stream, _) = connection.unwrap();
                        let state = shared.clone();
                        requests.spawn(async move { let _ = answer(stream, state).await; });
                    }
                    _ = requests.join_next(), if !requests.is_empty() => {}
                }
            }
        });
        Ok(Self { state, url, task })
    }
}

async fn answer(stream: TcpStream, state: Arc<Mutex<NodeState>>) -> Result<()> {
    let mut stream = BufReader::new(stream);
    let mut length = None;
    loop {
        let mut line = String::new();
        ensure!(
            stream.read_line(&mut line).await? != 0,
            "request ended early"
        );
        if line == "\r\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                length = Some(value.trim().parse::<usize>()?);
            }
        }
    }
    let mut body = vec![0; length.context("missing content length")?];
    stream.read_exact(&mut body).await?;
    let request: Value = serde_json::from_slice(&body)?;
    let reply = match request["method"].as_str() {
        Some("getblockhash") => Reply::Result(json!("00".repeat(32))),
        Some("getblockchaininfo") => Reply::Result(json!({"chain":"regtest"})),
        Some("validateaddress") => {
            let address = request["params"][0].as_str().context("missing address")?;
            let mut state = state.lock().await;
            state.addresses.push(address.into());
            state
                .replies
                .remove(address)
                .unwrap_or_else(|| Reply::Result(valid_address(address)))
        }
        _ => bail!("unexpected RPC {}", request["method"]),
    };
    let body = match reply {
        Reply::Disconnect => return Ok(()),
        Reply::Delay => {
            tokio::time::sleep(Duration::from_millis(600)).await;
            json!({"id":request["id"],"result":valid_address(request["params"][0].as_str().unwrap()),"error":null}).to_string()
        }
        Reply::InvalidJson => "{invalid JSON".into(),
        Reply::RpcError => {
            json!({"id":request["id"],"result":null,"error":{"code":-28,"message":"warming up"}})
                .to_string()
        }
        Reply::Result(result) => {
            json!({"id":request["id"],"result":result,"error":null}).to_string()
        }
    };
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.get_mut().write_all(response.as_bytes()).await?;
    Ok(())
}

async fn verify_authorization(coordinator: &Coordinator, node: &Node) -> Result<()> {
    let failures = [
        ("timeout", Reply::Delay),
        ("transport", Reply::Disconnect),
        ("rpc-error", Reply::RpcError),
        ("json-error", Reply::InvalidJson),
        ("null-result", Reply::Result(Value::Null)),
        (
            "missing-validity",
            Reply::Result(json!({"scriptPubKey":"5220"})),
        ),
        ("string-validity", Reply::Result(json!({"isvalid":"false"}))),
        ("missing-script", Reply::Result(json!({"isvalid":true}))),
        (
            "numeric-script",
            Reply::Result(json!({"isvalid":true,"scriptPubKey":34})),
        ),
        (
            "empty-script",
            Reply::Result(json!({"isvalid":true,"scriptPubKey":""})),
        ),
        (
            "nonhex-script",
            Reply::Result(json!({"isvalid":true,"scriptPubKey":format!("5220{}","gg".repeat(32))})),
        ),
        (
            "truncated-p2mr",
            Reply::Result(json!({"isvalid":true,"scriptPubKey":format!("5220{}","11".repeat(31))})),
        ),
        (
            "truncated-p2pkh",
            Reply::Result(
                json!({"isvalid":true,"scriptPubKey":format!("76a914{}88ac","11".repeat(19))}),
            ),
        ),
        (
            "invalid-v0-size",
            Reply::Result(json!({"isvalid":true,"scriptPubKey":"00021111"})),
        ),
        (
            "invalid-witness-push",
            Reply::Result(json!({"isvalid":true,"scriptPubKey":"53031111"})),
        ),
        (
            "unknown-script",
            Reply::Result(json!({"isvalid":true,"scriptPubKey":"51"})),
        ),
    ];
    for (name, reply) in failures {
        let username = format!("{name}.rig");
        let before = {
            let mut state = node.state.lock().await;
            state.replies.insert(name.into(), reply);
            state.addresses.len()
        };
        let error = coordinator.authorize(&username).await.unwrap_err();
        ensure!(
            error.reason_id.as_deref() == Some("backend-rpc-unavailable"),
            "{name}: {error:?}"
        );
        ensure!(
            node.state.lock().await.addresses[before..] == [name],
            "{name}: failure consulted fallback"
        );

        // Recovery must resolve the original recipient, not a poisoned fallback
        // cache entry. A subsequent successful cache hit must avoid another RPC.
        let worker = coordinator.authorize(&username).await?;
        ensure!(
            worker.payout_address == name && worker.p2mr_program_hex == "11".repeat(32),
            "{name}: retry redirected payout"
        );
        ensure!(worker.username == username && worker.worker_name.as_deref() == Some("rig"));
        let cached = coordinator.authorize(&username).await?;
        ensure!(
            cached.payout_address == name && cached.p2mr_program_hex == worker.p2mr_program_hex
        );
        ensure!(
            node.state.lock().await.addresses[before..] == [name, name],
            "{name}: failed request cached or successful request not cached"
        );
    }

    let unsupported = [
        ("p2pkh", format!("76a914{}88ac", "33".repeat(20))),
        ("p2sh", format!("a914{}87", "33".repeat(20))),
        ("p2wpkh", format!("0014{}", "33".repeat(20))),
        ("p2wsh", format!("0020{}", "33".repeat(32))),
        ("p2tr", format!("5120{}", "33".repeat(32))),
        ("p2a", "51024e73".into()),
        ("future-witness", format!("6028{}", "33".repeat(40))),
    ];
    let negatives = std::iter::once(("alias", json!({"isvalid":false}))).chain(
        unsupported
            .into_iter()
            .map(|(name, script)| (name, json!({"isvalid":true,"scriptPubKey":script}))),
    );
    for (name, validation) in negatives {
        let before = {
            let mut state = node.state.lock().await;
            state.replies.insert(name.into(), Reply::Result(validation));
            state.addresses.len()
        };
        let username = format!("{name}.rig");
        let worker = coordinator.authorize(&username).await?;
        ensure!(worker.payout_address == FALLBACK && worker.p2mr_program_hex == "22".repeat(32));
        ensure!(worker.username == username && worker.worker_name.as_deref() == Some("rig"));
        let cached = coordinator.authorize(&username).await?;
        ensure!(
            cached.payout_address == FALLBACK && cached.p2mr_program_hex == worker.p2mr_program_hex
        );
        ensure!(
            node.state.lock().await.addresses[before..] == [name, FALLBACK],
            "{name}: fallback or identity cache changed"
        );
    }

    // The fallback address also requires definitive positive validation. Its
    // transient failure may not authorize or prevent a later clean resolution.
    for fallback_reply in [Reply::RpcError, Reply::Result(json!({"isvalid":false}))] {
        let mut state = node.state.lock().await;
        state.replies.insert(
            "fallback-retry".into(),
            Reply::Result(json!({"isvalid":false})),
        );
        state.replies.insert(FALLBACK.into(), fallback_reply);
        drop(state);
        ensure!(coordinator.authorize("fallback-retry.rig").await.is_err());
    }
    let worker = coordinator.authorize("fallback-retry.rig").await?;
    ensure!(
        worker.payout_address == "fallback-retry" && worker.p2mr_program_hex == "11".repeat(32)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authorization_fallback_requires_a_definitive_address_result() -> Result<()> {
    if std::env::var_os("PRISM_AUTHORIZATION_TEST_CHILD").is_none() {
        let Ok(database_url) = std::env::var("PRISM_TEST_DATABASE_URL") else {
            eprintln!("skipping authorization RPC integration; set PRISM_TEST_DATABASE_URL");
            return Ok(());
        };
        let output = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "authorization_fallback_requires_a_definitive_address_result",
                "--nocapture",
            ])
            .env_clear()
            .env("PRISM_AUTHORIZATION_TEST_CHILD", "1")
            .env("PRISM_DATABASE_URL", database_url)
            .env("PRISM_ALLOW_TEST_SIGNING_SEEDS", "1")
            .env("PRISM_RUNTIME_WORKERS", "2")
            .env("QBIT_CHAIN", "regtest")
            .env("PRISM_USERNAME_FALLBACK_ADDRESS", FALLBACK)
            .output()?;
        ensure!(
            output.status.success(),
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return Ok(());
    }
    let mut config = Config::from_env()?;
    let admin = sqlx::PgPool::connect(&config.database_url).await?;
    let schema = format!("prism_auth_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await?;
    let mut url = url::Url::parse(&config.database_url)?;
    url.query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    config.database_url = url.into();
    config.initialize_schema = true;
    let node = Node::open().await?;
    config.rpc_url = node.url.clone();
    config.rpc_timeout = Duration::from_millis(150);
    let coordinator = Coordinator::new(
        config,
        std::sync::Arc::new(qbit_prism_server::metrics::Metrics::default()),
    )
    .await?;
    let result = verify_authorization(&coordinator, &node).await;
    coordinator.ledger.pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await?;
    admin.close().await;
    result
}
