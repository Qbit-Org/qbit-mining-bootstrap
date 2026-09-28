//! #525: a work build that keeps failing on a new tip must be visible to the
//! paging rule, not only as a `template refresh deferred` WARN line.
//!
//! The child is the real server binary (`run`) against the in-process fake
//! node (`support/fake_qbitd.rs`) and a PostgreSQL database of its own. One
//! miner holds the published job on tip A. The node then moves to tip B and
//! keeps answering a template for A, so every refresh observes B and fails
//! after the observation, as a `PayoutExceedsCandidateBalance` refresh does:
//! no job is ever published on B. The published generation does not change,
//! so the miner still holds "current" work and semantic coverage reads 1 for
//! the whole stall; `qbit_prism_current_parent_work_missing_seconds` is the
//! signal that grows. A template for B then publishes and it returns to 0.
//!
//! ```text
//! PRISM_TEST_DATABASE_URL=postgresql://user@127.0.0.1:5432/postgres \
//!   cargo test --locked -p qbit-prism-server --test parent_work_metric
//! ```
use anyhow::{ensure, Context, Result};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism_server::ledger::Ledger;
use qbit_prism_test_gate as gate;
use serde_json::json;
use std::{
    net::TcpListener,
    process::Stdio,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    process::{Child, Command},
};

#[allow(dead_code)]
#[path = "support/fake_qbitd.rs"]
mod fake_qbitd;
use fake_qbitd::{FakeNode, TEMPLATE_BITS};

#[allow(dead_code)]
#[path = "support/ledger_database.rs"]
mod ledger_database;
use ledger_database::FixtureDatabase;

const MISSING: &str = "qbit_prism_current_parent_work_missing_seconds";
const COVERAGE: &str = "qbit_prism_stratum_semantic_current_work_ratio";
const AUTHORIZED: &str = "qbit_prism_authorized_clients";
const HEALTH: &str = "qbit_prism_health_state";
/// The hang guard for one wait; no property is decided by comparing to it.
const DEADLINE: Duration = Duration::from_secs(90);
const POLL: Duration = Duration::from_millis(200);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failing_refresh_on_a_new_tip_grows_the_parent_work_gauge_while_coverage_reads_one(
) -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let database = FixtureDatabase::open(&raw, "prism_parent_work_").await?;
    // The parent initializes the schema and registers no instance.
    let ledger = match Ledger::connect_tool(
        &database.url,
        "parent-work-fixture".into(),
        4,
        true,
        None,
    )
    .await
    {
        Ok(ledger) => ledger,
        Err(error) => return Err(database.abandon(error).await),
    };
    let outcome = stall(&database.url).await;
    ledger.pool.close().await;
    database.close(outcome).await
}

async fn stall(database_url: &str) -> Result<()> {
    let node = FakeNode::open().await?;
    let (stratum, api) = (free_port()?, free_port()?);
    let log = tempfile::NamedTempFile::new()?;
    let mut server = spawn(database_url, &node.url, stratum, api, &log)?;
    let result = async {
        let client = reqwest::Client::new();
        let metrics = format!("http://127.0.0.1:{api}/metrics");
        let _miner = Miner::connect(stratum, &mut server).await?;

        // Work on tip A reaches the miner.
        wait(&client, &metrics, &mut server, "work on tip A", |body| {
            sample(body, MISSING) == Some(0.)
                && sample(body, HEALTH) == Some(1.)
                && sample(body, AUTHORIZED) == Some(1.)
                && sample(body, COVERAGE) == Some(1.)
        })
        .await?;

        // Tip B, whose every work build fails after the tip observation.
        let tip_a = "ab".repeat(32);
        node.set_template(Some(template(&tip_a, 101)));
        node.set_tip(&"bc".repeat(32), &tip_a, 101, "02");
        let first = wait(&client, &metrics, &mut server, "the stall", |body| {
            sample(body, MISSING).is_some_and(|age| age >= 2.)
        })
        .await?;
        tokio::time::sleep(Duration::from_secs(3)).await;
        let later = scrape(&client, &metrics).await?;
        let (before, after) = (
            sample(&first, MISSING).unwrap(),
            sample(&later, MISSING).context("gauge vanished")?,
        );
        ensure!(
            after >= before + 2.,
            "the gauge must keep growing through the stall: {before} then {after}"
        );
        // The published generation never changed, so the connected miner
        // still counts as covered: the coverage rules cannot see this stall.
        for body in [&first, &later] {
            ensure!(sample(body, COVERAGE) == Some(1.), "coverage moved: {body}");
            ensure!(sample(body, AUTHORIZED) == Some(1.), "miner left: {body}");
            ensure!(sample(body, HEALTH) == Some(0.), "health stayed ready");
        }

        // A template on B publishes, and the gauge returns to zero.
        node.set_template(None);
        wait(&client, &metrics, &mut server, "work on tip B", |body| {
            sample(body, MISSING) == Some(0.) && sample(body, HEALTH) == Some(1.)
        })
        .await?;
        Ok(())
    }
    .await;
    let _ = server.start_kill();
    let _ = server.wait().await;
    result.map_err(|error| {
        let bytes = std::fs::read(log.path()).unwrap_or_default();
        let tail = String::from_utf8_lossy(&bytes[bytes.len().saturating_sub(4096)..]).into_owned();
        error.context(format!("server stderr tail:\n{tail}"))
    })
}

fn template(parent: &str, height: u64) -> serde_json::Value {
    let now = chrono::Utc::now().timestamp();
    json!({
        "height":height,"coinbasevalue":5_000_000_000u64,"previousblockhash":parent,
        "version":0x20000000u32,"bits":TEMPLATE_BITS,"curtime":now,"mintime":now-1,
        "transactions":[]
    })
}

fn free_port() -> Result<u16> {
    Ok(TcpListener::bind("127.0.0.1:0")?.local_addr()?.port())
}

fn spawn(
    database_url: &str,
    node_url: &str,
    stratum: u16,
    api: u16,
    log: &tempfile::NamedTempFile,
) -> Result<Child> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_qbit-prism-server"));
    for (key, _) in
        std::env::vars().filter(|(key, _)| key.starts_with("PRISM_") || key.starts_with("QBIT_"))
    {
        command.env_remove(key);
    }
    let ledger_seed = "22".repeat(32);
    command
        .arg("run")
        .kill_on_drop(true)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(log.reopen()?))
        .env("RUST_LOG", "warn")
        .env("PRISM_DATABASE_URL", database_url)
        .env("PRISM_INSTANCE_ID", "parent-work-frontend")
        .env("PRISM_DATABASE_MAX_CONNECTIONS", "8")
        .env("PRISM_RUNTIME_WORKERS", "2")
        .env("PRISM_JOB_BUILD_EXECUTOR_WORKERS", "2")
        .env("QBIT_CHAIN", "testnet")
        .env("QBIT_RPC_URL", node_url)
        .env("PRISM_ALLOW_TEST_SIGNING_SEEDS", "1")
        .env("PRISM_MANIFEST_SIGNING_SEED_HEX", "11".repeat(32))
        .env("PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX", &ledger_seed)
        .env(
            "PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX",
            ManifestSigningKey::from_seed_hex(&ledger_seed)?.public_key_hex(),
        )
        .env("PRISM_STRATUM_BIND", "127.0.0.1")
        .env("PRISM_STRATUM_PORT", stratum.to_string())
        .env("PRISM_AUDIT_PORT", api.to_string())
        // The node has no long poll; a short poll sees tip B promptly, and a
        // one-second health tick republishes the metrics snapshot.
        .env("PRISM_BLOCKWAIT_ENABLED", "0")
        .env("PRISM_BLOCKPOLL_SECONDS", "0.2")
        .env("PRISM_HEALTH_REFRESH_SECONDS", "1")
        .env("PRISM_HASHRATE_ROLLUP_ENABLED", "0");
    command.spawn().context("spawning the server binary")
}

/// One authorized Stratum session that keeps whatever work it is sent.
struct Miner {
    _reader: tokio::task::JoinHandle<()>,
}

impl Miner {
    async fn connect(port: u16, server: &mut Child) -> Result<Self> {
        let started = Instant::now();
        let stream = loop {
            match TcpStream::connect(("127.0.0.1", port)).await {
                Ok(stream) => break stream,
                Err(error) => {
                    alive(server)?;
                    ensure!(
                        started.elapsed() < DEADLINE,
                        "Stratum never listened: {error}"
                    );
                    tokio::time::sleep(POLL).await;
                }
            }
        };
        let (read, mut write) = stream.into_split();
        for request in [
            json!({"id":1,"method":"mining.subscribe","params":["parent-work/1"]}),
            json!({"id":2,"method":"mining.authorize","params":["parent-work.rig","x"]}),
        ] {
            write.write_all(format!("{request}\n").as_bytes()).await?;
        }
        let reader = tokio::spawn(async move {
            let _write = write;
            let mut lines = BufReader::new(read).lines();
            while let Ok(Some(_)) = lines.next_line().await {}
        });
        Ok(Self { _reader: reader })
    }
}

fn alive(server: &mut Child) -> Result<()> {
    if let Some(status) = server.try_wait()? {
        anyhow::bail!("the server exited with {status}");
    }
    Ok(())
}

async fn scrape(client: &reqwest::Client, url: &str) -> Result<String> {
    Ok(client
        .get(url)
        .timeout(Duration::from_secs(5))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?)
}

async fn wait(
    client: &reqwest::Client,
    url: &str,
    server: &mut Child,
    what: &str,
    done: impl Fn(&str) -> bool,
) -> Result<String> {
    let started = Instant::now();
    let mut last = String::new();
    loop {
        alive(server)?;
        if let Ok(body) = scrape(client, url).await {
            if done(&body) {
                return Ok(body);
            }
            last = body;
        }
        ensure!(
            started.elapsed() < DEADLINE,
            "timed out waiting for {what}; last scrape:\n{}",
            last.lines()
                .filter(|line| [MISSING, COVERAGE, AUTHORIZED, HEALTH]
                    .iter()
                    .any(|name| line.starts_with(name)))
                .collect::<Vec<_>>()
                .join("\n")
        );
        tokio::time::sleep(POLL).await;
    }
}

fn sample(body: &str, name: &str) -> Option<f64> {
    body.lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(' '))
        .and_then(|value| value.trim().parse().ok())
}
