//! #525: a template refresh that keeps failing must be visible to the paging
//! rule, not only as a `template refresh deferred` WARN line.
//!
//! The child is the real server binary (`run`) against the in-process fake
//! node (`support/fake_qbitd.rs`) and a PostgreSQL database of its own. One
//! miner holds the published job on tip A. Two stalls follow, each ended by a
//! good template:
//!
//! - on the same parent: the node stays on A and answers a template for
//!   another parent, so every refresh fails with the tip unchanged, as a
//!   `PayoutExceedsCandidateBalance` rebuild on the same parent does;
//! - on a new tip: the node moves to B and keeps answering a template for A,
//!   so every refresh observes B and fails after the observation.
//!
//! Neither stall changes the published generation, so the miner still holds
//! "current" work and semantic coverage reads 1 throughout;
//! `qbit_prism_work_refresh_stalled_seconds` is the signal that grows, and it
//! returns to about zero once a refresh succeeds.
//!
//! ```text
//! PRISM_TEST_DATABASE_URL=postgresql://user@127.0.0.1:5432/postgres \
//!   cargo test --locked -p qbit-prism-server --test work_refresh_metric
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
#[path = "support/pool_fee.rs"]
mod pool_fee;

const STALLED: &str = "qbit_prism_work_refresh_stalled_seconds";
const COVERAGE: &str = "qbit_prism_stratum_semantic_current_work_ratio";
const AUTHORIZED: &str = "qbit_prism_authorized_clients";
const HEALTH: &str = "qbit_prism_health_state";
/// A healthy frontend refreshes every 0.2 s here and republishes the gauge
/// every second, so it reads well below this.
const HEALTHY: f64 = 3.;
/// The hang guard for one wait; no property is decided by comparing to it.
const DEADLINE: Duration = Duration::from_secs(90);
const POLL: Duration = Duration::from_millis(200);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failing_refreshes_grow_the_refresh_stall_gauge_while_coverage_reads_one() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let database = FixtureDatabase::open(&raw, "prism_work_refresh_").await?;
    // The parent initializes the schema and registers no instance.
    let ledger =
        match Ledger::connect_tool(&database.url, "work-refresh-fixture".into(), 4, true, None)
            .await
        {
            Ok(ledger) => ledger,
            Err(error) => return Err(database.abandon(error).await),
        };
    let outcome = stalls(&database.url).await;
    ledger.pool.close().await;
    database.close(outcome).await
}

async fn stalls(database_url: &str) -> Result<()> {
    let node = FakeNode::open().await?;
    let (stratum, api) = (free_port()?, free_port()?);
    let log = tempfile::NamedTempFile::new()?;
    let mut server = spawn(database_url, &node.url, stratum, api, &log)?;
    let result = async {
        let client = reqwest::Client::new();
        let metrics = format!("http://127.0.0.1:{api}/metrics");
        let _miner = Miner::connect(stratum, &mut server).await?;
        let serving = |body: &str| {
            sample(body, STALLED).is_some_and(|age| (0. ..HEALTHY).contains(&age))
                && sample(body, HEALTH) == Some(1.)
                && sample(body, AUTHORIZED) == Some(1.)
                && sample(body, COVERAGE) == Some(1.)
        };
        let (tip_a, tip_b) = ("ab".repeat(32), "bc".repeat(32));

        wait(&client, &metrics, &mut server, "work on tip A", serving).await?;
        // Healthy refreshes keep renewing it.
        tokio::time::sleep(Duration::from_secs(3)).await;
        ensure!(
            serving(&scrape(&client, &metrics).await?),
            "healthy frontend drifted"
        );

        // Same parent: a template for another parent fails every refresh
        // while the node's tip stays the published one.
        node.set_template(Some(template(&"cd".repeat(32), 101)));
        stalled(&client, &metrics, &mut server, "the same-parent stall").await?;
        node.set_template(None);
        wait(&client, &metrics, &mut server, "recovery on tip A", serving).await?;

        // New tip: B, whose every refresh fails after the tip observation.
        node.set_template(Some(template(&tip_a, 101)));
        node.set_tip(&tip_b, &tip_a, 101, "02");
        stalled(&client, &metrics, &mut server, "the new-tip stall").await?;
        node.set_template(None);
        wait(&client, &metrics, &mut server, "work on tip B", serving).await?;
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

/// The gauge grows through a stall while the published generation, so
/// semantic coverage, stays put. `health_state` is not asserted: on the same
/// parent it stays ready until the health timeout.
async fn stalled(
    client: &reqwest::Client,
    url: &str,
    server: &mut Child,
    what: &str,
) -> Result<()> {
    let first = wait(client, url, server, what, |body| {
        sample(body, STALLED).is_some_and(|age| age >= 5.)
    })
    .await?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let later = scrape(client, url).await?;
    let (before, after) = (
        sample(&first, STALLED).unwrap(),
        sample(&later, STALLED).context("gauge vanished")?,
    );
    ensure!(
        after >= before + 2.,
        "{what}: the gauge must keep growing: {before} then {after}"
    );
    for body in [&first, &later] {
        ensure!(sample(body, COVERAGE) == Some(1.), "{what}: coverage moved");
        ensure!(sample(body, AUTHORIZED) == Some(1.), "{what}: miner left");
    }
    Ok(())
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
        .env("PRISM_INSTANCE_ID", "work-refresh-frontend")
        .env("PRISM_DATABASE_MAX_CONNECTIONS", "8")
        .env("PRISM_RUNTIME_WORKERS", "2")
        .env("PRISM_JOB_BUILD_EXECUTOR_WORKERS", "2")
        .env("QBIT_CHAIN", "testnet")
        .env("QBIT_RPC_URL", node_url)
        .env("PRISM_ALLOW_TEST_SIGNING_SEEDS", "1")
        .envs(pool_fee::ZERO_BPS_POOL_FEE)
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
            json!({"id":1,"method":"mining.subscribe","params":["work-refresh/1"]}),
            json!({"id":2,"method":"mining.authorize","params":["work-refresh.rig","x"]}),
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
                .filter(|line| [STALLED, COVERAGE, AUTHORIZED, HEALTH]
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
