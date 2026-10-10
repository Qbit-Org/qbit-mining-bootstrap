//! 3.1 readiness at the frontend's sockets (D4): the real server binary
//! against the in-process fake node and a PostgreSQL database of its own.
//!
//! A dual-writer frontend that does not admit miners refuses every Stratum
//! connection at the socket (no handshake completes), its readiness endpoint
//! answers 503 to the token and 401 without it, and `/healthz` says why. A
//! single writer with the endpoint on listens from startup, as 3.0 does, and
//! its endpoint answers 200 once it is ready. A rebuild after a payout
//! revision bump leaves the endpoint at 200 for the whole admission grace,
//! and one held past it withdraws the frontend until its work is rebuilt.
//! The `healthcheck` subcommand is liveness for a dual-writer frontend (it
//! passes while the frontend catches up, and fails when it writes to the
//! other node's database) and 3.0's readiness rule for a single writer.
//!
//! ```text
//! PRISM_TEST_DATABASE_URL=postgresql://user@127.0.0.1:5432/postgres \
//!   cargo test --locked -p qbit-prism-server --test readiness_frontend
//! ```
use anyhow::{ensure, Context, Result};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism_server::{
    ledger::Ledger, node_identity::NodeIndex, readiness::admission::DEFAULT_GRACE,
};
use qbit_prism_test_gate as gate;
use serde_json::Value;
use std::{
    future::Future,
    io,
    net::TcpListener,
    process::Stdio,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};
use tokio::{
    net::TcpStream,
    process::{Child, Command},
};

#[allow(dead_code)]
#[path = "support/fake_qbitd.rs"]
mod fake_qbitd;
use fake_qbitd::FakeNode;

#[allow(dead_code)]
#[path = "support/ledger_database.rs"]
mod ledger_database;
use ledger_database::FixtureDatabase;
#[path = "support/pool_fee.rs"]
mod pool_fee;

const TOKEN: &str = "readiness-frontend-token-0123456789";
/// The hang guard for one wait; no property is decided by comparing to it.
const DEADLINE: Duration = Duration::from_secs(90);
const POLL: Duration = Duration::from_millis(200);

struct Ports {
    stratum: u16,
    audit: u16,
    readiness: u16,
}

/// Ports held by their bound listeners until the server is about to bind
/// them (#639).
fn reserve_ports() -> Result<(Ports, Vec<TcpListener>)> {
    let held: Vec<TcpListener> = (0..3)
        .map(|_| TcpListener::bind("127.0.0.1:0"))
        .collect::<io::Result<_>>()?;
    let port = |index: usize| held[index].local_addr().map(|addr| addr.port());
    Ok((
        Ports {
            stratum: port(0)?,
            audit: port(1)?,
            readiness: port(2)?,
        },
        held,
    ))
}

fn spawn(
    database_url: &str,
    node_url: &str,
    ports: &Ports,
    held: Vec<TcpListener>,
    dual_writer: bool,
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
        .env("PRISM_INSTANCE_ID", "readiness-frontend")
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
        .env("PRISM_STRATUM_PORT", ports.stratum.to_string())
        .env("PRISM_AUDIT_PORT", ports.audit.to_string())
        .env("PRISM_READINESS_BIND", "127.0.0.1")
        .env("PRISM_READINESS_PORT", ports.readiness.to_string())
        .env("PRISM_READINESS_TOKEN", TOKEN)
        .env("PRISM_BLOCKWAIT_ENABLED", "0")
        .env("PRISM_BLOCKPOLL_SECONDS", "0.2")
        .env("PRISM_HEALTH_REFRESH_SECONDS", "1")
        .env("PRISM_HASHRATE_ROLLUP_ENABLED", "0");
    if dual_writer {
        command
            .env("PRISM_DUAL_WRITER", "1")
            .env("PRISM_NODE_INDEX", "1")
            .env("PRISM_CARRY_OWNER", "0")
            // No sync engine pulls in this test; the peer is never dialled.
            .env(
                "PRISM_PEER_DATABASE_URL",
                "postgresql://prism_peer_sync@127.0.0.1:1/peer",
            );
    }
    drop(held);
    command.spawn().context("spawning the server binary")
}

fn alive(server: &mut Child) -> Result<()> {
    ensure!(
        server.try_wait()?.is_none(),
        "the server exited before the test finished"
    );
    Ok(())
}

async fn get(client: &reqwest::Client, url: &str, token: Option<&str>) -> Result<(u16, String)> {
    let mut request = client.get(url);
    if let Some(token) = token {
        request = request.header("X-Qbit-Healthcheck-Token", token);
    }
    let response = request.send().await?;
    Ok((response.status().as_u16(), response.text().await?))
}

/// Poll the operator `/healthz` until `accept` holds.
async fn health_until(
    client: &reqwest::Client,
    ports: &Ports,
    server: &mut Child,
    what: &str,
    accept: impl Fn(&Value) -> bool,
) -> Result<Value> {
    let started = Instant::now();
    let url = format!("http://127.0.0.1:{}/healthz", ports.audit);
    loop {
        alive(server)?;
        if let Ok((_, body)) = get(client, &url, None).await {
            if let Ok(health) = serde_json::from_str::<Value>(&body) {
                if accept(&health) {
                    return Ok(health);
                }
            }
        }
        ensure!(
            started.elapsed() < DEADLINE,
            "{what}: /healthz never matched"
        );
        tokio::time::sleep(POLL).await;
    }
}

fn sample(body: &str, key: &str) -> Option<f64> {
    body.lines()
        .find_map(|line| line.strip_prefix(&format!("{key} ")))
        .and_then(|value| value.parse().ok())
}

async fn refused(port: u16) -> Result<bool> {
    match tokio::time::timeout(
        Duration::from_secs(2),
        TcpStream::connect(("127.0.0.1", port)),
    )
    .await
    {
        Ok(Err(error)) => Ok(error.kind() == io::ErrorKind::ConnectionRefused),
        Ok(Ok(_)) => Ok(false),
        Err(_) => anyhow::bail!("a Stratum connect neither completed nor was refused"),
    }
}

/// Bump the payout revision, as every landed block does; returns the new
/// revision.
async fn bump_payout_revision(pool: &sqlx::PgPool) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1 \
         WHERE singleton RETURNING payout_revision",
    )
    .fetch_one(pool)
    .await?)
}

/// Run `work` while polling the readiness endpoint every 100 ms. Returns
/// its output and every answer that was not 200, with when it came.
async fn polling_readiness<T>(
    client: &reqwest::Client,
    ports: &Ports,
    work: impl Future<Output = Result<T>>,
) -> Result<(T, Vec<(Duration, u16)>)> {
    let readyz = format!("http://127.0.0.1:{}/readyz", ports.readiness);
    let started = Instant::now();
    let done = AtomicBool::new(false);
    let poll = async {
        let mut refusals = Vec::new();
        while !done.load(Ordering::Relaxed) {
            // 0: no answer at all.
            let status = get(client, &readyz, Some(TOKEN))
                .await
                .map_or(0, |(status, _)| status);
            if status != 200 {
                refusals.push((started.elapsed(), status));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        refusals
    };
    let work = async {
        let output = work.await;
        done.store(true, Ordering::Relaxed);
        output
    };
    let (output, refusals) = tokio::join!(work, poll);
    Ok((output?, refusals))
}

/// `qbit-prism-server healthcheck` against this frontend's operator
/// `/healthz`, in an environment of its own: whether it passed, and its
/// stderr.
async fn healthcheck(ports: &Ports) -> Result<(bool, String)> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_qbit-prism-server"));
    for (key, _) in
        std::env::vars().filter(|(key, _)| key.starts_with("PRISM_") || key.starts_with("QBIT_"))
    {
        command.env_remove(key);
    }
    let url = format!("http://127.0.0.1:{}/healthz", ports.audit);
    let output = tokio::time::timeout(
        Duration::from_secs(10),
        command
            .args(["healthcheck", "--url", &url])
            .kill_on_drop(true)
            .stdin(Stdio::null())
            .output(),
    )
    .await
    .context("the healthcheck hung")??;
    Ok((
        output.status.success(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    ))
}

async fn with_server(
    database_url: &str,
    dual_writer: bool,
    check: impl AsyncFnOnce(&reqwest::Client, &Ports, &mut Child, &FakeNode) -> Result<()>,
) -> Result<()> {
    let node = FakeNode::open().await?;
    let (ports, held) = reserve_ports()?;
    let log = tempfile::NamedTempFile::new()?;
    let mut server = spawn(database_url, &node.url, &ports, held, dual_writer, &log)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let result = check(&client, &ports, &mut server, &node).await;
    let _ = server.start_kill();
    let _ = server.wait().await;
    result.map_err(|error| {
        let bytes = std::fs::read(log.path()).unwrap_or_default();
        let tail = String::from_utf8_lossy(&bytes[bytes.len().saturating_sub(4096)..]).into_owned();
        error.context(format!("server stderr tail:\n{tail}"))
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dual_writer_frontend_that_is_not_ready_refuses_stratum_and_answers_not_ready(
) -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let database = FixtureDatabase::open(&raw, "prism_d4_frontend_dual_").await?;
    let ledger = match Ledger::connect_tool(
        &database.url,
        "d4-frontend-fixture".into(),
        2,
        true,
        None,
    )
    .await
    {
        Ok(ledger) => ledger,
        Err(error) => return Err(database.abandon(error).await),
    };
    let outcome = async {
        ledger.set_node_identity(NodeIndex::B, "d4-test").await?;
        with_server(&database.url, true, async |client, ports, server, _| {
            // The publisher has run: the frontend is not ready, and says why.
            let health = health_until(client, ports, server, "a published health", |health| {
                health.get("admission").is_some()
            })
            .await?;
            ensure!(health["ok"] == false, "{health}");
            ensure!(health["status"] == "own-log-behind", "{health}");
            ensure!(health["dual_writer"]["node_index"] == 1, "{health}");
            ensure!(
                health["dual_writer"]["own_log_caught_up"] == false,
                "{health}"
            );
            ensure!(health["dual_writer"]["writer_path"] == "local", "{health}");
            ensure!(health["admission"]["admitting"] == false, "{health}");
            ensure!(health["admission"]["state"] == "starting", "{health}");
            // Catching up on its own log is expected: the container is healthy.
            let (healthy, stderr) = healthcheck(ports).await?;
            ensure!(
                healthy,
                "a dual-writer frontend catching up failed its healthcheck: {stderr}"
            );

            // No Stratum handshake completes, however often a miner tries.
            for _ in 0..10 {
                ensure!(
                    refused(ports.stratum).await?,
                    "a frontend that does not admit accepted a Stratum connection"
                );
                tokio::time::sleep(POLL).await;
            }

            let readyz = format!("http://127.0.0.1:{}/readyz", ports.readiness);
            let (status, body) = get(client, &readyz, Some(TOKEN)).await?;
            ensure!(status == 503 && body == "not ready\n", "{status} {body:?}");
            let (status, body) = get(client, &readyz, None).await?;
            ensure!(
                status == 401 && body == "unauthorized\n",
                "{status} {body:?}"
            );
            let (status, _) = get(client, &readyz, Some("wrong-token-0123456789")).await?;
            ensure!(status == 401, "{status}");

            let metrics = format!("http://127.0.0.1:{}/metrics", ports.audit);
            let (_, body) = get(client, &metrics, None).await?;
            for (key, expected) in [
                (
                    "qbit_prism_stratum_listener_accepting{listener=\"default\"}",
                    0.,
                ),
                ("qbit_prism_admission_admitting", 0.),
                ("qbit_prism_admission_state{state=\"starting\"}", 1.),
                ("qbit_prism_dual_writer_writer_path{path=\"local\"}", 1.),
                ("qbit_prism_dual_writer_node_index", 1.),
                ("qbit_prism_dual_writer_carry_owner", 0.),
            ] {
                ensure!(
                    sample(&body, key) == Some(expected),
                    "{key}: {:?}",
                    sample(&body, key)
                );
            }
            // The metrics body is a snapshot the publisher renews each tick,
            // so the endpoint's counter shows within a tick of its answer.
            let started = Instant::now();
            loop {
                let (_, body) = get(client, &metrics, None).await?;
                if sample(
                    &body,
                    "qbit_prism_readiness_requests_total{result=\"not_ready\"}",
                )
                .is_some_and(|count| count >= 1.)
                {
                    break;
                }
                ensure!(
                    started.elapsed() < DEADLINE,
                    "the not-ready answer was never counted"
                );
                tokio::time::sleep(POLL).await;
            }
            Ok(())
        })
        .await
    }
    .await;
    ledger.pool.close().await;
    database.close(outcome).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dual_writer_frontend_on_the_other_nodes_database_fails_its_healthcheck() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let database = FixtureDatabase::open(&raw, "prism_d4_frontend_remote_").await?;
    let ledger = match Ledger::connect_tool(
        &database.url,
        "d4-frontend-fixture".into(),
        2,
        true,
        None,
    )
    .await
    {
        Ok(ledger) => ledger,
        Err(error) => return Err(database.abandon(error).await),
    };
    let outcome = async {
        // Node A's database behind a frontend configured as node B: a DSN
        // left pointing at the peer.
        ledger.set_node_identity(NodeIndex::A, "d4-test").await?;
        with_server(&database.url, true, async |client, ports, server, _| {
            let health = health_until(client, ports, server, "the remote writer", |health| {
                health["dual_writer"]["writer_path"] == "remote"
            })
            .await?;
            ensure!(health["ok"] == false, "{health}");
            let (healthy, stderr) = healthcheck(ports).await?;
            ensure!(
                !healthy,
                "a frontend writing to the other node's database passed its healthcheck"
            );
            ensure!(stderr.contains("writer_path"), "{stderr}");
            Ok(())
        })
        .await
    }
    .await;
    ledger.pool.close().await;
    database.close(outcome).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_single_writer_frontend_listens_from_startup_and_its_endpoint_answers_ready() -> Result<()>
{
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let database = FixtureDatabase::open(&raw, "prism_d4_frontend_single_").await?;
    let ledger = match Ledger::connect_tool(
        &database.url,
        "d4-frontend-fixture".into(),
        2,
        true,
        None,
    )
    .await
    {
        Ok(ledger) => ledger,
        Err(error) => return Err(database.abandon(error).await),
    };
    let outcome = with_server(&database.url, false, async |client, ports, server, _| {
        // 3.0's listener: accepting whatever readiness says.
        let started = Instant::now();
        loop {
            alive(server)?;
            if TcpStream::connect(("127.0.0.1", ports.stratum))
                .await
                .is_ok()
            {
                break;
            }
            ensure!(started.elapsed() < DEADLINE, "Stratum never listened");
            tokio::time::sleep(POLL).await;
        }
        let readyz = format!("http://127.0.0.1:{}/readyz", ports.readiness);
        let started = Instant::now();
        loop {
            alive(server)?;
            if let Ok((200, body)) = get(client, &readyz, Some(TOKEN)).await {
                ensure!(body == "ready\n", "{body:?}");
                break;
            }
            ensure!(
                started.elapsed() < DEADLINE,
                "the endpoint never answered ready"
            );
            tokio::time::sleep(POLL).await;
        }
        let (status, _) = get(client, &readyz, None).await?;
        ensure!(status == 401, "{status}");
        let health = health_until(client, ports, server, "admitting", |health| {
            health["admission"]["admitting"] == true
        })
        .await?;
        ensure!(health.get("dual_writer").is_none(), "{health}");
        ensure!(health["admission"]["state"] == "admitting", "{health}");
        let (healthy, stderr) = healthcheck(ports).await?;
        ensure!(
            healthy,
            "a ready single writer failed its healthcheck: {stderr}"
        );
        let metrics = format!("http://127.0.0.1:{}/metrics", ports.audit);
        let (_, body) = get(client, &metrics, None).await?;
        ensure!(sample(&body, "qbit_prism_admission_admitting") == Some(1.));
        ensure!(
            !body
                .lines()
                .any(|line| line.starts_with("qbit_prism_stratum_listener_accepting{")),
            "a single writer's listeners are not gated"
        );
        ensure!(
            !body
                .lines()
                .any(|line| line.starts_with("qbit_prism_dual_writer_") && !line.starts_with('#')),
            "a single writer reports no dual-writer state"
        );
        Ok(())
    })
    .await;
    ledger.pool.close().await;
    database.close(outcome).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rebuild_inside_the_grace_keeps_the_endpoint_ready_and_a_longer_one_withdraws(
) -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let database = FixtureDatabase::open(&raw, "prism_d4_frontend_grace_").await?;
    let ledger = match Ledger::connect_tool(
        &database.url,
        "d4-frontend-fixture".into(),
        2,
        true,
        None,
    )
    .await
    {
        Ok(ledger) => ledger,
        Err(error) => return Err(database.abandon(error).await),
    };
    let pool = &ledger.pool;
    let outcome = with_server(&database.url, false, async |client, ports, server, node| {
        health_until(client, ports, server, "admitting", |health| {
            health["admission"]["admitting"] == true
        })
        .await?;

        // Every landed block bumps the payout revision, and readiness stays
        // false until the frontend's work is rebuilt on it (D6's flapping
        // /healthz). Polled through five bumps, the endpoint never leaves 200.
        let ((), refusals) = polling_readiness(client, ports, async {
            for _ in 0..5 {
                let revision = bump_payout_revision(pool).await?;
                health_until(client, ports, server, "rebuilt on the bump", |health| {
                    health["payout_state_generation"] == revision
                })
                .await?;
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Ok(())
        })
        .await?;
        ensure!(
            refusals.is_empty(),
            "revision bumps turned the endpoint away: {refusals:?}"
        );

        // A rebuild held open: every refresh fetches a template first, so
        // with the next fetch unanswered the work stays on the old revision.
        let mut held = node.pause_next("getblocktemplate")?;
        tokio::time::timeout(DEADLINE, held.entered()).await??;
        // Taken before the bump commits. The grace runs from the frontend's
        // last ready publication, which came at most one publication
        // interval (PRISM_HEALTH_REFRESH_SECONDS, 1 s here) before the bump.
        let bumped = Instant::now();
        let revision = bump_payout_revision(pool).await?;
        let readyz = format!("http://127.0.0.1:{}/readyz", ports.readiness);
        let healthz = format!("http://127.0.0.1:{}/healthz", ports.audit);
        let mut dipped = false;
        let withdrawn_after = loop {
            alive(server)?;
            let (status, body) = get(client, &readyz, Some(TOKEN)).await?;
            if status != 200 {
                ensure!(status == 503 && body == "not ready\n", "{status} {body:?}");
                break bumped.elapsed();
            }
            if !dipped {
                let (_, body) = get(client, &healthz, None).await?;
                let health: Value = serde_json::from_str(&body)?;
                dipped = health["ok"] == false && health["admission"]["state"] == "grace";
                if dipped {
                    // A single writer's healthcheck is 3.0's readiness rule:
                    // the grace that keeps /readyz at 200 never passes it.
                    let (healthy, stderr) = healthcheck(ports).await?;
                    ensure!(
                        !healthy && stderr.contains("PRISM is unhealthy (HTTP 503"),
                        "a single writer inside the grace: healthy {healthy}, {stderr}"
                    );
                }
            }
            ensure!(
                bumped.elapsed() < DEADLINE,
                "a rebuild held past the grace never withdrew the frontend"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        ensure!(dipped, "readiness never dipped while the rebuild was held");
        // One publication interval before the bump, plus scheduling jitter.
        let earliest = DEFAULT_GRACE - Duration::from_secs(2);
        ensure!(
            withdrawn_after >= earliest,
            "withdrawn {withdrawn_after:?} after the bump, inside the {DEFAULT_GRACE:?} grace"
        );
        let health = health_until(client, ports, server, "withdrawn", |health| {
            health["admission"]["state"] == "withdrawn"
        })
        .await?;
        ensure!(health["admission"]["reason"] == "not-ready", "{health}");

        // Released, the work is rebuilt on the bump and the frontend
        // admits miners again.
        held.release();
        health_until(client, ports, server, "readmitted", |health| {
            health["admission"]["state"] == "admitting"
                && health["payout_state_generation"] == revision
        })
        .await?;
        let (status, _) = get(client, &readyz, Some(TOKEN)).await?;
        ensure!(status == 200, "{status}");
        Ok(())
    })
    .await;
    ledger.pool.close().await;
    database.close(outcome).await
}
