//! 3.1 dual-writer readiness against PostgreSQL (D4). A dual-writer frontend
//! is ready only while its own log is caught up (the peer sync's latch,
//! decision D-8) and its database is this node's writable primary (the
//! identity row of decision D-9); `/healthz` carries the `dual_writer`
//! object of CONTRACT.md §3. A single writer's health is unchanged.
//!
//! ```text
//! PRISM_TEST_DATABASE_URL=postgresql://user@127.0.0.1:5432/postgres \
//!   cargo test --locked -p qbit-prism-server --test dual_writer_readiness
//! ```
use anyhow::{ensure, Context, Result};
use qbit_prism_server::{
    config::{Config, DualWriterConfig},
    coordinator::Coordinator,
    ledger::Ledger,
    metrics::Metrics,
    node_identity::{NodeIdentity, NodeIndex},
    peer_sync::PeerSyncPublisher,
};
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

#[allow(dead_code)]
#[path = "support/fake_qbitd.rs"]
mod fake_qbitd;
use fake_qbitd::FakeNode;

#[allow(dead_code)]
#[path = "support/ledger_database.rs"]
mod ledger_database;
use ledger_database::FixtureDatabase;

/// The hang guard for one wait; no property is decided by comparing to it.
const DEADLINE: Duration = Duration::from_secs(30);

fn dual_writer(node: NodeIndex) -> DualWriterConfig {
    DualWriterConfig {
        identity: NodeIdentity {
            node,
            carry_owner: node == NodeIndex::A,
        },
        // No sync engine runs in these tests; the peer is never dialled.
        peer_database_url: "postgresql://prism_peer_sync@127.0.0.1:1/peer".into(),
        peer_database_url_fallback: None,
        peer_sync_interval: Duration::from_millis(250),
        peer_sync_batch_rows: 5000,
        // No peer to wait for before a found block's offer.
        peer_ingest_wait: Duration::ZERO,
    }
}

async fn frontend(
    database: &FixtureDatabase,
    node: &FakeNode,
    instance: &str,
    dual: Option<DualWriterConfig>,
) -> Result<Arc<Coordinator>> {
    let mut config: Config = fake_qbitd::coordinator_config(database.url.clone(), node, instance)?;
    config.dual_writer = dual;
    let coordinator = Coordinator::new(config, Arc::new(Metrics::default())).await?;
    coordinator.refresh_once().await?;
    Ok(coordinator)
}

/// Poll `health` until `accept` holds, at least past the writer probe's
/// one-second reuse, so a change in the database is seen.
async fn health_until(
    coordinator: &Coordinator,
    what: &str,
    accept: impl Fn(&Value) -> bool,
) -> Result<Value> {
    let started = Instant::now();
    loop {
        let health = coordinator.health().await;
        if accept(&health) {
            return Ok(health);
        }
        ensure!(
            started.elapsed() < DEADLINE,
            "{what}: health never matched; last {health}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Set or reset the fixture database's read-only default from outside it,
/// then end every session in it, so the coordinator's pool reconnects and
/// runs with the new default.
async fn set_read_only(database: &FixtureDatabase, name: &str, read_only: bool) -> Result<()> {
    let change = if read_only {
        "SET default_transaction_read_only = on"
    } else {
        "RESET default_transaction_read_only"
    };
    sqlx::query(&format!("ALTER DATABASE \"{name}\" {change}"))
        .execute(&database.admin)
        .await?;
    sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1")
        .bind(name)
        .execute(&database.admin)
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dual_writer_health_follows_the_own_log_latch_and_the_database_identity() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let database = FixtureDatabase::open(&raw, "prism_d4_readiness_").await?;
    let ledger =
        match Ledger::connect_tool(&database.url, "d4-readiness-fixture".into(), 2, true, None)
            .await
        {
            Ok(ledger) => ledger,
            Err(error) => return Err(database.abandon(error).await),
        };
    let outcome = latch_and_identity(&database, &ledger).await;
    ledger.pool.close().await;
    database.close(outcome).await
}

async fn latch_and_identity(database: &FixtureDatabase, ledger: &Ledger) -> Result<()> {
    let node = FakeNode::open().await?;
    let frontend = frontend(
        database,
        &node,
        "d4-readiness-b",
        Some(dual_writer(NodeIndex::B)),
    )
    .await
    .context("starting the dual-writer frontend")?;

    // No sync attached and no identity: the own log is not caught up, which
    // is reported first, and the database is not yet any node's.
    let health = frontend.health().await;
    ensure!(
        health["ok"] == false && health["ready"] == false,
        "{health}"
    );
    ensure!(health["status"] == "own-log-behind", "{health}");
    ensure!(
        health["dual_writer"]["own_log_caught_up"] == false,
        "{health}"
    );
    ensure!(
        health["dual_writer"]["writer_path"] == "unidentified",
        "{health}"
    );

    // The latch alone is not enough while the database is unidentified.
    let (sync, status) = PeerSyncPublisher::new();
    frontend.peer_sync.set(status).expect("attached once");
    sync.update(|status| {
        status.peer_reachable = true;
        status.own_log_caught_up = true;
    });
    let health = frontend.health().await;
    ensure!(health["ok"] == false, "{health}");
    ensure!(health["status"] == "writer-not-local", "{health}");

    // Personalised as this node: ready, with the contract's object.
    ledger.set_node_identity(NodeIndex::B, "d4-test").await?;
    let health = health_until(&frontend, "personalised as B", |health| {
        health["dual_writer"]["writer_path"] == "local"
    })
    .await?;
    ensure!(health["ok"] == true && health["status"] == "ok", "{health}");
    ensure!(
        health["dual_writer"]
            == json!({
                "node_index": 1,
                "carry_owner": false,
                "own_log_caught_up": true,
                "peer_sync": {"peer_reachable": true, "own_log_caught_up": true, "per_table": {}},
                "writer_path": "local",
            }),
        "{health}"
    );

    // D-8: losing the peer later never withdraws the node.
    sync.update(|status| status.peer_reachable = false);
    let health = frontend.health().await;
    ensure!(health["ok"] == true, "{health}");
    ensure!(
        health["dual_writer"]["peer_sync"]["peer_reachable"] == false,
        "{health}"
    );

    // The database stops taking writes, as a fence's read-only default does.
    let name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&ledger.pool)
        .await?;
    set_read_only(database, &name, true).await?;
    let health = health_until(&frontend, "the database read-only", |health| {
        health["dual_writer"]["writer_path"] == "read_only"
    })
    .await?;
    ensure!(health["ok"] == false, "{health}");
    ensure!(health["status"] == "writer-not-local", "{health}");

    set_read_only(database, &name, false).await?;
    health_until(&frontend, "the database writable again", |health| {
        health["dual_writer"]["writer_path"] == "local" && health["ok"] == true
    })
    .await?;
    frontend.ledger.pool.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_database_personalised_as_the_peer_keeps_the_frontend_out() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let database = FixtureDatabase::open(&raw, "prism_d4_remote_").await?;
    let ledger = match Ledger::connect_tool(
        &database.url,
        "d4-remote-fixture".into(),
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
        // B's frontend whose database is A's: a writer left pointing at
        // the peer.
        ledger.set_node_identity(NodeIndex::A, "d4-test").await?;
        let node = FakeNode::open().await?;
        let frontend = frontend(
            &database,
            &node,
            "d4-remote-b",
            Some(dual_writer(NodeIndex::B)),
        )
        .await?;
        let (sync, status) = PeerSyncPublisher::new();
        frontend.peer_sync.set(status).expect("attached once");
        sync.update(|status| status.own_log_caught_up = true);
        let health = frontend.health().await;
        ensure!(health["ok"] == false, "{health}");
        ensure!(health["status"] == "writer-not-local", "{health}");
        ensure!(health["dual_writer"]["writer_path"] == "remote", "{health}");
        ensure!(health["dual_writer"]["node_index"] == 1, "{health}");
        frontend.ledger.pool.close().await;
        Ok(())
    }
    .await;
    ledger.pool.close().await;
    database.close(outcome).await
}

/// Under heavy share load every ledger connection can be busy. The writer
/// probe and the health reads run on the frontend's own small health pool,
/// so a busy but healthy database still answers them in time and the node is
/// not withdrawn as `unanswered`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_saturated_ledger_pool_leaves_the_writer_local() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let database = FixtureDatabase::open(&raw, "prism_d4_saturated_").await?;
    let ledger =
        match Ledger::connect_tool(&database.url, "d4-saturated-fixture".into(), 2, true, None)
            .await
        {
            Ok(ledger) => ledger,
            Err(error) => return Err(database.abandon(error).await),
        };
    let outcome = async {
        ledger.set_node_identity(NodeIndex::B, "d4-test").await?;
        let node = FakeNode::open().await?;
        let frontend = frontend(
            &database,
            &node,
            "d4-saturated-b",
            Some(dual_writer(NodeIndex::B)),
        )
        .await?;
        let (sync, status) = PeerSyncPublisher::new();
        frontend.peer_sync.set(status).expect("attached once");
        sync.update(|status| status.own_log_caught_up = true);
        health_until(&frontend, "a local writer", |health| {
            health["dual_writer"]["writer_path"] == "local"
        })
        .await?;
        // Every ledger connection busy, as under saturated share appends.
        let pool = &frontend.ledger.pool;
        let mut held = Vec::new();
        for _ in 0..pool.options().get_max_connections() {
            held.push(
                tokio::time::timeout(Duration::from_secs(20), pool.acquire())
                    .await
                    .context("holding a ledger connection")??,
            );
        }
        ensure!(
            tokio::time::timeout(Duration::from_millis(200), pool.acquire())
                .await
                .is_err(),
            "the ledger pool still had a connection to give"
        );
        // Past the probe's one-second reuse, so this health read probes.
        tokio::time::sleep(Duration::from_millis(1200)).await;
        let health = tokio::time::timeout(Duration::from_secs(3), frontend.health())
            .await
            .context("a health read waited on the saturated ledger pool")?;
        ensure!(health["dual_writer"]["writer_path"] == "local", "{health}");
        ensure!(
            health["status"] != "writer-not-local" && health["status"] != "own-log-behind",
            "{health}"
        );
        drop(held);
        frontend.ledger.pool.close().await;
        Ok(())
    }
    .await;
    ledger.pool.close().await;
    database.close(outcome).await
}

/// Stratum's `mining.get_health` reads health too, and any client can send
/// it in a loop. In dual mode every caller shares one health refresh: a
/// burst of 50 at once runs one, so the health pool is never starved and the
/// writer stays local.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_burst_of_health_reads_shares_one_refresh() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let database = FixtureDatabase::open(&raw, "prism_d4_burst_").await?;
    let ledger =
        match Ledger::connect_tool(&database.url, "d4-burst-fixture".into(), 2, true, None).await {
            Ok(ledger) => ledger,
            Err(error) => return Err(database.abandon(error).await),
        };
    let outcome = async {
        ledger.set_node_identity(NodeIndex::B, "d4-test").await?;
        let node = FakeNode::open().await?;
        let frontend = frontend(
            &database,
            &node,
            "d4-burst-b",
            Some(dual_writer(NodeIndex::B)),
        )
        .await?;
        let (sync, status) = PeerSyncPublisher::new();
        frontend.peer_sync.set(status).expect("attached once");
        sync.update(|status| status.own_log_caught_up = true);
        health_until(&frontend, "a local writer", |health| {
            health["dual_writer"]["writer_path"] == "local"
        })
        .await?;
        let burst = async |frontend: &Arc<Coordinator>| -> Result<()> {
            let mut calls = tokio::task::JoinSet::new();
            for _ in 0..50 {
                let frontend = frontend.clone();
                calls.spawn(async move { frontend.health().await });
            }
            while let Some(health) = calls.join_next().await {
                let health = health?;
                ensure!(health["dual_writer"]["writer_path"] == "local", "{health}");
            }
            Ok(())
        };
        // Past the reuse window, so the burst's first caller refreshes.
        tokio::time::sleep(Duration::from_millis(1200)).await;
        let before = frontend
            .dual_writer_health_refreshes()
            .context("a dual-writer frontend counts its refreshes")?;
        let started = Instant::now();
        burst(&frontend).await?;
        ensure!(
            started.elapsed() < Duration::from_secs(3),
            "the burst took {:?}",
            started.elapsed()
        );
        let after = frontend.dual_writer_health_refreshes().unwrap_or_default();
        ensure!(
            after - before == 1,
            "50 health reads ran {} refreshes",
            after - before
        );
        // A second burst right after reuses that refresh, or runs at most one
        // more if the window has passed.
        burst(&frontend).await?;
        let again = frontend.dual_writer_health_refreshes().unwrap_or_default();
        ensure!(
            again - after <= 1,
            "a second burst ran {} refreshes",
            again - after
        );
        frontend.ledger.pool.close().await;
        Ok(())
    }
    .await;
    ledger.pool.close().await;
    database.close(outcome).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_single_writer_health_carries_no_dual_writer_state() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let database = FixtureDatabase::open(&raw, "prism_d4_single_").await?;
    let ledger = match Ledger::connect_tool(
        &database.url,
        "d4-single-fixture".into(),
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
        let node = FakeNode::open().await?;
        let frontend = frontend(&database, &node, "d4-single", None).await?;
        // A latch reported to a single writer changes nothing.
        let (_sync, status) = PeerSyncPublisher::new();
        frontend.peer_sync.set(status).expect("attached once");
        let health = frontend.health().await;
        ensure!(health["ok"] == true && health["status"] == "ok", "{health}");
        ensure!(health.get("dual_writer").is_none(), "{health}");
        ensure!(health.get("admission").is_none(), "{health}");
        frontend.ledger.pool.close().await;
        Ok(())
    }
    .await;
    ledger.pool.close().await;
    database.close(outcome).await
}
