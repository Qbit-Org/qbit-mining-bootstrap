//! A job build on a pooled connection whose backend died inside the
//! checkout-probe window fails that one attempt with a Stratum error, and the
//! very next attempt serves work for the same tip: the pool discards the dead
//! connection on release, so recovery does not wait for the next refresh.
//! (The session loop retries a failed delivery on its one-second timer; see
//! `stratum::stale_grace_tests::failed_replacement_keeps_retired_job_until_actual_delivery_then_expires`.)
use anyhow::{ensure, Context, Result};
use qbit_prism_server::{coordinator::Coordinator, metrics::Metrics, stratum::MiningBackend};
use qbit_prism_test_gate as gate;
use std::sync::{Arc, LazyLock};

#[path = "support/fake_qbitd.rs"]
#[allow(dead_code)]
mod fake_qbitd;
#[path = "support/ledger_database.rs"]
#[allow(dead_code)]
mod ledger_database;

/// Read by `Ledger::connect*` from the process environment; forced before any
/// ledger opens so every checkout in this binary is inside the window.
static PROBE_WINDOW: LazyLock<()> =
    LazyLock::new(|| std::env::set_var("PRISM_DATABASE_ACQUIRE_PROBE_IDLE_MS", "600000"));

#[tokio::test]
async fn a_build_on_a_connection_killed_inside_the_probe_window_fails_once_then_serves_the_same_tip(
) -> Result<()> {
    LazyLock::force(&PROBE_WINDOW);
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = ledger_database::FixtureDatabase::open(&raw, "probe_window_").await?;
    let result = async {
        let node = fake_qbitd::FakeNode::open().await?;
        let mut config = fake_qbitd::coordinator_config(db.url.clone(), &node, "probe-window")?;
        // Two slots: one dead connection is a large share of the pool, and the
        // bound below stays small.
        config.database_connections = 2;
        let coordinator = Coordinator::new(config, Arc::new(Metrics::default())).await?;
        coordinator.refresh_once().await?;
        let tip = coordinator
            .prepared
            .read()
            .await
            .as_ref()
            .context("no prepared work after refresh")?
            .template["previousblockhash"]
            .as_str()
            .context("prepared work has no parent")?
            .to_owned();
        let worker = MiningBackend::authorize(&*coordinator, "probe.worker").await?;
        // A build proves the path works and leaves its connections idle.
        let first = MiningBackend::build_job(&*coordinator, &worker, "00000001", 1e-12, 0.0).await?;
        ensure!(first.wire.previousblockhash == tip);

        // Terminate every idle backend of this fixture database from a
        // separate session. Inside the window no checkout probes them.
        let admin = sqlx::PgPool::connect(&db.url).await?;
        let killed: Vec<bool> = sqlx::query_scalar(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND backend_type='client backend' AND state='idle'",
        )
        .fetch_all(&admin)
        .await?;
        ensure!(!killed.is_empty(), "no idle backend to terminate");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let mut failures = 0usize;
        let mut served = None;
        // Each dead connection can fail at most one attempt before the pool
        // discards it, so the bound is the pool size plus one.
        for attempt in 0..=2usize {
            match MiningBackend::build_job(&*coordinator, &worker, "00000002", 1e-12, 0.0).await {
                Ok(job) => {
                    served = Some((attempt, job));
                    break;
                }
                Err(error) => {
                    failures += 1;
                    eprintln!("attempt {attempt} failed inside the probe window: {error}");
                }
            }
        }
        let (attempt, job) = served.context("no attempt served work within the pool-size bound")?;
        ensure!(failures >= 1, "expected at least one failed attempt on a dead idle connection");
        ensure!(attempt <= 2, "recovery took {attempt} attempts");
        ensure!(
            job.wire.previousblockhash == tip,
            "recovered work is for {} not the current tip {tip}",
            job.wire.previousblockhash
        );
        admin.close().await;
        Ok(())
    }
    .await;
    db.close(result).await
}
