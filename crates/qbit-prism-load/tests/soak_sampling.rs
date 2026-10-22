//! The per-PR soak check (#575 item 2), in seconds against the PR shard's
//! PostgreSQL: the soak's database sampler reads a fresh, migrated PRISM
//! schema; its process sampler reads this test process; the soak's forced
//! rollover advances the schema's share sequence to below its partition's
//! bound; and the soak gates, held to those real samples, pass on the
//! crossing and fail when asked for one more than happened.
//!
//! The four-minute end-to-end soak, with live frontends, retention and the
//! archive restore, is the nightly opt-in test `soak_smoke`.

use anyhow::{ensure, Context, Result};
use qbit_prism_load::{gate, soak, soak_driver};
use qbit_prism_server::ledger::Ledger;
use qbit_prism_test_gate as test_gate;
use sqlx::PgPool;

fn sample(elapsed_seconds: f64, database: &soak::DatabasePoint) -> soak::Sample {
    soak::Sample {
        schema: soak::SAMPLE_SCHEMA.into(),
        at: chrono::Utc::now(),
        elapsed_seconds,
        phase: None,
        steady: true,
        processes: vec![soak::process_point(
            "soak-sampling",
            Some(std::process::id()),
        )],
        database: database.clone(),
        latency: None,
        ledger: None,
    }
}

fn gates(min_rollovers: u64) -> soak::Gates {
    // Trends of a test process over seconds are not what this test holds;
    // it holds that the gates read real samples and decide on them.
    soak::Gates {
        warmup_minutes: 0.0,
        min_gated_samples: 3,
        rss_slope_mib_per_hour_max: 1e9,
        rss_trend_window_minutes: 20.0,
        min_trend_windows: 3,
        rss_warmup_peak_multiple_max: None,
        rss_expected_failure: None,
        fd_slope_per_hour_max: 1e9,
        pool_connections_max: 1000,
        pool_connections_drift_max: 1000.0,
        wal_bytes_max: u64::MAX,
        min_rollovers,
        min_archive_cycles: 0,
        one_lifetime: true,
    }
}

#[tokio::test]
async fn soak_samples_a_prism_database_and_a_process_rolls_over_and_gates() -> Result<()> {
    let Some(raw) = test_gate::database_url(test_gate::site!())? else {
        return Ok(());
    };
    let admin = PgPool::connect(&raw).await?;
    let schema = format!("prism_soak_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await?;
    let separator = if raw.contains('?') { '&' } else { '?' };
    let url =
        format!("{raw}{separator}options=-csearch_path%3D{schema}&application_name=soak-sampling");
    let result = run(&url).await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await?;
    admin.close().await;
    result
}

async fn run(url: &str) -> Result<()> {
    let ledger = Ledger::connect(url, "soak-sampling".into(), 4, true).await?;
    let pool = &ledger.pool;
    // Only this test's own connections: the shard's database is shared with
    // whatever else the shard runs, whose sessions this test does not own.
    let own = vec!["soak-sampling".to_owned()];

    // Every part of the database sample reads on a fresh ledger: nothing is
    // unknown, and a fresh ledger's zeros are real zeros.
    let first = soak::database_point(pool, Some(&own)).await;
    ensure!(first.unknown.is_empty(), "unknown: {:?}", first.unknown);
    ensure!(first.wal_bytes.is_some_and(|bytes| bytes > 0), "{first:?}");
    ensure!(first.connections.is_some(), "{first:?}");
    ensure!(first.idle_in_transaction_over_60s == Some(0), "{first:?}");
    ensure!(first.payout_divergences == Some(0), "{first:?}");
    ensure!(
        first.relations.contains_key("qbit_prism_share_partitions"),
        "{:?}",
        first.relations
    );
    let attached = first
        .partitions
        .iter()
        .filter(|p| p.state == "attached")
        .count();
    ensure!(attached >= 2, "the release partition and a lead: {first:?}");
    let next = first.next_share_seq.context("next_share_seq")?;

    // The rollover leaves the sequence just below the bound it is under.
    let bound = first
        .partitions
        .iter()
        .find(|p| p.lower_seq.is_none_or(|lower| lower <= next) && p.upper_seq > next)
        .map(|p| p.upper_seq)
        .context("the partition the sequence is in")?;
    let detail = soak_driver::rollover(pool, 100).await?;
    ensure!(detail["bound"] == bound, "{detail}");
    ensure!(detail["to_next_share_seq"] == bound - 100, "{detail}");
    let rolled = soak::database_point(pool, Some(&own)).await;
    ensure!(rolled.next_share_seq == Some(bound - 100), "{rolled:?}");
    // It never moves the sequence down, and refuses to roll again while the
    // load has not yet crossed the bound it was brought to.
    ensure!(
        soak_driver::rollover(pool, 1000).await.is_err(),
        "a second rollover inside the margin is refused"
    );
    // What the live load does next: draw past the bound.
    sqlx::query("SELECT nextval('qbit_share_ledger_share_seq_seq') FROM generate_series(1, 150)")
        .execute(pool)
        .await?;
    let crossed = soak::database_point(pool, Some(&own)).await;
    ensure!(
        crossed.next_share_seq.is_some_and(|n| n > bound),
        "{crossed:?}"
    );

    // Four samples an hour apart in total, a real process each time.
    let samples = vec![
        sample(0.0, &first),
        sample(1200.0, &first),
        sample(2400.0, &crossed),
        sample(3600.0, &crossed),
    ];
    let process = &samples[0].processes[0];
    ensure!(
        process.rss_bytes.is_some() && process.open_fds.is_some() && process.threads.is_some(),
        "{process:?}"
    );
    let checks = soak::evaluate(&samples, &gates(1));
    let table = gate::markdown("soak sampling", &checks);
    ensure!(gate::passed(&checks), "{table}");
    let rollovers = checks
        .iter()
        .find(|check| check.name == "share partition rollovers")
        .context("the rollover gate")?;
    ensure!(
        rollovers.observed.starts_with("1 bound(s) crossed"),
        "{table}"
    );
    // The same real samples fail a soak that asked for two rollovers.
    let asked_more = soak::evaluate(&samples, &gates(2));
    ensure!(
        !gate::passed(&asked_more),
        "{}",
        gate::markdown("soak sampling, two rollovers asked", &asked_more)
    );
    ledger.pool.close().await;
    Ok(())
}
