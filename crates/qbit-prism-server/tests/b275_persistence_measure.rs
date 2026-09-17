//! Measurement first: these tests do not assert that the one-second target is met.
use anyhow::Result;
use futures_util::FutureExt;
use qbit_prism_test_gate as gate;

#[path = "support/b275_persistence_measure/mod.rs"]
mod support;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_public_delivery_measurement() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    for frontends in [1, 2] {
        support::run(&raw, frontends, |fixture, deadline| {
            support::delivery(fixture, 8, deadline).boxed_local()
        })
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_second_landing_lock_baseline() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    support::run(&raw, 1, |fixture, _| {
        support::landing_lock(fixture).boxed_local()
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_revision_fence_survives_lock_wait() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    support::run(&raw, 1, |fixture, _| {
        support::revision_fence(fixture).boxed_local()
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retried_delivery_is_not_reported_as_failure_free() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    support::run(&raw, 1, |fixture, deadline| {
        support::retry_attribution(fixture, deadline).boxed_local()
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "coordinate an uncontended disposable PostgreSQL16 measurement slot first"]
async fn measure_2000_sessions_one_and_two_frontends() -> Result<()> {
    let raw = gate::required_database_url(gate::site!())?;
    // Measurement controls only: the session count and original deadlines stay fixed.
    let shares = std::env::var("PRISM_B275_WINDOW_SHARES").unwrap_or_else(|_| "16".into());
    let shares: u64 = shares.parse()?;
    anyhow::ensure!(
        [16, 400_000, 500_000].contains(&shares),
        "unsupported window size"
    );
    let order = std::env::var("PRISM_B275_FRONTEND_ORDER").unwrap_or_else(|_| "1,2".into());
    let order: &[usize] = match order.as_str() {
        "1,2" => &[1, 2],
        "2,1" => &[2, 1],
        "1" => &[1],
        "2" => &[2],
        _ => anyhow::bail!("frontend order must be 1,2 / 2,1 / 1 / 2"),
    };
    // Separate schemas and fully closed listeners/pools between topologies.
    for &frontends in order {
        support::run_window(&raw, frontends, shares, |fixture, deadline| {
            support::delivery(fixture, 2_000, deadline).boxed_local()
        })
        .await?;
    }
    Ok(())
}
