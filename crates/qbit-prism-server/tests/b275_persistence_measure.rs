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
        support::run(&raw, frontends, |fixture| {
            support::delivery(fixture, 8).boxed_local()
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
    support::run(&raw, 1, |fixture| {
        support::landing_lock(fixture).boxed_local()
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_revision_fence_survives_lock_wait() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    support::run(&raw, 1, |fixture| {
        support::revision_fence(fixture).boxed_local()
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "coordinate an uncontended disposable PostgreSQL16 measurement slot first"]
async fn measure_2000_sessions_one_and_two_frontends() -> Result<()> {
    let raw = gate::required_database_url(gate::site!())?;
    // Separate schemas and fully closed listeners/pools between topologies.
    for frontends in [1, 2] {
        support::run(&raw, frontends, |fixture| {
            support::delivery(fixture, 2_000).boxed_local()
        })
        .await?;
    }
    Ok(())
}
