//! Measurement first: these tests do not assert that the one-second target is met.
use anyhow::{Context, Result};
use futures_util::FutureExt;
use qbit_prism_test_gate as gate;

#[path = "support/b275_persistence_measure/mod.rs"]
mod support;

fn control(name: &str, default: &str) -> Result<(String, &'static str)> {
    match std::env::var(name) {
        Ok(value) => Ok((value, "environment")),
        Err(std::env::VarError::NotPresent) => Ok((default.into(), "default")),
        Err(error) => Err(error).with_context(|| format!("invalid {name}")),
    }
}

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
    let (shares, shares_source) = control("PRISM_B275_WINDOW_SHARES", "16")?;
    let shares: u64 = shares.parse().context("invalid PRISM_B275_WINDOW_SHARES")?;
    // The historical fixture and the two requested qualification scales only.
    anyhow::ensure!(
        [16, 400_000, 500_000].contains(&shares),
        "unsupported window size"
    );
    let (order, order_source) = control("PRISM_B275_FRONTEND_ORDER", "1,2")?;
    let order: &[usize] = match order.as_str() {
        "1,2" => &[1, 2],
        "2,1" => &[2, 1],
        "1" => &[1],
        "2" => &[2],
        _ => anyhow::bail!("frontend order must be 1,2 / 2,1 / 1 / 2"),
    };
    let mut completed = Vec::new();
    let scope = serde_json::json!({"frontend_order":order,"fixture_shares":shares,
        "window_shares_source":shares_source,"frontend_order_source":order_source,
        "single_topology_run":order.len()==1,"sessions_total_per_topology":2000});
    println!("B275_RUN {scope}");
    // Separate schemas and fully closed listeners/pools between topologies.
    for (position, &frontends) in order.iter().enumerate() {
        let mut context = scope.clone();
        context["topology_position"] = position.into();
        let result = support::run_window(&raw, frontends, shares, context, |fixture, deadline| {
            support::delivery(fixture, 2_000, deadline).boxed_local()
        })
        .await;
        if result.is_ok() {
            completed.push(frontends);
        }
        println!(
            "B275_RUN_PROGRESS {}",
            serde_json::json!({"scope":scope,
            "topologies_completed":completed,"current_topology":frontends,
            "current_case_succeeded":result.is_ok(),"both_topologies_completed":completed.contains(&1)&&completed.contains(&2),
            "performance_acceptance":"use each case measurement and durable verification; process success is not the one-second gate"})
        );
        result?;
    }
    Ok(())
}
