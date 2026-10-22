//! Live node checks shared by work publication and settlement observations.
use crate::metrics::{Metrics, NodeObservation};
use crate::rpc::Rpc;
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// The same check without a registry, for callers that own no metrics.
pub async fn chain_info(rpc: &Rpc, chain: &str, min_peers: u64) -> Result<Value> {
    chain_info_with_metrics(rpc, chain, min_peers, None).await
}

/// Public nodes must have downloaded every known header and retain the
/// configured peer floor. Regtest permits absent header metadata and no peers.
///
/// This is also the single node observation site. It records what the attempt
/// already fetched and adds no RPC call, so a short-circuited or refused check
/// leaves the values it never learned unknown instead of stale.
pub async fn chain_info_with_metrics(
    rpc: &Rpc,
    chain: &str,
    min_peers: u64,
    metrics: Option<&Metrics>,
) -> Result<Value> {
    let mut observation = NodeObservation::started();
    let result = observe_chain_info(rpc, chain, min_peers, &mut observation).await;
    if let Some(metrics) = metrics {
        metrics.record_node_observation(observation);
    }
    result
}

async fn observe_chain_info(
    rpc: &Rpc,
    chain: &str,
    min_peers: u64,
    observation: &mut NodeObservation,
) -> Result<Value> {
    let info = rpc.call("getblockchaininfo", json!([])).await?;
    // The node answered, so its sync state is known even though this check
    // then refuses a synchronizing node.
    if let Some(initial_block_download) = info["initialblockdownload"].as_bool() {
        observation.chain = Some((initial_block_download, Instant::now()));
    }
    ensure!(
        info["initialblockdownload"] == false,
        "qbit is still synchronizing"
    );
    let public_chain = chain != "regtest";
    if public_chain || (!info["blocks"].is_null() && !info["headers"].is_null()) {
        let blocks = info["blocks"]
            .as_u64()
            .context("qbit did not report a nonnegative integer block height")?;
        let headers = info["headers"]
            .as_u64()
            .context("qbit did not report a nonnegative integer header height")?;
        ensure!(
            blocks == headers,
            "qbit is not caught up: blocks={blocks}, headers={headers}"
        );
    }
    if public_chain {
        ensure!(min_peers > 0, "PRISM_MIN_PEERS must be positive");
        let network = rpc.call("getnetworkinfo", json!([])).await?;
        let peers = network["connections"]
            .as_u64()
            .context("qbit did not report a nonnegative integer peer count")?;
        // A count below the floor is still an answered count, not unknown.
        observation.peers = Some(peers);
        ensure!(
            peers >= min_peers,
            "qbit has {peers} peers, requires at least {min_peers}"
        );
    }
    Ok(info)
}

pub fn validate_template_age(template: &Value, max_age: Duration) -> Result<()> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    validate_template_age_at(template, max_age, now)
}

fn validate_template_age_at(template: &Value, max_age: Duration, now: u64) -> Result<()> {
    let timestamp = template["curtime"]
        .as_u64()
        .context("getblocktemplate.curtime must be a nonnegative integer")?;
    // A future template time is permitted by the legacy readiness policy.
    let age = now.saturating_sub(timestamp);
    ensure!(
        age <= max_age.as_secs(),
        "qbit block template is stale: age={age}s exceeds {}s",
        max_age.as_secs()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn template_age_boundary_zero_and_future_time_match_legacy_policy() {
        let limit = Duration::from_secs(120);
        assert!(validate_template_age_at(&json!({"curtime":880}), limit, 1000).is_ok());
        assert!(validate_template_age_at(&json!({"curtime":879}), limit, 1000).is_err());
        assert!(validate_template_age_at(&json!({"curtime":1001}), Duration::ZERO, 1000).is_ok());
        assert!(validate_template_age_at(&json!({"curtime":1000}), Duration::ZERO, 1000).is_ok());
        assert!(validate_template_age_at(&json!({"curtime":999}), Duration::ZERO, 1000).is_err());
        for value in [Value::Null, json!(-1), json!(1.5), json!("1000")] {
            assert!(validate_template_age_at(&json!({"curtime":value}), limit, 1000).is_err());
        }
    }
}
