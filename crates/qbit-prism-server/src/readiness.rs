//! Live node checks shared by work publication and settlement observations.
use crate::rpc::Rpc;
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Public nodes must have downloaded every known header and retain the
/// configured peer floor. Regtest permits absent header metadata and no peers.
pub async fn chain_info(rpc: &Rpc, chain: &str, min_peers: u64) -> Result<Value> {
    let info = rpc.call("getblockchaininfo", json!([])).await?;
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
