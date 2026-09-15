//! Offline fee-policy transitions. There is deliberately no force/live mode.
use super::*;
use crate::{config::Config, rpc::Rpc};
use serde_json::json;
use std::time::Duration;

impl Ledger {
    pub async fn transition_policy(&self, current: &Config, target: &Config) -> Result<Value> {
        ensure!(
            current.database_url == target.database_url,
            "policy-transition cannot change PRISM_DATABASE_URL"
        );
        tokio::time::timeout(Duration::from_secs(120), self.transition_policy_in(current, target))
            .await.context("policy transition exceeded 120 seconds; inspect qbit_prism_policy_transitions before retrying")?
    }

    async fn transition_policy_in(&self, current: &Config, target: &Config) -> Result<Value> {
        let (old, genesis) = resolved_policy(current, false).await?;
        let (new, new_genesis) = resolved_policy(target, true).await?;
        ensure!(
            genesis == new_genesis,
            "policy-transition cannot change genesis"
        );
        let previous_policy = old.policy_document(&genesis)?;
        let policy = new.policy_document(&genesis)?;
        validate_change(&previous_policy, &policy)?;
        let previous = old.fingerprint(&genesis)?;
        let next = new.fingerprint(&genesis)?;

        let mut tx = self.begin().await?;
        self.lock(&mut tx, SETTLEMENT_LOCK).await?;
        self.lock(&mut tx, ORDER_LOCK).await?;
        // Also exclude registrations and heartbeats racing the stopped scan.
        sqlx::query(
            "LOCK TABLE qbit_prism_instances, qbit_ledger_writer_lease IN SHARE ROW EXCLUSIVE MODE",
        )
        .execute(&mut *tx)
        .await?;
        writable(&mut tx).await?;
        let (saved, revision): (Option<String>, i64) = sqlx::query_as(
            "SELECT config_fingerprint,payout_revision FROM qbit_prism_cluster WHERE singleton FOR UPDATE")
            .fetch_one(&mut *tx).await?;
        ensure!(saved.as_deref() == Some(&previous),
            "current policy fingerprint does not match the cluster at payout revision {revision}; use its current configuration and inspect qbit_prism_policy_transitions before retrying");
        let instances: Vec<Value> = sqlx::query_scalar(
            "SELECT to_jsonb(i) FROM qbit_prism_instances i ORDER BY instance_id",
        )
        .fetch_all(&mut *tx)
        .await?;
        let live: Vec<&str> = instances
            .iter()
            .filter(|i| {
                !matches!(
                    serde_json::from_value::<HeartbeatStatus>(i["status"].clone()),
                    Ok(HeartbeatStatus::Stopped)
                )
            })
            .map(|i| i["instance_id"].as_str().unwrap_or("<unknown>"))
            .collect();
        ensure!(
            live.is_empty(),
            "policy-transition requires every frontend stopped; offending instances: {}",
            live.join(", ")
        );
        // An unusual pending row with an already-landed audit needs explicit
        // reconciliation; never alter existing accounting to discard it.
        let landed: Vec<String> = sqlx::query_scalar(
            "SELECT o.block_hash FROM qbit_block_candidate_outbox o JOIN qbit_pool_blocks b USING(block_hash) WHERE o.state='pending' ORDER BY o.block_hash")
            .fetch_all(&mut *tx).await?;
        ensure!(
            landed.is_empty(),
            "reconcile pending candidates with landed blocks before policy-transition: {}",
            landed.join(", ")
        );
        let abandoned = sqlx::query(
            "UPDATE qbit_block_candidate_outbox SET state='abandoned',candidate=NULL,block_bytes=NULL,window_anchor_ms=NULL,window_prior_balances_sha256=NULL,window_first_share_seq=NULL,window_last_share_seq=NULL,window_share_count=NULL,window_snapshot_sha256=NULL,completed_at=clock_timestamp(),updated_at=clock_timestamp(),last_error='epoch-superseded',claim_token=NULL,claim_instance_id=NULL,claim_expires_at=NULL WHERE state='pending'")
            .execute(&mut *tx).await?.rows_affected();
        // Offered, reserved and reconciliation rows keep their as-issued
        // policy, keys, bytes and state. Fence old tokens and allow recovery
        // immediately after restart, without waiting for a departed owner.
        // Parked rows still require their existing operator recovery procedure.
        let retained = sqlx::query(
            "UPDATE qbit_block_candidate_outbox SET claim_token=NULL,claim_instance_id=NULL,claim_expires_at=NULL,next_attempt_at=CASE WHEN next_attempt_at='infinity'::timestamptz THEN next_attempt_at ELSE clock_timestamp() END,updated_at=clock_timestamp() WHERE state IN ('offer_reserved','offered','reconciliation')")
            .execute(&mut *tx).await?.rows_affected();
        let next_revision: i64 = sqlx::query_scalar(
            "UPDATE qbit_prism_cluster SET config_fingerprint=$1,payout_revision=payout_revision+1,updated_at=clock_timestamp() WHERE singleton RETURNING payout_revision")
            .bind(&next).fetch_one(&mut *tx).await?;
        let event: Value = sqlx::query_scalar(
            "INSERT INTO qbit_prism_policy_transitions(previous_fingerprint,config_fingerprint,previous_policy,policy,previous_revision,payout_revision,instances,abandoned_candidates,retained_candidates) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING to_jsonb(qbit_prism_policy_transitions)")
            .bind(previous).bind(next).bind(previous_policy).bind(policy)
            .bind(revision).bind(next_revision).bind(json!(instances))
            .bind(i64::try_from(abandoned)?).bind(i64::try_from(retained)?)
            .fetch_one(&mut *tx).await?;
        tx.commit().await.context("policy transition commit failed; outcome may be unknown; inspect qbit_prism_policy_transitions before retrying")?;
        Ok(event)
    }
}

fn validate_change(previous: &Value, target: &Value) -> Result<()> {
    ensure!(previous != target, "target policy is unchanged");
    let fixed = |policy: &Value| {
        let mut fixed = policy.clone();
        fixed["payout_policy"]
            .as_object_mut()
            .unwrap()
            .remove("pool_fee_policy");
        let object = fixed.as_object_mut().unwrap();
        object.remove("ctv_fee");
        object.remove("ctv_auto_fee_premium_bps");
        fixed
    };
    ensure!(fixed(previous) == fixed(target),
        "only pool-fee and CTV fee-rate changes are supported; signing keys, genesis and all other policy settings must stay unchanged");
    Ok(())
}

// Match the normal startup's address resolution before hashing either policy.
async fn resolved_policy(config: &Config, check_fee: bool) -> Result<(Config, String)> {
    let rpc = Rpc::new(
        config.rpc_url.clone(),
        config.rpc_user.clone(),
        config.rpc_password.clone(),
        config.rpc_timeout,
    )?;
    let genesis = rpc.call("getblockhash", json!([0])).await?;
    let genesis = genesis
        .as_str()
        .context("qbit genesis hash missing")?
        .to_owned();
    config.verify_genesis(&genesis)?;
    let info = rpc.call("getblockchaininfo", json!([])).await?;
    let chain = match config.chain.as_str() {
        "mainnet" => "main",
        "testnet" => "test",
        other => other,
    };
    ensure!(
        info["chain"].as_str() == Some(chain),
        "configured QBIT_CHAIN differs from connected node"
    );
    let mut resolved = config.clone();
    if let Some(address) = &config.fee_address {
        let validation = rpc.call("validateaddress", json!([address])).await?;
        let script = validation["scriptPubKey"]
            .as_str()
            .context("pool fee address has no script")?;
        ensure!(
            validation["isvalid"] == true
                && script.starts_with("5220")
                && hex::decode(script)?.len() == 34,
            "pool fee address must be P2MR"
        );
        resolved
            .payout_policy
            .pool_fee_policy
            .as_mut()
            .context("missing fee policy")?
            .p2mr_program_hex = script[4..].into();
    }
    // The old rate may be exactly what stopped the cluster from starting.
    // Only the target must satisfy the current node's relay/mempool floors.
    if check_fee && config.ctv_enabled {
        crate::coordinator::validated_ctv_fee_policy(
            &rpc,
            config.ctv_fee,
            config.ctv_fee_premium_bps,
        )
        .await?;
    }
    Ok((resolved, genesis))
}
