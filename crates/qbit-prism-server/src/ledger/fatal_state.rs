//! Explicit operator recovery. The ordinary write guard never ignores a halt.
use super::*;
use crate::{config::Config, rpc::Rpc};
use serde_json::json;
use std::time::Duration;

impl Ledger {
    pub async fn fatal_state(&self) -> Result<Value> {
        // to_jsonb also works before migration 010: an unavailable timestamp is
        // unknown, rather than the cluster's unrelated last-update timestamp.
        let mut state: Value = sqlx::query_scalar(
            "SELECT jsonb_build_object('fatal_error', fatal_error, \
             'set_at', to_jsonb(c)->'fatal_error_set_at') \
             FROM qbit_prism_cluster c WHERE singleton",
        )
        .fetch_one(&self.pool)
        .await?;
        let (block, fanout) = fatal_subject(state["fatal_error"].as_str());
        state["block_hash"] = json!(block);
        state["fanout_txid"] = json!(fanout);
        state["halted"] = json!(!state["fatal_error"].is_null());
        state["schema"] = json!("qbit.prism.fatal-state.v1");
        Ok(state)
    }

    pub async fn clear_fatal_state(&self, config: &Config, reason: &str) -> Result<Value> {
        ensure!(
            !reason.trim().is_empty() && reason.len() <= 4096,
            "--reason must contain 1 to 4096 bytes of nonblank text"
        );
        // Bound the whole operation, including cumulative RPC time while locks
        // are held. Cancellation before COMMIT rolls back. Once COMMIT has
        // been sent, a lost response must be resolved from the durable event.
        tokio::time::timeout(
            Duration::from_secs(120),
            self.clear_fatal_state_in(config, reason),
        )
        .await
        .context("fatal-state recovery exceeded 120 seconds; inspect fatal-state show and recovery events before retrying")?
    }

    async fn clear_fatal_state_in(&self, config: &Config, reason: &str) -> Result<Value> {
        let mut tx = self.begin().await?;
        self.lock(&mut tx, SETTLEMENT_LOCK).await?;
        self.lock(&mut tx, ORDER_LOCK).await?;
        // Block heartbeat updates AND new registrations, not just existing
        // rows. A concurrent startup cannot pass an instance scan unseen.
        sqlx::query(
            "LOCK TABLE qbit_prism_instances, qbit_ledger_writer_lease IN SHARE ROW EXCLUSIVE MODE",
        )
        .execute(&mut *tx)
        .await?;
        let migrated: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM qbit_prism_schema_migrations WHERE version=10)",
        )
        .fetch_one(&mut *tx)
        .await?;
        ensure!(
            migrated,
            "fatal-state recovery requires migration 010; run qbit-prism-server migrate first"
        );
        let row = sqlx::query(
            "SELECT fatal_error,fatal_error_set_at,config_fingerprint \
             FROM qbit_prism_cluster WHERE singleton FOR UPDATE",
        )
        .fetch_one(&mut *tx)
        .await?;
        let fatal: String = row
            .try_get::<Option<String>, _>("fatal_error")?
            .context("cluster has no fatal state to clear")?;
        let set_at: Option<DateTime<Utc>> = row.try_get("fatal_error_set_at")?;
        let instances: Vec<Value> = sqlx::query_scalar(
            "SELECT to_jsonb(i) FROM qbit_prism_instances i ORDER BY instance_id",
        )
        .fetch_all(&mut *tx)
        .await?;
        let offenders: Vec<&str> = instances
            .iter()
            .filter(|instance| {
                !matches!(
                    instance["status"]["state"].as_str(),
                    Some("drained" | "stopped")
                )
            })
            .map(|instance| instance["instance_id"].as_str().unwrap_or("<unknown>"))
            .collect();
        ensure!(offenders.is_empty(),
            "fatal-state clear requires every instance to report drained or stopped; offending instances: {}",
            offenders.join(", "));
        let legacy: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM qbit_ledger_writer_lease WHERE lease_expires_at>clock_timestamp())",
        ).fetch_one(&mut *tx).await?;
        ensure!(!legacy, "live legacy Python writer lease");

        let rpc = Rpc::new(
            config.rpc_url.clone(),
            config.rpc_user.clone(),
            config.rpc_password.clone(),
            config.rpc_timeout,
        )?;
        let genesis = rpc.call("getblockhash", json!([0])).await?;
        let genesis = genesis.as_str().context("qbit genesis hash missing")?;
        config.verify_genesis(genesis)?;
        let mut resolved = config.clone();
        // Startup resolves the fee address into its script before computing
        // this fingerprint. Validate it without starting workers.
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
        let saved: Option<String> = row.try_get("config_fingerprint")?;
        ensure!(saved.as_deref() == Some(&resolved.fingerprint(genesis)?),
            "cluster configuration fingerprint mismatch or missing; use the cluster's configuration");
        let chain = crate::readiness::chain_info(&rpc, &config.chain, config.min_peers).await?;
        let expected_chain = match config.chain.as_str() {
            "mainnet" => "main",
            "testnet" | "testnet3" => "test",
            other => other,
        };
        ensure!(
            chain["chain"].as_str() == Some(expected_chain),
            "configured QBIT_CHAIN differs from connected node"
        );
        let work = num_bigint::BigUint::parse_bytes(
            chain["chainwork"]
                .as_str()
                .context("qbit chainwork missing")?
                .as_bytes(),
            16,
        )
        .context("invalid qbit chainwork")?;
        let caught_up: bool = sqlx::query_scalar(
            "SELECT $1::text::numeric>=best_chainwork FROM qbit_prism_cluster WHERE singleton",
        )
        .bind(work.to_str_radix(10))
        .fetch_one(&mut *tx)
        .await?;
        ensure!(
            caught_up,
            "local node is behind the cluster's cumulative chainwork"
        );
        let tip = chain["bestblockhash"]
            .as_str()
            .context("qbit tip hash missing")?;
        let height = chain["blocks"]
            .as_u64()
            .context("qbit tip height missing")?;

        // Recovery does a fresh scan, including ALL mature checkpoints. The
        // ordinary observer's incremental cache is deliberately not involved.
        let blocks = sqlx::query("SELECT block_hash,block_height,maturity_state FROM qbit_pool_blocks WHERE chain_state IN ('prepared','confirmed','inactive') AND maturity_state IN ('immature','mature') ORDER BY block_height,block_hash")
            .fetch_all(&mut *tx).await?;
        let mut observations = Vec::with_capacity(blocks.len());
        for block in &blocks {
            let hash: String = block.try_get("block_hash")?;
            let block_height = u64::try_from(block.try_get::<i64, _>("block_height")?)?;
            let mature = block.try_get::<String, _>("maturity_state")? == "mature";
            ensure!(
                !mature || block_height <= height,
                "node is behind mature pool block {hash} at height {block_height}"
            );
            let active = block_height <= height
                && rpc.call("getblockhash", json!([block_height])).await? == json!(hash);
            observations.push(BlockObservation {
                block_hash: hash,
                active,
            });
        }
        let fanouts = sqlx::query("SELECT fanout_txid,confirmed_block_hash,confirmed_block_height FROM qbit_ctv_fanout_artifacts WHERE settlement_status='confirmed' AND confirmed_depth>=1000 ORDER BY fanout_txid")
            .fetch_all(&mut *tx).await?;
        for fanout in &fanouts {
            let txid: String = fanout.try_get("fanout_txid")?;
            let hash: String = fanout
                .try_get::<Option<String>, _>("confirmed_block_hash")?
                .with_context(|| format!("deep confirmed fanout {txid} lacks a block hash"))?;
            let at = u64::try_from(
                fanout
                    .try_get::<Option<i64>, _>("confirmed_block_height")?
                    .with_context(|| format!("deep confirmed fanout {txid} lacks a height"))?,
            )?;
            ensure!(
                at <= height,
                "node is behind deep confirmed fanout {txid} at height {at}"
            );
            ensure!(rpc.call("getblockhash", json!([at])).await? == json!(hash),
                "deep confirmed CTV fanout remains disconnected: {txid} at height {at}; reconcile before clearing");
        }
        ensure!(
            rpc.call("getbestblockhash", json!([])).await? == json!(tip),
            "tip changed during fatal-state reconciliation"
        );
        // Reuse precisely the normal accounting transitions in this same
        // transaction. A recurring fatal result is rolled back with the rest.
        if let Some(error) = self
            .reconcile_blocks_in(&mut tx, &observations, height)
            .await?
        {
            bail!("reconciliation refused recovery: {error}");
        }
        let integrity: Value = sqlx::query_scalar("SELECT qbit_carry_forward_integrity_report()")
            .fetch_one(&mut *tx)
            .await?;
        for field in ["mismatch_count", "current_drift_count"] {
            ensure!(
                integrity[field].as_u64() == Some(0),
                "fatal-state reconciliation failed {field}: {integrity}"
            );
        }
        ensure!(
            rpc.call("getbestblockhash", json!([])).await? == json!(tip),
            "tip changed before fatal-state recovery commit"
        );
        let reconciliation = json!({"genesis_hash":genesis,"tip_hash":tip,"tip_height":height,
            "blocks_checked":blocks.len(),"deep_fanouts_checked":fanouts.len(),"integrity":integrity});
        // Even a recovery with no changed payout rows invalidates old jobs and
        // chain observations collected before the operator's decision.
        sqlx::query("UPDATE qbit_prism_cluster SET fatal_error=NULL,payout_revision=payout_revision+1,updated_at=clock_timestamp() WHERE singleton")
            .execute(&mut *tx).await?;
        let event: Value = sqlx::query_scalar(
            "INSERT INTO qbit_prism_fatal_state_events(reason,fatal_error,fatal_error_set_at,instances,reconciliation) \
             VALUES($1,$2,$3,$4,$5) RETURNING to_jsonb(qbit_prism_fatal_state_events)",
        ).bind(reason).bind(fatal).bind(set_at).bind(json!(instances)).bind(reconciliation)
            .fetch_one(&mut *tx).await?;
        tx.commit().await.context("fatal-state recovery commit failed; outcome may be unknown; inspect fatal-state show and recovery events before retrying")?;
        Ok(event)
    }
}

fn fatal_subject(message: Option<&str>) -> (Option<String>, Option<String>) {
    let subject = |prefix| {
        message
            .and_then(|message| message.strip_prefix(prefix))
            .and_then(|rest| rest.split(';').next())
            .map(str::trim)
            .map(str::to_owned)
    };
    (
        subject("mature pool block disconnected: "),
        subject("deep confirmed CTV fanout disconnected: "),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_both_fatal_subjects_without_requiring_the_new_suffix() {
        assert_eq!(
            fatal_subject(Some(
                "mature pool block disconnected: abc; manual reconciliation required"
            )),
            (Some("abc".into()), None)
        );
        assert_eq!(fatal_subject(Some("deep confirmed CTV fanout disconnected: def; manual reconciliation required; after investigation run qbit-prism-server fatal-state clear --reason <text>")), (None, Some("def".into())));
        assert_eq!(fatal_subject(None), (None, None));
        assert_eq!(fatal_subject(Some("unknown halt")), (None, None));
    }
}
