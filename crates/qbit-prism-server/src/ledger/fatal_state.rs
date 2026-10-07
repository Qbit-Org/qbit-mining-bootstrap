//! Explicit operator recovery. The ordinary write guard never ignores a halt.
use super::*;
use crate::{
    config::Config,
    rpc::{Rpc, RpcReplyError},
};
use serde_json::json;
use std::time::Duration;

/// `fatal-state clear`'s bound when the operator gives no `--timeout-seconds`.
pub const FATAL_STATE_CLEAR_BOUND: Duration = Duration::from_secs(120);

/// What the bound keeps back from the integrity report for the UPDATE, the
/// event INSERT and the COMMIT. One node RPC timeout is kept on top, for the
/// closing tip check's node calls, so that one slow call cannot push the
/// deadline into the COMMIT, whose outcome would then be unknown.
const COMMIT_HEADROOM: Duration = Duration::from_secs(5);

/// The least time the integrity report is started with. With less of the
/// bound left, the clear refuses instead of starting a report it must cancel.
const MIN_REPORT_TIME: Duration = Duration::from_secs(1);

/// How far above the captured height a new tip may stand for the clear to
/// walk its ancestry back to the captured tip. A clear's bound lets the chain
/// grow by a few dozen blocks at most; a node this far ahead was not caught up.
const MAX_TIP_EXTENSION: u64 = 1_000;

/// The rule every operator's `--reason` follows before it is journaled or
/// written to a row: `fatal-state clear`, `candidates abandon` and
/// `submission-hold set` and `clear` (#664). The journals' CHECKs (010, 023)
/// hold the database to the same bound.
pub fn require_operator_reason(reason: &str) -> Result<()> {
    ensure!(
        !reason.trim().is_empty() && reason.len() <= 4096,
        "--reason must contain 1 to 4096 bytes of nonblank text"
    );
    Ok(())
}

impl Ledger {
    /// Read-only diagnostics remain available before the current migrations.
    /// Recovery writes use connect_operator and its full startup gates.
    pub async fn inspect_fatal_state(url: &str) -> Result<Value> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(15))
            .after_connect(|connection, _| {
                Box::pin(async move {
                    sqlx::query("SELECT set_config('default_transaction_read_only','on',false),set_config('statement_timeout','15s',false),set_config('lock_timeout','5s',false)")
                        .execute(&mut *connection).await?;
                    Ok(())
                })
            })
            .connect(url)
            .await?;
        let state = Self::read_fatal_state(pool.acquire()).await;
        pool.close().await;
        state
    }

    pub async fn fatal_state(&self) -> Result<Value> {
        Self::read_fatal_state(self.acquire()).await
    }

    async fn read_fatal_state(
        acquisition: impl std::future::Future<
            Output = sqlx::Result<sqlx::pool::PoolConnection<Postgres>>,
        >,
    ) -> Result<Value> {
        // to_jsonb also works before migration 010: an unavailable timestamp is
        // unknown, rather than the cluster's unrelated last-update timestamp.
        let mut state: Value = sqlx::query_scalar(
            "SELECT jsonb_build_object('fatal_error', fatal_error, \
             'set_at', to_jsonb(c)->'fatal_error_set_at') \
             FROM qbit_prism_cluster c WHERE singleton",
        )
        .fetch_one(&mut *acquisition.await?)
        .await?;
        let (block, fanout) = fatal_subject(state["fatal_error"].as_str());
        state["block_hash"] = json!(block);
        state["fanout_txid"] = json!(fanout);
        state["halted"] = json!(!state["fatal_error"].is_null());
        state["schema"] = json!("qbit.prism.fatal-state.v1");
        Ok(state)
    }

    /// [`Ledger::clear_fatal_state_within`] under the default 120 s bound.
    pub async fn clear_fatal_state(&self, config: &Config, reason: &str) -> Result<Value> {
        self.clear_fatal_state_within(config, reason, FATAL_STATE_CLEAR_BOUND)
            .await
    }

    /// Reconcile a stopped or drained cluster and durably record why it was
    /// cleared, all within `bound` (`fatal-state clear --timeout-seconds`,
    /// #737). The integrity report's own statement ends by the same deadline.
    pub async fn clear_fatal_state_within(
        &self,
        config: &Config,
        reason: &str,
        bound: Duration,
    ) -> Result<Value> {
        require_operator_reason(reason)?;
        // Bound the whole operation, including cumulative RPC time while locks
        // are held. Cancellation before COMMIT rolls back. Once COMMIT has
        // been sent, a lost response must be resolved from the durable event.
        let deadline = tokio::time::Instant::now()
            .checked_add(bound)
            .context("fatal-state recovery bound is out of range")?;
        tokio::time::timeout_at(deadline, self.clear_fatal_state_in(config, reason, deadline))
            .await
            .with_context(|| format!("fatal-state recovery exceeded {} seconds; inspect fatal-state show and recovery events before retrying", bound.as_secs()))?
    }

    async fn clear_fatal_state_in(
        &self,
        config: &Config,
        reason: &str,
        deadline: tokio::time::Instant,
    ) -> Result<Value> {
        let mut tx = self.begin().await?;
        self.lock(&mut tx, SETTLEMENT_LOCK).await?;
        let _order = self
            .lock_order(&mut tx, crate::metrics::OrderLockHolder::FatalState)
            .await?;
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
        // The loop below calls every pool block above the captured height
        // inactive without asking the node, which only the captured tip
        // keeps true (see `tip_unchanged_or_extended`). Any maturity counts.
        let pool_block_above: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM qbit_pool_blocks WHERE block_height>$1 AND chain_state IN ('prepared','confirmed','inactive'))")
            .bind(i64::try_from(height)?)
            .fetch_one(&mut *tx).await?;
        let mut observations = Vec::with_capacity(blocks.len());
        let mut observed_heights = Vec::with_capacity(blocks.len());
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
            observed_heights.push(block_height);
            observations.push(BlockObservation {
                block_hash: hash,
                active,
            });
        }
        let fanouts = sqlx::query("SELECT fanout_txid,confirmed_block_hash,confirmed_block_height FROM qbit_ctv_fanout_artifacts WHERE settlement_status='confirmed' AND confirmed_depth>=1000 ORDER BY fanout_txid")
            .fetch_all(&mut *tx).await?;
        let mut deep_fanouts = Vec::with_capacity(fanouts.len());
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
            deep_fanouts.push((txid, hash, at));
        }
        // #737: the first tip check. Once the tip has moved, every observation
        // is asked again before the new tip is accepted, so a chain that went
        // over to a fork during the loop and came back onto a longer chain
        // leaves no stale one behind (see `tip_unchanged_or_extended`).
        let observed = Observed {
            heights: &observed_heights,
            blocks: &observations,
            fanouts: &deep_fanouts,
        };
        tip_unchanged_or_extended(&rpc, tip, height, pool_block_above, Some(observed)).await?;
        // Reuse precisely the normal accounting transitions in this same
        // transaction. A recurring fatal result is rolled back with the rest.
        let (reconcile_error, _) = self
            .reconcile_blocks_in(&mut tx, &observations, height)
            .await?;
        if let Some(error) = reconcile_error {
            bail!("reconciliation refused recovery: {error}");
        }
        // #737: the report runs under a statement timeout of its own, the
        // bound's time left less the commit's headroom, whatever the
        // session's: it neither fails at the operator connection's 15 s nor
        // runs on in the server after this call has given up.
        let headroom = COMMIT_HEADROOM.saturating_add(config.rpc_timeout);
        let report_time = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .and_then(|left| left.checked_sub(headroom))
            .filter(|time| *time >= MIN_REPORT_TIME)
            .context("fatal-state recovery has less than a second of its bound left for the carry-forward integrity report; raise --timeout-seconds and retry")?;
        let integrity = integrity_report_bounded(&mut tx, report_time)
            .await
            .map_err(|error| {
                if statement_timed_out(&error) {
                    error.context(format!("the carry-forward integrity report did not finish in the {:.1} s the fatal-state recovery bound left it; raise --timeout-seconds and retry", report_time.as_secs_f64()))
                } else {
                    error
                }
            })?;
        for field in ["mismatch_count", "current_drift_count"] {
            ensure!(
                integrity[field].as_u64() == Some(0),
                "fatal-state reconciliation failed {field}: {integrity}"
            );
        }
        // The check the recovery rests on: the chain still holds the captured
        // tip as the commit begins. Nothing was observed since the first
        // check, so a chain that flapped meanwhile left no stale observation.
        let final_tip =
            tip_unchanged_or_extended(&rpc, tip, height, pool_block_above, None).await?;
        let reconciliation = json!({"genesis_hash":genesis,"tip_hash":tip,"tip_height":height,
            "final_tip_hash":final_tip.as_deref().unwrap_or(tip),"tip_extended":final_tip.is_some(),
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

/// #737: whether the chain the clear observed is still the node's. The tip it
/// captured, `tip` at `height`, is either still the best block (`None`), or
/// the best block descends from it, the chain having only grown on top of
/// it, and the best block is returned.
///
/// Why an extension needs no second look at the database: a block commits to
/// its whole ancestry, so a best block whose headers lead back to `tip` at
/// `height` stands on the very blocks at or below `height` that the capture
/// saw. Every pool-block and deep-fanout observation the clear asked the node
/// for is at or below `height`, so, as long as the node answered it from a
/// chain through `tip` (below), it holds on the new chain, and so does the
/// reconciliation built on them. Maturity was computed at the captured
/// height, below the new tip, so it can only mature a block late, which the
/// next frontend reconcile makes good, never early. The one observation the
/// node was not asked for is "inactive" for a pool block above `height`, and
/// the new blocks could hold one, so a moved tip refuses whenever such a row
/// exists (`pool_block_above`).
///
/// The ancestry is walked by headers from the best block itself, not read
/// from the active chain afterwards, so the block returned, which the event
/// records, is one that descends from `tip`. A best block at or below
/// `height`, a walk that reaches another block at `height` and a block the
/// node does not know (RPC_INVALID_ADDRESS_OR_KEY, -5) refuse as a
/// reorganization, and a best block more than `MAX_TIP_EXTENSION` blocks up
/// refuses too. Any other failure to read a header propagates.
///
/// The chain can go over to a fork while the loop asks the node, though, and
/// come back by the first check onto a longer chain through `tip`: the walk
/// passes, and what the loop read on the fork is stale. So the first check,
/// given the loop's observations as `observed`, asks the node for each of them
/// again once the tip has moved, before the walk, and an answer that differs
/// refuses as a reorganization. The check before the commit is given none:
/// nothing is observed after the first check, so a flap after it leaves no
/// stale observation, and its walk proves the chain holds `tip` as the commit
/// begins. Two flaps still pass. The chain can leave `tip` during the loop
/// and be back on exactly `tip` by the first check, which then finds the tip
/// unchanged and asks nothing again, as the exact-tip check before #737 did;
/// `tip` has less work than the fork, so it is the best block again only once
/// the fork is invalidated. And the chain can flap again during the re-read
/// itself, back onto the fork the loop saw, so that the re-read repeats the
/// loop's stale answers.
async fn tip_unchanged_or_extended(
    rpc: &Rpc,
    tip: &str,
    height: u64,
    pool_block_above: bool,
    observed: Option<Observed<'_>>,
) -> Result<Option<String>> {
    let best = rpc.call("getbestblockhash", json!([])).await?;
    let best = best.as_str().context("qbit best block hash missing")?;
    if best == tip {
        return Ok(None);
    }
    ensure!(
        !pool_block_above,
        "the tip moved and a pool block lies above the captured height {height}; retry fatal-state clear"
    );
    if let Some(observed) = observed {
        observed.still_hold(rpc).await?;
    }
    let header = block_header(rpc, best).await?;
    let best_height = header["height"]
        .as_u64()
        .context("qbit block header has no height")?;
    ensure!(
        best_height > height,
        "the chain reorganized during fatal-state recovery: the node's tip {best} is at height {best_height}, not above the captured height {height}; retry fatal-state clear"
    );
    let moved = best_height - height;
    ensure!(
        moved <= MAX_TIP_EXTENSION,
        "the node's tip moved {moved} blocks during fatal-state recovery; retry fatal-state clear"
    );
    let mut at_height = previous_block(&header)?;
    for _ in 1..moved {
        at_height = previous_block(&block_header(rpc, &at_height).await?)?;
    }
    ensure!(
        at_height == tip,
        "the chain reorganized during fatal-state recovery: the node's tip {best} descends from {at_height} at the captured height {height}, not from the captured tip {tip}; retry fatal-state clear"
    );
    tracing::info!(
        captured_tip = tip,
        captured_height = height,
        best_block = best,
        best_height,
        "the tip grew on top of the captured tip during fatal-state recovery; every observation stands"
    );
    Ok(Some(best.to_owned()))
}

/// What the clear's observation loop asked the node, for the first tip check
/// to ask again once the tip has moved (#737).
struct Observed<'a> {
    /// The height of each of `blocks`, in the same order.
    heights: &'a [u64],
    blocks: &'a [BlockObservation],
    /// Each deep confirmed fanout's txid, confirming block and its height.
    fanouts: &'a [(String, String, u64)],
}

impl Observed<'_> {
    /// Ask the node for every observation again. An answer that differs
    /// means the chain moved under the loop, and refuses as a reorganization.
    async fn still_hold(&self, rpc: &Rpc) -> Result<()> {
        let state = |active| if active { "active" } else { "inactive" };
        for (block, at) in self.blocks.iter().zip(self.heights) {
            let active = block_at(rpc, *at).await?.as_deref() == Some(block.block_hash.as_str());
            ensure!(
                active == block.active,
                "the chain reorganized during fatal-state recovery: pool block {} at height {at} was {} when observed and is {} now; retry fatal-state clear",
                block.block_hash,
                state(block.active),
                state(active)
            );
        }
        for (txid, hash, at) in self.fanouts {
            ensure!(
                block_at(rpc, *at).await?.as_deref() == Some(hash.as_str()),
                "the chain reorganized during fatal-state recovery: deep confirmed CTV fanout {txid} at height {at} is no longer connected; retry fatal-state clear"
            );
        }
        Ok(())
    }
}

/// The active chain's block at `height`, or `None` beyond its tip
/// (RPC_INVALID_PARAMETER, -8).
async fn block_at(rpc: &Rpc, height: u64) -> Result<Option<String>> {
    match rpc.call("getblockhash", json!([height])).await {
        Ok(hash) => Ok(Some(
            hash.as_str().context("qbit block hash missing")?.to_owned(),
        )),
        Err(error) if reply_code(&error) == Some(-8) => Ok(None),
        Err(error) => Err(error),
    }
}

/// A block's header. A block the node does not know
/// (RPC_INVALID_ADDRESS_OR_KEY, -5) refuses as a reorganization.
async fn block_header(rpc: &Rpc, hash: &str) -> Result<Value> {
    match rpc.call("getblockheader", json!([hash])).await {
        Err(error) if reply_code(&error) == Some(-5) => bail!(
            "the chain reorganized during fatal-state recovery: the node does not know block {hash}; retry fatal-state clear"
        ),
        header => header,
    }
}

/// The block a header names as its parent.
fn previous_block(header: &Value) -> Result<String> {
    Ok(header["previousblockhash"]
        .as_str()
        .context("qbit block header has no previousblockhash")?
        .to_owned())
}

/// The node's JSON-RPC error code, when the node answered with an error.
fn reply_code(error: &anyhow::Error) -> Option<i64> {
    error
        .downcast_ref::<RpcReplyError>()
        .and_then(RpcReplyError::code)
}

/// `qbit_carry_forward_integrity_report()` in `tx`, under a statement timeout
/// of its own, `timeout`, whatever the session's (#737). At production size
/// the report takes about a minute, against the 15 s every ledger session
/// runs with: `fatal-state clear` gives it the time left in its bound, and
/// `self-check` a fixed allowance. The timeout is set for the transaction
/// only, and the session's value is put back for the statements after the
/// report; `RESET` would leave them the server's default instead. A failed
/// report leaves the transaction aborted, and its rollback restores the value.
pub async fn integrity_report_bounded(
    tx: &mut Transaction<'_, Postgres>,
    timeout: Duration,
) -> Result<Value> {
    // A statement_timeout of 0 disables it, so a bound never rounds down to 0.
    let millis = timeout.as_millis().clamp(1, i32::MAX as u128).to_string();
    // One round trip: the select list is evaluated in order, so the value
    // read is the session's, from before the set.
    let (saved, _): (String, String) = sqlx::query_as(
        "SELECT current_setting('statement_timeout'),set_config('statement_timeout',$1,true)",
    )
    .bind(millis)
    .fetch_one(&mut **tx)
    .await?;
    let report = sqlx::query_scalar("SELECT qbit_carry_forward_integrity_report()")
        .fetch_one(&mut **tx)
        .await?;
    sqlx::query("SELECT set_config('statement_timeout',$1,true)")
        .bind(saved)
        .execute(&mut **tx)
        .await?;
    Ok(report)
}

/// Whether a statement was cancelled by its statement timeout. An operator's
/// cancel request carries the same SQLSTATE and another message.
fn statement_timed_out(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<sqlx::Error>()
        .and_then(sqlx::Error::as_database_error)
        .is_some_and(|error| {
            error.code().as_deref() == Some("57014")
                && error.message().contains("statement timeout")
        })
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
