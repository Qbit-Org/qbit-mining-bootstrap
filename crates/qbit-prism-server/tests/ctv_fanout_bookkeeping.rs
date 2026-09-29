//! #573: the CTV broadcaster's claim and attempt bookkeeping at its edges, on
//! a real PostgreSQL through the public broadcaster and ledger APIs. A fanout
//! refused on every attempt does not hold up the rows behind it, a failed
//! attempt whose completion is lost keeps its backoff, a shutdown mid-attempt
//! hands the claim back at once, and a send whose completion is refused is
//! still an attempt. No sleeps decide when a race is injected.
use anyhow::{ensure, Context, Result};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{AuditBundle, FoundBlock, PayoutPolicy};
use qbit_prism_server::{
    broadcaster, codec,
    ledger::{BlockObservation, Candidate, CandidateCtv, SignerKeys, WindowRef},
};
use serde_json::{json, Value};
use sqlx::Row;
use std::time::Duration;
use tokio::{sync::watch, time::timeout};

#[allow(dead_code)]
#[path = "support/compact_runtime_e2e/mod.rs"]
mod support;
use support::{
    execution::{Fault, FaultPhase},
    run, Fixture,
};

const BOUND: Duration = Duration::from_secs(5);

/// A landed CTV block whose fanouts are mature, as in `refresh_liveness`.
/// `observed` rows are already confirmed, so an attempt is a check-only
/// observation; otherwise every row is still to be broadcast.
async fn mature_fanouts(f: &Fixture, observed: bool) -> Result<usize> {
    let snapshot = f.a.ledger.snapshot(1_000_000).await?;
    let manifest_key = ManifestSigningKey::from_seed_hex(&f.a.config.manifest_seed)?;
    let ledger_key = ManifestSigningKey::from_seed_hex(&f.a.config.ledger_seed)?;
    let found = FoundBlock {
        block_height: 101,
        coinbase_value_sats: 5_000_000_000,
        network_difficulty: 1_000_000,
        anchor_job_issued_at_ms: snapshot.anchor_ms,
    };
    let options = CandidateCtv {
        direct_floor_sats: u64::MAX,
        settlement_config: qbit_prism::SettlementModeConfig {
            max_fanout_recipients_per_transaction: 1,
            ..Default::default()
        },
        fanout_fee_policy: Some(qbit_prism::FanoutFeeRatePolicy::new(1000, 12000)),
    };
    let bundle: AuditBundle = qbit_prism::build_audit_bundle_with_ctv_settlement_options(
        snapshot.shares.clone(),
        found,
        snapshot.prior_balances.clone(),
        PayoutPolicy::day_one_default(),
        options.direct_floor_sats,
        options.settlement_config,
        options.fanout_fee_policy,
        None,
        vec![],
        &manifest_key,
        &ledger_key,
    )?;
    let report = qbit_prism::verify_audit_bundle_with_ledger_public_key(
        &bundle,
        &ledger_key.public_key_hex(),
    )?;
    let mut block = vec![0u8; 80];
    block[..4].copy_from_slice(&0x20000000u32.to_le_bytes());
    block[4..36].fill(0xab);
    let mut txid = hex::decode(&report.coinbase_txid)?;
    txid.reverse();
    block[36..68].copy_from_slice(&txid);
    block[68..72].copy_from_slice(&(chrono::Utc::now().timestamp() as u32).to_le_bytes());
    block[72..76].copy_from_slice(&0x207fffffu32.to_le_bytes());
    let hash = codec::hash_display(&codec::double_sha256(&block));
    block.push(1);
    block.extend(hex::decode(&report.coinbase_tx_hex)?);
    let candidate = Candidate {
        block_hash: hash.clone(),
        block_sha256: Candidate::block_digest_hex(&block),
        job_id: "fanout-bookkeeping".into(),
        payout_revision: snapshot.payout_revision,
        window: WindowRef::from_snapshot(&snapshot)?,
        bootstrap_share: None,
        found_block: bundle.found_block.clone(),
        payout_policy: bundle.payout_policy.clone(),
        ctv: Some(options),
        audit_builder_version: qbit_prism::AUDIT_BUILDER_VERSION,
        signer_keys: SignerKeys::of(&manifest_key, &ledger_key),
        leased: false,
        coinbase_suffix_hex: bundle
            .coinbase_script_sig_suffix_hex
            .clone()
            .unwrap_or_else(|| "00".repeat(12)),
        deferred_share: None,
        block_bytes: block,
        as_issued_balances: snapshot.prior_balances,
    };
    f.a.ledger.enqueue_candidate(candidate).await?;
    let count = bundle
        .ctv_fanout_manifest_set
        .as_ref()
        .context("CTV fanouts")?
        .fanout_count as usize;
    ensure!(count > 2, "fixture needs multiple remaining rows");
    let claim =
        f.a.ledger
            .claim_candidate(60)
            .await?
            .context("parent claim")?
            .with_bundle(bundle);
    f.a.ledger
        .land_candidate(&claim, &f.a.config.ledger_public_key)
        .await?;
    f.a.ledger.finish_candidate(&claim, true, None).await?;
    f.a.ledger
        .reconcile_blocks_at_revision(
            &[BlockObservation {
                block_hash: hash.clone(),
                active: true,
            }],
            1101,
            f.a.ledger.payout_revision().await?,
        )
        .await?;
    f.node
        .set_tip(&"ab".repeat(32), &"cd".repeat(32), 1101, "02");
    f.node.set_reply(
        "getblockheader",
        json!([hash]),
        json!({"previousblockhash":"cd".repeat(32),"height":101,"confirmations":1001}),
    );
    f.node.set_reply("getblockhash", json!([101]), json!(hash));
    f.node
        .set_reply("getblockhash", json!([1101]), json!("ab".repeat(32)));
    if observed {
        sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET settlement_status='confirmed',confirmed_block_hash=$1,confirmed_block_height=1101,confirmed_depth=1")
            .bind("ab".repeat(32)).execute(f.pool()).await?;
    } else {
        sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET settlement_status='broadcastable'")
            .execute(f.pool())
            .await?;
    }
    f.a.refresh_once().await?;
    Ok(count)
}

/// Let every unconfirmed fanout reach its send: the covenant output is live,
/// the fanout is in no mempool, and the node takes the built-in-fee fanout.
async fn sendable(f: &Fixture) -> Result<()> {
    let manifests: Vec<Value> =
        sqlx::query_scalar("SELECT manifest FROM qbit_ctv_fanout_artifacts")
            .fetch_all(f.pool())
            .await?;
    for manifest in manifests {
        ensure!(
            manifest["precommitment"]["fanout_fee_sats"].as_u64() > Some(0),
            "fixture fanouts must carry their own fee"
        );
        let value = manifest["covenant_output_value_sats"]
            .as_u64()
            .context("covenant value")?;
        f.node.set_reply(
            "gettxout",
            json!([manifest["parent_coinbase_txid"], manifest["parent_coinbase_vout"], false]),
            json!({"scriptPubKey":{"hex":manifest["covenant_script_pubkey_hex"]},"value":format!("{}.{:08}",value/100_000_000,value%100_000_000)}),
        );
        f.node.set_reply(
            "sendrawtransaction",
            json!([manifest["fanout_tx_hex"]]),
            manifest["fanout_txid"].clone(),
        );
    }
    Ok(())
}

async fn claimed_fanout(f: &Fixture) -> Result<String> {
    Ok(sqlx::query_scalar(
        "SELECT fanout_txid FROM qbit_ctv_fanout_artifacts WHERE claim_token IS NOT NULL",
    )
    .fetch_one(f.pool())
    .await?)
}

async fn bump_revision(f: &Fixture) -> Result<()> {
    sqlx::query("UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1 WHERE singleton")
        .execute(f.pool())
        .await?;
    Ok(())
}

/// #573 (1): a refused completion hands its row back due at once but behind
/// the rows already due, so the next claim reaches another row rather than
/// retrying the refused one first on every pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_completion_requeues_its_fanout_behind_the_rows_already_due() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let count = mature_fanouts(f, true).await?;
            // Every row has been due for a while, as rows rescheduled by
            // earlier passes are: the requeue must order the refused row
            // after all of them, not merely after never-attempted rows.
            sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET next_broadcast_attempt_at=clock_timestamp()-interval '1 minute'")
                .execute(f.pool())
                .await?;
            let mut held = f.node.pause_next("getbestblockhash")?;
            let a = f.a.clone();
            let pass = tokio::spawn(async move { broadcaster::run_once(&a).await });
            timeout(BOUND, held.entered()).await??;
            let refused = claimed_fanout(f).await?;
            bump_revision(f).await?;
            held.release();
            let error = timeout(BOUND, pass)
                .await??
                .err()
                .context("a completion at a moved revision reported success")?;
            ensure!(
                format!("{error:#}").contains("payout revision changed"),
                "completion failed for another reason: {error:#}"
            );
            let row = sqlx::query("SELECT claim_token IS NULL AS released,broadcast_attempt_count,next_broadcast_attempt_at<=clock_timestamp() AS due FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1")
                .bind(&refused).fetch_one(f.pool()).await?;
            ensure!(row.try_get::<bool, _>("released")?, "the refused completion kept its claim");
            ensure!(
                row.try_get::<i64, _>("broadcast_attempt_count")? == 0,
                "a refused observation was recorded as an attempt"
            );
            ensure!(
                row.try_get::<Option<bool>, _>("due")? == Some(true),
                "the refused fanout is not due again at once"
            );
            // Every other row is due too; the refused one is no longer first.
            let next = f.b.ledger.claim_fanout(120).await?.context("next claim")?;
            ensure!(
                next.fanout_txid != refused,
                "the refused fanout was claimed first again, ahead of {} other due rows",
                count - 1
            );
            ensure!(f.b.ledger.release_fanout_claim(&next).await?);
            // Nothing is starved: the next pass settles every row.
            let later = timeout(BOUND, broadcaster::run_once(&f.a)).await??;
            ensure!(later == count, "later pass settled {later} of {count} rows");
            Ok(())
        })
    })
    .await
}

/// #573 (2): when an attempt fails and recording that failure is lost too,
/// the fallback still records the failed attempt and its backoff and hands
/// the claim back, instead of holding it for the lease and then retrying at
/// once. It does not take the settlement status the lost completion would
/// have written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_attempt_whose_failure_is_lost_keeps_its_backoff() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            // No live covenant output answers: every attempt fails.
            let count = mature_fanouts(f, false).await?;
            sqlx::raw_sql(
                r#"
                    CREATE FUNCTION mark_failed_attempt() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN
                        RAISE NOTICE 'prism-execution-marker ctv_failed_attempt INSERT';
                        RETURN NEW;
                    END $$;
                    CREATE TRIGGER mark_failed_attempt AFTER INSERT ON qbit_ctv_fanout_broadcast_attempts
                        FOR EACH ROW WHEN (NEW.attempt_status='failed') EXECUTE FUNCTION mark_failed_attempt();
                "#,
            )
            .execute(f.pool())
            .await?;
            // The first failure record loses its connection after executing,
            // so its transaction aborts.
            f.proxy.plan(Fault {
                table: "ctv_failed_attempt".into(),
                op: "INSERT".into(),
                phase: FaultPhase::AfterExecution,
            });
            let attempted = timeout(BOUND, broadcaster::run_once(&f.a)).await??;
            ensure!(f.proxy.fired().is_some(), "the completion fault did not fire");
            ensure!(attempted == count, "the pass attempted {attempted} of {count} rows");
            let rows = sqlx::query("SELECT a.fanout_txid,a.settlement_status,a.claim_token IS NULL AS released,a.broadcast_attempt_count,a.last_broadcast_attempt_status,a.broadcast_attempt_status_counts->>'failed' AS failed,a.broadcast_retry_backoff_seconds,a.next_broadcast_attempt_at>clock_timestamp()+interval '5 seconds' AS backed_off,a.last_broadcast_error,(SELECT count(*) FROM qbit_ctv_fanout_broadcast_attempts t WHERE t.fanout_txid=a.fanout_txid AND t.attempt_status='failed') AS history FROM qbit_ctv_fanout_artifacts a")
                .fetch_all(f.pool()).await?;
            let mut unfinished = 0;
            for row in &rows {
                let fanout: String = row.try_get("fanout_txid")?;
                ensure!(row.try_get::<bool, _>("released")?, "{fanout}: the claim was held");
                ensure!(
                    row.try_get::<i64, _>("broadcast_attempt_count")? == 1
                        && row.try_get::<Option<String>, _>("last_broadcast_attempt_status")?.as_deref() == Some("failed")
                        && row.try_get::<Option<String>, _>("failed")?.as_deref() == Some("1")
                        && row.try_get::<i64, _>("history")? == 1,
                    "{fanout}: the failed attempt was not recorded exactly once"
                );
                ensure!(
                    row.try_get::<i64, _>("broadcast_retry_backoff_seconds")? == 10
                        && row.try_get::<Option<bool>, _>("backed_off")? == Some(true),
                    "{fanout}: the failed attempt's backoff was lost"
                );
                ensure!(
                    row.try_get::<Option<String>, _>("last_broadcast_error")?.is_some_and(|error| error.contains("unexpected RPC")),
                    "{fanout}: the attempt's own error was not recorded"
                );
                // The fallback leaves the settlement status alone.
                if row.try_get::<String, _>("settlement_status")? == "broadcastable" {
                    unfinished += 1;
                }
            }
            ensure!(rows.len() == count, "{} rows", rows.len());
            ensure!(unfinished == 1, "{unfinished} rows kept their status");
            // Both hand-backs are fenced by the holder's token: a late one
            // from an expired claim neither releases nor records anything on
            // the row another frontend took over.
            sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET next_broadcast_attempt_at=NULL")
                .execute(f.pool())
                .await?;
            let stale = f.a.ledger.claim_fanout(120).await?.context("claim")?;
            sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET claim_expires_at=clock_timestamp() WHERE fanout_txid=$1")
                .bind(&stale.fanout_txid).execute(f.pool()).await?;
            let taken = f.b.ledger.claim_fanout(120).await?.context("takeover")?;
            ensure!(taken.fanout_txid == stale.fanout_txid, "fixture took over another row");
            ensure!(
                !f.a.ledger.release_failed_fanout(&stale, "late").await?
                    && !f.a.ledger.requeue_refused_fanout(&stale, &json!({}), "late").await?,
                "a stale token handed back another frontend's claim"
            );
            let row = sqlx::query("SELECT claim_token,broadcast_attempt_count,(SELECT count(*) FROM qbit_ctv_fanout_broadcast_attempts t WHERE t.fanout_txid=a.fanout_txid) AS history FROM qbit_ctv_fanout_artifacts a WHERE fanout_txid=$1")
                .bind(&taken.fanout_txid).fetch_one(f.pool()).await?;
            ensure!(
                row.try_get::<Option<String>, _>("claim_token")?.as_deref() == Some(taken.claim_token.as_str())
                    && row.try_get::<i64, _>("broadcast_attempt_count")? == 1
                    && row.try_get::<i64, _>("history")? == 1,
                "a stale hand-back changed the row another frontend holds"
            );
            ensure!(f.b.ledger.release_failed_fanout(&taken, "holder").await?);
            Ok(())
        })
    })
    .await
}

/// #573 (3): a shutdown in the middle of an attempt abandons it and hands the
/// claim back at once, so another frontend takes the same row without
/// waiting out the 120-second lease; the pass stops claiming.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shutdown_mid_attempt_hands_the_fanout_claim_back() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let count = mature_fanouts(f, true).await?;
            let mut held = f.node.pause_next("getbestblockhash")?;
            let (stop, shutdown) = watch::channel(false);
            let a = f.a.clone();
            let pass = tokio::spawn(async move { broadcaster::run_pass(&a, &shutdown).await });
            timeout(BOUND, held.entered()).await??;
            let abandoned = claimed_fanout(f).await?;
            stop.send(true)?;
            // The node never answers the held call: only the shutdown ends it.
            let finished = timeout(BOUND, pass)
                .await
                .context("the pass waited for its attempt after shutdown")???;
            ensure!(finished == 0, "a shutdown pass reported {finished} attempts");
            let row = sqlx::query("SELECT claim_token IS NULL AND claim_instance_id IS NULL AND claim_expires_at IS NULL AS released,broadcast_attempt_count,next_broadcast_attempt_at IS NULL AS due FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1")
                .bind(&abandoned).fetch_one(f.pool()).await?;
            ensure!(
                row.try_get::<bool, _>("released")?,
                "the shutdown left its claim held"
            );
            ensure!(
                row.try_get::<i64, _>("broadcast_attempt_count")? == 0 && row.try_get::<bool, _>("due")?,
                "the abandoned attempt was recorded or rescheduled"
            );
            // Another frontend takes over the same row at once.
            let taken = f.b.ledger.claim_fanout(120).await?.context("takeover")?;
            ensure!(
                taken.fanout_txid == abandoned,
                "the successor could not take the abandoned row"
            );
            let claimed: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM qbit_ctv_fanout_artifacts WHERE claim_token IS NOT NULL",
            )
            .fetch_one(f.pool())
            .await?;
            ensure!(claimed == 1, "the stopped pass left {} claims", claimed - 1);
            ensure!(f.b.ledger.release_fanout_claim(&taken).await?);
            held.release();
            // A pass that starts stopped claims nothing.
            let (_stop, stopped) = watch::channel(true);
            ensure!(broadcaster::run_pass(&f.a, &stopped).await? == 0);
            ensure!(timeout(BOUND, broadcaster::run_once(&f.a)).await?? == count);
            Ok(())
        })
    })
    .await
}

/// #573 (4): a send whose completion is refused is still an attempt: it is
/// recorded as `submitted` with the refusal as its error, so the history and
/// the attempt counters see it. The status the refused completion would have
/// written is left alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_send_whose_completion_is_refused_is_recorded_as_an_attempt() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            mature_fanouts(f, false).await?;
            sendable(f).await?;
            // The node has taken the fanout when the revision moves.
            let mut held = f.node.pause_next("sendrawtransaction")?;
            let a = f.a.clone();
            let pass = tokio::spawn(async move { broadcaster::run_once(&a).await });
            timeout(BOUND, held.entered()).await??;
            let sent = claimed_fanout(f).await?;
            bump_revision(f).await?;
            held.release();
            let error = timeout(BOUND, pass)
                .await??
                .err()
                .context("a completion at a moved revision reported success")?;
            ensure!(
                format!("{error:#}").contains("payout revision changed"),
                "completion failed for another reason: {error:#}"
            );
            let row = sqlx::query("SELECT claim_token IS NULL AS released,settlement_status,broadcast_attempt_count,last_broadcast_attempt_status,broadcast_attempt_status_counts->>'submitted' AS submitted,last_broadcast_error,last_broadcast_submit_result->>'submit_result' AS result,broadcast_retry_backoff_seconds FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1")
                .bind(&sent).fetch_one(f.pool()).await?;
            ensure!(row.try_get::<bool, _>("released")?, "the refused send kept its claim");
            ensure!(
                row.try_get::<i64, _>("broadcast_attempt_count")? == 1
                    && row.try_get::<Option<String>, _>("last_broadcast_attempt_status")?.as_deref() == Some("submitted")
                    && row.try_get::<Option<String>, _>("submitted")?.as_deref() == Some("1"),
                "the refused send was not counted as a submitted attempt"
            );
            ensure!(
                row.try_get::<Option<String>, _>("result")?.as_deref() == Some(sent.as_str()),
                "the send's result was not kept"
            );
            ensure!(
                row.try_get::<Option<String>, _>("last_broadcast_error")?
                    .is_some_and(|error| error.contains("completion not persisted") && error.contains("payout revision changed")),
                "the refusal was not recorded as the attempt's error"
            );
            ensure!(
                row.try_get::<String, _>("settlement_status")? == "broadcastable"
                    && row.try_get::<i64, _>("broadcast_retry_backoff_seconds")? == 0,
                "the refused completion's status or a backoff was written"
            );
            let history = sqlx::query("SELECT attempt_status,submit_result->>'submit_result' AS result,error FROM qbit_ctv_fanout_broadcast_attempts WHERE fanout_txid=$1")
                .bind(&sent).fetch_all(f.pool()).await?;
            ensure!(
                history.len() == 1
                    && history[0].try_get::<String, _>("attempt_status")? == "submitted"
                    && history[0].try_get::<Option<String>, _>("result")?.as_deref() == Some(sent.as_str())
                    && history[0].try_get::<Option<String>, _>("error")?.is_some(),
                "the attempt history has {} rows for the refused send",
                history.len()
            );
            Ok(())
        })
    })
    .await
}
