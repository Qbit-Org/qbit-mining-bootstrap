//! #573: the CTV broadcaster's claim and attempt bookkeeping at its edges, on
//! a real PostgreSQL through the public broadcaster and ledger APIs. A fanout
//! refused on every attempt does not hold up the rows behind it, a failed
//! attempt whose completion is lost keeps its backoff, a shutdown mid-attempt
//! hands the claim back at once, and a send whose completion is refused is
//! still an attempt. No sleeps decide when a race is injected.
use anyhow::{ensure, Context, Result};
use qbit_prism_server::broadcaster;
use serde_json::{json, Value};
use sqlx::Row;
use std::time::Duration;
use tokio::{sync::watch, time::timeout};

#[allow(dead_code)]
#[path = "support/compact_runtime_e2e/mod.rs"]
mod support;
use support::{
    ctv_fanout::mature_fanouts,
    execution::{Fault, FaultPhase},
    run, run_tuned, Fixture,
};

const BOUND: Duration = Duration::from_secs(5);

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
            // from a taken-over claim neither releases nor records anything on
            // the row another frontend took over.
            sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET next_broadcast_attempt_at=NULL")
                .execute(f.pool())
                .await?;
            let stale = f.a.ledger.claim_fanout(120).await?.context("claim")?;
            // #654: only a takeover ends a claim; revoking it stands in
            // for a lease the other frontend watched go unrenewed.
            qbit_prism_server::ledger::revoke_fanout_claims(f.pool(), Some(&stale.fanout_txid))
                .await?;
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

/// #654: a forward database clock step in the middle of an attempt makes the
/// holder's claim look expired at once. Another frontend does not take the
/// fanout over, and the holder's settlement after its send is recorded, one
/// attempt, its claim released by the holder itself. Before #654 the other
/// frontend's claim took the fanout and the holder's settlement was refused.
/// The step is made as `ledger_postgres`'s clock-step tests make it: every
/// stored fanout timestamp moves back by the step.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_forward_database_clock_step_mid_attempt_keeps_the_holders_claim_and_settlement(
) -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let count = mature_fanouts(f, false).await?;
            sendable(f).await?;
            let mut held = f.node.pause_next("sendrawtransaction")?;
            let a = f.a.clone();
            let pass = tokio::spawn(async move { broadcaster::run_once(&a).await });
            timeout(BOUND, held.entered()).await??;
            let sending = claimed_fanout(f).await?;
            sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET updated_at=updated_at-interval '2 hours',claim_expires_at=claim_expires_at-interval '2 hours',first_broadcast_attempt_at=first_broadcast_attempt_at-interval '2 hours',last_broadcast_attempt_at=last_broadcast_attempt_at-interval '2 hours',next_broadcast_attempt_at=next_broadcast_attempt_at-interval '2 hours'")
                .execute(f.pool())
                .await?;
            let expired: bool = sqlx::query_scalar(
                "SELECT claim_expires_at<=clock_timestamp() FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1",
            )
            .bind(&sending)
            .fetch_one(f.pool())
            .await?;
            ensure!(expired, "the step did not expire the claim by the database clock");
            // The other frontend may take another fanout, never the one in
            // flight; it hands any it took straight back.
            if let Some(other) = f.b.ledger.claim_fanout(120).await? {
                ensure!(
                    other.fanout_txid != sending,
                    "a forward step handed a live fanout claim to another frontend"
                );
                ensure!(f.b.ledger.release_fanout_claim(&other).await?);
            }
            held.release();
            let attempted = timeout(BOUND, pass)
                .await??
                .context("the pass failed after the step")?;
            ensure!(attempted == count, "the pass attempted {attempted} of {count} fanouts");
            let row = sqlx::query("SELECT claim_token IS NULL AS released,settlement_status,broadcast_attempt_count,last_broadcast_attempt_status FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1")
                .bind(&sending).fetch_one(f.pool()).await?;
            ensure!(
                row.try_get::<bool, _>("released")?
                    && row.try_get::<String, _>("settlement_status")? == "broadcast_submitted"
                    && row.try_get::<i64, _>("broadcast_attempt_count")? == 1
                    && row.try_get::<Option<String>, _>("last_broadcast_attempt_status")?.as_deref() == Some("submitted"),
                "a forward step refused the holder's settlement"
            );
            Ok(())
        })
    })
    .await
}

/// Fanouts claimed or attempted so far, and the attempts their history holds.
async fn touched(f: &Fixture) -> Result<(i64, i64)> {
    let rows = sqlx::query_scalar("SELECT count(*) FROM qbit_ctv_fanout_artifacts WHERE claim_token IS NOT NULL OR broadcast_attempt_count>0")
        .fetch_one(f.pool())
        .await?;
    let attempts = sqlx::query_scalar("SELECT count(*) FROM qbit_ctv_fanout_broadcast_attempts")
        .fetch_one(f.pool())
        .await?;
    Ok((rows, attempts))
}

/// #291: under `PRISM_BLOCK_SUBMIT_ENABLED=0` a broadcaster pass over due,
/// sendable fanouts is refused before it reads the node or claims a row, so
/// nothing is claimed, attempted or sent. The frontend with submission
/// enabled then sends every one of them: the refusal held back real sends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_frontend_with_block_submission_disabled_claims_and_sends_no_fanout() -> Result<()> {
    run_tuned(
        qbit_prism_test_gate::site!(),
        // `runtime-a` holds; `runtime-b` is the control.
        |config| config.block_submit_enabled = config.instance_id != "runtime-a",
        |f| {
            Box::pin(async move {
                f.refresh(true).await?;
                let count = mature_fanouts(f, false).await?;
                sendable(f).await?;
                let refused = timeout(BOUND, broadcaster::run_once(&f.a))
                    .await?
                    .err()
                    .context("the held frontend ran a broadcaster pass")?;
                ensure!(
                    refused
                        .to_string()
                        .contains("block submission is disabled by PRISM_BLOCK_SUBMIT_ENABLED"),
                    "{refused:#}"
                );
                ensure!(
                    touched(f).await? == (0, 0),
                    "the held frontend claimed or attempted a fanout"
                );
                let settled = timeout(BOUND, broadcaster::run_once(&f.b)).await??;
                ensure!(
                    settled == count,
                    "the enabled frontend settled {settled} of {count} fanouts"
                );
                let (rows, attempts) = touched(f).await?;
                ensure!(
                    rows == count as i64 && attempts == count as i64,
                    "the enabled frontend attempted {rows} rows with {attempts} sends of {count}"
                );
                Ok(())
            })
        },
    )
    .await
}
