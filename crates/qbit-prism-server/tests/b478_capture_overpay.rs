//! #478 block capture and payout divergence, through the real submit, offer,
//! rebuild, landing and confirmation code against PostgreSQL 16. Adopted from
//! the payout-integrity review, with its assertions turned from the refuted
//! per-landing bound into what the code now guarantees and records.
//!
//! A sub-floor carry (5,000 sats, below the 14,720-sat day-one floor) sits on
//! recipient `aa`. Block A lands and pays most of it down. Work issued after
//! the tip moved to A but before A confirmed snapshots balances without A, so
//! a block found on it pays that carry again: `aa` goes into debt, exactly
//! (on-chain equals the coinbases, the drift and mismatch counts stay zero).
//!
//! What is pinned:
//! - every divergent landing records the debt it created, per account, and
//!   the records sum to the debt the balances carry;
//! - a block whose payout revision was superseded is offered only when the
//!   bound its offer reservation computed is within the ceiling, and that
//!   bound covers the debt its landing realizes, including two consecutive
//!   captures (P, C1, C2);
//! - over the ceiling the block is abandoned unoffered and counted; a ceiling
//!   of 0 refuses capture at submit and abandons at offer; a bound that
//!   cannot be computed abandons nothing;
//! - the integrity report's divergence line and the metrics show the debt.
use anyhow::{ensure, Context, Result};
use qbit_prism_server::{
    codec,
    coordinator::{Coordinator, JobContext},
    ledger::OfferReservation,
    stratum::{MiningBackend, MiningJob, Worker},
};
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use std::time::Duration;

#[allow(dead_code)]
#[path = "support/compact_runtime_e2e/mod.rs"]
mod support;
use support::{run, run_tuned, Fixture, DIFFICULTY};

const SEED_CARRY: i64 = 5_000;
const COINBASE: i64 = 5_000_000_000;

fn aa() -> String {
    "aa".repeat(32)
}

// ---------------------------------------------------------------------------
// Scenarios adopted from the review.

/// Only the candidate captures B: it is found on its pre-A job after A's
/// settlement bump. Its debt is recorded, within the bound its offer computed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_capture_after_the_settlement_bump_lands_within_its_recorded_bound() -> Result<()> {
    run(gate::site!(), |f| {
        Box::pin(async move { two_blocks(f, true).await })
    })
    .await
}

/// The base-reachable order: B is current and offered before A confirms, so
/// A is the divergent landing. It is recorded without any offer decision.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_offer_before_the_settlement_bump_lands_divergent_and_is_recorded() -> Result<()> {
    run(gate::site!(), |f| {
        Box::pin(async move { two_blocks(f, false).await })
    })
    .await
}

async fn two_blocks(f: &Fixture, capture_after_bump: bool) -> Result<()> {
    let aa = aa();
    seed_carry(f, &aa, SEED_CARRY).await?;
    f.refresh(true).await?;
    ensure!(
        balance(f, &aa).await? == SEED_CARRY,
        "seed carry not current"
    );
    let (hash_a, _) = own_block_on_the_node(f, "landa", "1a2b3c4d").await?;
    // Frontend b observes A and issues work on it before A lands.
    f.b.refresh_once().await?;
    let (wb, jb) = issue(&f.b, "landb", "1a2b3c4e").await?;
    ensure!(jb.wire.previousblockhash == hash_a, "B's job is not on A");
    let (pb, hash_b) = find_block_proof(&jb)?;
    if capture_after_bump {
        land_next(f, &hash_a).await?;
        ensure!(
            f.b.ledger.payout_revision().await? > jb.wire.payout_revision,
            "no settlement bump"
        );
        let submit = f.b.submit(&wb, &jb, pb, false.into());
        let (submitted, driven) = tokio::join!(submit, drive(f, &hash_b));
        submitted.map_err(|e| anyhow::anyhow!("capture refused: {e}"))?;
        driven?;
    } else {
        f.b.submit(&wb, &jb, pb, false.into()).await?;
        land_next(f, &hash_b).await?;
        land_next(f, &hash_a).await?;
    }
    for hash in [&hash_a, &hash_b] {
        ensure!(
            state(f, hash).await?.as_deref() == Some("submitted"),
            "{hash} did not land"
        );
    }
    let onchain: i64 = sqlx::query_scalar(
        "SELECT COALESCE(sum(onchain_amount_sats),0)::bigint FROM qbit_pool_payout_entries WHERE block_hash=ANY($1)",
    )
    .bind(vec![hash_a.clone(), hash_b.clone()])
    .fetch_one(f.pool())
    .await?;
    ensure!(
        onchain == 2 * COINBASE,
        "on-chain {onchain} != two coinbases"
    );
    let integrity = integrity(f).await?;
    ensure!(
        integrity["current_drift_count"] == 0 && integrity["mismatch_count"] == 0,
        "the ledger stays exact: {integrity}"
    );
    let debt = -balance(f, &aa).await?;
    ensure!(debt > 0, "aa was not paid twice: debt {debt}");
    ensure!(
        debt <= SEED_CARRY,
        "one divergence pays the carry once more at most"
    );
    // The divergent landing is B after the bump, A in the base order.
    let divergent = if capture_after_bump { &hash_b } else { &hash_a };
    let other = if capture_after_bump { &hash_a } else { &hash_b };
    let record = divergence(f, divergent)
        .await?
        .context("no divergence record")?;
    ensure!(
        record.overpay == Some(i128::from(debt)),
        "{record:?} vs debt {debt}"
    );
    ensure!(
        record.pool_debt_after == Some(i128::from(debt)),
        "{record:?}"
    );
    ensure!(
        divergence(f, other).await?.is_none(),
        "{other} did not diverge"
    );
    let accounts = overpaid_accounts(f, divergent).await?;
    ensure!(
        accounts == vec![(aa.clone(), i128::from(debt))],
        "{accounts:?}"
    );
    if capture_after_bump {
        ensure!(record.decision.as_deref() == Some("offered"), "{record:?}");
        let bound = record.bound.context("no bound")?;
        ensure!(bound >= i128::from(debt), "bound {bound} < realized {debt}");
        ensure!(bound <= record.ceiling.context("no ceiling")?, "{record:?}");
        ensure!(
            metric(
                &f.b,
                "qbit_prism_capture_offer_decisions_total{decision=\"offered\"}"
            ) == 1.
        );
        ensure!(metric(&f.b, "qbit_prism_divergent_landings_total") == 1.);
        ensure!(metric(&f.b, "qbit_prism_divergent_landing_overpay_sats_total") == debt as f64);
        ensure!(metric(&f.b, "qbit_prism_carry_forward_debt_sats") == debt as f64);
    } else {
        // Current at its offer, adopted when found active: no decision.
        ensure!(record.decision.is_none(), "{record:?}");
    }
    let line = &integrity["payout_divergence"];
    ensure!(line["divergent_landings"] == 1, "{line}");
    ensure!(sats(&line["overpay_sats"]) == Some(debt.into()), "{line}");
    ensure!(
        line["debtor_count"] == 1 && sats(&line["debt_sats"]) == Some(debt.into()),
        "{line}"
    );
    Ok(())
}

/// Three own blocks A <- B <- C whose work was all issued before A (and B)
/// confirmed. Each pays the pre-A carry again, so `aa`'s debt exceeds the
/// carry: the bound is per divergent landing, not per landing. Every sat of
/// it is recorded, and the capture's bound covers its own landing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consecutive_divergent_landings_compound_and_every_sat_is_recorded() -> Result<()> {
    run(gate::site!(), |f| {
        Box::pin(async move { chain_of_three(f, true).await })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consecutive_base_order_landings_compound_and_every_sat_is_recorded() -> Result<()> {
    run(gate::site!(), |f| {
        Box::pin(async move { chain_of_three(f, false).await })
    })
    .await
}

async fn chain_of_three(f: &Fixture, capture: bool) -> Result<()> {
    let aa = aa();
    seed_carry(f, &aa, SEED_CARRY).await?;
    f.refresh(true).await?;
    let (hash_a, _) = own_block_on_the_node(f, "landa", "1a2b3c4d").await?;
    f.b.refresh_once().await?;
    let (wb, jb) = issue(&f.b, "landb", "1a2b3c4e").await?;
    let (pb, hash_b) = find_block_proof(&jb)?;
    let block = pb.block_hex.clone();
    f.b.submit(&wb, &jb, pb, false.into()).await?;
    ensure!(f.b.rpc.call("submitblock", json!([block])).await?.is_null());
    f.b.refresh_once().await?;
    let (wc, jc) = issue(&f.b, "landc", "1a2b3c4f").await?;
    let (pc, hash_c) = find_block_proof(&jc)?;
    if capture {
        land_all(f).await?;
        ensure!(
            f.b.ledger.payout_revision().await? > jc.wire.payout_revision,
            "no settlement bump before C"
        );
        let submit = f.b.submit(&wc, &jc, pc, false.into());
        let (submitted, driven) = tokio::join!(submit, drive(f, &hash_c));
        submitted.map_err(|e| anyhow::anyhow!("capture refused: {e}"))?;
        driven?;
    } else {
        f.b.submit(&wc, &jc, pc, false.into()).await?;
        land_all(f).await?;
    }
    for hash in [&hash_a, &hash_b, &hash_c] {
        ensure!(
            state(f, hash).await?.as_deref() == Some("submitted"),
            "{hash}"
        );
    }
    let integrity = integrity(f).await?;
    ensure!(integrity["current_drift_count"] == 0 && integrity["mismatch_count"] == 0);
    let debt = i128::from(-balance(f, &aa).await?);
    ensure!(
        debt > i128::from(SEED_CARRY),
        "two divergent landings each paid the carry again: debt {debt}"
    );
    let recorded: Vec<(String, i128)> = sqlx::query_as::<_, (String, String)>(
        "SELECT block_hash,overpay_sats::text FROM qbit_prism_payout_divergences WHERE confirmed_at IS NOT NULL AND divergent_accounts > 0 ORDER BY confirmed_at",
    )
    .fetch_all(f.pool())
    .await?
    .into_iter()
    .map(|(hash, sats)| Ok((hash, sats.parse()?)))
    .collect::<Result<_>>()?;
    ensure!(
        recorded.iter().map(|(_, sats)| sats).sum::<i128>() == debt,
        "every sat of debt is recorded: {recorded:?} vs {debt}"
    );
    ensure!(
        sats(&integrity["payout_divergence"]["overpay_sats"]) == Some(debt),
        "{integrity}"
    );
    // Every block offered on a superseded revision was bounded, and its bound
    // covers its own landing. After the bump, C is such a capture; in the
    // base order the claim order decides which blocks land divergent.
    let decided: Vec<(String, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT block_hash,overpay_bound_sats::text,overpay_sats::text FROM qbit_prism_payout_divergences WHERE offer_decision IS NOT NULL",
    )
    .fetch_all(f.pool())
    .await?;
    for (hash, bound, overpay) in &decided {
        let bound: i128 = bound.as_deref().context("no bound")?.parse()?;
        let overpay: i128 = overpay.as_deref().map(str::parse).transpose()?.unwrap_or(0);
        ensure!(
            bound >= overpay,
            "{hash}: bound {bound} < realized {overpay}"
        );
    }
    if capture {
        let c = divergence(f, &hash_c).await?.context("C did not diverge")?;
        ensure!(c.decision.as_deref() == Some("offered"), "{c:?}");
    }
    Ok(())
}

/// A captured candidate whose parent stops being the tip before the offer is
/// abandoned, never offered, its deferred share never credited, and the row
/// is never claimed again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_captured_candidate_whose_parent_changes_is_abandoned_unoffered() -> Result<()> {
    run(gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let (worker, job) = issue(&f.a, "stale", "1a2b3c4d").await?;
            bump_revision_same_parent(f).await?;
            f.a.refresh_once().await?;
            let (proof, hash) = find_block_proof(&job)?;
            f.node.accept_blocks();
            let parent = job.wire.previousblockhash.clone();
            let competitor = "ef".repeat(32);
            let submit = f.a.submit(&worker, &job, proof, false.into());
            let drive = async {
                wait_enqueued(f, &hash).await?;
                // A real parent change lands before the offer.
                f.node.set_tip(&competitor, &parent, 101, "02");
                let claim = f.a.ledger.claim_candidate(60).await?.context("no claim")?;
                ensure!(claim.candidate.block_hash == hash);
                f.a.process_candidate(&claim).await
            };
            let (submitted, driven) = tokio::join!(submit, drive);
            driven?;
            let (state, error) = outcome(f, &hash).await?;
            let tip = f.a.rpc.call("getbestblockhash", json!([])).await?;
            ensure!(state == "abandoned", "{state}");
            ensure!(error.as_deref() == Some("parent superseded"), "{error:?}");
            ensure!(
                tip == json!(competitor),
                "the stale block was offered: tip {tip}"
            );
            ensure!(
                credited(f, &worker, &hash).await? == 0,
                "deferred share credited"
            );
            // A lost race is the pool's own staleness decision: `stale-job`, as base answered at submit, never `ledger-confirmation-failed`.
            let answer = submitted.expect_err("an abandoned capture was acknowledged");
            ensure!(
                answer.reason_id.as_deref() == Some("stale-job"),
                "{answer:?}"
            );
            ensure!(
                metric(
                    &f.a,
                    "qbit_prism_stale_job_rejections_total{cause=\"parent_grace\"}"
                ) == 1.,
                "the abandon is counted once under parent_grace"
            );
            ensure!(
                f.a.ledger.claim_candidate(60).await?.is_none(),
                "claimed again"
            );
            Ok(())
        })
    })
    .await
}

// ---------------------------------------------------------------------------
// The ceiling (coordinator decision): bound, abandonment, kill switch.

/// P lands; C1, found on work issued before P landed, is captured; C2, found
/// on work issued after C1 reached the node but before C1 landed, is captured
/// too. Each offer's bound, computed under its reservation from durable
/// state, covers the debt its own landing realizes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_consecutive_captures_each_land_within_their_bound() -> Result<()> {
    run(gate::site!(), |f| {
        Box::pin(async move {
            let aa = aa();
            seed_carry(f, &aa, SEED_CARRY).await?;
            f.refresh(true).await?;
            let (hash_p, _) = own_block_on_the_node(f, "landp", "1a2b3c4d").await?;
            f.b.refresh_once().await?;
            let (w1, j1) = issue(&f.b, "landc1", "1a2b3c4e").await?;
            let (p1, hash_c1) = find_block_proof(&j1)?;
            land_next(f, &hash_p).await?;
            let after_p = balance(f, &aa).await?;
            // C1 is captured and offered; the node accepts it and its tip moves.
            // Its landing is held back (its as-issued snapshot is withheld at
            // the offer, so the post-offer rebuild fails and the row waits in
            // reconciliation), and meanwhile C2's work is issued on C1 from
            // balances that do not include C1, as a frontend's new-tip work
            // is in production. Then C1 lands, confirms, and C2 is captured.
            let submit_c1 = f.b.submit(&w1, &j1, p1, false.into());
            let drive_c1 = async {
                wait_enqueued(f, &hash_c1).await?;
                let digest: String = sqlx::query_scalar("SELECT window_prior_balances_sha256 FROM qbit_block_candidate_outbox WHERE block_hash=$1")
                    .bind(&hash_c1)
                    .fetch_one(f.pool())
                    .await?;
                let snapshot: Vec<u8> = sqlx::query_scalar("SELECT balances FROM qbit_prism_balance_snapshots WHERE prior_balances_digest=$1")
                    .bind(&digest)
                    .fetch_one(f.pool())
                    .await?;
                let mut offer = f.node.pause_next("submitblock")?;
                let claim = f.b.ledger.claim_candidate(60).await?.context("no C1 claim")?;
                ensure!(claim.candidate.block_hash == hash_c1);
                let withheld = &digest;
                let control = async move {
                    offer.entered().await?;
                    sqlx::query("DELETE FROM qbit_prism_balance_snapshots WHERE prior_balances_digest=$1")
                        .bind(withheld)
                        .execute(f.pool())
                        .await?;
                    offer.release();
                    anyhow::Ok(())
                };
                let (processed, controlled) = tokio::join!(f.b.process_candidate(&claim), control);
                controlled?;
                processed?;
                ensure!(
                    state(f, &hash_c1).await?.as_deref() == Some("reconciliation"),
                    "C1's landing was held back"
                );
                ensure!(
                    f.a.rpc.call("getbestblockhash", json!([])).await? == json!(hash_c1),
                    "the node accepted C1"
                );
                f.a.refresh_once().await?;
                let (w2, j2) = issue(&f.a, "landc2", "1a2b3c50").await?;
                ensure!(j2.wire.previousblockhash == hash_c1, "C2's work is not on C1");
                sqlx::query("INSERT INTO qbit_prism_balance_snapshots(prior_balances_digest,balances) VALUES($1,$2)")
                    .bind(&digest)
                    .bind(&snapshot)
                    .execute(f.pool())
                    .await?;
                sqlx::query("UPDATE qbit_block_candidate_outbox SET next_attempt_at=clock_timestamp() WHERE block_hash=$1")
                    .bind(&hash_c1)
                    .execute(f.pool())
                    .await?;
                let claim = f.b.ledger.claim_candidate(60).await?.context("no C1 retry")?;
                f.b.process_candidate(&claim).await?;
                anyhow::Ok((w2, j2))
            };
            let (submitted, driven) = tokio::join!(submit_c1, drive_c1);
            let (w2, j2) = driven?;
            submitted.map_err(|e| anyhow::anyhow!("C1 capture refused: {e}"))?;
            ensure!(state(f, &hash_c1).await?.as_deref() == Some("submitted"));
            let after_c1 = balance(f, &aa).await?;
            ensure!(
                f.a.ledger.payout_revision().await? > j2.wire.payout_revision,
                "no settlement bump before C2"
            );
            let (p2, hash_c2) = find_block_proof(&j2)?;
            let submit = f.a.submit(&w2, &j2, p2, false.into());
            let (submitted, driven) = tokio::join!(submit, drive(f, &hash_c2));
            submitted.map_err(|e| anyhow::anyhow!("C2 capture refused: {e}"))?;
            driven?;
            let after_c2 = balance(f, &aa).await?;
            let debt = |balance: i64| i128::from((-balance).max(0));
            for (hash, before, after) in [
                (&hash_c1, after_p, after_c1),
                (&hash_c2, after_c1, after_c2),
            ] {
                let record = divergence(f, hash).await?.context("no record")?;
                let realized = debt(after) - debt(before);
                println!("B478-EVIDENCE capture {hash} aa {before} -> {after}: realized {realized}, record {record:?}");
                ensure!(record.decision.as_deref() == Some("offered"), "{record:?}");
                ensure!(record.overpay == Some(realized), "{record:?} realized {realized}");
                ensure!(
                    record.bound.context("no bound")? >= realized,
                    "the bound does not cover the realized debt: {record:?} realized {realized}"
                );
            }
            // C1 paid aa's pre-P carry again. C2 was issued on C1's
            // (post-P) prior, which may or may not be paid out again; either
            // way its landing stayed within its bound above.
            ensure!(debt(after_c1) > 0, "C1 paid aa again");
            ensure!(debt(after_c2) >= debt(after_c1), "debt only grows here");
            let integrity = integrity(f).await?;
            ensure!(integrity["current_drift_count"] == 0 && integrity["mismatch_count"] == 0);
            Ok(())
        })
    })
    .await
}

/// A capture whose bound exceeds the ceiling is abandoned before its one
/// offer: the node never sees it, its deferred share is never credited, and
/// the decision, the bound and the ceiling are recorded and counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_capture_over_the_ceiling_is_abandoned_unoffered_and_counted() -> Result<()> {
    // 1 bp of a 5,000,000,000-sat coinbase is 500,000 sats; a 10,000,000-sat
    // carry paid down by A puts B's bound far above it.
    run_tuned(
        gate::site!(),
        |config| config.capture_overpay_ceiling_bps = 1,
        |f| {
            Box::pin(async move {
                let aa = aa();
                seed_carry(f, &aa, 10_000_000).await?;
                f.refresh(true).await?;
                let (hash_a, _) = own_block_on_the_node(f, "landa", "1a2b3c4d").await?;
                f.b.refresh_once().await?;
                let (wb, jb) = issue(&f.b, "landb", "1a2b3c4e").await?;
                let (pb, hash_b) = find_block_proof(&jb)?;
                land_next(f, &hash_a).await?;
                let submit = f.b.submit(&wb, &jb, pb, false.into());
                let (submitted, driven) = tokio::join!(submit, drive(f, &hash_b));
                driven?;
                let (state, error) = outcome(f, &hash_b).await?;
                ensure!(state == "abandoned", "{state}");
                let error = error.unwrap_or_default();
                ensure!(
                    error.contains("exceeds the ceiling 500000 sats (1 bps"),
                    "{error}"
                );
                ensure!(
                    f.a.rpc.call("getbestblockhash", json!([])).await? == json!(hash_a),
                    "the block was offered"
                );
                // A ceiling refusal is a policy decision on a block the ledger wrote correctly: `stale-job`, never `ledger-confirmation-failed`, which feeds the share-append failure warning.
                let answer = submitted.expect_err("an abandoned capture was acknowledged");
                ensure!(
                    answer.reason_id.as_deref() == Some("stale-job"),
                    "{answer:?}"
                );
                ensure!(
                    metric(
                        &f.b,
                        "qbit_prism_stale_job_rejections_total{cause=\"payout_revision\"}"
                    ) == 1.,
                    "the abandon is counted once under payout_revision"
                );
                ensure!(
                    credited(f, &wb, &hash_b).await? == 0,
                    "deferred share credited"
                );
                let record = divergence(f, &hash_b)
                    .await?
                    .context("no decision record")?;
                ensure!(
                    record.decision.as_deref() == Some("abandoned"),
                    "{record:?}"
                );
                ensure!(record.ceiling == Some(500_000), "{record:?}");
                ensure!(record.bound.context("no bound")? > 500_000, "{record:?}");
                ensure!(record.overpay.is_none(), "it never landed: {record:?}");
                ensure!(
                    metric(
                        &f.b,
                        "qbit_prism_capture_offer_decisions_total{decision=\"abandoned_ceiling\"}"
                    ) == 1.
                );
                ensure!(
                    metric(
                        &f.b,
                        "qbit_prism_capture_offer_decisions_total{decision=\"offered\"}"
                    ) == 0.
                );
                let line = &integrity(f).await?["payout_divergence"];
                ensure!(
                    line["offers_abandoned_by_ceiling"] == 1 && line["divergent_landings"] == 0,
                    "{line}"
                );
                Ok(())
            })
        },
    )
    .await
}

/// A ceiling of 0 is the pre-#478 behaviour: a block on superseded work is
/// refused `stale-job` at submit, and a pending one whose revision moved
/// before its offer is abandoned unoffered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_zero_ceiling_turns_capture_off_at_submit_and_at_offer() -> Result<()> {
    run_tuned(
        gate::site!(),
        |config| config.capture_overpay_ceiling_bps = 0,
        |f| {
            Box::pin(async move {
                f.refresh(true).await?;
                f.node.accept_blocks();
                // At submit.
                let (worker, job) = issue(&f.a, "off", "1a2b3c4d").await?;
                bump_revision_same_parent(f).await?;
                f.a.refresh_once().await?;
                let (proof, hash) = find_block_proof(&job)?;
                let refused =
                    f.a.submit(&worker, &job, proof, false.into())
                        .await
                        .expect_err("capture is off");
                ensure!(
                    refused.reason_id.as_deref() == Some("stale-job"),
                    "{refused}"
                );
                ensure!(state(f, &hash).await?.is_none(), "nothing may be enqueued");
                // At offer: current when enqueued, superseded before the offer.
                let (worker, job) = issue(&f.a, "late", "1a2b3c4e").await?;
                let (proof, hash) = find_block_proof(&job)?;
                f.a.submit(&worker, &job, proof, false.into()).await?;
                bump_revision_same_parent(f).await?;
                land_next(f, &hash).await?;
                let (state, error) = outcome(f, &hash).await?;
                ensure!(state == "abandoned", "{state}");
                ensure!(
                    error.as_deref() == Some("payout revision or parent superseded"),
                    "{error:?}"
                );
                ensure!(
                    metric(
                        &f.a,
                        "qbit_prism_capture_offer_decisions_total{decision=\"abandoned_disabled\"}"
                    ) == 1.
                );
                // A revision that moves after the coordinator's cached screen
                // but before the reservation reaches the reservation itself:
                // it refuses the block and records the decision as
                // `disabled`, never as a ceiling abandon.
                f.a.refresh_once().await?;
                let (worker, job) = issue(&f.a, "race", "1a2b3c4f").await?;
                let (proof, hash) = find_block_proof(&job)?;
                f.a.submit(&worker, &job, proof, false.into()).await?;
                bump_revision_same_parent(f).await?;
                let claim = f.a.ledger.claim_candidate(60).await?.context("no claim")?;
                ensure!(claim.candidate.block_hash == hash);
                let OfferReservation::Refused(bound) =
                    f.a.ledger.reserve_offer_within(&claim, Some(0)).await?
                else {
                    anyhow::bail!("capture off must refuse a superseded reservation");
                };
                ensure!(bound.ceiling_bps == 0, "{bound:?}");
                let record = divergence(f, &hash).await?.context("no decision record")?;
                ensure!(record.decision.as_deref() == Some("disabled"), "{record:?}");
                let line = &integrity(f).await?["payout_divergence"];
                ensure!(
                    line["offers_refused_capture_off"] == 2
                        && line["offers_abandoned_by_ceiling"] == 0,
                    "{line}"
                );
                Ok(())
            })
        },
    )
    .await
}

/// A settlement bump that commits while an offer is being reserved cannot
/// slip past the ceiling: the reservation reads the revision `FOR SHARE`, so
/// it waits for the bump and then bounds the block as superseded, instead of
/// reserving it as current on a stale read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bump_concurrent_with_the_reservation_is_bounded_not_bypassed() -> Result<()> {
    run(gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let (worker, job) = issue(&f.a, "concurrent", "1a2b3c4d").await?;
            let (proof, hash) = find_block_proof(&job)?;
            f.a.submit(&worker, &job, proof, false.into()).await?;
            let claim = f.a.ledger.claim_candidate(60).await?.context("no claim")?;
            ensure!(claim.candidate.block_hash == hash);
            // The bump is written but not committed yet.
            let mut bump = f.pool().begin().await?;
            sqlx::query("UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1,updated_at=clock_timestamp() WHERE singleton")
                .execute(&mut *bump)
                .await?;
            let ledger = f.a.ledger.clone();
            let reservation =
                tokio::spawn(async move { ledger.reserve_offer_within(&claim, Some(100)).await });
            tokio::time::sleep(Duration::from_millis(500)).await;
            ensure!(
                !reservation.is_finished(),
                "the reservation read the revision past an uncommitted bump"
            );
            bump.commit().await?;
            let reserved = reservation.await??;
            let OfferReservation::Reserved { bound: Some(bound) } = reserved else {
                anyhow::bail!("the block must be bounded as superseded: {reserved:?}");
            };
            ensure!(bound.observed_revision > job.wire.payout_revision, "{bound:?}");
            let record = divergence(f, &hash).await?.context("no decision record")?;
            ensure!(record.decision.as_deref() == Some("offered"), "{record:?}");
            ensure!(state(f, &hash).await?.as_deref() == Some("offer_reserved"));
            Ok(())
        })
    })
    .await
}

/// A bound that cannot be computed (here, the capture's as-issued snapshot is
/// gone) is an offer failure like any other: nothing is reserved, nothing is
/// abandoned, nothing is decided, and the row stays pending for a retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unknown_bound_abandons_nothing() -> Result<()> {
    run(gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let (worker, job) = issue(&f.a, "unknown", "1a2b3c4d").await?;
            bump_revision_same_parent(f).await?;
            f.a.refresh_once().await?;
            let (proof, hash) = find_block_proof(&job)?;
            f.node.accept_blocks();
            let submit = tokio::spawn({
                let a = f.a.clone();
                async move { a.submit(&worker, &job, proof, false.into()).await }
            });
            wait_enqueued(f, &hash).await?;
            sqlx::query("DELETE FROM qbit_prism_balance_snapshots WHERE prior_balances_digest=(SELECT window_prior_balances_sha256 FROM qbit_block_candidate_outbox WHERE block_hash=$1)")
                .bind(&hash)
                .execute(f.pool())
                .await?;
            let claim = f.a.ledger.claim_candidate(60).await?.context("no claim")?;
            let error = f
                .a
                .process_candidate(&claim)
                .await
                .expect_err("an unknown bound must fail the offer");
            ensure!(format!("{error:#}").contains("overpay bound is unknown"), "{error:#}");
            ensure!(state(f, &hash).await?.as_deref() == Some("pending"), "not abandoned");
            ensure!(divergence(f, &hash).await?.is_none(), "no decision was made");
            let tip = f.a.rpc.call("getbestblockhash", json!([])).await?;
            ensure!(tip == json!(job_parent(f, &hash).await?), "the block was offered");
            submit.abort();
            Ok(())
        })
    })
    .await
}

// ---------------------------------------------------------------------------
// Helpers.

/// A report amount, which the report serializes as text.
fn sats(value: &Value) -> Option<i128> {
    value.as_str()?.parse().ok()
}

/// Decision, bound, ceiling, overpay and pool debt after, as text.
type DivergenceRow = (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

#[derive(Debug)]
struct Divergence {
    decision: Option<String>,
    bound: Option<i128>,
    ceiling: Option<i128>,
    overpay: Option<i128>,
    pool_debt_after: Option<i128>,
}

async fn divergence(f: &Fixture, hash: &str) -> Result<Option<Divergence>> {
    let row: Option<DivergenceRow> =
        sqlx::query_as("SELECT offer_decision,overpay_bound_sats::text,overpay_ceiling_sats::text,overpay_sats::text,pool_debt_after_sats::text FROM qbit_prism_payout_divergences WHERE block_hash=$1")
            .bind(hash)
            .fetch_optional(f.pool())
            .await?;
    let parse = |value: Option<String>| value.map(|v| v.parse::<i128>()).transpose();
    row.map(|(decision, bound, ceiling, overpay, debt)| {
        Ok(Divergence {
            decision,
            bound: parse(bound)?,
            ceiling: parse(ceiling)?,
            overpay: parse(overpay)?,
            pool_debt_after: parse(debt)?,
        })
    })
    .transpose()
}

async fn overpaid_accounts(f: &Fixture, hash: &str) -> Result<Vec<(String, i128)>> {
    sqlx::query_as::<_, (String, String)>(
        "SELECT encode(p2mr_program,'hex'),overpay_sats::text FROM qbit_prism_payout_divergence_accounts WHERE block_hash=$1 ORDER BY 1",
    )
    .bind(hash)
    .fetch_all(f.pool())
    .await?
    .into_iter()
    .map(|(program, sats)| Ok((program, sats.parse()?)))
    .collect()
}

async fn integrity(f: &Fixture) -> Result<Value> {
    let mut report: Value = sqlx::query_scalar("SELECT qbit_carry_forward_integrity_report()")
        .fetch_one(f.pool())
        .await?;
    report["payout_divergence"] =
        sqlx::query_scalar("SELECT qbit_prism_payout_divergence_report()")
            .fetch_one(f.pool())
            .await?;
    Ok(report)
}

fn metric(frontend: &Coordinator, series: &str) -> f64 {
    let body = frontend.metrics.render();
    body.lines()
        .find_map(|line| line.strip_prefix(&format!("{series} ")))
        .and_then(|value| value.parse().ok())
        .unwrap_or(f64::NAN)
}

async fn state(f: &Fixture, hash: &str) -> Result<Option<String>> {
    Ok(
        sqlx::query_scalar("SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$1")
            .bind(hash)
            .fetch_optional(f.pool())
            .await?,
    )
}

async fn outcome(f: &Fixture, hash: &str) -> Result<(String, Option<String>)> {
    Ok(sqlx::query_as(
        "SELECT state,last_error FROM qbit_block_candidate_outbox WHERE block_hash=$1",
    )
    .bind(hash)
    .fetch_one(f.pool())
    .await?)
}

async fn job_parent(f: &Fixture, hash: &str) -> Result<String> {
    let bytes: Vec<u8> = sqlx::query_scalar(
        "SELECT block_bytes FROM qbit_block_candidate_outbox WHERE block_hash=$1",
    )
    .bind(hash)
    .fetch_one(f.pool())
    .await?;
    let mut parent = bytes[4..36].to_vec();
    parent.reverse();
    Ok(hex::encode(parent))
}

async fn credited(f: &Fixture, worker: &Worker, hash: &str) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger WHERE share_id=$1")
            .bind(format!("{}:{}", worker.username, hash))
            .fetch_one(f.pool())
            .await?,
    )
}

async fn issue(
    frontend: &Coordinator,
    name: &str,
    extranonce1: &str,
) -> Result<(Worker, MiningJob<JobContext>)> {
    let worker = frontend.authorize(&format!("{name}.rig")).await?;
    let job = frontend
        .build_job(&worker, extranonce1, DIFFICULTY, 0.0)
        .await?;
    frontend
        .persist_issued_job(&worker, &job, 0, Duration::from_secs(30))
        .await?;
    Ok((worker, job))
}

/// An ordinary current block, enqueued on frontend a, whose block then
/// reaches the node (the offer happened), so the node's tip moves to it
/// before it lands.
async fn own_block_on_the_node(
    f: &Fixture,
    name: &str,
    extranonce1: &str,
) -> Result<(String, Worker)> {
    let (worker, job) = issue(&f.a, name, extranonce1).await?;
    let (proof, hash) = find_block_proof(&job)?;
    let block = proof.block_hex.clone();
    f.node.accept_blocks();
    f.a.submit(&worker, &job, proof, false.into()).await?;
    let reply = f.a.rpc.call("submitblock", json!([block])).await?;
    ensure!(reply.is_null(), "fake node refused {name}: {reply}");
    Ok((hash, worker))
}

async fn seed_carry(f: &Fixture, program: &str, sats: i64) -> Result<()> {
    let hash = "5e".repeat(32);
    // Keep the seed block active through every reconcile.
    f.node
        .set_reply("getblockhash", json!([50]), json!(hash.clone()));
    let mut tx = f.pool().begin().await?;
    sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state) VALUES($1,50,repeat('00',32),repeat('5e',32),repeat('5e',32),'prepared')")
        .bind(&hash).execute(&mut *tx).await?;
    sqlx::query("INSERT INTO qbit_payout_carry_forward(block_hash,block_height,miner_id,payout_order_key,p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,onchain_amount_sats,carry_forward_balance_sats,action) VALUES($1,50,'seed-aa','k',decode($2,'hex'),$3,0,$3,0,$3,'accrued')")
        .bind(&hash).bind(program).bind(sats).execute(&mut *tx).await?;
    sqlx::query("UPDATE qbit_pool_blocks SET chain_state='confirmed' WHERE block_hash=$1")
        .bind(&hash)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

async fn balance(f: &Fixture, program: &str) -> Result<i64> {
    let value: Option<String> = sqlx::query_scalar(
        "SELECT balance_sats::text FROM qbit_current_carry_forward_balances() WHERE p2mr_program=decode($1,'hex')",
    )
    .bind(program)
    .fetch_optional(f.pool())
    .await?;
    Ok(value.map(|v| v.parse::<i64>()).transpose()?.unwrap_or(0))
}

/// The same-parent payout-revision bump a landing's settlement produces.
async fn bump_revision_same_parent(f: &Fixture) -> Result<()> {
    let bumped = sqlx::query(
        "UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1,updated_at=clock_timestamp() WHERE singleton",
    )
    .execute(f.pool())
    .await?
    .rows_affected();
    ensure!(bumped == 1, "the singleton cluster row was not bumped");
    Ok(())
}

async fn land_next(f: &Fixture, hash: &str) -> Result<()> {
    let claim =
        f.a.ledger
            .claim_candidate(60)
            .await?
            .context("no candidate to claim")?;
    ensure!(claim.candidate.block_hash == hash, "claimed another block");
    f.a.process_candidate(&claim).await
}

async fn land_all(f: &Fixture) -> Result<()> {
    while let Some(claim) = f.a.ledger.claim_candidate(60).await? {
        f.a.process_candidate(&claim).await?;
    }
    Ok(())
}

async fn wait_enqueued(f: &Fixture, hash: &str) -> Result<()> {
    for _ in 0..400 {
        if state(f, hash).await?.is_some() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    anyhow::bail!("the captured candidate was never enqueued")
}

/// Claim the captured candidate once it is enqueued and run its offer
/// lifecycle, as the runtime would, while its submit waits.
async fn drive(f: &Fixture, hash: &str) -> Result<()> {
    wait_enqueued(f, hash).await?;
    let claim = f.b.ledger.claim_candidate(60).await?.context("no claim")?;
    ensure!(claim.candidate.block_hash == hash, "claimed another block");
    f.b.process_candidate(&claim).await
}

fn find_block_proof(job: &MiningJob<JobContext>) -> Result<(codec::Submission, String)> {
    for nonce in 0..10_000u32 {
        let proof = job.wire.assemble_submission(
            &"00".repeat(job.wire.extranonce2_size),
            &format!("{:08x}", job.wire.ntime),
            &format!("{nonce:08x}"),
            None,
            0,
        )?;
        if proof.block_pass {
            let hash = proof.block_hash_hex.clone();
            return Ok((proof, hash));
        }
    }
    anyhow::bail!("no block proof in the bounded search")
}
