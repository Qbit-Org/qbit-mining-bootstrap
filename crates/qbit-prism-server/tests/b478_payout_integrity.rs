//! #478 payout integrity, adopted from the round-2 payout-integrity review
//! (its three tests failed on the round-2 candidate by design; they pin the
//! fixes):
//!
//! 1. The divergence records, the counters and the debt gauge carry the debt
//!    the balances carry, also when landings interleave: debt is realized at
//!    CONFIRMATION, when a block's carry rows start to count, and that is
//!    where it is recorded.
//! 2. A capture-off abandonment on the ordinary pre-offer path is recorded
//!    as decision `disabled`.
//! 3. With the real payout policy, a later current block that confirms
//!    before a captured block drives the captured block's realized debt up
//!    to its whole positive as-issued float. That float, not a bound taken
//!    from the balances at the reservation, is what the ceiling holds: it
//!    covers the realized debt in every order.
use anyhow::{ensure, Context, Result};
use qbit_prism::{
    apply_payout_policy, CarryForwardBalance, PayoutPolicy, PayoutPolicyAccount,
    PayoutPolicyAccountType, PoolFeePolicy, PrismRewardManifest,
};
use qbit_prism_server::{
    codec,
    coordinator::{Coordinator, JobContext},
    ledger::{landing_divergence, overpay_bound, overpay_ceiling_sats},
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

/// Block hash, offer decision, bound and overpay, as text.
type RecordRow = (String, Option<String>, Option<String>, Option<String>);

fn aa() -> String {
    "aa".repeat(32)
}

// ---------------------------------------------------------------------------
// 1. PostgreSQL-gated: a landing that precedes another block's confirmation.

/// A lands but does not confirm yet (its node view does not show it active,
/// so it waits in reconciliation, as after any post-landing confirmation
/// failure). B's work was issued on tip A before A confirmed, so it snapshots
/// the pre-A balances. B is current at its offer and lands against the same
/// pre-A balances: not divergent. B confirms, then A confirms. `aa` is paid
/// its pre-A carry twice. The balances carry the debt; the divergence records,
/// the counters and the gauge must carry it too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn debt_realized_at_confirmation_is_recorded_when_landings_interleave() -> Result<()> {
    run(gate::site!(), |f| {
        Box::pin(async move { interleaved(f).await })
    })
    .await
}

async fn interleaved(f: &Fixture) -> Result<()> {
    let aa = aa();
    seed_carry(f, &aa, SEED_CARRY).await?;
    f.refresh(true).await?;
    ensure!(
        balance(f, &aa).await? == SEED_CARRY,
        "seed carry not current"
    );

    // A: an ordinary current block on frontend a.
    let height_a = f.a.rpc.call("getblockchaininfo", json!([])).await?["blocks"]
        .as_u64()
        .context("no node height")?
        + 1;
    let (wa, ja) = issue(&f.a, "landa", "1a2b3c4d").await?;
    let (pa, hash_a) = find_block_proof(&ja)?;
    f.node.accept_blocks();
    // After its offer the node reports another block at A's height, so A's
    // post-landing observation is "not active": A lands, is not confirmed,
    // and waits in reconciliation.
    let other = "ee".repeat(32);
    f.node
        .set_reply("getblockhash", json!([height_a]), json!(other.clone()));
    f.a.submit(&wa, &ja, pa, false.into()).await?;
    land_next(f, &f.a, &hash_a).await?;
    ensure!(
        state(f, &hash_a).await?.as_deref() == Some("reconciliation"),
        "A was not held in reconciliation: {:?}",
        state(f, &hash_a).await?
    );
    ensure!(
        block_state(f, &hash_a).await?.as_deref() == Some("prepared"),
        "A did not land prepared"
    );
    ensure!(
        balance(f, &aa).await? == SEED_CARRY,
        "A's rows count already"
    );

    // B: frontend b issues work on tip A. A is landed but not confirmed, so
    // B's as-issued balances are the pre-A balances.
    f.b.refresh_once().await?;
    let (wb, jb) = issue(&f.b, "landb", "1a2b3c4e").await?;
    ensure!(jb.wire.previousblockhash == hash_a, "B's job is not on A");
    let (pb, hash_b) = find_block_proof(&jb)?;
    f.b.submit(&wb, &jb, pb, false.into()).await?;
    land_next(f, &f.b, &hash_b).await?;
    ensure!(
        state(f, &hash_b).await?.as_deref() == Some("submitted"),
        "B did not land and confirm: {:?}",
        state(f, &hash_b).await?
    );
    let after_b = balance(f, &aa).await?;

    // A's node view recovers and its reconciliation confirms it.
    f.node
        .set_reply("getblockhash", json!([height_a]), json!(hash_a.clone()));
    sqlx::query("UPDATE qbit_block_candidate_outbox SET next_attempt_at=clock_timestamp() WHERE block_hash=$1")
        .bind(&hash_a)
        .execute(f.pool())
        .await?;
    land_next(f, &f.a, &hash_a).await?;
    ensure!(
        state(f, &hash_a).await?.as_deref() == Some("submitted"),
        "A did not confirm: {:?}",
        state(f, &hash_a).await?
    );

    // Evidence.
    let debt = -balance(f, &aa).await?;
    let integrity = integrity(f).await?;
    let records: Vec<RecordRow> = sqlx::query_as(
        "SELECT block_hash,offer_decision,overpay_bound_sats::text,overpay_sats::text FROM qbit_prism_payout_divergences ORDER BY block_hash",
    )
    .fetch_all(f.pool())
    .await?;
    let recorded: i128 = records
        .iter()
        .filter_map(|(_, _, _, overpay)| overpay.as_deref())
        .map(|sats| sats.parse::<i128>())
        .sum::<Result<i128, _>>()?;
    let gauge_a = metric(&f.a, "qbit_prism_carry_forward_debt_sats");
    let gauge_b = metric(&f.b, "qbit_prism_carry_forward_debt_sats");
    let landings_a = metric(&f.a, "qbit_prism_divergent_landings_total");
    let landings_b = metric(&f.b, "qbit_prism_divergent_landings_total");
    println!(
        "B478-EVIDENCE aa after B={after_b} final={}; debt={debt}; records={records:?}; recorded_overpay={recorded}; \
         payout_divergence={}; drift={} mismatch={}; gauge a={gauge_a} b={gauge_b}; divergent_landings_total a={landings_a} b={landings_b}",
        -debt,
        integrity["payout_divergence"],
        integrity["current_drift_count"],
        integrity["mismatch_count"],
    );
    // Controls: the ledger is exact and aa really was paid twice.
    ensure!(
        integrity["current_drift_count"] == 0 && integrity["mismatch_count"] == 0,
        "ledger not exact: {integrity}"
    );
    ensure!(debt > 0, "aa was not paid twice (control): debt {debt}");
    ensure!(
        sats(&integrity["payout_divergence"]["debt_sats"]) == Some(i128::from(debt)),
        "the balance-derived debt line is wrong (control)"
    );
    // The claim under test.
    ensure!(
        recorded == i128::from(debt),
        "the divergence records do not carry the debt the balances carry: recorded {recorded} vs debt {debt}"
    );
    ensure!(
        landings_a + landings_b >= 1.,
        "no divergent landing was counted for a double payment of {debt} sats"
    );
    ensure!(
        gauge_a == debt as f64 || gauge_b == debt as f64,
        "qbit_prism_carry_forward_debt_sats shows a={gauge_a} b={gauge_b} while the balances carry {debt}"
    );
    Ok(())
}

/// With a ceiling of 0 a pending block whose revision moved is abandoned at
/// offer by the ordinary pre-offer probe, and recorded as decision
/// `disabled`, not only when a reservation races the bump.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_capture_off_abandonment_is_recorded_as_disabled() -> Result<()> {
    run_tuned(
        gate::site!(),
        |config| config.capture_overpay_ceiling_bps = 0,
        |f| {
            Box::pin(async move {
                f.refresh(true).await?;
                f.node.accept_blocks();
                let (worker, job) = issue(&f.a, "late", "1a2b3c4e").await?;
                let (proof, hash) = find_block_proof(&job)?;
                f.a.submit(&worker, &job, proof, false.into()).await?;
                sqlx::query("UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1 WHERE singleton")
                    .execute(f.pool())
                    .await?;
                land_next(f, &f.a, &hash).await?;
                let row: Option<(String, Option<String>)> = sqlx::query_as(
                    "SELECT o.state,d.offer_decision FROM qbit_block_candidate_outbox o LEFT JOIN qbit_prism_payout_divergences d USING(block_hash) WHERE o.block_hash=$1",
                )
                .bind(&hash)
                .fetch_optional(f.pool())
                .await?;
                let metric_disabled = metric(
                    &f.a,
                    "qbit_prism_capture_offer_decisions_total{decision=\"abandoned_disabled\"}",
                );
                let line = &integrity(f).await?["payout_divergence"];
                println!("B478-OFF row={row:?} metric_abandoned_disabled={metric_disabled} report={line}");
                ensure!(
                    row.as_ref().map(|(state, _)| state.as_str()) == Some("abandoned"),
                    "control: the block was not abandoned: {row:?}"
                );
                ensure!(metric_disabled == 1., "control: metric {metric_disabled}");
                ensure!(
                    row.and_then(|(_, decision)| decision).as_deref() == Some("disabled")
                        && line["offers_refused_capture_off"] == 1,
                    "the capture-off abandonment wrote no `disabled` record while the metric counted it: report {line}"
                );
                Ok(())
            })
        },
    )
    .await
}

// ---------------------------------------------------------------------------
// 2. The bound, under the real payout policy (no database).

const V: u64 = 5_000_000_000;
const FLOOR: u64 = 14_720;

fn policy() -> PayoutPolicy {
    PayoutPolicy {
        p2mr_spend_input_bytes: 1,
        target_feerate_sats_per_byte: 1,
        safety_multiplier: 1,
        min_output_sats: Some(FLOOR),
        pool_fee_policy: Some(PoolFeePolicy {
            fee_bps: 0,
            recipient_id: "pool".into(),
            order_key: "zz".into(),
            p2mr_program_hex: "ff".repeat(32),
        }),
        coinbase_output_policy: Default::default(),
    }
}

fn manifest(aa_gross: u64) -> Result<PrismRewardManifest> {
    Ok(serde_json::from_value(json!({
        "schema": "qbit.prism.reward-manifest.v1",
        "block_height": 101,
        "coinbase_value_sats": V,
        "network_difficulty": 1,
        "window_multiplier": 1,
        "requested_window_weight": 1,
        "counted_window_weight": 1,
        "anchor_job_issued_at_ms": 0,
        "anchor_share_seq": 0,
        "newest_share_seq": 0,
        "oldest_share_seq": 0,
        "included_share_count": 0,
        "share_slice_digest_hex": "00".repeat(32),
        "shares": [],
        "entitlements": [
            {"recipient_id": "aa", "order_key": "aa", "p2mr_program_hex": "aa".repeat(32), "weight": aa_gross},
            {"recipient_id": "bb", "order_key": "bb", "p2mr_program_hex": "bb".repeat(32), "weight": V - aa_gross},
        ],
    }))?)
}

fn balances(map: &[(&str, i128)]) -> Vec<CarryForwardBalance> {
    map.iter()
        .filter(|(_, sats)| *sats != 0)
        .map(|(name, sats)| CarryForwardBalance {
            recipient_id: (*name).into(),
            order_key: (*name).into(),
            p2mr_program_hex: name.repeat(32),
            balance_sats: *sats,
        })
        .collect()
}

fn get(balances: &[CarryForwardBalance], name: &str) -> i128 {
    balances
        .iter()
        .filter(|b| b.p2mr_program_hex == name.repeat(32))
        .map(|b| b.balance_sats)
        .sum()
}

/// Apply a landing's miner rows to balances, as confirmation does
/// (balance += gross - onchain).
fn confirm(
    before: &[CarryForwardBalance],
    accounts: &[PayoutPolicyAccount],
) -> Vec<CarryForwardBalance> {
    let mut out: Vec<(String, i128)> = ["aa", "bb"]
        .iter()
        .map(|name| (name.to_string(), get(before, name)))
        .collect();
    for account in accounts
        .iter()
        .filter(|a| a.account_type == PayoutPolicyAccountType::Miner)
    {
        let name = &account.p2mr_program_hex[..2];
        let entry = out
            .iter_mut()
            .find(|(n, _)| n == name)
            .expect("known account");
        entry.1 += i128::from(account.gross_amount_sats) - i128::from(account.onchain_amount_sats);
    }
    let refs: Vec<(&str, i128)> = out.iter().map(|(n, s)| (n.as_str(), *s)).collect();
    balances(&refs)
}

#[test]
fn a_later_current_block_confirming_first_stays_within_the_captured_bound() -> Result<()> {
    let policy = policy();
    // S0: aa has a sub-floor carry of 10,000.
    let s0 = balances(&[("aa", 10_000)]);
    // P (issued on S0): aa's gross 3,000 keeps it under the floor, so P
    // accrues it. P confirms: S1 = aa 13,000.
    let p = apply_payout_policy(&manifest(3_000)?, &s0, &policy)?;
    let s1 = confirm(&s0, &p.accounts);
    ensure!(get(&s1, "aa") == 13_000, "S1 {s1:?}");
    // X was issued on S0 before P confirmed: captured after P's settlement
    // bump. Its gross 5,000 lifts aa over the floor, so X pays aa out.
    let x = apply_payout_policy(&manifest(5_000)?, &s0, &policy)?;
    // X's bound is its positive as-issued float, whatever happens later.
    let f_x = overpay_bound(&s0);
    // Y's work is issued on tip X (after X's offer) from S1. Y is current at
    // its own offer and creates no debt itself.
    let y = apply_payout_policy(&manifest(3_000)?, &s1, &policy)?;
    let y_record = landing_divergence(&y.accounts, &s1).overpay_sats;
    // Order 1: X confirms before Y.
    let x_first = landing_divergence(&x.accounts, &s1).overpay_sats;
    // Order 2: X's landing stalls; Y lands and confirms first (S2); then X.
    let s2 = confirm(&s1, &y.accounts);
    let x_after_y = landing_divergence(&x.accounts, &s2).overpay_sats;
    let ceiling = overpay_ceiling_sats(V, 100);
    println!(
        "B478-BOUND S1={s1:?} S2={s2:?} F_X={f_x} Y_record={y_record} realized(X before Y)={x_first} \
         realized(X after Y)={x_after_y} ceiling={ceiling}"
    );
    ensure!(y_record == 0, "Y is current and creates no debt itself");
    // The order that broke the round-2 bound realizes the whole float...
    ensure!(x_after_y == 10_000, "realized {x_after_y}");
    // ...and the float bounds it, in both orders, within the default ceiling.
    ensure!(
        x_first <= f_x && x_after_y <= f_x,
        "realized {x_first}/{x_after_y} > F {f_x}"
    );
    ensure!(
        f_x == 10_000 && f_x <= ceiling,
        "F {f_x}, ceiling {ceiling}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers (as in b478_capture_overpay.rs).

fn sats(value: &Value) -> Option<i128> {
    value.as_str()?.parse().ok()
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
    frontend
        .metrics
        .render()
        .lines()
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

async fn block_state(f: &Fixture, hash: &str) -> Result<Option<String>> {
    Ok(
        sqlx::query_scalar("SELECT chain_state FROM qbit_pool_blocks WHERE block_hash=$1")
            .bind(hash)
            .fetch_optional(f.pool())
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

async fn seed_carry(f: &Fixture, program: &str, sats: i64) -> Result<()> {
    let hash = "5e".repeat(32);
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

async fn land_next(f: &Fixture, frontend: &Coordinator, hash: &str) -> Result<()> {
    let _ = f;
    let claim = frontend
        .ledger
        .claim_candidate(60)
        .await?
        .context("no candidate to claim")?;
    ensure!(claim.candidate.block_hash == hash, "claimed another block");
    frontend.process_candidate(&claim).await
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
