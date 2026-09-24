//! #478 block-capture pin: a block found on a job whose payout revision was
//! bumped **with the same parent** — the second, settlement bump every landing
//! produces — is a valid block on the current tip and must be offered to the
//! node and landed. Dropping it discards a full coinbase.
//!
//! The base **deliberately** dropped such a block, refusing the block-bearing
//! share `stale-job` (`coordinator/miner_submit.rs`, `StaleJobCause::PayoutRevision`)
//! before it decided whether the submission carried a block. Option B admits a
//! block-bearing proof on the current tip regardless of payout revision, defers
//! its solver share (credited only on confirmation, keeping the revision fence),
//! and lands it as-issued at the observed revision, carrying any payout
//! divergence as debt in the additive balance (`land_offered`,
//! `ledger/blocks.rs`).
//!
//! On the base this test FAILS at the submit (`21: stale job`); with Option B it
//! captures the block, which lands `submitted`, and the deferred solver share is
//! credited exactly once on confirmation. The capture ACK blocks on the block's
//! disposition (the block-only path), so a concurrent driver claims and
//! processes the candidate while the submit is in flight — as the coordinator's
//! runtime does in production.
//!
//! The second test pins block-only work (general review B1): a job a Stratum
//! session retired at a same-parent payout replacement is captured the same
//! way, but it reaches no credit path. A plain share on it is refused
//! `stale-job` with or without grace; its block is enqueued with no deferred
//! share and its share is refused `stale-job` at once, so confirmation
//! credits nothing and the miner is never told the share was accepted.
//!
//! Run through: test/prism-native-tests.sh cargo-args --locked -p
//! qbit-prism-server --test b478_stale_revision_block -- --nocapture

use anyhow::{ensure, Result};
use qbit_prism_server::{
    codec::{self, JobKind},
    coordinator::JobContext,
    stratum::{MiningBackend, MiningJob},
};
use qbit_prism_test_gate as gate;
use std::time::Duration;

#[allow(dead_code)]
#[path = "support/compact_runtime_e2e/mod.rs"]
mod support;
use support::{run, Fixture, DIFFICULTY};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_on_a_same_parent_revision_bumped_job_still_reaches_the_node() -> Result<()> {
    run(gate::site!(), |f| {
        Box::pin(async move { scenario(f).await })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn block_only_work_lands_its_block_but_earns_no_share_credit() -> Result<()> {
    run(gate::site!(), |f| {
        Box::pin(async move { block_only_scenario(f).await })
    })
    .await
}

async fn scenario(f: &Fixture) -> Result<()> {
    // Work is published on the node's tip at the current payout revision, and a
    // miner is issued a job against it.
    f.refresh(true).await?;
    let worker = f.a.authorize("b478.rig").await?;
    let job = f.a.build_job(&worker, "1a2b3c4d", DIFFICULTY, 0.0).await?;
    f.a.persist_issued_job(&worker, &job, 0, Duration::from_secs(30))
        .await?;
    let job_parent = job.wire.previousblockhash.clone();
    let job_revision = job.wire.payout_revision;

    // A landing settles: the payout revision is bumped with the SAME parent.
    // This is byte-for-byte the statement `bump_revision` runs on block
    // settlement (`ledger/blocks.rs`); the node's tip does not move, so
    // `job_parent` is still the chain tip and the job's solution is a valid
    // block on it.
    bump_revision_same_parent(f).await?;
    // The frontend catches up to the new revision on its next refresh, exactly
    // as every session did about 3 s after each landing. The miner is still
    // mining the job it captured before the bump.
    f.a.refresh_once().await?;
    ensure!(
        f.a.ledger.payout_revision().await? > job_revision,
        "the durable revision was not bumped past the issued job"
    );
    ensure!(
        f.a.build_job(&worker, "1a2b3c4d", DIFFICULTY, 0.0)
            .await?
            .wire
            .previousblockhash
            == job_parent,
        "the tip moved; this would no longer be the same-parent case"
    );

    // The miner finds a network-target solution on the job it holds. The node
    // is ready to accept the block.
    f.node.accept_blocks();
    let (proof, hash) = find_block_proof(&job)?;

    // Submit the block-bearing share and, concurrently, drive its candidate
    // through the offer lifecycle. On the base the submit returns `stale-job`
    // immediately (no candidate is ever enqueued) and this test fails there.
    let submit = f.a.submit(&worker, &job, proof, false.into());
    let (submit_res, drive_res) = tokio::join!(submit, drive_candidate(f, &hash));
    submit_res.map_err(|error| {
        anyhow::anyhow!(
            "#478: the server refused a block-bearing share on a job whose payout \
             revision was bumped with the parent unchanged: {error}"
        )
    })?;
    drive_res?;

    // The block landed at the node.
    let state: Option<String> =
        sqlx::query_scalar("SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$1")
            .bind(&hash)
            .fetch_optional(f.pool())
            .await?;
    ensure!(
        state.as_deref() == Some("submitted"),
        "the block did not land at the node: outbox state {state:?}"
    );
    // The deferred solver share is credited exactly once, on confirmation.
    let share_id = format!("{}:{}", worker.username, hash);
    let credited: i64 =
        sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger WHERE share_id=$1")
            .bind(&share_id)
            .fetch_one(f.pool())
            .await?;
    ensure!(
        credited == 1,
        "the deferred solver share was not credited exactly once: count {credited}"
    );
    Ok(())
}

async fn block_only_scenario(f: &Fixture) -> Result<()> {
    f.refresh(true).await?;
    let worker = f.a.authorize("b478.retired").await?;
    let mut job = f.a.build_job(&worker, "1a2b3c4d", DIFFICULTY, 0.0).await?;
    f.a.persist_issued_job(&worker, &job, 0, Duration::from_secs(30))
        .await?;
    bump_revision_same_parent(f).await?;
    f.a.refresh_once().await?;
    // What the Stratum session does to the work the replacement supersedes.
    job.wire.kind = JobKind::BlockOnly;

    // Every credit path is refused: a plain share, with or without grace.
    let (proof, hash) = find_block_proof(&job)?;
    let mut share_only = find_share_only_proof(&job)?;
    share_only.block_pass = false;
    for grace in [false, true] {
        let refused =
            f.a.submit(&worker, &job, share_only.clone(), grace.into())
                .await
                .expect_err("block-only work earned share credit");
        ensure!(
            refused.response(serde_json::json!(1))["error"][2]["reason_id"] == "stale-job",
            "unexpected refusal for a share on block-only work: {refused:?}"
        );
    }

    // Its block is still captured and lands, but its share is refused
    // `stale-job` as soon as the block is enqueued: nothing will ever credit
    // it, so the miner is not held until confirmation.
    f.node.accept_blocks();
    let refused =
        f.a.submit(&worker, &job, proof, false.into())
            .await
            .expect_err("block-only work's share must not be acknowledged");
    ensure!(
        refused.reason_id.as_deref() == Some("stale-job"),
        "unexpected answer for block-only work's block: {refused:?}"
    );
    drive_candidate(f, &hash).await?;
    let state: Option<String> =
        sqlx::query_scalar("SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$1")
            .bind(&hash)
            .fetch_optional(f.pool())
            .await?;
    ensure!(
        state.as_deref() == Some("submitted"),
        "the block did not land at the node: outbox state {state:?}"
    );
    // No deferred share was stored, and confirmation credited nothing.
    let deferred: i64 =
        sqlx::query_scalar("SELECT count(*) FROM qbit_prism_deferred_shares WHERE block_hash=$1")
            .bind(&hash)
            .fetch_one(f.pool())
            .await?;
    ensure!(
        deferred == 0,
        "block-only work stored a deferred share: {deferred}"
    );
    let credited: i64 =
        sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger WHERE share_id LIKE $1 || ':%'")
            .bind(&worker.username)
            .fetch_one(f.pool())
            .await?;
    ensure!(
        credited == 0,
        "block-only work was credited {credited} share(s)"
    );
    Ok(())
}

/// A share-target solution on `job`, used as a plain share.
fn find_share_only_proof(job: &MiningJob<JobContext>) -> Result<codec::Submission> {
    for nonce in 10_000..20_000u32 {
        let proof = job.wire.assemble_submission(
            &"00".repeat(job.wire.extranonce2_size),
            &format!("{:08x}", job.wire.ntime),
            &format!("{nonce:08x}"),
            None,
            0,
        )?;
        if proof.share_pass {
            return Ok(proof);
        }
    }
    anyhow::bail!("no share-target solution found in the bounded search")
}

/// The same-parent payout-revision bump a landing's settlement produces. This
/// is the exact statement `bump_revision` runs in `ledger/blocks.rs`.
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

/// Find a network-target solution on `job`; return the submission and its block
/// hash. The bounded search matches the landing fixtures' own block search.
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
    anyhow::bail!("no network-target solution found in the bounded search")
}

/// Claim the captured candidate as soon as it is enqueued and run it through
/// the offer lifecycle, as the coordinator's runtime would. Bounded so a bug
/// that never enqueues the candidate fails instead of hanging.
async fn drive_candidate(f: &Fixture, hash: &str) -> Result<()> {
    for _ in 0..600 {
        if let Some(claim) = f.a.ledger.claim_candidate(60).await? {
            ensure!(
                claim.candidate.block_hash == hash,
                "a different candidate was claimed"
            );
            return f.a.process_candidate(&claim).await;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    anyhow::bail!("#478: the block-bearing share produced no candidate to offer")
}
