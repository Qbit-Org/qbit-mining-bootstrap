//! #574: a block-bearing submission's acknowledgement is capped at
//! `share_commit_timeout`, not `max(share_commit_timeout, 60 s)`.
//!
//! A captured (#478) block proof's share is deferred to its block's landing,
//! and its answer used to wait up to 60 s for that landing while the miner's
//! session could submit nothing else. Both tests hold the block's database
//! write with a trigger that waits for the test to release it, answer the
//! miner at the cap with the reply every share still pending at its deadline
//! gets (`20`, "share outcome is not yet known", `ledger-outcome-unknown`),
//! count it in `qbit_prism_block_proof_ack_capped_total`, and credit the share
//! exactly once after the release.
//!
//! - A captured proof whose landing is held at its `offered` record.
//! - A share-pass proof carrying a current-revision block whose append is held
//!   at its candidate insert.
//!
//! Both frontends take their block bound from
//! `config::block_only_ack_timeout`, the function `Config::from_env` uses, so
//! restoring the 60 s floor there fails both at the cap.
//!
//! Run through: test/prism-native-tests.sh cargo-args --locked -p
//! qbit-prism-server --test b574_ack_cap -- --nocapture

use anyhow::{ensure, Context, Result};
use qbit_prism_server::{
    codec,
    config::{self, Config},
    coordinator::{Coordinator, JobContext},
    stratum::{MiningBackend, MiningJob, Worker},
};
use qbit_prism_test_gate as gate;
use std::time::{Duration, Instant};

#[allow(dead_code)]
#[path = "support/compact_runtime_e2e/mod.rs"]
mod support;
use support::{run_tuned, Fixture, DIFFICULTY};

/// Short, so the held write stays well inside the ledger's 15 s
/// `statement_timeout` and the test inside the fixture's 45 s budget.
const SHARE_COMMIT: Duration = Duration::from_secs(3);
/// Scheduling slack past the cap. A 60 s bound misses it by 50 s.
const SLACK: Duration = Duration::from_secs(7);

fn tune(config: &mut Config) {
    config.share_commit_timeout = SHARE_COMMIT;
    config.block_only_ack_timeout = config::block_only_ack_timeout(SHARE_COMMIT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_captured_proof_is_answered_at_the_cap_and_credited_after_its_landing() -> Result<()> {
    run_tuned(gate::site!(), tune, |f| Box::pin(captured(f))).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_carrying_share_append_is_answered_at_the_cap_and_credited_once() -> Result<()> {
    run_tuned(gate::site!(), tune, |f| Box::pin(share_pass(f))).await
}

async fn captured(f: &Fixture) -> Result<()> {
    f.refresh(true).await?;
    let worker = f.a.authorize("b574.captured").await?;
    let job = issue(f, &worker).await?;
    // A landing settles: the payout revision moves, the parent does not, so
    // the job's block is captured with its share deferred (#478).
    let bumped = sqlx::query(
        "UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1,updated_at=clock_timestamp() WHERE singleton",
    )
    .execute(f.pool())
    .await?
    .rows_affected();
    ensure!(bumped == 1, "the singleton cluster row was not bumped");
    f.a.refresh_once().await?;
    f.node.accept_blocks();
    let (proof, hash) = find_block_proof(&job)?;
    let share_id = format!("{}:{hash}", worker.username);
    // The node accepts the block; its landing then stops at the offered record,
    // before the confirmation that credits the deferred share.
    hold(
        f,
        "BEFORE UPDATE ON qbit_block_candidate_outbox FOR EACH ROW WHEN (OLD.state='offer_reserved' AND NEW.state='offered')",
    )
    .await?;
    let answer = async {
        let checked = async {
            let refused = answer_at_cap(f, &worker, &job, proof).await?;
            ensure!(
                held(f).await?,
                "the landing never reached the hold, so it was not in flight"
            );
            ensure!(
                credited(f, &share_id).await? == 0,
                "the deferred share was credited before its landing finished"
            );
            ensure!(
                capped(&f.a, "block_only") == 1. && capped(&f.a, "share") == 0.,
                "the capped acknowledgement was not counted once on the block_only path: {refused:?}"
            );
            anyhow::Ok(())
        }
        .await;
        // Release on every path, so a failed check does not leave the landing
        // waiting out its loop.
        let released = release(f).await;
        checked.and(released)
    };
    let (answered, landed) = tokio::join!(answer, drive_candidate(f, &hash));
    answered?;
    landed?;
    let state: Option<String> =
        sqlx::query_scalar("SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$1")
            .bind(&hash)
            .fetch_optional(f.pool())
            .await?;
    ensure!(
        state.as_deref() == Some("submitted"),
        "the block did not land: outbox state {state:?}"
    );
    ensure!(
        credited(f, &share_id).await? == 1,
        "the deferred share was not credited exactly once after its landing"
    );
    Ok(())
}

async fn share_pass(f: &Fixture) -> Result<()> {
    f.refresh(true).await?;
    let worker = f.a.authorize("b574.share").await?;
    let job = issue(f, &worker).await?;
    f.node.accept_blocks();
    let (proof, hash) = find_block_proof(&job)?;
    ensure!(
        proof.share_pass,
        "the block proof must also pass the share target"
    );
    let share_id = format!("{}:{hash}", worker.username);
    // The append that commits the share with its block stops at the candidate
    // insert. It is never refused, so it runs on past the answer.
    hold(
        f,
        "BEFORE INSERT ON qbit_block_candidate_outbox FOR EACH ROW",
    )
    .await?;
    let answered = async {
        let refused = answer_at_cap(f, &worker, &job, proof).await?;
        ensure!(
            held(f).await?,
            "the append never reached the hold, so it was not in flight"
        );
        ensure!(
            credited(f, &share_id).await? == 0,
            "the share was credited before its append committed"
        );
        ensure!(
            capped(&f.a, "share") == 1. && capped(&f.a, "block_only") == 0.,
            "the capped acknowledgement was not counted once on the share path: {refused:?}"
        );
        anyhow::Ok(())
    }
    .await;
    let released = release(f).await;
    answered?;
    released?;
    // The followed append commits the share and the block together.
    let deadline = Instant::now() + Duration::from_secs(10);
    while credited(f, &share_id).await? == 0 {
        ensure!(
            Instant::now() < deadline,
            "the append never committed after its release"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    // Landing the block adds no second credit: the share was not deferred.
    drive_candidate(f, &hash).await?;
    let state: Option<String> =
        sqlx::query_scalar("SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$1")
            .bind(&hash)
            .fetch_optional(f.pool())
            .await?;
    ensure!(
        state.as_deref() == Some("submitted"),
        "the block did not land: outbox state {state:?}"
    );
    ensure!(
        credited(f, &share_id).await? == 1,
        "the share was not credited exactly once"
    );
    Ok(())
}

async fn issue(f: &Fixture, worker: &Worker) -> Result<MiningJob<JobContext>> {
    let job = f.a.build_job(worker, "1a2b3c4d", DIFFICULTY, 0.0).await?;
    f.a.persist_issued_job(worker, &job, 0, Duration::from_secs(30))
        .await?;
    Ok(job)
}

/// Submit and require the answer at the cap: not before it, not after
/// `SLACK`, and the reply a share still pending at its deadline gets.
async fn answer_at_cap(
    f: &Fixture,
    worker: &Worker,
    job: &MiningJob<JobContext>,
    proof: codec::Submission,
) -> Result<qbit_prism_server::stratum::StratumError> {
    let started = Instant::now();
    let answer = tokio::time::timeout(
        SHARE_COMMIT + SLACK,
        f.a.submit(worker, job, proof, false.into()),
    )
    .await
    .with_context(|| {
        format!(
            "the block proof was not answered within {:?} of a {SHARE_COMMIT:?} share commit timeout: its acknowledgement is not capped there",
            SHARE_COMMIT + SLACK
        )
    })?;
    let elapsed = started.elapsed();
    let refused = match answer {
        Ok(()) => anyhow::bail!("the proof was accepted while its block write was held"),
        Err(refused) => refused,
    };
    ensure!(
        refused.code == 20
            && refused.reason_id.as_deref() == Some("ledger-outcome-unknown")
            && refused.message == "share outcome is not yet known",
        "the capped proof did not get the pending-outcome reply: {refused:?}"
    );
    ensure!(
        elapsed >= SHARE_COMMIT,
        "answered after {elapsed:?}, before the {SHARE_COMMIT:?} cap"
    );
    Ok(refused)
}

/// Install a one-shot hold: the first row the trigger fires for waits until
/// `release`, or at most 10 s, inside its own statement and transaction.
async fn hold(f: &Fixture, trigger: &str) -> Result<()> {
    sqlx::raw_sql(&format!(
        r#"
        CREATE SEQUENCE b574_hold_reached;
        CREATE SEQUENCE b574_hold_released;
        CREATE FUNCTION b574_hold() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF (SELECT is_called FROM b574_hold_reached) THEN RETURN NEW; END IF;
            PERFORM nextval('b574_hold_reached');
            FOR i IN 1..500 LOOP
                EXIT WHEN (SELECT is_called FROM b574_hold_released);
                PERFORM pg_sleep(0.02);
            END LOOP;
            RETURN NEW;
        END $$;
        CREATE TRIGGER b574_hold {trigger} EXECUTE FUNCTION b574_hold();
        "#
    ))
    .execute(f.pool())
    .await?;
    Ok(())
}

/// Sequences are not transactional, so the held transaction's progress and
/// the release are visible across sessions at once.
async fn held(f: &Fixture) -> Result<bool> {
    Ok(
        sqlx::query_scalar("SELECT is_called FROM b574_hold_reached")
            .fetch_one(f.pool())
            .await?,
    )
}

async fn release(f: &Fixture) -> Result<()> {
    sqlx::query("SELECT setval('b574_hold_released', 1, true)")
        .execute(f.pool())
        .await?;
    Ok(())
}

async fn credited(f: &Fixture, share_id: &str) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger WHERE share_id=$1")
            .bind(share_id)
            .fetch_one(f.pool())
            .await?,
    )
}

fn capped(frontend: &Coordinator, path: &str) -> f64 {
    let series = format!("qbit_prism_block_proof_ack_capped_total{{path=\"{path}\"}} ");
    frontend
        .metrics
        .render()
        .lines()
        .find_map(|line| line.strip_prefix(&series))
        .and_then(|value| value.parse().ok())
        .unwrap_or(f64::NAN)
}

/// A network-target solution on `job`, from the same bounded search as the
/// #478 capture tests.
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

/// Claim the candidate once enqueued and run it through the offer lifecycle,
/// as the coordinator's runtime does.
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
    anyhow::bail!("the block proof produced no candidate to offer")
}
