//! #574: a block-bearing submission's acknowledgement is bounded by the share
//! deadline, not `max(share_commit_timeout, 60 s)`, and the bound only ends
//! the wait, never the work that offers the block or credits the share.
//!
//! A captured (#478) block proof's share is deferred to its block's landing,
//! and its answer used to wait up to 60 s for that landing while the miner's
//! session could submit nothing else. Each test holds one database step with a
//! trigger or function that waits for the test to release it:
//!
//! - A captured proof whose landing is held at its `offered` record, and one
//!   whose pre-enqueue duplicate probe is held: each is answered at the cap
//!   with the reply every share still pending at its deadline gets (`20`,
//!   "share outcome is not yet known", `ledger-outcome-unknown`), counted once
//!   in `qbit_prism_block_proof_ack_capped_total{path="block_only"}`, and its
//!   block is still enqueued, landed and its share credited exactly once after
//!   the release.
//! - A share-pass proof carrying a current-revision block whose append is held
//!   at its candidate insert, before COMMIT: answered at the share deadline,
//!   counted on `path="share"`, and credited once after the release.
//! - The same append held inside its COMMIT: like a plain share, it gets the
//!   share grace and is accepted, never served worse than a plain share.
//!
//! The block-only bound is `share_commit_timeout` itself, read where the
//! coordinator waits for the proof's disposition, so restoring the 60 s floor
//! there fails the block-only tests at the cap.
//!
//! Run through: test/prism-native-tests.sh cargo-args --locked -p
//! qbit-prism-server --test b574_ack_cap -- --nocapture

use anyhow::{ensure, Context, Result};
use qbit_prism_server::{
    codec,
    config::Config,
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
/// Scheduling slack past the cap: less than the 5 s share grace, so an answer
/// that waited out the grace, or a 60 s bound, misses it.
const SLACK: Duration = Duration::from_secs(2);
/// The coordinator's share grace (`Config::share_commit_grace`).
const GRACE: Duration = Duration::from_secs(5);

fn tune(config: &mut Config) {
    config.share_commit_timeout = SHARE_COMMIT;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_captured_proof_is_answered_at_the_cap_and_credited_after_its_landing() -> Result<()> {
    run_tuned(gate::site!(), tune, |f| Box::pin(captured(f))).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_captured_proof_whose_probe_outlasts_the_cap_is_still_enqueued_and_credited() -> Result<()>
{
    run_tuned(gate::site!(), tune, |f| Box::pin(probe_held(f))).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_carrying_share_append_is_answered_at_the_cap_and_credited_once() -> Result<()> {
    run_tuned(gate::site!(), tune, |f| Box::pin(share_pass(f))).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_carrying_append_whose_commit_is_in_flight_gets_the_share_grace() -> Result<()> {
    run_tuned(gate::site!(), tune, |f| Box::pin(commit_in_flight(f))).await
}

/// A job whose payout revision a landing then moved with the parent
/// unchanged, so its block is captured with its share deferred (#478).
async fn captured_job(f: &Fixture, name: &str) -> Result<(Worker, MiningJob<JobContext>)> {
    f.refresh(true).await?;
    let worker = f.a.authorize(name).await?;
    let job = issue(f, &worker).await?;
    let bumped = sqlx::query(
        "UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1,updated_at=clock_timestamp() WHERE singleton",
    )
    .execute(f.pool())
    .await?
    .rows_affected();
    ensure!(bumped == 1, "the singleton cluster row was not bumped");
    f.a.refresh_once().await?;
    f.node.accept_blocks();
    Ok((worker, job))
}

async fn captured(f: &Fixture) -> Result<()> {
    let (worker, job) = captured_job(f, "b574.captured").await?;
    let (proof, hash) = find_block_proof(&job)?;
    let share_id = format!("{}:{hash}", worker.username);
    // The node accepts the block; its landing then stops at the offered record,
    // before the confirmation that credits the deferred share.
    hold(
        f,
        "CREATE TRIGGER b574_hold BEFORE UPDATE ON qbit_block_candidate_outbox FOR EACH ROW WHEN (OLD.state='offer_reserved' AND NEW.state='offered') EXECUTE FUNCTION b574_hold()",
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
        "CREATE TRIGGER b574_hold BEFORE INSERT ON qbit_block_candidate_outbox FOR EACH ROW EXECUTE FUNCTION b574_hold()",
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

async fn probe_held(f: &Fixture) -> Result<()> {
    let (worker, job) = captured_job(f, "b574.probe").await?;
    let (proof, hash) = find_block_proof(&job)?;
    let share_id = format!("{}:{hash}", worker.username);
    // The duplicate probe that runs before the enqueue calls this function
    // first; its first call waits for the release, as a probe queued on a
    // saturated pool or a slow leaf would. Nothing is written until it returns.
    sqlx::raw_sql(&format!(
        r#"
        {GATES}
        ALTER FUNCTION qbit_prism_share_probe_floor() RENAME TO b574_probe_floor;
        CREATE FUNCTION qbit_prism_share_probe_floor() RETURNS bigint LANGUAGE plpgsql STABLE AS $$
        BEGIN
            {WAIT}
            RETURN b574_probe_floor();
        END $$;
        "#
    ))
    .execute(f.pool())
    .await?;
    let answered = async {
        let refused = answer_at_cap(f, &worker, &job, proof).await?;
        ensure!(held(f).await?, "the probe never reached the hold");
        let enqueued: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM qbit_block_candidate_outbox WHERE block_hash=$1",
        )
        .bind(&hash)
        .fetch_one(f.pool())
        .await?;
        ensure!(
            enqueued == 0,
            "the block was enqueued before its probe returned"
        );
        ensure!(
            capped(&f.a, "block_only") == 1. && capped(&f.a, "share") == 0.,
            "the capped acknowledgement was not counted once on the block_only path: {refused:?}"
        );
        anyhow::Ok(())
    }
    .await;
    let released = release(f).await;
    answered?;
    released?;
    // The probe resumes past the answer and enqueues the block, which lands
    // and credits the deferred share. Before #574's fix the bound cut the
    // probe off, answered `ledger-confirmation-failed` and dropped the block.
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
        "the deferred share was not credited exactly once after its landing"
    );
    Ok(())
}

async fn commit_in_flight(f: &Fixture) -> Result<()> {
    f.refresh(true).await?;
    let worker = f.a.authorize("b574.commit").await?;
    let job = issue(f, &worker).await?;
    f.node.accept_blocks();
    let (proof, hash) = find_block_proof(&job)?;
    ensure!(
        proof.share_pass,
        "the block proof must also pass the share target"
    );
    let share_id = format!("{}:{hash}", worker.username);
    // A deferred constraint trigger runs inside COMMIT, so the append's gate
    // is committing while it waits.
    hold(
        f,
        "CREATE CONSTRAINT TRIGGER b574_hold AFTER INSERT ON qbit_block_candidate_outbox DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION b574_hold()",
    )
    .await?;
    let late = metric(&f.a, "qbit_prism_late_confirmed_shares_total");
    let started = Instant::now();
    let submit = f.a.submit(&worker, &job, proof, false.into());
    // Release a second past the deadline, inside the grace.
    let release_late = async {
        tokio::time::sleep(SHARE_COMMIT + Duration::from_secs(1)).await;
        let reached = held(f).await;
        let released = release(f).await;
        reached.and_then(|reached| released.map(|()| reached))
    };
    let (answer, reached) = tokio::join!(
        tokio::time::timeout(SHARE_COMMIT + GRACE + SLACK, submit),
        release_late
    );
    let elapsed = started.elapsed();
    ensure!(
        reached?,
        "the append never reached COMMIT before the deadline"
    );
    answer
        .context("the block-carrying append was not answered by its grace")?
        .map_err(|refused| {
            anyhow::anyhow!(
                "a block-carrying append whose COMMIT was in flight at the deadline was not accepted within the share grace, as a plain share is: {refused:?} after {elapsed:?}"
            )
        })?;
    ensure!(
        elapsed >= SHARE_COMMIT + Duration::from_secs(1) && elapsed < SHARE_COMMIT + GRACE,
        "accepted after {elapsed:?}, outside the release-to-grace window"
    );
    ensure!(
        metric(&f.a, "qbit_prism_late_confirmed_shares_total") == late + 1.,
        "the late confirmation was not counted"
    );
    ensure!(
        capped(&f.a, "share") == 0. && capped(&f.a, "block_only") == 0.,
        "an accepted append was counted as capped"
    );
    ensure!(
        credited(f, &share_id).await? == 1,
        "the share was not credited exactly once"
    );
    drive_candidate(f, &hash).await?;
    ensure!(
        credited(f, &share_id).await? == 1,
        "landing the block credited the share again"
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

/// The two gates every hold uses. Sequences are not transactional, so the held
/// transaction's progress and the release are visible across sessions at once.
const GATES: &str = "CREATE SEQUENCE b574_hold_reached; CREATE SEQUENCE b574_hold_released;";
/// One-shot: the first caller marks the hold reached and waits for `release`,
/// or at most 10 s, inside its own statement and transaction.
const WAIT: &str = "IF NOT (SELECT is_called FROM b574_hold_reached) THEN
                PERFORM nextval('b574_hold_reached');
                FOR i IN 1..500 LOOP
                    EXIT WHEN (SELECT is_called FROM b574_hold_released);
                    PERFORM pg_sleep(0.02);
                END LOOP;
            END IF;";

/// Install `trigger`, which runs `b574_hold()`: a one-shot [`WAIT`].
async fn hold(f: &Fixture, trigger: &str) -> Result<()> {
    sqlx::raw_sql(&format!(
        r#"
        {GATES}
        CREATE FUNCTION b574_hold() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            {WAIT}
            RETURN NEW;
        END $$;
        {trigger};
        "#
    ))
    .execute(f.pool())
    .await?;
    Ok(())
}

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
    metric(
        frontend,
        &format!("qbit_prism_block_proof_ack_capped_total{{path=\"{path}\"}}"),
    )
}

fn metric(frontend: &Coordinator, series: &str) -> f64 {
    let series = format!("{series} ");
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
