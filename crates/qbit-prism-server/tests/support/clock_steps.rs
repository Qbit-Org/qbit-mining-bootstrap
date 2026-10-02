//! #581: a database clock step of either sign neither ends a live candidate
//! claim early nor stretches a dead holder's lease or a retry's backoff by
//! the size of the step.
//!
//! PostgreSQL's `clock_timestamp()` cannot be stepped without libfaketime,
//! which this suite does not require, so a step is made the other way round:
//! every timestamp the outbox stored is moved by the step in the opposite
//! direction. Each decision compares a stored timestamp with the clock, so
//! it sees exactly what the step would show it. The frontends' monotonic
//! clocks, which time a lease, are untouched, as they are by a real step.
use super::*;
use qbit_prism_server::ledger::{revoke_candidate_claims, RecoveryClaim, RecoveryTakeover};

const STEP_SECONDS: i64 = 2 * 60 * 60;

/// Step the database clock by `seconds` (forward when positive) as every
/// stored outbox timestamp sees it.
async fn step_database_clock(pool: &PgPool, seconds: i64) -> Result<()> {
    sqlx::query(
        "UPDATE qbit_block_candidate_outbox SET created_at=created_at-$1*interval '1 second',\
         updated_at=updated_at-$1*interval '1 second',\
         claim_expires_at=claim_expires_at-$1*interval '1 second',\
         offer_reserved_at=offer_reserved_at-$1*interval '1 second',\
         completed_at=completed_at-$1*interval '1 second',\
         next_attempt_at=CASE WHEN next_attempt_at='infinity' THEN next_attempt_at ELSE next_attempt_at-$1*interval '1 second' END",
    )
    .bind(seconds)
    .execute(pool)
    .await?;
    Ok(())
}

async fn claim_expired_by_the_database_clock(pool: &PgPool, hash: &str) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT claim_expires_at<=clock_timestamp() FROM qbit_block_candidate_outbox WHERE block_hash=$1",
    )
    .bind(hash)
    .fetch_one(pool)
    .await?)
}

/// Forward: the database clock calls a live claim expired at once. No other
/// frontend takes the row, and the holder keeps renewing and writing under
/// its token. Before #581 the second frontend's claim lane took the row and
/// the holder's renewal was refused.
#[tokio::test]
async fn a_forward_database_clock_step_never_ends_a_live_claim_early() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    a.append(share(1), None).await?;
    let block = candidate(&a.snapshot(100).await?, 581)?;
    let hash = block.block_hash.clone();
    a.enqueue_candidate(block.candidate.clone()).await?;
    let owner = block.claim(a.claim_candidate(60).await?.context("candidate missing")?);
    ensure!(
        b.claim_candidate(60).await?.is_none(),
        "a live claim was taken"
    );

    step_database_clock(&a.pool, STEP_SECONDS).await?;
    ensure!(claim_expired_by_the_database_clock(&a.pool, &hash).await?);
    ensure!(
        b.claim_candidate(60).await?.is_none(),
        "a forward step handed a live claim to another frontend"
    );
    a.renew_candidate_claim(&owner, 60)
        .await
        .context("a forward step ended the holder's own lease")?;
    ensure!(
        b.claim_candidate(60).await?.is_none(),
        "a renewed claim was taken"
    );
    // The holder's other writes are fenced on its token too: its release
    // reschedules the row, which the other frontend then claims.
    a.retry_candidate(&owner, "released after the step").await?;
    revoke_candidate_claims(&a.pool, Some(&hash), true).await?;
    let next = b
        .claim_candidate(60)
        .await?
        .context("the released row was not claimable")?;
    ensure!(next.candidate.block_hash == hash && next.claim_token != owner.claim_token);
    db.close(vec![a, b]).await
}

/// Backward: a dead holder's claim looks live to the database clock for the
/// lease plus the step. Another frontend takes it once it has watched it go
/// unrenewed for its lease, not before, and not a step later. Before #581
/// the row waited out the step.
#[tokio::test]
async fn a_backward_database_clock_step_never_stretches_a_dead_holders_lease() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    a.append(share(1), None).await?;
    let block = candidate(&a.snapshot(100).await?, 582)?;
    let hash = block.block_hash.clone();
    a.enqueue_candidate(block.candidate.clone()).await?;
    let owner = block.claim(a.claim_candidate(60).await?.context("candidate missing")?);
    // The holder's last renewal takes a one-second lease; then it dies.
    a.renew_candidate_claim(&owner, 1).await?;
    ensure!(
        b.claim_candidate(60).await?.is_none(),
        "a live claim was taken"
    );
    let watched_from = tokio::time::Instant::now();

    step_database_clock(&a.pool, -STEP_SECONDS).await?;
    ensure!(!claim_expired_by_the_database_clock(&a.pool, &hash).await?);
    ensure!(
        b.claim_candidate(60).await?.is_none(),
        "the claim was taken before its lease had passed"
    );
    tokio::time::sleep(Duration::from_millis(1100)).await;
    ensure!(!claim_expired_by_the_database_clock(&a.pool, &hash).await?);
    let taken = b
        .claim_candidate(60)
        .await?
        .context("a backward step stretched a dead holder's lease past its own length")?;
    ensure!(watched_from.elapsed() >= Duration::from_secs(1));
    ensure!(taken.candidate.block_hash == hash && taken.claim_token != owner.claim_token);
    ensure!(
        a.renew_candidate_claim(&owner, 60).await.is_err(),
        "the taken-over claim was renewed"
    );
    db.close(vec![a, b]).await
}

/// Backward: a released row's retry looks the step plus its backoff away.
/// Its last write is later than the clock, which proves the step, so it is
/// due at once; a row parked for the operator stays parked. Before #581 the
/// retry waited out the step (the stranded reconciliation row of #586's
/// clock-jump test).
#[tokio::test]
async fn a_backward_database_clock_step_never_stretches_a_retry() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    a.append(share(1), None).await?;
    let snapshot = a.snapshot(100).await?;
    let block = candidate(&snapshot, 583)?;
    let parked = candidate(&snapshot, 584)?;
    let hash = block.block_hash.clone();
    a.enqueue_candidate(block.candidate.clone()).await?;
    a.enqueue_candidate(parked.candidate.clone()).await?;
    sqlx::query(
        "UPDATE qbit_block_candidate_outbox SET next_attempt_at='infinity' WHERE block_hash=$1",
    )
    .bind(&parked.block_hash)
    .execute(&a.pool)
    .await?;
    let owner = block.claim(a.claim_candidate(60).await?.context("candidate missing")?);
    ensure!(owner.candidate.block_hash == hash);
    a.retry_candidate(&owner, "the attempt failed").await?;

    step_database_clock(&a.pool, -STEP_SECONDS).await?;
    let ahead: f64 = sqlx::query_scalar("SELECT extract(epoch FROM next_attempt_at-clock_timestamp())::float8 FROM qbit_block_candidate_outbox WHERE block_hash=$1")
        .bind(&hash).fetch_one(&a.pool).await?;
    ensure!(
        ahead > STEP_SECONDS as f64,
        "the step did not move the retry: {ahead}"
    );
    let retried = b
        .claim_candidate(60)
        .await?
        .context("a backward step stretched a retry by the step")?;
    ensure!(retried.candidate.block_hash == hash);
    ensure!(
        b.claim_candidate(60).await?.is_none(),
        "a parked row became due"
    );
    let parked_still: bool = sqlx::query_scalar("SELECT next_attempt_at='infinity' AND claim_token IS NULL FROM qbit_block_candidate_outbox WHERE block_hash=$1")
        .bind(&parked.block_hash).fetch_one(&a.pool).await?;
    ensure!(parked_still, "the parked row was rescheduled or claimed");
    db.close(vec![a, b]).await
}

/// The operator recovery claim times a holder's claim as a frontend does:
/// a forward step does not let it take a live claim, unless the operator
/// asks for the database clock's rule by name.
#[tokio::test]
async fn a_forward_database_clock_step_does_not_hand_a_live_claim_to_recovery() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    a.append(share(1), None).await?;
    let block = candidate(&a.snapshot(100).await?, 585)?;
    let hash = block.block_hash.clone();
    a.enqueue_candidate(block.candidate.clone()).await?;
    let owner = block.claim(a.claim_candidate(60).await?.context("candidate missing")?);
    step_database_clock(&a.pool, STEP_SECONDS).await?;

    let RecoveryClaim::Refused(outcome) = b
        .claim_candidate_for_recovery(&hash, 120, "observed", RecoveryTakeover::Observed)
        .await?
    else {
        bail!("the recovery claim took a live claim after a forward step");
    };
    ensure!(outcome["outcome"] == "claimed", "{outcome}");
    ensure!(outcome["claim_instance_id"] == "a", "{outcome}");
    let left = outcome["lease_remaining_ms"]
        .as_u64()
        .context("no time left")?;
    ensure!((59_000..=60_000).contains(&left), "{outcome}");
    // The holder still holds it, and renews it.
    a.renew_candidate_claim(&owner, 60).await?;

    // The pre-021 rule, by name, takes it as soon as the database clock is
    // past the expiry, and a second forward step puts it there: the unsafe
    // path the flag's name warns about.
    step_database_clock(&a.pool, STEP_SECONDS).await?;
    let RecoveryClaim::Claimed(taken) = b
        .claim_candidate_for_recovery(
            &hash,
            120,
            "database-clock",
            RecoveryTakeover::DatabaseClock,
        )
        .await?
    else {
        bail!("the database clock's rule refused a claim its clock calls expired");
    };
    ensure!(taken.claim_token == "database-clock");
    ensure!(
        a.renew_candidate_claim(&owner, 60).await.is_err(),
        "the holder kept a claim recovery took"
    );
    db.close(vec![a, b]).await
}

/// `candidates abandon` times a holder's claim as a frontend does: a forward
/// step does not let it terminalize a pending row its live holder is still
/// landing, and a holder that stops renewing is abandonable once watched for
/// its whole lease. Before the fix the abandon took the row at once and the
/// holder's next write was refused, losing a block it was about to offer.
#[tokio::test]
async fn a_forward_database_clock_step_does_not_let_abandon_take_a_live_claim() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let operator = db.ledger("operator").await?;
    a.append(share(1), None).await?;
    let block = candidate(&a.snapshot(100).await?, 588)?;
    let hash = block.block_hash.clone();
    a.enqueue_candidate(block.candidate.clone()).await?;
    let owner = block.claim(a.claim_candidate(60).await?.context("candidate missing")?);
    step_database_clock(&a.pool, STEP_SECONDS).await?;
    ensure!(claim_expired_by_the_database_clock(&a.pool, &hash).await?);

    let refused = operator
        .abandon_candidate(&hash, "superseded", RecoveryTakeover::Observed)
        .await?;
    ensure!(
        refused["outcome"] == "claimed",
        "abandon took a live claim after a forward step: {refused}"
    );
    ensure!(refused["claim_instance_id"] == "a", "{refused}");
    let left = refused["lease_remaining_ms"]
        .as_u64()
        .context("no time left")?;
    ensure!((59_000..=60_000).contains(&left), "{refused}");
    let state: String =
        sqlx::query_scalar("SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$1")
            .bind(&hash)
            .fetch_one(&a.pool)
            .await?;
    ensure!(state == "pending", "the refused abandon wrote {state}");

    // The holder renews with a one-second lease and dies: a new version,
    // which the operator times from its next read and abandons once it has
    // watched it for that whole second.
    a.renew_candidate_claim(&owner, 1).await?;
    let refused = operator
        .abandon_candidate(&hash, "superseded", RecoveryTakeover::Observed)
        .await?;
    ensure!(refused["outcome"] == "claimed", "{refused}");
    ensure!(
        refused["lease_remaining_ms"]
            .as_u64()
            .is_some_and(|left| left <= 1_000),
        "{refused}"
    );
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let done = operator
        .abandon_candidate(&hash, "superseded", RecoveryTakeover::Observed)
        .await?;
    ensure!(done["outcome"] == "abandoned", "{done}");
    ensure!(
        a.renew_candidate_claim(&owner, 60).await.is_err(),
        "the holder kept a claim the abandon ended"
    );
    db.close(vec![a, operator]).await
}

/// Backward, then a renewal: the holder renews once on the stepped-back
/// clock and dies. Its row still carries the schedule it was claimed with,
/// a step ahead of the clock now, and the renewal's `updated_at` no longer
/// shows the step; the takeover must not wait for that schedule. Before the
/// fix the row waited out the step after its lease (lens A's finding).
#[tokio::test]
async fn a_backward_step_then_one_renewal_never_strands_a_dead_holders_row() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    a.append(share(1), None).await?;
    let block = candidate(&a.snapshot(100).await?, 586)?;
    let hash = block.block_hash.clone();
    a.enqueue_candidate(block.candidate.clone()).await?;
    let owner = block.claim(a.claim_candidate(60).await?.context("candidate missing")?);

    step_database_clock(&a.pool, -STEP_SECONDS).await?;
    // The holder's heartbeat renews once on the new clock, with a one-second
    // lease, and then the holder dies.
    a.renew_candidate_claim(&owner, 1).await?;
    let (ahead, stepped): (f64, bool) = sqlx::query_as("SELECT extract(epoch FROM next_attempt_at-clock_timestamp())::float8,updated_at>clock_timestamp() FROM qbit_block_candidate_outbox WHERE block_hash=$1")
        .bind(&hash).fetch_one(&a.pool).await?;
    ensure!(
        ahead > (STEP_SECONDS - 60) as f64 && !stepped,
        "the fixture did not leave the schedule a step ahead: {ahead} s, stepped {stepped}"
    );
    ensure!(
        b.claim_candidate(60).await?.is_none(),
        "a live claim was taken"
    );
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let taken = b
        .claim_candidate(60)
        .await?
        .context("a renewal after a backward step left a dead holder's row waiting out the step")?;
    ensure!(taken.candidate.block_hash == hash && taken.claim_token != owner.claim_token);
    db.close(vec![a, b]).await
}

/// Backward, then the operator: `candidates recover` claims a row still in
/// backoff on the stepped-back clock and releases it. The release keeps the
/// schedule, but never further ahead than the row's own backoff, so the
/// retry does not wait out the step that its write hid from every claim
/// poll. Before the fix the row waited about two hours (lens A's finding).
#[tokio::test]
async fn a_recovery_claim_and_release_after_a_backward_step_never_stretch_a_retry() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let operator = db.ledger("operator").await?;
    a.append(share(1), None).await?;
    let block = candidate(&a.snapshot(100).await?, 587)?;
    let hash = block.block_hash.clone();
    a.enqueue_candidate(block.candidate.clone()).await?;
    let owner = block.claim(a.claim_candidate(60).await?.context("candidate missing")?);
    a.retry_candidate(&owner, "the attempt failed").await?;

    step_database_clock(&a.pool, -STEP_SECONDS).await?;
    let RecoveryClaim::Claimed(recovery) = operator
        .claim_candidate_for_recovery(&hash, 120, "operator-recovery", RecoveryTakeover::Observed)
        .await?
    else {
        bail!("the operator could not claim an unclaimed row");
    };
    ensure!(
        operator
            .release_recovery_claim(&recovery, "operator stopped")
            .await?,
        "the recovery claim was not released"
    );
    let (ahead, parked): (f64, bool) = sqlx::query_as("SELECT extract(epoch FROM next_attempt_at-clock_timestamp())::float8,next_attempt_at='infinity' FROM qbit_block_candidate_outbox WHERE block_hash=$1")
        .bind(&hash).fetch_one(&a.pool).await?;
    ensure!(
        !parked && ahead <= 60.0,
        "the released retry is {ahead} s out, beyond its own backoff"
    );
    let retried = tokio::time::timeout(Duration::from_secs(70), async {
        loop {
            if let Some(claim) = a.claim_candidate(60).await? {
                return Ok::<_, anyhow::Error>(claim);
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .context("the retry waited past its own backoff")??;
    ensure!(retried.candidate.block_hash == hash);
    db.close(vec![a, operator]).await
}
