//! #581: a database clock step of either sign neither ends a live candidate
//! claim early nor stretches a dead holder's lease or a retry's backoff by
//! the size of the step. #654: nor a CTV fanout claim's, nor a fanout's
//! attempt schedule.
//!
//! PostgreSQL's `clock_timestamp()` cannot be stepped without libfaketime,
//! which this suite does not require, so a step is made the other way round:
//! every timestamp the outbox and the fanout table stored is moved by the
//! step in the opposite direction. Each decision compares a stored timestamp
//! with the clock, so it sees exactly what the step would show it. The
//! frontends' monotonic clocks, which time a lease, are untouched, as they
//! are by a real step.
use super::*;
use qbit_prism_server::ledger::{
    revoke_candidate_claims, FanoutClaim, RecoveryClaim, RecoveryTakeover,
};

const STEP_SECONDS: i64 = 2 * 60 * 60;

/// Step the database clock by `seconds` (forward when positive) as every
/// stored outbox and fanout timestamp sees it.
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
    sqlx::query(
        "UPDATE qbit_ctv_fanout_artifacts SET updated_at=updated_at-$1*interval '1 second',\
         claim_expires_at=claim_expires_at-$1*interval '1 second',\
         first_broadcast_attempt_at=first_broadcast_attempt_at-$1*interval '1 second',\
         last_broadcast_attempt_at=last_broadcast_attempt_at-$1*interval '1 second',\
         next_broadcast_attempt_at=CASE WHEN next_broadcast_attempt_at IN ('infinity','-infinity') THEN next_broadcast_attempt_at ELSE next_broadcast_attempt_at-$1*interval '1 second' END",
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

/// #654: one mature CTV fanout per miner, all claimable.
async fn mature_fanouts(ledger: &Ledger, count: u8) -> Result<()> {
    prepare_mature_cpfp_fanouts(ledger, count).await
}

async fn fanout_claim_expired_by_the_database_clock(pool: &PgPool, txid: &str) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT claim_expires_at<=clock_timestamp() FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1",
    )
    .bind(txid)
    .fetch_one(pool)
    .await?)
}

/// How far ahead of the database clock a fanout's next attempt is, in
/// seconds; `None` while it has none.
async fn fanout_attempt_ahead(pool: &PgPool, txid: &str) -> Result<Option<f64>> {
    Ok(sqlx::query_scalar(
        "SELECT extract(epoch FROM next_broadcast_attempt_at-clock_timestamp())::float8 FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1",
    )
    .bind(txid)
    .fetch_one(pool)
    .await?)
}

/// Make a fanout's next attempt due now, the way a scheduled attempt falls
/// due, without a write that would date it: `updated_at` stays the earlier
/// write's.
async fn fanout_attempt_due(pool: &PgPool, txid: &str) -> Result<()> {
    sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET next_broadcast_attempt_at=clock_timestamp()-interval '1 second' WHERE fanout_txid=$1")
        .bind(txid)
        .execute(pool)
        .await?;
    Ok(())
}

/// Claim the one fanout `ledger` attempted and failed once, after its
/// backoff, so the claim carries a recorded schedule (#654).
async fn reclaim_after_one_failure(ledger: &Ledger) -> Result<FanoutClaim> {
    let first = ledger.claim_fanout(60).await?.context("fanout missing")?;
    ledger
        .finish_fanout(&first, "failed", None, Some("node unavailable"))
        .await?;
    fanout_attempt_due(&ledger.pool, &first.fanout_txid).await?;
    let claim = ledger
        .claim_fanout(60)
        .await?
        .context("the due fanout was not claimable")?;
    ensure!(claim.fanout_txid == first.fanout_txid && claim.attempt_count == 1);
    Ok(claim)
}

/// Forward: the database clock calls a live fanout claim expired at once. No
/// other frontend takes the fanout, and the holder keeps renewing and
/// writing under its token, its settlement included. Before #654 the second
/// frontend's claim took the fanout, and the holder's renewal and every
/// settlement write were refused.
#[tokio::test]
async fn a_forward_database_clock_step_never_ends_a_live_fanout_claim_early() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    mature_fanouts(&a, 1).await?;
    let owner = a.claim_fanout(60).await?.context("fanout missing")?;
    ensure!(
        b.claim_fanout(60).await?.is_none(),
        "a live fanout claim was taken"
    );

    step_database_clock(&a.pool, STEP_SECONDS).await?;
    ensure!(fanout_claim_expired_by_the_database_clock(&a.pool, &owner.fanout_txid).await?);
    ensure!(
        b.claim_fanout(60).await?.is_none(),
        "a forward step handed a live fanout claim to another frontend"
    );
    a.renew_fanout_claim(&owner, 60)
        .await
        .context("a forward step ended the holder's own lease")?;
    a.record_fanout_scan(&owner, 1103, None)
        .await
        .context("a forward step fenced the holder's progress out")?;
    ensure!(
        b.claim_fanout(60).await?.is_none(),
        "a renewed fanout claim was taken"
    );
    a.finish_fanout(&owner, "failed", None, Some("the attempt failed"))
        .await
        .context("a forward step refused the holder's settlement")?;
    let (released, attempts): (bool, i64) = sqlx::query_as(
        "SELECT claim_token IS NULL AND claim_lease_seconds IS NULL AND claim_renewals=0,broadcast_attempt_count FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1",
    )
    .bind(&owner.fanout_txid)
    .fetch_one(&a.pool)
    .await?;
    ensure!(
        released && attempts == 1,
        "the settlement did not record one attempt and release the claim"
    );
    db.close(vec![a, b]).await
}

/// Backward: a dead holder's fanout claim looks live to the database clock
/// for the lease plus the step. Another frontend takes it once it has
/// watched it go unrenewed for its lease, not before, and not a step later;
/// the old holder is then fenced out. Before #654 the fanout waited out the
/// step.
#[tokio::test]
async fn a_backward_database_clock_step_never_stretches_a_dead_fanout_holders_lease() -> Result<()>
{
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    mature_fanouts(&a, 1).await?;
    let owner = a.claim_fanout(60).await?.context("fanout missing")?;
    // The holder's last renewal takes a two-second lease; then it dies.
    a.renew_fanout_claim(&owner, 2).await?;
    ensure!(
        b.claim_fanout(60).await?.is_none(),
        "a live fanout claim was taken"
    );
    let watched_from = tokio::time::Instant::now();

    step_database_clock(&a.pool, -STEP_SECONDS).await?;
    ensure!(!fanout_claim_expired_by_the_database_clock(&a.pool, &owner.fanout_txid).await?);
    ensure!(
        b.claim_fanout(60).await?.is_none(),
        "the fanout claim was taken before its lease had passed"
    );
    tokio::time::sleep(Duration::from_millis(2200)).await;
    ensure!(!fanout_claim_expired_by_the_database_clock(&a.pool, &owner.fanout_txid).await?);
    let taken = b
        .claim_fanout(60)
        .await?
        .context("a backward step stretched a dead fanout holder's lease past its own length")?;
    ensure!(watched_from.elapsed() >= Duration::from_secs(2));
    ensure!(taken.fanout_txid == owner.fanout_txid && taken.claim_token != owner.claim_token);
    ensure!(
        a.renew_fanout_claim(&owner, 60).await.is_err(),
        "the taken-over fanout claim was renewed"
    );
    ensure!(
        a.finish_fanout(&owner, "failed", None, Some("late"))
            .await
            .is_err(),
        "the taken-over holder settled the fanout"
    );
    db.close(vec![a, b]).await
}

/// Backward: a failed attempt's retry looks the step plus its backoff away.
/// Its last write is later than the clock, which proves the step, so it is
/// due at once; a fanout held at `infinity` stays held. Before #654 the
/// retry waited out the step.
#[tokio::test]
async fn a_backward_database_clock_step_never_stretches_a_fanout_retry() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    mature_fanouts(&a, 2).await?;
    let retried = a.claim_fanout(60).await?.context("fanout missing")?;
    let held = a.claim_fanout(60).await?.context("second fanout missing")?;
    for claim in [&retried, &held] {
        a.finish_fanout(claim, "failed", None, Some("node unavailable"))
            .await?;
    }
    sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET next_broadcast_attempt_at='infinity' WHERE fanout_txid=$1")
        .bind(&held.fanout_txid)
        .execute(&a.pool)
        .await?;

    step_database_clock(&a.pool, -STEP_SECONDS).await?;
    let ahead = fanout_attempt_ahead(&a.pool, &retried.fanout_txid)
        .await?
        .context("no retry scheduled")?;
    ensure!(
        ahead > STEP_SECONDS as f64,
        "the step did not move the retry: {ahead}"
    );
    let again = b
        .claim_fanout(60)
        .await?
        .context("a backward step stretched a fanout retry by the step")?;
    ensure!(again.fanout_txid == retried.fanout_txid && again.attempt_count == 1);
    ensure!(
        b.claim_fanout(60).await?.is_none(),
        "a fanout held at infinity became due"
    );
    let held_still: bool = sqlx::query_scalar("SELECT next_broadcast_attempt_at='infinity' AND claim_token IS NULL FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1")
        .bind(&held.fanout_txid).fetch_one(&a.pool).await?;
    ensure!(held_still, "the held fanout was rescheduled or claimed");
    db.close(vec![a, b]).await
}

/// Backward, then a renewal: the holder of a retried fanout renews once on
/// the stepped-back clock and dies. The fanout still carries the schedule it
/// was claimed at, a step ahead of the clock now, and its database expiry
/// has passed; the takeover must not wait for that schedule. Before #654 the
/// fanout waited out the step after its lease.
#[tokio::test]
async fn a_backward_step_then_one_renewal_never_strands_a_dead_fanout_holders_row() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    mature_fanouts(&a, 1).await?;
    let owner = reclaim_after_one_failure(&a).await?;

    step_database_clock(&a.pool, -STEP_SECONDS).await?;
    // The holder renews once on the new clock, with a two-second lease, and
    // then dies.
    a.renew_fanout_claim(&owner, 2).await?;
    let ahead = fanout_attempt_ahead(&a.pool, &owner.fanout_txid)
        .await?
        .context("no schedule")?;
    ensure!(
        ahead > (STEP_SECONDS - 60) as f64,
        "the fixture did not leave the schedule a step ahead: {ahead} s"
    );
    ensure!(
        b.claim_fanout(60).await?.is_none(),
        "a live fanout claim was taken"
    );
    tokio::time::sleep(Duration::from_millis(2200)).await;
    ensure!(fanout_claim_expired_by_the_database_clock(&a.pool, &owner.fanout_txid).await?);
    let taken = b.claim_fanout(60).await?.context(
        "a renewal after a backward step left a dead fanout holder's row waiting out the step",
    )?;
    ensure!(taken.fanout_txid == owner.fanout_txid && taken.claim_token != owner.claim_token);
    db.close(vec![a, b]).await
}

/// Backward, then a hand-back: the holder of a retried fanout releases its
/// claim at shutdown on the stepped-back clock. The release keeps a due
/// schedule due instead of keeping the step-ahead one its own `updated_at`
/// would hide from every claim poll. Before #654 the fanout waited out the
/// step.
#[tokio::test]
async fn a_fanout_release_after_a_backward_step_never_stretches_its_next_attempt() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    mature_fanouts(&a, 1).await?;
    let owner = reclaim_after_one_failure(&a).await?;

    step_database_clock(&a.pool, -STEP_SECONDS).await?;
    ensure!(
        a.release_fanout_claim(&owner).await?,
        "the holder could not release its claim"
    );
    let ahead = fanout_attempt_ahead(&a.pool, &owner.fanout_txid)
        .await?
        .context("the release dropped the schedule")?;
    ensure!(
        ahead <= 0.0,
        "the released fanout is {ahead} s out, though it was due when claimed"
    );
    let next = b
        .claim_fanout(60)
        .await?
        .context("a backward step stretched a released fanout's next attempt")?;
    ensure!(next.fanout_txid == owner.fanout_txid && next.claim_token != owner.claim_token);
    db.close(vec![a, b]).await
}
