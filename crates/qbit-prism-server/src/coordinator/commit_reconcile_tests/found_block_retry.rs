//! A found block whose append fails before COMMIT for a passing reason is
//! run again while its job's parent is still the observed tip, through the
//! real [`Coordinator`] and the scripted node of the parent module.
//!
//! Contention comes from a second session holding `ORDER_LOCK` past the
//! ledger sessions' `lock_timeout`, never from load. The variable that sets
//! it, `PRISM_DATABASE_LOCK_TIMEOUT_MS`, is read by every ledger this test
//! binary opens, in parallel with these tests, so the timeout is lowered on
//! the coordinator's own pooled sessions instead ([`lower_lock_timeout`]).
//! Each deliberate refusal is shown to end the append at its first attempt,
//! on both the share-pass and the block-only path.
use super::*;

/// The ledger sessions' `lock_timeout` while `ORDER_LOCK` is held here, in
/// place of the 5 s default.
const LOCK_TIMEOUT: &str = "300ms";

/// The WARN each attempt that runs again logs.
const RAN_AGAIN: &str = "found block's append failed before COMMIT; appending it again";
/// The WARN a block recorded by a later attempt logs.
const RECORDED_LATER: &str = "found block recorded after its append was run again";
/// The ERROR a retry that ends with the block unrecorded logs.
const GAVE_UP: &str = "found block not recorded";

/// Lower `lock_timeout` on every session of the coordinator's ledger pool:
/// the pool is filled to its size, each session told, and all returned. The
/// pool opens no other session while these live, and none of these tests
/// makes it retire one.
async fn lower_lock_timeout(fixture: &Fixture) -> Result<()> {
    let pool = &fixture.coordinator.ledger.pool;
    let size = pool.options().get_max_connections();
    let mut sessions = Vec::new();
    for _ in 0..size {
        let mut session = pool.acquire().await?;
        sqlx::query(&format!("SET lock_timeout='{LOCK_TIMEOUT}'"))
            .execute(&mut *session)
            .await?;
        sessions.push(session);
    }
    ensure!(
        pool.size() == size,
        "the ledger pool holds {} sessions, not {size}",
        pool.size()
    );
    Ok(())
}

async fn release_order_lock(holder: &mut sqlx::pool::PoolConnection<sqlx::Postgres>) -> Result<()> {
    sqlx::query(&format!("SELECT pg_advisory_unlock({ORDER_LOCK_KEY})"))
        .execute(&mut **holder)
        .await?;
    Ok(())
}

/// The node's chain moved on as far as this frontend can tell: the observed
/// tip is no longer the parent of any job built so far.
async fn move_the_observed_tip(fixture: &Fixture) {
    *fixture.coordinator.observed_tip.write().await = TipState::baseline("bb".repeat(32));
}

async fn outbox_rows(fixture: &Fixture, block_hash: &str) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT count(*) FROM qbit_block_candidate_outbox WHERE block_hash=$1")
            .bind(block_hash)
            .fetch_one(&fixture.side)
            .await?,
    )
}

/// The block was never recorded: no outbox row, no credited share, nothing
/// claimable and nothing offered.
async fn ensure_unrecorded(fixture: &Fixture, proof_ids: (&str, &str)) -> Result<()> {
    let (share_id, block_hash) = proof_ids;
    ensure!(
        outbox_rows(fixture, block_hash).await? == 0,
        "the block has an outbox row"
    );
    ensure!(fixture.rows(share_id).await? == 0, "the share was credited");
    ensure!(
        fixture.submitblock_calls().await == 0,
        "the block was offered"
    );
    Ok(())
}

/// The append ended at its first attempt, without a retry or its give-up.
fn ensure_not_run_again(log: &SharedLog) -> Result<()> {
    let text = log.text();
    ensure!(
        !text.contains(RAN_AGAIN) && !text.contains(GAVE_UP),
        "a refused append was run again or logged as given up:\n{text}"
    );
    Ok(())
}

/// The retry ended for `stopped` with the block unrecorded, and said so at
/// ERROR with the block's hash.
fn ensure_gave_up(log: &SharedLog, block_hash: &str, stopped: &str) -> Result<()> {
    let text = log.text();
    let line = text
        .lines()
        .find(|line| line.contains(GAVE_UP))
        .with_context(|| format!("the retry's end was not logged:\n{text}"))?;
    ensure!(
        line.contains("ERROR") && line.contains(block_hash) && line.contains(stopped),
        "the retry's end was not logged at ERROR with the block and `{stopped}`: {line}"
    );
    Ok(())
}

/// A proof for a job whose window holds a share that this primary no longer
/// holds as the window counted it (#619): a plain share is credited, a
/// refresh builds work over it, and then that row is stamped past the
/// window's anchor, as a promoted primary that reissued its number would
/// hold it. The immutability trigger is lifted for that one statement, in
/// this test's own schema.
async fn proof_over_a_window_not_held(fixture: &Fixture, block_only: bool) -> Result<Proof> {
    let (credited, _) = fixture.submit(fixture.proof(false).await?);
    let credited = answer(credited).await?;
    ensure!(
        credited.is_ok(),
        "the window's share was answered {:?}",
        reason(&credited)
    );
    fixture.coordinator.refresh_once().await?;
    let proof = if block_only {
        fixture.proof(true).await?
    } else {
        fixture.block_bearing_proof().await?
    };
    let range = proof
        .job
        .context
        .prepared
        .window
        .shares
        .context("the refreshed job's window holds no share")?;
    let mut tx = fixture.side.begin().await?;
    sqlx::query("ALTER TABLE qbit_share_ledger DISABLE TRIGGER qbit_prism_immutable_share_history")
        .execute(&mut *tx)
        .await?;
    let altered = sqlx::query(
        "UPDATE qbit_share_ledger SET accepted_at=accepted_at+interval '1 hour' WHERE share_seq=$1",
    )
    .bind(i64::try_from(range.last_share_seq)?)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    sqlx::query("ALTER TABLE qbit_share_ledger ENABLE TRIGGER qbit_prism_immutable_share_history")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    ensure!(altered == 1, "restamped {altered} rows, expected 1");
    Ok(proof)
}

/// A fingerprint that is not the one this frontend pinned: another
/// frontend's configuration, or a reset in flight.
async fn foreign_fingerprint(fixture: &Fixture) -> Result<()> {
    sqlx::query("UPDATE qbit_prism_cluster SET config_fingerprint=$1 WHERE singleton")
        .bind("ff".repeat(32))
        .execute(&fixture.side)
        .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Share-pass appends that carry a found block
// ---------------------------------------------------------------------------

/// The finding's case: `ORDER_LOCK` held past `lock_timeout` while a
/// block-bearing share is submitted. The append fails before COMMIT, runs
/// again, and once the lock is released records the share and its block
/// once; the block is offered once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn found_block_retry_share_append_held_past_its_lock_timeout_is_recorded_and_offered_once(
) -> Result<()> {
    let Some(raw) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::open(&raw, |config| {
        config.share_commit_timeout = Duration::from_secs(10)
    })
    .await?;
    let outcome = async {
        lower_lock_timeout(&fixture).await?;
        let proof = fixture.block_bearing_proof().await?;
        let (share_id, block_hash) = (proof.share_id.clone(), proof.block_hash.clone());
        let (mut holder, _) = fixture.hold_order_lock().await?;
        let (submitted, log) = fixture.submit(proof);
        until("the append to fail and run again", || async {
            Ok(log.text().contains(RAN_AGAIN))
        })
        .await?;
        ensure!(
            outbox_rows(&fixture, &block_hash).await? == 0,
            "a failed attempt left an outbox row"
        );
        release_order_lock(&mut holder).await?;
        let answer = answer(submitted).await?;
        ensure!(
            answer.is_ok(),
            "the block-bearing share was answered {:?}:\n{}",
            reason(&answer),
            log.text()
        );
        let text = log.text();
        ensure!(
            text.contains(RECORDED_LATER) && text.contains(&block_hash) && !text.contains(GAVE_UP),
            "the recovered block was not logged with its hash:\n{text}"
        );
        ensure!(
            outbox_rows(&fixture, &block_hash).await? == 1,
            "expected exactly one outbox row"
        );
        ensure!(
            fixture.rows(&share_id).await? == 1,
            "expected exactly one credit row"
        );
        fixture.drive_candidate().await?;
        ensure!(
            fixture
                .coordinator
                .ledger
                .claim_candidate(10)
                .await?
                .is_none(),
            "a candidate is still waiting"
        );
        ensure!(
            fixture.submitblock_calls().await == 1,
            "offered other than once"
        );
        ensure!(
            outbox_rows(&fixture, &block_hash).await? == 1,
            "expected exactly one outbox row after the offer"
        );
        drop(holder);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    settle(outcome, fixture.close().await)
}

/// The control: the lock is released only after the job's parent stopped
/// being the observed tip. The block can no longer extend the chain, so the
/// append is not run again, nothing is recorded, and the end is logged at
/// ERROR with the block's hash.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn found_block_retry_share_append_stops_once_its_parent_is_no_longer_the_tip() -> Result<()> {
    let Some(raw) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::open(&raw, |config| {
        config.share_commit_timeout = Duration::from_secs(10)
    })
    .await?;
    let outcome = async {
        lower_lock_timeout(&fixture).await?;
        let proof = fixture.block_bearing_proof().await?;
        let (share_id, block_hash) = (proof.share_id.clone(), proof.block_hash.clone());
        let (mut holder, holder_pid) = fixture.hold_order_lock().await?;
        let (submitted, log) = fixture.submit(proof);
        wait_until_blocked(&fixture, holder_pid).await?;
        move_the_observed_tip(&fixture).await;
        until("the retry to end", || async {
            Ok(log.text().contains(GAVE_UP))
        })
        .await?;
        release_order_lock(&mut holder).await?;
        let answer = answer(submitted).await?;
        ensure!(
            reason(&answer).as_deref() == Some("ledger-confirmation-failed"),
            "the unrecorded block's share was answered {:?}",
            reason(&answer)
        );
        ensure_gave_up(&log, &block_hash, "parent-changed")?;
        ensure_unrecorded(&fixture, (&share_id, &block_hash)).await?;
        ensure!(
            fixture
                .coordinator
                .ledger
                .claim_candidate(10)
                .await?
                .is_none(),
            "a candidate is waiting"
        );
        drop(holder);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    settle(outcome, fixture.close().await)
}

/// A shutdown closes the ledger pool last; a retry still running then ends
/// with it, so it never outlives the frontend.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn found_block_retry_share_append_ends_when_the_ledger_pool_closes() -> Result<()> {
    let Some(raw) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::open(&raw, |config| {
        config.share_commit_timeout = Duration::from_secs(10)
    })
    .await?;
    let outcome = async {
        lower_lock_timeout(&fixture).await?;
        let proof = fixture.block_bearing_proof().await?;
        let (share_id, block_hash) = (proof.share_id.clone(), proof.block_hash.clone());
        let (mut holder, _) = fixture.hold_order_lock().await?;
        let (submitted, log) = fixture.submit(proof);
        until("the append to fail and run again", || async {
            Ok(log.text().contains(RAN_AGAIN))
        })
        .await?;
        tokio::time::timeout(PATIENCE, fixture.coordinator.ledger.pool.close())
            .await
            .context("the ledger pool did not close")?;
        let answer = answer(submitted).await?;
        ensure!(
            reason(&answer).as_deref() == Some("ledger-confirmation-failed"),
            "the unrecorded block's share was answered {:?}",
            reason(&answer)
        );
        ensure_gave_up(&log, &block_hash, "stopping")?;
        release_order_lock(&mut holder).await?;
        ensure_unrecorded(&fixture, (&share_id, &block_hash)).await?;
        drop(holder);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    settle(outcome, fixture.close().await)
}

/// A lost own log (D-8, D-9) refuses the append before any statement; it is
/// not run again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn found_block_retry_share_append_refused_for_a_lost_own_log_is_not_run_again() -> Result<()>
{
    let Some(raw) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::open(&raw, |config| {
        config.share_commit_timeout = Duration::from_secs(10)
    })
    .await?;
    let outcome = async {
        let proof = fixture.block_bearing_proof().await?;
        let (share_id, block_hash) = (proof.share_id.clone(), proof.block_hash.clone());
        fixture.coordinator.ledger.set_own_log_lost(true);
        let (submitted, log) = fixture.submit(proof);
        let answer = answer(submitted).await;
        fixture.coordinator.ledger.set_own_log_lost(false);
        let answer = answer?;
        ensure!(
            reason(&answer).as_deref() == Some("ledger-confirmation-failed"),
            "a share refused for a lost own log was answered {:?}",
            reason(&answer)
        );
        ensure_not_run_again(&log)?;
        ensure_unrecorded(&fixture, (&share_id, &block_hash)).await
    }
    .await;
    settle(outcome, fixture.close().await)
}

/// With capture off, the payout-revision fence refuses a block whose revision
/// moved before its commit; the refusal is not run again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn found_block_retry_share_append_refused_by_the_revision_fence_is_not_run_again(
) -> Result<()> {
    let Some(raw) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::open(&raw, |config| {
        config.share_commit_timeout = Duration::from_secs(10);
        config.capture_overpay_ceiling_bps = 0;
    })
    .await?;
    let outcome = async {
        let proof = fixture.block_bearing_proof().await?;
        let (share_id, block_hash) = (proof.share_id.clone(), proof.block_hash.clone());
        let (mut holder, holder_pid) = fixture.hold_order_lock().await?;
        let (submitted, log) = fixture.submit(proof);
        wait_until_blocked(&fixture, holder_pid).await?;
        fixture.bump_revision_and_release(&mut holder).await?;
        let answer = answer(submitted).await?;
        ensure!(
            reason(&answer).as_deref() == Some("stale-job"),
            "a share past its revision was answered {:?}",
            reason(&answer)
        );
        ensure_not_run_again(&log)?;
        ensure_unrecorded(&fixture, (&share_id, &block_hash)).await?;
        drop(holder);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    settle(outcome, fixture.close().await)
}

/// A block whose window this primary does not hold (#619) is refused before
/// its enqueue; the refusal is not run again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn found_block_retry_share_append_refused_for_a_window_not_held_is_not_run_again(
) -> Result<()> {
    let Some(raw) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::open(&raw, |config| {
        config.share_commit_timeout = Duration::from_secs(10);
        config.snapshot_interval = Duration::ZERO;
    })
    .await?;
    let outcome = async {
        let proof = proof_over_a_window_not_held(&fixture, false).await?;
        let (share_id, block_hash) = (proof.share_id.clone(), proof.block_hash.clone());
        let (submitted, log) = fixture.submit(proof);
        let answer = answer(submitted).await?;
        ensure!(
            reason(&answer).as_deref() == Some("stale-job"),
            "a block over a window not held was answered {:?}",
            reason(&answer)
        );
        ensure!(
            log.text().contains("#619"),
            "the refusal was not the window probe's:\n{}",
            log.text()
        );
        ensure_not_run_again(&log)?;
        ensure_unrecorded(&fixture, (&share_id, &block_hash)).await
    }
    .await;
    settle(outcome, fixture.close().await)
}

/// A cluster fingerprint that is not this frontend's refuses the candidate
/// at its writer fence; the refusal is not run again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn found_block_retry_share_append_refused_by_a_foreign_fingerprint_is_not_run_again(
) -> Result<()> {
    let Some(raw) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::open(&raw, |config| {
        config.share_commit_timeout = Duration::from_secs(10)
    })
    .await?;
    let outcome = async {
        let proof = fixture.block_bearing_proof().await?;
        let (share_id, block_hash) = (proof.share_id.clone(), proof.block_hash.clone());
        foreign_fingerprint(&fixture).await?;
        let (submitted, log) = fixture.submit(proof);
        let answer = answer(submitted).await?;
        ensure!(
            reason(&answer).as_deref() == Some("ledger-confirmation-failed"),
            "a block refused by the writer fence was answered {:?}",
            reason(&answer)
        );
        ensure!(
            log.text().contains("cluster configuration fingerprint"),
            "the refusal was not the writer fence's:\n{}",
            log.text()
        );
        ensure_not_run_again(&log)?;
        ensure_unrecorded(&fixture, (&share_id, &block_hash)).await
    }
    .await;
    settle(outcome, fixture.close().await)
}

// ---------------------------------------------------------------------------
// Block-only proofs: the duplicate probe and the candidate enqueue
// ---------------------------------------------------------------------------

/// The block-only enqueue waits on `ORDER_LOCK` past `lock_timeout`, fails
/// before COMMIT, runs again, and once the lock is released enqueues the
/// block once; the block is offered once and the proof credited once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn found_block_retry_block_only_enqueue_held_past_its_lock_timeout_is_enqueued_and_offered_once(
) -> Result<()> {
    let Some(raw) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::open(&raw, |config| {
        config.share_commit_timeout = Duration::from_secs(10)
    })
    .await?;
    let outcome = async {
        lower_lock_timeout(&fixture).await?;
        let proof = fixture.proof(true).await?;
        let (share_id, block_hash) = (proof.share_id.clone(), proof.block_hash.clone());
        let (mut holder, _) = fixture.hold_order_lock().await?;
        let (submitted, log) = fixture.submit(proof);
        until("the enqueue to fail and run again", || async {
            Ok(log.text().contains(RAN_AGAIN))
        })
        .await?;
        ensure!(
            outbox_rows(&fixture, &block_hash).await? == 0,
            "a failed attempt left an outbox row"
        );
        release_order_lock(&mut holder).await?;
        until("the pending candidate", || async {
            Ok(fixture.outbox_state(&block_hash).await?.as_deref() == Some("pending"))
        })
        .await?;
        ensure!(
            !submitted.is_finished(),
            "a block-only proof was answered before its candidate's disposition"
        );
        fixture.drive_candidate().await?;
        let answer = answer(submitted).await?;
        ensure!(
            answer.is_ok(),
            "a block-only proof enqueued on its second attempt was answered {:?}:\n{}",
            reason(&answer),
            log.text()
        );
        let text = log.text();
        ensure!(
            text.contains(RECORDED_LATER) && text.contains(&block_hash) && !text.contains(GAVE_UP),
            "the recovered block was not logged with its hash:\n{text}"
        );
        ensure!(
            outbox_rows(&fixture, &block_hash).await? == 1,
            "expected exactly one outbox row"
        );
        ensure!(
            fixture.rows(&share_id).await? == 1,
            "expected exactly one credit row"
        );
        ensure!(
            fixture
                .coordinator
                .ledger
                .claim_candidate(10)
                .await?
                .is_none(),
            "a candidate is still waiting"
        );
        ensure!(
            fixture.submitblock_calls().await == 1,
            "offered other than once"
        );
        drop(holder);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    settle(outcome, fixture.close().await)
}

/// The control for the block-only path: the lock is released only after the
/// parent stopped being the observed tip, and nothing is enqueued.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn found_block_retry_block_only_enqueue_stops_once_its_parent_is_no_longer_the_tip(
) -> Result<()> {
    let Some(raw) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::open(&raw, |config| {
        config.share_commit_timeout = Duration::from_secs(10)
    })
    .await?;
    let outcome = async {
        lower_lock_timeout(&fixture).await?;
        let proof = fixture.proof(true).await?;
        let (share_id, block_hash) = (proof.share_id.clone(), proof.block_hash.clone());
        let (mut holder, holder_pid) = fixture.hold_order_lock().await?;
        let (submitted, log) = fixture.submit(proof);
        wait_until_blocked(&fixture, holder_pid).await?;
        move_the_observed_tip(&fixture).await;
        until("the retry to end", || async {
            Ok(log.text().contains(GAVE_UP))
        })
        .await?;
        release_order_lock(&mut holder).await?;
        let answer = answer(submitted).await?;
        ensure!(
            reason(&answer).as_deref() == Some("ledger-confirmation-failed"),
            "the unrecorded block-only proof was answered {:?}",
            reason(&answer)
        );
        ensure_gave_up(&log, &block_hash, "parent-changed")?;
        ensure_unrecorded(&fixture, (&share_id, &block_hash)).await?;
        drop(holder);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    settle(outcome, fixture.close().await)
}

/// The block-only enqueue does not itself refuse a lost own log, so the
/// retry does: a frontend whose own log is lost is stopping, and the enqueue
/// that failed before COMMIT is not run again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn found_block_retry_block_only_enqueue_is_not_run_again_once_the_own_log_is_lost(
) -> Result<()> {
    let Some(raw) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::open(&raw, |config| {
        config.share_commit_timeout = Duration::from_secs(10)
    })
    .await?;
    let outcome = async {
        lower_lock_timeout(&fixture).await?;
        let proof = fixture.proof(true).await?;
        let (share_id, block_hash) = (proof.share_id.clone(), proof.block_hash.clone());
        let (mut holder, holder_pid) = fixture.hold_order_lock().await?;
        let (submitted, log) = fixture.submit(proof);
        wait_until_blocked(&fixture, holder_pid).await?;
        fixture.coordinator.ledger.set_own_log_lost(true);
        let ended = until("the retry to end", || async {
            Ok(log.text().contains(GAVE_UP))
        })
        .await;
        release_order_lock(&mut holder).await?;
        let answer = answer(submitted).await;
        fixture.coordinator.ledger.set_own_log_lost(false);
        ended?;
        let answer = answer?;
        ensure!(
            reason(&answer).as_deref() == Some("ledger-confirmation-failed"),
            "the unrecorded block-only proof was answered {:?}",
            reason(&answer)
        );
        ensure_gave_up(&log, &block_hash, "stopping")?;
        ensure_unrecorded(&fixture, (&share_id, &block_hash)).await?;
        drop(holder);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    settle(outcome, fixture.close().await)
}

/// A block-only proof over a window this primary does not hold (#619) is
/// refused at its enqueue; the refusal is not run again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn found_block_retry_block_only_enqueue_refused_for_a_window_not_held_is_not_run_again(
) -> Result<()> {
    let Some(raw) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::open(&raw, |config| {
        config.share_commit_timeout = Duration::from_secs(10);
        config.snapshot_interval = Duration::ZERO;
    })
    .await?;
    let outcome = async {
        let proof = proof_over_a_window_not_held(&fixture, true).await?;
        let (share_id, block_hash) = (proof.share_id.clone(), proof.block_hash.clone());
        let (submitted, log) = fixture.submit(proof);
        let answer = answer(submitted).await?;
        ensure!(
            reason(&answer).as_deref() == Some("stale-job"),
            "a block-only proof over a window not held was answered {:?}",
            reason(&answer)
        );
        ensure!(
            log.text().contains("#619"),
            "the refusal was not the window probe's:\n{}",
            log.text()
        );
        ensure_not_run_again(&log)?;
        ensure_unrecorded(&fixture, (&share_id, &block_hash)).await
    }
    .await;
    settle(outcome, fixture.close().await)
}

/// A cluster fingerprint that is not this frontend's refuses the block-only
/// enqueue at its writer fence; the refusal is not run again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn found_block_retry_block_only_enqueue_refused_by_a_foreign_fingerprint_is_not_run_again(
) -> Result<()> {
    let Some(raw) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::open(&raw, |config| {
        config.share_commit_timeout = Duration::from_secs(10)
    })
    .await?;
    let outcome = async {
        let proof = fixture.proof(true).await?;
        let (share_id, block_hash) = (proof.share_id.clone(), proof.block_hash.clone());
        foreign_fingerprint(&fixture).await?;
        let (submitted, log) = fixture.submit(proof);
        let answer = answer(submitted).await?;
        ensure!(
            reason(&answer).as_deref() == Some("ledger-confirmation-failed"),
            "a block-only proof refused by the writer fence was answered {:?}",
            reason(&answer)
        );
        ensure!(
            log.text().contains("cluster configuration fingerprint"),
            "the refusal was not the writer fence's:\n{}",
            log.text()
        );
        ensure_not_run_again(&log)?;
        ensure_unrecorded(&fixture, (&share_id, &block_hash)).await
    }
    .await;
    settle(outcome, fixture.close().await)
}
