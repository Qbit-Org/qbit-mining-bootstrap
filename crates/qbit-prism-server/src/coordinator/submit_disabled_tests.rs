//! #291 on a real PostgreSQL: a frontend with `PRISM_BLOCK_SUBMIT_ENABLED=0`
//! never sends a found block to the node. Its submit loop claims nothing, and
//! a claim processed any other way is refused before the probe and the
//! reservation, so the row stays `pending` and untouched. A frontend with
//! submission enabled then offers the same row once and lands it, so the
//! node's silence is the switch's doing and not the fixture's.
use super::*;

impl Fixture {
    /// A third frontend on the fixture's database with the kill switch on.
    async fn held_frontend(&self) -> Result<Arc<Coordinator>> {
        let frontend = Coordinator::new(
            Config {
                instance_id: "candidate-held".into(),
                block_submit_enabled: false,
                ..(*self.coordinator.config).clone()
            },
            Arc::new(crate::metrics::Metrics::default()),
        )
        .await?;
        *frontend.observed_tip.write().await = TipState::baseline("aa".repeat(32));
        Ok(frontend)
    }

    /// The row as the fixture enqueued it: `pending`, never reserved or
    /// offered, its evidence intact.
    async fn untouched(&self, when: &str) -> Result<()> {
        let row = self.row().await?;
        ensure!(
            row.state == "pending"
                && row.reserved_by.is_none()
                && row.outcome.is_none()
                && row.offered_at_ms.is_none()
                && row.evidence,
            "{when}: the row is {} reserved by {:?} with outcome {:?}",
            row.state,
            row.reserved_by,
            row.outcome
        );
        ensure!(
            self.submissions().await == 0,
            "{when}: the node saw a submitblock"
        );
        Ok(())
    }
}

/// `qbit_prism_candidate_dispatch_sequence` advances once per claim
/// transaction that found due work, and never for an empty poll.
async fn dispatch_slots(fixture: &Fixture) -> Result<i64> {
    let (last, called): (i64, bool) =
        sqlx::query_as("SELECT last_value,is_called FROM qbit_prism_candidate_dispatch_sequence")
            .fetch_one(&fixture.coordinator.ledger.pool)
            .await?;
    Ok(if called { last } else { 0 })
}

/// `process_candidate` takes any live claim, so the claim's processing itself
/// refuses under the kill switch, before the probe and the reservation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_offer_on_a_frontend_with_block_submission_disabled_is_refused_before_its_reservation(
) -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let held = fixture.held_frontend().await?;
    let result = async {
        let refused = held
            .process_candidate_with_lease(&fixture.claim, CANDIDATE_LEASE)
            .await
            .err()
            .context("the held frontend processed a pending block")?
            .to_string();
        ensure!(
            refused.contains("block submission is disabled by PRISM_BLOCK_SUBMIT_ENABLED")
                && refused.contains(&fixture.claim.candidate.block_hash),
            "{refused}"
        );
        fixture.untouched("after the refused offer").await?;
        fixture
            .coordinator
            .process_candidate_with_lease(&fixture.claim, CANDIDATE_LEASE)
            .await?;
        ensure!(fixture.state().await? == "submitted");
        ensure!(fixture.landed().await?);
        ensure!(
            fixture.submissions().await == 1,
            "the enabled frontend did not offer the block exactly once"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    held.ledger.pool.close().await;
    fixture.close().await?;
    result
}

/// The held submit loop takes no claim on a due row in a window of many
/// drain ticks, and ends at the shutdown at once; the enabled loop offers the
/// same row on its first ticks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_submit_loop_with_block_submission_disabled_claims_nothing_until_shutdown() -> Result<()>
{
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let held = fixture.held_frontend().await?;
    let result = async {
        fixture
            .coordinator
            .ledger
            .retry_candidate(&fixture.claim, "hand the row to the submit loops")
            .await?;
        sqlx::query("UPDATE qbit_block_candidate_outbox SET next_attempt_at=clock_timestamp() WHERE block_hash=$1")
            .bind(&fixture.claim.candidate.block_hash)
            .execute(&fixture.coordinator.ledger.pool)
            .await?;
        let slots = dispatch_slots(&fixture).await?;
        let (stop, receiver) = watch::channel(false);
        let task = tokio::spawn(held.clone().submit_loop(receiver));
        // Ten of the drain's 100 ms ticks: a running drain claims a due row
        // on its first.
        tokio::time::sleep(Duration::from_secs(1)).await;
        ensure!(!task.is_finished(), "the held loop ended before its shutdown");
        fixture.untouched("while the held loop ran").await?;
        ensure!(
            fixture.row().await?.token.is_none() && dispatch_slots(&fixture).await? == slots,
            "the held loop claimed the row"
        );
        stop.send(true)?;
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .context("the held loop did not end at the shutdown")??;
        fixture.untouched("after the held loop's shutdown").await?;

        let (stop, receiver) = watch::channel(false);
        let task = tokio::spawn(fixture.coordinator.clone().submit_loop(receiver));
        let drained = tokio::time::timeout(Duration::from_secs(5), async {
            while fixture.state().await? != "submitted" {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await;
        stop.send(true)?;
        tokio::time::timeout(Duration::from_secs(5), task).await??;
        drained.context("the enabled loop did not land the row")??;
        ensure!(
            fixture.submissions().await == 1,
            "the enabled loop did not offer the block exactly once"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    held.ledger.pool.close().await;
    fixture.close().await?;
    result
}

/// #664: with the cluster's block submission hold set, a frontend whose own
/// switch is on is refused at the reservation for a claim it took before the
/// hold, claims nothing new, and reports its switch and the hold apart.
/// Clearing is refused while the row is pending unless told to offer it;
/// then the same frontend offers the row once and lands it. Both changes are
/// journaled, and neither the journal nor the hold row can be removed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cluster_hold_stops_an_enabled_frontend_until_it_is_cleared() -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let frontend = &fixture.coordinator;
        let ledger = &frontend.ledger;
        ensure!(ledger.submission_hold().await?.is_none());
        ensure!(
            ledger.set_submission_hold(" ").await.is_err(),
            "a blank reason was accepted"
        );
        let reason = "rehearsal on a restored ledger";
        let (hold, newly_set) = ledger.set_submission_hold(reason).await?;
        ensure!(newly_set && hold.reason == reason, "{hold:?}");
        let (again, newly_set) = ledger.set_submission_hold("another reason").await?;
        ensure!(
            !newly_set && again == hold,
            "a second set changed the hold in force: {again:?}"
        );

        let refused = frontend
            .process_candidate_with_lease(&fixture.claim, CANDIDATE_LEASE)
            .await
            .err()
            .context("an enabled frontend offered a block under the cluster hold")?;
        ensure!(
            refused.is::<crate::ledger::SubmissionHeld>(),
            "{refused:#}"
        );
        fixture.untouched("after the refused reservation").await?;

        ledger
            .retry_candidate(&fixture.claim, "hand the row back under the hold")
            .await?;
        sqlx::query("UPDATE qbit_block_candidate_outbox SET next_attempt_at=clock_timestamp() WHERE block_hash=$1")
            .bind(&fixture.claim.candidate.block_hash)
            .execute(&ledger.pool)
            .await?;
        let slots = dispatch_slots(&fixture).await?;
        ensure!(
            ledger.claim_candidate(10).await?.is_none(),
            "a due row was claimed under the cluster hold"
        );
        ensure!(
            dispatch_slots(&fixture).await? == slots,
            "the held claim used a dispatch slot"
        );
        let health = frontend.health().await;
        ensure!(
            health["block_submission_enabled"] == true
                && health["block_submission_hold"]
                    == json!({"held": true, "reason": reason, "set_at": hold.set_at, "set_by": hold.set_by}),
            "the frontend's own switch and the hold were not reported apart: {health}"
        );

        ensure!(
            ledger.clear_submission_hold(" ", true).await.is_err(),
            "a blank clearing reason was accepted"
        );
        let error = ledger
            .clear_submission_hold("rehearsal over", false)
            .await
            .err()
            .context("the hold cleared over a pending candidate")?;
        ensure!(
            error.to_string().contains("while candidates are pending (1)"),
            "{error:#}"
        );
        ensure!(ledger.submission_hold().await? == Some(hold.clone()));
        let cleared = ledger
            .clear_submission_hold("offer the held block", true)
            .await?;
        ensure!(
            cleared.cleared == Some(hold) && cleared.pending_candidates == 1,
            "{cleared:?}"
        );
        let health = frontend.health().await;
        ensure!(
            health["block_submission_enabled"] == true
                && health["block_submission_hold"]["held"] == false,
            "{health}"
        );
        let events: Vec<(String, String, i64, bool)> = sqlx::query_as(
            "SELECT action,reason,pending_candidates,operator_identity=session_user \
             FROM qbit_prism_submission_hold_events ORDER BY event_id",
        )
        .fetch_all(&ledger.pool)
        .await?;
        ensure!(
            events
                == [
                    ("set".to_owned(), reason.to_owned(), 1, true),
                    ("clear".to_owned(), "offer the held block".to_owned(), 1, true),
                ],
            "the journal holds {events:?}"
        );
        for (statement, refusal) in [
            ("UPDATE qbit_prism_submission_hold_events SET reason='changed'", "immutable"),
            ("DELETE FROM qbit_prism_submission_hold_events", "immutable"),
            ("TRUNCATE qbit_prism_submission_hold_events", "immutable"),
            ("DELETE FROM qbit_prism_submission_hold", "permanent"),
            ("TRUNCATE qbit_prism_submission_hold", "permanent"),
        ] {
            let error = sqlx::query(statement)
                .execute(&ledger.pool)
                .await
                .err()
                .with_context(|| format!("{statement} succeeded"))?;
            ensure!(error.to_string().contains(refusal), "{statement}: {error}");
        }

        let claim = ledger
            .claim_candidate(10)
            .await?
            .context("nothing was claimed once the hold cleared")?;
        frontend
            .process_candidate_with_lease(&claim, CANDIDATE_LEASE)
            .await?;
        ensure!(fixture.state().await? == "submitted");
        ensure!(
            fixture.submissions().await == 1,
            "the block was not offered exactly once after the hold cleared"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    fixture.close().await?;
    result
}

/// #664: an offer reservation reads the hold `FOR SHARE` on the hold's own
/// row. A hold being set holds that row, so the reservation waits for it and
/// then sees the hold. The fence takes no lock on the cluster row, so a
/// landing holding that `FOR UPDATE` delays neither `clear` nor a reservation
/// without the capture check (#478), as a leased candidate's is.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_hold_fence_waits_for_a_hold_being_set_and_never_for_the_cluster_row() -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let ledger = &fixture.coordinator.ledger;
        // A landing's revision bump holds the cluster row until it commits.
        let mut landing = ledger.pool.begin().await?;
        sqlx::query("SELECT singleton FROM qbit_prism_cluster WHERE singleton FOR UPDATE")
            .fetch_one(&mut *landing)
            .await?;
        // `submission-hold set` holds the hold row while it records a hold.
        let mut setting = ledger.pool.begin().await?;
        sqlx::query("SELECT singleton FROM qbit_prism_submission_hold WHERE singleton FOR UPDATE")
            .fetch_one(&mut *setting)
            .await?;
        let reservation = ledger.reserve_offer_within(&fixture.claim, None);
        tokio::pin!(reservation);
        let waiting = async {
            loop {
                let waiting: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() \
                     AND wait_event_type='Lock' AND query LIKE '%FOR SHARE OF h')",
                )
                .fetch_one(&ledger.pool)
                .await?;
                if waiting {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::select! {
            reserved = &mut reservation => anyhow::bail!(
                "the reservation did not wait for the hold being set (reserved: {})",
                reserved.is_ok()
            ),
            waited = tokio::time::timeout(Duration::from_secs(5), waiting) => waited??,
        }
        sqlx::query(
            "UPDATE qbit_prism_submission_hold SET reason='rehearsal',set_at=clock_timestamp(),set_by=session_user WHERE singleton",
        )
        .execute(&mut *setting)
        .await?;
        setting.commit().await?;
        let refused = tokio::time::timeout(Duration::from_secs(5), reservation)
            .await?
            .err()
            .context("a reservation that waited for the hold went ahead")?;
        ensure!(
            refused.is::<crate::ledger::SubmissionHeld>(),
            "{refused:#}"
        );
        fixture.untouched("after the hold refused the reservation").await?;

        // Neither clearing the hold nor the reservation waits for the landing.
        tokio::time::timeout(
            Duration::from_secs(5),
            ledger.clear_submission_hold("the fence was shown", true),
        )
        .await
        .context("clearing the hold waited for a landing holding the cluster row")??;
        let reserved = tokio::time::timeout(
            Duration::from_secs(5),
            ledger.reserve_offer_within(&fixture.claim, None),
        )
        .await
        .context("a found block's reservation waited for a landing holding the cluster row")??;
        ensure!(
            matches!(reserved, crate::ledger::OfferReservation::Reserved { bound: None }),
            "the reservation was refused"
        );
        landing.rollback().await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    fixture.close().await?;
    result
}
