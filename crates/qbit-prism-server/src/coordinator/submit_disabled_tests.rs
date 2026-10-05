//! #291 on a real PostgreSQL: a frontend with `PRISM_BLOCK_SUBMIT_ENABLED=0`
//! never sends a found block to the node. Its submit loop claims nothing, and
//! an offer reached any other way is refused before the reservation, so the
//! row stays `pending` and untouched. A frontend with submission enabled then
//! offers the same row once and lands it, so the node's silence is the
//! switch's doing and not the fixture's.
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

/// `process_candidate` takes any live claim, so the offer itself refuses
/// under the kill switch, before the staleness probe and the reservation.
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
            refused.contains("block submission is disabled (PRISM_BLOCK_SUBMIT_ENABLED=0)")
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
