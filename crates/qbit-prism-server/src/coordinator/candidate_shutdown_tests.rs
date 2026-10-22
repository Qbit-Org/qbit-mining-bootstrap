//! #573 on a real PostgreSQL: a submit loop that shuts down in the middle of
//! an attempt hands its candidate claim back, so another frontend takes the
//! row over at once instead of waiting out the lease on every rolling
//! restart, and recovers it without a second offer.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn submit_loop_shutdown_hands_the_claim_back_for_an_immediate_takeover() -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        // The attempt offers, then waits for build capacity to land: it is in
        // flight when the shutdown arrives.
        let held = fixture.coordinator.build_slots.clone().acquire_owned().await?;
        fixture
            .coordinator
            .ledger
            .retry_candidate(&fixture.claim, "start shutdown release test")
            .await?;
        sqlx::query("UPDATE qbit_block_candidate_outbox SET next_attempt_at=clock_timestamp() WHERE block_hash=$1")
            .bind(&fixture.claim.candidate.block_hash).execute(&fixture.coordinator.ledger.pool).await?;
        let (shutdown, receiver) = watch::channel(false);
        let task = tokio::spawn(fixture.coordinator.clone().submit_loop(receiver));
        fixture.wait_for_offer().await?;
        let owner = fixture.row().await?.token.context("the loop holds no claim")?;
        shutdown.send(true)?;
        tokio::time::timeout(Duration::from_secs(1), task).await??;
        let row = fixture.row().await?;
        ensure!(row.token.is_none(), "the shutdown left its claim held");
        ensure!(
            row.state == "offered" && row.evidence,
            "the release changed the offered row: {}",
            row.state
        );
        ensure!(
            row.last_error.as_deref() == Some("claim released at shutdown"),
            "{:?}",
            row.last_error
        );
        // No expiry: the successor claims the row at once.
        let successor = fixture
            .successor
            .claim_candidate(10)
            .await?
            .context("the successor had to wait for the lease")?;
        ensure!(successor.lifecycle.state == CandidateState::Offered);
        ensure!(successor.claim_token != owner);
        drop(held);
        fixture
            .successor_coordinator
            .process_candidate(&successor)
            .await?;
        ensure!(fixture.state().await? == "submitted");
        ensure!(fixture.landed().await?);
        ensure!(
            fixture.submissions().await == 1,
            "the takeover offered the block again"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    fixture.close().await?;
    result
}
