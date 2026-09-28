//! #526 on a real PostgreSQL: a `submitblock` that qbitd answered from its
//! warmup (`RPC_IN_WARMUP`, -28) was never run by the node, so it returns its
//! reservation to `pending` like #522's refused connection, and the block is
//! offered once the node is warm, exactly once. A warmup answer to any call
//! after the `submitblock` ran changes nothing: the offer stays recorded and
//! the block is never offered again. The relay answers in place of the node
//! with qbitd's own warmup reply, so the fixture's node never sees a call it
//! did not run.
use super::*;

/// The node answers the block's `submitblock` from its warmup: the row goes
/// back to `pending` with no outcome, no call time, no first-offer sample and
/// the warmup reply as its reason, and backs off. While the node is still
/// warming up, the next attempt does the same. Once it is warm, the block
/// lands with exactly one `submitblock` the node ran. A classifier that took
/// the warmup answer for an unknown outcome would never offer the block
/// again: lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_submitblock_answered_in_warmup_returns_the_row_to_pending_and_lands_the_block_once_when_the_node_is_warm(
) -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let relay = Relay::start(fixture.node_address()?).await?;
    let frontend = fixture.relayed_frontend(&relay, "candidate-warmup").await?;
    let result = async {
        relay.submit(Submit::Warmup);
        frontend
            .process_candidate_with_lease(&fixture.claim, CANDIDATE_LEASE)
            .await?;
        let row = fixture.row().await?;
        let reason = row.last_error.clone().unwrap_or_default();
        ensure!(
            row.state == "pending"
                && row.token.is_none()
                && row.outcome.is_none()
                && row.offered_at_ms.is_none()
                && row.reserved_by.is_none()
                && row.evidence,
            "a warmup answer did not return the row to pending with its offer columns empty: state {}, outcome {:?}, reason {reason}",
            row.state,
            row.outcome
        );
        ensure!(
            reason.starts_with(crate::ledger::OFFER_NOT_SENT_REASON_PREFIX)
                && reason.contains("-28")
                && reason.contains("warming up")
                && reason.contains("candidate-warmup"),
            "the warmup answer is not recorded: {reason}"
        );
        ensure!(fixture.backed_off().await?, "the row answered in warmup did not back off");
        ensure!(fixture.submissions().await == 0);
        ensure!(
            fixture.unknown_outcomes().await? == 0,
            "a warmup answer was counted as an unknown outcome"
        );
        ensure!(
            first_offer_samples(&frontend.metrics) == 0,
            "a call the node never ran recorded a first-offer sample"
        );
        // Still warming up: the next attempt is not run either.
        fixture.expire().await?;
        let again = frontend
            .ledger
            .claim_candidate(10)
            .await?
            .context("the row answered in warmup was not claimable")?;
        ensure!(again.lifecycle.state == CandidateState::Pending);
        frontend
            .process_candidate_with_lease(&again, CANDIDATE_LEASE)
            .await?;
        ensure!(fixture.state().await? == "pending" && fixture.submissions().await == 0);
        // The node is warm.
        relay.warm();
        fixture.expire().await?;
        let offer = frontend
            .ledger
            .claim_candidate(10)
            .await?
            .context("the row answered in warmup was not claimable once the node was warm")?;
        frontend
            .process_candidate_with_lease(&offer, CANDIDATE_LEASE)
            .await?;
        let row = fixture.row().await?;
        ensure!(
            row.state == "submitted"
                && row.outcome.as_deref() == Some("accepted")
                && row.offered_at_ms.is_some()
                && row.reserved_by.as_deref() == Some("candidate-warmup"),
            "the re-offered block did not land: state {}, outcome {:?}",
            row.state,
            row.outcome
        );
        ensure!(fixture.landed().await?);
        ensure!(fixture.submissions().await == 1, "the block was offered more than once");
        ensure!(first_offer_samples(&frontend.metrics) == 1);
        fixture.expire().await?;
        ensure!(fixture.successor.claim_candidate(10).await?.is_none());
        Ok::<_, anyhow::Error>(())
    }
    .await;
    drop(relay);
    frontend.ledger.pool.close().await;
    fixture.close().await?;
    result
}

/// The node ran the `submitblock` and accepted the block, then restarted
/// into its warmup, so every observation after the offer is answered -28.
/// Those answers are not the offer's: the acceptance stays recorded, the
/// row never returns to `pending`, and once the node is warm the block
/// lands without a second `submitblock`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_warmup_answer_after_the_submitblock_ran_keeps_the_offer_and_never_offers_again(
) -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let relay = Relay::start(fixture.node_address()?).await?;
    let frontend = fixture
        .relayed_frontend(&relay, "candidate-warmup-after")
        .await?;
    let result = async {
        relay.submit(Submit::WarmupAfterReply);
        // The post-offer observations fail; the row settles for a later,
        // read-only recovery.
        let _ = frontend
            .process_candidate_with_lease(&fixture.claim, CANDIDATE_LEASE)
            .await;
        let row = fixture.row().await?;
        ensure!(
            row.state != "pending"
                && row.outcome.as_deref() == Some("accepted")
                && row.offered_at_ms.is_some(),
            "warmup answers after the offer changed the recorded offer: state {}, outcome {:?}, reason {:?}",
            row.state,
            row.outcome,
            row.last_error
        );
        ensure!(fixture.submissions().await == 1);
        // Every later claim, still warming up and then warm, observes and
        // never offers.
        for warm in [false, true] {
            if warm {
                relay.warm();
            }
            fixture.expire().await?;
            let claim = frontend
                .ledger
                .claim_candidate(10)
                .await?
                .context("the offered row was not recoverable")?;
            ensure!(claim.lifecycle.state != CandidateState::Pending);
            let _ = frontend
                .process_candidate_with_lease(&claim, CANDIDATE_LEASE)
                .await;
        }
        ensure!(
            fixture.submissions().await == 1,
            "the block was offered again after a warmup answer that followed the offer"
        );
        ensure!(fixture.state().await? == "submitted");
        ensure!(fixture.landed().await?);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    drop(relay);
    frontend.ledger.pool.close().await;
    fixture.close().await?;
    result
}
