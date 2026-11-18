//! On a real PostgreSQL: a `submitblock` that qbitd refused with one of its
//! two HTTP 503 replies written before it dispatches a request (its work
//! queue full, or its HTTP server shutting down) was never run by the node,
//! so it returns its reservation to `pending` like #526's warmup answer, and
//! the block is offered again, exactly once. Any other 503, which a reverse
//! proxy may write after it forwarded the call, stays unknown and is never
//! offered again. The relay answers in place of the node with the node's
//! own bytes, so the fixture's node never sees a call it did not run.
use super::*;

/// qbitd's reply when its work queue is full (Qbit Core v1.0.1,
/// `src/httpserver.cpp`, `http_request_cb`), on a kept-alive connection.
const WORK_QUEUE_FULL: Submit = Submit::RefuseBeforeDispatch {
    body: "Work queue depth exceeded",
    close: false,
};

/// qbitd's reply once its HTTP server was interrupted for shutdown
/// (`http_reject_request_cb`): libevent 2.1.12's 503 page, and the
/// connection closed.
const SHUTTING_DOWN: Submit = Submit::RefuseBeforeDispatch {
    body: "<HTML><HEAD>\n<TITLE>503 Service Unavailable</TITLE>\n</HEAD><BODY>\n<H1>Service Unavailable</H1>\n</BODY></HTML>\n",
    close: true,
};

/// The node refuses the block's `submitblock` before dispatching it: the row
/// goes back to `pending` with no outcome, no call time, no first-offer
/// sample and the refusal as its reason, and backs off. The next claim, the
/// node now accepting calls, lands the block with exactly one `submitblock`
/// the node ran. A classifier that took the refusal for an unknown outcome
/// would never offer the block again: lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_submitblock_the_node_refused_before_dispatch_returns_the_row_to_pending_and_lands_the_block_once(
) -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    for (refusal, wording) in [
        (WORK_QUEUE_FULL, "Work queue depth exceeded"),
        (SHUTTING_DOWN, "shutting down"),
    ] {
        let Some(fixture) = Fixture::open().await? else {
            return Ok(());
        };
        let relay = Relay::start(fixture.node_address()?).await?;
        let frontend = fixture
            .relayed_frontend(&relay, "candidate-undispatched")
            .await?;
        let result = async {
            relay.submit(refusal);
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
                "{wording}: a pre-dispatch refusal did not return the row to pending with its offer columns empty: state {}, outcome {:?}, reason {reason}",
                row.state,
                row.outcome
            );
            ensure!(
                reason.starts_with(crate::ledger::OFFER_NOT_SENT_REASON_PREFIX)
                    && reason.contains("refused before dispatch (HTTP 503 Service Unavailable)")
                    && reason.contains(wording)
                    && reason.contains("candidate-undispatched"),
                "the refusal is not recorded: {reason}"
            );
            ensure!(fixture.backed_off().await?, "{wording}: the refused row did not back off");
            ensure!(fixture.submissions().await == 0);
            ensure!(
                fixture.unknown_outcomes().await? == 0,
                "{wording}: a pre-dispatch refusal was counted as an unknown outcome"
            );
            ensure!(
                first_offer_samples(&frontend.metrics) == 0,
                "{wording}: a call the node never ran recorded a first-offer sample"
            );
            // The relay forwards again: the next claim offers the block.
            fixture.expire().await?;
            let offer = frontend
                .ledger
                .claim_candidate(10)
                .await?
                .context("the refused row was not claimable")?;
            ensure!(offer.lifecycle.state == CandidateState::Pending);
            frontend
                .process_candidate_with_lease(&offer, CANDIDATE_LEASE)
                .await?;
            let row = fixture.row().await?;
            ensure!(
                row.state == "submitted"
                    && row.outcome.as_deref() == Some("accepted")
                    && row.offered_at_ms.is_some()
                    && row.reserved_by.as_deref() == Some("candidate-undispatched"),
                "{wording}: the re-offered block did not land: state {}, outcome {:?}",
                row.state,
                row.outcome
            );
            ensure!(fixture.landed().await?);
            ensure!(
                fixture.submissions().await == 1,
                "{wording}: the block was offered more than once"
            );
            ensure!(first_offer_samples(&frontend.metrics) == 1);
            fixture.expire().await?;
            ensure!(fixture.successor.claim_candidate(10).await?.is_none());
            Ok::<_, anyhow::Error>(())
        }
        .await;
        drop(relay);
        frontend.ledger.pool.close().await;
        fixture.close().await?;
        result?;
    }
    Ok(())
}

/// The control: the node ran the `submitblock` and accepted the block, and a
/// reverse proxy answered with its own 503 page. That page proves nothing,
/// so delivery is unknown, the row never returns to `pending`, and no
/// frontend offers the block again. A classifier that went by the status
/// alone would offer the block a second time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn any_other_503_after_the_node_ran_the_call_stays_unknown_and_is_never_offered_again(
) -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let relay = Relay::start(fixture.node_address()?).await?;
    let frontend = fixture
        .relayed_frontend(&relay, "candidate-proxy-503")
        .await?;
    let result = async {
        relay.submit(Submit::UnavailableAfterForward);
        frontend
            .process_candidate_with_lease(&fixture.claim, CANDIDATE_LEASE)
            .await?;
        relay.submit(Submit::Forward);
        let row = fixture.row().await?;
        ensure!(
            row.state != "pending"
                && row.outcome.as_deref() == Some("unknown")
                && row.offered_at_ms.is_some(),
            "a proxy's 503 after the call ran was not recorded as an unknown outcome: state {}, outcome {:?}, reason {:?}",
            row.state,
            row.outcome,
            row.last_error
        );
        ensure!(fixture.submissions().await == 1);
        // Every later claim, on any frontend, observes and never offers.
        for _ in 0..2 {
            fixture.expire().await?;
            if let Some(claim) = fixture.successor.claim_candidate(10).await? {
                ensure!(claim.lifecycle.state != CandidateState::Pending);
                fixture
                    .successor_coordinator
                    .process_candidate_with_lease(&claim, CANDIDATE_LEASE)
                    .await?;
            }
        }
        ensure!(
            fixture.submissions().await == 1,
            "the block was offered again after a 503 that followed the call"
        );
        ensure!(fixture.state().await? == "submitted");
        Ok::<_, anyhow::Error>(())
    }
    .await;
    drop(relay);
    frontend.ledger.pool.close().await;
    fixture.close().await?;
    result
}
