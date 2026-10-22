//! #578 on a real PostgreSQL: a graceful shutdown that arrives after a found
//! block's offer reservation committed lets the offer finish. The block is
//! sent exactly once and the node's answer recorded before the submit loop
//! exits, so recovery lands it instead of reconciling a reservation that was
//! never sent. The frontend has the #570 standby wait configured (no standby
//! streams on the fixture's database, so it ends `absent`), and the attempt
//! is parked either right after the reservation, before that wait, or after
//! it, right before the send.
use super::*;

impl Fixture {
    /// A third frontend on the fixture's database with the standby wait on.
    async fn waiting_frontend(&self) -> Result<Arc<Coordinator>> {
        let frontend = Coordinator::new(
            Config {
                instance_id: "candidate-shutdown".into(),
                offer_standby: Some(crate::ledger::OfferStandbyWait {
                    application_name: "prism_standby_absent".into(),
                    bound: Duration::from_millis(250),
                }),
                ..(*self.coordinator.config).clone()
            },
            Arc::new(crate::metrics::Metrics::default()),
        )
        .await?;
        *frontend.observed_tip.write().await = TipState::baseline("aa".repeat(32));
        Ok(frontend)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_after_the_offer_reservation_sends_and_records_the_block_once() -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    for stage in ["after the reservation", "before the send"] {
        let Some(fixture) = Fixture::open().await? else {
            return Ok(());
        };
        let result = async {
            let frontend = fixture.waiting_frontend().await?;
            let probe = Arc::new(OfferProbe::default());
            let seam = if stage == "after the reservation" {
                &frontend.offer_reserved_probe
            } else {
                &frontend.offer_probe
            };
            *seam.lock().unwrap() = Some(probe.clone());
            fixture
                .coordinator
                .ledger
                .retry_candidate(&fixture.claim, "handed to the shutting-down frontend")
                .await?;
            sqlx::query("UPDATE qbit_block_candidate_outbox SET next_attempt_at=clock_timestamp() WHERE block_hash=$1")
                .bind(&fixture.claim.candidate.block_hash)
                .execute(&fixture.coordinator.ledger.pool)
                .await?;
            let (shutdown, receiver) = watch::channel(false);
            let task = tokio::spawn(frontend.clone().submit_loop(receiver));
            tokio::time::timeout(Duration::from_secs(10), probe.entered.notified())
                .await
                .with_context(|| format!("{stage}: the loop never reserved the offer"))?;
            ensure!(
                fixture.state().await? == "offer_reserved" && fixture.submissions().await == 0,
                "{stage}: the attempt is not parked between its reservation and the send"
            );
            shutdown.send(true)?;
            tokio::time::sleep(Duration::from_millis(300)).await;
            ensure!(
                !task.is_finished(),
                "{stage}: shutdown dropped the offer after its reservation committed"
            );
            probe.release.notify_one();
            tokio::time::timeout(Duration::from_secs(10), task)
                .await
                .with_context(|| format!("{stage}: the loop did not stop after the offer"))??;
            let row = fixture.row().await?;
            ensure!(
                fixture.submissions().await == 1
                    && matches!(row.state.as_str(), "offered" | "submitted")
                    && row.outcome.as_deref() == Some("accepted")
                    && row.offered_at_ms.is_some(),
                "{stage}: shutdown left the block {} with outcome {:?} after {} submitblock calls",
                row.state,
                row.outcome,
                fixture.submissions().await
            );
            // Whatever the shutdown dropped after the answer was recorded is
            // landed by the next claim, without another offer.
            if row.state == "offered" {
                fixture.expire().await?;
                let recovered = fixture
                    .successor
                    .claim_candidate(10)
                    .await?
                    .context("the offered row could not be claimed")?;
                fixture
                    .successor_coordinator
                    .process_candidate(&recovered)
                    .await?;
            }
            let row = fixture.row().await?;
            ensure!(
                row.state == "submitted" && row.outcome.as_deref() == Some("accepted"),
                "{stage}: the offered block landed as {} with outcome {:?}",
                row.state,
                row.outcome
            );
            ensure!(fixture.landed().await?);
            ensure!(
                fixture.submissions().await == 1,
                "{stage}: the block was offered again"
            );
            Ok::<_, anyhow::Error>(())
        }
        .await;
        fixture.close().await?;
        result?;
    }
    Ok(())
}
