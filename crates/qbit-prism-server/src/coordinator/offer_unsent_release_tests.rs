//! On a real PostgreSQL: a submit-loop attempt that took a found block's
//! offer reservation and then failed before its `submitblock` could leave
//! (the strictly-live renewal before the send failed, or a heartbeat renewal
//! failed and dropped the attempt's work), knows that nothing was sent. Its
//! failed-attempt release returns the reservation to `pending`, and the block
//! is offered again, exactly once, instead of being settled as delivery
//! unknown by the next claim and never offered. When that release write fails
//! too, the deferred retry returns it. An attempt dropped while its
//! `submitblock` was in flight may have been run by the node, and stays
//! unknown. Failures are injected by a trigger that fails the next matching
//! update of the outbox row once, so each case is deterministic.
use super::*;
use std::sync::atomic::Ordering;

/// The heartbeat renews only after 10 s, so within a test the only renewal
/// after the attempt parks is the strictly-live one before the send.
const PRE_SEND_LEASE: CandidateLease = CandidateLease {
    seconds: 30,
    interval: Duration::from_secs(10),
    timeout: Duration::from_millis(500),
    rebuild_deadline: Duration::from_secs(60),
};

/// The heartbeat renews every 200 ms, so a failure injected while the
/// attempt is parked reaches the heartbeat first, which drops the work.
const HEARTBEAT_LEASE: CandidateLease = CandidateLease {
    interval: Duration::from_millis(200),
    ..PRE_SEND_LEASE
};

/// The outbox update that renews a claim (`renew_candidate_claim`).
const RENEWAL: &str = "NEW.claim_renewals > OLD.claim_renewals";

/// The outbox update that returns a reservation to `pending`
/// (`Ledger::release_unsent_offer`).
const RETURN_TO_PENDING: &str = "OLD.state = 'offer_reserved' AND NEW.state = 'pending'";

/// The ordinary claim release, which keeps the row's state
/// (`Ledger::release_candidate_claim`, `Ledger::release_recovery_claim`).
const ORDINARY_RELEASE: &str =
    "OLD.claim_token IS NOT NULL AND NEW.claim_token IS NULL AND NEW.state = OLD.state";

/// The heartbeat renews every second, so a deferred release is retried a
/// second after it failed, and a failure injected after the offer reaches
/// the heartbeat within a second.
const AFTER_OFFER_LEASE: CandidateLease = CandidateLease {
    interval: Duration::from_secs(1),
    ..PRE_SEND_LEASE
};

const LOOPING: &str = "candidate-looping";

/// Where the looping frontend's first attempt waits.
#[derive(Clone, Copy, PartialEq)]
enum Park {
    /// Nowhere: it runs straight through.
    No,
    /// At the offer probe, after the reservation and right before the
    /// strictly-live renewal and the send.
    BeforeSend,
    /// For build capacity, after the node accepted the block and the offer
    /// was recorded: the test holds the frontend's only build slot.
    AfterOffer,
}

/// A frontend's submit loop working on the fixture's candidate.
struct Looping {
    frontend: Arc<Coordinator>,
    /// The probe its first attempt parked at ([`Park::BeforeSend`]); later
    /// attempts do not park.
    probe: Option<Arc<OfferProbe>>,
    /// Its only build slot ([`Park::AfterOffer`]).
    _build_slot: Option<tokio::sync::OwnedSemaphorePermit>,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl Looping {
    fn probe(&self) -> Result<&OfferProbe> {
        self.probe.as_deref().context("the attempt was not parked")
    }

    async fn stop(mut self) -> Result<Arc<Coordinator>> {
        self.shutdown.send(true)?;
        tokio::time::timeout(Duration::from_secs(10), &mut self.task)
            .await
            .context("the submit loop did not stop")??;
        Ok(self.frontend.clone())
    }
}

/// A test that failed midway leaves no loop running on a dropped schema.
impl Drop for Looping {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Fixture {
    /// Hand the fixture's candidate to a new frontend's submit loop, reaching
    /// the node at `rpc_url`, under `lease`, and wait until its first attempt
    /// is where `park` says.
    async fn start_loop(
        &self,
        rpc_url: String,
        lease: CandidateLease,
        block_submit_timeout: Duration,
        park: Park,
    ) -> Result<Looping> {
        let frontend = Coordinator::new(
            Config {
                instance_id: LOOPING.into(),
                rpc_url,
                block_submit_timeout,
                ..(*self.coordinator.config).clone()
            },
            Arc::new(crate::metrics::Metrics::default()),
        )
        .await?;
        *frontend.observed_tip.write().await = TipState::baseline("aa".repeat(32));
        let probe = (park == Park::BeforeSend).then(|| Arc::new(OfferProbe::default()));
        *frontend.offer_probe.lock().unwrap() = probe.clone();
        let build_slot = match park {
            Park::AfterOffer => Some(frontend.build_slots.clone().acquire_owned().await?),
            Park::No | Park::BeforeSend => None,
        };
        self.coordinator
            .ledger
            .retry_candidate(&self.claim, "handed to the looping frontend")
            .await?;
        self.due_now().await?;
        let (shutdown, receiver) = watch::channel(false);
        let task = tokio::spawn(frontend.clone().submit_loop_with(lease, receiver));
        if let Some(probe) = &probe {
            tokio::time::timeout(Duration::from_secs(10), probe.entered.notified())
                .await
                .context("the loop never reserved the offer")?;
            let row = self.row().await?;
            ensure!(
                row.state == "offer_reserved" && row.reserved_by.as_deref() == Some(LOOPING),
                "the parked attempt holds no reservation: {} by {:?}",
                row.state,
                row.reserved_by
            );
            // Later attempts run straight through.
            *frontend.offer_probe.lock().unwrap() = None;
        }
        if park == Park::AfterOffer {
            self.wait_for_row("offered", |row| row.state == "offered")
                .await?;
            ensure!(self.submissions().await == 1);
        }
        Ok(Looping {
            frontend,
            probe,
            _build_slot: build_slot,
            shutdown,
            task,
        })
    }

    async fn due_now(&self) -> Result<()> {
        sqlx::query("UPDATE qbit_block_candidate_outbox SET next_attempt_at=clock_timestamp() WHERE block_hash=$1")
            .bind(&self.claim.candidate.block_hash)
            .execute(&self.coordinator.ledger.pool)
            .await?;
        Ok(())
    }

    /// Fail the next `times` outbox updates matching `when` with a database
    /// error raised before they write anything. The count is a sequence, so a
    /// failure's rollback does not re-arm it; [`Fixture::injected`] reads how
    /// many matching updates were attempted.
    async fn fail_next_updates(&self, name: &str, when: &str, times: i64) -> Result<()> {
        sqlx::raw_sql(&format!(
            "CREATE SEQUENCE {name}_seq; \
             CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF nextval('{name}_seq') <= {times} THEN RAISE EXCEPTION 'injected {name}'; END IF; RETURN NEW; END $$; \
             CREATE TRIGGER {name} BEFORE UPDATE ON qbit_block_candidate_outbox FOR EACH ROW WHEN ({when}) EXECUTE FUNCTION {name}();"
        ))
        .execute(&self.coordinator.ledger.pool)
        .await?;
        Ok(())
    }

    /// How many updates [`Fixture::fail_next_updates`]'s `name` has seen.
    async fn injected(&self, name: &str) -> Result<i64> {
        Ok(sqlx::query_scalar(&format!(
            "SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM {name}_seq"
        ))
        .fetch_one(&self.coordinator.ledger.pool)
        .await?)
    }

    /// Wait until the fixture's row satisfies `done`.
    async fn wait_for_row(&self, what: &str, done: impl Fn(&Row) -> bool) -> Result<Row> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let row = self.row().await?;
                if done(&row) {
                    return Ok::<_, anyhow::Error>(row);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .with_context(|| {
            format!(
                "the row never became {what}: {:?}",
                self.claim.candidate.block_hash
            )
        })?
    }

    /// The row, returned unsent by the looping frontend: `pending`, no claim,
    /// no offer record, its evidence kept, and a reason that says so.
    async fn returned_unsent(&self, says: &str) -> Result<()> {
        let row = self
            .wait_for_row("pending as not sent", |row| {
                // The hand-over left it `pending` too, with another reason.
                row.state == "pending"
                    && row.token.is_none()
                    && row.last_error.as_deref().is_some_and(|reason| {
                        reason.starts_with(crate::ledger::OFFER_NOT_SENT_REASON_PREFIX)
                    })
            })
            .await?;
        let reason = row.last_error.clone().unwrap_or_default();
        ensure!(
            row.outcome.is_none()
                && row.offered_at_ms.is_none()
                && row.reserved_by.is_none()
                && row.evidence,
            "the returned row kept an offer record: outcome {:?}, call time {:?}, reserved by {:?}",
            row.outcome,
            row.offered_at_ms,
            row.reserved_by
        );
        ensure!(
            reason.starts_with(crate::ledger::OFFER_NOT_SENT_REASON_PREFIX)
                && reason.contains(LOOPING)
                && reason.contains(says)
                && reason.contains("returned to pending for another offer"),
            "the return is not recorded: {reason}"
        );
        ensure!(
            self.backed_off().await?,
            "the returned row did not back off"
        );
        ensure!(
            self.submissions().await == 0,
            "the unsent block reached the node"
        );
        ensure!(
            self.unknown_outcomes().await? == 0,
            "an unsent block was counted as an unknown outcome"
        );
        Ok(())
    }

    /// The returned row is offered again by the running loop, once, and lands.
    async fn offered_once_more(&self, looping: Looping) -> Result<()> {
        self.due_now().await?;
        self.wait_for_row("submitted", |row| row.state == "submitted")
            .await?;
        let frontend = looping.stop().await?;
        let row = self.row().await?;
        ensure!(
            row.outcome.as_deref() == Some("accepted")
                && row.offered_at_ms.is_some()
                && row.reserved_by.as_deref() == Some(LOOPING),
            "the re-offered block landed with outcome {:?} reserved by {:?}",
            row.outcome,
            row.reserved_by
        );
        ensure!(self.landed().await?);
        ensure!(
            self.submissions().await == 1,
            "the block was offered {} times",
            self.submissions().await
        );
        ensure!(first_offer_samples(&frontend.metrics) == 1);
        frontend.ledger.pool.close().await;
        Ok(())
    }
}

/// The attempt holds the reservation and fails before its `submitblock` can
/// leave: the strictly-live renewal right before the send fails, or the
/// heartbeat's renewal fails and drops the attempt's work while it waits
/// (the standby or peer-ingest wait's place). Its release returns the row to
/// `pending`, with no outcome and the failure as its reason, and the next
/// attempt offers the block exactly once. With the release write failing
/// too, the deferred retry returns it a moment later. Before, the release
/// kept `offer_reserved`, and the next claim settled the block as delivery
/// unknown without ever offering it: lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_attempt_that_fails_before_its_send_returns_the_reservation_to_pending_and_the_block_is_offered_once(
) -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    for pre_send in [true, false] {
        let (failure, lease) = if pre_send {
            ("the renewal before the send", PRE_SEND_LEASE)
        } else {
            ("the heartbeat", HEARTBEAT_LEASE)
        };
        for release_fails in [false, true] {
            let Some(fixture) = Fixture::open().await? else {
                return Ok(());
            };
            let result = async {
                let looping = fixture
                    .start_loop(
                        fixture.coordinator.config.rpc_url.clone(),
                        lease,
                        Duration::from_secs(1),
                        Park::BeforeSend,
                    )
                    .await?;
                if release_fails {
                    fixture
                        .fail_next_updates("fail_return", RETURN_TO_PENDING, 1)
                        .await?;
                }
                fixture
                    .fail_next_updates("fail_renewal", RENEWAL, 1)
                    .await?;
                let probe = looping.probe()?;
                if pre_send {
                    // Let the attempt run on into its renewal before the send.
                    // Otherwise it stays parked until the heartbeat drops it.
                    probe.release.notify_one();
                }
                fixture.returned_unsent("renewal").await?;
                ensure!(
                    probe.left.load(Ordering::SeqCst),
                    "the row was returned while the attempt's work was still running"
                );
                ensure!(fixture.injected("fail_renewal").await? >= 1);
                if release_fails {
                    ensure!(
                        fixture.injected("fail_return").await? >= 2,
                        "the row was returned without the deferred retry"
                    );
                }
                fixture.offered_once_more(looping).await
            }
            .await;
            fixture.close().await?;
            result.with_context(|| format!("{failure}, release write failing: {release_fails}"))?;
        }
    }
    Ok(())
}

/// The node refused the `submitblock` before dispatching it (its work queue
/// was full), and the attempt's own write returning the reservation to
/// `pending` failed, as did its failed-attempt release right after: the
/// attempt fails holding a reservation the node provably never ran. The
/// deferred retry returns it, and the block is offered once more.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_offer_whose_return_to_pending_fails_is_returned_by_the_deferred_retry(
) -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let relay = Relay::start(fixture.node_address()?).await?;
    let result = async {
        relay.submit(Submit::RefuseBeforeDispatch {
            body: "Work queue depth exceeded",
            close: false,
        });
        fixture
            .fail_next_updates("fail_return", RETURN_TO_PENDING, 2)
            .await?;
        let looping = fixture
            .start_loop(
                relay.url(),
                PRE_SEND_LEASE,
                Duration::from_secs(1),
                Park::No,
            )
            .await?;
        fixture
            .returned_unsent("refused before dispatch (HTTP 503 Service Unavailable)")
            .await?;
        ensure!(
            fixture.injected("fail_return").await? >= 3,
            "the row was returned without the deferred retry"
        );
        fixture.offered_once_more(looping).await
    }
    .await;
    drop(relay);
    fixture.close().await?;
    result
}

/// The control: the heartbeat fails while the `submitblock` is in flight,
/// after the node received and accepted the block, and drops the call. The
/// call may have run, so the attempt's release keeps the reservation, and
/// every later claim settles the block as delivery unknown without offering
/// it again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_attempt_dropped_while_its_submitblock_is_in_flight_keeps_the_reservation_and_never_offers_again(
) -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let relay = Relay::start(fixture.node_address()?).await?;
    let result = async {
        // The call's own deadline is far beyond the heartbeat's failure.
        let looping = fixture
            .start_loop(
                relay.url(),
                HEARTBEAT_LEASE,
                Duration::from_secs(20),
                Park::BeforeSend,
            )
            .await?;
        relay.submit(Submit::HoldReply);
        looping.probe()?.release.notify_one();
        tokio::time::timeout(Duration::from_secs(5), async {
            while fixture.submissions().await == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("the submitblock never reached the node")?;
        // The node has run the call; its reply is held. Now the heartbeat
        // fails and drops the call in flight.
        fixture
            .fail_next_updates("fail_renewal", RENEWAL, 1)
            .await?;
        let row = fixture
            .wait_for_row("released", |row| row.token.is_none())
            .await?;
        let frontend = looping.stop().await?;
        relay.submit(Submit::Forward);
        ensure!(
            row.state == "offer_reserved" && row.outcome.is_none(),
            "a call dropped in flight did not keep its reservation: {} {:?}, reason {:?}",
            row.state,
            row.outcome,
            row.last_error
        );
        ensure!(
            !row.last_error
                .as_deref()
                .unwrap_or_default()
                .starts_with(crate::ledger::OFFER_NOT_SENT_REASON_PREFIX),
            "a call dropped in flight was recorded as not sent: {:?}",
            row.last_error
        );
        ensure!(fixture.submissions().await == 1);
        // Every later claim, on any frontend, settles and never offers.
        for _ in 0..2 {
            fixture.expire().await?;
            fixture.due_now().await?;
            if let Some(claim) = fixture.successor.claim_candidate(10).await? {
                ensure!(claim.lifecycle.state != CandidateState::Pending);
                fixture
                    .successor_coordinator
                    .process_candidate_with_lease(&claim, CANDIDATE_LEASE)
                    .await?;
            }
        }
        let row = fixture.row().await?;
        ensure!(
            row.state == "submitted" && row.outcome.as_deref() == Some("unknown"),
            "the recovered reservation settled as {} with outcome {:?}",
            row.state,
            row.outcome
        );
        ensure!(
            fixture.submissions().await == 1,
            "the block was offered again after a call that may have run"
        );
        frontend.ledger.pool.close().await;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    drop(relay);
    fixture.close().await?;
    result
}

/// A shutdown that stops waiting for an attempt parked between its
/// reservation and its send (the offer section's bound, here 2.5 s, runs
/// out) drops the attempt, and the claim's hand-back returns the reservation
/// to `pending`, due at once, as the ordinary hand-back keeps a row
/// claimable at once. Another frontend offers the block, once. Before, the
/// hand-back kept the reservation, and the block was never offered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shutdown_that_stops_waiting_before_the_send_returns_the_reservation_to_pending_due_at_once(
) -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let looping = fixture
            .start_loop(
                fixture.coordinator.config.rpc_url.clone(),
                PRE_SEND_LEASE,
                Duration::from_secs(1),
                Park::BeforeSend,
            )
            .await?;
        let probe = looping
            .probe
            .clone()
            .context("the attempt was not parked")?;
        let bound = looping.frontend.offer_section_bound(PRE_SEND_LEASE);
        // The probe is never released: the shutdown waits out the bound.
        let stopping = Instant::now();
        let frontend = looping.stop().await?;
        ensure!(
            stopping.elapsed() >= bound,
            "the shutdown did not wait for the offer in flight: {:?} of {bound:?}",
            stopping.elapsed()
        );
        ensure!(
            probe.left.load(Ordering::SeqCst),
            "the parked attempt was not dropped"
        );
        let row = fixture.row().await?;
        let reason = row.last_error.clone().unwrap_or_default();
        ensure!(
            row.state == "pending"
                && row.token.is_none()
                && row.outcome.is_none()
                && row.offered_at_ms.is_none()
                && row.reserved_by.is_none()
                && row.evidence,
            "the hand-back kept the unsent reservation: {} {:?}, reason {reason}",
            row.state,
            row.outcome
        );
        ensure!(
            reason.starts_with(crate::ledger::OFFER_NOT_SENT_REASON_PREFIX)
                && reason.contains("stopped at shutdown")
                && reason.contains(LOOPING),
            "the return is not recorded: {reason}"
        );
        ensure!(
            !fixture.backed_off().await?,
            "the handed-back row is not due at once"
        );
        ensure!(fixture.submissions().await == 0);
        frontend.ledger.pool.close().await;
        // Another frontend offers it at once.
        let offer = fixture
            .successor
            .claim_candidate(10)
            .await?
            .context("the handed-back row was not claimable at once")?;
        ensure!(offer.lifecycle.state == CandidateState::Pending);
        fixture
            .successor_coordinator
            .process_candidate(&offer)
            .await?;
        let row = fixture.row().await?;
        ensure!(
            row.state == "submitted" && row.outcome.as_deref() == Some("accepted"),
            "the returned block landed as {} with outcome {:?}",
            row.state,
            row.outcome
        );
        ensure!(
            fixture.submissions().await == 1,
            "the block was offered more than once"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    fixture.close().await?;
    result
}

/// The control for the ordinary release, which the attempts above no longer
/// reach: the node accepted the block and the offer was recorded, then the
/// heartbeat dropped the attempt waiting to land it, and its claim release
/// failed once. The deferred retry, or the shutdown's release, frees the
/// claim keeping the `offered` row, never returning it to `pending`, and the
/// next claim lands it without a second `submitblock`. The deferred retry
/// backs the row off; the shutdown's keeps its schedule.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_release_after_the_offer_is_retried_and_keeps_the_offer_without_a_second_send(
) -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    for at_shutdown in [false, true] {
        let Some(fixture) = Fixture::open().await? else {
            return Ok(());
        };
        let result = async {
            let looping = fixture
                .start_loop(
                    fixture.coordinator.config.rpc_url.clone(),
                    AFTER_OFFER_LEASE,
                    Duration::from_secs(1),
                    Park::AfterOffer,
                )
                .await?;
            if at_shutdown {
                // Every release that backs the row off fails, so only the
                // shutdown's, which keeps its schedule, can free the claim.
                fixture
                    .fail_next_updates(
                        "fail_release",
                        &format!(
                            "{ORDINARY_RELEASE} AND NEW.next_attempt_at > OLD.next_attempt_at"
                        ),
                        i64::MAX,
                    )
                    .await?;
            } else {
                fixture
                    .fail_next_updates("fail_release", ORDINARY_RELEASE, 1)
                    .await?;
            }
            fixture
                .fail_next_updates("fail_renewal", RENEWAL, 1)
                .await?;
            // The heartbeat drops the attempt and its immediate release fails.
            tokio::time::timeout(Duration::from_secs(10), async {
                while fixture.injected("fail_release").await? == 0 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await
            .context("the dropped attempt never tried to release its claim")??;
            let frontend = if at_shutdown {
                ensure!(
                    fixture.row().await?.token.is_some(),
                    "a release that backs the row off freed the claim"
                );
                looping.stop().await?
            } else {
                fixture
                    .wait_for_row("released", |row| row.token.is_none())
                    .await?;
                let frontend = looping.stop().await?;
                ensure!(
                    fixture.injected("fail_release").await? >= 2,
                    "the claim was freed without a second release"
                );
                frontend
            };
            let row = fixture.row().await?;
            let reason = row.last_error.clone().unwrap_or_default();
            ensure!(
                row.state == "offered"
                    && row.token.is_none()
                    && row.outcome.as_deref() == Some("accepted")
                    && row.offered_at_ms.is_some()
                    && !reason.starts_with(crate::ledger::OFFER_NOT_SENT_REASON_PREFIX),
                "the released offer changed: {} {:?}, reason {reason}",
                row.state,
                row.outcome
            );
            ensure!(
                fixture.backed_off().await? != at_shutdown,
                "the {} release did not keep its schedule rule",
                if at_shutdown {
                    "shutdown's"
                } else {
                    "deferred"
                }
            );
            frontend.ledger.pool.close().await;
            fixture.due_now().await?;
            let recovered = fixture
                .successor
                .claim_candidate(10)
                .await?
                .context("the released offer was not claimable")?;
            ensure!(recovered.lifecycle.state == CandidateState::Offered);
            fixture
                .successor_coordinator
                .process_candidate(&recovered)
                .await?;
            ensure!(fixture.state().await? == "submitted");
            ensure!(fixture.landed().await?);
            ensure!(
                fixture.submissions().await == 1,
                "the recovery offered the block again"
            );
            Ok::<_, anyhow::Error>(())
        }
        .await;
        fixture.close().await?;
        result.with_context(|| format!("at shutdown: {at_shutdown}"))?;
    }
    Ok(())
}

/// The attempt holds what it believes is its unsent reservation, but the row
/// it finds is `pending` again under its own claim, as after a failover to a
/// standby that had the claim's commit and not the reservation's. The return
/// to `pending` misses its fence; the claim is then freed at once with the
/// ordinary release instead of being left to its 30 s lease, and the block is
/// offered once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unsent_return_that_finds_the_reservation_gone_frees_the_claim_at_once() -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let looping = fixture
            .start_loop(
                fixture.coordinator.config.rpc_url.clone(),
                PRE_SEND_LEASE,
                Duration::from_secs(1),
                Park::BeforeSend,
            )
            .await?;
        // The reservation's commit is lost; the claim's is not.
        sqlx::query("UPDATE qbit_block_candidate_outbox SET state='pending',offer_reserved_at=NULL,offer_reserved_by=NULL WHERE block_hash=$1 AND claim_token IS NOT NULL")
            .bind(&fixture.claim.candidate.block_hash)
            .execute(&fixture.coordinator.ledger.pool)
            .await?;
        fixture.fail_next_updates("fail_renewal", RENEWAL, 1).await?;
        looping.probe()?.release.notify_one();
        // Freed well inside the 30 s lease, with the backoff of the ordinary
        // release.
        fixture.returned_unsent("renewal").await?;
        fixture.offered_once_more(looping).await
    }
    .await;
    fixture.close().await?;
    result
}
