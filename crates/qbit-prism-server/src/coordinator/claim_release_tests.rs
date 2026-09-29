//! #529 on a real PostgreSQL: a failed attempt whose claim release also
//! failed, because the database stopped answering (a failover takes the
//! pool's connections with the old primary), is released by the holder once
//! the database answers again, instead of holding the row for the rest of
//! its lease. A third frontend reaches the fixture's database through a TCP
//! relay the test cuts and heals; its submit loop runs with a 30 s lease, so
//! only the release can free the row within these tests' bounds.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

/// Long enough that the lease cannot free the row during a test; a short
/// heartbeat and bound, so the cut fails the attempt and its first release
/// at once.
const LOOP_LEASE: CandidateLease = CandidateLease {
    seconds: 30,
    interval: Duration::from_millis(200),
    timeout: Duration::from_millis(500),
    rebuild_deadline: Duration::from_secs(60),
};

/// A TCP relay in front of the fixture's PostgreSQL. [`DbRelay::cut`] drops
/// every open connection and closes new ones as soon as they are accepted,
/// which is what a frontend's pool sees across a failover, until
/// [`DbRelay::heal`].
struct DbRelay {
    port: u16,
    cut: Arc<AtomicBool>,
    connections: Arc<std::sync::Mutex<Vec<tokio::task::AbortHandle>>>,
    accept: JoinHandle<()>,
}

impl DbRelay {
    async fn open(upstream: String) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let cut = Arc::new(AtomicBool::new(false));
        let connections: Arc<std::sync::Mutex<Vec<tokio::task::AbortHandle>>> = Arc::default();
        let accept = tokio::spawn({
            let (cut, connections) = (cut.clone(), connections.clone());
            async move {
                while let Ok((mut client, _)) = listener.accept().await {
                    let upstream = upstream.clone();
                    let relay = tokio::spawn(async move {
                        if let Ok(mut server) = tokio::net::TcpStream::connect(&upstream).await {
                            let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
                        }
                    });
                    let mut open = connections.lock().unwrap();
                    if cut.load(Ordering::SeqCst) {
                        relay.abort();
                    } else {
                        open.push(relay.abort_handle());
                    }
                }
            }
        });
        Ok(Self {
            port,
            cut,
            connections,
            accept,
        })
    }

    fn cut(&self) {
        let mut open = self.connections.lock().unwrap();
        self.cut.store(true, Ordering::SeqCst);
        for relay in open.drain(..) {
            relay.abort();
        }
    }

    fn heal(&self) {
        let _open = self.connections.lock().unwrap();
        self.cut.store(false, Ordering::SeqCst);
    }
}

impl Drop for DbRelay {
    fn drop(&mut self) {
        self.cut();
        self.accept.abort();
    }
}

/// The relayed frontend's submit loop, with its attempt on the fixture's
/// candidate parked at the offer probe, after the durable reservation.
struct Parked {
    relay: DbRelay,
    probe: Arc<OfferProbe>,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl Parked {
    /// Until the parked attempt has left the probe: its work was dropped.
    async fn work_dropped(&self) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !self.probe.left.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("the cut did not end the parked attempt")
    }

    async fn stop(self) -> Result<()> {
        self.shutdown.send(true)?;
        tokio::time::timeout(Duration::from_secs(10), self.task)
            .await
            .context("the relayed submit loop did not stop")??;
        Ok(())
    }
}

impl Fixture {
    /// Hand the fixture's candidate to a relayed frontend's submit loop,
    /// running under `lease`, and wait until its attempt is parked with the
    /// reservation taken.
    async fn park_relayed(&self, lease: CandidateLease) -> Result<Parked> {
        let url = url::Url::parse(&self.coordinator.config.database_url)?;
        let upstream = format!(
            "{}:{}",
            url.host_str().context("database URL without a host")?,
            url.port_or_known_default().unwrap_or(5432)
        );
        let relay = DbRelay::open(upstream).await?;
        let mut relayed = url.clone();
        relayed.set_host(Some("127.0.0.1")).context("relay host")?;
        relayed
            .set_port(Some(relay.port))
            .map_err(|()| anyhow::anyhow!("relay port"))?;
        let frontend = Coordinator::new(
            Config {
                instance_id: "candidate-relayed".into(),
                database_url: relayed.to_string(),
                ..(*self.coordinator.config).clone()
            },
            Arc::new(crate::metrics::Metrics::default()),
        )
        .await?;
        *frontend.observed_tip.write().await = TipState::baseline("aa".repeat(32));
        let probe = Arc::new(OfferProbe::default());
        *frontend.offer_probe.lock().unwrap() = Some(probe.clone());
        self.coordinator
            .ledger
            .retry_candidate(&self.claim, "handed to the relayed frontend")
            .await?;
        sqlx::query("UPDATE qbit_block_candidate_outbox SET next_attempt_at=clock_timestamp() WHERE block_hash=$1")
            .bind(&self.claim.candidate.block_hash)
            .execute(&self.coordinator.ledger.pool)
            .await?;
        let (shutdown, receiver) = watch::channel(false);
        let task = tokio::spawn(frontend.submit_loop_with(lease, receiver));
        tokio::time::timeout(Duration::from_secs(10), probe.entered.notified())
            .await
            .context("the relayed loop never reserved the offer")?;
        let row = self.row().await?;
        ensure!(
            row.state == "offer_reserved"
                && row.reserved_by.as_deref() == Some("candidate-relayed"),
            "the parked attempt holds no reservation: {} by {:?}",
            row.state,
            row.reserved_by
        );
        Ok(Parked {
            relay,
            probe,
            shutdown,
            task,
        })
    }

    /// The row's claim: its holder's instance, and whether it is live.
    async fn claim_holder(&self) -> Result<(Option<String>, Option<String>, bool)> {
        Ok(sqlx::query_as(
            "SELECT claim_token,claim_instance_id,COALESCE(claim_expires_at>clock_timestamp(),false) FROM qbit_block_candidate_outbox WHERE block_hash=$1",
        )
        .bind(&self.claim.candidate.block_hash)
        .fetch_one(&self.coordinator.ledger.pool)
        .await?)
    }
}

/// The relayed frontend's attempt dies with its connections and its release
/// fails with them. Once the database answers again, the holder releases the
/// claim itself, well inside the 30 s lease, and only after the attempt's work
/// was dropped. Another frontend then recovers the reservation, which is never
/// offered again, and lands it with the outcome `unknown` (#529 a); no
/// `submitblock` was ever made.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_release_is_retried_after_the_attempt_stops_and_frees_the_row_before_its_lease(
) -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let parked = fixture.park_relayed(LOOP_LEASE).await?;
        parked.relay.cut();
        // Past the immediate release's own bound: the release is deferred.
        tokio::time::sleep(LOOP_LEASE.timeout + Duration::from_millis(300)).await;
        let (token, holder, live) = fixture.claim_holder().await?;
        ensure!(
            token.is_some() && holder.as_deref() == Some("candidate-relayed") && live,
            "the cut attempt's claim was released without the database: {holder:?}, live {live}"
        );
        let healed = Instant::now();
        parked.relay.heal();
        let freed = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if fixture.claim_holder().await?.0.is_none() {
                    // Read at once: the release must not have overtaken the
                    // work it frees.
                    return Ok::<_, anyhow::Error>(parked.probe.left.load(Ordering::SeqCst));
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("the holder never released its claim once the database answered; the row waits for the 30 s lease")??;
        ensure!(freed, "the claim was released while the attempt's work was still running");
        let released_after = healed.elapsed();
        parked.stop().await?;
        let row = fixture.row().await?;
        ensure!(
            row.state == "offer_reserved" && row.outcome.is_none() && row.offered_at_ms.is_none(),
            "the released row changed its reservation: {} {:?}",
            row.state,
            row.outcome
        );

        // Recovery on another frontend: the block reached the chain through
        // an offer whose answer was never recorded.
        fixture.activate_on_node().await;
        sqlx::query("UPDATE qbit_block_candidate_outbox SET next_attempt_at=clock_timestamp() WHERE block_hash=$1")
            .bind(&fixture.claim.candidate.block_hash)
            .execute(&fixture.coordinator.ledger.pool)
            .await?;
        let recovered = fixture
            .successor
            .claim_candidate(10)
            .await?
            .context("the released row could not be claimed")?;
        ensure!(recovered.lifecycle.state == CandidateState::OfferReserved);
        fixture
            .successor_coordinator
            .process_candidate(&recovered)
            .await?;
        let row = fixture.row().await?;
        ensure!(
            row.state == "submitted"
                && row.outcome.as_deref() == Some("unknown")
                && row.offered_at_ms.is_none()
                && row.reserved_by.as_deref() == Some("candidate-relayed"),
            "the recovered reservation landed as {} with outcome {:?} at {:?} reserved by {:?}",
            row.state,
            row.outcome,
            row.offered_at_ms,
            row.reserved_by
        );
        ensure!(fixture.landed().await?);
        ensure!(
            fixture.submissions().await == 0,
            "a reservation was offered again"
        );
        eprintln!(
            "deferred release: claim released {:.2} s after the database answered (lease {} s)",
            released_after.as_secs_f64(),
            LOOP_LEASE.seconds
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    fixture.close().await?;
    result
}

/// A deferred release is fenced on its own token: once the lease expired and
/// another frontend took the row, the holder's release, retried after the
/// database answers, leaves the new claim untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deferred_release_never_frees_a_row_another_frontend_took() -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let parked = fixture.park_relayed(LOOP_LEASE).await?;
        parked.relay.cut();
        parked.work_dropped().await?;
        tokio::time::sleep(LOOP_LEASE.timeout + Duration::from_millis(300)).await;
        fixture.expire().await?;
        let taken = fixture
            .successor
            .claim_candidate(10)
            .await?
            .context("the expired row could not be claimed")?;
        ensure!(taken.lifecycle.state == CandidateState::OfferReserved);
        parked.relay.heal();
        // Retries are due every 200 ms; watch the new claim through several.
        let watched = Instant::now();
        while watched.elapsed() < Duration::from_secs(3) {
            let (token, holder, live) = fixture.claim_holder().await?;
            ensure!(
                token.as_deref() == Some(taken.claim_token.as_str())
                    && holder.as_deref() == Some("candidate-successor")
                    && live,
                "the old holder's deferred release changed another frontend's claim: {holder:?}, live {live}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        parked.stop().await?;
        fixture.successor_coordinator.process_candidate(&taken).await?;
        ensure!(
            fixture.submissions().await == 0,
            "a reservation was offered again"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    fixture.close().await?;
    result
}

/// A shutdown attempts each deferred release once. The heartbeat interval is
/// 1 s here, so the next scheduled retry falls after the shutdown and only
/// the shutdown's attempt can free the row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_attempts_each_deferred_release_once() -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let lease = CandidateLease {
        interval: Duration::from_secs(1),
        timeout: Duration::from_millis(300),
        ..LOOP_LEASE
    };
    let result = async {
        let parked = fixture.park_relayed(lease).await?;
        parked.relay.cut();
        parked.work_dropped().await?;
        // The immediate release has failed; the next retry is about 1 s out.
        tokio::time::sleep(lease.timeout + Duration::from_millis(100)).await;
        let (token, ..) = fixture.claim_holder().await?;
        ensure!(
            token.is_some(),
            "the claim was released without the database"
        );
        parked.relay.heal();
        parked.stop().await?;
        let (token, holder, _) = fixture.claim_holder().await?;
        ensure!(
            token.is_none(),
            "shutdown left the deferred release to the lease: still held by {holder:?}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    fixture.close().await?;
    result
}
