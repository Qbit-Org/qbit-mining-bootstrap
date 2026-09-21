//! Socket/session delivery with Coordinator's REAL credit and persistence decision.
use super::*;
use crate::coordinator::{
    miner_tests::{Fixture, Gate},
    JobContext,
};
use tokio::net::tcp::OwnedReadHalf;

struct Backend {
    fixture: Fixture,
    next: Mutex<MiningJob<JobContext>>,
    fail_build: Mutex<bool>,
    sequence: AtomicU64,
    slow_resume: Mutex<Option<(Instant, Arc<Gate>)>>,
    after_persist: Mutex<Option<Arc<Gate>>>,
}

impl MiningBackend for Backend {
    type Context = JobContext;
    async fn observed_tip_hint(&self) -> Option<RetentionTip> {
        self.fixture.coordinator.observed_tip_hint().await
    }
    async fn new_session_id(&self) -> Result<SessionId, StratumError> {
        Ok(SessionId::from(1))
    }
    async fn authorize(&self, _username: &str) -> Result<Worker, StratumError> {
        Ok(self.next.lock().unwrap().context.worker.clone())
    }
    async fn build_job(
        &self,
        _worker: &Worker,
        _extra: &str,
        _difficulty: f64,
        _minimum: f64,
    ) -> Result<MiningJob<JobContext>, StratumError> {
        if *self.fail_build.lock().unwrap() {
            return Err(StratumError::backend("controlled build failure"));
        }
        let mut job = self.next.lock().unwrap().clone();
        job.wire.job_id = format!("job-{}", self.sequence.fetch_add(1, Ordering::SeqCst));
        Ok(job)
    }
    async fn persist_issued_job(
        &self,
        worker: &Worker,
        job: &MiningJob<JobContext>,
        mask: u32,
        ttl: Duration,
    ) -> Result<(), StratumError> {
        self.fixture
            .coordinator
            .persist_issued_job(worker, job, mask, ttl)
            .await?;
        let gate = self.after_persist.lock().unwrap().take();
        if let Some(gate) = gate {
            // Delay delivery only after real persistence/revision checks have
            // succeeded. A pre-commit gate would instead reject the old job.
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        Ok(())
    }
    async fn resume_job(
        &self,
        worker: &Worker,
        id: &str,
    ) -> Result<Option<MiningJob<JobContext>>, StratumError> {
        let mut stored = self.fixture.coordinator.resume_job(worker, id).await?;
        let delay = self.slow_resume.lock().unwrap().take();
        if let Some((expires, gate)) = delay {
            // Fault injection AFTER the real persisted-work ownership,
            // revision, parent, reconstruction and absolute-expiry checks.
            stored.as_mut().unwrap().wire.resume_expires_at = Some(expires);
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        Ok(stored)
    }
    async fn submit(
        &self,
        worker: &Worker,
        job: &MiningJob<JobContext>,
        proof: Submission,
        grace: StaleGrace,
    ) -> Result<(), StratumError> {
        self.fixture
            .coordinator
            .submit(worker, job, proof, grace)
            .await
    }
}

struct Connection {
    backend: Backend,
    session: Session<JobContext>,
    config: StratumConfig,
    writer: OwnedWriteHalf,
    reader: BufReader<OwnedReadHalf>,
    _peer: OwnedWriteHalf,
    _server_reader: OwnedReadHalf,
    clock_guard: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(task) = &self.clock_guard {
            task.abort();
        }
    }
}

impl Connection {
    fn pause_time(&mut self) {
        tokio::time::pause();
        // Socket readiness is external to Tokio. Keep a runnable task so the
        // paused clock advances ONLY when this test explicitly requests it.
        self.clock_guard = Some(tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        }));
    }
    async fn new(retention: f64, max_jobs: usize) -> Self {
        Self::with_max_age(retention, max_jobs, Duration::from_secs(600)).await
    }
    async fn with_max_age(retention: f64, max_jobs: usize, max_age: Duration) -> Self {
        Self::with_fixture(Fixture::new(max_age).await, retention, max_jobs).await
    }
    async fn with_fixture(fixture: Fixture, retention: f64, max_jobs: usize) -> Self {
        fixture.coordinator.refresh_once().await.unwrap();
        let worker = fixture.job(1, 0, "original.worker").context.worker.clone();
        let first = fixture
            .coordinator
            .build_job(&worker, "00000000", 1e-12, 0.0)
            .await
            .unwrap();
        let config = StratumConfig {
            job_retention_seconds: retention,
            max_jobs_per_connection: max_jobs,
            ..Default::default()
        };
        let mut session = Session::new(&config, SessionObservation::new(config.stats.clone()));
        session.extranonce1 = Some(first.wire.extranonce1.clone());
        session.worker = Some(first.context.worker.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let (_server_reader, writer) = server.into_split();
        let (reader, _peer) = peer.into_split();
        Self {
            backend: Backend {
                fixture,
                next: Mutex::new(first),
                fail_build: Mutex::new(false),
                sequence: AtomicU64::new(1),
                slow_resume: Mutex::new(None),
                after_persist: Mutex::new(None),
            },
            session,
            config,
            writer,
            reader: BufReader::new(reader),
            _peer,
            _server_reader,
            clock_guard: None,
        }
    }
    async fn read(&mut self) -> Value {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        serde_json::from_str(&line).unwrap()
    }
    async fn deliver(&mut self) -> MiningJob<JobContext> {
        deliver_job(
            &self.backend,
            &mut self.session,
            &mut self.writer,
            &self.config,
            &crate::metrics::Metrics::default(),
        )
        .await
        .unwrap();
        assert_eq!(self.read().await["method"], "mining.set_difficulty");
        assert_eq!(self.read().await["method"], "mining.notify");
        self.session.jobs.back().unwrap().job.clone()
    }
    async fn submit(&mut self, job: &MiningJob<JobContext>, nonce: u32) -> Value {
        let proof = self.backend.fixture.proof(job, nonce);
        self.request(json!({"id":41,"method":"mining.submit","params":[
            self.session.worker.as_ref().unwrap().username,job.wire.job_id,"00".repeat(8),
            format!("{:08x}",proof.ntime),format!("{:08x}",proof.nonce)]}))
            .await
    }
    async fn request(&mut self, input: Value) -> Value {
        request(
            &self.backend,
            &mut self.session,
            &mut self.writer,
            &self.config,
            input,
            tokio::time::Instant::now(),
            // One registry for Stratum and coordinator decisions, as in production.
            &self.backend.fixture.coordinator.metrics,
        )
        .await
        .unwrap();
        self.read().await
    }
    async fn tip(&mut self, tip: u8) {
        self.backend.fixture.node.lock().unwrap().tip = format!("{tip:02x}").repeat(32);
        self.backend
            .fixture
            .coordinator
            .refresh_once()
            .await
            .unwrap();
        let worker = self.backend.next.lock().unwrap().context.worker.clone();
        *self.backend.next.lock().unwrap() = self
            .backend
            .fixture
            .coordinator
            .build_job(&worker, "00000000", 1e-12, 0.0)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn failed_replacement_keeps_retired_job_until_actual_delivery_then_expires() {
    let mut client = Connection::new(3.0, 64).await;
    let old = client.deliver().await;
    client.deliver().await; // Retired same-tip work, before the observation.
    client.tip(2).await;
    *client.backend.fail_build.lock().unwrap() = true;
    assert!(deliver_job(
        &client.backend,
        &mut client.session,
        &mut client.writer,
        &client.config,
        &crate::metrics::Metrics::default(),
    )
    .await
    .is_err());
    client.pause_time();
    tokio::time::advance(Duration::from_secs(20)).await;
    assert_eq!(client.submit(&old, 0).await["result"], true);
    *client.backend.fail_build.lock().unwrap() = false;
    client.deliver().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    let result = client.submit(&old, 100).await;
    assert_eq!(result["result"], true, "{result}");
    tokio::time::advance(Duration::from_secs(2)).await;
    let rejected = client.submit(&old, 200).await;
    assert_eq!(rejected["error"][1], "stale job");
    assert!(matches!(
        rejected["error"][2]["reason_id"].as_str(),
        Some("stale-job" | "unknown-job")
    ));
}

#[tokio::test]
async fn same_tip_delivery_does_not_slide_grace_but_real_flip_reanchors() {
    let mut client = Connection::new(30.0, 64).await;
    let old = client.deliver().await;
    client.tip(2).await;
    client.deliver().await;
    let first = client.session.tip_work_delivered.clone();
    client.pause_time();
    tokio::time::advance(Duration::from_secs(2)).await;
    client.deliver().await;
    assert_eq!(client.session.tip_work_delivered, first);
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(
        client.submit(&old, 0).await["error"][2]["reason_id"],
        "stale-job"
    );
    tokio::time::resume();
    client.tip(1).await;
    // A reorg also replaces the payout revision; use work delivered for that
    // publication, which retires the prior same-parent payout snapshot.
    let reorg_work = client.deliver().await;
    client.tip(2).await;
    client.deliver().await;
    assert_ne!(client.session.tip_work_delivered, first);
    assert_eq!(client.submit(&reorg_work, 100).await["result"], true);
}

#[tokio::test]
async fn same_tip_capacity_eviction_survives_beyond_one_second_and_keeps_original_worker() {
    let mut client = Connection::new(30.0, 1).await;
    let old = client.deliver().await;
    client.deliver().await;
    assert!(client
        .session
        .jobs
        .iter()
        .all(|issued| issued.job.wire.job_id != old.wire.job_id));
    client.pause_time();
    tokio::time::advance(Duration::from_secs(2)).await;
    client.session.worker.as_mut().unwrap().username = "new.worker".into();
    assert_eq!(client.submit(&old, 0).await["result"], true);
    let duplicate = client.submit(&old, 0).await;
    assert_eq!(duplicate["error"][1], "duplicate share");
    assert_eq!(duplicate["error"][2]["reason_id"], "duplicate-share");
    let records = client.backend.fixture.store.records.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert!(records[0].0.share_id.starts_with("original.worker:"));
    assert_eq!(
        records[0].0.share_difficulty,
        codec::scaled_target_difficulty(&old.wire.share_target).unwrap()
    );
    assert_eq!(records[0].0.credit_policy, None);
}

#[tokio::test]
async fn unknown_job_rejects_before_any_node_rpc_in_real_request_and_coordinator_resume() {
    let mut client = Connection::new(30.0, 64).await;
    client.deliver().await;
    client.backend.fixture.node.lock().unwrap().calls.clear();
    let response = client
        .request(json!({"id":41,"method":"mining.submit","params":[
        "original.worker","unknown-job","00".repeat(8),"6b49d200","00000000"]}))
        .await;
    assert_eq!(response["error"][1], "stale job");
    assert_eq!(response["error"][2]["reason_id"], "unknown-job");
    assert!(client.backend.fixture.node.lock().unwrap().calls.is_empty());
}

#[tokio::test]
async fn pending_tip_never_extends_an_absolute_resume_expiry() {
    let mut client = Connection::new(30.0, 64).await;
    let job = client.deliver().await;
    client
        .session
        .jobs
        .back_mut()
        .unwrap()
        .job
        .wire
        .resume_expires_at = Some(tokio::time::Instant::now().into_std() + Duration::from_secs(1));
    client.tip(2).await;
    client.pause_time();
    tokio::time::advance(Duration::from_secs(2)).await;
    client.session.prune_jobs(
        &client.config,
        client
            .backend
            .fixture
            .coordinator
            .observed_tip_hint()
            .await
            .as_ref(),
    );
    assert!(client
        .session
        .jobs
        .iter()
        .all(|issued| issued.job.wire.job_id != job.wire.job_id));
}

#[tokio::test]
async fn slow_resume_crossing_absolute_expiry_is_rejected_before_coordinator_submit() {
    let mut client = Connection::new(30.0, 1).await;
    let old = client.deliver().await;
    client.deliver().await;
    client.pause_time();
    // Reconnect recovery has no same-connection graveyard capability.
    client.session.retained = retained_jobs::RetainedJobs::default();
    let gate = Arc::new(Gate::default());
    *client.backend.slow_resume.lock().unwrap() = Some((
        tokio::time::Instant::now().into_std() + Duration::from_secs(1),
        gate.clone(),
    ));
    let (response, ()) = tokio::join!(client.submit(&old, 0), async {
        gate.entered.notified().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        gate.release.notify_one();
    });
    assert_eq!(response["error"][1], "stale job");
    assert_eq!(response["error"][2]["reason_id"], "stale-job");
    assert!(client
        .backend
        .fixture
        .store
        .records
        .lock()
        .unwrap()
        .is_empty());
}

/// Scrape `/metrics` from the real API router over a real HTTP connection.
async fn http_metrics(metrics: Arc<crate::metrics::Metrics>) -> String {
    use tokio::io::AsyncReadExt;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://invalid@127.0.0.1:1/invalid")
        .unwrap();
    let state = crate::api::ApiState::new(pool, crate::api::ApiConfig::default(), metrics.clone());
    state.publish_metrics(metrics.render()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, crate::api::router(state))
            .await
            .unwrap()
    });
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: prism\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    server.abort();
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    assert!(head.starts_with("HTTP/1.1 200 "), "{head}");
    body.to_owned()
}

#[tokio::test]
async fn resume_expiry_is_attributed_before_a_failing_fee_floor_and_counted_once() {
    use crate::coordinator::miner_tests::stale_causes::stale_causes;
    let fixture = Fixture::build(
        Duration::from_secs(600),
        |config| {
            config.ctv_enabled = true;
            config.ctv_fee = Some(qbit_prism::FanoutFeeRatePolicy::new(1000, 12000));
        },
        None,
    )
    .await;
    let mut client = Connection::with_fixture(fixture, 30.0, 1).await;
    let metrics = client.backend.fixture.coordinator.metrics.clone();
    let stale_rejections = |metrics: &crate::metrics::Metrics| {
        let key = "qbit_prism_rejections_total{reason_id=\"stale-job\"} ";
        let body = metrics.render();
        let values: Vec<f64> = body
            .lines()
            .filter_map(|line| line.strip_prefix(key))
            .map(|value| value.parse().unwrap())
            .collect();
        assert_eq!(values.len(), 1);
        values[0]
    };
    let old = client.deliver().await;
    let current = client.deliver().await;
    client.pause_time();
    client.session.retained = retained_jobs::RetainedJobs::default();
    let coordinator = client.backend.fixture.coordinator.clone();
    let gate = Arc::new(Gate::default());
    *client.backend.slow_resume.lock().unwrap() = Some((
        tokio::time::Instant::now().into_std() + Duration::from_secs(1),
        gate.clone(),
    ));
    let response = {
        let submit = client.submit(&old, 0);
        tokio::pin!(submit);
        tokio::select! {
            // A resume that answers before its pause fails here instead of
            // leaving the gate waiter pending forever.
            response = &mut submit => panic!("resume returned before reaching its gate: {response}"),
            () = gate.entered.notified() => {
                // Resume has passed its own fee admission. While it waits, the
                // floor rises and the lease expires, so both predicates fail at
                // Stratum's expiry check and at the coordinator's fee check behind it.
                crate::coordinator::miner_tests::raise_ctv_fee_floor(&coordinator).await;
                tokio::time::advance(Duration::from_secs(2)).await;
                gate.release.notify_one();
                submit.await
            }
        }
    };
    assert_eq!(
        response,
        json!({"id":41,"result":null,"error":[21,"stale job",{"reason_id":"stale-job"}]})
    );
    assert_eq!(stale_causes(&metrics), [1., 0., 0., 0.]);
    assert_eq!(
        stale_rejections(&metrics),
        1.,
        "coarse reason still counted once"
    );

    // Unexpired current work reaches the coordinator, whose fee floor refuses it.
    let response = client.submit(&current, 100).await;
    assert_eq!(
        response,
        json!({"id":41,"result":null,"error":[21,"job CTV fee is below the current relay floor",{"reason_id":"stale-job"}]})
    );
    assert_eq!(stale_causes(&metrics), [1., 1., 0., 0.]);
    assert_eq!(stale_rejections(&metrics), 2.);
    assert!(client
        .backend
        .fixture
        .store
        .records
        .lock()
        .unwrap()
        .is_empty());

    tokio::time::resume();
    let body = http_metrics(metrics).await;
    for expected in [
        "# TYPE qbit_prism_stale_job_rejections_total counter",
        "qbit_prism_stale_job_rejections_total{cause=\"resume_expired\"} 1",
        "qbit_prism_stale_job_rejections_total{cause=\"fee_floor\"} 1",
        "qbit_prism_stale_job_rejections_total{cause=\"parent_grace\"} 0",
        "qbit_prism_stale_job_rejections_total{cause=\"payout_revision\"} 0",
        "qbit_prism_rejections_total{reason_id=\"stale-job\"} 2",
    ] {
        assert!(
            body.lines().any(|line| line == expected),
            "missing {expected}"
        );
    }
    let excerpt: Vec<_> = body
        .lines()
        .filter(|line| line.contains("qbit_prism_stale_job_rejections_total"))
        .collect();
    println!(
        "HTTP /metrics excerpt after real stale-job decisions:\n{}",
        excerpt.join("\n")
    );
}

#[path = "retained_tests.rs"]
mod retained_tests;

#[path = "session_timer_tests.rs"]
mod session_timer_tests;
