//! The live session timer must account for both active and retained miner work.
use super::*;

struct LiveSession {
    backend: Arc<Backend>,
    config: StratumConfig,
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    task: tokio::task::JoinHandle<Result<()>>,
    _shutdown: watch::Sender<bool>,
    clock_guard: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for LiveSession {
    fn drop(&mut self) {
        self.task.abort();
        if let Some(task) = &self.clock_guard {
            task.abort();
        }
    }
}

impl LiveSession {
    async fn new(fail_build: bool) -> Self {
        let fixture = Fixture::new(Duration::from_secs(600)).await;
        fixture.coordinator.refresh_once().await.unwrap();
        let worker = fixture.job(1, 0, "original.worker").context.worker.clone();
        let next = fixture
            .coordinator
            .build_job(&worker, "00000001", 1e-12, 0.0)
            .await
            .unwrap();
        let backend = Arc::new(Backend {
            fixture,
            next: Mutex::new(next),
            fail_build: Mutex::new(fail_build),
            sequence: AtomicU64::new(1),
            slow_resume: Mutex::new(None),
            after_persist: Mutex::new(None),
        });
        let mut config = StratumConfig {
            initial_job_timeout_seconds: 0.5,
            max_jobs_per_connection: 1,
            job_retention_seconds: 3.0,
            stale_grace_seconds: 0.0,
            max_connections_per_username: 1,
            ..Default::default()
        };
        config.vardiff.enabled = false;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let (reader, writer) = client.into_split();
        let (shutdown, receiver) = watch::channel(false);
        let task = tokio::spawn(session(
            server,
            backend.clone(),
            config.clone(),
            backend.fixture.coordinator.refresh.subscribe(),
            receiver,
        ));
        Self {
            backend,
            config,
            reader: BufReader::new(reader),
            writer,
            task,
            _shutdown: shutdown,
            clock_guard: None,
        }
    }

    async fn read(&mut self) -> Value {
        let mut line = String::new();
        assert_ne!(
            self.reader.read_line(&mut line).await.unwrap(),
            0,
            "session disconnected while newer retained work remained creditable"
        );
        serde_json::from_str(&line).unwrap()
    }

    async fn request(&mut self, id: u64, method: &str, params: Value) -> Value {
        let mut bytes =
            serde_json::to_vec(&json!({"id":id,"method":method,"params":params})).unwrap();
        bytes.push(b'\n');
        self.writer.write_all(&bytes).await.unwrap();
        loop {
            let response = self.read().await;
            if response["id"] == id {
                return response;
            }
        }
    }

    async fn delivered(&mut self) -> MiningJob<JobContext> {
        loop {
            let value = self.read().await;
            if value["method"] == "mining.notify" {
                let mut job = self.backend.next.lock().unwrap().clone();
                job.wire.job_id = value["params"][0].as_str().unwrap().into();
                return job;
            }
        }
    }

    async fn login(&mut self) {
        assert!(self.request(1, "mining.subscribe", json!([])).await["error"].is_null());
        assert_eq!(
            self.request(2, "mining.authorize", json!(["original.worker", "x"]))
                .await["result"],
            true
        );
    }

    async fn submit(&mut self, id: u64, job: &MiningJob<JobContext>) -> Value {
        let proof = self.backend.fixture.proof(job, id as u32 * 10_000);
        self.request(
            id,
            "mining.submit",
            json!([
                "original.worker",
                job.wire.job_id,
                "00".repeat(8),
                format!("{:08x}", proof.ntime),
                format!("{:08x}", proof.nonce)
            ]),
        )
        .await
    }

    async fn mature_and_pause(&mut self) {
        // The established-connection deadline uses std::Instant. Cross that
        // short configured deadline in real time, then control timer/retention
        // ticks explicitly without waiting for the default thirty seconds.
        tokio::time::sleep(Duration::from_millis(600)).await;
        tokio::time::pause();
        self.clock_guard = Some(tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        }));
    }

    async fn tick(&self) {
        tokio::time::advance(Duration::from_secs(1)).await;
        // The timer's observed-tip hint reads the real Coordinator's local
        // publication; no node RPC or external readiness is involved here.
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
    }

    fn reserved_username_slots(&self) -> usize {
        self.config
            .username_connections
            .lock()
            .unwrap()
            .get("original.worker")
            .and_then(Weak::upgrade)
            .map_or(1, |slots| slots.available_permits())
    }
}

#[tokio::test]
async fn mature_session_keeps_newer_retained_work_after_real_resumed_job_expires() {
    let mut live = LiveSession::new(false).await;
    live.login().await;
    let original = live.delivered().await;
    for (id, difficulty) in [(3, 1e-10), (4, 2e-10)] {
        assert_eq!(
            live.request(id, "mining.suggest_difficulty", json!([difficulty]))
                .await["result"],
            true
        );
        let delivered = live.delivered().await;
        if id == 3 {
            assert_ne!(delivered.wire.job_id, original.wire.job_id);
        }
    }
    // N=1 leaves job-3 active, job-2 retained and job-1 durable-only.
    let mut newer = live.backend.next.lock().unwrap().clone();
    newer.wire.job_id = "job-3".into();
    live.mature_and_pause().await;
    {
        let store = &live.backend.fixture.store;
        let mut rows = store.jobs.lock().unwrap();
        let old = rows.get_mut(&original.wire.job_id).unwrap();
        old.expires_at_ms = store.database_now() + 200;
        old.payload["expires_at_ms"] = json!(old.expires_at_ms);
    }
    assert_eq!(live.submit(5, &original).await["result"], true);
    assert_eq!(live.reserved_username_slots(), 0);
    *live.backend.fail_build.lock().unwrap() = true;
    live.tick().await; // Removes the resumed job at its original absolute expiry.
    live.tick().await; // Previously disconnected despite the newer graveyard job.
    assert!(
        !live.task.is_finished(),
        "initial-job timer ignored creditable retained work"
    );
    assert_eq!(
        live.reserved_username_slots(),
        0,
        "creditable work keeps its original username permit"
    );
    assert_eq!(live.submit(6, &newer).await["result"], true);
    let records = live.backend.fixture.store.records.lock().unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[1].0.job_id, newer.wire.job_id);
    assert!(records[1].0.share_id.starts_with("original.worker"));
    drop(records);
    for _ in 0..3 {
        live.tick().await;
    }
    assert!(
        live.task.is_finished(),
        "all expired work must still release the session"
    );
    assert_eq!(live.reserved_username_slots(), 1);
    assert_eq!(live.config.stats.snapshot(1).connections, 0);
}

#[tokio::test]
async fn initial_job_timer_still_closes_unauthenticated_and_never_usable_sessions() {
    for authorized in [false, true] {
        let mut live = LiveSession::new(true).await;
        if authorized {
            live.login().await;
        } else {
            assert_eq!(
                live.request(1, "mining.get_health", json!([])).await["result"]["ready"],
                true
            );
        }
        live.mature_and_pause().await;
        live.tick().await;
        assert!(
            live.task.is_finished(),
            "a session without usable work must still time out"
        );
        assert_eq!(live.reserved_username_slots(), 1);
        assert_eq!(live.config.stats.snapshot(1).connections, 0);
        tokio::time::resume();
    }
}
