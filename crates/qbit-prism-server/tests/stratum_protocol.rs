use qbit_pool_builder::{build_manifest, CoinbaseBuildRequest, WeightedEntitlement};
use qbit_prism_server::{
    codec::{Job, Submission},
    stratum::*,
};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream,
    },
    sync::watch,
    time::timeout,
};

type StoredMockJob = (MiningJob<()>, Worker, u32, Instant);
type MockDifficulties = Mutex<HashMap<(String, String), (f64, Instant)>>;

#[derive(Default)]
struct Backend {
    sessions: AtomicU32,
    generation: AtomicU64,
    payout_revision: AtomicU64,
    jobs: AtomicU64,
    shares: Mutex<HashSet<String>>,
    credited_workers: Mutex<Vec<String>>,
    grace: Mutex<Vec<bool>>,
    stored: Mutex<HashMap<String, StoredMockJob>>,
    hints: MockDifficulties,
    ready: AtomicBool,
    network_bits: AtomicU32,
    fail_builds: AtomicU32,
    hint_reads_unavailable: AtomicBool,
    submit_gate: Mutex<Option<Arc<observability::Gate>>>,
    submit_error: Mutex<Option<StratumError>>,
    build_gate: Mutex<Option<Arc<observability::Gate>>>,
    hint_gate: Mutex<Option<Arc<observability::Gate>>>,
    authorize_calls: AtomicU32,
    resume_calls: AtomicU32,
}

impl MiningBackend for Backend {
    async fn observed_tip_hint(&self) -> Option<RetentionTip> {
        let generation = self.generation.load(Ordering::SeqCst);
        Some(RetentionTip {
            hash: format!("{generation:064x}"),
            parent: None,
            transitioned: generation > 0,
        })
    }
    type Context = ();
    async fn health_ready(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
    }
    async fn worker_difficulty(
        &self,
        listener: &str,
        worker: &Worker,
        ttl: u64,
    ) -> anyhow::Result<Option<(f64, Duration)>> {
        anyhow::ensure!(
            !self.hint_reads_unavailable.load(Ordering::Relaxed),
            "hint store unavailable"
        );
        Ok(self
            .hints
            .lock()
            .unwrap()
            .get(&(listener.into(), worker.username.clone()))
            .filter(|(_, at)| at.elapsed() <= Duration::from_secs(ttl))
            .map(|(difficulty, at)| (*difficulty, at.elapsed())))
    }
    async fn remember_worker_difficulty(
        &self,
        listener: &str,
        worker: &Worker,
        difficulty: f64,
        evidence: Option<&str>,
        downward: bool,
    ) -> anyhow::Result<()> {
        let gate = self.hint_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.wait().await;
        }
        let mut hints = self.hints.lock().unwrap();
        let key = (listener.into(), worker.username.clone());
        if downward {
            if let Some((old, _)) = hints.get_mut(&key) {
                *old = old.min(difficulty);
            }
        } else if evidence.is_some() {
            hints.insert(key, (difficulty, Instant::now()));
        }
        Ok(())
    }
    async fn new_session_id(&self) -> Result<qbit_prism_server::ledger::SessionId, StratumError> {
        Ok((self.sessions.fetch_add(1, Ordering::Relaxed) + 1).into())
    }
    async fn authorize(&self, username: &str) -> Result<Worker, StratumError> {
        self.authorize_calls.fetch_add(1, Ordering::SeqCst);
        if !username.starts_with("miner") {
            return Err(StratumError::new(
                20,
                "invalid payout",
                "unauthorized-worker",
            ));
        }
        Ok(Worker {
            username: username.into(),
            payout_address: username.split('.').next().unwrap().into(),
            worker_name: username.split_once('.').map(|(_, w)| w.into()),
            p2mr_program_hex: "ab".repeat(32),
        })
    }
    async fn build_job(
        &self,
        worker: &Worker,
        extranonce1: &str,
        difficulty: f64,
        minimum: f64,
    ) -> Result<MiningJob<()>, StratumError> {
        if self
            .fail_builds
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(StratumError::backend("temporary builder failure"));
        }
        let gate = self.build_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.wait().await;
        }
        let generation = self.generation.load(Ordering::SeqCst);
        let bits = self.network_bits.load(Ordering::Relaxed);
        let bits = if bits == 0 { 0x207fffff } else { bits };
        let template = json!({"version":0x20000000u32,"bits":format!("{bits:08x}"),"curtime":1_700_000_000u32,"previousblockhash":format!("{generation:064x}"),"transactions":[]});
        let manifest = build_manifest(CoinbaseBuildRequest {
            block_height: 1 + generation,
            coinbase_value_sats: 5_000_000_000,
            entitlements: vec![WeightedEntitlement {
                recipient_id: worker.payout_address.clone(),
                order_key: worker.payout_address.clone(),
                p2mr_program_hex: worker.p2mr_program_hex.clone(),
                weight: 1,
            }],
            witness_nonce_hex: None,
            witness_merkle_leaves_hex: vec![],
            coinbase_script_sig_suffix_hex: Some(format!("{extranonce1}{}", "00".repeat(8))),
            pinned_first_output: None,
        })
        .unwrap();
        let mut job = Job::from_manifest(
            format!("job-{}", self.jobs.fetch_add(1, Ordering::Relaxed)),
            &template,
            &manifest,
            extranonce1,
            8,
            difficulty,
            minimum,
            true,
        )
        .unwrap();
        job.refresh_generation = generation;
        job.payout_revision = self.payout_revision.load(Ordering::Relaxed) as i64;
        Ok(MiningJob {
            wire: job,
            context: Arc::new(()),
        })
    }
    async fn submit(
        &self,
        worker: &Worker,
        job: &MiningJob<()>,
        submission: Submission,
        grace: StaleGrace,
    ) -> Result<(), StratumError> {
        let gate = self.submit_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.wait().await;
        }
        if let Some(error) = self.submit_error.lock().unwrap().take() {
            return Err(error);
        }
        let tip = format!("{:064x}", self.generation.load(Ordering::SeqCst));
        let grace = grace.eligible_for(&tip);
        if job.wire.previousblockhash != tip && !grace {
            return Err(StratumError::new(21, "stale job", "stale-job"));
        }
        if !submission.share_pass && !submission.block_pass {
            return Err(StratumError::new(
                23,
                "low difficulty share",
                "low-difficulty",
            ));
        }
        if !self
            .shares
            .lock()
            .unwrap()
            .insert(submission.block_hash_hex)
        {
            return Err(StratumError::new(22, "duplicate share", "duplicate-share"));
        }
        self.credited_workers
            .lock()
            .unwrap()
            .push(worker.username.clone());
        self.grace.lock().unwrap().push(grace);
        Ok(())
    }
    async fn persist_issued_job(
        &self,
        worker: &Worker,
        job: &MiningJob<()>,
        mask: u32,
        ttl: Duration,
    ) -> Result<(), StratumError> {
        self.stored.lock().unwrap().insert(
            job.wire.job_id.clone(),
            (job.clone(), worker.clone(), mask, Instant::now() + ttl),
        );
        Ok(())
    }
    async fn resume_job(
        &self,
        worker: &Worker,
        id: &str,
    ) -> Result<Option<MiningJob<()>>, StratumError> {
        self.resume_calls.fetch_add(1, Ordering::SeqCst);
        let stored = self.stored.lock().unwrap();
        let Some((job, original, mask, expires)) = stored.get(id) else {
            return Ok(None);
        };
        if worker.username != original.username
            || Instant::now() >= *expires
            || job.wire.previousblockhash
                != format!("{:064x}", self.generation.load(Ordering::SeqCst))
            || job.wire.payout_revision != self.payout_revision.load(Ordering::Relaxed) as i64
        {
            return Ok(None);
        }
        let mut job = job.clone();
        job.wire.version_mask = *mask;
        job.wire.resume_expires_at = Some(*expires);
        Ok(Some(job))
    }
}

struct Client {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    extranonce1: String,
    notify: Value,
    difficulty: f64,
}
impl Client {
    async fn connect(address: std::net::SocketAddr) -> Self {
        let (reader, writer) = TcpStream::connect(address).await.unwrap().into_split();
        Self {
            reader: BufReader::new(reader),
            writer,
            extranonce1: String::new(),
            notify: Value::Null,
            difficulty: 0.0,
        }
    }
    async fn send(&mut self, value: Value) {
        self.writer
            .write_all(format!("{value}\n").as_bytes())
            .await
            .unwrap();
    }
    async fn read(&mut self) -> Value {
        let mut line = String::new();
        assert!(
            timeout(Duration::from_secs(5), self.reader.read_line(&mut line))
                .await
                .unwrap()
                .unwrap()
                > 0
        );
        let value: Value = serde_json::from_str(&line).unwrap();
        if value["method"] == "mining.set_difficulty" {
            self.difficulty = value["params"][0].as_f64().unwrap();
        }
        value
    }
    async fn request_health(&mut self, id: u64) -> Value {
        self.send(json!({"id":id,"method":"mining.get_health","params":[]}))
            .await;
        self.response(id).await
    }
    /// A well-formed submit for a job this session was never issued.
    fn unknown_job_submit(&self, id: u64, username: &str, job_id: &str) -> Value {
        json!({"id":id,"method":"mining.submit","params":[username,job_id,"0".repeat(16),"00000000","00000000"]})
    }
    async fn request_submit(&mut self, id: u64, username: &str, job_id: &str) -> Value {
        let request = self.unknown_job_submit(id, username, job_id);
        self.send(request).await;
        self.response(id).await
    }
    /// Read every remaining response and require the listener to close. The
    /// error answer that names a spent budget arrives before the close.
    async fn expect_closed(&mut self) -> Vec<Value> {
        let mut seen = Vec::new();
        loop {
            let mut line = String::new();
            let read = timeout(Duration::from_secs(5), self.reader.read_line(&mut line))
                .await
                .expect("the listener never closed the connection")
                .unwrap();
            if read == 0 {
                return seen;
            }
            seen.push(serde_json::from_str(&line).unwrap());
        }
    }
    async fn response(&mut self, id: u64) -> Value {
        loop {
            let value = self.read().await;
            if value["id"] == id {
                return value;
            }
        }
    }
    async fn next_job(&mut self) {
        loop {
            let value = self.read().await;
            if value["method"] == "mining.notify" {
                self.notify = value;
                break;
            }
        }
    }
    async fn login(&mut self, username: &str) {
        self.send(json!({"id":1,"method":"mining.subscribe","params":[]}))
            .await;
        self.extranonce1 = self.response(1).await["result"][1].as_str().unwrap().into();
        self.send(json!({"id":2,"method":"mining.authorize","params":[username,"x"]}))
            .await;
        assert_eq!(self.response(2).await["result"], true);
        self.next_job().await;
    }
    fn solved_submit(&self, id: u64, username: &str, nonce_start: u32) -> Value {
        self.solved_submit_version(id, username, nonce_start, None)
    }
    fn solved_share(&self, id: u64, username: &str, nonce_start: u32) -> Value {
        let p = self.notify["params"].as_array().unwrap();
        let coinbase = hex::decode(format!(
            "{}{}0000000000000000{}",
            p[2].as_str().unwrap(),
            self.extranonce1,
            p[3].as_str().unwrap()
        ))
        .unwrap();
        let merkle = qbit_prism_server::codec::double_sha256(&coinbase);
        let mut previous = hex::decode(p[1].as_str().unwrap()).unwrap();
        for word in previous.as_chunks_mut::<4>().0 {
            word.reverse();
        }
        let target = qbit_prism_server::codec::difficulty_target(self.difficulty).unwrap();
        for nonce in nonce_start..nonce_start + 100_000 {
            let header = [
                u32::from_str_radix(p[5].as_str().unwrap(), 16)
                    .unwrap()
                    .to_le_bytes()
                    .as_slice(),
                previous.as_slice(),
                merkle.as_slice(),
                u32::from_str_radix(p[7].as_str().unwrap(), 16)
                    .unwrap()
                    .to_le_bytes()
                    .as_slice(),
                u32::from_str_radix(p[6].as_str().unwrap(), 16)
                    .unwrap()
                    .to_le_bytes()
                    .as_slice(),
                nonce.to_le_bytes().as_slice(),
            ]
            .concat();
            if num_bigint::BigUint::from_bytes_le(&qbit_prism_server::codec::double_sha256(&header))
                <= target
            {
                return json!({"id":id,"method":"mining.submit","params":[username,p[0],"0000000000000000",p[7],format!("{nonce:08x}")]});
            }
        }
        panic!("constrained share proof not found");
    }
    fn solved_submit_version(
        &self,
        id: u64,
        username: &str,
        nonce_start: u32,
        version_bits: Option<u32>,
    ) -> Value {
        let p = self.notify["params"].as_array().unwrap();
        let coinbase = hex::decode(format!(
            "{}{}0000000000000000{}",
            p[2].as_str().unwrap(),
            self.extranonce1,
            p[3].as_str().unwrap()
        ))
        .unwrap();
        let merkle = qbit_prism_server::codec::double_sha256(&coinbase);
        let mut previous = hex::decode(p[1].as_str().unwrap()).unwrap();
        for word in previous.as_chunks_mut::<4>().0 {
            word.reverse();
        }
        for nonce in nonce_start..nonce_start + 1000 {
            let header = [
                (u32::from_str_radix(p[5].as_str().unwrap(), 16).unwrap()
                    | version_bits.unwrap_or(0))
                .to_le_bytes()
                .as_slice(),
                previous.as_slice(),
                merkle.as_slice(),
                u32::from_str_radix(p[7].as_str().unwrap(), 16)
                    .unwrap()
                    .to_le_bytes()
                    .as_slice(),
                0x207fffffu32.to_le_bytes().as_slice(),
                nonce.to_le_bytes().as_slice(),
            ]
            .concat();
            let hash = qbit_prism_server::codec::double_sha256(&header);
            if hash[31] < 127 {
                let mut request = json!({"id":id,"method":"mining.submit","params":[username,p[0],"0000000000000000",p[7],format!("{nonce:08x}")]});
                if let Some(bits) = version_bits {
                    request["params"]
                        .as_array_mut()
                        .unwrap()
                        .push(json!(format!("{bits:08x}")));
                }
                return request;
            }
        }
        panic!("regtest nonce not found")
    }
}

async fn start(
    config: StratumConfig,
) -> (
    std::net::SocketAddr,
    Arc<Backend>,
    watch::Sender<u64>,
    watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
) {
    start_with_metrics(
        config,
        Arc::new(qbit_prism_server::metrics::Metrics::default()),
    )
    .await
}

async fn start_with_metrics(
    config: StratumConfig,
    metrics: Arc<qbit_prism_server::metrics::Metrics>,
) -> (
    std::net::SocketAddr,
    Arc<Backend>,
    watch::Sender<u64>,
    watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let backend = Arc::new(Backend::default());
    let (refresh, refresh_rx) = watch::channel(0);
    let (shutdown, shutdown_rx) = watch::channel(false);
    let task = {
        let backend = backend.clone();
        tokio::spawn(async move {
            run_listener(listener, config, backend, refresh_rx, shutdown_rx, metrics)
                .await
                .unwrap();
        })
    };
    (address, backend, refresh, shutdown, task)
}

#[tokio::test]
async fn identity_free_health_probe_reports_readiness_without_creating_jobs() {
    let config = StratumConfig::default();
    let stats = config.stats.clone();
    let (address, backend, _refresh, shutdown, task) = start(config).await;
    let mut client = Client::connect(address).await;
    for (id, ready) in [(1, false), (2, true)] {
        backend.ready.store(ready, Ordering::Relaxed);
        client
            .send(json!({"id":id,"method":"mining.get_health","params":[]}))
            .await;
        let response = client.response(id).await;
        assert_eq!(response["result"], json!({"ready":ready}));
        assert!(response["error"].is_null());
    }
    client
        .send(json!({"id":3,"method":"mining.get_health","params":["miner.secret"]}))
        .await;
    assert!(!client.response(3).await["error"].is_null());
    assert_eq!(backend.jobs.load(Ordering::Relaxed), 0);
    assert_eq!(stats.snapshot(0).authorized, 0);
    assert_eq!(stats.snapshot(0).accepted_submissions, 0);
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fast_arrival_retries_failed_paired_delivery_and_only_proven_difficulty_resumes() {
    let mut config = StratumConfig {
        startup_difficulty: 1e-8,
        ..Default::default()
    };
    config.vardiff.minimum = 1e-8;
    config.vardiff.initial_min_seconds = 0.05;
    let (address, backend, _refresh, shutdown, task) = start(config).await;
    backend.network_bits.store(0x1d00ffff, Ordering::Relaxed);
    let mut client = Client::connect(address).await;
    client.login("miner.fast").await;
    let start_difficulty = client.difficulty;
    for index in 0..8u32 {
        if index == 7 {
            backend.fail_builds.store(1, Ordering::Relaxed);
        }
        client
            .send(client.solved_share(u64::from(index) + 10, "miner.fast", index * 100_000))
            .await;
        assert_eq!(client.response(u64::from(index) + 10).await["result"], true);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    client.next_job().await;
    assert!(
        client.difficulty >= start_difficulty * 4.0
            && client.difficulty <= start_difficulty * 64.000001
    );
    assert_eq!(backend.fail_builds.load(Ordering::Relaxed), 0);
    let retained = backend.hints.lock().unwrap()[&("default".into(), "miner.fast".into())].0;
    assert!(
        (retained - start_difficulty).abs() < 1e-16,
        "unproven early jump was durably retained"
    );
    let advertised = client.difficulty;
    client
        .send(client.solved_share(99, "miner.fast", 1_000_000))
        .await;
    assert_eq!(client.response(99).await["result"], true);
    timeout(Duration::from_secs(1), async {
        loop {
            if backend.hints.lock().unwrap()[&("default".into(), "miner.fast".into())].0
                == advertised
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut reconnect = Client::connect(address).await;
    reconnect.login("miner.fast").await;
    assert!(
        (reconnect.difficulty - advertised).abs() < 1e-15,
        "share-backed retarget did not survive reconnect"
    );
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn idle_retarget_lowers_reconnect_difficulty_without_renewing_evidence() {
    let mut config = StratumConfig {
        startup_difficulty: 0.004,
        resume_ttl_seconds: 60,
        ..Default::default()
    };
    config.vardiff.minimum = 0.001;
    config.vardiff.retarget_seconds = 0.05;
    let cache = config.retained_difficulties.clone();
    let (address, backend, _refresh, shutdown, task) = start(config).await;
    backend.network_bits.store(0x1d00ffff, Ordering::Relaxed);
    let key = ("default".into(), "miner.idle".into());
    let evidence = Instant::now() - Duration::from_secs(10);
    backend
        .hints
        .lock()
        .unwrap()
        .insert(key.clone(), (0.004, evidence));
    let mut client = Client::connect(address).await;
    client.login("miner.idle").await;
    assert_eq!(client.difficulty, 0.004);
    let cached_evidence = cache.lock().unwrap()[&key].1;
    client.next_job().await;
    assert_eq!(client.difficulty, 0.001);
    timeout(Duration::from_secs(1), async {
        loop {
            if cache.lock().unwrap()[&key].0 == 0.001 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(backend.hints.lock().unwrap()[&key].1, evidence);
    assert_eq!(cache.lock().unwrap()[&key].1, cached_evidence);
    backend
        .hint_reads_unavailable
        .store(true, Ordering::Relaxed);
    let mut reconnect = Client::connect(address).await;
    reconnect.login("miner.idle").await;
    assert_eq!(reconnect.difficulty, 0.001);
    cache.lock().unwrap().get_mut(&key).unwrap().1 = Instant::now() - Duration::from_secs(61);
    let mut expired = Client::connect(address).await;
    expired.login("miner.idle").await;
    assert_eq!(
        expired.difficulty, 0.004,
        "outage fallback revived expired evidence"
    );
    assert!(backend.shares.lock().unwrap().is_empty());
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn resumed_difficulty_is_lane_scoped_ttl_bounded_and_explicit_requests_win() {
    let mut config = StratumConfig {
        startup_difficulty: 0.001,
        resume_max_start_factor: 4.0,
        resume_ttl_seconds: 1,
        ..Default::default()
    };
    config.vardiff.minimum = 0.001;
    config.vardiff.maximum = 1.0;
    let (address, backend, refresh, shutdown, task) = start(config.clone()).await;
    backend.network_bits.store(0x1d00ffff, Ordering::Relaxed);
    backend.hints.lock().unwrap().insert(
        ("default".into(), "miner.retained".into()),
        (0.1, Instant::now()),
    );
    let mut first = Client::connect(address).await;
    first.login("miner.retained").await;
    assert!(
        (first.difficulty - 0.004).abs() < 1e-12,
        "resume did not obey startup-factor cap"
    );
    first
        .send(json!({"id":4,"method":"mining.authorize","params":["miner.retained","d=0.02"]}))
        .await;
    assert_eq!(first.response(4).await["result"], true);
    first.next_job().await;
    assert!((first.difficulty - 0.02).abs() < 1e-12);
    first
        .send(json!({"id":5,"method":"mining.authorize","params":["miner.retained","md=0.03"]}))
        .await;
    assert_eq!(first.response(5).await["result"], true);
    first.next_job().await;
    assert!(
        (first.difficulty - 0.03).abs() < 1e-12,
        "md should clamp the live converged value"
    );
    let evidence_before =
        backend.hints.lock().unwrap()[&("default".into(), "miner.retained".into())].1;
    drop(first);
    assert_eq!(
        backend.hints.lock().unwrap()[&("default".into(), "miner.retained".into())].1,
        evidence_before,
        "unproven password request renewed durable evidence"
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let high_address = listener.local_addr().unwrap();
    config.listener_name = "highdiff".into();
    let high_task = tokio::spawn(run_listener(
        listener,
        config,
        backend.clone(),
        refresh.subscribe(),
        shutdown.subscribe(),
        std::sync::Arc::new(qbit_prism_server::metrics::Metrics::default()),
    ));
    let mut other_lane = Client::connect(high_address).await;
    other_lane.login("miner.retained").await;
    assert!(
        (other_lane.difficulty - 0.001).abs() < 1e-12,
        "other listener inherited the default lane hint"
    );
    backend.hints.lock().unwrap().insert(
        ("default".into(), "miner.retained".into()),
        (0.1, Instant::now() - Duration::from_secs(2)),
    );
    let mut expired = Client::connect(address).await;
    expired.login("miner.retained").await;
    assert!(
        (expired.difficulty - 0.001).abs() < 1e-12,
        "expired evidence resumed"
    );
    shutdown.send(true).unwrap();
    task.await.unwrap();
    high_task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multiple_miners_unique_extranonces_durable_accept_and_duplicate_rejection() {
    let config = StratumConfig::default();
    let stats = config.stats.clone();
    let (address, backend, _refresh, shutdown, task) = start(config).await;
    let mut miners = Vec::new();
    for worker in ["miner.rig1", "miner.rig2", "miner2.rig1"] {
        let mut client = Client::connect(address).await;
        client.login(worker).await;
        let submit = client.solved_submit(10, worker, 0);
        client.send(submit.clone()).await;
        assert_eq!(client.response(10).await["result"], true);
        client.send(submit).await;
        let duplicate = client.response(10).await;
        assert_eq!(duplicate["error"][0], 22);
        assert_eq!(duplicate["error"][2]["reason_id"], "duplicate-share");
        miners.push(client);
    }
    assert_eq!(
        miners
            .iter()
            .map(|m| &m.extranonce1)
            .collect::<HashSet<_>>()
            .len(),
        3
    );
    assert_eq!(backend.shares.lock().unwrap().len(), 3);
    miners[0]
        .send(json!({"id":20,"method":"mining.authorize","params":["invalid-address","x"]}))
        .await;
    assert_eq!(miners[0].response(20).await["error"][0], 20);
    assert_eq!(
        stats.snapshot(0).rejected_submissions,
        3,
        "authorization errors must not count as rejected shares"
    );
    miners[0]
        .send(json!({"id":21,"method":"mining.submit","params":[]}))
        .await;
    assert_eq!(miners[0].response(21).await["error"][0], 20);
    miners[0].send(json!({"id":22,"method":"mining.submit","params":["miner.rig1","missing-job","0000000000000000","6553f100","00000000"]})).await;
    assert_eq!(miners[0].response(22).await["error"][0], 21);
    let mut malformed = miners[0].solved_submit(23, "miner.rig1", 0);
    malformed["params"][4] = json!("not-hex!");
    miners[0].send(malformed).await;
    assert_eq!(miners[0].response(23).await["error"][0], 20);
    let low_nonce = {
        let stored = backend.stored.lock().unwrap();
        let job = &stored[miners[0].notify["params"][0].as_str().unwrap()]
            .0
            .wire;
        (0..1000)
            .find(|nonce| {
                let proof = job
                    .assemble_submission(
                        "0000000000000000",
                        "6553f100",
                        &format!("{nonce:08x}"),
                        None,
                        0,
                    )
                    .unwrap();
                !proof.share_pass && !proof.block_pass
            })
            .unwrap()
    };
    let mut low = miners[0].solved_submit(24, "miner.rig1", 0);
    low["params"][4] = json!(format!("{low_nonce:08x}"));
    miners[0].send(low).await;
    assert_eq!(miners[0].response(24).await["error"][0], 23);
    assert_eq!(stats.snapshot(0).accepted_submissions, 3);
    assert_eq!(stats.snapshot(0).rejected_submissions, 7);
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn negotiation_reauthorization_retains_original_job_worker_and_grace_expires() {
    let config = StratumConfig {
        stale_grace_seconds: 0.05,
        ..Default::default()
    };
    let (address, backend, refresh, shutdown, task) = start(config).await;
    let mut client = Client::connect(address).await;
    client.send(json!({"id":0,"method":"mining.configure","params":[["version-rolling","unknown"],{"version-rolling.mask":"0000e000"}]})).await;
    let configured = client.response(0).await;
    assert_eq!(configured["result"]["version-rolling.mask"], "0000e000");
    assert_eq!(configured["result"]["unknown"], false);
    client.login("miner.old").await;
    let mut old_submit = client.solved_submit(4, "miner.new", 0);
    client
        .send(json!({"id":3,"method":"mining.authorize","params":["miner.new","d=2,md=1"]}))
        .await;
    assert_eq!(client.response(3).await["result"], true);
    client.next_job().await;
    assert_eq!(client.notify["params"][8], false);
    client.send(old_submit.clone()).await;
    assert_eq!(client.response(4).await["result"], true);
    assert_eq!(backend.credited_workers.lock().unwrap()[0], "miner.old");
    backend.generation.store(1, Ordering::SeqCst);
    refresh.send(1).unwrap();
    client.next_job().await;
    assert_eq!(client.notify["params"][8], true);
    tokio::time::sleep(Duration::from_millis(100)).await;
    old_submit["id"] = json!(5);
    client.send(old_submit).await;
    assert_eq!(client.response(5).await["error"][0], 21);
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn same_parent_payout_replacement_clears_retained_work_but_equivalent_updates_do_not() {
    let (address, backend, refresh, shutdown, task) = start(StratumConfig::default()).await;
    let mut client = Client::connect(address).await;
    client.login("miner.payout").await;
    let old = client.solved_submit(10, "miner.payout", 0);
    let parent = client.notify["params"][1].clone();
    refresh.send(1).unwrap();
    client.next_job().await;
    assert_eq!(client.notify["params"][8], false);
    backend.payout_revision.store(1, Ordering::Relaxed);
    refresh.send(2).unwrap();
    client.next_job().await;
    assert_eq!(client.notify["params"][1], parent);
    assert_eq!(client.notify["params"][8], true);
    client.send(old).await;
    assert_eq!(client.response(10).await["error"][0], 21);
    assert!(backend.shares.lock().unwrap().is_empty());
    client
        .send(client.solved_submit(11, "miner.payout", 0))
        .await;
    assert_eq!(client.response(11).await["result"], true);
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

// This runs against an in-memory backend, so it proves only that the frame is
// forwarded: the credited amount and its timing are covered by
// `coordinator::d2_below_target_tests` (decision D2b).
#[tokio::test]
async fn highdiff_floor_still_forwards_valid_block_below_share_target() {
    let mut config = StratumConfig {
        minimum_difficulty: 500_000.0,
        startup_difficulty: 500_000.0,
        ..Default::default()
    };
    config.vardiff.minimum = 500_000.0;
    config.vardiff.maximum = 1_000_000.0;
    let (address, backend, _refresh, shutdown, task) = start(config).await;
    let advertised =
        probe_first_difficulty(&address.to_string(), "miner.probe", Duration::from_secs(2))
            .await
            .unwrap();
    assert!(advertised >= 500_000.0);
    let mut client = Client::connect(address).await;
    client.login("miner.large").await;
    client.send(client.solved_submit(8, "miner.large", 0)).await;
    assert_eq!(client.response(8).await["result"], true);
    assert_eq!(backend.shares.lock().unwrap().len(), 1);
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn oversized_frames_close_connection_and_fragmented_json_survives_timer_ticks() {
    let (address, _backend, _refresh, shutdown, task) = start(StratumConfig {
        max_message_bytes: 256,
        ..Default::default()
    })
    .await;
    let mut client = Client::connect(address).await;
    client
        .writer
        .write_all(b"{\"id\":42,\"method\":")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    client
        .writer
        .write_all(b"\"mining.subscribe\",\"params\":[]}\n")
        .await
        .unwrap();
    assert!(client.response(42).await["result"].is_array());
    client.writer.write_all(&vec![b'x'; 257]).await.unwrap();
    assert_eq!(
        client.read().await["error"][2]["reason_id"],
        "malformed-submit"
    );
    let mut trailing = String::new();
    assert_eq!(
        timeout(
            Duration::from_secs(2),
            client.reader.read_line(&mut trailing)
        )
        .await
        .unwrap()
        .unwrap(),
        0
    );
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconnect_on_another_listener_preserves_original_entropy_mask_and_expiry() {
    let config = StratumConfig {
        job_retention_seconds: 0.25,
        stale_grace_seconds: 0.0,
        ..Default::default()
    };
    let (first, backend, refresh, shutdown, first_task) = start(config.clone()).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second = listener.local_addr().unwrap();
    let second_task = {
        let backend = backend.clone();
        let refresh = refresh.subscribe();
        let shutdown = shutdown.subscribe();
        tokio::spawn(async move {
            run_listener(
                listener,
                config,
                backend,
                refresh,
                shutdown,
                std::sync::Arc::new(qbit_prism_server::metrics::Metrics::default()),
            )
            .await
            .unwrap();
        })
    };
    let mut original = Client::connect(first).await;
    original.send(json!({"id":0,"method":"mining.configure","params":[["version-rolling"],{"version-rolling.mask":"0000e000"}]})).await;
    assert_eq!(
        original.response(0).await["result"]["version-rolling"],
        true
    );
    original.login("miner.resume").await;
    let submit = original.solved_submit_version(42, "miner.resume", 0, Some(0x2000));
    let old_extra = original.extranonce1.clone();
    drop(original);
    let mut resumed = Client::connect(second).await;
    resumed.login("miner.resume").await;
    assert_ne!(resumed.extranonce1, old_extra);
    // No configure on the replacement: its negotiated mask is zero, while
    // the old work was rolled under 0000e000 on another physical frontend.
    resumed.send(submit.clone()).await;
    assert_eq!(resumed.response(42).await["result"], true);
    let mut thief = Client::connect(second).await;
    thief.login("miner.thief").await;
    let mut stolen = submit.clone();
    stolen["params"][0] = json!("miner.thief");
    thief.send(stolen).await;
    assert_eq!(thief.response(42).await["error"][0], 21);
    tokio::time::sleep(Duration::from_millis(300)).await;
    resumed.send(submit).await;
    assert_eq!(resumed.response(42).await["error"][0], 21);
    shutdown.send(true).unwrap();
    first_task.await.unwrap();
    second_task.await.unwrap();
}

#[tokio::test]
async fn per_username_capacity_preserves_prior_authorization_on_failed_reauthorize() {
    let (address, backend, _refresh, shutdown, task) = start(StratumConfig {
        max_connections_per_username: 1,
        ..Default::default()
    })
    .await;
    let mut first = Client::connect(address).await;
    first.login("miner.one").await;
    let mut second = Client::connect(address).await;
    second.login("miner.two").await;
    second
        .send(json!({"id":9,"method":"mining.authorize","params":["miner.one","d=100"]}))
        .await;
    assert_eq!(
        second.response(9).await["error"][1],
        "too many connections for username"
    );
    second.send(second.solved_submit(10, "miner.two", 0)).await;
    assert_eq!(second.response(10).await["result"], true);
    assert_eq!(backend.credited_workers.lock().unwrap()[0], "miner.two");
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

async fn authorize_eventually(client: &mut Client, username: &str) {
    timeout(Duration::from_secs(5), async {
        loop {
            client
                .send(json!({"id":90,"method":"mining.authorize","params":[username,"x"]}))
                .await;
            let response = client.response(90).await;
            if response["result"] == true {
                break;
            }
            assert_eq!(response["error"][1], "too many connections for username");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("username capacity was not reclaimed");
}

async fn assert_username_full(client: &mut Client, username: &str) {
    client
        .send(json!({"id":91,"method":"mining.authorize","params":[username,"x"]}))
        .await;
    assert_eq!(
        client.response(91).await["error"][1],
        "too many connections for username"
    );
}

#[tokio::test]
async fn reauthorization_retains_original_username_capacity_until_timer_expiry() {
    let mut config = StratumConfig {
        max_connections_per_username: 1,
        max_jobs_per_connection: 1,
        job_retention_seconds: 0.5,
        stale_grace_seconds: 0.0,
        ..Default::default()
    };
    config.vardiff.enabled = false;
    let (address, backend, _refresh, shutdown, task) = start(config).await;
    let mut first = Client::connect(address).await;
    first.login("miner.A").await;
    let mut old_submit = first.solved_submit(10, "miner.B", 0);
    first
        .send(json!({"id":3,"method":"mining.authorize","params":["miner.B","x"]}))
        .await;
    assert_eq!(first.response(3).await["result"], true);
    first.next_job().await;
    let mut second = Client::connect(address).await;
    assert_username_full(&mut second, "miner.A").await;
    first.send(old_submit.clone()).await;
    assert_eq!(first.response(10).await["result"], true);
    assert_eq!(backend.credited_workers.lock().unwrap()[0], "miner.A");

    // Switching back must reuse the session's retained A slot, including when
    // one is the entire limit. Both names still have exactly one occupied slot.
    for (id, username) in [(4, "miner.A"), (5, "miner.B")] {
        first
            .send(json!({"id":id,"method":"mining.authorize","params":[username,"x"]}))
            .await;
        assert_eq!(first.response(id).await["result"], true);
        first.next_job().await;
    }
    assert_username_full(&mut second, "miner.A").await;
    assert_username_full(&mut second, "miner.B").await;

    // The first connection receives no requests or new work while its timer
    // releases expired A jobs. A second connection can then enter as A.
    authorize_eventually(&mut second, "miner.A").await;
    old_submit["id"] = json!(11);
    first.send(old_submit).await;
    assert_eq!(first.response(11).await["error"][0], 21);
    first.send(first.solved_submit(12, "miner.B", 100)).await;
    assert_eq!(first.response(12).await["result"], true);
    assert_eq!(backend.credited_workers.lock().unwrap()[1], "miner.B");
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn reauthorization_disconnect_reclaims_current_and_retained_username_capacity() {
    let (address, _backend, _refresh, shutdown, task) = start(StratumConfig {
        max_connections_per_username: 1,
        max_jobs_per_connection: 1,
        ..Default::default()
    })
    .await;
    let mut first = Client::connect(address).await;
    first.login("miner.A").await;
    first
        .send(json!({"id":3,"method":"mining.authorize","params":["miner.B","x"]}))
        .await;
    assert_eq!(first.response(3).await["result"], true);
    first.next_job().await;
    let mut second = Client::connect(address).await;
    let mut third = Client::connect(address).await;
    assert_username_full(&mut second, "miner.A").await;
    assert_username_full(&mut third, "miner.B").await;
    drop(first);
    authorize_eventually(&mut second, "miner.A").await;
    authorize_eventually(&mut third, "miner.B").await;
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn retained_username_capacity_reclaims_evicted_and_replaced_jobs() {
    for max_jobs in [1, 64] {
        let (address, backend, refresh, shutdown, task) = start(StratumConfig {
            max_connections_per_username: 1,
            max_jobs_per_connection: max_jobs,
            ..Default::default()
        })
        .await;
        let mut first = Client::connect(address).await;
        first.login("miner.A").await;
        let old_submit = first.solved_submit(10, "miner.B", 0);
        first
            .send(json!({"id":3,"method":"mining.authorize","params":["miner.B","x"]}))
            .await;
        assert_eq!(first.response(3).await["result"], true);
        first.next_job().await;
        let mut second = Client::connect(address).await;
        // Active eviction keeps original work creditable in this connection's
        // graveyard, so it also keeps the original username reservation.
        assert_username_full(&mut second, "miner.A").await;
        backend.payout_revision.store(1, Ordering::Relaxed);
        refresh.send(1).unwrap();
        first.next_job().await;
        assert_eq!(first.notify["params"][8], true);
        authorize_eventually(&mut second, "miner.A").await;
        first.send(old_submit).await;
        assert_eq!(first.response(10).await["error"][0], 21);
        assert!(backend.credited_workers.lock().unwrap().is_empty());
        shutdown.send(true).unwrap();
        task.await.unwrap();
    }
}

#[tokio::test]
async fn resumed_expiry_does_not_release_other_retained_work_during_build_failure() {
    let mut config = StratumConfig {
        max_connections_per_username: 1,
        max_jobs_per_connection: 1,
        job_retention_seconds: 30.0,
        ..Default::default()
    };
    config.vardiff.enabled = false;
    let (address, backend, refresh, shutdown, task) = start(config).await;
    let mut original = Client::connect(address).await;
    original.login("miner.A").await;
    let submit = original.solved_submit(10, "miner.A", 0);
    drop(original);
    let mut resumed = Client::connect(address).await;
    authorize_eventually(&mut resumed, "miner.A").await;
    resumed
        .send(json!({"id":1,"method":"mining.subscribe","params":[]}))
        .await;
    resumed.extranonce1 = resumed.response(1).await["result"][1]
        .as_str()
        .unwrap()
        .into();
    resumed.next_job().await;
    backend
        .stored
        .lock()
        .unwrap()
        .get_mut(submit["params"][1].as_str().unwrap())
        .unwrap()
        .3 = Instant::now() + Duration::from_millis(500);
    // With one retained job, restoring the original evicts the new A job.
    resumed.send(submit).await;
    assert_eq!(resumed.response(10).await["result"], true);
    backend.fail_builds.store(100, Ordering::Relaxed);
    resumed
        .send(json!({"id":3,"method":"mining.authorize","params":["miner.B","x"]}))
        .await;
    assert_eq!(resumed.response(3).await["result"], true);
    let mut second = Client::connect(address).await;
    assert_username_full(&mut second, "miner.A").await;
    assert_username_full(&mut second, "miner.B").await;
    // The resumed job expires, but the newly issued A work it evicted still
    // belongs to this connection's graveyard and can credit A.
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_username_full(&mut second, "miner.A").await;
    backend.fail_builds.store(0, Ordering::Relaxed);
    backend.payout_revision.store(1, Ordering::Relaxed);
    refresh.send(1).unwrap();
    resumed.next_job().await;
    authorize_eventually(&mut second, "miner.A").await;
    assert_username_full(&mut second, "miner.B").await;
    assert_eq!(
        backend.credited_workers.lock().unwrap().as_slice(),
        ["miner.A"]
    );
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_cpu_miner_obeys_rate_budget_and_solves_real_headers() {
    let (address, backend, _refresh, shutdown, task) = start(StratumConfig::default()).await;
    let output = timeout(
        Duration::from_secs(10),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_qbit-prism-miner"))
            .args([
                "--address",
                &address.to_string(),
                "--username",
                "miner.native",
                "--threads",
                "2",
                "--hashes-per-second",
                "20",
                "--duration-seconds",
                "5",
                "--max-shares",
                "2",
                "--pause-after-share-ms",
                "10",
            ])
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8(output.stdout).unwrap();
    let summary: Value = serde_json::from_str(text.lines().last().unwrap()).unwrap();
    assert_eq!(summary["accepted"], 2);
    assert_eq!(summary["threads"], 2);
    let hashes = summary["hashes"].as_u64().unwrap() as f64;
    let elapsed = summary["elapsed_seconds"].as_f64().unwrap();
    assert!(
        hashes <= elapsed * 20.0 + 4.0,
        "miner exceeded aggregate budget: {summary}"
    );
    assert_eq!(backend.shares.lock().unwrap().len(), 2);
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delivery_stats_follow_admission_progress_generation_and_disconnect() {
    let config = StratumConfig::default();
    let stats = config.stats.clone();
    let admission = config.initial_job_limit.clone();
    let held = admission.acquire_many_owned(128).await.unwrap();
    let (address, backend, refresh, shutdown, task) = start(config).await;
    let mut client = Client::connect(address).await;
    client
        .send(json!({"id":1,"method":"mining.subscribe","params":[]}))
        .await;
    client.extranonce1 = client.response(1).await["result"][1]
        .as_str()
        .unwrap()
        .into();
    client
        .send(json!({"id":2,"method":"mining.authorize","params":["miner.stats","x"]}))
        .await;
    assert_eq!(client.response(2).await["result"], true);
    timeout(Duration::from_secs(2), async {
        while stats.snapshot(0).pending_builds == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let waiting = stats.snapshot(0);
    assert_eq!(waiting.connections, 1);
    assert_eq!(waiting.authorized, 1);
    assert_eq!(waiting.pending_builds, 1);
    assert_eq!(waiting.authorized_with_current_work, 0);
    drop(held);
    client.next_job().await;
    timeout(Duration::from_secs(2), async {
        while stats.snapshot(0).job_delivery_successes == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let delivered = stats.snapshot(0);
    assert_eq!(delivered.pending_builds, 0);
    assert_eq!(delivered.authorized_with_current_work, 1);
    assert!(delivered.last_delivery_progress_age_seconds.is_some());
    backend.generation.store(1, Ordering::SeqCst);
    refresh.send(1).unwrap();
    client.next_job().await;
    timeout(Duration::from_secs(2), async {
        while stats.snapshot(1).authorized_with_current_work == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(stats.snapshot(0).authorized_with_current_work, 0);
    drop(client);
    timeout(Duration::from_secs(2), async {
        while stats.snapshot(1).connections != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let drained = stats.snapshot(1);
    assert_eq!(drained.authorized, 0);
    assert_eq!(drained.pending_builds, 0);
    assert_eq!(drained.authorized_with_current_work, 0);
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn admission_timeout_records_failure_and_shutdown_releases_session_stats() {
    let config = StratumConfig {
        initial_job_timeout_seconds: 0.1,
        ..Default::default()
    };
    let stats = config.stats.clone();
    let held = config
        .initial_job_limit
        .clone()
        .acquire_many_owned(128)
        .await
        .unwrap();
    let (address, _backend, _refresh, shutdown, task) = start(config).await;
    let mut client = Client::connect(address).await;
    client
        .send(json!({"id":1,"method":"mining.subscribe","params":[]}))
        .await;
    client.response(1).await;
    client
        .send(json!({"id":2,"method":"mining.authorize","params":["miner.timeout","x"]}))
        .await;
    assert_eq!(client.response(2).await["result"], true);
    timeout(Duration::from_secs(2), async {
        while stats.snapshot(0).job_delivery_failures == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(stats.snapshot(0).pending_builds, 0);
    shutdown.send(true).unwrap();
    task.await.unwrap();
    drop(held);
    let snapshot = stats.snapshot(0);
    assert_eq!(snapshot.connections, 0);
    assert_eq!(snapshot.authorized, 0);
    assert_eq!(snapshot.pending_builds, 0);
}

// --- P2 admission: per-source cap and per-session budgets (#276) -------------

fn refusal_total(metrics: &qbit_prism_server::metrics::Metrics, reason: &str) -> f64 {
    let key = format!("qbit_prism_stratum_connection_refusals_total{{reason=\"{reason}\"}} ");
    metrics
        .render()
        .lines()
        .find_map(|line| line.strip_prefix(&key))
        .map(|value| value.parse().unwrap())
        .unwrap_or(f64::NAN)
}

fn budget_refusals(metrics: &qbit_prism_server::metrics::Metrics) -> [f64; 3] {
    [
        refusal_total(metrics, "malformed_frame_budget"),
        refusal_total(metrics, "unknown_job_budget"),
        refusal_total(metrics, "authorize_budget"),
    ]
}

async fn wait_for_connections(stats: &Arc<StratumStats>, count: usize) {
    timeout(Duration::from_secs(5), async {
        while stats.snapshot(0).connections != count {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the session count never reached the expected value");
}

/// Refusal at accept closes the socket without writing a byte, exactly as the
/// existing global limit does.
async fn assert_socket_refused(address: std::net::SocketAddr) {
    use tokio::io::AsyncReadExt;
    let mut refused = TcpStream::connect(address).await.unwrap();
    let mut bytes = Vec::new();
    match timeout(Duration::from_secs(5), refused.read_to_end(&mut bytes))
        .await
        .expect("the refused socket stayed open")
    {
        Ok(_) => assert!(bytes.is_empty(), "refused socket received {bytes:?}"),
        Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset),
    }
}

#[tokio::test]
async fn per_source_cap_refuses_at_accept_without_backend_work_and_frees_the_slot_on_close() {
    let metrics = Arc::new(qbit_prism_server::metrics::Metrics::default());
    let config = StratumConfig {
        max_connections_per_ip: 1,
        ..Default::default()
    };
    let stats = config.stats.clone();
    let (address, backend, _refresh, shutdown, task) =
        start_with_metrics(config, metrics.clone()).await;
    let mut held = Client::connect(address).await;
    held.login("miner.held").await;
    assert_eq!(refusal_total(&metrics, "ip_limit"), 0.);
    let allocated = backend.sessions.load(Ordering::SeqCst);

    assert_socket_refused(address).await;
    assert_eq!(refusal_total(&metrics, "ip_limit"), 1.);
    // The decision is taken at accept: no session identity was allocated and
    // no other refusal reason was recorded for it.
    assert_eq!(backend.sessions.load(Ordering::SeqCst), allocated);
    assert_eq!(backend.authorize_calls.load(Ordering::SeqCst), 1);
    assert_eq!(refusal_total(&metrics, "global_limit"), 0.);
    assert_eq!(stats.snapshot(0).connections, 1);

    // The permit is released by the session task, so the slot returns.
    drop(held);
    wait_for_connections(&stats, 0).await;
    let mut replacement = Client::connect(address).await;
    replacement.login("miner.replacement").await;
    assert_eq!(refusal_total(&metrics, "ip_limit"), 1.);
    drop(replacement);
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn per_source_cap_is_one_budget_across_the_default_and_highdiff_listeners() {
    let metrics = Arc::new(qbit_prism_server::metrics::Metrics::default());
    let config = StratumConfig {
        max_connections_per_ip: 2,
        ..Default::default()
    };
    // `highdiff_config` derives its listener by cloning the default config;
    // derive it the same way so both listeners hold the one registry Arc.
    let mut highdiff = config.clone();
    highdiff.listener_name = "highdiff".into();
    let stats = config.stats.clone();
    let (address, backend, refresh, shutdown, task) =
        start_with_metrics(config, metrics.clone()).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let high_address = listener.local_addr().unwrap();
    let high_task = tokio::spawn(run_listener(
        listener,
        highdiff,
        backend.clone(),
        refresh.subscribe(),
        shutdown.subscribe(),
        metrics.clone(),
    ));
    let mut ordinary = Client::connect(address).await;
    ordinary.login("miner.ordinary").await;
    let mut high = Client::connect(high_address).await;
    high.login("miner.high").await;
    // Two slots are spent, one on each port: both ports now refuse.
    assert_socket_refused(high_address).await;
    assert_socket_refused(address).await;
    assert_eq!(refusal_total(&metrics, "ip_limit"), 2.);

    // Closing the highdiff connection frees a slot the default port can use.
    drop(high);
    wait_for_connections(&stats, 1).await;
    let mut replacement = Client::connect(address).await;
    replacement.login("miner.replacement").await;
    assert_eq!(refusal_total(&metrics, "ip_limit"), 2.);
    drop((ordinary, replacement));
    shutdown.send(true).unwrap();
    task.await.unwrap();
    high_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn per_source_cap_of_zero_admits_every_connection_from_one_address() {
    let metrics = Arc::new(qbit_prism_server::metrics::Metrics::default());
    let config = StratumConfig::default();
    assert_eq!(config.max_connections_per_ip, 0, "the default must be off");
    let stats = config.stats.clone();
    let (address, _backend, _refresh, shutdown, task) =
        start_with_metrics(config, metrics.clone()).await;
    let mut clients = Vec::new();
    for index in 0..8 {
        let mut client = Client::connect(address).await;
        client.login(&format!("miner.rig{index}")).await;
        clients.push(client);
    }
    wait_for_connections(&stats, 8).await;
    assert_eq!(refusal_total(&metrics, "ip_limit"), 0.);
    drop(clients);
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn malformed_frame_budget_counts_frames_not_bytes_and_then_disconnects() {
    let metrics = Arc::new(qbit_prism_server::metrics::Metrics::default());
    let config = StratumConfig {
        max_malformed_frames_per_interval: 4,
        session_budget_interval_seconds: 600.0,
        ..Default::default()
    };
    let (address, _backend, _refresh, shutdown, task) =
        start_with_metrics(config, metrics.clone()).await;
    let mut client = Client::connect(address).await;
    client.login("miner.junk").await;
    // Valid traffic is never charged, so the whole budget is still available.
    // Each junk frame is far longer than the budget: a byte count would have
    // disconnected on the first one.
    for (index, frame) in ["not json at all", "[1,2,3]", "{\"id\":1}", "null"]
        .into_iter()
        .enumerate()
    {
        client.writer.write_all(frame.as_bytes()).await.unwrap();
        client.writer.write_all(b"\n").await.unwrap();
        let error = client.read().await;
        assert_eq!(
            error["error"][2]["reason_id"], "malformed-submit",
            "{index}"
        );
    }
    let health = client.request_health(90).await;
    assert_eq!(health["result"]["ready"], false, "budget closed early");
    assert_eq!(budget_refusals(&metrics), [0., 0., 0.]);

    client
        .send(json!({"id":91,"method":"mining.nonsense","params":[]}))
        .await;
    let closing = client.expect_closed().await;
    assert_eq!(
        closing.last().unwrap()["error"][2]["reason_id"],
        "malformed-submit",
        "the spent budget must answer before closing"
    );
    assert_eq!(budget_refusals(&metrics), [1., 0., 0.]);
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn every_unknown_job_miss_is_requeried_and_charged_until_the_budget_disconnects() {
    let metrics = Arc::new(qbit_prism_server::metrics::Metrics::default());
    let config = StratumConfig {
        max_unknown_jobs_per_interval: 8,
        session_budget_interval_seconds: 600.0,
        ..Default::default()
    };
    let (address, backend, _refresh, shutdown, task) =
        start_with_metrics(config, metrics.clone()).await;
    let mut client = Client::connect(address).await;
    client.login("miner.stale").await;
    // No negative cache: a repeated unknown ID is looked up again every time,
    // and every lookup that misses is charged.
    for id in 0..8 {
        let rejected = client
            .request_submit(1000 + id, "miner.stale", "repeated-unknown")
            .await;
        assert_eq!(rejected["error"][2]["reason_id"], "unknown-job");
    }
    assert_eq!(backend.resume_calls.load(Ordering::SeqCst), 8);
    assert_eq!(budget_refusals(&metrics), [0., 0., 0.]);

    for id in 0..200 {
        let request = client.unknown_job_submit(3000 + id, "miner.stale", "repeated-unknown");
        client.send(request).await;
    }
    client.expect_closed().await;
    let lookups = backend.resume_calls.load(Ordering::SeqCst);
    assert_eq!(
        lookups, 9,
        "200 unknown submits beyond a budget of 8 must cost exactly one more lookup"
    );
    assert_eq!(budget_refusals(&metrics), [0., 1., 0.]);
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn a_transient_resume_miss_recovers_on_the_next_submit_of_the_same_job() {
    let metrics = Arc::new(qbit_prism_server::metrics::Metrics::default());
    let (address, backend, _refresh, shutdown, task) =
        start_with_metrics(StratumConfig::default(), metrics.clone()).await;
    // The job is issued to one connection and submitted on another, so the
    // second session must resume it from the backend.
    let mut issuer = Client::connect(address).await;
    issuer.login("miner.race").await;
    let job_id = issuer.notify["params"][0].as_str().unwrap().to_string();
    let mut resumer = Client::connect(address).await;
    resumer.login("miner.race").await;
    assert_ne!(resumer.notify["params"][0], issuer.notify["params"][0]);

    // A frontend momentarily behind on the payout revision misses the resume.
    backend.payout_revision.fetch_add(1, Ordering::SeqCst);
    let missed = resumer.request_submit(1, "miner.race", &job_id).await;
    assert_eq!(missed["error"][2]["reason_id"], "unknown-job");
    backend.payout_revision.fetch_sub(1, Ordering::SeqCst);
    // The very next submit re-queries and finds the job: nothing remembered
    // the transient miss.
    let recovered = resumer.request_submit(2, "miner.race", &job_id).await;
    assert_ne!(
        recovered["error"][2]["reason_id"], "unknown-job",
        "a transient miss must not stick: {recovered}"
    );
    assert_eq!(backend.resume_calls.load(Ordering::SeqCst), 2);
    assert_eq!(budget_refusals(&metrics), [0., 0., 0.]);
    drop((issuer, resumer));
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn authorize_budget_bounds_username_cycling_and_keeps_reauthorization_working() {
    let metrics = Arc::new(qbit_prism_server::metrics::Metrics::default());
    let config = StratumConfig {
        max_authorize_attempts_per_interval: 8,
        session_budget_interval_seconds: 600.0,
        ..Default::default()
    };
    let (address, backend, _refresh, shutdown, task) =
        start_with_metrics(config, metrics.clone()).await;
    let mut client = Client::connect(address).await;
    client.login("miner.first").await;
    // Re-authorizing to switch usernames stays supported inside the budget.
    for id in 0..7 {
        client
            .send(json!({"id":500+id,"method":"mining.authorize","params":[format!("miner.rig{id}"),"x"]}))
            .await;
        assert_eq!(client.response(500 + id).await["result"], true);
    }
    assert_eq!(backend.authorize_calls.load(Ordering::SeqCst), 8);
    assert_eq!(budget_refusals(&metrics), [0., 0., 0.]);

    for id in 0..200 {
        client
            .send(json!({"id":600+id,"method":"mining.authorize","params":[format!("miner.cycle{id}"),"x"]}))
            .await;
    }
    client.expect_closed().await;
    let calls = backend.authorize_calls.load(Ordering::SeqCst);
    assert!(
        calls <= 8,
        "{calls} address validations for 200 authorize attempts beyond a budget of 8"
    );
    assert_eq!(budget_refusals(&metrics), [0., 0., 1.]);
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn recommended_budgets_admit_a_thousand_valid_submits_and_an_honest_stale_burst() {
    let metrics = Arc::new(qbit_prism_server::metrics::Metrics::default());
    // The values docs/prism-b4-stratum-admission.md recommends as a start.
    let config = StratumConfig {
        session_budget_interval_seconds: 60.0,
        max_malformed_frames_per_interval: 64,
        max_unknown_jobs_per_interval: 256,
        max_authorize_attempts_per_interval: 32,
        max_connections_per_ip: 0,
        ..Default::default()
    };
    // Hold the share difficulty still so the control measures budgets only.
    let mut config = config;
    config.vardiff.enabled = false;
    let stats = config.stats.clone();
    let (address, backend, refresh, shutdown, task) =
        start_with_metrics(config, metrics.clone()).await;
    let mut client = Client::connect(address).await;
    client.login("miner.honest").await;
    let started = Instant::now();
    for index in 0..1000u32 {
        let request =
            client.solved_submit_version(10_000 + index as u64, "miner.honest", index * 4096, None);
        client.send(request).await;
        let accepted = client.response(10_000 + index as u64).await;
        assert!(
            accepted["error"].is_null(),
            "valid submit {index} was refused: {accepted}"
        );
    }
    assert!(
        started.elapsed() < Duration::from_secs(60),
        "the control must stay inside one budget window"
    );
    assert_eq!(budget_refusals(&metrics), [0., 0., 0.]);
    assert_eq!(stats.snapshot(0).connections, 1);

    // An honest staleness burst: a payout revision change retires the work
    // this session still holds, so its in-flight submits arrive as unknown.
    let stale: Vec<Value> = (0..64)
        .map(|index| {
            client.solved_submit_version(
                20_000 + index,
                "miner.honest",
                900_000 + index as u32 * 4096,
                None,
            )
        })
        .collect();
    backend.payout_revision.fetch_add(1, Ordering::SeqCst);
    backend.generation.fetch_add(1, Ordering::SeqCst);
    refresh.send(1).unwrap();
    for (index, request) in stale.into_iter().enumerate() {
        let id = request["id"].as_u64().unwrap();
        client.send(request).await;
        let response = client.response(id).await;
        assert!(
            response["error"].is_null()
                || response["error"][2]["reason_id"] == "unknown-job"
                || response["error"][2]["reason_id"] == "stale-job",
            "unexpected answer to stale submit {index}: {response}"
        );
    }
    assert_eq!(
        budget_refusals(&metrics),
        [0., 0., 0.],
        "an honest staleness burst inside the budget must not disconnect"
    );
    assert_eq!(stats.snapshot(0).connections, 1);

    // Re-authorizing still works with every budget enabled.
    client
        .send(json!({"id":30_000,"method":"mining.authorize","params":["miner.second","x"]}))
        .await;
    assert_eq!(client.response(30_000).await["result"], true);
    drop(client);
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn a_budget_disconnect_releases_the_global_per_source_and_per_username_permits() {
    let metrics = Arc::new(qbit_prism_server::metrics::Metrics::default());
    let config = StratumConfig {
        connection_limit: ConnectionLimit::new(1),
        max_connections_per_ip: 1,
        max_connections_per_username: 1,
        max_malformed_frames_per_interval: 1,
        session_budget_interval_seconds: 600.0,
        ..Default::default()
    };
    let stats = config.stats.clone();
    let (address, _backend, _refresh, shutdown, task) =
        start_with_metrics(config, metrics.clone()).await;
    let mut client = Client::connect(address).await;
    client.login("miner.only").await;
    for _ in 0..2 {
        client
            .send(json!({"id":1,"method":"mining.nonsense","params":[]}))
            .await;
    }
    client.expect_closed().await;
    drop(client);
    wait_for_connections(&stats, 0).await;
    assert_eq!(budget_refusals(&metrics), [1., 0., 0.]);
    // Every keyed and global permit the disconnected session held is back.
    let mut replacement = Client::connect(address).await;
    replacement.login("miner.only").await;
    assert_eq!(refusal_total(&metrics, "global_limit"), 0.);
    assert_eq!(refusal_total(&metrics, "ip_limit"), 0.);
    assert_eq!(refusal_total(&metrics, "username_limit"), 0.);
    drop(replacement);
    shutdown.send(true).unwrap();
    task.await.unwrap();
}

/// Run the real `check-config` entry point in an isolated process, as
/// `config_cli.rs` does, so validation sees the same effective value the
/// runtime reads.
async fn check_config(settings: &[(&str, &str)]) -> std::process::Output {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_qbit-prism-server"));
    command.arg("check-config").kill_on_drop(true);
    for (name, _) in std::env::vars().filter(|(name, _)| {
        name.starts_with("PRISM_") || name.starts_with("QBIT_") || name == "RUST_LOG"
    }) {
        command.env_remove(name);
    }
    command
        .env("PRISM_RUNTIME_WORKERS", "2")
        .env(
            "PRISM_DATABASE_URL",
            "postgresql://operator:test-only-password@127.0.0.1:1/offline",
        )
        .env("QBIT_RPC_URL", "http://127.0.0.1:1/")
        .env("QBIT_CHAIN", "regtest")
        .env("PRISM_ALLOW_TEST_SIGNING_SEEDS", "1");
    for (name, value) in settings {
        command.env(name, value);
    }
    timeout(Duration::from_secs(30), command.output())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn admission_settings_reject_malformed_and_out_of_range_values_by_name() {
    let names = [
        "PRISM_STRATUM_MAX_CONNECTIONS_PER_IP",
        "PRISM_STRATUM_SESSION_BUDGET_INTERVAL_SECONDS",
        "PRISM_STRATUM_MAX_MALFORMED_FRAMES_PER_INTERVAL",
        "PRISM_STRATUM_MAX_UNKNOWN_JOBS_PER_INTERVAL",
        "PRISM_STRATUM_MAX_AUTHORIZE_ATTEMPTS_PER_INTERVAL",
    ];
    // Malformed values fail as `invalid <name>` through the shared parser.
    for name in names {
        for value in ["banana", "-1", "1e999999"] {
            let output = check_config(&[(name, value)]).await;
            let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
            assert!(!output.status.success(), "{name}={value} was accepted");
            assert!(stderr.contains(name), "{name}={value}: {stderr}");
        }
    }
    // Out-of-range values fail with the setting's own named failure.
    for (name, value) in [
        ("PRISM_STRATUM_SESSION_BUDGET_INTERVAL_SECONDS", "3601"),
        ("PRISM_STRATUM_SESSION_BUDGET_INTERVAL_SECONDS", "0"),
        ("PRISM_STRATUM_SESSION_BUDGET_INTERVAL_SECONDS", "NaN"),
        ("PRISM_STRATUM_SESSION_BUDGET_INTERVAL_SECONDS", "inf"),
        ("PRISM_STRATUM_MAX_MALFORMED_FRAMES_PER_INTERVAL", "1000001"),
        ("PRISM_STRATUM_MAX_UNKNOWN_JOBS_PER_INTERVAL", "1000001"),
        (
            "PRISM_STRATUM_MAX_AUTHORIZE_ATTEMPTS_PER_INTERVAL",
            "1000001",
        ),
    ] {
        let output = check_config(&[(name, value)]).await;
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        assert!(!output.status.success(), "{name}={value} was accepted");
        assert!(stderr.contains(name), "{name}={value}: {stderr}");
    }
    // Empty selects the default, and every boundary value is accepted.
    for (name, value) in [
        ("PRISM_STRATUM_MAX_CONNECTIONS_PER_IP", ""),
        ("PRISM_STRATUM_MAX_CONNECTIONS_PER_IP", "0"),
        ("PRISM_STRATUM_MAX_CONNECTIONS_PER_IP", "1"),
        ("PRISM_STRATUM_SESSION_BUDGET_INTERVAL_SECONDS", ""),
        ("PRISM_STRATUM_SESSION_BUDGET_INTERVAL_SECONDS", "3600"),
        ("PRISM_STRATUM_SESSION_BUDGET_INTERVAL_SECONDS", "0.5"),
        ("PRISM_STRATUM_MAX_MALFORMED_FRAMES_PER_INTERVAL", "1000000"),
        ("PRISM_STRATUM_MAX_UNKNOWN_JOBS_PER_INTERVAL", "0"),
        ("PRISM_STRATUM_MAX_AUTHORIZE_ATTEMPTS_PER_INTERVAL", "1"),
    ] {
        let output = check_config(&[(name, value)]).await;
        assert!(
            output.status.success(),
            "{name}={value} rejected: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[path = "observability/stratum.rs"]
mod observability;

#[path = "observability/privacy.rs"]
mod metrics_privacy;
