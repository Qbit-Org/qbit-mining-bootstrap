//! #604: the rebuild lane. A session that holds current work (its newest
//! job, for its worker, is on the published parent and payout revision)
//! rebuilds through `StratumConfig::rebuild_job_limit` and keeps that permit
//! until its job is persisted; a session without current work (no job yet, a
//! new worker, a tip change, a payout revision landing) never waits for it.
use qbit_pool_builder::{build_manifest, CoinbaseBuildRequest, WeightedEntitlement};
use qbit_prism_server::{
    codec::{Job, Submission},
    ledger::SessionId,
    stratum::*,
};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, Lines, ReadHalf, WriteHalf},
    sync::{watch, Notify, Semaphore},
    time::timeout,
};

/// Holds the next `persist_issued_job` until released.
#[derive(Default)]
struct PersistGate {
    entered: Notify,
    release: Notify,
}

#[derive(Default)]
struct Backend {
    sessions: AtomicU32,
    jobs: AtomicU64,
    persist_gate: Mutex<Option<Arc<PersistGate>>>,
    /// The published tip, as a number, and payout revision.
    tip: AtomicU64,
    revision: AtomicU64,
    /// Every build fails while set.
    fail_builds: AtomicBool,
}

impl Backend {
    fn parent(&self) -> String {
        // Hex letters, so the case of the published hint matters.
        format!("{:064x}", 0xfeed_0000 + self.tip.load(Ordering::SeqCst))
    }
}

impl MiningBackend for Backend {
    type Context = ();
    async fn published_work_hint(&self) -> Option<(String, i64)> {
        // Uppercase, as a node may report it; jobs carry it lowercased.
        Some((
            self.parent().to_ascii_uppercase(),
            self.revision.load(Ordering::SeqCst) as i64,
        ))
    }
    async fn new_session_id(&self) -> Result<SessionId, StratumError> {
        Ok((self.sessions.fetch_add(1, Ordering::Relaxed) + 1).into())
    }
    async fn authorize(&self, username: &str) -> Result<Worker, StratumError> {
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
        if self.fail_builds.load(Ordering::SeqCst) {
            return Err(StratumError::backend("build refused by the test"));
        }
        let template = json!({"version":0x20000000u32,"bits":"207fffff","curtime":1_700_000_000u32,
            "previousblockhash":self.parent(),"transactions":[]});
        let manifest = build_manifest(CoinbaseBuildRequest {
            block_height: 2,
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
        .map_err(|error| StratumError::internal(error.to_string()))?;
        let mut wire = Job::from_manifest(
            format!("job-{}", self.jobs.fetch_add(1, Ordering::Relaxed)),
            &template,
            &manifest,
            extranonce1,
            8,
            difficulty,
            minimum,
            true,
        )
        .map_err(|error| StratumError::internal(error.to_string()))?;
        wire.payout_revision = self.revision.load(Ordering::SeqCst) as i64;
        Ok(MiningJob {
            wire,
            context: Arc::new(()),
        })
    }
    /// Any job ID resumes as a job on the published work.
    async fn resume_job(
        &self,
        worker: &Worker,
        job_id: &str,
    ) -> Result<Option<MiningJob<()>>, StratumError> {
        let mut job = self.build_job(worker, "00000000", 1.0, 1.0).await?;
        job.wire.job_id = job_id.into();
        Ok(Some(job))
    }
    async fn persist_issued_job(
        &self,
        _worker: &Worker,
        _job: &MiningJob<()>,
        _version_mask: u32,
        _ttl: Duration,
    ) -> Result<(), StratumError> {
        let gate = self.persist_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        Ok(())
    }
    async fn submit(
        &self,
        _worker: &Worker,
        _job: &MiningJob<()>,
        _submission: Submission,
        _stale_grace: StaleGrace,
    ) -> Result<(), StratumError> {
        Err(StratumError::internal("not used"))
    }
}

struct Harness {
    backend: Arc<Backend>,
    config: StratumConfig,
    refresh: watch::Sender<u64>,
    shutdown: watch::Sender<bool>,
    sessions: tokio::task::JoinSet<anyhow::Result<()>>,
}

struct Client {
    writer: WriteHalf<DuplexStream>,
    lines: Lines<BufReader<ReadHalf<DuplexStream>>>,
}

impl Client {
    async fn send(&mut self, id: u64, method: &str, params: Value) {
        let mut frame =
            serde_json::to_vec(&json!({"id":id,"method":method,"params":params})).unwrap();
        frame.push(b'\n');
        self.writer.write_all(&frame).await.unwrap();
    }
    async fn next(&mut self) -> Value {
        let line = self
            .lines
            .next_line()
            .await
            .unwrap()
            .expect("session closed");
        serde_json::from_str(&line).unwrap()
    }
    async fn response(&mut self, id: u64) -> Value {
        loop {
            let value = self.next().await;
            if value["id"] == id {
                return value;
            }
        }
    }
    async fn open(&mut self, username: &str) {
        self.send(1, "mining.subscribe", json!([])).await;
        assert!(self.response(1).await["error"].is_null());
        self.send(2, "mining.authorize", json!([username, "x"]))
            .await;
        assert_eq!(self.response(2).await["result"], true);
    }
    async fn job(&mut self) {
        while self.next().await["method"] != "mining.notify" {}
    }
}

impl Harness {
    fn new(rebuild_permits: usize, initial_job_timeout_seconds: f64) -> Self {
        let mut config = StratumConfig {
            rebuild_job_limit: Arc::new(Semaphore::new(rebuild_permits)),
            initial_job_timeout_seconds,
            ..Default::default()
        };
        config.vardiff.enabled = false;
        Self {
            backend: Arc::new(Backend::default()),
            config,
            refresh: watch::channel(0).0,
            shutdown: watch::channel(false).0,
            sessions: tokio::task::JoinSet::new(),
        }
    }
    fn connect(&mut self) -> Client {
        let (client, server) = tokio::io::duplex(1 << 16);
        let (reader, writer) = tokio::io::split(server);
        self.sessions.spawn(serve_connection(
            reader,
            writer,
            self.backend.clone(),
            self.config.clone(),
            self.refresh.subscribe(),
            self.shutdown.subscribe(),
            Arc::new(qbit_prism_server::metrics::Metrics::default()),
        ));
        let (reader, writer) = tokio::io::split(client);
        Client {
            writer,
            lines: BufReader::new(reader).lines(),
        }
    }
    async fn until(&self, what: &str, done: impl Fn(&StratumStatsSnapshot) -> bool) {
        timeout(Duration::from_secs(5), async {
            while !done(&self.config.stats.snapshot(0)) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
    }
}

#[test]
fn the_lane_is_a_quarter_of_the_initial_job_permits() {
    assert_eq!(rebuild_lane_permits(128), 32);
    assert_eq!(rebuild_lane_permits(3), 1);
    assert_eq!(rebuild_lane_permits(1), 1);
    assert_eq!(
        StratumConfig::default()
            .rebuild_job_limit
            .available_permits(),
        rebuild_lane_permits(
            StratumConfig::default()
                .initial_job_limit
                .available_permits()
        )
    );
}

/// With every rebuild-lane permit held, a first job is still delivered, a
/// rebuild waits and is counted, a rebuild that times out in the lane stops
/// being counted, and the rebuild is delivered once the lane frees.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_jobs_bypass_a_full_rebuild_lane_and_rebuilds_wait_in_it() {
    let mut harness = Harness::new(1, 0.5);
    let held = harness
        .config
        .rebuild_job_limit
        .clone()
        .acquire_owned()
        .await
        .unwrap();
    let mut holder = harness.connect();
    holder.open("miner-a.rig").await;
    timeout(Duration::from_secs(2), holder.job())
        .await
        .expect("a first job waited for the full rebuild lane");
    harness.refresh.send_modify(|_| {});
    harness
        .until("the rebuild to wait in the lane", |s| {
            s.rebuild_lane_waiters == 1
        })
        .await;
    let mut newcomer = harness.connect();
    newcomer.open("miner-b.rig").await;
    timeout(Duration::from_secs(2), newcomer.job())
        .await
        .expect("a new session's first job waited behind a rebuild");
    // The held lane outlasts the delivery deadline: the rebuild fails like
    // any timed-out delivery and releases its place until the session's
    // timer retries it.
    harness
        .until("the lane wait to time out", |s| {
            s.job_delivery_failures >= 1 && s.rebuild_lane_waiters == 0
        })
        .await;
    drop(held);
    timeout(Duration::from_secs(5), holder.job())
        .await
        .expect("the rebuild was never retried after the lane freed");
    assert_eq!(harness.config.rebuild_job_limit.available_permits(), 1);
    let _ = harness.shutdown.send(true);
    harness.sessions.abort_all();
}

/// A rebuild holds its lane permit through persistence, and only there, not
/// the shared initial-job permit; a first job's persistence takes neither.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rebuild_keeps_its_lane_permit_until_its_job_is_persisted() {
    let mut harness = Harness::new(4, 5.0);
    let initial = harness.config.initial_job_limit.available_permits();
    let mut client = harness.connect();
    let gate = Arc::new(PersistGate::default());
    *harness.backend.persist_gate.lock().unwrap() = Some(gate.clone());
    client.open("miner-a.rig").await;
    timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .expect("the first job never reached persistence");
    assert_eq!(
        harness.config.rebuild_job_limit.available_permits(),
        4,
        "a first job's persistence took a rebuild-lane permit"
    );
    gate.release.notify_one();
    client.job().await;

    let gate = Arc::new(PersistGate::default());
    *harness.backend.persist_gate.lock().unwrap() = Some(gate.clone());
    harness.refresh.send_modify(|_| {});
    timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .expect("the rebuild never reached persistence");
    assert_eq!(
        harness.config.rebuild_job_limit.available_permits(),
        3,
        "a rebuild released its lane permit before its job was persisted"
    );
    assert_eq!(
        harness.config.initial_job_limit.available_permits(),
        initial,
        "a rebuild kept its shared initial-job permit through persistence"
    );
    gate.release.notify_one();
    client.job().await;
    assert_eq!(harness.config.rebuild_job_limit.available_permits(), 4);
    let _ = harness.shutdown.send(true);
    harness.sessions.abort_all();
}

/// With every rebuild-lane permit held, a session whose work a tip change or
/// a payout revision landing superseded, or that re-authorized as another
/// worker, still gets its job at once; a same-tip rebuild of current work
/// waits in the lane.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sessions_without_current_work_bypass_a_full_rebuild_lane() {
    let mut harness = Harness::new(1, 5.0);
    let held = harness
        .config
        .rebuild_job_limit
        .clone()
        .acquire_owned()
        .await
        .unwrap();
    let mut client = harness.connect();
    client.open("miner-a.rig").await;
    client.job().await;
    for change in ["tip change", "payout revision landing"] {
        match change {
            "tip change" => harness.backend.tip.fetch_add(1, Ordering::SeqCst),
            _ => harness.backend.revision.fetch_add(1, Ordering::SeqCst),
        };
        harness.refresh.send_modify(|_| {});
        timeout(Duration::from_secs(2), client.job())
            .await
            .unwrap_or_else(|_| panic!("work superseded by a {change} waited for the lane"));
    }
    client
        .send(3, "mining.authorize", json!(["miner-c.rig", "x"]))
        .await;
    assert_eq!(client.response(3).await["result"], true);
    timeout(Duration::from_secs(2), client.job())
        .await
        .expect("a re-authorized worker's first job waited for the lane");
    assert_eq!(harness.config.stats.snapshot(0).rebuild_lane_waiters, 0);
    harness.refresh.send_modify(|_| {});
    harness
        .until("the same-tip rebuild to wait in the lane", |s| {
            s.rebuild_lane_waiters == 1
        })
        .await;
    drop(held);
    timeout(Duration::from_secs(2), client.job())
        .await
        .expect("the same-tip rebuild was never delivered");
    let _ = harness.shutdown.send(true);
    harness.sessions.abort_all();
}

/// A same-tip rebuild waiting in a full lane stays there through another
/// same-tip publication, and leaves it for the shared admission once a tip
/// change or a payout revision landing supersedes its work.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rebuild_leaves_the_lane_when_a_publication_supersedes_its_work() {
    let mut harness = Harness::new(1, 5.0);
    let held = harness
        .config
        .rebuild_job_limit
        .clone()
        .acquire_owned()
        .await
        .unwrap();
    let mut client = harness.connect();
    client.open("miner-a.rig").await;
    client.job().await;
    for change in ["tip change", "payout revision landing"] {
        harness.refresh.send_modify(|_| {});
        harness
            .until("the same-tip rebuild to wait in the lane", |s| {
                s.rebuild_lane_waiters == 1
            })
            .await;
        harness.refresh.send_modify(|_| {});
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            harness.config.stats.snapshot(0).rebuild_lane_waiters,
            1,
            "a same-tip publication moved the rebuild out of the lane"
        );
        match change {
            "tip change" => harness.backend.tip.fetch_add(1, Ordering::SeqCst),
            _ => harness.backend.revision.fetch_add(1, Ordering::SeqCst),
        };
        harness.refresh.send_modify(|_| {});
        timeout(Duration::from_secs(2), client.job())
            .await
            .unwrap_or_else(|_| panic!("a rebuild superseded by a {change} stayed in the lane"));
        // The publications sent while it waited still make the session
        // rebuild its now current work, which queues in the lane again.
    }
    drop(held);
    let _ = harness.shutdown.send(true);
    harness.sessions.abort_all();
}

/// A session whose only job was resumed after a reconnect (its first
/// delivery failed, then the miner submitted on its old job) holds retired
/// work, not current work, so its next delivery bypasses a full lane.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resumed_job_is_not_current_work() {
    let mut harness = Harness::new(1, 5.0);
    let _held = harness
        .config
        .rebuild_job_limit
        .clone()
        .acquire_owned()
        .await
        .unwrap();
    harness.backend.fail_builds.store(true, Ordering::SeqCst);
    let mut client = harness.connect();
    client.open("miner-a.rig").await;
    harness
        .until("the first delivery to fail", |s| {
            s.job_delivery_failures >= 1
        })
        .await;
    harness.backend.fail_builds.store(false, Ordering::SeqCst);
    // The submit resumes the old job; the backend's `submit` then refuses the
    // share, and the session retries its delivery.
    client
        .send(
            3,
            "mining.submit",
            json!([
                "miner-a.rig",
                "old-job",
                "0".repeat(16),
                "00000000",
                "00000000"
            ]),
        )
        .await;
    timeout(Duration::from_secs(2), client.job())
        .await
        .expect("a session holding only a resumed job waited for the lane");
    assert_eq!(harness.config.stats.snapshot(0).rebuild_lane_waiters, 0);
    let _ = harness.shutdown.send(true);
    harness.sessions.abort_all();
}
