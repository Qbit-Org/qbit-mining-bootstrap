//! Ungated decision tests: real Coordinator::submit, fake I/O, frozen economics.
//! Legacy reference: 95ffe063846d51f83999a66cc654da5f7476fdef,
//! tests/test_prism_retained_jobs.py and tests/test_prism_hot_path.py.
use super::*;
use axum::{extract::State, routing::post, Json, Router};
use futures_util::future::BoxFuture;
use std::sync::{
    atomic::{AtomicBool, AtomicI64, AtomicUsize},
    Mutex as StdMutex,
};
use submit_ledger::CommitGate;

mod admission_races;
mod authority_lease;
mod blockwait;
mod commit_reconcile;
mod compact_authority;
mod compact_prepared;
mod config;
mod credit;
mod interleavings;
mod observations;
mod prepared_expiry;
mod published_lease;
mod refresh;
mod resume_inputs;
pub(crate) mod stale_causes;
mod work_store;

pub(super) fn hash(byte: u8) -> String {
    format!("{byte:02x}").repeat(32)
}

#[derive(Default)]
pub(crate) struct Gate {
    pub entered: Notify,
    pub release: Notify,
}

#[derive(Default)]
pub(crate) struct MemoryLedger {
    pub revision: AtomicI64,
    pub records: StdMutex<Vec<(AcceptedShare, Option<Candidate>, i64)>>,
    pub revision_gate: StdMutex<Option<Arc<Gate>>>,
    pub append_gate: StdMutex<Option<Arc<Gate>>>,
    pub fail_revision: AtomicBool,
    pub jobs: StdMutex<HashMap<String, work_store::MemoryJob>>,
    pub compact: work_store::CompactStore,
    pub clock_offset_ms: AtomicI64,
    pub snapshot: StdMutex<Option<Snapshot>>,
    pub tip: StdMutex<Option<String>>,
    pub save_gate: StdMutex<Option<Arc<Gate>>>,
    pub fail_save: AtomicBool,
    /// Holds an append after its gate reached `Committing` and before its
    /// record is visible: COMMIT in flight.
    pub commit_gate: StdMutex<Option<Arc<Gate>>>,
    /// Fails the next COMMIT with an indeterminate error.
    pub fail_commit: StdMutex<Option<FailCommit>>,
    /// Appends dropped before they returned, as an aborted task is.
    pub cancelled: AtomicUsize,
}

/// An indeterminate COMMIT failure: the reply was lost, before or after the
/// commit became durable.
#[derive(Clone, Copy)]
pub(crate) enum FailCommit {
    NotRecorded,
    Recorded,
}

/// Counts a future dropped before it disarms the probe.
struct CancelProbe<'a>(&'a AtomicUsize, bool);

impl Drop for CancelProbe<'_> {
    fn drop(&mut self) {
        if !self.1 {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
}

impl MemoryLedger {
    async fn append_gated(
        &self,
        mut share: AcceptedShare,
        candidate: Option<Candidate>,
        revision: i64,
        commit: &CommitGate,
    ) -> Result<bool> {
        let gate = self.append_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        // Model the production atomic append fence, not share decisions.
        ensure!(
            revision == self.revision.load(Ordering::SeqCst),
            "payout revision changed"
        );
        let existing = self
            .records
            .lock()
            .unwrap()
            .iter()
            .find(|(old, _, _)| old.share_id == share.share_id)
            .map(|(old, _, _)| old.clone());
        if let Some(old) = &existing {
            share.share_seq = old.share_seq;
            share.accepted_at_ms = old.accepted_at_ms;
            ensure!(share == *old, "duplicate share_id payload mismatch");
        }
        // Model the production pre-commit hook: every statement has run.
        if !commit.begin_commit() {
            if existing.is_some() && candidate.is_none() {
                return Ok(false);
            }
            return Err(crate::ledger::CommitGateClosed.into());
        }
        let gate = self.commit_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        let failure = self.fail_commit.lock().unwrap().take();
        let lost = || sqlx::Error::Io(std::io::ErrorKind::ConnectionReset.into());
        if matches!(failure, Some(FailCommit::NotRecorded)) {
            return Err(lost().into());
        }
        let mut records = self.records.lock().unwrap();
        if records
            .iter()
            .any(|(old, _, _)| old.share_id == share.share_id)
        {
            return Ok(false);
        }
        records.push((share, candidate, revision));
        if matches!(failure, Some(FailCommit::Recorded)) {
            return Err(lost().into());
        }
        Ok(true)
    }
}

impl submit_ledger::SubmitLedger for MemoryLedger {
    fn payout_revision(&self) -> BoxFuture<'_, Result<i64>> {
        Box::pin(async move {
            let revision = self.revision.load(Ordering::SeqCst);
            let gate = self.revision_gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
            ensure!(!self.fail_revision.load(Ordering::SeqCst), "unavailable");
            Ok(revision)
        })
    }
    fn append_at_revision(
        &self,
        share: AcceptedShare,
        candidate: Option<Candidate>,
        revision: i64,
        gate: Arc<CommitGate>,
    ) -> BoxFuture<'_, Result<bool>> {
        Box::pin(async move {
            let mut probe = CancelProbe(&self.cancelled, false);
            let result = self.append_gated(share, candidate, revision, &gate).await;
            probe.1 = true;
            result
        })
    }
}

/// A `tracing` sink tests can read back, to see the phase and share ID that
/// an unknown answer logs for the operator.
#[derive(Clone, Default)]
pub(crate) struct SharedLog(Arc<StdMutex<Vec<u8>>>);

impl SharedLog {
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }

    pub fn dispatch(&self) -> tracing::Dispatch {
        tracing::Dispatch::new(
            tracing_subscriber::fmt()
                .with_writer(self.clone())
                .with_ansi(false)
                .with_max_level(tracing::Level::WARN)
                .finish(),
        )
    }
}

impl std::io::Write for SharedLog {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedLog {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

pub(crate) struct Node {
    pub tip: String,
    pub parents: HashMap<String, String>,
    pub calls: Vec<String>,
    pub fail: Option<String>,
    pub gate: Option<(String, Arc<Gate>)>,
}

async fn reply(State(node): State<Arc<StdMutex<Node>>>, Json(request): Json<Value>) -> Json<Value> {
    let method = request["method"].as_str().unwrap();
    let (result, failed, gate) = {
        let mut node = node.lock().unwrap();
        node.calls.push(method.into());
        let gate = if node
            .gate
            .as_ref()
            .is_some_and(|(expected, _)| expected == method)
        {
            node.gate.take().map(|(_, gate)| gate)
        } else {
            None
        };
        let result = match method {
            "waitfornewblock" => json!({"hash":node.tip,"height":100}),
            "getbestblockhash" | "getblockhash" => json!(node.tip),
            "getblocktemplate" => json!({"version":0x20000000u32,"bits":"207fffff",
                "curtime":chrono::Utc::now().timestamp(),"previousblockhash":node.tip,
                "transactions":[],"height":101,"coinbasevalue":500_000_000}),
            "getblockheader" => {
                json!({"previousblockhash":node.parents.get(request["params"][0].as_str().unwrap())})
            }
            "getblockchaininfo" => {
                json!({"chain":"regtest","initialblockdownload":false,"blocks":100,"headers":100,"bestblockhash":node.tip,"chainwork":"01"})
            }
            "getmempoolinfo" => json!({"minrelaytxfee":0.00001,"mempoolminfee":0.00001}),
            _ => panic!("unexpected node RPC {method}"),
        };
        (result, node.fail.as_deref() == Some(method), gate)
    };
    if let Some(gate) = gate {
        gate.entered.notify_one();
        gate.release.notified().await;
    } else if method == "waitfornewblock" {
        // Model the real idle long-poll instead of allowing a mock hot loop to
        // repair a discarded notification with an immediate second response.
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    Json(
        json!({"id":request["id"],"result":if failed {Value::Null} else {result},
        "error":if failed {json!({"code":-1,"message":"controlled node failure"})} else {Value::Null}}),
    )
}

pub(crate) struct Fixture {
    pub coordinator: Arc<Coordinator>,
    pub store: Arc<MemoryLedger>,
    pub node: Arc<StdMutex<Node>>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Fixture {
    pub async fn new(max_age: Duration) -> Self {
        Self::build(max_age, |_| {}, None).await
    }

    /// A fixture whose configuration `tune` adjusts, with the ledger sessions'
    /// effective `statement_timeout`.
    pub async fn build(
        max_age: Duration,
        tune: impl FnOnce(&mut Config),
        statement_timeout: Option<Duration>,
    ) -> Self {
        let node = Arc::new(StdMutex::new(Node {
            tip: hash(1),
            parents: [(hash(1), hash(0)), (hash(2), hash(1)), (hash(3), hash(2))].into(),
            calls: vec![],
            fail: None,
            gate: None,
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let app = Router::new()
            .route("/", post(reply))
            .with_state(node.clone());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let store = Arc::new(MemoryLedger::default());
        let mut config = config::test_config();
        config.submit_tip_max_age = max_age;
        tune(&mut config);
        let ledger = Arc::new(Ledger::offline_for_tests(
            sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgresql://unused@127.0.0.1:1/unused")
                .unwrap(),
            "offline-decisions".into(),
        ));
        let (refresh, _) = watch::channel(1);
        let coordinator = Arc::new(Coordinator {
            metrics: Arc::new(crate::metrics::Metrics::default()),
            rpc: Rpc::new(url, "test".into(), "test".into(), Duration::from_secs(5)).unwrap(),
            config: Arc::new(config),
            ledger,
            submit_ledger: store.clone(),
            work_ledger: store.clone(),
            prepared: Arc::new(RwLock::new(None)),
            refresh,
            wake: Notify::new(),
            accepted: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            blocks: AtomicU64::new(0),
            readiness: Arc::new(RwLock::new(ReadinessState {
                last_poll: Some(Instant::now()),
                ..Default::default()
            })),
            observed_tip: Arc::new(RwLock::new(TipState::default())),
            last_error: RwLock::new(None),
            build_slots: Arc::new(Semaphore::new(1)),
            window_reads: Arc::new(Semaphore::new(1)),
            refresh_lock: Mutex::new(()),
            identities: Mutex::new(HashMap::new()),
            chain_cache: Mutex::new(None),
            statement_timeout,
        });
        let fixture = Self {
            coordinator,
            store,
            node,
            server,
        };
        let job = fixture.job(1, 0, "original.worker");
        *fixture.store.snapshot.lock().unwrap() = Some((*job.context.prepared.snapshot).clone());
        *fixture.coordinator.prepared.write().await = Some(job.context.prepared.clone());
        fixture
    }

    pub async fn observe(&self, tip: u8, cache_parent: bool) {
        self.node.lock().unwrap().tip = hash(tip);
        self.coordinator.observe_chain_info(true).await.unwrap();
        if cache_parent {
            self.coordinator.cache_tip_parent(&hash(tip)).await.unwrap();
        }
        // Publication in production always couples the observed tip to work
        // built for that tip while holding the publication boundary. Keep the
        // fixture on that reachable path instead of publishing a new tip with
        // the previous tip's prepared work.
        let revision = self
            .coordinator
            .prepared
            .read()
            .await
            .as_ref()
            .map(|prepared| prepared.snapshot.payout_revision)
            .unwrap_or_default();
        let job = self.job(tip, revision, "original.worker");
        *self.store.snapshot.lock().unwrap() = Some((*job.context.prepared.snapshot).clone());
        *self.coordinator.prepared.write().await = Some(job.context.prepared.clone());
        self.coordinator
            .observed_tip
            .write()
            .await
            .publish(&hash(tip))
            .unwrap();
    }

    pub async fn detect(&self, tip: u8) {
        self.node.lock().unwrap().tip = hash(tip);
        self.coordinator.observe_chain_info(true).await.unwrap();
    }

    pub fn job(&self, tip: u8, revision: i64, username: &str) -> MiningJob<JobContext> {
        let worker = Worker {
            username: username.into(),
            payout_address: "miner-original".into(),
            worker_name: Some("worker".into()),
            p2mr_program_hex: hash(0xab),
        };
        let share = AcceptedShare {
            share_seq: 1,
            share_id: "fixture-share".into(),
            miner_id: worker.payout_address.clone(),
            order_key: worker.payout_address.clone(),
            p2mr_program_hex: worker.p2mr_program_hex.clone(),
            share_difficulty: 1_000_000,
            network_difficulty: 1_000_000,
            template_height: 100,
            job_id: "seed".into(),
            job_issued_at_ms: 100_000,
            accepted_at_ms: 100_000,
            ntime: 1_800_000_000,
            credit_policy: None,
        };
        let snapshot = Arc::new(Snapshot {
            anchor_ms: 100_000,
            share_seq: 1,
            payout_revision: revision,
            shares: vec![share.clone()],
            prior_balances: vec![],
        });
        let bundle = Arc::new(
            qbit_prism::build_audit_bundle_with_coinbase_options(
                vec![share],
                FoundBlock {
                    block_height: 101,
                    coinbase_value_sats: 500_000_000,
                    network_difficulty: 1_000_000,
                    anchor_job_issued_at_ms: 100_000,
                },
                vec![],
                qbit_prism::PayoutPolicy::day_one_default(),
                Some("00".repeat(12)),
                vec![],
                &ManifestSigningKey::from_seed_hex(&hash(0x11)).unwrap(),
                &ManifestSigningKey::from_seed_hex(&hash(0x22)).unwrap(),
            )
            .unwrap(),
        );
        let template = json!({"version":0x20000000u32,"bits":"207fffff","curtime":1_800_000_000u32,
            "previousblockhash":hash(tip),"transactions":[]});
        let mut wire = codec::Job::from_manifest(
            "issued-job".into(),
            &template,
            &bundle.signed_coinbase_manifest.manifest,
            "00000000",
            8,
            1e-12,
            0.0,
            true,
        )
        .unwrap();
        // Frozen target/difficulty pair: full regtest network target credits
        // exactly 1,000,000 scaled units, independently asserted by the tests.
        wire.share_target = codec::target_from_compact(0x207fffff).unwrap();
        wire.payout_revision = revision;
        let inputs = BundleInputs {
            payout_policy: qbit_prism::PayoutPolicy::day_one_default(),
            ctv: None,
            signer_keys: SignerKeys::of(
                &ManifestSigningKey::from_seed_hex(&hash(0x11)).unwrap(),
                &ManifestSigningKey::from_seed_hex(&hash(0x22)).unwrap(),
            ),
            audit_builder_version: qbit_prism::AUDIT_BUILDER_VERSION,
        };
        let prepared = Arc::new(Prepared {
            stored: Arc::new(StoredPrepared {
                template: template.clone(),
                snapshot: snapshot.clone(),
                bundle: Some(bundle.clone()),
                inputs: Some(inputs.clone()),
                fee: None,
                fingerprint: "fixture".into(),
                generation: 1,
                parent_of_tip: hash(tip.saturating_sub(1)),
                coinbase_suffix: "00".repeat(12),
            }),
            repair: Arc::new(Mutex::new(())),
            repair_probe: Default::default(),
            window: WindowRef::from_snapshot(&snapshot).expect("fixture window reference"),
            inputs,
            template,
            snapshot,
            bundle: Some(bundle.clone()),
            base_wire: None,
            storage_key: "fixture".into(),
            fee: None,
            fingerprint: "fixture".into(),
            generation: 1,
            created: Instant::now(),
            parent_of_tip: hash(tip.saturating_sub(1)),
        });
        MiningJob {
            wire,
            context: Arc::new(JobContext {
                prepared,
                worker,
                bundle,
                bootstrap_share: None,
                issuance_authority: None,
            }),
        }
    }

    pub fn proof(&self, job: &MiningJob<JobContext>, nonce: u32) -> codec::Submission {
        // Coordinator consumes codec-validated submissions. Use the real codec
        // and only choose a valid proof; tests may toggle block_pass to cover
        // candidate fencing after the shared difficulty validation boundary.
        (nonce..nonce + 10_000)
            .find_map(|nonce| {
                let proof = job
                    .wire
                    .assemble_submission(
                        &"00".repeat(8),
                        "6b49d200",
                        &format!("{nonce:08x}"),
                        None,
                        0,
                    )
                    .unwrap();
                proof.share_pass.then_some(proof)
            })
            .unwrap()
    }

    pub async fn submit(
        &self,
        job: &MiningJob<JobContext>,
        grace: impl Into<StaleGrace>,
    ) -> Result<(), StratumError> {
        self.coordinator
            .submit(&job.context.worker, job, self.proof(job, 0), grace.into())
            .await
    }
}

/// Raise the live relay floor above the fixture's CTV fee policy, as a refresh
/// observing a higher node floor would.
pub(crate) async fn raise_ctv_fee_floor(coordinator: &Coordinator) {
    coordinator.readiness.write().await.ctv_fee_floor = Some(2000);
}

pub(super) fn assert_error(error: StratumError, reason: &str, message: &str) {
    let response = error.response(json!(41));
    assert_eq!(response["error"][1], message);
    assert_eq!(response["error"][2]["reason_id"], reason);
}
