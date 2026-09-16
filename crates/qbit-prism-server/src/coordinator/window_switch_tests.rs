//! The claim side of the candidate switch (#265, slice 3), driven through
//! `Coordinator::process_candidate` against a real PostgreSQL and a fake node:
//! what the claim refuses to rebuild, what it does with a landed audit, how
//! every window read failure is settled, and the leased order.
//!
//! PRISM_TEST_DATABASE_URL=... cargo test -p qbit-prism-server --lib window_switch_tests
use super::*;
use crate::ledger::ShareRange;
use anyhow::bail;
use axum::{extract::State, routing::post, Json, Router};
use miner_submit::enqueue_failed_before_commit;
use qbit_prism_test_gate as gate;
use sqlx::PgPool;
use tokio::task::JoinHandle;

/// The ledger's advisory locks are cluster-wide constants, not schema-scoped.
use super::test_serial::TEST_LOCK;

const PARENT: &str = "aa";
const ORDER_LOCK: i64 = 0x505249534d000002;

#[derive(Default)]
struct ReplyGate {
    entered: Notify,
    release: Notify,
}

struct NodeState {
    tip: String,
    height: u64,
    chainwork: String,
    submissions: usize,
    /// Whether a submitted block becomes the tip.
    accept: bool,
    /// What `submitblock` answers: `null` for accepted, a string for a reason.
    submit_result: Value,
    submit_gate: Option<Arc<ReplyGate>>,
    /// The active chain by height, for `getblockhash`. An accepted block is
    /// appended at the next height; heights not recorded answer the tip.
    blocks: HashMap<u64, String>,
    /// Whether `getblockchaininfo` fails, as an unreachable or unsafe node.
    fail_chain_info: bool,
    /// A reorg to apply the moment the chain observation in flight has
    /// completed. `observe_candidate` ends with `getbestblockhash` and then
    /// the readiness proof's `getblockchaininfo`; the reorg fires after that
    /// second call is answered, so the observation that just returned saw
    /// the old chain in full and the next one sees the new.
    reorg_after_observation: Option<PendingReorg>,
}

struct PendingReorg {
    hash: String,
    height: u64,
    chainwork: String,
    /// Set once `getbestblockhash` has been answered since the reorg was
    /// armed: the next `getblockchaininfo` ends the observation.
    seen_best: bool,
}

impl NodeState {
    /// Make `hash` the tip at `height`, as a block from elsewhere would.
    fn reorg_to(&mut self, hash: &str, height: u64, chainwork: &str) {
        self.tip = hash.into();
        self.height = height;
        self.chainwork = chainwork.into();
        self.blocks.insert(height, hash.into());
    }

    /// Arm a reorg to `hash` at `height` for the end of the next completed
    /// chain observation; see `reorg_after_observation`.
    fn reorg_after_next_observation(&mut self, hash: &str, height: u64, chainwork: &str) {
        self.reorg_after_observation = Some(PendingReorg {
            hash: hash.into(),
            height,
            chainwork: chainwork.into(),
            seen_best: false,
        });
    }
}

async fn node_reply(
    State(node): State<Arc<Mutex<NodeState>>>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let gate = node.lock().await.submit_gate.clone();
    if request["method"] == "submitblock" {
        if let Some(gate) = &gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
    }
    let mut node = node.lock().await;
    let result = match request["method"].as_str().unwrap() {
        "getblockhash" if request["params"][0] == 0 => json!("00".repeat(32)),
        "getblockhash" => {
            let height = request["params"][0].as_u64().unwrap_or(0);
            json!(node
                .blocks
                .get(&height)
                .cloned()
                .unwrap_or_else(|| node.tip.clone()))
        }
        "getblockchaininfo" if node.fail_chain_info => {
            return Json(json!({"id":request["id"],"result":null,
                "error":{"code":-28,"message":"chain info unavailable"}}));
        }
        "getblockchaininfo" => {
            let info = json!({"chain":"test","initialblockdownload":false,
                "blocks":node.height,"headers":node.height,"bestblockhash":node.tip,"chainwork":node.chainwork});
            if node
                .reorg_after_observation
                .as_ref()
                .is_some_and(|pending| pending.seen_best)
            {
                let pending = node.reorg_after_observation.take().unwrap();
                node.reorg_to(&pending.hash, pending.height, &pending.chainwork);
            }
            info
        }
        "getbestblockhash" => {
            if let Some(pending) = node.reorg_after_observation.as_mut() {
                pending.seen_best = true;
            }
            json!(node.tip)
        }
        "getnetworkinfo" => json!({"connections":2}),
        "submitblock" => {
            let block = hex::decode(request["params"][0].as_str().unwrap()).unwrap();
            node.submissions += 1;
            if node.accept {
                let hash = codec::hash_display(&codec::double_sha256(&block[..80]));
                let height = node.height + 1;
                // Cumulative work grows with every block, so a second
                // accepted block is more work than the first, never an
                // equal-work conflicting tip.
                let chainwork = format!("{height:x}");
                node.reorg_to(&hash, height, &chainwork);
            }
            node.submit_result.clone()
        }
        method => panic!("unexpected candidate RPC {method}"),
    };
    Json(json!({"id":request["id"],"result":result,"error":null}))
}

/// A slim candidate beside the bundle the fixture built it from.
struct Found {
    candidate: Candidate,
    bundle: AuditBundle,
}

struct Fixture {
    admin: PgPool,
    schema: String,
    coordinator: Arc<Coordinator>,
    /// Further frontends on the same database and node, each with its own
    /// build capacity; see `Fixture::second_frontend`. Closed with the fixture.
    frontends: Mutex<Vec<Arc<Coordinator>>>,
    node: Arc<Mutex<NodeState>>,
    server: JoinHandle<()>,
    snapshot: Snapshot,
}

impl Fixture {
    async fn open() -> Result<Option<Self>> {
        Self::open_with(|_| {}).await
    }

    /// `open`, with the frontend's configuration adjusted before it starts.
    async fn open_with(configure: impl FnOnce(&mut Config)) -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let admin = PgPool::connect(&raw).await?;
        let schema = format!("prism_window_switch_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut url = url::Url::parse(&raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        let node = Arc::new(Mutex::new(NodeState {
            tip: PARENT.repeat(32),
            height: 100,
            chainwork: "01".into(),
            submissions: 0,
            accept: true,
            submit_result: Value::Null,
            submit_gate: None,
            blocks: HashMap::from([(100, PARENT.repeat(32))]),
            fail_chain_info: false,
            reorg_after_observation: None,
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let rpc_url = format!("http://{}/", listener.local_addr()?);
        let app = Router::new()
            .route("/", post(node_reply))
            .with_state(node.clone());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut config = Config {
            database_url: url.to_string(),
            instance_id: "window-switch".into(),
            // clamp(6 - 2, 1, 1): one window read at a time, one build slot.
            database_connections: 6,
            initialize_schema: true,
            chain: "testnet".into(),
            expected_genesis_hash: None,
            min_peers: 1,
            template_max_age: Duration::from_secs(120),
            submit_tip_max_age: Duration::from_secs(10),
            template_refresh_failure_exit: Duration::from_secs(120),
            rpc_url,
            rpc_user: "test".into(),
            rpc_password: "test".into(),
            rpc_timeout: Duration::from_secs(5),
            block_submit_timeout: Duration::from_secs(5),
            poll_interval: Duration::from_secs(1),
            blockwait: false,
            build_workers: 1,
            runtime_workers: 2,
            snapshot_interval: Duration::from_secs(60),
            health_timeout: Duration::from_secs(15),
            share_commit_timeout: Duration::from_secs(15),
            share_commit_grace: Duration::from_secs(5),
            block_only_ack_timeout: Duration::from_secs(60),
            extranonce2_size: 8,
            coinbase_tag: "/PRISM/".into(),
            manifest_seed: "11".repeat(32),
            ledger_seed: "22".repeat(32),
            ledger_public_key: ManifestSigningKey::from_seed_hex(&"22".repeat(32))?
                .public_key_hex(),
            username_fallback: None,
            payout_policy: qbit_prism::PayoutPolicy::day_one_default(),
            fee_address: None,
            ctv_enabled: false,
            ctv_config: qbit_prism::SettlementModeConfig::default(),
            ctv_direct_floor: 10_485_760,
            ctv_fee: None,
            ctv_fee_premium_bps: 12000,
            ctv_broadcast: false,
            ctv_broadcast_interval: Duration::from_secs(10),
            version_mask: codec::VERSION_ROLLING_MASK,
            audit_bind: "127.0.0.1".into(),
            audit_port: 0,
        };
        configure(&mut config);
        let coordinator =
            Coordinator::new(config, Arc::new(crate::metrics::Metrics::default())).await?;
        coordinator
            .ledger
            .observe_chain_view(&PARENT.repeat(32), 100, "01")
            .await?;
        *coordinator.observed_tip.write().await = TipState::baseline(PARENT.repeat(32));
        for index in 1..=3u64 {
            coordinator
                .ledger
                .append(
                    AcceptedShare {
                        share_seq: 0,
                        share_id: format!("miner:{index:064x}"),
                        miner_id: format!("miner-{}", index % 2),
                        order_key: format!("miner-{}", index % 2),
                        p2mr_program_hex: format!("{:02x}", 0x10 + index).repeat(32),
                        share_difficulty: 100,
                        network_difficulty: 100,
                        template_height: 100,
                        job_id: "seed".into(),
                        job_issued_at_ms: 1,
                        accepted_at_ms: 0,
                        ntime: 1_800_000_000,
                        credit_policy: None,
                    },
                    None,
                )
                .await?;
        }
        let snapshot = coordinator.ledger.snapshot(100).await?;
        Ok(Some(Self {
            admin,
            schema,
            coordinator,
            frontends: Mutex::new(Vec::new()),
            node,
            server,
            snapshot,
        }))
    }

    /// A second frontend, `instance_id`, on this fixture's database and node
    /// with the same configuration and keys but its own build capacity and
    /// metrics, observing `tip`. What it claims, reserves and lands is
    /// recorded under its own instance id.
    async fn second_frontend(&self, instance_id: &str, tip: &str) -> Result<Arc<Coordinator>> {
        let mut config = (*self.coordinator.config).clone();
        config.instance_id = instance_id.into();
        let frontend =
            Coordinator::new(config, Arc::new(crate::metrics::Metrics::default())).await?;
        *frontend.observed_tip.write().await = TipState::baseline(tip.to_owned());
        self.frontends.lock().await.push(frontend.clone());
        Ok(frontend)
    }

    fn keys(&self) -> (ManifestSigningKey, ManifestSigningKey) {
        (
            ManifestSigningKey::from_seed_hex(&self.coordinator.config.manifest_seed).unwrap(),
            ManifestSigningKey::from_seed_hex(&self.coordinator.config.ledger_seed).unwrap(),
        )
    }

    /// A candidate found on the fixture's window with this frontend's keys,
    /// so a rebuild reproduces exactly the bundle it was found with.
    fn found(&self, nonce_start: u32) -> Result<Found> {
        self.found_on(&self.snapshot, 101, &PARENT.repeat(32), nonce_start)
    }

    /// [`Fixture::found`] for a job issued on `snapshot` at `parent`, for a
    /// block at `height`. The candidate carries the snapshot's balances as
    /// its as-issued set, as the share path does.
    fn found_on(
        &self,
        snapshot: &Snapshot,
        height: u64,
        parent: &str,
        nonce_start: u32,
    ) -> Result<Found> {
        self.found_with(snapshot, height, parent, nonce_start, None)
    }

    /// A CTV candidate found on the fixture's window with the stored
    /// settlement inputs `ctv`, which need not be this frontend's
    /// configuration: they are what the block's coinbase commits to.
    fn found_ctv(&self, nonce_start: u32, ctv: CandidateCtv) -> Result<Found> {
        self.found_with(
            &self.snapshot,
            101,
            &PARENT.repeat(32),
            nonce_start,
            Some(ctv),
        )
    }

    /// [`Fixture::found_on`], with the block's coinbase built under the
    /// stored CTV settlement inputs when `ctv` is given.
    fn found_with(
        &self,
        snapshot: &Snapshot,
        height: u64,
        parent: &str,
        nonce_start: u32,
        ctv: Option<CandidateCtv>,
    ) -> Result<Found> {
        let (manifest_key, ledger_key) = self.keys();
        let bundle = match &ctv {
            Some(ctv) => self.ctv_bundle_on(snapshot, height, ctv)?,
            None => qbit_prism::build_audit_bundle_with_coinbase_options(
                snapshot.shares.clone(),
                FoundBlock {
                    block_height: height,
                    coinbase_value_sats: 500_000_000,
                    network_difficulty: 100,
                    anchor_job_issued_at_ms: snapshot.anchor_ms,
                },
                snapshot.prior_balances.clone(),
                qbit_prism::PayoutPolicy::day_one_default(),
                Some("00".repeat(12)),
                vec![],
                &manifest_key,
                &ledger_key,
            )?,
        };
        let template = json!({"version":0x20000000u32,"bits":"207fffff","curtime":1_800_000_000u32,
            "previousblockhash":parent,"transactions":[]});
        let job = codec::Job::from_manifest(
            "window-switch".into(),
            &template,
            &bundle.signed_coinbase_manifest.manifest,
            "00000000",
            8,
            1e-12,
            0.0,
            true,
        )?;
        let proof = (nonce_start..nonce_start + 10_000)
            .find_map(|nonce| {
                let proof = job
                    .assemble_submission(
                        &"00".repeat(8),
                        &format!("{:08x}", job.ntime),
                        &format!("{nonce:08x}"),
                        None,
                        0,
                    )
                    .ok()?;
                proof.block_pass.then_some(proof)
            })
            .context("constrained block proof missing")?;
        let block_bytes = hex::decode(&proof.block_hex)?;
        Ok(Found {
            candidate: Candidate {
                block_hash: proof.block_hash_hex,
                block_sha256: Candidate::block_digest_hex(&block_bytes),
                job_id: job.job_id,
                payout_revision: snapshot.payout_revision,
                window: WindowRef::from_snapshot(snapshot)?,
                bootstrap_share: None,
                found_block: bundle.found_block.clone(),
                payout_policy: qbit_prism::PayoutPolicy::day_one_default(),
                ctv,
                audit_builder_version: qbit_prism::AUDIT_BUILDER_VERSION,
                signer_keys: SignerKeys::of(&manifest_key, &ledger_key),
                leased: false,
                coinbase_suffix_hex: "00".repeat(12),
                deferred_share: None,
                block_bytes,
                as_issued_balances: snapshot.prior_balances.clone(),
            },
            bundle,
        })
    }

    /// The CTV bundle for the fixture's window under the settlement inputs
    /// `ctv`, as the building frontend made it.
    fn ctv_bundle(&self, ctv: &CandidateCtv) -> Result<AuditBundle> {
        self.ctv_bundle_on(&self.snapshot, 101, ctv)
    }

    /// [`Fixture::ctv_bundle`] for a block at `height` found on `snapshot`.
    fn ctv_bundle_on(
        &self,
        snapshot: &Snapshot,
        height: u64,
        ctv: &CandidateCtv,
    ) -> Result<AuditBundle> {
        let (manifest_key, ledger_key) = self.keys();
        Ok(qbit_prism::build_audit_bundle_with_ctv_settlement_options(
            snapshot.shares.clone(),
            FoundBlock {
                block_height: height,
                coinbase_value_sats: 500_000_000,
                network_difficulty: 100,
                anchor_job_issued_at_ms: snapshot.anchor_ms,
            },
            snapshot.prior_balances.clone(),
            qbit_prism::PayoutPolicy::day_one_default(),
            ctv.direct_floor_sats,
            ctv.settlement_config,
            ctv.fanout_fee_policy,
            Some("00".repeat(12)),
            vec![],
            &manifest_key,
            &ledger_key,
        )?)
    }

    async fn enqueue_and_claim(&self, found: &Found) -> Result<CandidateClaim> {
        self.coordinator
            .ledger
            .enqueue_candidate(found.candidate.clone())
            .await?;
        self.coordinator
            .ledger
            .claim_candidate(120)
            .await?
            .context("candidate claim missing")
    }

    fn process(&self, claim: &CandidateClaim) -> JoinHandle<Result<()>> {
        self.process_with(claim, CANDIDATE_LEASE)
    }

    fn process_with(
        &self,
        claim: &CandidateClaim,
        lease: CandidateLease,
    ) -> JoinHandle<Result<()>> {
        let coordinator = self.coordinator.clone();
        let claim = claim.clone();
        tokio::spawn(async move {
            coordinator
                .process_candidate_with_lease(&claim, lease)
                .await
        })
    }

    /// `(state, claim_token, last_error)` of the outbox row.
    async fn row(&self, block_hash: &str) -> Result<(String, Option<String>, Option<String>)> {
        Ok(sqlx::query_as(
            "SELECT state,claim_token,last_error FROM qbit_block_candidate_outbox WHERE block_hash=$1",
        )
        .bind(block_hash)
        .fetch_one(&self.coordinator.ledger.pool)
        .await?)
    }

    async fn landed(&self, block_hash: &str) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM qbit_pool_audit_bundles WHERE block_hash=$1)",
        )
        .bind(block_hash)
        .fetch_one(&self.coordinator.ledger.pool)
        .await?)
    }

    async fn submissions(&self) -> usize {
        self.node.lock().await.submissions
    }

    async fn expire(&self, block_hash: &str) -> Result<()> {
        sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_expires_at=clock_timestamp()-interval '1 second',next_attempt_at=clock_timestamp() WHERE block_hash=$1")
            .bind(block_hash).execute(&self.coordinator.ledger.pool).await?;
        Ok(())
    }

    /// Wait until the row reaches `state`, or report where it is and how
    /// the attempt driving it ended.
    async fn wait_for_state(
        &self,
        process: &mut JoinHandle<Result<()>>,
        block_hash: &str,
        state: &str,
    ) -> Result<()> {
        let reached = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if self.row(block_hash).await?.0 == state {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        if reached.is_err() {
            let row = self.row(block_hash).await?;
            let outcome = if process.is_finished() {
                format!("{:?}", process.await)
            } else {
                "still running".into()
            };
            bail!("the row never reached {state}: it is {row:?}; the attempt is {outcome}");
        }
        reached?
    }

    /// The current canonical balances, by program, as the ledger sums them.
    async fn current_balances(&self) -> Result<Vec<(String, i128)>> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT encode(p2mr_program,'hex'),balance_sats::text FROM qbit_current_carry_forward_balances() ORDER BY 1",
        )
        .fetch_all(&self.coordinator.ledger.pool)
        .await?;
        rows.into_iter()
            .map(|(program, balance)| Ok((program, balance.parse()?)))
            .collect()
    }

    /// The semantic digest of the current canonical balances: what a job
    /// issued now would carry as its window reference.
    async fn current_digest(&self) -> Result<[u8; 32]> {
        let snapshot = self.coordinator.ledger.snapshot(100).await?;
        Ok(qbit_prism::prior_balances_digest(&snapshot.prior_balances))
    }

    /// `(mismatch_count, current_drift_count)` of the integrity report.
    async fn integrity(&self) -> Result<(u64, u64)> {
        let report: Value = sqlx::query_scalar("SELECT qbit_carry_forward_integrity_report()")
            .fetch_one(&self.coordinator.ledger.pool)
            .await?;
        Ok((
            report["mismatch_count"]
                .as_u64()
                .context("mismatch_count")?,
            report["current_drift_count"]
                .as_u64()
                .context("current_drift_count")?,
        ))
    }

    async fn close(self) -> Result<()> {
        self.server.abort();
        self.coordinator.ledger.pool.close().await;
        for frontend in self.frontends.into_inner() {
            frontend.ledger.pool.close().await;
        }
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await?;
        self.admin.close().await;
        Ok(())
    }
}

/// A row settled after its offer: in reconciliation, released, with a reason
/// that names the landing failure, and with its evidence intact.
async fn assert_reconciled(fixture: &Fixture, block_hash: &str, reason: &str) -> Result<()> {
    let (state, token, error) = fixture.row(block_hash).await?;
    ensure!(
        state == "reconciliation",
        "the row is {state}, not reconciliation"
    );
    ensure!(token.is_none(), "the claim was not released");
    let error = error.context("no reason was recorded")?;
    ensure!(
        error.to_ascii_lowercase().contains(reason),
        "reason {error:?} does not mention {reason:?}"
    );
    ensure!(
        error.contains("landing failed after the offer"),
        "reason {error:?} does not say the landing failed after the offer"
    );
    let evidence: bool = sqlx::query_scalar(
        "SELECT candidate IS NOT NULL AND block_bytes IS NOT NULL AND window_anchor_ms IS NOT NULL FROM qbit_block_candidate_outbox WHERE block_hash=$1",
    )
    .bind(block_hash)
    .fetch_one(&fixture.coordinator.ledger.pool)
    .await?;
    ensure!(evidence, "the reconciliation row lost its evidence");
    Ok(())
}

// ---------------------------------------------------------------------------
// Stored inputs
// ---------------------------------------------------------------------------

/// A stored builder version or signer pair that is not this binary's is
/// found after the offer (the block itself is valid and is offered), and
/// the row settles in reconciliation with a reason naming both values,
/// never rebuilt: the build slot is held by the test for the whole attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn builder_version_or_signer_mismatch_retries_without_a_rebuild() -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        // The node takes each offer without moving its tip, so the second
        // candidate is not superseded by the first.
        fixture.node.lock().await.accept = false;
        let held = fixture
            .coordinator
            .build_slots
            .clone()
            .acquire_owned()
            .await?;
        let mut version = fixture.found(0)?;
        version.candidate.audit_builder_version = 99;
        let mut keys = fixture.found(10_000)?;
        keys.candidate.signer_keys = SignerKeys {
            manifest_key_hex: "ab".repeat(32),
            ledger_key_hex: "cd".repeat(32),
        };
        for (found, mentions) in [
            (
                &version,
                vec![
                    "99".to_owned(),
                    qbit_prism::AUDIT_BUILDER_VERSION.to_string(),
                ],
            ),
            (
                &keys,
                vec![
                    "ab".repeat(32),
                    "cd".repeat(32),
                    keys.candidate.signer_keys.manifest_key_hex.clone(),
                    fixture.coordinator.config.ledger_public_key.clone(),
                ],
            ),
        ] {
            let claim = fixture.enqueue_and_claim(found).await?;
            tokio::time::timeout(Duration::from_secs(10), fixture.process(&claim))
                .await
                .context(
                    "a mismatched candidate waited for the build slot: it tried to rebuild",
                )???;
            let hash = &found.candidate.block_hash;
            let (_, _, error) = fixture.row(hash).await?;
            let error = error.context("no alert was recorded")?;
            for value in mentions {
                ensure!(error.contains(&value), "{error:?} does not name {value}");
            }
            assert_reconciled(&fixture, hash, "not rebuilding").await?;
            ensure!(
                !fixture.landed(hash).await?,
                "a mismatched candidate landed"
            );
        }
        ensure!(
            fixture.submissions().await == 2,
            "each valid block is offered exactly once before its inputs are checked"
        );
        drop(held);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    fixture.close().await?;
    result
}

// ---------------------------------------------------------------------------
// The landed audit
// ---------------------------------------------------------------------------

/// A claim whose audit already landed offers the block, then authenticates
/// the landed row against its block and skips the rebuild and landing (the
/// build slot is held by the test) before finishing. A forged landed row is
/// refused after the offer and the candidate stays in reconciliation with
/// its evidence, never rebuilt over the existing rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn landed_audit_is_authenticated_and_the_claim_continues_to_submitblock() -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    for case in ["authentic", "forged coinbase", "forged audit root"] {
        let Some(fixture) = Fixture::open().await? else {
            return Ok(());
        };
        let result = async {
            let found = fixture.found(0)?;
            let hash = found.candidate.block_hash.clone();
            let first = fixture.enqueue_and_claim(&found).await?;
            fixture
                .coordinator
                .ledger
                .land_candidate(
                    &first.with_bundle(found.bundle.clone()),
                    &fixture.coordinator.config.ledger_public_key,
                )
                .await?;
            ensure!(fixture.landed(&hash).await?);
            let forgery = match case {
                // Flip the digit to something it is not. Placing a literal
                // would be a no-op whenever the digit already held that value,
                // and the coinbase varies per run because the bundle's anchor
                // comes from the ledger clock: the forgery would then change
                // nothing and its acceptance would be correct, so the case
                // would pass or fail at random.
                "forged coinbase" => Some(
                    "UPDATE qbit_pool_audit_bundles SET coinbase_tx_hex=overlay(coinbase_tx_hex placing CASE WHEN substr(coinbase_tx_hex,length(coinbase_tx_hex)-8,1)='0' THEN '1' ELSE '0' END from length(coinbase_tx_hex)-8 for 1) WHERE block_hash=$1",
                ),
                "forged audit root" => Some(
                    "UPDATE qbit_pool_audit_bundles SET audit_commitment_leaves_hex=to_jsonb(ARRAY[repeat('ab',32)]) WHERE block_hash=$1",
                ),
                _ => None,
            };
            if let Some(statement) = forgery {
                // Read the row back rather than trusting the statement: a
                // forgery that altered nothing would be accepted, and the case
                // would then prove nothing while appearing to pass.
                let before: Value = sqlx::query_scalar(
                    "SELECT to_jsonb(b) FROM qbit_pool_audit_bundles b WHERE block_hash=$1",
                )
                .bind(&hash)
                .fetch_one(&fixture.coordinator.ledger.pool)
                .await?;
                sqlx::query(statement)
                    .bind(&hash)
                    .execute(&fixture.coordinator.ledger.pool)
                    .await?;
                let after: Value = sqlx::query_scalar(
                    "SELECT to_jsonb(b) FROM qbit_pool_audit_bundles b WHERE block_hash=$1",
                )
                .bind(&hash)
                .fetch_one(&fixture.coordinator.ledger.pool)
                .await?;
                ensure!(before != after, "case {case}: the forgery changed nothing");
            }
            fixture.expire(&hash).await?;
            let second = fixture
                .coordinator
                .ledger
                .claim_candidate(120)
                .await?
                .context("the landed candidate was not reclaimable")?;
            let held = fixture.coordinator.build_slots.clone().acquire_owned().await?;
            let outcome = tokio::time::timeout(Duration::from_secs(10), fixture.process(&second))
                .await
                .context("the recovered claim waited for the build slot: it rebuilt a landed audit")??;
            drop(held);
            let (state, _, _) = fixture.row(&hash).await?;
            outcome?;
            ensure!(fixture.submissions().await == 1, "the block was not offered exactly once");
            if forgery.is_none() {
                ensure!(state == "submitted", "the recovered claim finished as {state}");
                ensure!(fixture.coordinator.blocks.load(Ordering::Relaxed) == 1);
            } else {
                assert_reconciled(&fixture, &hash, "does not authenticate").await?;
                ensure!(
                    fixture.coordinator.blocks.load(Ordering::Relaxed) == 0,
                    "the forgery was counted as a confirmed block"
                );
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        fixture.close().await?;
        result.with_context(|| format!("case: {case}"))?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Window read failures
// ---------------------------------------------------------------------------

/// `Incomplete`, `SnapshotDigestMismatch`, a reference whose balances are
/// neither snapshotted nor current, and the rebuild deadline all fail the
/// landing after the offer: the row settles in reconciliation, released,
/// with the reason and its evidence, nothing lands, and nothing is
/// abandoned or offered twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn window_read_failures_retry_with_an_alert_and_never_abandon() -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        // The node takes each offer without moving its tip, so no candidate
        // is superseded by an earlier one.
        fixture.node.lock().await.accept = false;
        let range = fixture
            .found(0)?
            .candidate
            .window
            .shares
            .context("the fixture window has a range")?;
        let mut incomplete = fixture.found(0)?;
        incomplete.candidate.window.shares = Some(ShareRange {
            first_share_seq: 5_000_000,
            last_share_seq: 5_000_002,
            share_count: 3,
            ..range
        });
        let mut digest = fixture.found(10_000)?;
        digest.candidate.window.shares = Some(ShareRange {
            snapshot_sha256: [9; 32],
            ..range
        });
        let mut balances = fixture.found(20_000)?;
        balances.candidate.window.prior_balances_digest = [1; 32];
        for (found, reason) in [
            (&incomplete, "incomplete"),
            (&digest, "digest mismatch"),
            (&balances, "no longer the reference's"),
        ] {
            let claim = fixture.enqueue_and_claim(found).await?;
            tokio::time::timeout(Duration::from_secs(10), fixture.process(&claim)).await???;
            assert_reconciled(&fixture, &found.candidate.block_hash, reason).await?;
            ensure!(!fixture.landed(&found.candidate.block_hash).await?);
        }
        // The deadline: the only window-read permit is held, so the read never
        // starts and the attempt expires, releasing the build slot it took.
        let held = fixture
            .coordinator
            .window_reads
            .clone()
            .acquire_owned()
            .await?;
        let found = fixture.found(30_000)?;
        let claim = fixture.enqueue_and_claim(&found).await?;
        let lease = CandidateLease {
            rebuild_deadline: Duration::from_millis(500),
            ..CANDIDATE_LEASE
        };
        tokio::time::timeout(Duration::from_secs(10), fixture.process_with(&claim, lease))
            .await???;
        assert_reconciled(&fixture, &found.candidate.block_hash, "deadline").await?;
        ensure!(
            fixture.coordinator.build_slots.available_permits() == 1,
            "the expired attempt kept its build slot"
        );
        drop(held);
        ensure!(
            fixture.submissions().await == 4,
            "each block is offered exactly once before its rebuild"
        );
        let blocks: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_pool_blocks")
            .fetch_one(&fixture.coordinator.ledger.pool)
            .await?;
        ensure!(blocks == 0, "a failed rebuild landed");
        Ok::<_, anyhow::Error>(())
    }
    .await;
    fixture.close().await?;
    result
}

/// The mapping itself, variant by variant: a cancelled or panicked blocking
/// hand-off is retryable with its own alert and never corruption; a database
/// error is not mapped but propagated.
#[tokio::test]
async fn classify_window_error_keeps_a_task_failure_apart_from_corruption() -> Result<()> {
    let join = tokio::spawn(async { panic!("deliberate blocking failure") })
        .await
        .unwrap_err();
    match classify_window_error(WindowError::TaskFailed(join))? {
        RebuildFailure::Retry(reason) => {
            ensure!(reason.contains("not corruption"), "{reason}");
            ensure!(!reason.contains("corruption or"), "{reason}");
        }
        other => bail!("a task failure mapped to {other:?}"),
    }
    for (error, expected) in [
        (
            WindowError::Incomplete {
                expected: 3,
                got: 1,
            },
            "incomplete",
        ),
        (
            WindowError::SnapshotDigestMismatch {
                expected: [0; 32],
                actual: [1; 32],
            },
            "digest mismatch",
        ),
        (
            WindowError::Decode(anyhow::anyhow!("bad row")),
            "decode error",
        ),
    ] {
        match classify_window_error(error)? {
            RebuildFailure::Retry(reason) => ensure!(reason.contains(expected), "{reason}"),
            other => bail!("{expected} mapped to {other:?}"),
        }
    }
    ensure!(matches!(
        classify_window_error(WindowError::PriorBalancesChanged {
            expected: [0; 32],
            actual: [1; 32],
        })?,
        RebuildFailure::PriorBalancesChanged
    ));
    // A missing as-issued snapshot is its own outcome: the caller falls back
    // to a `Current` read rather than settling on the missing row.
    ensure!(matches!(
        classify_window_error(WindowError::BalanceSnapshotMissing { digest: [2; 32] })?,
        RebuildFailure::BalanceSnapshotMissing
    ));
    ensure!(
        classify_window_error(WindowError::Database(sqlx::Error::PoolClosed)).is_err(),
        "a database error was mapped instead of propagated"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// The rebuild
// ---------------------------------------------------------------------------

/// The claim's read, rebuild and landing never take `ORDER_LOCK`: with the
/// lock held by another session the audit lands, and only the terminal
/// update, which takes the lock, waits. With one build worker the rebuild
/// also proves it never nests `build_bundle`, whose own permit would wait on
/// itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_rebuild_and_landing_never_wait_for_the_order_lock() -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let found = fixture.found(0)?;
        let hash = found.candidate.block_hash.clone();
        // The enqueue itself takes `ORDER_LOCK`, so the row exists before the
        // lock is held for the claim.
        let claim = fixture.enqueue_and_claim(&found).await?;
        let mut holder = fixture.admin.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(ORDER_LOCK)
            .execute(&mut *holder)
            .await?;
        let process = fixture.process(&claim);
        tokio::time::timeout(Duration::from_secs(10), async {
            while !fixture.landed(&hash).await? {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("the audit did not land while ORDER_LOCK was held: the claim waited for it")??;
        let (state, _, _) = fixture.row(&hash).await?;
        ensure!(
            state == "offered",
            "the terminal update did not wait for ORDER_LOCK, or the offer did: {state}"
        );
        holder.rollback().await?;
        tokio::time::timeout(Duration::from_secs(10), process).await???;
        let (state, _, _) = fixture.row(&hash).await?;
        ensure!(state == "submitted", "the claim finished as {state}");
        ensure!(fixture.submissions().await == 1);
        ensure!(
            fixture.coordinator.build_slots.available_permits() == 1
                && fixture.coordinator.window_reads.available_permits() == 1,
            "a permit was kept after the claim finished"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    fixture.close().await?;
    result
}

/// A CTV candidate is rebuilt from the settlement inputs it stores, never from
/// this frontend's configuration. The fixture runs with CTV enabled and a
/// 10,485,760-sat direct floor; the candidate stores a 400,000,000-sat floor,
/// above both miners' payouts, so the two floors build different coinbases.
/// A rebuild that read the configured floor would produce a coinbase the
/// block does not commit to, and landing would refuse it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ctv_candidate_rebuilds_from_its_stored_settlement_inputs_not_configuration() -> Result<()>
{
    let _serial = TEST_LOCK.lock().await;
    let fee = FanoutFeeRatePolicy::new(1000, 12000);
    let Some(fixture) = Fixture::open_with(|config| {
        config.ctv_enabled = true;
        config.ctv_fee = Some(fee);
    })
    .await?
    else {
        return Ok(());
    };
    let result = async {
        let config = &fixture.coordinator.config;
        let stored = CandidateCtv {
            direct_floor_sats: 400_000_000,
            settlement_config: config.ctv_config,
            fanout_fee_policy: Some(fee),
        };
        let configured = CandidateCtv {
            direct_floor_sats: config.ctv_direct_floor,
            ..stored.clone()
        };
        ensure!(
            stored.direct_floor_sats != configured.direct_floor_sats,
            "the stored floor must differ from the configured one"
        );
        let found = fixture.found_ctv(0, stored)?;
        let from_config = fixture.ctv_bundle(&configured)?;
        ensure!(
            serde_json::to_vec(&found.bundle.signed_coinbase_manifest.manifest)?
                != serde_json::to_vec(&from_config.signed_coinbase_manifest.manifest)?,
            "the stored and configured floors build the same coinbase, so this case proves nothing"
        );
        let hash = found.candidate.block_hash.clone();
        let claim = fixture.enqueue_and_claim(&found).await?;
        ensure!(claim.candidate.ctv == found.candidate.ctv);
        tokio::time::timeout(Duration::from_secs(30), fixture.process(&claim)).await???;
        let (state, _, error) = fixture.row(&hash).await?;
        ensure!(
            state == "submitted",
            "the CTV candidate finished as {state}: {error:?}"
        );
        ensure!(
            fixture.landed(&hash).await?,
            "the CTV candidate did not land"
        );
        ensure!(fixture.submissions().await == 1);
        let report = qbit_prism::verify_audit_bundle_with_ledger_public_key(
            &found.bundle,
            &config.ledger_public_key,
        )?;
        let landed: String = sqlx::query_scalar(
            "SELECT audit_bundle_sha256 FROM qbit_pool_audit_bundles WHERE block_hash=$1",
        )
        .bind(&hash)
        .fetch_one(&fixture.coordinator.ledger.pool)
        .await?;
        ensure!(
            landed == report.audit_bundle_sha256_hex,
            "the landed CTV audit is not the bundle the block was found on"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    fixture.close().await?;
    result
}

// ---------------------------------------------------------------------------
// Leased candidates
// ---------------------------------------------------------------------------

/// A `leased` candidate submits its stored block before anything else, with
/// no staleness screen, then rebuilds from its as-issued balances and lands
/// before it takes any outcome: an accepted block finishes submitted, a
/// rejected one stays in reconciliation with the node's reason and its
/// evidence (never abandoned after an offer), and a missing as-issued
/// snapshot falls back to the current balances, which still hash to the
/// reference here, so the block lands and confirms.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leased_candidate_submits_first_then_lands_as_issued_before_any_terminal_outcome(
) -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    for case in ["accepted", "rejected", "snapshot-missing"] {
        let Some(fixture) = Fixture::open().await? else {
            return Ok(());
        };
        let result = async {
            let mut found = fixture.found(0)?;
            found.candidate.leased = true;
            found.candidate.as_issued_balances = fixture.snapshot.prior_balances.clone();
            let hash = found.candidate.block_hash.clone();
            let digest = hex::encode(found.candidate.window.prior_balances_digest);
            let claim = fixture.enqueue_and_claim(&found).await?;
            let snapshots: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM qbit_prism_balance_snapshots WHERE prior_balances_digest=$1",
            )
            .bind(&digest)
            .fetch_one(&fixture.coordinator.ledger.pool)
            .await?;
            ensure!(
                snapshots == 1,
                "the leased enqueue wrote no as-issued snapshot"
            );
            let gate = Arc::new(ReplyGate::default());
            {
                let mut node = fixture.node.lock().await;
                node.submit_gate = Some(gate.clone());
                if case == "rejected" {
                    node.accept = false;
                    node.submit_result = json!("duplicate");
                }
            }
            if case == "snapshot-missing" {
                sqlx::query("DELETE FROM qbit_prism_balance_snapshots")
                    .execute(&fixture.coordinator.ledger.pool)
                    .await?;
            }
            let process = fixture.process(&claim);
            tokio::time::timeout(Duration::from_secs(10), gate.entered.notified())
                .await
                .context("the leased candidate was not submitted")?;
            // Nothing is landed or decided while the node holds the block:
            // the row is the reservation, still claimed.
            ensure!(
                !fixture.landed(&hash).await?,
                "{case}: landed before submitblock"
            );
            let (state, token, _) = fixture.row(&hash).await?;
            ensure!(
                state == "offer_reserved" && token.is_some(),
                "{case}: {state} before submitblock returned"
            );
            gate.release.notify_one();
            tokio::time::timeout(Duration::from_secs(10), process).await???;
            ensure!(fixture.submissions().await == 1);
            let (state, token, error) = fixture.row(&hash).await?;
            let (outcome, reply): (Option<String>, Option<String>) = sqlx::query_as(
                "SELECT offer_outcome,offer_reply FROM qbit_block_candidate_outbox WHERE block_hash=$1",
            )
            .bind(&hash)
            .fetch_one(&fixture.coordinator.ledger.pool)
            .await?;
            match case {
                "accepted" => {
                    ensure!(
                        fixture.landed(&hash).await?,
                        "the accepted block did not land"
                    );
                    ensure!(state == "submitted", "accepted block finished as {state}");
                    ensure!(outcome.as_deref() == Some("accepted"));
                }
                "rejected" => {
                    ensure!(
                        fixture.landed(&hash).await?,
                        "the rejected block did not land"
                    );
                    ensure!(
                        state == "reconciliation" && token.is_none(),
                        "rejected block finished as {state}"
                    );
                    ensure!(outcome.as_deref() == Some("rejected"), "{outcome:?}");
                    ensure!(reply.as_deref() == Some("duplicate"), "{reply:?}");
                    ensure!(
                        error.as_deref().is_some_and(|e| e.contains("duplicate")
                            && e.contains("not on the active chain")),
                        "{error:?}"
                    );
                }
                _ => {
                    ensure!(
                        fixture.landed(&hash).await?,
                        "the current balances still hash to the reference, so the block lands"
                    );
                    ensure!(state == "submitted", "{state} {token:?} {error:?}");
                    let snapshots: i64 = sqlx::query_scalar(
                        "SELECT count(*) FROM qbit_prism_balance_snapshots",
                    )
                    .fetch_one(&fixture.coordinator.ledger.pool)
                    .await?;
                    ensure!(snapshots == 0, "the fallback wrote a snapshot back");
                }
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        fixture.close().await?;
        result.with_context(|| format!("case {case}"))?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Off-runtime release
// ---------------------------------------------------------------------------

struct Probe(Option<tokio::sync::oneshot::Sender<std::thread::ThreadId>>);

impl Drop for Probe {
    fn drop(&mut self) {
        let _ = self.0.take().unwrap().send(std::thread::current().id());
    }
}

/// Whatever `OffRuntime` guards is dropped on a blocking thread, on the
/// ordinary path and when the future holding it is cancelled.
#[tokio::test(flavor = "current_thread")]
async fn off_runtime_releases_its_value_on_a_blocking_thread() -> Result<()> {
    let runtime_thread = std::thread::current().id();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    drop(OffRuntime::new(Probe(Some(sender))));
    let thread = tokio::time::timeout(Duration::from_secs(2), receiver).await??;
    ensure!(thread != runtime_thread, "dropped on the runtime thread");
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let (entered, ready) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let _held = OffRuntime::new(Probe(Some(sender)));
        entered.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    ready.await?;
    task.abort();
    ensure!(task.await.unwrap_err().is_cancelled());
    let thread = tokio::time::timeout(Duration::from_secs(2), receiver).await??;
    ensure!(
        thread != runtime_thread,
        "a cancelled holder dropped on the runtime thread"
    );
    Ok(())
}

/// Every check `prepare_candidate` makes is a definite rejection, never an
/// unknown outcome.
///
/// #324 decides what a miner is told when a block-only enqueue fails:
/// `enqueue_failed_before_commit` answers a provably uncommitted failure with
/// `ledger-confirmation-failed`, and anything else with
/// `ledger-outcome-unknown`, which asks an operator to follow the outbox row.
/// Slice 3 added the checks that guard the reference — the block's length, its
/// header hash, its digest, the coinbase suffix, the reference invariants and a
/// leased candidate's balances — and every one of them is raised before the
/// enqueue takes a connection, so none carries an `sqlx::Error` and all
/// classify as definite. A corrupt candidate must never send a miner chasing a
/// row that was never written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_candidate_is_a_definite_rejection_not_an_unknown_outcome() -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        /// What a case does to a candidate the fixture just found.
        type Corrupt = fn(&mut Candidate);
        let cases: [(&str, &str, Corrupt); 3] = [
            ("truncated block", "candidate block is truncated", |c| {
                c.block_bytes.truncate(40);
            }),
            ("block digest", "candidate block digest mismatch", |c| {
                c.block_sha256 = "ff".repeat(32);
            }),
            (
                "coinbase suffix",
                "candidate coinbase suffix must be non-empty hex",
                |c| c.coinbase_suffix_hex.clear(),
            ),
        ];
        for (index, (name, message, corrupt)) in cases.into_iter().enumerate() {
            let mut found = fixture.found(20_000 * (index as u32 + 1))?;
            let block_hash = found.candidate.block_hash.clone();
            corrupt(&mut found.candidate);
            let error = fixture
                .coordinator
                .ledger
                .enqueue_candidate_once(found.candidate)
                .await
                .err()
                .with_context(|| format!("{name}: the enqueue was accepted"))?;
            ensure!(
                error.to_string().contains(message),
                "{name}: refused with {error}, expected {message}"
            );
            ensure!(
                enqueue_failed_before_commit(&error),
                "{name}: classified as an unknown outcome, so the miner would be told \
                 ledger-outcome-unknown for a row that was never written: {error}"
            );
            let rows: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM qbit_block_candidate_outbox WHERE block_hash=$1",
            )
            .bind(&block_hash)
            .fetch_one(&fixture.coordinator.ledger.pool)
            .await?;
            ensure!(rows == 0, "{name}: the refused candidate left {rows} rows");
        }
        anyhow::Ok(())
    }
    .await;
    fixture.close().await?;
    result
}

/// A resumed job reuses its stored bundle only while the current inputs still
/// describe it.
///
/// The candidate stores the inputs rather than the bundle, and a claim rebuilds
/// the audit from them, so a resume that paired an old bundle with today's
/// configuration would produce a coinbase the block does not commit to: the
/// claim fails its comparison before `submitblock` and retries until the job
/// expires. Both halves matter — a change must be refused, and an unchanged
/// configuration must still hit the cache, or every resume would rebuild.
#[test]
fn a_resume_reuses_a_stored_bundle_only_while_its_inputs_still_describe_it() -> Result<()> {
    let manifest_key = ManifestSigningKey::from_seed_hex(&"42".repeat(32))?;
    let ledger_key = ManifestSigningKey::from_seed_hex(&"43".repeat(32))?;
    let bundle = qbit_prism::build_audit_bundle_with_coinbase_options(
        vec![AcceptedShare {
            share_seq: 1,
            share_id: "resume:share".into(),
            miner_id: "miner-a".into(),
            order_key: "miner-a".into(),
            p2mr_program_hex: "11".repeat(32),
            share_difficulty: 100,
            network_difficulty: 100,
            template_height: 100,
            job_id: "resume".into(),
            job_issued_at_ms: 1,
            accepted_at_ms: 2,
            ntime: 1_800_000_000,
            credit_policy: None,
        }],
        FoundBlock {
            block_height: 101,
            coinbase_value_sats: 500_000_000,
            network_difficulty: 100,
            anchor_job_issued_at_ms: 3,
        },
        vec![],
        qbit_prism::PayoutPolicy::day_one_default(),
        Some("00".repeat(12)),
        vec![],
        &manifest_key,
        &ledger_key,
    )?;

    let issued = BundleInputs {
        payout_policy: bundle.payout_policy.clone(),
        ctv: None,
        signer_keys: SignerKeys::of(&manifest_key, &ledger_key),
        audit_builder_version: qbit_prism::AUDIT_BUILDER_VERSION,
    };
    ensure!(
        issued.describes(&bundle),
        "the inputs the bundle was built with did not describe it, so every resume would rebuild"
    );

    let mut rotated = issued.clone();
    rotated.signer_keys = SignerKeys::of(
        &ManifestSigningKey::from_seed_hex(&"52".repeat(32))?,
        &ManifestSigningKey::from_seed_hex(&"53".repeat(32))?,
    );
    ensure!(
        !rotated.describes(&bundle),
        "a signer rotation still described a bundle signed with the old keys"
    );

    let mut one_key_rotated = issued.clone();
    one_key_rotated.signer_keys.ledger_key_hex =
        ManifestSigningKey::from_seed_hex(&"53".repeat(32))?.public_key_hex();
    ensure!(
        !one_key_rotated.describes(&bundle),
        "rotating only the ledger key still described the stored bundle"
    );

    let mut repriced = issued.clone();
    repriced.payout_policy.target_feerate_sats_per_byte = repriced
        .payout_policy
        .target_feerate_sats_per_byte
        .wrapping_add(1);
    ensure!(
        !repriced.describes(&bundle),
        "a payout-policy change still described a bundle built under the old policy"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// The post-offer phase
// ---------------------------------------------------------------------------

/// The chain is observed again after the landing, not only before it. The
/// post-offer phase observes the chain once after the rebuild and lands at
/// that revision, then observes again before it confirms. Here another block
/// wins height 101 the moment that first observation has completed, seeing
/// this block active, and before the landing transaction: a confirmation
/// taken from the first observation would be stale. The block lands
/// (prepared, never confirmed), keeps its evidence in reconciliation with
/// the reason, and is never offered again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reorg_during_the_landing_is_seen_by_the_fresh_post_landing_observation() -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let found = fixture.found(0)?;
        let hash = found.candidate.block_hash.clone();
        let claim = fixture.enqueue_and_claim(&found).await?;
        let held = fixture
            .coordinator
            .build_slots
            .clone()
            .acquire_owned()
            .await?;
        let mut process = fixture.process(&claim);
        fixture
            .wait_for_state(&mut process, &hash, "offered")
            .await?;
        ensure!(fixture.submissions().await == 1);
        // The rebuild, and so the first post-offer observation, still waits
        // for the build slot the test holds. Arm the reorg for the end of
        // that observation: it sees this block active, the landing then runs
        // against a chain that has already moved on, and only the
        // observation after the landing can see that.
        fixture
            .node
            .lock()
            .await
            .reorg_after_next_observation(&"bb".repeat(32), 101, "ff");
        drop(held);
        tokio::time::timeout(Duration::from_secs(10), process).await???;
        ensure!(
            fixture.node.lock().await.reorg_after_observation.is_none(),
            "the reorg never fired: no chain observation completed after the offer"
        );
        let (state, token, error) = fixture.row(&hash).await?;
        ensure!(
            fixture.landed(&hash).await?,
            "the audit did not land: {state} {error:?}"
        );
        ensure!(
            state == "reconciliation" && token.is_none(),
            "the stale pre-landing observation confirmed a reorged block: {state}"
        );
        ensure!(
            error
                .as_deref()
                .is_some_and(|reason| reason.contains("not on the active chain")),
            "{error:?}"
        );
        let chain_state: String =
            sqlx::query_scalar("SELECT chain_state FROM qbit_pool_blocks WHERE block_hash=$1")
                .bind(&hash)
                .fetch_one(&fixture.coordinator.ledger.pool)
                .await?;
        ensure!(chain_state == "prepared", "{chain_state}");
        ensure!(
            fixture.coordinator.blocks.load(Ordering::Relaxed) == 0,
            "a reorged block was counted as confirmed"
        );
        ensure!(
            fixture.submissions().await == 1,
            "the block was offered again"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    fixture.close().await?;
    result
}

/// A node error after the node accepted the block (its chain cannot be
/// observed) is settled atomically: the row goes to reconciliation with the
/// error as its reason and its accepted outcome intact, the claim is
/// released, and the next attempt lands and confirms it without an offer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_error_after_acceptance_settles_the_row_in_reconciliation_with_the_reason(
) -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let found = fixture.found(0)?;
        let hash = found.candidate.block_hash.clone();
        let claim = fixture.enqueue_and_claim(&found).await?;
        fixture.node.lock().await.fail_chain_info = true;
        tokio::time::timeout(Duration::from_secs(10), fixture.process(&claim)).await???;
        ensure!(
            fixture.submissions().await == 1,
            "the block was not offered"
        );
        let (state, token, error) = fixture.row(&hash).await?;
        ensure!(
            state == "reconciliation" && token.is_none(),
            "{state} {token:?}"
        );
        let error = error.context("no reason was recorded")?;
        ensure!(
            error.contains("post-offer processing failed")
                && error.contains("node accepted the offer")
                && error.contains("chain info unavailable"),
            "{error}"
        );
        let outcome: Option<String> = sqlx::query_scalar(
            "SELECT offer_outcome FROM qbit_block_candidate_outbox WHERE block_hash=$1",
        )
        .bind(&hash)
        .fetch_one(&fixture.coordinator.ledger.pool)
        .await?;
        ensure!(outcome.as_deref() == Some("accepted"), "{outcome:?}");
        ensure!(
            !fixture.landed(&hash).await?,
            "landed without observing the chain"
        );
        // The node is back: the recovery lands and confirms without an offer.
        fixture.node.lock().await.fail_chain_info = false;
        fixture.expire(&hash).await?;
        let again = fixture
            .coordinator
            .ledger
            .claim_candidate(120)
            .await?
            .context("the reconciliation row was not claimable")?;
        ensure!(again.lifecycle.state == CandidateState::Reconciliation);
        tokio::time::timeout(Duration::from_secs(10), fixture.process(&again)).await???;
        let (state, _, _) = fixture.row(&hash).await?;
        ensure!(state == "submitted", "{state}");
        ensure!(fixture.landed(&hash).await?);
        ensure!(
            fixture.submissions().await == 1,
            "the recovery offered again"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    fixture.close().await?;
    result
}

// ---------------------------------------------------------------------------
// As-issued landing after a genuine balance divergence (#358)
// ---------------------------------------------------------------------------

/// The per-program `(gross - onchain)` deltas of a bundle's miner accounts.
fn miner_deltas(bundle: &AuditBundle) -> Vec<(String, i128)> {
    bundle
        .payout_policy_manifest
        .accounts
        .iter()
        .filter(|account| account.account_type == qbit_prism::PayoutPolicyAccountType::Miner)
        .map(|account| {
            (
                account.p2mr_program_hex.clone(),
                i128::from(account.gross_amount_sats) - i128::from(account.onchain_amount_sats),
            )
        })
        .collect()
}

fn add_deltas(balances: &mut Vec<(String, i128)>, deltas: &[(String, i128)]) {
    for (program, delta) in deltas {
        match balances.iter_mut().find(|(known, _)| known == program) {
            Some((_, balance)) => *balance += delta,
            None => balances.push((program.clone(), *delta)),
        }
    }
    balances.retain(|(_, balance)| *balance != 0);
    balances.sort();
}
/// A history the chain can produce (#358): the pool's parent block P is on
/// the node's chain and its accounting is still landing when a child job is
/// issued on top of it with the balances P has not yet moved; the child C is
/// found on a second frontend, offered and accepted while P's accounting is
/// still waiting. P then lands and confirms first, moving the canonical
/// balances, and C lands afterwards on balances that are no longer the ones
/// its coinbase commits to.
///
/// C lands as issued: its immutable audit, coinbase and payout manifest are
/// the ones its coinbase commits to, its carry rows are the manifest's
/// as-issued accounts, settlement fees included (C settles through a CTV
/// fanout, so its miner accounts carry a fee that is stored and validated
/// but never enters a balance), and the current balances are the additive
/// sum of every active block's `(gross - onchain)`, so the program whose
/// carried balance both blocks paid on chain ends in debt rather than being
/// paid a third time. The two frontends have their own build capacity, so
/// the order is controlled, never raced: P is proven confirmed, and the
/// current balances proven to hash to something other than C's reference,
/// while C's landing is still held. The integrity validator accepts both
/// marked blocks; a reorg of either block removes exactly its deltas and a
/// reactivation restores them, in either confirmation order; and a
/// corrupted amount is still a finding.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_parent_landing_late_diverges_the_child_which_lands_as_issued_with_additive_balances(
) -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let fee = FanoutFeeRatePolicy::new(1000, 12000);
    let Some(fixture) = Fixture::open_with(|config| {
        config.ctv_enabled = true;
        config.ctv_fee = Some(fee);
    })
    .await?
    else {
        return Ok(());
    };
    let result = async {
        let pool = &fixture.coordinator.ledger.pool;
        let ledger = &fixture.coordinator.ledger;
        // The carried balance one program brings into the window: miner-1's
        // program (share 1's, 0x11) has 1,000 sats carried from block 100.
        // Every block found on this window pays that balance on chain beside
        // the program's share of the reward.
        let carried = "11".repeat(32);
        sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state) VALUES($1,100,repeat('00',32),repeat('ab',32),repeat('ac',32),'confirmed')")
            .bind(PARENT.repeat(32)).execute(pool).await?;
        sqlx::query("INSERT INTO qbit_payout_carry_forward(block_height,block_hash,miner_id,payout_order_key,p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,onchain_amount_sats,carry_forward_balance_sats,action) VALUES(100,$1,'miner-1','miner-1',decode($2,'hex'),1000,0,1000,0,1000,'accrued')")
            .bind(PARENT.repeat(32)).bind(&carried).execute(pool).await?;
        let seed = vec![(carried.clone(), 1000i128)];
        ensure!(fixture.current_balances().await? == seed, "seed");

        // P: found on the window as it is, at height 101 on the parent, on
        // this frontend, whose one build slot the test holds. It is offered
        // and accepted; its landing waits.
        let issued = ledger.snapshot(100).await?;
        ensure!(
            issued.prior_balances.len() == 1,
            "the window carries one balance"
        );
        let parent = fixture.found_on(&issued, 101, &PARENT.repeat(32), 0)?;
        let parent_hash = parent.candidate.block_hash.clone();
        let parent_claim = fixture.enqueue_and_claim(&parent).await?;
        let held = fixture
            .coordinator
            .build_slots
            .clone()
            .acquire_owned()
            .await?;
        let mut parent_process = fixture.process(&parent_claim);
        fixture
            .wait_for_state(&mut parent_process, &parent_hash, "offered")
            .await?;
        ensure!(fixture.submissions().await == 1);
        {
            let node = fixture.node.lock().await;
            ensure!(
                node.tip == parent_hash && node.height == 101,
                "P is not the tip"
            );
        }

        // C: a CTV job issued on P's tip while P's accounting is still
        // landing, so its as-issued balances are the ones P has not yet
        // moved. It is found on a second frontend whose build slot the test
        // also holds, offered and accepted; its landing waits too.
        let child_snapshot = ledger.snapshot(100).await?;
        ensure!(
            child_snapshot.prior_balances == issued.prior_balances,
            "P's accounting moved the balances before it landed"
        );
        let ctv = CandidateCtv {
            direct_floor_sats: 400_000_000,
            settlement_config: fixture.coordinator.config.ctv_config,
            fanout_fee_policy: Some(fee),
        };
        let child = fixture.found_with(&child_snapshot, 102, &parent_hash, 10_000, Some(ctv))?;
        let child_hash = child.candidate.block_hash.clone();
        let miner_accounts = |bundle: &AuditBundle| {
            bundle
                .payout_policy_manifest
                .accounts
                .iter()
                .filter(|account| {
                    account.account_type == qbit_prism::PayoutPolicyAccountType::Miner
                })
                .cloned()
                .collect::<Vec<_>>()
        };
        let child_fees: Vec<(String, u64)> = miner_accounts(&child.bundle)
            .iter()
            .map(|account| {
                (
                    account.p2mr_program_hex.clone(),
                    account.settlement_fee_sats,
                )
            })
            .collect();
        ensure!(
            child_fees.iter().all(|(_, fee)| *fee > 0),
            "C's manifest carries no settlement fee, so this case proves nothing about fees: {child_fees:?}"
        );
        let second = fixture
            .second_frontend("window-switch-second", &parent_hash)
            .await?;
        let second_held = second.build_slots.clone().acquire_owned().await?;
        let child_claim = fixture.enqueue_and_claim(&child).await?;
        let mut child_process = {
            let second = second.clone();
            let claim = child_claim.clone();
            tokio::spawn(async move { second.process_candidate(&claim).await })
        };
        fixture
            .wait_for_state(&mut child_process, &child_hash, "offered")
            .await?;
        ensure!(fixture.submissions().await == 2);
        {
            let node = fixture.node.lock().await;
            ensure!(
                node.tip == child_hash && node.height == 102,
                "C is not the tip"
            );
            ensure!(
                node.blocks.get(&101) == Some(&parent_hash),
                "P left the chain"
            );
        }
        // The boundary before P lands: the current balances are still the
        // set C's coinbase commits to.
        ensure!(
            fixture.current_digest().await? == child.candidate.window.prior_balances_digest,
            "the balances moved before P landed"
        );

        // P's build slot returns: P lands and confirms in its own attempt,
        // moving the balances, while C's landing is still held.
        drop(held);
        tokio::time::timeout(Duration::from_secs(20), parent_process).await???;
        let (state, _, error) = fixture.row(&parent_hash).await?;
        ensure!(state == "submitted", "P finished as {state}: {error:?}");
        ensure!(fixture.landed(&parent_hash).await?);
        let mut expected = seed.clone();
        add_deltas(&mut expected, &miner_deltas(&parent.bundle));
        let after_parent = expected.clone();
        ensure!(
            fixture.current_balances().await? == after_parent,
            "P's confirmation did not move the balances to {after_parent:?}"
        );
        // The boundary before C lands: the current balances no longer hash
        // to C's reference, and C, still offered, has landed nothing.
        ensure!(
            fixture.current_digest().await? != child.candidate.window.prior_balances_digest,
            "P's confirmation left C's reference current, so this case proves nothing"
        );
        ensure!(
            !child_process.is_finished() && !fixture.landed(&child_hash).await?,
            "C landed before its build slot was released"
        );
        let (state, _, _) = fixture.row(&child_hash).await?;
        ensure!(state == "offered", "C is {state} before its landing");

        // C's build slot returns: C lands as issued, on balances that are
        // not its own, in its own attempt, and confirms.
        drop(second_held);
        tokio::time::timeout(Duration::from_secs(20), child_process).await???;
        let (state, _, error) = fixture.row(&child_hash).await?;
        ensure!(state == "submitted", "C finished as {state}: {error:?}");
        ensure!(fixture.submissions().await == 2, "a block was offered twice");
        add_deltas(&mut expected, &miner_deltas(&child.bundle));
        let current = fixture.current_balances().await?;
        ensure!(
            current == expected,
            "current balances {current:?} are not the additive sum {expected:?}"
        );
        let carried_now = current
            .iter()
            .find(|(program, _)| *program == carried)
            .map(|(_, balance)| *balance)
            .context("the carried program vanished")?;
        ensure!(
            carried_now < 0,
            "the program paid its carried balance on chain twice must be in debt, not {carried_now}"
        );
        eprintln!(
            "#358 divergence: seed {seed:?}; after P {after_parent:?}; after C {current:?}; C settlement fees {child_fees:?}"
        );

        // C's evidence is as issued: the audit the block commits to, its
        // coinbase, its payout manifest, and carry rows equal to the
        // manifest's accounts, fees included.
        let report = qbit_prism::verify_audit_bundle_with_ledger_public_key(
            &child.bundle,
            &fixture.coordinator.config.ledger_public_key,
        )?;
        let (audit_sha, coinbase, marker): (String, String, Option<String>) = sqlx::query_as(
            "SELECT a.audit_bundle_sha256,a.coinbase_tx_hex,b.as_issued_audit_sha256 FROM qbit_pool_audit_bundles a JOIN qbit_pool_blocks b USING(block_hash) WHERE a.block_hash=$1",
        )
        .bind(&child_hash)
        .fetch_one(pool)
        .await?;
        ensure!(
            audit_sha == report.audit_bundle_sha256_hex,
            "C's audit is not the issued one"
        );
        ensure!(
            coinbase == report.coinbase_tx_hex,
            "C's coinbase is not the issued one"
        );
        ensure!(
            marker.as_deref() == Some(audit_sha.as_str()),
            "C is not marked as issued"
        );
        let stored_manifest: Value = sqlx::query_scalar(
            "SELECT audit_bundle->'payout_policy_manifest' FROM qbit_pool_audit_bundles WHERE block_hash=$1",
        )
        .bind(&child_hash)
        .fetch_one(pool)
        .await?;
        ensure!(
            stored_manifest == serde_json::to_value(&child.bundle.payout_policy_manifest)?,
            "C's stored payout manifest is not the issued one"
        );
        for account in miner_accounts(&child.bundle) {
            let row: (String, i64, String, i64, i64, String) = sqlx::query_as(
                "SELECT prior_balance_sats::text,gross_amount_sats,candidate_balance_sats::text,onchain_amount_sats,settlement_fee_sats,carry_forward_balance_sats::text FROM qbit_payout_carry_forward WHERE block_hash=$1 AND p2mr_program=decode($2,'hex')",
            )
            .bind(&child_hash)
            .bind(&account.p2mr_program_hex)
            .fetch_one(pool)
            .await?;
            ensure!(
                row == (
                    account.prior_balance_sats.to_string(),
                    i64::try_from(account.gross_amount_sats)?,
                    account.candidate_balance_sats.to_string(),
                    i64::try_from(account.onchain_amount_sats)?,
                    i64::try_from(account.settlement_fee_sats)?,
                    account.carry_forward_balance_sats.to_string(),
                ),
                "C's carry row for {} is not the issued account: {row:?}",
                account.p2mr_program_hex
            );
        }
        ensure!(
            fixture.integrity().await? == (0, 0),
            "{:?}",
            fixture.integrity().await?
        );

        // A reorg of either block removes exactly its deltas, and its
        // reactivation restores them, whatever order the two are confirmed
        // in: the balances are the sum of the active deltas, and the
        // validator checks each marked block against its own manifest.
        let observe = |hash: &str, active: bool| {
            let hash = hash.to_owned();
            async move {
                let revision = ledger.payout_revision().await?;
                ledger
                    .reconcile_blocks_at_revision(
                        &[BlockObservation {
                            block_hash: hash,
                            active,
                        }],
                        102,
                        revision,
                    )
                    .await?;
                Ok::<_, anyhow::Error>(())
            }
        };
        let mut child_only = seed.clone();
        add_deltas(&mut child_only, &miner_deltas(&child.bundle));
        for (label, hash, active, balances) in [
            ("C inactive", &child_hash, false, &after_parent),
            ("C active again", &child_hash, true, &expected),
            ("P inactive under C", &parent_hash, false, &child_only),
            ("P active again, confirmed after C", &parent_hash, true, &expected),
        ] {
            observe(hash, active).await?;
            let current = fixture.current_balances().await?;
            ensure!(
                current == *balances,
                "{label}: balances {current:?}, not {balances:?}"
            );
            ensure!(
                fixture.integrity().await? == (0, 0),
                "{label}: {:?}",
                fixture.integrity().await?
            );
        }

        // A corrupted marked amount is still a finding.
        sqlx::query("UPDATE qbit_payout_carry_forward SET onchain_amount_sats=onchain_amount_sats+1 WHERE block_hash=$1 AND p2mr_program=decode($2,'hex')")
            .bind(&child_hash).bind(&carried).execute(pool).await?;
        let (mismatches, drift) = fixture.integrity().await?;
        ensure!(
            mismatches == 1 && drift == 0,
            "mismatches {mismatches} drift {drift}"
        );
        let reason: String = sqlx::query_scalar(
            "SELECT mismatch_reason FROM qbit_carry_forward_integrity_mismatches()",
        )
        .fetch_one(pool)
        .await?;
        ensure!(
            reason.contains("onchain_amount") && reason.contains("carry_arithmetic"),
            "{reason}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    fixture.close().await?;
    result
}
