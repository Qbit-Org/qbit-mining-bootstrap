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
use qbit_prism_test_gate as gate;
use sqlx::PgPool;
use tokio::task::JoinHandle;

/// The ledger's advisory locks are cluster-wide constants, not schema-scoped.
static TEST_LOCK: Mutex<()> = Mutex::const_new(());

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
        "getblockhash" => json!(node.tip),
        "getblockchaininfo" => json!({"chain":"test","initialblockdownload":false,
            "blocks":node.height,"headers":node.height,"bestblockhash":node.tip,"chainwork":node.chainwork}),
        "getbestblockhash" => json!(node.tip),
        "getnetworkinfo" => json!({"connections":2}),
        "submitblock" => {
            let block = hex::decode(request["params"][0].as_str().unwrap()).unwrap();
            node.submissions += 1;
            if node.accept {
                node.tip = codec::hash_display(&codec::double_sha256(&block[..80]));
                node.height = 101;
                node.chainwork = "02".into();
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
            node,
            server,
            snapshot,
        }))
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
        let (manifest_key, ledger_key) = self.keys();
        let bundle = qbit_prism::build_audit_bundle_with_coinbase_options(
            self.snapshot.shares.clone(),
            FoundBlock {
                block_height: 101,
                coinbase_value_sats: 500_000_000,
                network_difficulty: 100,
                anchor_job_issued_at_ms: self.snapshot.anchor_ms,
            },
            self.snapshot.prior_balances.clone(),
            qbit_prism::PayoutPolicy::day_one_default(),
            Some("00".repeat(12)),
            vec![],
            &manifest_key,
            &ledger_key,
        )?;
        let template = json!({"version":0x20000000u32,"bits":"207fffff","curtime":1_800_000_000u32,
            "previousblockhash":PARENT.repeat(32),"transactions":[]});
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
                payout_revision: self.snapshot.payout_revision,
                window: WindowRef::from_snapshot(&self.snapshot)?,
                bootstrap_share: None,
                found_block: bundle.found_block.clone(),
                payout_policy: qbit_prism::PayoutPolicy::day_one_default(),
                ctv: None,
                audit_builder_version: qbit_prism::AUDIT_BUILDER_VERSION,
                signer_keys: SignerKeys::of(&manifest_key, &ledger_key),
                leased: false,
                coinbase_suffix_hex: "00".repeat(12),
                deferred_share: None,
                block_bytes,
                as_issued_balances: Vec::new(),
            },
            bundle,
        })
    }

    /// A CTV candidate found on the fixture's window with the stored
    /// settlement inputs `ctv`, which need not be this frontend's
    /// configuration: they are what the block's coinbase commits to.
    fn found_ctv(&self, nonce_start: u32, ctv: CandidateCtv) -> Result<Found> {
        let (manifest_key, ledger_key) = self.keys();
        let bundle = self.ctv_bundle(&ctv)?;
        let template = json!({"version":0x20000000u32,"bits":"207fffff","curtime":1_800_000_000u32,
            "previousblockhash":PARENT.repeat(32),"transactions":[]});
        let job = codec::Job::from_manifest(
            "window-switch-ctv".into(),
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
                payout_revision: self.snapshot.payout_revision,
                window: WindowRef::from_snapshot(&self.snapshot)?,
                bootstrap_share: None,
                found_block: bundle.found_block.clone(),
                payout_policy: qbit_prism::PayoutPolicy::day_one_default(),
                ctv: Some(ctv),
                audit_builder_version: qbit_prism::AUDIT_BUILDER_VERSION,
                signer_keys: SignerKeys::of(&manifest_key, &ledger_key),
                leased: false,
                coinbase_suffix_hex: "00".repeat(12),
                deferred_share: None,
                block_bytes,
                as_issued_balances: Vec::new(),
            },
            bundle,
        })
    }

    /// The CTV bundle for the fixture's window under the settlement inputs
    /// `ctv`, as the building frontend made it.
    fn ctv_bundle(&self, ctv: &CandidateCtv) -> Result<AuditBundle> {
        let (manifest_key, ledger_key) = self.keys();
        Ok(qbit_prism::build_audit_bundle_with_ctv_settlement_options(
            self.snapshot.shares.clone(),
            FoundBlock {
                block_height: 101,
                coinbase_value_sats: 500_000_000,
                network_difficulty: 100,
                anchor_job_issued_at_ms: self.snapshot.anchor_ms,
            },
            self.snapshot.prior_balances.clone(),
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

    async fn close(self) -> Result<()> {
        self.server.abort();
        self.coordinator.ledger.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await?;
        self.admin.close().await;
        Ok(())
    }
}

/// A retried row: pending, released, with a reason.
async fn assert_retried(fixture: &Fixture, block_hash: &str, reason: &str) -> Result<()> {
    let (state, token, error) = fixture.row(block_hash).await?;
    ensure!(state == "pending", "the row is {state}, not pending");
    ensure!(token.is_none(), "the claim was not released for retry");
    let error = error.context("no retry reason was recorded")?;
    ensure!(
        error.to_ascii_lowercase().contains(reason),
        "retry reason {error:?} does not mention {reason:?}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Stored inputs
// ---------------------------------------------------------------------------

/// A stored builder version or signer pair that is not this binary's is
/// retried with an alert naming both values, and never rebuilt: the build
/// slot is held by the test for the whole attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn builder_version_or_signer_mismatch_retries_without_a_rebuild() -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
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
            assert_retried(&fixture, hash, "not rebuilding").await?;
            ensure!(
                !fixture.landed(hash).await?,
                "a mismatched candidate landed"
            );
        }
        ensure!(
            fixture.submissions().await == 0,
            "a mismatched candidate was submitted"
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

/// A claim whose audit already landed authenticates the row against its
/// block, skips the rebuild and landing (the build slot is held by the test),
/// and still observes, renews and submits before finishing. A forged landed
/// row is refused and the candidate stays recoverable.
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
                "forged coinbase" => Some(
                    "UPDATE qbit_pool_audit_bundles SET coinbase_tx_hex=overlay(coinbase_tx_hex placing '0' from length(coinbase_tx_hex)-8 for 1) WHERE block_hash=$1",
                ),
                "forged audit root" => Some(
                    "UPDATE qbit_pool_audit_bundles SET audit_commitment_leaves_hex=to_jsonb(ARRAY[repeat('ab',32)]) WHERE block_hash=$1",
                ),
                _ => None,
            };
            if let Some(statement) = forgery {
                sqlx::query(statement)
                    .bind(&hash)
                    .execute(&fixture.coordinator.ledger.pool)
                    .await?;
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
            if forgery.is_none() {
                outcome?;
                ensure!(fixture.submissions().await == 1, "the landed block was not submitted");
                ensure!(state == "submitted", "the recovered claim finished as {state}");
                ensure!(fixture.coordinator.blocks.load(Ordering::Relaxed) == 1);
            } else {
                let error = outcome.err().context("the forgery was accepted")?;
                ensure!(
                    format!("{error:#}").contains("does not authenticate"),
                    "{error:#}"
                );
                ensure!(state == "pending", "the forgery finished the candidate as {state}");
                ensure!(fixture.submissions().await == 0, "the forged block was submitted");
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

/// `Incomplete`, `SnapshotDigestMismatch`, `PriorBalancesChanged` on a block
/// that is still current, and the rebuild deadline all fail the attempt
/// through `retry_candidate` with an alert: the row stays pending and
/// released, nothing lands and nothing is submitted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn window_read_failures_retry_with_an_alert_and_never_abandon() -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
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
            (&balances, "prior balances changed"),
        ] {
            let claim = fixture.enqueue_and_claim(found).await?;
            tokio::time::timeout(Duration::from_secs(10), fixture.process(&claim)).await???;
            assert_retried(&fixture, &found.candidate.block_hash, reason).await?;
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
        assert_retried(&fixture, &found.candidate.block_hash, "deadline").await?;
        ensure!(
            fixture.coordinator.build_slots.available_permits() == 1,
            "the expired attempt kept its build slot"
        );
        drop(held);
        ensure!(
            fixture.submissions().await == 0,
            "a failed rebuild was submitted"
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
        (
            WindowError::BalanceSnapshotMissing { digest: [2; 32] },
            "balance snapshot",
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
            state == "pending",
            "the terminal update did not wait for ORDER_LOCK"
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

/// A `leased` candidate submits its stored block before anything else, then
/// rebuilds from its as-issued balances and lands before it takes any
/// terminal outcome: an accepted block finishes submitted, a rejected one
/// abandoned with the node's reason, and a missing as-issued snapshot keeps
/// it pending through `retry_candidate`.
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
                if case != "accepted" {
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
            // Nothing is landed or decided while the node holds the block.
            ensure!(
                !fixture.landed(&hash).await?,
                "{case}: landed before submitblock"
            );
            let (state, token, _) = fixture.row(&hash).await?;
            ensure!(
                state == "pending" && token.is_some(),
                "{case}: a terminal outcome or release before submitblock"
            );
            gate.release.notify_one();
            tokio::time::timeout(Duration::from_secs(10), process).await???;
            ensure!(fixture.submissions().await == 1);
            let (state, token, error) = fixture.row(&hash).await?;
            match case {
                "accepted" => {
                    ensure!(
                        fixture.landed(&hash).await?,
                        "the accepted block did not land"
                    );
                    ensure!(state == "submitted", "accepted block finished as {state}");
                }
                "rejected" => {
                    ensure!(
                        fixture.landed(&hash).await?,
                        "the rejected block did not land"
                    );
                    ensure!(state == "abandoned", "rejected block finished as {state}");
                    ensure!(error.as_deref() == Some("duplicate"), "{error:?}");
                }
                _ => {
                    ensure!(
                        !fixture.landed(&hash).await?,
                        "landed without its balance snapshot"
                    );
                    ensure!(state == "pending" && token.is_none(), "{state} {token:?}");
                    ensure!(
                        error
                            .as_deref()
                            .is_some_and(|e| e.contains("balance snapshot")),
                        "{error:?}"
                    );
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
