//! Real PostgreSQL ownership tests with short leases and deliberately blocked
//! work, in the offer-before-landing order (#266): a pending candidate is
//! reserved and offered before any builder admission, the landing follows,
//! and an offered row is never offered again by any frontend.
use super::*;
use axum::{extract::State, routing::post, Json, Router};
use qbit_prism_test_gate as gate;
use sqlx::PgPool;
use tokio::task::JoinHandle;

#[path = "candidate_checkout_tests.rs"]
mod checkout_tests;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn landing_transaction_renews_across_expiries_and_terminal_contention_finishes_once(
) -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    for phase in ["landing", "terminal"] {
        let Some(fixture) = Fixture::open().await? else {
            return Ok(());
        };
        let result=async {
            let target=if phase=="landing" {"BEFORE INSERT ON qbit_pool_blocks FOR EACH ROW"}
                else {"BEFORE UPDATE OF state ON qbit_block_candidate_outbox FOR EACH ROW WHEN (NEW.state <> OLD.state)"};
            sqlx::raw_sql(&format!("CREATE FUNCTION pause_candidate_phase() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(hashtext(TG_TABLE_SCHEMA)::bigint); RETURN NEW; END $$; CREATE TRIGGER pause_candidate_phase {target} EXECUTE FUNCTION pause_candidate_phase();"))
                .execute(&fixture.coordinator.ledger.pool).await?;
            let mut gate=fixture.successor.pool.begin().await?;
            sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1)::bigint)").bind(&fixture.schema).execute(&mut *gate).await?;
            let gate_pid:i32=sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut *gate).await?;
            let process=fixture.process(SHORT_LEASE);
            tokio::time::timeout(Duration::from_secs(3),async {
                loop {
                    let blocked:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))")
                        .bind(gate_pid).fetch_one(&fixture.admin).await?;
                    if blocked {return Ok::<_,anyhow::Error>(());}
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }).await??;
            tokio::time::sleep(Duration::from_millis(if phase=="landing" {2200} else {550})).await;
            ensure!(!process.is_finished(),"{phase} self-contention canceled a valid attempt");
            ensure!(fixture.successor.claim_candidate(10).await?.is_none(),"{phase} ownership was stolen");
            if phase=="landing" {
                let live:bool=sqlx::query_scalar("SELECT claim_expires_at>clock_timestamp() FROM qbit_block_candidate_outbox WHERE block_hash=$1")
                    .bind(&fixture.claim.candidate.block_hash).fetch_one(&fixture.coordinator.ledger.pool).await?;
                ensure!(live,"long landing transaction prevented lease renewal");
            }
            gate.commit().await?;
            tokio::time::timeout(Duration::from_secs(5),process).await???;
            ensure!(fixture.state().await?=="submitted");
            ensure!(fixture.node.lock().await.submissions==1);
            Ok::<_,anyhow::Error>(())
        }.await;
        fixture.close().await?;
        result?;
    }
    Ok(())
}

const SHORT_LEASE: CandidateLease = CandidateLease {
    seconds: 1,
    interval: Duration::from_millis(100),
    timeout: Duration::from_millis(300),
    rebuild_deadline: Duration::from_secs(60),
};

// Deliberately long ledger transactions share advisory locks across schemas.
use super::test_serial::TEST_LOCK;

#[derive(Default)]
struct ReplyGate {
    entered: Notify,
    release: Notify,
}

struct NodeState {
    tip: String,
    height: u64,
    chainwork: String,
    network_calls: usize,
    pause_network: Option<usize>,
    gate: Arc<ReplyGate>,
    submissions: usize,
}

async fn node_reply(
    State(node): State<Arc<Mutex<NodeState>>>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let mut node = node.lock().await;
    let mut gate = None;
    let result = match request["method"].as_str().unwrap() {
        "getblockhash" if request["params"][0] == 0 => json!("00".repeat(32)),
        "getblockhash" => json!(node.tip),
        "getblockchaininfo" => json!({"chain":"test","initialblockdownload":false,
            "blocks":node.height,"headers":node.height,"bestblockhash":node.tip,"chainwork":node.chainwork}),
        "getbestblockhash" => json!(node.tip),
        "getnetworkinfo" => {
            node.network_calls += 1;
            if node.pause_network == Some(node.network_calls) {
                node.pause_network = None;
                gate = Some(node.gate.clone());
            }
            json!({"connections":2})
        }
        "submitblock" => {
            let block = hex::decode(request["params"][0].as_str().unwrap()).unwrap();
            node.tip = codec::hash_display(&codec::double_sha256(&block[..80]));
            node.height = 101;
            node.chainwork = "02".into();
            node.submissions += 1;
            Value::Null
        }
        method => panic!("unexpected candidate RPC {method}"),
    };
    drop(node);
    if let Some(gate) = gate {
        gate.entered.notify_one();
        gate.release.notified().await;
    }
    Json(json!({"id":request["id"],"result":result,"error":null}))
}

/// The lifecycle columns of the fixture's outbox row.
struct Row {
    state: String,
    token: Option<String>,
    last_error: Option<String>,
    outcome: Option<String>,
    offered_at_ms: Option<i64>,
    reserved_by: Option<String>,
    /// The document, the block bytes and the window reference are all present.
    evidence: bool,
}

/// The number of samples the first-offer histogram holds, `0` while it is
/// declared without any.
fn first_offer_samples(metrics: &crate::metrics::Metrics) -> u64 {
    metrics
        .render()
        .lines()
        .find_map(|line| line.strip_prefix("qbit_prism_block_submit_seconds_count "))
        .and_then(|value| value.trim().parse::<f64>().ok())
        .map_or(0, |value| value as u64)
}

struct Fixture {
    admin: PgPool,
    schema: String,
    /// The frontend that claims first: `candidate-owner`.
    coordinator: Arc<Coordinator>,
    /// A second frontend on the same database and node: `candidate-successor`,
    /// with its own build capacity and its own metrics registry.
    successor_coordinator: Arc<Coordinator>,
    successor: Ledger,
    node: Arc<Mutex<NodeState>>,
    server: JoinHandle<()>,
    claim: CandidateClaim,
    snapshot: Snapshot,
}

impl Fixture {
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let admin = PgPool::connect(&raw).await?;
        let schema = format!("prism_candidate_lease_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut url = url::Url::parse(&raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        let node = Arc::new(Mutex::new(NodeState {
            tip: "aa".repeat(32),
            height: 100,
            chainwork: "01".into(),
            network_calls: 0,
            pause_network: None,
            gate: Arc::new(ReplyGate::default()),
            submissions: 0,
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let rpc_url = format!("http://{}/", listener.local_addr()?);
        let app = Router::new()
            .route("/", post(node_reply))
            .with_state(node.clone());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let config = |instance_id: &str| Config {
            database_url: url.to_string(),
            instance_id: instance_id.into(),
            database_connections: 6,
            initialize_schema: true,
            chain: "testnet".into(),
            expected_genesis_hash: None,
            min_peers: 1,
            template_max_age: Duration::from_secs(120),
            submit_tip_max_age: Duration::from_secs(10),
            template_refresh_failure_exit: Duration::from_secs(120),
            rpc_url: rpc_url.clone(),
            rpc_user: "test".into(),
            rpc_password: "test".into(),
            rpc_timeout: Duration::from_secs(5),
            block_submit_timeout: Duration::from_secs(1),
            poll_interval: Duration::from_secs(1),
            blockwait: false,
            build_workers: 1,
            refresh_build_threads: None,
            runtime_workers: 2,
            snapshot_interval: Duration::from_secs(60),
            health_timeout: Duration::from_secs(15),
            share_commit_timeout: Duration::from_secs(15),
            share_commit_grace: Duration::from_secs(5),
            block_only_ack_timeout: Duration::from_secs(60),
            candidate_orphan_confirmations: 6,
            capture_overpay_ceiling_bps: 100,
            extranonce2_size: 8,
            coinbase_tag: "/PRISM/".into(),
            manifest_seed: "11".repeat(32),
            ledger_seed: "22".repeat(32),
            ledger_public_key: ManifestSigningKey::from_seed_hex(&"22".repeat(32))
                .unwrap()
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
        let coordinator = Coordinator::new(
            config("candidate-owner"),
            std::sync::Arc::new(crate::metrics::Metrics::default()),
        )
        .await?;
        let successor_coordinator = Coordinator::new(
            config("candidate-successor"),
            std::sync::Arc::new(crate::metrics::Metrics::default()),
        )
        .await?;
        let successor = (*successor_coordinator.ledger).clone();
        coordinator
            .ledger
            .observe_chain_view(&"aa".repeat(32), 100, "01")
            .await?;
        for frontend in [&coordinator, &successor_coordinator] {
            *frontend.observed_tip.write().await = TipState::baseline("aa".repeat(32));
        }
        coordinator
            .ledger
            .append(
                AcceptedShare {
                    share_seq: 0,
                    share_id: format!("miner:{}", "11".repeat(32)),
                    miner_id: "miner".into(),
                    order_key: "miner".into(),
                    p2mr_program_hex: "11".repeat(32),
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
        let snapshot = coordinator.ledger.snapshot(100).await?;
        // The fixture's own candidate, enqueued with a proof time as the
        // share path enqueues it, and claimed by the owner.
        coordinator
            .ledger
            .enqueue_candidate_observed(found_on(&snapshot, 0)?, Some(unix_ms_now()?))
            .await?;
        let claim = coordinator
            .ledger
            .claim_candidate(10)
            .await?
            .context("candidate claim missing")?;
        Ok(Some(Self {
            admin,
            schema,
            coordinator,
            successor_coordinator,
            successor,
            node,
            server,
            claim,
            snapshot,
        }))
    }

    /// A block found on the fixture's window with the owner's keys, so a
    /// rebuild reproduces exactly the audit it was found with. `nonce_start`
    /// distinguishes candidates.
    fn found(&self, nonce_start: u32) -> Result<Candidate> {
        found_on(&self.snapshot, nonce_start)
    }

    fn process(&self, lease: CandidateLease) -> JoinHandle<Result<()>> {
        let coordinator = self.coordinator.clone();
        let claim = self.claim.clone();
        tokio::spawn(async move {
            coordinator
                .process_candidate_with_lease(&claim, lease)
                .await
        })
    }
}

/// See [`Fixture::found`].
fn found_on(snapshot: &Snapshot, nonce_start: u32) -> Result<Candidate> {
    {
        let manifest_key = ManifestSigningKey::from_seed_hex(&"11".repeat(32))?;
        let ledger_key = ManifestSigningKey::from_seed_hex(&"22".repeat(32))?;
        let bundle = qbit_prism::build_audit_bundle_with_coinbase_options(
            snapshot.shares.clone(),
            FoundBlock {
                block_height: 101,
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
        )?;
        let template = json!({"version":0x20000000u32,"bits":"207fffff","curtime":1_800_000_000u32,
            "previousblockhash":"aa".repeat(32),"transactions":[]});
        let job = codec::Job::from_manifest(
            "lease-test".into(),
            &template,
            &bundle.signed_coinbase_manifest.manifest,
            "00000000",
            8,
            1e-12,
            0.0,
            true,
        )?;
        let proof = (nonce_start..nonce_start + 10_000u32)
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
        Ok(Candidate {
            block_hash: proof.block_hash_hex,
            block_sha256: Candidate::block_digest_hex(&block_bytes),
            job_id: job.job_id,
            payout_revision: snapshot.payout_revision,
            window: WindowRef::from_snapshot(snapshot)?,
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
            as_issued_balances: snapshot.prior_balances.clone(),
        })
    }
}

impl Fixture {
    async fn state(&self) -> Result<String> {
        Ok(self.row().await?.state)
    }

    async fn row(&self) -> Result<Row> {
        self.row_of(&self.claim.candidate.block_hash).await
    }

    async fn row_of(&self, block_hash: &str) -> Result<Row> {
        let (state, token, last_error, outcome, offered_at_ms, reserved_by, evidence) =
            sqlx::query_as::<_, (String, Option<String>, Option<String>, Option<String>, Option<i64>, Option<String>, bool)>(
                "SELECT state,claim_token,last_error,offer_outcome,offered_at_ms,offer_reserved_by,candidate IS NOT NULL AND block_bytes IS NOT NULL AND window_anchor_ms IS NOT NULL FROM qbit_block_candidate_outbox WHERE block_hash=$1",
            )
            .bind(block_hash)
            .fetch_one(&self.coordinator.ledger.pool)
            .await?;
        Ok(Row {
            state,
            token,
            last_error,
            outcome,
            offered_at_ms,
            reserved_by,
            evidence,
        })
    }

    async fn landed(&self) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM qbit_pool_audit_bundles WHERE block_hash=$1)",
        )
        .bind(&self.claim.candidate.block_hash)
        .fetch_one(&self.coordinator.ledger.pool)
        .await?)
    }

    async fn submissions(&self) -> usize {
        self.node.lock().await.submissions
    }

    /// Wait until the owner's attempt has offered the block: the row is
    /// `offered` and the node saw one `submitblock`.
    async fn wait_for_offer(&self) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if self.state().await? == "offered" && self.submissions().await == 1 {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("the candidate was not offered before its landing")??;
        Ok(())
    }

    async fn wait_for_renewal(&self) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(2),async {
            loop {
                let renewed:bool=sqlx::query_scalar("SELECT claim_expires_at<clock_timestamp()+interval '2 seconds' FROM qbit_block_candidate_outbox WHERE block_hash=$1")
                    .bind(&self.claim.candidate.block_hash).fetch_one(&self.coordinator.ledger.pool).await?;
                if renewed {return Ok::<_,anyhow::Error>(());}
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await??;
        Ok(())
    }

    async fn expire(&self) -> Result<()> {
        self.expire_hash(&self.claim.candidate.block_hash).await
    }

    async fn expire_hash(&self, block_hash: &str) -> Result<()> {
        sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_expires_at=clock_timestamp()-interval '1 second',next_attempt_at=clock_timestamp() WHERE block_hash=$1")
            .bind(block_hash).execute(&self.coordinator.ledger.pool).await?;
        Ok(())
    }

    /// Make the fixture's block the node's active tip without any offer, as
    /// a competing frontend's or an earlier, unrecorded offer would have.
    async fn activate_on_node(&self) {
        let mut node = self.node.lock().await;
        node.tip = self.claim.candidate.block_hash.clone();
        node.height = 101;
        node.chainwork = "02".into();
    }

    async fn close(self) -> Result<()> {
        self.server.abort();
        self.coordinator.ledger.pool.close().await;
        self.successor.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await?;
        self.admin.close().await;
        Ok(())
    }
}

/// With every build slot taken, the block is offered anyway: the offer needs
/// no builder admission. The lease is kept for as long as the landing waits,
/// no other frontend can take the row, and once capacity frees the same
/// attempt lands and confirms with exactly one `submitblock` and exactly
/// one proof-to-first-offer sample.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn saturated_build_capacity_keeps_the_lease_until_one_candidate_confirms() -> Result<()> {
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
        let process = fixture.process(SHORT_LEASE);
        fixture.wait_for_offer().await?;
        fixture.wait_for_renewal().await?;
        tokio::time::sleep(Duration::from_millis(2200)).await;
        ensure!(
            !process.is_finished(),
            "the landing finished without build capacity"
        );
        ensure!(
            fixture.successor.claim_candidate(10).await?.is_none(),
            "another frontend stole work waiting for build capacity"
        );
        ensure!(
            fixture.submissions().await == 1,
            "the block was not offered while waiting for build capacity"
        );
        ensure!(!fixture.landed().await?, "landed without build capacity");
        let row = fixture.row().await?;
        ensure!(row.state == "offered" && row.evidence, "{}", row.state);
        ensure!(row.outcome.as_deref() == Some("accepted"));
        ensure!(
            row.offered_at_ms.is_some(),
            "the call time was not recorded"
        );
        ensure!(row.reserved_by.as_deref() == Some("candidate-owner"));
        drop(held);
        tokio::time::timeout(Duration::from_secs(5), process).await???;
        ensure!(fixture.state().await? == "submitted");
        ensure!(fixture.landed().await?);
        ensure!(
            fixture.submissions().await == 1,
            "candidate was not submitted exactly once"
        );
        ensure!(
            first_offer_samples(&fixture.coordinator.metrics) == 1,
            "the normal path did not record exactly one first-offer sample"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    fixture.close().await?;
    result
}

/// The work queued on build capacity is now the post-offer landing. An
/// attempt that is aborted, taken over or blocked on its renewal while it
/// waits must not land, must release the build slot, and must leave the
/// offered row for a successor that lands it without a second offer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn canceled_or_lost_renewal_cannot_resume_queued_work() -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    for failure in ["abort", "takeover", "timeout"] {
        let Some(fixture) = Fixture::open().await? else {
            return Ok(());
        };
        let result=async {
            let held=fixture.coordinator.build_slots.clone().acquire_owned().await?;
            let process=fixture.process(SHORT_LEASE);
            fixture.wait_for_offer().await?;
            fixture.wait_for_renewal().await?;
            let mut taken=None;
            match failure {
                "abort" => {
                    process.abort();
                    ensure!(process.await.unwrap_err().is_cancelled());
                    // No detached heartbeat may renew after cancellation.
                    tokio::time::sleep(Duration::from_millis(1100)).await;
                }
                "takeover" => {
                    fixture.expire().await?;
                    let successor=fixture.successor.claim_candidate(10).await?.context("successor could not claim expired work")?;
                    ensure!(successor.claim_token!=fixture.claim.claim_token);
                    ensure!(successor.lifecycle.state==CandidateState::Offered,"{:?}",successor.lifecycle.state);
                    let error=tokio::time::timeout(Duration::from_secs(2),process).await??.unwrap_err();
                    ensure!(error.to_string().contains("lease renewal"),"{error}");
                    taken=Some(successor);
                }
                _ => {
                    let mut lock=fixture.successor.pool.begin().await?;
                    sqlx::query("SELECT block_hash FROM qbit_block_candidate_outbox WHERE block_hash=$1 FOR UPDATE")
                        .bind(&fixture.claim.candidate.block_hash).fetch_one(&mut *lock).await?;
                    let error=tokio::time::timeout(Duration::from_secs(2),process).await??.unwrap_err();
                    ensure!(error.to_string().contains("lease"),"{error}");
                    lock.rollback().await?;
                    fixture.expire().await?;
                }
            }
            drop(held);
            tokio::time::sleep(Duration::from_millis(150)).await;
            ensure!(!fixture.landed().await?,"{failure}: canceled queued work landed");
            ensure!(fixture.submissions().await==1,"{failure}: the offer was repeated or missing");
            ensure!(fixture.coordinator.build_slots.available_permits()==1,"{failure}: canceled waiter retained build capacity");
            let row=fixture.row().await?;
            ensure!(row.state=="offered" && row.evidence,"{failure}: the offered row lost its state or evidence: {}",row.state);
            let successor=match taken {
                Some(successor)=>successor,
                None=>{
                    sqlx::query("UPDATE qbit_block_candidate_outbox SET next_attempt_at=clock_timestamp() WHERE block_hash=$1")
                        .bind(&fixture.claim.candidate.block_hash).execute(&fixture.coordinator.ledger.pool).await?;
                    fixture.successor.claim_candidate(10).await?.context("canceled owner kept its lease alive")?
                }
            };
            fixture.coordinator.process_candidate(&successor).await?;
            ensure!(fixture.state().await?=="submitted");
            ensure!(fixture.landed().await?);
            ensure!(fixture.submissions().await==1,"{failure}: the recovery offered the block again");
            Ok::<_,anyhow::Error>(())
        }.await;
        fixture.close().await?;
        result?;
    }
    Ok(())
}

/// The submit loop offers a claimed block before it waits for build
/// capacity; a shutdown drops the waiting landing, releases the capacity,
/// and leaves the offered row for a successor to land without a second offer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn submit_loop_shutdown_drops_waiting_work_and_allows_recovery() -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let held = fixture.coordinator.build_slots.clone().acquire_owned().await?;
        fixture.coordinator.ledger.retry_candidate(&fixture.claim, "start shutdown test").await?;
        sqlx::query("UPDATE qbit_block_candidate_outbox SET next_attempt_at=clock_timestamp() WHERE block_hash=$1")
            .bind(&fixture.claim.candidate.block_hash).execute(&fixture.coordinator.ledger.pool).await?;
        let (shutdown, receiver) = watch::channel(false);
        let task = tokio::spawn(fixture.coordinator.clone().submit_loop(receiver));
        fixture.wait_for_offer().await?;
        shutdown.send(true)?;
        tokio::time::timeout(Duration::from_secs(1), task).await??;
        drop(held);
        tokio::time::sleep(Duration::from_millis(150)).await;
        ensure!(!fixture.landed().await?, "shutdown left a queued landing running");
        ensure!(fixture.submissions().await == 1, "the loop offered the block more than once, or never");
        ensure!(fixture.coordinator.build_slots.available_permits() == 1);
        fixture.expire().await?;
        let successor = fixture.successor.claim_candidate(10).await?.context("shutdown prevented takeover")?;
        ensure!(successor.lifecycle.state == CandidateState::Offered);
        fixture.successor_coordinator.process_candidate(&successor).await?;
        ensure!(fixture.state().await? == "submitted");
        ensure!(fixture.submissions().await == 1, "the recovery offered the block again");
        Ok::<_, anyhow::Error>(())
    }.await;
    fixture.close().await?;
    result
}

/// The strictly-live token is fenced between the durable reservation and
/// the `submitblock` call. A token lost in that window never reaches the
/// node; the successor that took the row finds the reservation, never
/// offers either (the call may have happened), lands the audit for a late
/// acceptance, and keeps the row in reconciliation as an unknown outcome.
/// Once the chain shows the block active, a later recovery confirms it,
/// still without any `submitblock`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn token_loss_between_heartbeats_is_fenced_immediately_before_submitblock() -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let probe = Arc::new(OfferProbe::default());
        *fixture.coordinator.offer_probe.lock().unwrap() = Some(probe.clone());
        let process = fixture.process(CANDIDATE_LEASE);
        tokio::time::timeout(Duration::from_secs(5), probe.entered.notified())
            .await
            .context("the reservation was never taken")?;
        let row = fixture.row().await?;
        ensure!(
            row.state == "offer_reserved" && row.evidence,
            "the reservation is not durable before the call: {}",
            row.state
        );
        ensure!(row.outcome.is_none() && row.offered_at_ms.is_none());
        fixture.expire().await?;
        let successor = fixture
            .successor
            .claim_candidate(10)
            .await?
            .context("takeover missing")?;
        ensure!(successor.lifecycle.state == CandidateState::OfferReserved);
        probe.release.notify_one();
        let error = tokio::time::timeout(Duration::from_secs(2), process)
            .await??
            .unwrap_err();
        ensure!(error.to_string().contains("lease renewal"), "{error}");
        ensure!(
            fixture.submissions().await == 0,
            "lost token submitted during heartbeat interval"
        );
        // The recovery: never a second invocation, even though the node has
        // never seen the block.
        fixture
            .successor_coordinator
            .process_candidate(&successor)
            .await?;
        ensure!(
            fixture.submissions().await == 0,
            "a recovered reservation called submitblock"
        );
        let row = fixture.row().await?;
        ensure!(
            row.state == "reconciliation" && row.evidence,
            "recovered reservation ended as {}",
            row.state
        );
        ensure!(
            row.outcome.as_deref() == Some("unknown"),
            "{:?}",
            row.outcome
        );
        ensure!(
            row.offered_at_ms.is_none(),
            "a reservation time was recorded as the call time"
        );
        ensure!(row.token.is_none(), "the recovery kept the claim");
        let reason = row.last_error.context("no reason was recorded")?;
        ensure!(
            reason.contains("delivery unknown") && reason.contains("candidate-owner"),
            "{reason}"
        );
        ensure!(
            fixture.landed().await?,
            "the recovered reservation did not land its audit for a late acceptance"
        );
        ensure!(
            first_offer_samples(&fixture.successor_coordinator.metrics) == 0
                && first_offer_samples(&fixture.coordinator.metrics) == 0,
            "a first-offer sample was recorded without any offer"
        );
        // The block turns out to be active after all: confirmed on the next
        // recovery, still without an offer.
        fixture.activate_on_node().await;
        fixture.expire().await?;
        let again = fixture
            .successor
            .claim_candidate(10)
            .await?
            .context("the reconciliation row was not claimable")?;
        ensure!(again.lifecycle.state == CandidateState::Reconciliation);
        fixture
            .successor_coordinator
            .process_candidate(&again)
            .await?;
        ensure!(fixture.state().await? == "submitted");
        ensure!(
            fixture.submissions().await == 0,
            "confirmation offered the block"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    fixture.close().await?;
    result
}

/// The cached staleness screen still runs before the offer for an ordinary
/// candidate: a block proven superseded is abandoned without ever reaching
/// the node, and a block the chain already holds is confirmed through its
/// audit, again without an offer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn early_chain_probe_skips_only_proven_stale_work_and_recovers_active_blocks() -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    for active in [false, true] {
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
            {
                let mut node = fixture.node.lock().await;
                node.tip = if active {
                    fixture.claim.candidate.block_hash.clone()
                } else {
                    "bb".repeat(32)
                };
                node.height = 101;
                node.chainwork = "02".into();
            }
            *fixture.coordinator.observed_tip.write().await = TipState::default();
            let process = fixture.process(SHORT_LEASE);
            if active {
                fixture.wait_for_renewal().await?;
                // Adopted, durably, before the landing that waits for the
                // build slot: the row is already in the no-resubmission
                // lifecycle, with the node's evidence and no fabricated call
                // time, so a crash from here recovers it as an adoption.
                tokio::time::timeout(Duration::from_secs(3), async {
                    loop {
                        if fixture.state().await? == "reconciliation" {
                            return Ok::<_, anyhow::Error>(());
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .context("the active block was not adopted before its landing")??;
                let row = fixture.row().await?;
                ensure!(
                    row.evidence && row.token.is_some(),
                    "adoption released the claim"
                );
                ensure!(
                    row.outcome.as_deref() == Some("unknown"),
                    "{:?}",
                    row.outcome
                );
                ensure!(
                    row.offered_at_ms.is_none(),
                    "an adoption fabricated a call time"
                );
                ensure!(row.reserved_by.as_deref() == Some("candidate-owner"));
                ensure!(
                    row.last_error
                        .as_deref()
                        .is_some_and(|reason| reason.contains("adopted")
                            && reason.contains("active at height 101")),
                    "{:?}",
                    row.last_error
                );
                ensure!(
                    !process.is_finished(),
                    "active block recovery bypassed its required audit"
                );
                // The crash: the adopting attempt is gone. Its successor
                // recovers the adoption, lands and confirms without any
                // offer.
                process.abort();
                ensure!(process.await.unwrap_err().is_cancelled());
                drop(held);
                fixture.expire().await?;
                let recovered = fixture
                    .successor
                    .claim_candidate(10)
                    .await?
                    .context("the adopted row was not recoverable")?;
                ensure!(recovered.lifecycle.state == CandidateState::Reconciliation);
                fixture
                    .successor_coordinator
                    .process_candidate(&recovered)
                    .await?;
                ensure!(fixture.state().await? == "submitted");
                ensure!(fixture.landed().await?);
            } else {
                tokio::time::timeout(Duration::from_secs(2), process).await???;
                let row = fixture.row().await?;
                ensure!(
                    row.state == "abandoned",
                    "proven stale backlog consumed build capacity"
                );
                ensure!(
                    row.outcome.is_none() && row.reserved_by.is_none(),
                    "an abandoned candidate carries an offer record"
                );
                drop(held);
            }
            ensure!(
                fixture.node.lock().await.submissions == 0,
                "early observation replayed an active or stale block"
            );
            ensure!(
                first_offer_samples(&fixture.coordinator.metrics) == 0,
                "a first-offer sample was recorded without an offer"
            );
            Ok::<_, anyhow::Error>(())
        }
        .await;
        fixture.close().await?;
        result?;
    }
    Ok(())
}

/// Incident 3: a frontend that crashes after its offer and before its
/// landing. Another frontend recovers the offered row, lands it and
/// confirms it without a second `submitblock`; the one first-offer sample
/// stays with the frontend that offered, and the recovery records none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_after_offer_before_landing_recovers_on_another_frontend_without_a_second_offer(
) -> Result<()> {
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
        let process = fixture.process(CANDIDATE_LEASE);
        fixture.wait_for_offer().await?;
        // The crash: the owner is gone with its lease still live.
        process.abort();
        ensure!(process.await.unwrap_err().is_cancelled());
        drop(held);
        ensure!(
            first_offer_samples(&fixture.coordinator.metrics) == 1,
            "the offering frontend did not record its sample"
        );
        let before = fixture.row().await?;
        ensure!(before.state == "offered" && before.evidence);
        let offered_at_ms = before
            .offered_at_ms
            .context("the durable call time is missing")?;
        fixture.expire().await?;
        let recovered = fixture
            .successor
            .claim_candidate(10)
            .await?
            .context("the offered row was not recoverable")?;
        ensure!(recovered.lifecycle.state == CandidateState::Offered);
        ensure!(recovered.lifecycle.offer.outcome == Some(OfferOutcome::Accepted));
        ensure!(recovered.lifecycle.offer.offered_at_ms == Some(offered_at_ms));
        fixture
            .successor_coordinator
            .process_candidate(&recovered)
            .await?;
        ensure!(fixture.state().await? == "submitted");
        ensure!(fixture.landed().await?);
        ensure!(
            fixture.submissions().await == 1,
            "the recovering frontend offered the block again"
        );
        ensure!(
            first_offer_samples(&fixture.successor_coordinator.metrics) == 0,
            "the recovering frontend recorded a duplicate first-offer sample"
        );
        ensure!(
            first_offer_samples(&fixture.coordinator.metrics) == 1,
            "the offering frontend's sample count changed"
        );
        let after = fixture.row().await?;
        ensure!(
            after.offered_at_ms == Some(offered_at_ms)
                && after.reserved_by.as_deref() == Some("candidate-owner"),
            "the offer record did not survive the terminal update"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    fixture.close().await?;
    result
}

/// A landing that fails after the node accepted the block reaches
/// reconciliation with the reason, keeps every piece of evidence, is never
/// abandoned, and heals on a later attempt without a second offer. A
/// landing refused for good (a builder version this binary cannot rebuild)
/// stays in reconciliation the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accepted_landing_failure_reaches_reconciliation_with_a_reason_never_abandoned(
) -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        // The only window-read permit is held, so the rebuild after the
        // offer expires on its deadline.
        let held = fixture
            .coordinator
            .window_reads
            .clone()
            .acquire_owned()
            .await?;
        let lease = CandidateLease {
            rebuild_deadline: Duration::from_millis(300),
            ..CANDIDATE_LEASE
        };
        let coordinator = fixture.coordinator.clone();
        let claim = fixture.claim.clone();
        tokio::time::timeout(Duration::from_secs(10), async move {
            coordinator
                .process_candidate_with_lease(&claim, lease)
                .await
        })
        .await??;
        ensure!(fixture.submissions().await == 1);
        let row = fixture.row().await?;
        ensure!(
            row.state == "reconciliation" && row.evidence,
            "an accepted block's landing failure ended as {}",
            row.state
        );
        ensure!(row.outcome.as_deref() == Some("accepted"));
        ensure!(row.token.is_none(), "the failed attempt kept its claim");
        let reason = row.last_error.context("no reason was recorded")?;
        ensure!(
            reason.contains("landing failed after the offer") && reason.contains("deadline"),
            "{reason}"
        );
        ensure!(!fixture.landed().await?);
        ensure!(
            fixture.coordinator.build_slots.available_permits() == 1,
            "the expired attempt kept its build slot"
        );
        drop(held);
        // The next attempt lands and confirms; the offer is not repeated.
        fixture.expire().await?;
        let again = fixture
            .successor
            .claim_candidate(10)
            .await?
            .context("the reconciliation row was not claimable")?;
        ensure!(again.lifecycle.state == CandidateState::Reconciliation);
        fixture.coordinator.process_candidate(&again).await?;
        ensure!(fixture.state().await? == "submitted");
        ensure!(fixture.landed().await?);
        ensure!(
            fixture.submissions().await == 1,
            "the recovery offered again"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    fixture.close().await?;
    result?;

    // A refusal for good: a builder version this binary cannot rebuild. The
    // block is still offered (it is a valid block) and then kept, with its
    // evidence, for a frontend that can land it; nothing is abandoned. The
    // fixture's own candidate stays claimed and unprocessed.
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let mut foreign = fixture.found(20_000)?;
        foreign.audit_builder_version = 99;
        let hash = foreign.block_hash.clone();
        fixture
            .coordinator
            .ledger
            .enqueue_candidate_observed(foreign, Some(unix_ms_now()?))
            .await?;
        let claim = fixture
            .coordinator
            .ledger
            .claim_candidate(10)
            .await?
            .context("the foreign-built candidate was not claimable")?;
        ensure!(claim.candidate.block_hash == hash);
        fixture.coordinator.process_candidate(&claim).await?;
        ensure!(
            fixture.submissions().await == 1,
            "the valid block was not offered"
        );
        let row = fixture.row_of(&hash).await?;
        ensure!(
            row.state == "reconciliation" && row.evidence,
            "a landing refused for good ended as {}",
            row.state
        );
        ensure!(row.outcome.as_deref() == Some("accepted"));
        let reason = row.last_error.context("no reason")?;
        ensure!(
            reason.contains("not rebuilding") && reason.contains("99"),
            "{reason}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    fixture.close().await?;
    result
}

/// The first-offer sample is honest about its clocks: a row without a proof
/// time yields no sample, and a proof time ahead of the offering frontend's
/// clock (skew between hosts) yields none either, never a clamped zero. The
/// block is offered, landed and confirmed all the same.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn first_offer_sample_is_skipped_for_an_unknown_or_skewed_proof_time() -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    for (proof, label) in [
        (None, "unknown"),
        (Some(unix_ms_now()? + 3_600_000), "skewed"),
    ] {
        let Some(fixture) = Fixture::open().await? else {
            return Ok(());
        };
        let result = async {
            // The fixture's own candidate carries a real proof time and is
            // left claimed and unprocessed; this one is enqueued with the
            // case's proof time and is the only claimable row.
            let candidate = fixture.found(30_000)?;
            let hash = candidate.block_hash.clone();
            fixture
                .coordinator
                .ledger
                .enqueue_candidate_observed(candidate, proof)
                .await?;
            let claim = fixture
                .coordinator
                .ledger
                .claim_candidate(10)
                .await?
                .context("candidate not claimable")?;
            ensure!(claim.candidate.block_hash == hash, "{label}");
            ensure!(claim.lifecycle.proof_observed_at_ms == proof, "{label}");
            fixture.coordinator.process_candidate(&claim).await?;
            let row = fixture.row_of(&hash).await?;
            ensure!(row.state == "submitted", "{label}: {}", row.state);
            ensure!(row.offered_at_ms.is_some(), "{label}: no call time");
            ensure!(fixture.submissions().await == 1, "{label}");
            ensure!(
                first_offer_samples(&fixture.coordinator.metrics) == 0,
                "{label}: a sample was recorded from an unusable proof time"
            );
            Ok::<_, anyhow::Error>(())
        }
        .await;
        fixture.close().await?;
        result?;
    }
    Ok(())
}
