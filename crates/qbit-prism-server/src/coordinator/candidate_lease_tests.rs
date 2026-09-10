//! Real PostgreSQL ownership tests with short leases and deliberately blocked work.
use super::*;
use axum::{extract::State, routing::post, Json, Router};
use sqlx::PgPool;
use tokio::task::JoinHandle;

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
};

// Deliberately long ledger transactions share advisory locks across schemas.
static TEST_LOCK: Mutex<()> = Mutex::const_new(());

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

struct Fixture {
    admin: PgPool,
    schema: String,
    coordinator: Arc<Coordinator>,
    successor: Ledger,
    node: Arc<Mutex<NodeState>>,
    server: JoinHandle<()>,
    claim: CandidateClaim,
}

impl Fixture {
    async fn open() -> Result<Option<Self>> {
        let Ok(raw) = std::env::var("PRISM_TEST_DATABASE_URL") else {
            eprintln!("set PRISM_TEST_DATABASE_URL for candidate lease integration");
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
        let config = Config {
            database_url: url.to_string(),
            instance_id: "candidate-owner".into(),
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
            block_submit_timeout: Duration::from_secs(1),
            poll_interval: Duration::from_secs(1),
            blockwait: false,
            build_workers: 1,
            runtime_workers: 2,
            snapshot_interval: Duration::from_secs(60),
            health_timeout: Duration::from_secs(15),
            share_commit_timeout: Duration::from_secs(15),
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
        let coordinator = Coordinator::new(
            config,
            std::sync::Arc::new(crate::metrics::Metrics::default()),
        )
        .await?;
        let successor =
            Ledger::connect(url.as_str(), "candidate-successor".into(), 4, false).await?;
        coordinator
            .ledger
            .observe_chain_view(&"aa".repeat(32), 100, "01")
            .await?;
        *coordinator.observed_tip.write().await = TipState::baseline("aa".repeat(32));
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
        let bundle = qbit_prism::build_audit_bundle_with_coinbase_options(
            snapshot.shares,
            FoundBlock {
                block_height: 101,
                coinbase_value_sats: 500_000_000,
                network_difficulty: 100,
                anchor_job_issued_at_ms: snapshot.anchor_ms,
            },
            snapshot.prior_balances,
            qbit_prism::PayoutPolicy::day_one_default(),
            Some("00".repeat(12)),
            vec![],
            &ManifestSigningKey::from_seed_hex(&"11".repeat(32))?,
            &ManifestSigningKey::from_seed_hex(&"22".repeat(32))?,
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
        let proof = (0..10_000u32)
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
        coordinator
            .ledger
            .enqueue_candidate(Candidate {
                block_hash: proof.block_hash_hex,
                block_hex: proof.block_hex,
                job_id: job.job_id,
                payout_revision: snapshot.payout_revision,
                bundle,
                coinbase_suffix_hex: Some("00".repeat(12)),
                deferred_share: None,
            })
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
            successor,
            node,
            server,
            claim,
        }))
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

    async fn state(&self) -> Result<String> {
        Ok(
            sqlx::query_scalar("SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$1")
                .bind(&self.claim.candidate.block_hash)
                .fetch_one(&self.coordinator.ledger.pool)
                .await?,
        )
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
        sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_expires_at=clock_timestamp()-interval '1 second' WHERE block_hash=$1")
            .bind(&self.claim.candidate.block_hash).execute(&self.coordinator.ledger.pool).await?;
        Ok(())
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
        fixture.wait_for_renewal().await?;
        tokio::time::sleep(Duration::from_millis(2200)).await;
        ensure!(
            fixture.successor.claim_candidate(10).await?.is_none(),
            "another frontend stole work waiting for build capacity"
        );
        ensure!(fixture.node.lock().await.submissions == 0);
        drop(held);
        tokio::time::timeout(Duration::from_secs(5), process).await???;
        ensure!(fixture.state().await? == "submitted");
        ensure!(
            fixture.node.lock().await.submissions == 1,
            "candidate was not submitted exactly once"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    fixture.close().await?;
    result
}

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
            fixture.wait_for_renewal().await?;
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
                    let error=tokio::time::timeout(Duration::from_secs(2),process).await??.unwrap_err();
                    ensure!(error.to_string().contains("lease renewal"),"{error}");
                    fixture.successor.retry_candidate(&successor,"lease test").await?;
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
            ensure!(fixture.node.lock().await.submissions==0,"canceled queued work reached submitblock");
            ensure!(fixture.coordinator.build_slots.available_permits()==1,"canceled waiter retained build capacity");
            sqlx::query("UPDATE qbit_block_candidate_outbox SET next_attempt_at=clock_timestamp() WHERE block_hash=$1")
                .bind(&fixture.claim.candidate.block_hash).execute(&fixture.coordinator.ledger.pool).await?;
            let successor=fixture.successor.claim_candidate(10).await?.context("canceled owner kept its lease alive")?;
            fixture.coordinator.process_candidate(&successor).await?;
            ensure!(fixture.state().await?=="submitted");
            Ok::<_,anyhow::Error>(())
        }.await;
        fixture.close().await?;
        result?;
    }
    Ok(())
}

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
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let token: Option<String> = sqlx::query_scalar("SELECT claim_token FROM qbit_block_candidate_outbox WHERE block_hash=$1")
                    .bind(&fixture.claim.candidate.block_hash).fetch_one(&fixture.coordinator.ledger.pool).await?;
                if token.as_deref().is_some_and(|token| token != fixture.claim.claim_token) {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await??;
        shutdown.send(true)?;
        tokio::time::timeout(Duration::from_secs(1), task).await??;
        drop(held);
        tokio::time::sleep(Duration::from_millis(150)).await;
        ensure!(fixture.node.lock().await.submissions == 0, "shutdown left a queued submission running");
        ensure!(fixture.coordinator.build_slots.available_permits() == 1);
        fixture.expire().await?;
        let successor = fixture.successor.claim_candidate(10).await?.context("shutdown prevented takeover")?;
        fixture.coordinator.process_candidate(&successor).await?;
        ensure!(fixture.state().await? == "submitted");
        ensure!(fixture.node.lock().await.submissions == 1);
        Ok::<_, anyhow::Error>(())
    }.await;
    fixture.close().await?;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn token_loss_between_heartbeats_is_fenced_immediately_before_submitblock() -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let gate = {
            let mut node = fixture.node.lock().await;
            node.pause_network = Some(4);
            node.gate.clone()
        };
        let process = fixture.process(CANDIDATE_LEASE);
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
        fixture.expire().await?;
        let successor = fixture
            .successor
            .claim_candidate(10)
            .await?
            .context("takeover missing")?;
        gate.release.notify_one();
        let error = tokio::time::timeout(Duration::from_secs(2), process)
            .await??
            .unwrap_err();
        ensure!(error.to_string().contains("lease renewal"), "{error}");
        ensure!(
            fixture.node.lock().await.submissions == 0,
            "lost token submitted during heartbeat interval"
        );
        fixture.coordinator.process_candidate(&successor).await?;
        ensure!(fixture.state().await? == "submitted");
        Ok::<_, anyhow::Error>(())
    }
    .await;
    fixture.close().await?;
    result
}

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
            let mut process = fixture.process(SHORT_LEASE);
            if active {
                fixture.wait_for_renewal().await?;
                ensure!(
                    tokio::time::timeout(Duration::from_millis(250), &mut process)
                        .await
                        .is_err(),
                    "active block recovery bypassed its required audit"
                );
                drop(held);
                tokio::time::timeout(Duration::from_secs(5), process).await???;
                ensure!(fixture.state().await? == "submitted");
            } else {
                tokio::time::timeout(Duration::from_secs(2), process).await???;
                ensure!(
                    fixture.state().await? == "abandoned",
                    "proven stale backlog consumed build capacity"
                );
                drop(held);
            }
            ensure!(
                fixture.node.lock().await.submissions == 0,
                "early observation replayed an active or stale block"
            );
            Ok::<_, anyhow::Error>(())
        }
        .await;
        fixture.close().await?;
        result?;
    }
    Ok(())
}
