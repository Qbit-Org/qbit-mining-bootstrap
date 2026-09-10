use anyhow::{ensure, Context, Result};
use axum::{extract::State, routing::post, Json, Router};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::AcceptedShare;
use qbit_prism_server::{
    config::Config, coordinator::Coordinator, ledger::Candidate, readiness, rpc::Rpc,
    stratum::MiningBackend,
};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{Mutex, Notify},
    task::JoinHandle,
};

#[derive(Default)]
struct NetworkReplyGate {
    paused: Notify,
    release: Notify,
}

struct NodeState {
    chain: Value,
    network: Value,
    template: Value,
    fee_estimate: Value,
    mempool: Value,
    fee_floor_calls: usize,
    network_calls: usize,
    drop_peers_after: Option<usize>,
    pause_network_after: Option<usize>,
    tip_parent: String,
    network_reply_gate: Arc<NetworkReplyGate>,
}
struct Node {
    state: Arc<Mutex<NodeState>>,
    rpc: Rpc,
    url: String,
    task: JoinHandle<()>,
}
impl Drop for Node {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Node {
    async fn open() -> Result<Self> {
        let now = chrono::Utc::now().timestamp() as u64;
        let state = Arc::new(Mutex::new(NodeState {
            chain: json!({"chain":"test","initialblockdownload":false,"blocks":100,"headers":100,"bestblockhash":"ab".repeat(32),"chainwork":"01"}),
            network: json!({"connections":2}),
            template: json!({"height":101,"coinbasevalue":5_000_000_000u64,"previousblockhash":"ab".repeat(32),"version":0x20000000u32,"bits":"207fffff","curtime":now,"mintime":now-1,"transactions":[]}),
            fee_estimate: json!({"feerate":"0.00001"}),
            mempool: json!({"minrelaytxfee":"0.00001","mempoolminfee":"0.00001"}),
            fee_floor_calls: 0,
            network_calls: 0,
            drop_peers_after: None,
            pause_network_after: None,
            tip_parent: "cd".repeat(32),
            network_reply_gate: Arc::new(NetworkReplyGate::default()),
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/", listener.local_addr()?);
        let app = Router::new()
            .route("/", post(answer))
            .with_state(state.clone());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Ok(Self {
            rpc: Rpc::new(
                url.clone(),
                "test".into(),
                "test".into(),
                Duration::from_secs(5),
            )?,
            state,
            url,
            task,
        })
    }
}
async fn answer(
    State(state): State<Arc<Mutex<NodeState>>>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let mut state = state.lock().await;
    let mut pause_reply = None;
    let result = match request["method"].as_str().unwrap_or("") {
        "getblockchaininfo" => state.chain.clone(),
        "getnetworkinfo" => {
            state.network_calls += 1;
            if state
                .drop_peers_after
                .is_some_and(|remaining| remaining == 0)
            {
                state.network["connections"] = json!(0);
            }
            if let Some(remaining) = &mut state.drop_peers_after {
                *remaining = remaining.saturating_sub(1);
            }
            if let Some(remaining) = state.pause_network_after {
                if remaining == 0 {
                    state.pause_network_after = None;
                    pause_reply = Some(state.network_reply_gate.clone());
                } else {
                    state.pause_network_after = Some(remaining - 1);
                }
            }
            state.network.clone()
        }
        "getblocktemplate" => state.template.clone(),
        "estimatesmartfee" => state.fee_estimate.clone(),
        "getmempoolinfo" => {
            state.fee_floor_calls += 1;
            state.mempool.clone()
        }
        "getbestblockhash" => state.chain["bestblockhash"].clone(),
        "getblockhash" if request["params"][0] == 0 => json!("00".repeat(32)),
        "getblockhash" => state.chain["bestblockhash"].clone(),
        "getblockheader" => json!({"previousblockhash":state.tip_parent}),
        "validateaddress" => {
            json!({"isvalid":true,"scriptPubKey":format!("5220{}","11".repeat(32))})
        }
        _ => {
            return Json(
                json!({"id":request["id"],"result":null,"error":{"code":-32601,"message":"unexpected RPC"}}),
            )
        }
    };
    drop(state);
    if let Some(gate) = pause_reply {
        gate.paused.notify_one();
        gate.release.notified().await;
    }
    Json(json!({"id":request["id"],"result":result,"error":null}))
}

#[tokio::test]
async fn public_rpc_readiness_requires_synced_headers_and_configured_peer_floor() -> Result<()> {
    let node = Node::open().await?;
    for chain in ["mainnet", "testnet", "testnet4", "signet"] {
        readiness::chain_info(&node.rpc, chain, 2).await?;
        ensure!(readiness::chain_info(&node.rpc, chain, 3).await.is_err());
    }
    for headers in [Value::Null, json!(-1), json!(101), json!(99), json!(100.5)] {
        node.state.lock().await.chain["headers"] = headers;
        ensure!(readiness::chain_info(&node.rpc, "mainnet", 1)
            .await
            .is_err());
    }
    node.state.lock().await.chain["headers"] = json!(100);
    for peers in [Value::Null, json!(-1), json!(0), json!(1.5)] {
        node.state.lock().await.network["connections"] = peers;
        ensure!(readiness::chain_info(&node.rpc, "testnet", 1)
            .await
            .is_err());
    }
    node.state.lock().await.network["connections"] = json!(2);
    for ibd in [Value::Null, json!(true), json!("false")] {
        node.state.lock().await.chain["initialblockdownload"] = ibd;
        ensure!(readiness::chain_info(&node.rpc, "testnet", 1)
            .await
            .is_err());
    }
    Ok(())
}

#[tokio::test]
async fn regtest_needs_no_peers_but_rejects_explicit_header_lag() -> Result<()> {
    let node = Node::open().await?;
    node.state.lock().await.chain["headers"] = Value::Null;
    node.state.lock().await.network["connections"] = json!(0);
    readiness::chain_info(&node.rpc, "regtest", 1).await?;
    ensure!(node.state.lock().await.network_calls == 0);
    node.state.lock().await.chain["headers"] = json!(101);
    ensure!(readiness::chain_info(&node.rpc, "regtest", 1)
        .await
        .is_err());
    Ok(())
}

fn coordinator_config(database_url: String, node: &Node) -> Result<Config> {
    Ok(Config {
        database_url,
        instance_id: "readiness-test".into(),
        database_connections: 4,
        initialize_schema: true,
        chain: "testnet".into(),
        expected_genesis_hash: None,
        min_peers: 2,
        template_max_age: Duration::from_secs(120),
        submit_tip_max_age: Duration::from_secs(10),
        template_refresh_failure_exit: Duration::from_secs(120),
        rpc_url: node.url.clone(),
        rpc_user: "test".into(),
        rpc_password: "test".into(),
        rpc_timeout: Duration::from_secs(5),
        block_submit_timeout: Duration::from_secs(1),
        poll_interval: Duration::from_secs(1),
        blockwait: false,
        build_workers: 2,
        runtime_workers: 2,
        snapshot_interval: Duration::from_secs(60),
        health_timeout: Duration::from_secs(15),
        share_commit_timeout: Duration::from_secs(15),
        extranonce2_size: 8,
        coinbase_tag: "/PRISM/".into(),
        manifest_seed: "11".repeat(32),
        ledger_seed: "22".repeat(32),
        ledger_public_key: ManifestSigningKey::from_seed_hex(&"22".repeat(32))?.public_key_hex(),
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
        version_mask: qbit_prism_server::codec::VERSION_ROLLING_MASK,
        audit_bind: "127.0.0.1".into(),
        audit_port: 0,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observed_readiness_failure_closes_cached_work_and_candidate_settlement() -> Result<()> {
    let Ok(raw) = std::env::var("PRISM_TEST_DATABASE_URL") else {
        eprintln!("skipping coordinator readiness integration; set PRISM_TEST_DATABASE_URL");
        return Ok(());
    };
    let admin = sqlx::PgPool::connect(&raw).await?;
    let schema = format!("prism_readiness_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await?;
    let mut url = url::Url::parse(&raw)?;
    url.query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let node = Node::open().await?;
    let coordinator = Coordinator::new(
        coordinator_config(url.into(), &node)?,
        std::sync::Arc::new(qbit_prism_server::metrics::Metrics::default()),
    )
    .await?;
    let result = async {
        coordinator
            .ledger
            .append(
                AcceptedShare {
                    share_seq: 0,
                    share_id: format!("miner:{}", "01".repeat(32)),
                    miner_id: "miner".into(),
                    order_key: "miner".into(),
                    p2mr_program_hex: "11".repeat(32),
                    share_difficulty: 1_000_000,
                    network_difficulty: 1_000_000,
                    template_height: 100,
                    job_id: "seed".into(),
                    job_issued_at_ms: 1,
                    accepted_at_ms: 0,
                    ntime: 1,
                    credit_policy: None,
                },
                None,
            )
            .await?;
        coordinator.refresh_once().await?;
        ensure!(coordinator.health().await["ready"] == true);
        let semantic_generation = coordinator.health().await["template_generation"].clone();
        {
            let mut prepared = coordinator.prepared.write().await;
            Arc::get_mut(prepared.as_mut().unwrap()).unwrap().created =
                std::time::Instant::now() - Duration::from_secs(61);
        }
        node.state.lock().await.template["curtime"] = json!(chrono::Utc::now().timestamp() + 1);
        coordinator.refresh_once().await?;
        ensure!(
            coordinator.health().await["template_generation"] == semantic_generation,
            "equivalent timer reanchor reset semantic delivery coverage"
        );
        let worker = coordinator.authorize("miner.test").await?;
        let extra = format!("{:08x}", coordinator.new_session_id().await?);
        let job = coordinator.build_job(&worker, &extra, 1e-9, 0.0).await?;
        coordinator
            .persist_issued_job(&worker, &job, 0, Duration::from_secs(60))
            .await?;
        let proof = (0..10_000u32)
            .find_map(|nonce| {
                let proof = job
                    .wire
                    .assemble_submission(
                        &"00".repeat(8),
                        &format!("{:08x}", job.wire.ntime),
                        &format!("{nonce:08x}"),
                        None,
                        0,
                    )
                    .ok()?;
                proof.block_pass.then_some(proof)
            })
            .context("no constrained proof")?;
        let candidate = Candidate {
            block_hash: proof.block_hash_hex.clone(),
            block_hex: proof.block_hex.clone(),
            job_id: job.wire.job_id.clone(),
            payout_revision: job.context.prepared.snapshot.payout_revision,
            bundle: (*job.context.bundle).clone(),
            coinbase_suffix_hex: Some(format!(
                "{}{}{}",
                hex::encode("/PRISM/"),
                extra,
                "00".repeat(8)
            )),
            deferred_share: None,
        };
        coordinator.ledger.enqueue_candidate(candidate).await?;
        let claim = coordinator
            .ledger
            .claim_candidate(120)
            .await?
            .context("candidate missing")?;

        for failure in ["headers", "peers", "template"] {
            {
                let mut state = node.state.lock().await;
                match failure {
                    "headers" => state.chain["headers"] = json!(101),
                    "peers" => state.network["connections"] = json!(1),
                    _ => state.template["curtime"] = json!(chrono::Utc::now().timestamp() - 121),
                }
            }
            ensure!(
                coordinator.refresh_once().await.is_err(),
                "unsafe {failure} was accepted"
            );
            ensure!(
                coordinator.health().await["ready"] == false,
                "{failure} retained cached readiness"
            );
            ensure!(coordinator
                .build_job(&worker, &extra, 1e-9, 0.0)
                .await
                .is_err());
            ensure!(coordinator
                .resume_job(&worker, &job.wire.job_id)
                .await
                .is_err());
            ensure!(coordinator
                .submit(&worker, &job, proof.clone(), false.into())
                .await
                .is_err());
            if failure != "template" {
                ensure!(coordinator.process_candidate(&claim).await.is_err());
                let blocks: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_pool_blocks")
                    .fetch_one(&coordinator.ledger.pool)
                    .await?;
                ensure!(
                    blocks == 0,
                    "unsafe node observation landed candidate payout rows"
                );
            }
            {
                let mut state = node.state.lock().await;
                state.chain["headers"] = json!(100);
                state.network["connections"] = json!(2);
                state.template["curtime"] = json!(chrono::Utc::now().timestamp());
            }
            coordinator.refresh_once().await?;
            ensure!(
                coordinator.health().await["ready"] == true,
                "{failure} recovery failed"
            );
        }
        // Peers disappear after the initial successful observation. The
        // end-of-proof check must also gate settlement and cache reuse.
        node.state.lock().await.drop_peers_after = Some(1);
        ensure!(coordinator.refresh_once().await.is_err());
        ensure!(coordinator.health().await["ready"] == false);
        {
            let mut state = node.state.lock().await;
            state.network["connections"] = json!(2);
            state.drop_peers_after = Some(1);
        }
        ensure!(coordinator.process_candidate(&claim).await.is_err());
        let blocks: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_pool_blocks")
            .fetch_one(&coordinator.ledger.pool)
            .await?;
        ensure!(
            blocks == 0,
            "peer loss during candidate proof landed payout rows"
        );
        {
            let mut state = node.state.lock().await;
            state.network["connections"] = json!(2);
            state.drop_peers_after = None;
        }
        coordinator.refresh_once().await?;
        let generation = coordinator.health().await["template_generation"].clone();
        {
            let mut state = node.state.lock().await;
            // Force a new immutable bundle, then lose peers after refresh's
            // initial observation and reconciliation have both succeeded.
            state.template["coinbasevalue"] = json!(4_999_999_999u64);
            state.drop_peers_after = Some(2);
        }
        ensure!(coordinator.refresh_once().await.is_err());
        let health = coordinator.health().await;
        ensure!(health["ready"] == false);
        ensure!(
            health["template_generation"] == generation,
            "unsafe new work was published"
        );
        let credited: i64 =
            sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger WHERE accepted")
                .fetch_one(&coordinator.ledger.pool)
                .await?;
        ensure!(credited == 1, "unsafe cached work credited shares");

        // A previously captured healthy RPC reply arrives after a concurrent
        // candidate observation has already revoked readiness. Exercise both
        // reuse and new publication without relying on scheduler timing.
        for rebuild in [false, true] {
            {
                let mut state = node.state.lock().await;
                state.network["connections"] = json!(2);
                state.drop_peers_after = None;
                state.template["curtime"] = json!(chrono::Utc::now().timestamp());
            }
            coordinator.refresh_once().await?;
            let generation = coordinator.health().await["template_generation"].clone();
            let gate = {
                let mut state = node.state.lock().await;
                if rebuild {
                    state.template["coinbasevalue"] = json!(4_999_999_998u64);
                }
                // Initial proof and reconciliation pass; delay the final
                // proof with the healthy response already captured.
                state.pause_network_after = Some(2);
                state.network_reply_gate.clone()
            };
            let refresh = tokio::spawn({
                let coordinator = coordinator.clone();
                async move { coordinator.refresh_once().await }
            });
            if tokio::time::timeout(Duration::from_secs(5), gate.paused.notified())
                .await
                .is_err()
            {
                refresh.abort();
                anyhow::bail!("refresh did not reach the final readiness proof");
            }
            node.state.lock().await.network["connections"] = json!(1);
            let candidate_result = coordinator.process_candidate(&claim).await;
            node.state.lock().await.network["connections"] = json!(2);
            gate.release.notify_one();
            let refresh_result = tokio::time::timeout(Duration::from_secs(5), refresh).await??;
            ensure!(
                candidate_result.is_err(),
                "unsafe concurrent candidate observation succeeded"
            );
            ensure!(
                refresh_result
                    .err()
                    .is_some_and(|error| error.to_string().contains("node readiness changed")),
                "older successful proof restored invalidated readiness"
            );
            let health = coordinator.health().await;
            ensure!(
                health["ready"] == false,
                "concurrent failure was overwritten"
            );
            ensure!(
                health["template_generation"] == generation,
                "revoked refresh published work"
            );
            ensure!(coordinator
                .build_job(&worker, &extra, 1e-9, 0.0)
                .await
                .is_err());
            ensure!(coordinator
                .resume_job(&worker, &job.wire.job_id)
                .await
                .is_err());
            ensure!(coordinator
                .submit(&worker, &job, proof.clone(), false.into())
                .await
                .is_err());
            coordinator.refresh_once().await?;
            ensure!(
                coordinator.health().await["ready"] == true,
                "fresh readiness proof did not recover"
            );
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    coordinator.ledger.pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await?;
    admin.close().await;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn another_frontend_payout_revision_retires_same_parent_work_and_preserves_parent_grace(
) -> Result<()> {
    let Ok(raw) = std::env::var("PRISM_TEST_DATABASE_URL") else {
        eprintln!("set PRISM_TEST_DATABASE_URL for shared payout revision regression");
        return Ok(());
    };
    let admin = sqlx::PgPool::connect(&raw).await?;
    let schema = format!("prism_job_revision_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await?;
    let mut url = url::Url::parse(&raw)?;
    url.query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let node = Node::open().await?;
    let first = Coordinator::new(
        coordinator_config(url.to_string(), &node)?,
        std::sync::Arc::new(qbit_prism_server::metrics::Metrics::default()),
    )
    .await?;
    let mut config = coordinator_config(url.into(), &node)?;
    config.instance_id = "readiness-second".into();
    let second = Coordinator::new(
        config,
        std::sync::Arc::new(qbit_prism_server::metrics::Metrics::default()),
    )
    .await?;
    let result = async {
        first.ledger.append(AcceptedShare {
            share_seq: 0, share_id: format!("miner:{}", "01".repeat(32)),
            miner_id: "miner".into(), order_key: "miner".into(),
            p2mr_program_hex: "11".repeat(32), share_difficulty: 1_000_000,
            network_difficulty: 1_000_000, template_height: 100,
            job_id: "seed".into(), job_issued_at_ms: 1, accepted_at_ms: 0,
            ntime: 1, credit_policy: None,
        }, None).await?;
        first.refresh_once().await?;
        second.refresh_once().await?;
        let worker = first.authorize("miner.revision").await?;
        let extra = format!("{:08x}", first.new_session_id().await?);
        let old = first.build_job(&worker, &extra, 1e-12, 0.0).await?;
        first.persist_issued_job(&worker, &old, 0, Duration::from_secs(60)).await?;
        ensure!(second.resume_job(&worker, &old.wire.job_id).await?.is_some());
        let solve = |job: &qbit_prism_server::stratum::MiningJob<qbit_prism_server::coordinator::JobContext>, start: u32| {
            (start..start+10_000).find_map(|nonce| {
                let proof = job.wire.assemble_submission(&"00".repeat(8),
                    &format!("{:08x}",job.wire.ntime), &format!("{nonce:08x}"), None, 0).ok()?;
                (proof.share_pass && proof.block_pass).then_some(proof)
            }).context("constrained miner found no valid block proof")
        };
        let old_proof = solve(&old, 0)?;
        // Reconciliation on another frontend commits a new payout revision
        // without necessarily changing this node's already-observed parent.
        // Isolate that durable revision transition from the independently
        // covered block-observation/RPC path.
        let revision: i64 = sqlx::query_scalar("UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1 WHERE singleton RETURNING payout_revision")
            .fetch_one(&second.ledger.pool).await?;
        ensure!(revision != old.context.prepared.snapshot.payout_revision);
        for frontend in [&first, &second] {
            ensure!(frontend.health().await["ready"] == false);
            for grace in [false, true] {
                let error = frontend.submit(&worker, &old, old_proof.clone(), grace.into()).await.unwrap_err();
                ensure!(error.reason_id.as_deref() == Some("stale-job"), "{error}");
            }
            ensure!(frontend.resume_job(&worker, &old.wire.job_id).await?.is_none());
            frontend.refresh_once().await?;
            ensure!(frontend.health().await["ready"] == true);
            for grace in [false, true] {
                let error = frontend.submit(&worker, &old, old_proof.clone(), grace.into()).await.unwrap_err();
                ensure!(error.reason_id.as_deref() == Some("stale-job"), "{error}");
            }
            ensure!(frontend.resume_job(&worker, &old.wire.job_id).await?.is_none());
        }
        let rejected: (i64,i64) = sqlx::query_as("SELECT (SELECT count(*) FROM qbit_share_ledger),(SELECT count(*) FROM qbit_block_candidate_outbox)")
            .fetch_one(&first.ledger.pool).await?;
        ensure!(rejected == (1,0), "superseded work was ACKed or enqueued");

        let fresh = first.build_job(&worker, &extra, 1e-12, 0.0).await?;
        ensure!(fresh.wire.previousblockhash == old.wire.previousblockhash);
        ensure!(fresh.wire.payout_revision == revision);
        let proof = solve(&fresh, 0)?;
        first.submit(&worker, &fresh, proof.clone(), false.into()).await?;
        let candidate: Value = sqlx::query_scalar("SELECT candidate FROM qbit_block_candidate_outbox WHERE block_hash=$1")
            .bind(&proof.block_hash_hex).fetch_one(&first.ledger.pool).await?;
        let candidate: Candidate = serde_json::from_value(candidate)?;
        ensure!(candidate.payout_revision == revision);
        ensure!(second.ledger.candidate_revision_valid(&candidate).await?);

        // A real parent change still grants only the immediately previous
        // parent's eligible shares. Even valid stale block proofs never enter
        // the candidate queue.
        {
            let mut state = node.state.lock().await;
            state.tip_parent = fresh.wire.previousblockhash.clone();
            state.chain["bestblockhash"] = json!("ef".repeat(32));
            state.chain["blocks"] = json!(101);
            state.chain["headers"] = json!(101);
            state.chain["chainwork"] = json!("02");
            state.template["previousblockhash"] = json!("ef".repeat(32));
            state.template["height"] = json!(102);
        }
        first.refresh_once().await?;
        let grace_proof = solve(&fresh, 20_000)?;
        ensure!(first.submit(&worker, &fresh, grace_proof.clone(), false.into()).await.unwrap_err().reason_id.as_deref() == Some("stale-job"));
        first.submit(&worker, &fresh, grace_proof.clone(), true.into()).await?;
        let credited: (String,bool) = sqlx::query_as("SELECT credit_policy,EXISTS(SELECT 1 FROM qbit_block_candidate_outbox WHERE block_hash=$2) FROM qbit_share_ledger WHERE share_id=$1")
            .bind(format!("{}:{}", worker.username, grace_proof.block_hash_hex))
            .bind(&grace_proof.block_hash_hex).fetch_one(&first.ledger.pool).await?;
        ensure!(credited == ("stale-grace".into(),false));
        Ok::<_,anyhow::Error>(())
    }.await;
    first.ledger.pool.close().await;
    second.ledger.pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await?;
    admin.close().await;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cached_ctv_work_revalidates_live_floors_and_fences_old_underfunded_jobs() -> Result<()> {
    let Ok(raw) = std::env::var("PRISM_TEST_DATABASE_URL") else {
        eprintln!("skipping coordinator fee integration; set PRISM_TEST_DATABASE_URL");
        return Ok(());
    };
    let admin = sqlx::PgPool::connect(&raw).await?;
    for explicit in [true, false] {
        let schema = format!("prism_fee_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut url = url::Url::parse(&raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        let node = Node::open().await?;
        let mut config = coordinator_config(url.into(), &node)?;
        config.ctv_enabled = true;
        config.ctv_config.max_direct_coinbase_outputs = 0;
        config.ctv_fee = explicit.then(|| qbit_prism::FanoutFeeRatePolicy::new(1000, 12000));
        let coordinator = Coordinator::new(
            config,
            std::sync::Arc::new(qbit_prism_server::metrics::Metrics::default()),
        )
        .await?;
        let result = async {
            coordinator
                .ledger
                .append(
                    AcceptedShare {
                        share_seq: 0,
                        share_id: format!("miner:{}", "01".repeat(32)),
                        miner_id: "miner".into(),
                        order_key: "miner".into(),
                        p2mr_program_hex: "11".repeat(32),
                        share_difficulty: 1_000_000,
                        network_difficulty: 1_000_000,
                        template_height: 100,
                        job_id: "seed".into(),
                        job_issued_at_ms: 1,
                        accepted_at_ms: 0,
                        ntime: 1,
                        credit_policy: None,
                    },
                    None,
                )
                .await?;
            coordinator.refresh_once().await?;
            let original_generation = coordinator.health().await["template_generation"].clone();
            let worker = coordinator.authorize("miner.test").await?;
            let extra = format!("{:08x}", coordinator.new_session_id().await?);
            let low_job = coordinator.build_job(&worker, &extra, 1e-12, 0.0).await?;
            ensure!(
                low_job.context.bundle.ctv_fanout_manifest_set.is_some(),
                "fixture did not build a CTV settlement"
            );
            coordinator
                .persist_issued_job(&worker, &low_job, 0, Duration::from_secs(60))
                .await?;
            let share_proof = |job: &qbit_prism_server::stratum::MiningJob<
                qbit_prism_server::coordinator::JobContext,
            >| {
                (0..10_000u32)
                    .find_map(|nonce| {
                        let proof = job
                            .wire
                            .assemble_submission(
                                &"00".repeat(8),
                                &format!("{:08x}", job.wire.ntime),
                                &format!("{nonce:08x}"),
                                None,
                                0,
                            )
                            .ok()?;
                        (proof.share_pass && !proof.block_pass).then_some(proof)
                    })
                    .context("no constrained share-only proof")
            };
            let low_proof = share_proof(&low_job)?;
            coordinator.refresh_once().await?;
            ensure!(
                coordinator.health().await["template_generation"] == original_generation,
                "identical work was rebuilt"
            );
            ensure!(
                node.state.lock().await.fee_floor_calls == 2,
                "cache reuse skipped live fee RPC"
            );
            node.state.lock().await.mempool["mempoolminfee"] = json!("0.00002");
            let error = coordinator.refresh_once().await.unwrap_err();
            ensure!(error.to_string().contains("required=2000"), "{error}");
            ensure!(
                coordinator.health().await["ready"] == false,
                "fee failure retained readiness"
            );
            ensure!(coordinator
                .build_job(&worker, &extra, 1e-12, 0.0)
                .await
                .is_err());
            ensure!(coordinator
                .resume_job(&worker, &low_job.wire.job_id)
                .await
                .is_err());
            ensure!(coordinator
                .submit(&worker, &low_job, low_proof.clone(), false.into())
                .await
                .is_err());
            ensure!(coordinator.health().await["template_generation"] == original_generation);

            if explicit {
                node.state.lock().await.mempool["mempoolminfee"] = json!("0.00001");
                coordinator.refresh_once().await?;
                ensure!(coordinator.health().await["ready"] == true);
                ensure!(coordinator.health().await["template_generation"] == original_generation);
                ensure!(coordinator
                    .resume_job(&worker, &low_job.wire.job_id)
                    .await?
                    .is_some());
            } else {
                node.state.lock().await.mempool["mempoolminfee"] = json!("0.00001");
                coordinator.refresh_once().await?;
                ensure!(coordinator.health().await["ready"] == true);
                let gate = {
                    let mut state = node.state.lock().await;
                    state.mempool["mempoolminfee"] = json!("0.00002");
                    state.fee_estimate["feerate"] = json!("0.00002");
                    state.pause_network_after = Some(2);
                    state.network_reply_gate.clone()
                };
                let refresh = {
                    let coordinator = coordinator.clone();
                    tokio::spawn(async move { coordinator.refresh_once().await })
                };
                tokio::time::timeout(Duration::from_secs(5), gate.paused.notified()).await?;
                // The higher live floor is visible before the rebuilt bundle
                // can publish; readiness cannot advertise the old low-fee work.
                ensure!(coordinator.health().await["ready"] == false);
                gate.release.notify_one();
                refresh.await??;
                ensure!(coordinator.health().await["ready"] == true);
                ensure!(
                    coordinator.health().await["template_generation"] != original_generation,
                    "changed fee reused old bundle"
                );
                let high_job = coordinator.build_job(&worker, &extra, 1e-12, 0.0).await?;
                ensure!(
                    high_job
                        .context
                        .prepared
                        .fee
                        .unwrap()
                        .market_fee_rate_sats_per_1000_weight
                        == 2000
                );
                coordinator
                    .persist_issued_job(&worker, &high_job, 0, Duration::from_secs(60))
                    .await?;
                // The pool is healthy again, but the persisted old fee remains unsafe.
                ensure!(coordinator
                    .resume_job(&worker, &low_job.wire.job_id)
                    .await
                    .is_err());
                ensure!(coordinator
                    .submit(&worker, &low_job, low_proof.clone(), false.into())
                    .await
                    .is_err());
                {
                    let mut state = node.state.lock().await;
                    state.mempool["mempoolminfee"] = json!("0.00001");
                    state.fee_estimate["feerate"] = json!("0.00001");
                }
                coordinator.refresh_once().await?;
                let current = coordinator.build_job(&worker, &extra, 1e-12, 0.0).await?;
                ensure!(
                    current
                        .context
                        .prepared
                        .fee
                        .unwrap()
                        .market_fee_rate_sats_per_1000_weight
                        == 1000
                );
                // A different automatic estimate does not invalidate an older,
                // higher fee that still meets the newly observed live floor.
                ensure!(coordinator
                    .resume_job(&worker, &high_job.wire.job_id)
                    .await?
                    .is_some());
                coordinator
                    .submit(&worker, &high_job, share_proof(&high_job)?, false.into())
                    .await?;
            }
            let count: i64 =
                sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger WHERE accepted")
                    .fetch_one(&coordinator.ledger.pool)
                    .await?;
            ensure!(
                count == if explicit { 1 } else { 2 },
                "unsafe fee work credited shares"
            );
            Ok::<_, anyhow::Error>(())
        }
        .await;
        coordinator.ledger.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
            .execute(&admin)
            .await?;
        result?;
    }
    admin.close().await;
    Ok(())
}
