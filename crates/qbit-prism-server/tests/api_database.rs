//! Run with PRISM_TEST_DATABASE_URL pointing to a disposable PostgreSQL database.
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    routing::post,
    Json, Router,
};
use qbit_prism_server::api::{router, ApiConfig, ApiState};
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool,
};
use std::str::FromStr;
use tower::ServiceExt;

async fn get(app: &Router, path: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 10_000_000).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}
async fn rpc(Json(input): Json<Value>) -> Json<Value> {
    let result = match input["method"].as_str().unwrap() {
        "getblockchaininfo" => {
            json!({"chain":"regtest","blocks":10,"bestblockhash":"b".repeat(64),"initialblockdownload":false})
        }
        "getblocktemplate" => json!({"bits":"207fffff","coinbasevalue":5000000000u64}),
        "getnetworkinfo" => json!({"connections":2}),
        _ => Value::Null,
    };
    Json(json!({"result":result,"error":null,"id":"prism-public"}))
}

#[tokio::test]
async fn shared_database_serves_all_contracts_and_global_reward_ranks() {
    let Some(url) = gate::database_url(gate::site!()).expect("integration gate") else {
        return;
    };
    let admin = PgPool::connect(&url).await.unwrap();
    let schema = format!("api_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await
        .unwrap();
    let options = PgConnectOptions::from_str(&url)
        .unwrap()
        .options([("search_path", schema.as_str())]);
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("../../qbit-prism/sql/001_share_ledger.sql"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("../migrations/002_multi_instance.sql"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("../migrations/003_2x_compatibility.sql"))
        .execute(&pool)
        .await
        .unwrap();
    for (id, miner, worker, difficulty, writer, seconds) in [
        ("1", "alice", "rig-a", 6000000i64, "server-a", 30i32),
        ("2", "bob", "rig-b", 4000000, "server-b", 20),
    ] {
        sqlx::query("INSERT INTO qbit_share_ledger (share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,writer_id,writer_epoch) VALUES ($1,$2,$2,decode(repeat($3,64),'hex'),$4::bigint,1000000,10,$1,clock_timestamp()-make_interval(secs=>$5),0,clock_timestamp()-make_interval(secs=>$5),$6,1)").bind(format!("{miner}.{worker}:{id}")).bind(miner).bind(id).bind(difficulty).bind(seconds as f64).bind(writer).execute(&pool).await.unwrap();
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, Router::new().route("/", post(rpc)))
            .await
            .unwrap()
    });
    let config = ApiConfig {
        rpc_url: format!("http://{address}/"),
        cache_enabled: false,
        instance_id: "server-a".into(),
        ..ApiConfig::default()
    };
    let app = router(ApiState::new(
        pool.clone(),
        config,
        std::sync::Arc::new(qbit_prism_server::metrics::Metrics::default()),
    ));
    let (status, fresh) = get(&app, "/public/v1/miners/alice").await;
    assert_eq!(status, StatusCode::OK, "{fresh}");
    assert!(fresh["estimated_next_block"]["estimated_reward_bits"].is_null());
    let (status, missing) = get(&app, &format!("/audit/blocks/{}/bundle", "a".repeat(64))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(missing["block_hash"], "a".repeat(64));
    let hash = "a".repeat(64);
    let commitment = "c".repeat(64);
    let audit_hash = "d".repeat(64);
    let audit = json!({"schema":"qbit.prism.audit-bundle.v1","found_block":{"block_height":10,"coinbase_value_sats":5000000000u64,"network_difficulty":1000000},"settlement_mode_decision":{"mode":"direct_coinbase"},"audit_commitment_leaves_hex":[commitment]});
    sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state) VALUES($1,10,$2,$3,$4,'confirmed')").bind(&hash).bind("b".repeat(64)).bind("e".repeat(64)).bind("f".repeat(64)).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO qbit_pool_audit_bundles(block_hash,audit_bundle,audit_bundle_sha256,coinbase_tx_hex,found_block_bits,found_block_network_difficulty,found_block_coinbase_value_sats) VALUES($1,$2,$3,'00','207fffff',1000000,5000000000)").bind(&hash).bind(&audit).bind(&audit_hash).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO qbit_payout_carry_forward(block_height,block_hash,miner_id,payout_order_key,p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,onchain_amount_sats,carry_forward_balance_sats,action) VALUES(10,$1,'alice','alice',decode(repeat('1',64),'hex'),100,0,100,0,100,'accrued')").bind(&hash).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO qbit_pool_payout_entries(block_hash,block_height,miner_id,payout_order_key,p2mr_program,onchain_amount_sats,carry_forward_balance_sats,action) VALUES($1,10,'alice','alice',decode(repeat('1',64),'hex'),0,100,'accrued')").bind(&hash).execute(&pool).await.unwrap();
    let (status, summary) = get(&app, "/public/v1/pool-summary").await;
    assert_eq!(status, StatusCode::OK, "{summary}");
    assert_eq!(summary["network"]["network_difficulty"], "1000000");
    assert_eq!(summary["pool"]["participants_3h"], 2);
    assert_eq!(summary["pool"]["reward_window"]["included_share_count"], 2);
    let (status, reward) = get(
        &app,
        "/public/v1/leaderboard?window=reward&recipient_id=bob",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{reward}");
    assert_eq!(reward["schema"], "prism.dashboard.leaderboard.v2");
    assert_eq!(reward["window"]["counted_window_weight"], "8000000");
    assert_eq!(reward["totals"]["participant_count"], 2);
    assert_eq!(reward["rows"][0]["rank"], 2);
    assert_eq!(reward["rows"][0]["counted_share_difficulty"], "4000000");
    assert_eq!(reward["rows"][0]["share_percent"], "50");
    let (status, miner) = get(&app, "/public/v1/miners/alice").await;
    assert_eq!(status, StatusCode::OK, "{miner}");
    assert_eq!(miner["owed_balance_bits"], 100);
    assert_eq!(miner["workers"][0]["worker_name"], "rig-a");
    assert_eq!(
        miner["estimated_next_block"]["estimated_reward_bits"],
        2500000000u64
    );
    for path in [
        "/public/v1/blocks".into(),
        "/public/v1/leaderboard".into(),
        "/public/v1/leaderboard?search=alice".into(),
        "/public/v1/hashrate-series?range=1w&bucket=5m".into(),
        "/public/v1/hashrate-series?subject=miner:alice&range=all".into(),
        "/public/v1/mining-configuration".into(),
        "/public/v1/miners/alice/earnings".into(),
        "/public/v1/miners/alice/payouts".into(),
        "/public/v1/miners/alice/workers".into(),
        "/public/v1/fanouts/pending".into(),
        format!("/public/v1/blocks/{hash}/settlement-artifacts"),
        format!("/public/v1/artifacts/{audit_hash}"),
        "/owed".into(),
        "/owed-balances".into(),
        "/audit/carry-forward-integrity".into(),
        "/audit/ledger-integrity".into(),
        "/miners/alice/status".into(),
        "/payouts/alice/status".into(),
        "/audit/fanouts/pending".into(),
        format!("/audit/blocks/{hash}/payouts"),
        format!("/audit/block/{hash}"),
        format!("/audit/blocks/{hash}/bundle"),
        format!("/audit/commitments/{commitment}/bundle"),
        format!(
            "/audit/share-window?anchor={}&network_difficulty=1000000",
            chrono::Utc::now().timestamp_millis()
        ),
    ] {
        let (status, body) = get(&app, &path).await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
    }
    let ctv_hash = "9".repeat(64);
    let fanout_hash = "8".repeat(64);
    let manifest_hash = hex::encode(sha2::Sha256::digest(b"{\"z\":1,\"a\":2}"));
    let set_hash = hex::encode(sha2::Sha256::digest(b"{\"z\":2,\"a\":1}"));
    sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state) VALUES($1,11,$2,$3,$4,'confirmed')").bind(&ctv_hash).bind(&hash).bind("5".repeat(64)).bind("4".repeat(64)).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO qbit_ctv_fanout_sets(block_hash,manifest_set_json,manifest_set,manifest_set_sha256,settlement_mode,parent_coinbase_txid,parent_coinbase_tx_hex,fanout_count,fanout_output_sum_sats,covenant_output_value_sats) VALUES($1,'{\"z\":2,\"a\":1}','{\"z\":2,\"a\":1}',$2,'ctv_fanout',$3,'00',1,100,100)").bind(&ctv_hash).bind(&set_hash).bind("5".repeat(64)).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO qbit_ctv_fanout_artifacts(fanout_txid,block_hash,manifest_set_sha256,manifest_json,manifest,manifest_sha256,precommitment_sha256,ctv_hash,commitment_witness_leaf_hex,chunk_index,chunk_count,parent_coinbase_txid,parent_coinbase_vout,fanout_tx_template_hex,fanout_tx_hex,anchor_vout,covenant_output_value_sats,fanout_output_sum_sats,settlement_status) VALUES($1,$2,$3,'{\"z\":1,\"a\":2}','{\"z\":1,\"a\":2}',$4,$4,$4,$4,0,1,$5,0,'00','00',1,100,100,'broadcastable')").bind(&fanout_hash).bind(&ctv_hash).bind(&set_hash).bind(&manifest_hash).bind("5".repeat(64)).execute(&pool).await.unwrap();
    for path in [
        format!("/public/v1/fanouts/{fanout_hash}"),
        format!("/public/v1/blocks/{ctv_hash}/settlement-artifacts"),
        format!("/public/v1/artifacts/{manifest_hash}"),
        format!("/public/v1/artifacts/{set_hash}"),
        format!("/audit/blocks/{ctv_hash}/ctv-fanouts"),
        format!("/audit/blocks/{ctv_hash}/ctv-fanout-manifest-set"),
        format!("/audit/fanouts/{fanout_hash}/status"),
        "/audit/latest".into(),
    ] {
        let (status, body) = get(&app, &path).await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
    }
    let (_, pending) = get(&app, "/public/v1/fanouts/pending").await;
    assert_eq!(pending["pagination"]["total_count"], 1);
    assert_eq!(pending["rows"][0]["cpfp_anchor_spendable"], true);
    assert_eq!(pending["rows"][0]["broadcastable_at_height"], 1011);
    assert_eq!(pending["rows"][0]["fanout_tx_sha256"], fanout_hash);
    // The router has caching disabled so every request observes the committed
    // retry deadline. A transient failure remains discoverable once due, while
    // delayed retries and terminal states stay outside both pending feeds.
    for (settlement_status, delay_seconds, expected_count) in [
        ("failed", Some(-3600i64), 1usize),
        ("failed", Some(3600), 0),
        ("failed", None, 1),
        ("confirmed", None, 0),
        ("reorged", None, 0),
        ("broadcastable", None, 1),
    ] {
        sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET settlement_status=$2,next_broadcast_attempt_at=clock_timestamp()+$3::bigint*interval '1 second' WHERE fanout_txid=$1")
            .bind(&fanout_hash)
            .bind(settlement_status)
            .bind(delay_seconds)
            .execute(&pool)
            .await
            .unwrap();
        for path in ["/public/v1/fanouts/pending", "/audit/fanouts/pending"] {
            let (status, pending) = get(&app, path).await;
            assert_eq!(status, StatusCode::OK, "{path}: {pending}");
            assert_eq!(
                pending["rows"].as_array().unwrap().len(),
                expected_count,
                "{path}: status={settlement_status}, retry delay={delay_seconds:?}"
            );
            let public = path.starts_with("/public/");
            let reported_count = if public {
                &pending["pagination"]["total_count"]
            } else {
                &pending["count"]
            };
            assert_eq!(reported_count, &json!(expected_count));
            if expected_count > 0 {
                assert_eq!(pending["rows"][0]["fanout_txid"], fanout_hash);
                assert_eq!(
                    pending["rows"][0][if public {
                        "status"
                    } else {
                        "settlement_status"
                    }],
                    settlement_status
                );
            }
        }
    }
    for (chain_state, maturity_state, expected_count) in [
        ("prepared", "immature", 0usize),
        ("inactive", "immature", 0),
        ("confirmed", "immature", 1),
        ("confirmed", "mature", 1),
    ] {
        sqlx::query("UPDATE qbit_pool_blocks SET chain_state=$2,maturity_state=$3,matured_at=CASE WHEN $3='mature' THEN clock_timestamp() ELSE NULL END WHERE block_hash=$1")
            .bind(&ctv_hash)
            .bind(chain_state)
            .bind(maturity_state)
            .execute(&pool)
            .await
            .unwrap();
        for path in ["/public/v1/fanouts/pending", "/audit/fanouts/pending"] {
            let (status, pending) = get(&app, path).await;
            assert_eq!(status, StatusCode::OK, "{path}: {pending}");
            assert_eq!(
                pending["rows"].as_array().unwrap().len(),
                expected_count,
                "{path}: parent {chain_state}/{maturity_state}"
            );
        }
        // Discovery excludes candidates, but the explicit status remains
        // available to explain the stored artifact's parent chain state.
        let (status, audit) = get(&app, &format!("/audit/fanouts/{fanout_hash}/status")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(audit["chain_state"], chain_state);
    }
    let (status, empty) = get(&app, "/public/v1/miners/alice/payouts?page=2&limit=1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(empty["pagination"]["total_count"], 1);
    assert_eq!(empty["rows"], json!([]));
    // Native range-backed audit bodies and legacy filesystem bodies expose
    // identical logical JSON, and neither can claim an incorrect canonical SHA.
    let mut scoped_url = url::Url::parse(&url).unwrap();
    scoped_url
        .query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let ledger = qbit_prism_server::ledger::Ledger::connect(
        scoped_url.as_str(),
        "api-hydration".into(),
        4,
        false,
    )
    .await
    .unwrap();
    let snapshot = ledger.snapshot(1_000_000).await.unwrap();
    let manifest_key =
        qbit_pool_builder::ManifestSigningKey::from_seed_hex(&"42".repeat(32)).unwrap();
    let ledger_key =
        qbit_pool_builder::ManifestSigningKey::from_seed_hex(&"43".repeat(32)).unwrap();
    let audit = qbit_prism::build_audit_bundle(
        snapshot.shares.clone(),
        qbit_prism::FoundBlock {
            block_height: 12,
            coinbase_value_sats: 500_000_000,
            network_difficulty: 1_000_000,
            anchor_job_issued_at_ms: snapshot.anchor_ms,
        },
        snapshot.prior_balances.clone(),
        qbit_prism::PayoutPolicy::day_one_default(),
        &manifest_key,
        &ledger_key,
    )
    .unwrap();
    use sha2::{Digest, Sha256};
    let canonical = qbit_prism::canonical_audit_bundle_bytes(&audit).unwrap();
    let digest = hex::encode(Sha256::digest(&canonical));
    let share_digest = hex::encode(Sha256::digest(
        serde_json::to_vec(&snapshot.shares).unwrap(),
    ));
    let logical = serde_json::to_value(&audit).unwrap();
    let mut metadata = logical.clone();
    metadata.as_object_mut().unwrap().remove("shares");
    sqlx::query("INSERT INTO qbit_prism_audit_snapshots(snapshot_sha256,first_share_seq,last_share_seq,anchor_ms,share_count) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING").bind(&share_digest).bind(snapshot.shares.first().unwrap().share_seq as i64).bind(snapshot.shares.last().unwrap().share_seq as i64).bind(snapshot.anchor_ms).bind(snapshot.shares.len()as i64).execute(&pool).await.unwrap();
    let native_hash = "3".repeat(64);
    sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state) VALUES($1,12,$2,$3,$4,'confirmed')").bind(&native_hash).bind(&ctv_hash).bind("2".repeat(64)).bind("1".repeat(64)).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO qbit_pool_audit_bundles(block_hash,audit_bundle,audit_bundle_sha256,coinbase_tx_hex,share_snapshot_sha256) VALUES($1,$2,$3,'00',$4)").bind(&native_hash).bind(&metadata).bind(&digest).bind(&share_digest).execute(&pool).await.unwrap();
    let (status, restored) = get(&app, &format!("/public/v1/artifacts/{digest}")).await;
    assert_eq!(status, StatusCode::OK, "{restored}");
    assert_eq!(restored, logical);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/public/v1/artifacts/{digest}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = to_bytes(response.into_body(), 10_000_000).await.unwrap();
    assert_eq!(
        bytes.as_ref(),
        canonical.as_slice(),
        "native HTTP body must retain canonical field ordering"
    );
    assert_eq!(hex::encode(Sha256::digest(&bytes)), digest);
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=jsonb_set(audit_bundle,'{found_block,coinbase_value_sats}','1') WHERE block_hash=$1").bind(&native_hash).execute(&pool).await.unwrap();
    let (status, corrupt) = get(&app, &format!("/public/v1/artifacts/{digest}")).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(corrupt["error"]["message"], "internal server error");
    let dir = tempfile::tempdir().unwrap();
    let external = dir.path().join("legacy-audit.json");
    std::fs::write(&external, &canonical).unwrap();
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=NULL,share_snapshot_sha256=NULL,body_uri=$2 WHERE block_hash=$1").bind(&native_hash).bind(external.to_str().unwrap()).execute(&pool).await.unwrap();
    let (status, restored) = get(&app, &format!("/public/v1/artifacts/{digest}")).await;
    assert_eq!(status, StatusCode::OK, "{restored}");
    assert_eq!(restored, logical);
    std::fs::write(&external, b"{}").unwrap();
    let (status, corrupt) = get(&app, &format!("/public/v1/artifacts/{digest}")).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(corrupt["error"]["message"], "internal server error");
    ledger.pool.close().await;
    server.abort();
    pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

#[tokio::test]
async fn accepted_public_blocks_and_earnings_follow_confirmed_chain_state() {
    let Some(url) = gate::database_url(gate::site!()).expect("integration gate") else {
        return;
    };
    let admin = PgPool::connect(&url).await.unwrap();
    let schema = format!("api_states_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await
        .unwrap();
    let options = PgConnectOptions::from_str(&url)
        .unwrap()
        .options([("search_path", schema.as_str())]);
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("../../qbit-prism/sql/001_share_ledger.sql"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("../migrations/002_multi_instance.sql"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("../migrations/003_2x_compatibility.sql"))
        .execute(&pool)
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, Router::new().route("/", post(rpc)))
            .await
            .unwrap()
    });
    let app = router(ApiState::new(
        pool.clone(),
        ApiConfig {
            rpc_url: format!("http://{address}/"),
            cache_enabled: false,
            ..ApiConfig::default()
        },
        std::sync::Arc::new(qbit_prism_server::metrics::Metrics::default()),
    ));
    let mut hashes = Vec::new();
    // Unaccepted candidates have greater heights and distinct values, so accidentally
    // treating them as accepted changes latest-block and reward estimates too.
    for (index, (chain_state, height, maturity, reward)) in [
        ("prepared", 20i64, "immature", 1_000_000i64),
        ("inactive", 21, "immature", 2_000_000),
        ("confirmed", 11, "immature", 3_000_000),
        ("confirmed", 10, "mature", 4_000_000),
    ]
    .into_iter()
    .enumerate()
    {
        let hash = format!("{:064x}", index + 1);
        let coinbase = format!("{:064x}", index + 10);
        let manifest = format!("{:064x}", index + 100);
        sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state,maturity_state,matured_at) VALUES($1,$2,$1,$3,$4,$5,$6,CASE WHEN $6='mature' THEN clock_timestamp() ELSE NULL END)")
            .bind(&hash).bind(height).bind(&coinbase).bind(&manifest).bind(chain_state).bind(maturity).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO qbit_pool_audit_bundles(block_hash,audit_bundle,audit_bundle_sha256,coinbase_tx_hex,found_block_bits,found_block_network_difficulty,found_block_coinbase_value_sats) VALUES($1,'{}',$2,'00','207fffff',1000000,$3)")
            .bind(&hash).bind(format!("{:064x}", index + 200)).bind(reward).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO qbit_share_ledger(share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,writer_id,writer_epoch) VALUES($1,'alice','alice',decode(repeat('1',64),'hex'),1000000,1000000,$2,$3,clock_timestamp()-interval '20 seconds',0,clock_timestamp()-interval '10 seconds','api-lifecycle',1)")
            .bind(format!("alice.lifecycle:{hash}")).bind(height - 1).bind(&hash).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO qbit_payout_carry_forward(block_height,block_hash,miner_id,payout_order_key,p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,onchain_amount_sats,carry_forward_balance_sats,action,maturity_state) VALUES($1,$2,'alice','alice',decode(repeat('1',64),'hex'),$3,0,$3,$3,0,'onchain',$4)")
            .bind(height).bind(&hash).bind(reward).bind(maturity).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO qbit_pool_payout_entries(block_hash,block_height,miner_id,payout_order_key,p2mr_program,onchain_amount_sats,carry_forward_balance_sats,action,maturity_state) VALUES($1,$2,'alice','alice',decode(repeat('1',64),'hex'),$3,0,'onchain',$4)")
            .bind(&hash).bind(height).bind(reward).bind(maturity).execute(&pool).await.unwrap();
        hashes.push(hash);
    }
    // These newer candidate payouts exceed the legacy status route's limit of
    // 50. Filtering after LIMIT would hide both older confirmed payments.
    sqlx::query("WITH candidates AS (INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state) SELECT lpad(to_hex(1000+n),64,'0'),100+n,repeat('0',64),lpad(to_hex(2000+n),64,'0'),lpad(to_hex(3000+n),64,'0'),CASE n%3 WHEN 0 THEN 'prepared' WHEN 1 THEN 'inactive' ELSE 'rejected' END FROM generate_series(1,60) n RETURNING block_hash,block_height) INSERT INTO qbit_pool_payout_entries(block_hash,block_height,miner_id,payout_order_key,p2mr_program,onchain_amount_sats,carry_forward_balance_sats,action) SELECT block_hash,block_height,'alice','alice',decode(repeat('1',64),'hex'),9000000,0,'onchain' FROM candidates")
        .execute(&pool)
        .await
        .unwrap();
    for stage in 0..3 {
        if stage > 0 {
            let hash = if stage == 1 { &hashes[0] } else { &hashes[2] };
            sqlx::query("UPDATE qbit_pool_blocks SET chain_state='inactive' WHERE block_hash=$1")
                .bind(hash)
                .execute(&pool)
                .await
                .unwrap();
        }
        let expected_hashes = if stage < 2 {
            vec![hashes[2].clone(), hashes[3].clone()]
        } else {
            vec![hashes[3].clone()]
        };
        let expected_count = expected_hashes.len();
        let earnings = if stage < 2 { 7_000_000 } else { 4_000_000 };
        let pending = if stage < 2 { 3_000_000 } else { 0 };
        let next_reward = if stage < 2 { 3_000_000 } else { 4_000_000 };
        let (status, blocks) = get(&app, "/public/v1/blocks").await;
        assert_eq!(status, StatusCode::OK, "{blocks}");
        assert_eq!(
            blocks["pagination"]["total_count"], expected_count,
            "stage {stage}"
        );
        assert_eq!(
            blocks["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|row| row["hash"].as_str().unwrap())
                .collect::<Vec<_>>(),
            expected_hashes
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
        );
        let (_, page) = get(&app, "/public/v1/blocks?page=2&limit=1").await;
        assert_eq!(page["pagination"]["total_count"], expected_count);
        assert_eq!(page["rows"].as_array().unwrap().len(), expected_count - 1);
        let (status, summary) = get(&app, "/public/v1/pool-summary").await;
        assert_eq!(status, StatusCode::OK, "{summary}");
        assert_eq!(summary["pool"]["blocks_found_total"], expected_count);
        assert_eq!(summary["pool"]["prism_blocks_total"], expected_count);
        assert_eq!(summary["pool"]["total_mined_bits"], earnings);
        assert_eq!(summary["pool"]["latest_block"]["hash"], expected_hashes[0]);
        for (path, count_field) in [
            ("/public/v1/leaderboard", "blocks_found"),
            ("/public/v1/leaderboard?window=reward", "blocks_found_total"),
        ] {
            let (status, board) = get(&app, path).await;
            assert_eq!(status, StatusCode::OK, "{path}: {board}");
            assert_eq!(board["rows"][0]["recipient_id"], "alice");
            assert_eq!(
                board["rows"][0][count_field], expected_count,
                "{path}: stage {stage}"
            );
        }
        let (status, miner) = get(&app, "/public/v1/miners/alice").await;
        assert_eq!(status, StatusCode::OK, "{miner}");
        assert_eq!(miner["lifetime_earnings_bits"], earnings);
        assert_eq!(miner["pending_maturity_bits"], pending);
        assert_eq!(
            miner["estimated_next_block"]["estimated_reward_bits"],
            next_reward
        );
        assert_eq!(
            miner["recent_payouts"].as_array().unwrap().len(),
            expected_count
        );
        for path in [
            "/public/v1/miners/alice/earnings",
            "/public/v1/miners/alice/payouts",
        ] {
            let (status, rows) = get(&app, path).await;
            assert_eq!(status, StatusCode::OK, "{path}: {rows}");
            assert_eq!(
                rows["pagination"]["total_count"], expected_count,
                "{path}: stage {stage}"
            );
            assert_eq!(
                rows["rows"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|row| row["block_hash"].as_str().unwrap())
                    .collect::<Vec<_>>(),
                expected_hashes
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
            );
        }
        for path in ["/miners/alice/status", "/payouts/alice/status"] {
            let (status, history) = get(&app, path).await;
            assert_eq!(status, StatusCode::OK, "{path}: {history}");
            assert_eq!(history["owed_balance_sats"], 0);
            assert_eq!(
                history["recent_payouts"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|row| row["block_hash"].as_str().unwrap())
                    .collect::<Vec<_>>(),
                expected_hashes
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
                "{path}: stage {stage}; newer candidate rows must not consume the history limit"
            );
        }
        for index in [0usize, 1] {
            let (status, audit) =
                get(&app, &format!("/audit/blocks/{}/payouts", hashes[index])).await;
            assert_eq!(status, StatusCode::OK, "{audit}");
            assert_eq!(audit["rows"].as_array().unwrap().len(), 1);
            assert_eq!(
                audit["rows"][0]["chain_state"],
                if index == 0 && stage == 0 {
                    "prepared"
                } else {
                    "inactive"
                }
            );
        }
    }
    server.abort();
    pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
