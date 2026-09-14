use super::*;
use qbit_prism_server::config::Config;
use serde_json::Value;
use std::{process::Output, time::Duration};
use tokio::process::Command;

#[allow(dead_code)]
#[path = "fake_qbitd.rs"]
mod fake;

async fn setup(db: &Database) -> Result<(Ledger, fake::FakeNode, Config)> {
    let ledger = db.ledger("frontend-a").await?;
    let node = fake::FakeNode::open().await?;
    let mut config = fake::coordinator_config(db.url.clone(), &node, "operator")?;
    config.username_fallback = Some("recovery-test-fallback".into());
    ledger
        .configure(
            &config.fingerprint(&"00".repeat(32))?,
            &qbit_prism_server::ledger::SignerKeys {
                manifest_key_hex: "11".repeat(32),
                ledger_key_hex: "22".repeat(32),
            },
        )
        .await?;
    Ok((ledger, node, config))
}

async fn halt(ledger: &Ledger, message: &str) -> Result<()> {
    sqlx::query("UPDATE qbit_prism_cluster SET fatal_error=$1 WHERE singleton")
        .bind(message)
        .execute(&ledger.pool)
        .await?;
    Ok(())
}

async fn stopped(ledger: &Ledger) -> Result<()> {
    ledger.heartbeat(json!({"state":"stopped"})).await
}

async fn block(ledger: &Ledger, hash: &str, height: i64, mature: bool) -> Result<()> {
    sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state,maturity_state,matured_at) VALUES($1,$2,'parent','coinbase','manifest',$3,$4,CASE WHEN $5 THEN clock_timestamp() ELSE NULL END)")
        .bind(hash).bind(height).bind(if mature {"confirmed"} else {"prepared"})
        .bind(if mature {"mature"} else {"immature"}).bind(mature).execute(&ledger.pool).await?;
    Ok(())
}

async fn cli(db: &Database, node: &fake::FakeNode, args: &[&str]) -> Result<Output> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_qbit-prism-server"));
    for (key, _) in
        std::env::vars().filter(|(key, _)| key.starts_with("PRISM_") || key.starts_with("QBIT_"))
    {
        command.env_remove(key);
    }
    command
        .args(args)
        .kill_on_drop(true)
        .env("PRISM_DATABASE_URL", &db.url)
        .env("QBIT_RPC_URL", &node.url)
        .env("QBIT_CHAIN", "testnet")
        .env("PRISM_USERNAME_FALLBACK_ADDRESS", "recovery-test-fallback")
        .env("PRISM_ALLOW_TEST_SIGNING_SEEDS", "1")
        .env("PRISM_RUNTIME_WORKERS", "2");
    if args != ["fatal-state", "show"] {
        command
            .env("PRISM_MANIFEST_SIGNING_SEED_HEX", "11".repeat(32))
            .env("PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX", "22".repeat(32))
            .env(
                "PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX",
                ManifestSigningKey::from_seed_hex(&"22".repeat(32))?.public_key_hex(),
            );
    }
    Ok(tokio::time::timeout(Duration::from_secs(20), command.output()).await??)
}

async fn unchanged(ledger: &Ledger, expected: &Value) -> Result<()> {
    assert_eq!(ledger.fatal_state().await?, *expected);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM qbit_prism_fatal_state_events")
            .fetch_one(&ledger.pool)
            .await?,
        0
    );
    assert!(ledger.append(share(999), None).await.is_err());
    Ok(())
}

#[tokio::test]
async fn show_and_clear_cli_resume_appends_and_record_operator_decision() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (ledger, node, _) = setup(&db).await?;
    let healthy = cli(&db, &node, &["fatal-state", "show"]).await?;
    assert!(
        healthy.status.success(),
        "{}",
        String::from_utf8_lossy(&healthy.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&healthy.stdout)?["halted"],
        false
    );
    let hash = "ab".repeat(32);
    block(&ledger, &hash, 90, false).await?;
    let message = format!("mature pool block disconnected: {hash}; manual reconciliation required");
    halt(&ledger, &message).await?;
    let initial = ledger.fatal_state().await?;
    assert!(initial["set_at"].is_string());
    sqlx::query("UPDATE qbit_prism_cluster SET updated_at=clock_timestamp() WHERE singleton")
        .execute(&ledger.pool)
        .await?;
    assert_eq!(
        ledger.fatal_state().await?,
        initial,
        "set time must not track unrelated writes"
    );
    let shown = cli(&db, &node, &["fatal-state", "show"]).await?;
    assert!(!shown.status.success());
    assert_eq!(serde_json::from_slice::<Value>(&shown.stdout)?, initial);
    assert!(String::from_utf8_lossy(&shown.stdout).contains(&message));
    assert_eq!(initial["block_hash"], hash);
    assert!(ledger.append(share(1), None).await.is_err());
    stopped(&ledger).await?;
    sqlx::query("INSERT INTO qbit_prism_instances(instance_id,status) VALUES('frontend-b','{\"state\":\"drained\"}')").execute(&ledger.pool).await?;
    let reason = "INC-290: Anatolie reviewed chain restoration and payout evidence";
    let cleared = cli(&db, &node, &["fatal-state", "clear", "--reason", reason]).await?;
    assert!(
        cleared.status.success(),
        "{}",
        String::from_utf8_lossy(&cleared.stderr)
    );
    let event: Value = serde_json::from_slice(&cleared.stdout)?;
    assert_eq!(event["reason"], reason);
    assert_eq!(event["fatal_error"], message);
    assert_eq!(event["fatal_error_set_at"], initial["set_at"]);
    assert_eq!(
        event["operator_identity"],
        sqlx::query_scalar::<_, String>("SELECT session_user::text")
            .fetch_one(&ledger.pool)
            .await?
    );
    assert!(event["cleared_at"].is_string());
    assert_eq!(event["instances"].as_array().unwrap().len(), 2);
    assert_eq!(event["reconciliation"]["blocks_checked"], 1);
    assert_eq!(event["reconciliation"]["integrity"]["mismatch_count"], 0);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT chain_state FROM qbit_pool_blocks WHERE block_hash=$1"
        )
        .bind(&hash)
        .fetch_one(&ledger.pool)
        .await?,
        "confirmed",
        "normal block reconciliation must actually run"
    );
    assert_eq!(
        sqlx::query_scalar::<_, Value>("SELECT to_jsonb(e) FROM qbit_prism_fatal_state_events e")
            .fetch_one(&ledger.pool)
            .await?,
        event
    );
    assert!(ledger.append(share(1), None).await?.inserted);
    assert!(ledger.payout_revision().await? > 0);
    assert!(cli(&db, &node, &["fatal-state", "show"])
        .await?
        .status
        .success());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM qbit_prism_instances")
            .fetch_one(&ledger.pool)
            .await?,
        2,
        "operator tools must not create heartbeats"
    );
    for statement in [
        "UPDATE qbit_prism_fatal_state_events SET reason='changed'",
        "DELETE FROM qbit_prism_fatal_state_events",
        "TRUNCATE qbit_prism_fatal_state_events",
    ] {
        assert!(sqlx::query(statement)
            .execute(&ledger.pool)
            .await
            .unwrap_err()
            .to_string()
            .contains("immutable"));
    }
    assert!(
        !cli(&db, &node, &["fatal-state", "clear", "--reason", reason])
            .await?
            .status
            .success()
    );
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn clear_refuses_live_starting_stale_and_unknown_instances() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (ledger, node, _) = setup(&db).await?;
    halt(&ledger, "test halt").await?;
    ledger
        .heartbeat(json!({"schema":"qbit.prism.audit-health.v1","ready":false}))
        .await?;
    sqlx::raw_sql("INSERT INTO qbit_prism_instances(instance_id,status,heartbeat_at) VALUES ('starting','{\"state\":\"starting\"}',clock_timestamp()),('stale-live','{\"ready\":true}',clock_timestamp()-interval '1 day'),('unknown','{}',clock_timestamp())").execute(&ledger.pool).await?;
    let before = ledger.fatal_state().await?;
    let out = cli(
        &db,
        &node,
        &["fatal-state", "clear", "--reason", "reviewed"],
    )
    .await?;
    assert!(!out.status.success());
    let error = String::from_utf8_lossy(&out.stderr);
    for id in ["frontend-a", "starting", "stale-live", "unknown"] {
        assert!(error.contains(id), "{error}");
    }
    unchanged(&ledger, &before).await?;
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn clear_rolls_back_reconciliation_if_mature_block_remains_disconnected() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (ledger, _node, config) = setup(&db).await?;
    let bad = "cd".repeat(32);
    block(&ledger, &bad, 50, true).await?;
    let error = ledger
        .reconcile_blocks(
            &[BlockObservation {
                block_hash: bad.clone(),
                active: false,
            }],
            100,
        )
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("qbit-prism-server fatal-state clear --reason"));
    let before = ledger.fatal_state().await?;
    assert!(before["set_at"].is_string());
    block(&ledger, &"ab".repeat(32), 40, false).await?;
    stopped(&ledger).await?;
    let error = ledger
        .clear_fatal_state(&config, "investigated")
        .await
        .unwrap_err();
    assert!(error.to_string().contains(&bad));
    unchanged(&ledger, &before).await?;
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT chain_state FROM qbit_pool_blocks WHERE block_height=40"
        )
        .fetch_one(&ledger.pool)
        .await?,
        "prepared",
        "earlier reconciliation transitions must roll back"
    );
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn clear_preserves_halt_on_integrity_failure_and_audit_insert_failure() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (ledger, _node, config) = setup(&db).await?;
    block(&ledger, &"ab".repeat(32), 40, false).await?;
    halt(&ledger, "test halt").await?;
    stopped(&ledger).await?;
    let before = ledger.fatal_state().await?;
    // Real materialized-balance drift makes the normal replay report fail.
    sqlx::query("INSERT INTO qbit_payout_carry_forward_current(miner_id,payout_order_key,p2mr_program,balance_sats,active_row_count) VALUES('miner','miner',decode($1,'hex'),100,1)")
        .bind("11".repeat(32)).execute(&ledger.pool).await?;
    let error = ledger
        .clear_fatal_state(&config, "investigated")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("current_drift_count"), "{error}");
    unchanged(&ledger, &before).await?;
    sqlx::query("DELETE FROM qbit_payout_carry_forward_current")
        .execute(&ledger.pool)
        .await?;
    sqlx::raw_sql("CREATE FUNCTION refuse_recovery_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected audit failure'; END; $$; CREATE TRIGGER refuse_recovery_event BEFORE INSERT ON qbit_prism_fatal_state_events FOR EACH ROW EXECUTE FUNCTION refuse_recovery_event()").execute(&ledger.pool).await?;
    let error = ledger
        .clear_fatal_state(&config, "investigated")
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("injected audit failure"),
        "{error}"
    );
    unchanged(&ledger, &before).await?;
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT chain_state FROM qbit_pool_blocks WHERE block_height=40"
        )
        .fetch_one(&ledger.pool)
        .await?,
        "prepared"
    );
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn migration_and_show_preserve_unknown_legacy_set_time_without_registration() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (ledger, node, _) = setup(&db).await?;
    // Model an already-halted pre-010 database, without inventing a set time.
    sqlx::raw_sql("DROP TRIGGER qbit_prism_stamp_fatal_state ON qbit_prism_cluster; DROP FUNCTION qbit_prism_stamp_fatal_state(); DROP TABLE qbit_prism_fatal_state_events; DROP FUNCTION qbit_prism_preserve_fatal_state_events(); ALTER TABLE qbit_prism_cluster DROP COLUMN fatal_error_set_at; DELETE FROM qbit_prism_schema_migrations WHERE version=10").execute(&ledger.pool).await?;
    halt(
        &ledger,
        "deep confirmed CTV fanout disconnected: legacy-tx; manual reconciliation required",
    )
    .await?;
    let shown = cli(&db, &node, &["fatal-state", "show"]).await?;
    assert!(!shown.status.success());
    let state: Value = serde_json::from_slice(&shown.stdout)?;
    assert!(state["set_at"].is_null());
    assert_eq!(state["fanout_txid"], "legacy-tx");
    let before: Value = sqlx::query_scalar("SELECT to_jsonb(i) FROM qbit_prism_instances i")
        .fetch_one(&ledger.pool)
        .await?;
    for _ in 0..2 {
        let output = cli(&db, &node, &["migrate"]).await?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert_eq!(ledger.fatal_state().await?, state);
    assert_eq!(
        sqlx::query_scalar::<_, Value>("SELECT to_jsonb(i) FROM qbit_prism_instances i")
            .fetch_one(&ledger.pool)
            .await?,
        before
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM qbit_prism_schema_migrations WHERE version=10"
        )
        .fetch_one(&ledger.pool)
        .await?,
        1
    );
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn clear_requires_nonblank_reason_before_loading_configuration() -> Result<()> {
    for args in [
        vec!["fatal-state", "clear"],
        vec!["fatal-state", "clear", "--reason", "   "],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_qbit-prism-server"))
            .args(args)
            .env_clear()
            .env("PRISM_RUNTIME_WORKERS", "2")
            .output()
            .await?;
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("--reason"));
    }
    Ok(())
}

#[tokio::test]
async fn deep_fanout_halt_names_recovery_and_refuses_unresolved_confirmation() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (ledger, _node, config) = setup(&db).await?;
    let parent = "ab".repeat(32);
    let txid = "ef".repeat(32);
    block(&ledger, &parent, 10, true).await?;
    sqlx::query("INSERT INTO qbit_ctv_fanout_sets(block_hash,manifest_set_json,manifest_set,manifest_set_sha256,settlement_mode,parent_coinbase_txid,parent_coinbase_tx_hex,fanout_count,fanout_output_sum_sats,covenant_output_value_sats) VALUES($1,'{}','{}','set','ctv_fanout','coinbase','00',1,1,1)")
        .bind(&parent).execute(&ledger.pool).await?;
    sqlx::query("INSERT INTO qbit_ctv_fanout_artifacts(fanout_txid,block_hash,manifest_set_sha256,manifest_json,manifest,manifest_sha256,precommitment_sha256,ctv_hash,commitment_witness_leaf_hex,chunk_index,chunk_count,parent_coinbase_txid,parent_coinbase_vout,fanout_tx_template_hex,fanout_tx_hex,covenant_output_value_sats,fanout_output_sum_sats,settlement_status,confirmed_depth,confirmed_block_hash,confirmed_block_height) VALUES($1,$2,'set','{}','{}','manifest','precommit','ctv','00',0,1,'coinbase',0,'00','00',1,1,'confirmed',1000,$3,20)")
        .bind(&txid).bind(&parent).bind("cd".repeat(32)).execute(&ledger.pool).await?;
    let claim = ledger
        .claim_fanout(60)
        .await?
        .context("missing fanout claim")?;
    ledger
        .halt_fanout_reorg(&claim, ledger.payout_revision().await?)
        .await?;
    let before = ledger.fatal_state().await?;
    assert_eq!(before["fanout_txid"], txid);
    assert!(before["fatal_error"]
        .as_str()
        .unwrap()
        .contains("qbit-prism-server fatal-state clear --reason"));
    stopped(&ledger).await?;
    let error = ledger
        .clear_fatal_state(&config, "reviewed")
        .await
        .unwrap_err();
    assert!(error.to_string().contains(&txid), "{error}");
    unchanged(&ledger, &before).await?;
    // The original confirmation is active again on the fake node.
    sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET confirmed_block_hash=$1")
        .bind(&parent)
        .execute(&ledger.pool)
        .await?;
    let event = ledger
        .clear_fatal_state(&config, "confirmation restored")
        .await?;
    assert_eq!(event["reconciliation"]["deep_fanouts_checked"], 1);
    assert!(ledger.append(share(1), None).await?.inserted);
    db.close(vec![ledger]).await
}

struct RpcProxy {
    url: String,
    state: std::sync::Arc<ProxyState>,
    task: tokio::task::JoinHandle<()>,
}

struct ProxyState {
    upstream: String,
    pause: bool,
    move_tip: bool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

impl Drop for RpcProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl RpcProxy {
    async fn open(node: &fake::FakeNode, pause: bool, move_tip: bool) -> Result<Self> {
        use axum::{extract::State, routing::post, Json, Router};
        let state = std::sync::Arc::new(ProxyState {
            upstream: node.url.clone(),
            pause,
            move_tip,
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let app = Router::new()
            .route(
                "/",
                post(
                    |State(state): State<std::sync::Arc<ProxyState>>,
                     Json(request): Json<Value>| async move {
                        if state.pause && request["method"] == "getblockchaininfo" {
                            state.entered.notify_one();
                            state.release.notified().await;
                        }
                        if state.move_tip && request["method"] == "getbestblockhash" {
                            return Json(
                                json!({"id":request["id"],"result":"99".repeat(32),"error":null}),
                            );
                        }
                        Json(
                            reqwest::Client::new()
                                .post(&state.upstream)
                                .json(&request)
                                .send()
                                .await
                                .unwrap()
                                .json::<Value>()
                                .await
                                .unwrap(),
                        )
                    },
                ),
            )
            .with_state(state.clone());
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/", socket.local_addr()?);
        let task = tokio::spawn(async move {
            axum::serve(socket, app).await.unwrap();
        });
        Ok(Self { url, state, task })
    }
}

#[tokio::test]
async fn recovery_serializes_new_heartbeats_and_concurrent_clear() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (ledger, node, mut config) = setup(&db).await?;
    halt(&ledger, "test halt").await?;
    stopped(&ledger).await?;
    let proxy = RpcProxy::open(&node, true, false).await?;
    config.rpc_url = proxy.url.clone();
    let recovering = ledger.clone();
    let first_config = config.clone();
    let clear = tokio::spawn(async move {
        recovering
            .clear_fatal_state(&first_config, "first operator")
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), proxy.state.entered.notified()).await?;
    let pool = ledger.pool.clone();
    let mut registration = tokio::spawn(async move {
        sqlx::query("INSERT INTO qbit_prism_instances(instance_id,status) VALUES('new-frontend','{\"state\":\"starting\"}')").execute(&pool).await
    });
    let recovering = ledger.clone();
    let mut second = tokio::spawn(async move {
        recovering
            .clear_fatal_state(&config, "second operator")
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut registration)
            .await
            .is_err(),
        "registration raced past the recovery instance snapshot"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut second)
            .await
            .is_err(),
        "second clear raced the first"
    );
    proxy.state.release.notify_one();
    let event = tokio::time::timeout(Duration::from_secs(5), clear).await???;
    assert_eq!(event["reason"], "first operator");
    tokio::time::timeout(Duration::from_secs(5), registration).await???;
    assert!(tokio::time::timeout(Duration::from_secs(5), second)
        .await??
        .is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM qbit_prism_fatal_state_events")
            .fetch_one(&ledger.pool)
            .await?,
        1
    );
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn chain_change_rpc_timeout_and_cancellation_keep_the_halt() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (ledger, node, mut config) = setup(&db).await?;
    halt(&ledger, "test halt").await?;
    stopped(&ledger).await?;
    let before = ledger.fatal_state().await?;
    let moved = RpcProxy::open(&node, false, true).await?;
    config.rpc_url = moved.url.clone();
    let error = ledger
        .clear_fatal_state(&config, "reviewed")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("tip changed"), "{error}");
    unchanged(&ledger, &before).await?;
    let slow = RpcProxy::open(&node, true, false).await?;
    config.rpc_url = slow.url.clone();
    config.rpc_timeout = Duration::from_millis(100);
    let error = ledger
        .clear_fatal_state(&config, "reviewed")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("transport failed"), "{error}");
    unchanged(&ledger, &before).await?;
    let paused = RpcProxy::open(&node, true, false).await?;
    config.rpc_url = paused.url.clone();
    config.rpc_timeout = Duration::from_secs(5);
    let recovering = ledger.clone();
    let clear =
        tokio::spawn(async move { recovering.clear_fatal_state(&config, "reviewed").await });
    tokio::time::timeout(Duration::from_secs(5), paused.state.entered.notified()).await?;
    clear.abort();
    assert!(clear.await.unwrap_err().is_cancelled());
    unchanged(&ledger, &before).await?;
    // Both transaction locks and the table lock must have been released.
    ledger.heartbeat(json!({"state":"stopped"})).await?;
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn recovery_rejects_wrong_cluster_and_unsuitable_node() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (ledger, _node, config) = setup(&db).await?;
    halt(&ledger, "test halt").await?;
    stopped(&ledger).await?;
    let before = ledger.fatal_state().await?;
    let mut wrong_key = config.clone();
    wrong_key.manifest_seed = "33".repeat(32);
    let mut wrong_genesis = config.clone();
    wrong_genesis.expected_genesis_hash = Some("ff".repeat(32));
    let mut wrong_chain = config.clone();
    wrong_chain.chain = "regtest".into();
    let mut low_peers = config.clone();
    low_peers.min_peers = 3;
    for (candidate, expected) in [
        (wrong_key, "fingerprint"),
        (wrong_genesis, "genesis"),
        (wrong_chain, "QBIT_CHAIN"),
        (low_peers, "peers"),
    ] {
        let error = ledger
            .clear_fatal_state(&candidate, "reviewed")
            .await
            .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
        unchanged(&ledger, &before).await?;
    }
    sqlx::query("UPDATE qbit_prism_cluster SET best_chainwork=2")
        .execute(&ledger.pool)
        .await?;
    let error = ledger
        .clear_fatal_state(&config, "reviewed")
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("cumulative chainwork"),
        "{error}"
    );
    unchanged(&ledger, &before).await?;
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn recovery_resolves_fee_address_before_verifying_cluster_fingerprint() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (ledger, _node, mut config) = setup(&db).await?;
    config.fee_address = Some("fee-address".into());
    config.payout_policy.pool_fee_policy = Some(qbit_prism::PoolFeePolicy {
        fee_bps: 100,
        recipient_id: "fee-address".into(),
        order_key: "fee-address".into(),
        p2mr_program_hex: String::new(),
    });
    let mut resolved = config.clone();
    resolved
        .payout_policy
        .pool_fee_policy
        .as_mut()
        .unwrap()
        .p2mr_program_hex = "11".repeat(32);
    // This is the fingerprint Coordinator::new persists after validateaddress.
    sqlx::query("UPDATE qbit_prism_cluster SET config_fingerprint=$1")
        .bind(resolved.fingerprint(&"00".repeat(32))?)
        .execute(&ledger.pool)
        .await?;
    halt(&ledger, "test halt").await?;
    stopped(&ledger).await?;
    ledger
        .clear_fatal_state(&config, "reviewed fee configuration")
        .await?;
    assert!(ledger.append(share(1), None).await?.inserted);
    db.close(vec![ledger]).await
}
