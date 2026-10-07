use super::*;
use qbit_prism_server::{
    config::Config,
    ledger::{HeartbeatHealth, HeartbeatStatus},
};
use serde_json::Value;
use std::{process::Output, time::Duration};
use tokio::process::Command;

use super::fake_qbitd as fake;

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
    ledger.heartbeat(HeartbeatStatus::Stopped).await
}

async fn block(ledger: &Ledger, hash: &str, height: i64, mature: bool) -> Result<()> {
    sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state,maturity_state,matured_at) VALUES($1,$2,'parent','coinbase','manifest',$3,$4,CASE WHEN $5 THEN clock_timestamp() ELSE NULL END)")
        .bind(hash).bind(height).bind(if mature {"confirmed"} else {"prepared"})
        .bind(if mature {"mature"} else {"immature"}).bind(mature).execute(&ledger.pool).await?;
    Ok(())
}

async fn cli(db: &Database, node: &fake::FakeNode, args: &[&str]) -> Result<Output> {
    cli_with_env(db, node, args, &[]).await
}

/// `cli` with extra environment, for a configured PRISM_INSTANCE_ID or a
/// deterministic probe failure. `extra` is applied last, so it overrides.
async fn cli_with_env(
    db: &Database,
    node: &fake::FakeNode,
    args: &[&str],
    extra: &[(&str, &str)],
) -> Result<Output> {
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
    for (key, value) in extra {
        command.env(key, value);
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
    // #737: the tip never moved, so the final tip is the captured one.
    assert_eq!(event["reconciliation"]["tip_extended"], false);
    assert_eq!(
        event["reconciliation"]["final_tip_hash"],
        event["reconciliation"]["tip_hash"]
    );
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

/// The four one-shot commands that write ordinary ledger rows. `self-check`
/// exits nonzero here because nothing serves the audit API; PRISM_AUDIT_PORT=1
/// keeps that probe failing deterministically instead of finding a listener.
/// `self-check` refuses a configuration without a pool fee (#535), so the
/// tools run a 0-bps one.
const TOOLS: [&str; 4] = [
    "import-audits",
    "backfill-ctv",
    "broadcast-ctv",
    "self-check",
];
const TOOL_ENV: [(&str, &str); 5] = [
    ("PRISM_AUDIT_PORT", "1"),
    super::pool_fee::ZERO_BPS_POOL_FEE[0],
    super::pool_fee::ZERO_BPS_POOL_FEE[1],
    super::pool_fee::ZERO_BPS_POOL_FEE[2],
    super::pool_fee::ZERO_BPS_POOL_FEE[3],
];
fn tool_stdout(tool: &str) -> Option<&'static str> {
    match tool {
        "import-audits" => Some("Imported 0 audit bodies"),
        "backfill-ctv" => Some("Backfilled 0 CTV manifest sets"),
        "broadcast-ctv" => Some("Processed 0 CTV fanouts"),
        _ => None,
    }
}

/// Assert a tool exit: the three importers succeed with their count line;
/// `self-check` fails (no audit API) but still prints one complete report
/// with `ok: false`, whose live-instance sample is returned for inspection.
fn assert_tool_exit(tool: &str, output: &Output, halted: bool) -> Result<Option<Value>> {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    if halted {
        assert!(
            !output.status.success(),
            "{tool} ran during a halt: {stdout}"
        );
        assert!(stderr.contains("cluster halted"), "{tool}: {stderr}");
    }
    match tool_stdout(tool) {
        Some(expected) => {
            if !halted {
                assert!(output.status.success(), "{tool}: {stderr}");
                assert!(stdout.contains(expected), "{tool}: {stdout}");
            }
            Ok(None)
        }
        None => {
            assert!(
                !output.status.success(),
                "{tool} must fail without an audit API"
            );
            let report: Value = serde_json::from_slice(&output.stdout)
                .with_context(|| format!("{tool} report: {stdout}; stderr={stderr}"))?;
            assert_eq!(report["ok"], false, "{report}");
            assert!(report["live_instances"]["status"].is_string(), "{report}");
            Ok(Some(report["live_instances"].clone()))
        }
    }
}

async fn instance_count(ledger: &Ledger) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT count(*) FROM qbit_prism_instances")
            .fetch_one(&ledger.pool)
            .await?,
    )
}

async fn frontend_row(ledger: &Ledger) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT to_jsonb(i) FROM qbit_prism_instances i WHERE instance_id='frontend-a'",
    )
    .fetch_one(&ledger.pool)
    .await?)
}

#[tokio::test]
async fn one_shot_tools_register_no_heartbeat_and_keep_the_halt_guard() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (ledger, node, mut config) = setup(&db).await?;
    // #535: `self-check` refuses a configuration without a pool fee, so this
    // cluster pins the 0-bps fee `TOOL_ENV` gives every tool.
    config.payout_policy.pool_fee_policy = Some(super::pool_fee::zero_bps_policy());
    sqlx::query("UPDATE qbit_prism_cluster SET config_fingerprint=$1")
        .bind(config.fingerprint(&"00".repeat(32))?)
        .execute(&ledger.pool)
        .await?;
    // 1. A generated instance ID leaves no row, on success and on failure:
    // `cli` sets no PRISM_INSTANCE_ID, so each command generates its own.
    for tool in TOOLS {
        let output = cli_with_env(&db, &node, &[tool], &TOOL_ENV).await?;
        if let Some(live) = assert_tool_exit(tool, &output, false)? {
            assert_eq!(live["status"], "inactive", "{live}");
        }
        assert_eq!(instance_count(&ledger).await?, 1, "{tool} registered a row");
    }
    // 2. A live frontend's row (status with its session-owner token,
    // heartbeat_at, started_at) is untouched when a tool runs under its ID.
    ledger
        .heartbeat(HeartbeatStatus::Health(HeartbeatHealth::new(
            true,
            Default::default(),
        )))
        .await?;
    let live_row = frontend_row(&ledger).await?;
    assert_eq!(live_row["status"]["ready"], true, "{live_row}");
    assert!(
        live_row["status"]["session_owner_token"].is_string(),
        "{live_row}"
    );
    let shared: Vec<_> = [("PRISM_INSTANCE_ID", "frontend-a")]
        .into_iter()
        .chain(TOOL_ENV)
        .collect();
    for tool in TOOLS {
        let output = cli_with_env(&db, &node, &[tool], &shared).await?;
        if let Some(live) = assert_tool_exit(tool, &output, false)? {
            assert_eq!(live["status"], "observed", "{live}");
            assert_eq!(live["instance_ids"], json!(["frontend-a"]), "{live}");
        }
        assert_eq!(
            frontend_row(&ledger).await?,
            live_row,
            "{tool} touched the live row"
        );
        assert_eq!(instance_count(&ledger).await?, 1, "{tool} registered a row");
    }
    // 3. The halt guard is preserved and failing exits leave no row.
    halt(&ledger, "test halt").await?;
    for tool in TOOLS {
        let output = cli_with_env(&db, &node, &[tool], &TOOL_ENV).await?;
        assert_tool_exit(tool, &output, true)?;
        assert_eq!(
            frontend_row(&ledger).await?,
            live_row,
            "{tool} touched the live row"
        );
        assert_eq!(instance_count(&ledger).await?, 1, "{tool} registered a row");
    }
    // 4. Recovery succeeds once the real frontend stops: no tool row is left
    // for `fatal-state clear` to refuse.
    stopped(&ledger).await?;
    let reason = "INC-381: operator tools ran while frontend-a was live";
    let clear = ["fatal-state", "clear", "--reason", reason];
    let cleared = cli_with_env(&db, &node, &clear, &TOOL_ENV[1..]).await?;
    assert!(
        cleared.status.success(),
        "{}",
        String::from_utf8_lossy(&cleared.stderr)
    );
    let event: Value = serde_json::from_slice(&cleared.stdout)?;
    let instances = event["instances"].as_array().context("instances")?;
    assert_eq!(instances.len(), 1, "{event}");
    assert_eq!(instances[0]["instance_id"], "frontend-a");
    assert_eq!(instances[0]["status"]["state"], "stopped");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM qbit_prism_fatal_state_events")
            .fetch_one(&ledger.pool)
            .await?,
        1
    );
    assert!(ledger.append(share(1), None).await?.inserted);
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
        .heartbeat(HeartbeatStatus::Health(HeartbeatHealth::new(
            false,
            Default::default(),
        )))
        .await?;
    sqlx::raw_sql("INSERT INTO qbit_prism_instances(instance_id,status,heartbeat_at) VALUES ('starting','{\"state\":\"starting\",\"candidate_offer_lifecycle\":1}',clock_timestamp()),('stale-live','{\"ready\":true}',clock_timestamp()-interval '1 day'),('unknown','{}',clock_timestamp())").execute(&ledger.pool).await?;
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

/// #708: one payout program's legacy chain under two labels that differ only
/// in case, as 2.x wrote it, passes the self-check's integrity gate and lets
/// a fatal state clear. A real break under either label still stops both.
#[tokio::test]
async fn self_check_and_clear_accept_a_programs_legacy_chain_across_case_labels() -> Result<()> {
    use super::case_labels;
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (ledger, node, mut config) = setup(&db).await?;
    // #535: `self-check` refuses a configuration without a pool fee.
    config.payout_policy.pool_fee_policy = Some(super::pool_fee::zero_bps_policy());
    sqlx::query("UPDATE qbit_prism_cluster SET config_fingerprint=$1")
        .bind(config.fingerprint(&"00".repeat(32))?)
        .execute(&ledger.pool)
        .await?;
    case_labels::seed(&ledger.pool).await?;
    // The mature blocks are on the node's chain.
    for (height, ..) in case_labels::ROWS {
        node.set_reply(
            "getblockhash",
            json!([height]),
            json!(case_labels::block_hash(height)),
        );
    }
    let self_check = || async {
        let output = cli_with_env(&db, &node, &["self-check"], &TOOL_ENV).await?;
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        let report: Value = serde_json::from_slice(&output.stdout)
            .with_context(|| format!("self-check report; stderr={stderr}"))?;
        Ok::<_, anyhow::Error>((report, stderr))
    };
    // Past the gate: durability is read after it, and the check fails only
    // on the audit API this fixture does not serve.
    let (report, stderr) = self_check().await?;
    assert_eq!(
        report["carry_forward_integrity"]["mismatch_count"], 0,
        "{report}"
    );
    assert!(report["durability"].is_array(), "{report}; {stderr}");
    assert!(
        !stderr.contains("carry-forward integrity failure"),
        "{stderr}"
    );
    // A break under the lowercase label stops it at the gate.
    case_labels::shift(&ledger.pool, case_labels::LAST_LOWER, 1).await?;
    let (report, stderr) = self_check().await?;
    assert_eq!(
        report["carry_forward_integrity"]["mismatch_count"], 1,
        "{report}"
    );
    assert!(report["durability"].is_null(), "{report}");
    assert!(
        stderr.contains("carry-forward integrity failure in mismatch_count"),
        "{stderr}"
    );
    case_labels::shift(&ledger.pool, case_labels::LAST_LOWER, -1).await?;

    halt(&ledger, "test halt").await?;
    stopped(&ledger).await?;
    let before = ledger.fatal_state().await?;
    // A break under the uppercase label refuses the clear and keeps the halt.
    case_labels::shift(&ledger.pool, case_labels::LAST_UPPER, 1).await?;
    let error = ledger
        .clear_fatal_state(&config, "investigated")
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("fatal-state reconciliation failed mismatch_count"),
        "{error}"
    );
    unchanged(&ledger, &before).await?;
    case_labels::shift(&ledger.pool, case_labels::LAST_UPPER, -1).await?;
    // The chain as 2.x wrote it clears.
    let event = ledger.clear_fatal_state(&config, "investigated").await?;
    assert_eq!(
        event["reconciliation"]["integrity"]["mismatch_count"], 0,
        "{event}"
    );
    assert_eq!(event["reconciliation"]["blocks_checked"], 7, "{event}");
    assert_eq!(ledger.fatal_state().await?["halted"], false);
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

/// How the proxy's chain moves away from the node's during a clear (#737).
/// The node's own tip, which the clear captures, is `ab..` at height 100; the
/// proxy reads both from the `getblockchaininfo` that captures them.
#[derive(Clone, Copy, PartialEq, Eq)]
enum TipMove {
    /// Every call reaches the node.
    Still,
    /// Two blocks on top of the captured tip: `getbestblockhash` answers the
    /// upper one, whose headers lead through the lower one to the captured
    /// tip, and every height up to the captured one keeps its block.
    Extend,
    /// Another chain, one block longer: `getbestblockhash` answers its tip,
    /// whose parent at the captured height is another block than the captured
    /// tip. `getblockhash` still answers the captured tip there, as if the
    /// other chain had been best only while its tip was read, so only that
    /// tip's own header shows the reorganization.
    Reorg,
    /// A shorter chain: its tip's header is below the captured height, which
    /// `getblockhash` answers as out of range (-8).
    Shorten,
    /// A tip 1,001 blocks above the captured height.
    Runaway,
    /// A tip the node answers `getblockheader` for as an unknown block (-5).
    Unknown,
    /// Until the first tip check, `getblockhash` answers another block at
    /// every height up to the captured one, as if the node had gone over to a
    /// fork while the clear's loop read it; by that check it is back, two
    /// blocks on top of the captured tip, as `Extend`.
    Flap,
    /// A tip `CRAWL_BLOCKS` above the captured height, each of whose headers
    /// the node answers after `CRAWL_DELAY`, well within the RPC timeout, so
    /// walking it back to the captured tip takes about 25 s.
    Crawl,
}

/// The best block `getbestblockhash` answers once the chain has moved.
fn moved_tip() -> String {
    "99".repeat(32)
}

/// The block a fork holds where the node's chain holds another.
fn fork_block() -> String {
    "98".repeat(32)
}

/// The lower of the two blocks `Extend` grows the chain by.
fn extension_block() -> String {
    "9a".repeat(32)
}

/// How far above the captured height `Crawl` puts the tip, and how long the
/// node takes over each header.
const CRAWL_BLOCKS: u64 = 500;
const CRAWL_DELAY: Duration = Duration::from_millis(50);

/// `Crawl`'s block at `at`, above the captured height: its hash names it.
fn crawl_block(at: u64) -> String {
    format!("c7{at:062x}")
}

struct ProxyState {
    upstream: String,
    pause: bool,
    tip_move: TipMove,
    /// How many `getbestblockhash` calls still see the node's tip before the
    /// chain moves: 0 moves it before the clear's first tip check, 1 only
    /// before its check at the commit, after the integrity report.
    unmoved_checks: usize,
    best_calls: std::sync::atomic::AtomicUsize,
    moved: std::sync::atomic::AtomicBool,
    /// The node's tip and its height, from the `getblockchaininfo` that
    /// captures them.
    captured: std::sync::Mutex<Option<(String, u64)>>,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

impl ProxyState {
    /// The moved chain's answer, where it differs from the node's: `Ok` with
    /// a result, or `Err` with a JSON-RPC error object.
    fn moved_answer(&self, method: &str, params: &Value) -> Option<Result<Value, Value>> {
        use std::sync::atomic::Ordering::SeqCst;
        if self.tip_move == TipMove::Still {
            return None;
        }
        let (tip, height) = self.captured.lock().unwrap().clone()?;
        if method == "getbestblockhash" {
            if self.best_calls.fetch_add(1, SeqCst) < self.unmoved_checks {
                return None;
            }
            self.moved.store(true, SeqCst);
            return Some(Ok(json!(moved_tip())));
        }
        let moved = self.moved.load(SeqCst);
        let at = params[0].as_u64().filter(|at| *at > 0);
        let header = |hash: String, at: u64, parent: &str| {
            Ok(json!({"hash": hash, "height": at, "previousblockhash": parent}))
        };
        match (self.tip_move, method) {
            (_, "getblockheader") if moved && params[0] == json!(moved_tip()) => {
                Some(match self.tip_move {
                    TipMove::Reorg => header(moved_tip(), height + 1, &fork_block()),
                    TipMove::Shorten => header(moved_tip(), height - 1, &"97".repeat(32)),
                    TipMove::Runaway => header(moved_tip(), height + 1_001, &"96".repeat(32)),
                    TipMove::Unknown => Err(json!({"code":-5,"message":"Block not found"})),
                    TipMove::Crawl => header(
                        moved_tip(),
                        height + CRAWL_BLOCKS,
                        &crawl_block(height + CRAWL_BLOCKS - 1),
                    ),
                    _ => header(moved_tip(), height + 2, &extension_block()),
                })
            }
            (TipMove::Extend | TipMove::Flap, "getblockheader")
                if moved && params[0] == json!(extension_block()) =>
            {
                Some(header(extension_block(), height + 1, &tip))
            }
            (TipMove::Crawl, "getblockheader") if moved => {
                let hash = params[0].as_str()?;
                let at = u64::from_str_radix(hash.strip_prefix("c7")?, 16).ok()?;
                let parent = if at - 1 == height {
                    tip
                } else {
                    crawl_block(at - 1)
                };
                Some(header(hash.to_owned(), at, &parent))
            }
            (TipMove::Flap, "getblockhash") if !moved && at.is_some_and(|at| at <= height) => {
                Some(Ok(json!(fork_block())))
            }
            (TipMove::Shorten, "getblockhash") if moved && at.is_some_and(|at| at >= height) => {
                Some(Err(
                    json!({"code":-8,"message":"Block height out of range"}),
                ))
            }
            _ => None,
        }
    }
}

impl Drop for RpcProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl RpcProxy {
    async fn open(node: &fake::FakeNode, pause: bool) -> Result<Self> {
        Self::open_moving(node, pause, TipMove::Still, 0).await
    }

    /// A proxy whose chain makes `tip_move` after `unmoved_checks` tip checks.
    async fn moving(
        node: &fake::FakeNode,
        tip_move: TipMove,
        unmoved_checks: usize,
    ) -> Result<Self> {
        Self::open_moving(node, false, tip_move, unmoved_checks).await
    }

    async fn open_moving(
        node: &fake::FakeNode,
        pause: bool,
        tip_move: TipMove,
        unmoved_checks: usize,
    ) -> Result<Self> {
        use axum::{extract::State, routing::post, Json, Router};
        let state = std::sync::Arc::new(ProxyState {
            upstream: node.url.clone(),
            pause,
            tip_move,
            unmoved_checks,
            best_calls: Default::default(),
            moved: Default::default(),
            captured: Default::default(),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let app = Router::new()
            .route(
                "/",
                post(
                    |State(state): State<std::sync::Arc<ProxyState>>,
                     Json(request): Json<Value>| async move {
                        let method = request["method"].as_str().unwrap_or_default();
                        if state.pause && method == "getblockchaininfo" {
                            state.entered.notify_one();
                            state.release.notified().await;
                        }
                        if state.tip_move == TipMove::Crawl && method == "getblockheader" {
                            tokio::time::sleep(CRAWL_DELAY).await;
                        }
                        if let Some(answer) = state.moved_answer(method, &request["params"]) {
                            let (result, error) = match answer {
                                Ok(result) => (result, Value::Null),
                                Err(error) => (Value::Null, error),
                            };
                            return Json(json!({"id":request["id"],"result":result,"error":error}));
                        }
                        let response = reqwest::Client::new()
                            .post(&state.upstream)
                            .json(&request)
                            .send()
                            .await
                            .unwrap()
                            .json::<Value>()
                            .await
                            .unwrap();
                        let info = &response["result"];
                        if let (true, Some(tip), Some(height)) = (
                            method == "getblockchaininfo",
                            info["bestblockhash"].as_str(),
                            info["blocks"].as_u64(),
                        ) {
                            *state.captured.lock().unwrap() = Some((tip.to_owned(), height));
                        }
                        Json(response)
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
    let proxy = RpcProxy::open(&node, true).await?;
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
        sqlx::query("INSERT INTO qbit_prism_instances(instance_id,status) VALUES('new-frontend','{\"state\":\"starting\",\"candidate_offer_lifecycle\":1}')").execute(&pool).await
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
    // #737: a moved tip alone no longer refuses (an extension is accepted);
    // a reorganization does.
    let moved = RpcProxy::moving(&node, TipMove::Reorg, 0).await?;
    config.rpc_url = moved.url.clone();
    let error = ledger
        .clear_fatal_state(&config, "reviewed")
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("the chain reorganized during fatal-state recovery"),
        "{error}"
    );
    unchanged(&ledger, &before).await?;
    let slow = RpcProxy::open(&node, true).await?;
    config.rpc_url = slow.url.clone();
    config.rpc_timeout = Duration::from_millis(100);
    let error = ledger
        .clear_fatal_state(&config, "reviewed")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("transport failed"), "{error}");
    unchanged(&ledger, &before).await?;
    let paused = RpcProxy::open(&node, true).await?;
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
    ledger.heartbeat(HeartbeatStatus::Stopped).await?;
    db.close(vec![ledger]).await
}

async fn chain_state(ledger: &Ledger, hash: &str) -> Result<String> {
    Ok(
        sqlx::query_scalar("SELECT chain_state FROM qbit_pool_blocks WHERE block_hash=$1")
            .bind(hash)
            .fetch_one(&ledger.pool)
            .await?,
    )
}

async fn recovery_events(ledger: &Ledger) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT count(*) FROM qbit_prism_fatal_state_events")
            .fetch_one(&ledger.pool)
            .await?,
    )
}

/// #737: a tip that only grew during the clear leaves every observation
/// standing, whether the chain grew before the first tip check, when every
/// observation is asked again, or only before the commit's, after the
/// integrity report, and whether a proxy grew it by two blocks or the node
/// itself by one: the clear commits, and its event names both tips, the final
/// one walked back to the captured one by its headers.
#[tokio::test]
async fn clear_accepts_a_tip_extended_during_the_run() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (ledger, node, mut config) = setup(&db).await?;
    // A pool block on the node's chain below the captured tip, which the
    // clear's reconciliation confirms.
    let hash = "ac".repeat(32);
    block(&ledger, &hash, 90, false).await?;
    node.set_reply("getblockhash", json!([90]), json!(hash));
    stopped(&ledger).await?;
    for (cleared, unmoved_checks) in [0, 1, 2].into_iter().enumerate() {
        halt(&ledger, "test halt").await?;
        let event = if unmoved_checks < 2 {
            let extended = RpcProxy::moving(&node, TipMove::Extend, unmoved_checks).await?;
            config.rpc_url = extended.url.clone();
            ledger.clear_fatal_state(&config, "reviewed").await?
        } else {
            // The node grows its own chain while the first tip check's reply
            // is held: that check still sees the captured tip, and the check
            // before the commit walks the new block's own header back to it.
            config.rpc_url = node.url.clone();
            let mut check = node.pause_next("getbestblockhash")?;
            let (recovering, recovery) = (ledger.clone(), config.clone());
            let clear =
                tokio::spawn(
                    async move { recovering.clear_fatal_state(&recovery, "reviewed").await },
                );
            tokio::time::timeout(Duration::from_secs(10), check.entered()).await??;
            node.set_tip(&moved_tip(), &"ab".repeat(32), 101, "02");
            check.release();
            tokio::time::timeout(Duration::from_secs(30), clear).await???
        };
        let reconciliation = &event["reconciliation"];
        assert_eq!(reconciliation["tip_hash"], "ab".repeat(32), "{event}");
        assert_eq!(reconciliation["tip_height"], 100, "{event}");
        assert_eq!(reconciliation["final_tip_hash"], moved_tip(), "{event}");
        assert_eq!(reconciliation["tip_extended"], true, "{event}");
        assert_eq!(reconciliation["blocks_checked"], 1, "{event}");
        assert_eq!(ledger.fatal_state().await?["halted"], false);
        assert_eq!(chain_state(&ledger, &hash).await?, "confirmed");
        assert_eq!(recovery_events(&ledger).await?, cleared as i64 + 1);
    }
    assert!(ledger.append(share(1), None).await?.inserted);
    db.close(vec![ledger]).await
}

/// #737: a reorganization during the clear refuses it, whether the node's new
/// tip descends from another block at the captured height, even while the
/// active chain still answers the captured tip there, is not above that
/// height, or is a block the node does not know, and whether the chain moved
/// before the first tip check or only before the commit's, after the
/// integrity report. The halt and the reconciliation's changes roll back. A
/// tip too far above the captured height refuses as well.
#[tokio::test]
async fn clear_refuses_a_reorg_during_the_run() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (ledger, node, mut config) = setup(&db).await?;
    let hash = "ac".repeat(32);
    block(&ledger, &hash, 90, false).await?;
    node.set_reply("getblockhash", json!([90]), json!(hash));
    halt(&ledger, "test halt").await?;
    stopped(&ledger).await?;
    let before = ledger.fatal_state().await?;
    let reorganized = "the chain reorganized during fatal-state recovery: ";
    for (tip_move, expected) in [
        (
            TipMove::Reorg,
            format!(
                "{reorganized}the node's tip {} descends from {} at the captured height 100, not from the captured tip",
                moved_tip(),
                fork_block()
            ),
        ),
        (
            TipMove::Shorten,
            format!(
                "{reorganized}the node's tip {} is at height 99, not above the captured height 100",
                moved_tip()
            ),
        ),
        (
            TipMove::Unknown,
            format!("{reorganized}the node does not know block {}", moved_tip()),
        ),
        (
            TipMove::Runaway,
            "the node's tip moved 1001 blocks during fatal-state recovery".into(),
        ),
    ] {
        for unmoved_checks in [0, 1] {
            let moved = RpcProxy::moving(&node, tip_move, unmoved_checks).await?;
            config.rpc_url = moved.url.clone();
            let error = ledger
                .clear_fatal_state(&config, "reviewed")
                .await
                .unwrap_err()
                .to_string();
            assert!(
                error.contains(&expected) && error.ends_with("; retry fatal-state clear"),
                "{error}"
            );
            unchanged(&ledger, &before).await?;
            assert_eq!(
                chain_state(&ledger, &hash).await?,
                "prepared",
                "the reconciliation's confirmation must roll back"
            );
        }
    }
    db.close(vec![ledger]).await
}

/// #737: a chain that went over to a fork while the clear's loop read it and
/// came back onto blocks on top of the captured tip by the first tip check
/// passes the ancestry walk, but what the loop read on the fork is stale. The
/// first check asks every observation again once the tip has moved, so the
/// clear refuses and changes nothing: first over a deep confirmed fanout that
/// only the fork confirmed, read as connected, then over a confirmed pool
/// block the node's chain holds, read as inactive, which the reconciliation
/// would deactivate.
#[tokio::test]
async fn clear_refuses_a_chain_that_flapped_to_a_fork_during_its_observations() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (ledger, node, mut config) = setup(&db).await?;
    // The fanout's pool block is on neither chain, so it reads inactive both
    // times; the fork's block confirmed the fanout at height 20.
    let (parent, txid) = ("ad".repeat(32), "ee".repeat(32));
    block(&ledger, &parent, 10, false).await?;
    sqlx::query("INSERT INTO qbit_ctv_fanout_sets(block_hash,manifest_set_json,manifest_set,manifest_set_sha256,settlement_mode,parent_coinbase_txid,parent_coinbase_tx_hex,fanout_count,fanout_output_sum_sats,covenant_output_value_sats) VALUES($1,'{}','{}','set','ctv_fanout','coinbase','00',1,1,1)")
        .bind(&parent).execute(&ledger.pool).await?;
    sqlx::query("INSERT INTO qbit_ctv_fanout_artifacts(fanout_txid,block_hash,manifest_set_sha256,manifest_json,manifest,manifest_sha256,precommitment_sha256,ctv_hash,commitment_witness_leaf_hex,chunk_index,chunk_count,parent_coinbase_txid,parent_coinbase_vout,fanout_tx_template_hex,fanout_tx_hex,covenant_output_value_sats,fanout_output_sum_sats,settlement_status,confirmed_depth,confirmed_block_hash,confirmed_block_height) VALUES($1,$2,'set','{}','{}','manifest','precommit','ctv','00',0,1,'coinbase',0,'00','00',1,1,'confirmed',1000,$3,20)")
        .bind(&txid).bind(&parent).bind(fork_block()).execute(&ledger.pool).await?;
    halt(&ledger, "test halt").await?;
    stopped(&ledger).await?;
    let before = ledger.fatal_state().await?;
    let reorganized = "the chain reorganized during fatal-state recovery: ";
    let fanout_flap = RpcProxy::moving(&node, TipMove::Flap, 0).await?;
    config.rpc_url = fanout_flap.url.clone();
    let error = ledger
        .clear_fatal_state(&config, "reviewed")
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains(&format!(
            "{reorganized}deep confirmed CTV fanout {txid} at height 20 is no longer connected; retry fatal-state clear"
        )),
        "{error}"
    );
    unchanged(&ledger, &before).await?;

    let hash = "ac".repeat(32);
    block(&ledger, &hash, 90, false).await?;
    sqlx::query("UPDATE qbit_pool_blocks SET chain_state='confirmed' WHERE block_hash=$1")
        .bind(&hash)
        .execute(&ledger.pool)
        .await?;
    node.set_reply("getblockhash", json!([90]), json!(hash));
    let block_flap = RpcProxy::moving(&node, TipMove::Flap, 0).await?;
    config.rpc_url = block_flap.url.clone();
    let error = ledger
        .clear_fatal_state(&config, "reviewed")
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains(&format!(
            "{reorganized}pool block {hash} at height 90 was inactive when observed and is active now; retry fatal-state clear"
        )),
        "{error}"
    );
    unchanged(&ledger, &before).await?;
    assert_eq!(chain_state(&ledger, &hash).await?, "confirmed");
    db.close(vec![ledger]).await
}

/// #737: the clear calls a pool block above the captured height inactive
/// without asking the node, and a block the chain grew by could be that one.
/// So a moved tip refuses while such a row exists, at either tip check, even
/// when the chain only grew. With the tip unchanged, the same row clears.
#[tokio::test]
async fn clear_refuses_an_extension_with_a_pool_block_above_the_captured_height() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (ledger, node, mut config) = setup(&db).await?;
    let above = "ef".repeat(32);
    block(&ledger, &above, 101, false).await?;
    halt(&ledger, "test halt").await?;
    stopped(&ledger).await?;
    let before = ledger.fatal_state().await?;
    for unmoved_checks in [0, 1] {
        let extended = RpcProxy::moving(&node, TipMove::Extend, unmoved_checks).await?;
        config.rpc_url = extended.url.clone();
        let error = ledger
            .clear_fatal_state(&config, "reviewed")
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("the tip moved and a pool block lies above the captured height 100"),
            "{error}"
        );
        unchanged(&ledger, &before).await?;
    }
    config.rpc_url = node.url.clone();
    let event = ledger.clear_fatal_state(&config, "reviewed").await?;
    assert_eq!(event["reconciliation"]["tip_extended"], false, "{event}");
    assert_eq!(
        event["reconciliation"]["final_tip_hash"],
        "ab".repeat(32),
        "{event}"
    );
    assert_eq!(chain_state(&ledger, &above).await?, "prepared");
    db.close(vec![ledger]).await
}

/// The ledger sessions' statement timeout in
/// `clear_and_self_check_run_the_report_under_their_own_timeout`, and how long
/// its stub report sleeps: under that timeout alone the report is cancelled.
const SESSION_TIMEOUT_MS: i64 = 1_000;
const STUB_REPORT_SLEEP_SECONDS: &str = "1.5";

/// #737: `fatal-state clear` and `self-check` run the integrity report under
/// statement timeouts of their own, so a session timeout shorter than the
/// report fails neither. The stub report returns the timeout it ran under:
/// self-check's fixed 300 s, and for the clear its --timeout-seconds less the
/// time already spent and what it keeps for the commit, 5 s and one node RPC
/// timeout. A trigger on the event INSERT shows that the statements after the
/// report run under the session's timeout again, not the server default a
/// RESET would leave.
#[tokio::test]
async fn clear_and_self_check_run_the_report_under_their_own_timeout() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (ledger, node, mut config) = setup(&db).await?;
    // #535: `self-check` refuses a configuration without a pool fee.
    config.payout_policy.pool_fee_policy = Some(super::pool_fee::zero_bps_policy());
    sqlx::query("UPDATE qbit_prism_cluster SET config_fingerprint=$1")
        .bind(config.fingerprint(&"00".repeat(32))?)
        .execute(&ledger.pool)
        .await?;
    sqlx::raw_sql(&format!(
        "CREATE OR REPLACE FUNCTION qbit_carry_forward_integrity_report() RETURNS jsonb LANGUAGE sql AS $$ \
           SELECT jsonb_build_object('mismatch_count',0,'current_drift_count',0, \
             'statement_timeout',current_setting('statement_timeout'), \
             'statement_timeout_ms',(SELECT setting::bigint FROM pg_settings WHERE name='statement_timeout')) \
           FROM pg_sleep({STUB_REPORT_SLEEP_SECONDS}) $$; \
         CREATE FUNCTION record_insert_timeout() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN \
           NEW.reconciliation := NEW.reconciliation || jsonb_build_object('insert_statement_timeout_ms', \
             (SELECT setting::bigint FROM pg_settings WHERE name='statement_timeout')); \
           RETURN NEW; END $$; \
         CREATE TRIGGER record_insert_timeout BEFORE INSERT ON qbit_prism_fatal_state_events \
           FOR EACH ROW EXECUTE FUNCTION record_insert_timeout()"
    ))
    .execute(&ledger.pool)
    .await?;
    let session_ms = SESSION_TIMEOUT_MS.to_string();
    let session = ("PRISM_DATABASE_STATEMENT_TIMEOUT_MS", session_ms.as_str());
    // Past the integrity gate: durability is read after it, and the check
    // fails only on the audit API this fixture does not serve.
    let env: Vec<_> = TOOL_ENV.into_iter().chain([session]).collect();
    let output = cli_with_env(&db, &node, &["self-check"], &env).await?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    let report: Value = serde_json::from_slice(&output.stdout)
        .with_context(|| format!("self-check report; stderr={stderr}"))?;
    let integrity = &report["carry_forward_integrity"];
    assert_eq!(
        integrity["statement_timeout_ms"], 300_000,
        "{report}; {stderr}"
    );
    assert!(report["durability"].is_array(), "{report}; {stderr}");

    halt(&ledger, "test halt").await?;
    stopped(&ledger).await?;
    let env: Vec<_> = TOOL_ENV[1..]
        .iter()
        .copied()
        .chain([session, ("PRISM_RPC_TIMEOUT_SECONDS", "5")])
        .collect();
    let clear = [
        "fatal-state",
        "clear",
        "--reason",
        "reviewed",
        "--timeout-seconds",
        "30",
    ];
    let cleared = cli_with_env(&db, &node, &clear, &env).await?;
    assert!(
        cleared.status.success(),
        "{}",
        String::from_utf8_lossy(&cleared.stderr)
    );
    let event: Value = serde_json::from_slice(&cleared.stdout)?;
    let report_ms = event["reconciliation"]["integrity"]["statement_timeout_ms"]
        .as_i64()
        .with_context(|| format!("{event}"))?;
    // 30 s, less 5 s and the 5 s RPC timeout kept for the commit.
    assert!((1_000..=20_000).contains(&report_ms), "{event}");
    assert_eq!(
        event["reconciliation"]["insert_statement_timeout_ms"], SESSION_TIMEOUT_MS,
        "{event}"
    );
    assert_eq!(ledger.fatal_state().await?["halted"], false);
    db.close(vec![ledger]).await
}

/// #737: the closing tip check ends by the commit's headroom before the
/// bound, whatever the node. A tip far above the captured one, on a node that
/// answers each of its headers slowly but well within the RPC timeout, would
/// walk into the 5 s the UPDATE, the INSERT and the COMMIT keep, so the clear
/// refuses there with nothing committed, well before the bound, which can then
/// never end inside the COMMIT. The halt stays.
#[tokio::test]
async fn clear_refuses_a_closing_tip_walk_that_would_run_into_the_commit() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (ledger, node, mut config) = setup(&db).await?;
    halt(&ledger, "test halt").await?;
    stopped(&ledger).await?;
    let before = ledger.fatal_state().await?;
    // The first tip check still sees the captured tip; the closing one sees
    // `Crawl`'s, about 25 s of headers away.
    let crawling = RpcProxy::moving(&node, TipMove::Crawl, 1).await?;
    config.rpc_url = crawling.url.clone();
    config.rpc_timeout = Duration::from_secs(1);
    let bound = Duration::from_secs(10);
    let started = std::time::Instant::now();
    let error = ledger
        .clear_fatal_state_within(&config, "reviewed", bound)
        .await
        .unwrap_err();
    let message = format!("{error:#}");
    assert!(
        message.contains("the closing tip check did not finish"),
        "{message}"
    );
    // Refused at the bound less the commit's 5 s, not at the bound itself.
    assert!(
        started.elapsed() < bound - Duration::from_secs(3),
        "{message}"
    );
    unchanged(&ledger, &before).await?;
    db.close(vec![ledger]).await
}

/// #737: the clear's integrity report ends in the server by the clear's own
/// deadline, so a clear that gives up leaves no statement running behind it
/// (and holding its locks) for the operator's retry to wait on. A bound that
/// leaves the report under a second refuses before starting it.
#[tokio::test]
async fn clear_report_statement_ends_within_the_bound() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (ledger, _node, mut config) = setup(&db).await?;
    // The commit keeps 5 s of the bound, plus this RPC timeout.
    config.rpc_timeout = Duration::from_secs(2);
    halt(&ledger, "test halt").await?;
    stopped(&ledger).await?;
    let before = ledger.fatal_state().await?;
    let error = ledger
        .clear_fatal_state_within(&config, "reviewed", Duration::from_secs(7))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("raise --timeout-seconds"),
        "{error}"
    );
    unchanged(&ledger, &before).await?;
    // A report that would outlast any bound and the session's timeout.
    sqlx::raw_sql(
        "CREATE OR REPLACE FUNCTION qbit_carry_forward_integrity_report() RETURNS jsonb LANGUAGE sql AS $$ \
           SELECT jsonb_build_object('mismatch_count',0,'current_drift_count',0) FROM pg_sleep(120) $$",
    )
    .execute(&ledger.pool)
    .await?;
    let bound = Duration::from_secs(12);
    let started = std::time::Instant::now();
    let error = ledger
        .clear_fatal_state_within(&config, "reviewed", bound)
        .await
        .unwrap_err();
    // The server cancelled the report, before the client's deadline.
    assert!(started.elapsed() < bound, "{error:#}");
    let message = format!("{error:#}");
    assert!(
        message.contains("raise --timeout-seconds")
            && message.contains("canceling statement due to statement timeout"),
        "{message}"
    );
    let running: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() \
         AND pid<>pg_backend_pid() AND state='active' \
         AND query LIKE '%qbit_carry_forward_integrity_report%'",
    )
    .fetch_one(&ledger.pool)
    .await?;
    assert_eq!(running, 0, "the report outlived the clear");
    unchanged(&ledger, &before).await?;
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
