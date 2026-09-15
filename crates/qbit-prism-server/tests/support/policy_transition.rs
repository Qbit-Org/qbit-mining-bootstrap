use super::*;
use qbit_prism_server::{
    config::Config,
    ledger::{HeartbeatHealth, HeartbeatStatus, OfferOutcome},
};
use serde_json::Value;
use std::time::Duration;

use super::fake_qbitd as fake;

async fn setup(db: &Database) -> Result<(Ledger, Ledger, fake::FakeNode, Config)> {
    setup_ctv(db, false).await
}

async fn setup_ctv(db: &Database, ctv: bool) -> Result<(Ledger, Ledger, fake::FakeNode, Config)> {
    let node = fake::FakeNode::open().await?;
    let mut config = fake::coordinator_config(db.url.clone(), &node, "operator")?;
    config.manifest_seed = "42".repeat(32);
    config.ledger_seed = "43".repeat(32);
    config.ledger_public_key = keys().1.public_key_hex();
    config.username_fallback = Some("policy-test-fallback".into());
    config.ctv_enabled = ctv;
    if ctv {
        config.ctv_fee = Some(qbit_prism::FanoutFeeRatePolicy::new(1, 12000));
    }
    let a = db.ledger("frontend-a").await?;
    let b = db.ledger("frontend-b").await?;
    for ledger in [&a, &b] {
        ledger
            .configure(
                &config.fingerprint(&"00".repeat(32))?,
                &SignerKeys::of(&keys().0, &keys().1),
            )
            .await?;
    }
    Ok((a, b, node, config))
}

fn changed_fee(config: &Config) -> Config {
    let mut next = config.clone();
    next.payout_policy.pool_fee_policy = Some(qbit_prism::PoolFeePolicy {
        fee_bps: 200,
        recipient_id: "fee".into(),
        order_key: "fee".into(),
        p2mr_program_hex: "11".repeat(32),
    });
    next
}

#[tokio::test]
async fn ctv_fee_transition_can_repair_a_rate_below_the_live_floor() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (a, b, node, config) = setup_ctv(&db, true).await?;
    a.heartbeat(HeartbeatStatus::Stopped).await?;
    b.heartbeat(HeartbeatStatus::Stopped).await?;
    let before = state(&a).await?;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("ctv.env");
    std::fs::write(
        &path,
        "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT=2\n",
    )?;
    let run = || {
        let mut command = cli(&db, &node, &path);
        command
            .env("PRISM_CTV_SETTLEMENT_ENABLED", "1")
            .env("PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT", "1");
        command
    };
    let output = run().output().await?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("relay floor"));
    assert_eq!(state(&a).await?, before);
    std::fs::write(&path, "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT=2000\nPRISM_CTV_FANOUT_FEE_PREMIUM_BPS=15000\n")?;
    let output = run().output().await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let event: Value = serde_json::from_slice(&output.stdout)?;
    let mut next = config;
    next.ctv_fee = Some(qbit_prism::FanoutFeeRatePolicy::new(2000, 15000));
    assert_eq!(
        event["config_fingerprint"],
        next.fingerprint(&"00".repeat(32))?
    );
    // Switching to the automatic estimator binds its premium; it uses the
    // same floor validation and does not replace that input with today's estimate.
    let mut automatic = next.clone();
    automatic.ctv_fee = None;
    automatic.ctv_fee_premium_bps = 16000;
    a.transition_policy(&next, &automatic).await?;
    let mut premium = automatic.clone();
    premium.ctv_fee_premium_bps = 17000;
    a.transition_policy(&automatic, &premium).await?;
    assert_eq!(
        a.payout_revision().await?,
        before["cluster"]["payout_revision"].as_i64().unwrap() + 3
    );
    db.close(vec![a, b]).await
}

async fn state(ledger: &Ledger) -> Result<Value> {
    Ok(sqlx::query_scalar("SELECT jsonb_build_object('cluster',(SELECT to_jsonb(c) FROM qbit_prism_cluster c),'outbox',(SELECT jsonb_agg(to_jsonb(o) ORDER BY block_hash) FROM qbit_block_candidate_outbox o),'events',(SELECT jsonb_agg(to_jsonb(e)) FROM qbit_prism_policy_transitions e))")
        .fetch_one(&ledger.pool).await?)
}

#[tokio::test]
async fn two_frontends_must_stop_and_old_work_and_startup_are_rejected() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (a, b, _node, config) = setup(&db).await?;
    let next = changed_fee(&config);
    let before = state(&a).await?;
    for status in [
        json!({"state":"starting","candidate_offer_lifecycle":1}),
        json!({"state":"draining"}),
        json!({"state":"drained"}),
        json!({}),
        json!({"schema":"qbit.prism.audit-health.v1","ready":true,"state":"stopped"}),
    ] {
        sqlx::query("UPDATE qbit_prism_instances SET status=$1,heartbeat_at=clock_timestamp()-interval '1 day' WHERE instance_id='frontend-b'")
            .bind(status).execute(&a.pool).await?;
        let error = a
            .transition_policy(&config, &next)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("frontend-b") && error.contains("stopped"),
            "{error}"
        );
        assert_eq!(state(&a).await?, before);
    }
    a.heartbeat(HeartbeatStatus::Stopped).await?;
    b.heartbeat(HeartbeatStatus::Stopped).await?;
    let revision = a.payout_revision().await?;
    let event = a.transition_policy(&config, &next).await?;
    assert_eq!(event["payout_revision"], revision + 1);
    assert_eq!(event["previous_revision"], revision);
    assert_eq!(
        event["config_fingerprint"],
        next.fingerprint(&"00".repeat(32))?
    );
    assert!(
        b.new_session_id().await.is_err(),
        "stopped frontend admitted a session"
    );
    assert!(b
        .save_job("old-job", &json!({}), revision, "parent", 60)
        .await
        .is_err());
    assert!(b
        .append_at_revision(share(1), None, revision)
        .await
        .is_err());
    let mut rejected_config = config.clone();
    rejected_config.instance_id = "frontend-b".into();
    let rejected = qbit_prism_server::coordinator::Coordinator::new(
        rejected_config,
        std::sync::Arc::new(qbit_prism_server::metrics::Metrics::default()),
    )
    .await
    .err()
    .context("old-policy coordinator started")?;
    assert!(rejected.to_string().contains("fingerprint mismatch"));
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT status->>'state' FROM qbit_prism_instances WHERE instance_id='frontend-b'"
        )
        .fetch_one(&a.pool)
        .await?,
        "starting"
    );
    let restart = db.ledger("frontend-b").await?;
    let error = restart
        .configure(
            &config.fingerprint(&"00".repeat(32))?,
            &SignerKeys::of(&keys().0, &keys().1),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains(&format!("payout revision {}", revision + 1)),
        "{error}"
    );
    restart
        .configure(
            &next.fingerprint(&"00".repeat(32))?,
            &SignerKeys::of(&keys().0, &keys().1),
        )
        .await?;
    restart
        .save_job("new-job", &json!({}), revision + 1, "parent", 60)
        .await?;
    assert!(
        restart
            .append_at_revision(share(2), None, revision + 1)
            .await?
            .inserted
    );
    db.close(vec![a, b, restart]).await
}

#[tokio::test]
async fn rejected_same_id_startup_cannot_authorize_a_policy_transition() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (a, b, _node, config) = setup(&db).await?;
    let next = changed_fee(&config);
    let held = a.new_session_id().await?;
    a.heartbeat(HeartbeatStatus::Health(HeartbeatHealth::new(
        true,
        Default::default(),
    )))
    .await?;
    b.heartbeat(HeartbeatStatus::Stopped).await?;
    a.append(share(1), None).await?;
    let pending = candidate(&a.snapshot(100).await?, 2895)?;
    a.enqueue_candidate(pending.candidate).await?;
    let before = state(&a).await?;

    // The rejected process shares a's ID but owns none of its live sessions.
    let mut rejected_config = next.clone();
    rejected_config.instance_id = "frontend-a".into();
    let rejected = qbit_prism_server::coordinator::Coordinator::new(
        rejected_config,
        std::sync::Arc::new(qbit_prism_server::metrics::Metrics::default()),
    )
    .await
    .err()
    .context("mismatched-policy coordinator started")?;
    assert!(rejected.to_string().contains("fingerprint mismatch"));

    // Try before a's next heartbeat: the shared row must not prove quiescence.
    let error = a
        .transition_policy(&config, &next)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("frontend-a") && error.contains("stopped"),
        "{error}"
    );
    assert_eq!(state(&a).await?, before);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT status->>'state' FROM qbit_prism_instances WHERE instance_id='frontend-a'"
        )
        .fetch_one(&a.pool)
        .await?,
        "starting"
    );
    a.new_session_id().await?.release().await?;
    assert!(a.heartbeat(HeartbeatStatus::Stopped).await.is_err());
    held.release().await?;
    a.heartbeat(HeartbeatStatus::Stopped).await?;
    let event = a.transition_policy(&config, &next).await?;
    assert_eq!(
        event["payout_revision"],
        before["cluster"]["payout_revision"].as_i64().unwrap() + 1
    );
    assert_eq!(event["abandoned_candidates"], 1);
    db.close(vec![a, b]).await
}

#[tokio::test]
async fn offered_candidates_keep_original_evidence_and_unoffered_are_abandoned() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (a, b, _node, config) = setup(&db).await?;
    a.append(share(1), None).await?;
    let snapshot = a.snapshot(100).await?;
    let mut offered = Vec::new();
    for index in 0..3 {
        let block = candidate(&snapshot, 2890 + index)?;
        a.enqueue_candidate(block.candidate.clone()).await?;
        let claim = a.claim_candidate(60).await?.unwrap();
        a.reserve_offer(&claim).await?;
        if index > 0 {
            a.record_offer(&claim, 1_700_000_000_123, OfferOutcome::Accepted, None)
                .await?;
        }
        if index > 1 {
            a.reconcile_candidate(&claim, "landing deferred").await?;
        }
        // Make this row unavailable so the next loop claims its new block.
        sqlx::query("UPDATE qbit_block_candidate_outbox SET next_attempt_at=clock_timestamp()+interval '1 day' WHERE block_hash=$1")
            .bind(&block.block_hash).execute(&a.pool).await?;
        offered.push((block, claim));
    }
    let parked = candidate(&snapshot, 2893)?;
    a.enqueue_candidate(parked.candidate.clone()).await?;
    let parked_claim = a.claim_candidate(60).await?.unwrap();
    a.reserve_offer(&parked_claim).await?;
    a.reconcile_candidate(&parked_claim, "requires operator recovery")
        .await?;
    sqlx::query(
        "UPDATE qbit_block_candidate_outbox SET next_attempt_at='infinity' WHERE block_hash=$1",
    )
    .bind(&parked.block_hash)
    .execute(&a.pool)
    .await?;
    let pending = candidate(&snapshot, 2894)?;
    a.enqueue_candidate(pending.candidate.clone()).await?;
    let pending_claim = a.claim_candidate(60).await?.unwrap();
    let evidence: Vec<Value> = sqlx::query_scalar("SELECT jsonb_build_object('state',state,'candidate',candidate,'block_bytes',encode(block_bytes,'hex'),'offer_reserved_at',offer_reserved_at,'offer_reply',offer_reply,'offer_outcome',offer_outcome,'parked',next_attempt_at='infinity'::timestamptz,'last_error',last_error) FROM qbit_block_candidate_outbox WHERE state<>'pending' ORDER BY block_hash")
        .fetch_all(&a.pool).await?;
    a.heartbeat(HeartbeatStatus::Stopped).await?;
    b.heartbeat(HeartbeatStatus::Stopped).await?;
    let event = a.transition_policy(&config, &changed_fee(&config)).await?;
    assert_eq!(event["abandoned_candidates"], 1);
    assert_eq!(event["retained_candidates"], 4);
    let after: Vec<Value> = sqlx::query_scalar("SELECT jsonb_build_object('state',state,'candidate',candidate,'block_bytes',encode(block_bytes,'hex'),'offer_reserved_at',offer_reserved_at,'offer_reply',offer_reply,'offer_outcome',offer_outcome,'parked',next_attempt_at='infinity'::timestamptz,'last_error',last_error) FROM qbit_block_candidate_outbox WHERE state<>'abandoned' ORDER BY block_hash")
        .fetch_all(&a.pool).await?;
    assert_eq!(evidence, after);
    let terminal: (String, String, bool) = sqlx::query_as("SELECT state,last_error,candidate IS NULL AND block_bytes IS NULL AND window_anchor_ms IS NULL FROM qbit_block_candidate_outbox WHERE block_hash=$1")
        .bind(&pending.block_hash).fetch_one(&a.pool).await?;
    assert_eq!(
        terminal,
        ("abandoned".into(), "epoch-superseded".into(), true)
    );
    assert!(a.reserve_offer(&pending_claim).await.is_err());
    for (_, old_claim) in &offered {
        assert!(a.renew_candidate_claim(old_claim, 60).await.is_err());
    }
    let recovery = db.ledger("recovery").await?;
    for _ in 0..3 {
        let claim = recovery
            .claim_candidate(60)
            .await?
            .context("retained candidate missing")?;
        let (block, _) = offered
            .iter()
            .find(|(block, _)| block.block_hash == claim.candidate.block_hash)
            .unwrap();
        assert!(
            recovery.reserve_offer(&claim).await.is_err(),
            "offered block could be offered twice"
        );
        let claim = block.claim(claim);
        let revision = recovery.payout_revision().await?;
        recovery
            .land_candidate_at_revision(&claim, &keys().1.public_key_hex(), revision)
            .await?;
        let bytes = qbit_prism::canonical_audit_bundle_bytes(&block.bundle)?;
        let stored = recovery.audit_bundle(&block.block_hash).await?.unwrap();
        let stored: AuditBundle = serde_json::from_value(stored)?;
        assert_eq!(qbit_prism::canonical_audit_bundle_bytes(&stored)?, bytes);
        verify_audit_bundle_with_ledger_public_key(&stored, &config.ledger_public_key)?;
        recovery
            .finish_candidate_at_revision(&claim, true, None, revision)
            .await?;
    }
    assert!(recovery.claim_candidate(60).await?.is_none());
    db.close(vec![a, b, recovery]).await
}

#[tokio::test]
async fn only_fee_fields_can_change_and_retry_does_not_bump_again() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (a, b, _node, config) = setup(&db).await?;
    a.heartbeat(HeartbeatStatus::Stopped).await?;
    b.heartbeat(HeartbeatStatus::Stopped).await?;
    let before = state(&a).await?;
    let mut variants = vec![config.clone()];
    let mut keys_changed = changed_fee(&config);
    keys_changed.manifest_seed = "44".repeat(32);
    variants.push(keys_changed);
    let mut ledger_key = changed_fee(&config);
    ledger_key.ledger_public_key =
        ManifestSigningKey::from_seed_hex(&"45".repeat(32))?.public_key_hex();
    ledger_key.ledger_seed = "45".repeat(32);
    variants.push(ledger_key);
    let mut payout = changed_fee(&config);
    payout.payout_policy.safety_multiplier += 1;
    variants.push(payout);
    let mut ctv = changed_fee(&config);
    ctv.ctv_enabled = true;
    variants.push(ctv);
    let mut fallback = changed_fee(&config);
    fallback.username_fallback = Some("other".into());
    variants.push(fallback);
    for target in variants {
        assert!(a.transition_policy(&config, &target).await.is_err());
        assert_eq!(state(&a).await?, before);
    }
    a.transition_policy(&config, &changed_fee(&config)).await?;
    let once = state(&a).await?;
    assert!(a
        .transition_policy(&config, &changed_fee(&config))
        .await
        .is_err());
    assert_eq!(state(&a).await?, once);
    for statement in [
        "UPDATE qbit_prism_policy_transitions SET abandoned_candidates=42",
        "DELETE FROM qbit_prism_policy_transitions",
        "TRUNCATE qbit_prism_policy_transitions",
    ] {
        assert!(sqlx::query(statement).execute(&a.pool).await.is_err());
    }
    db.close(vec![a, b]).await
}

#[tokio::test]
async fn event_failure_rolls_back_fingerprint_revision_and_candidate_disposition() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (a, b, _node, config) = setup(&db).await?;
    a.append(share(1), None).await?;
    a.enqueue_candidate(candidate(&a.snapshot(100).await?, 2895)?.candidate)
        .await?;
    a.heartbeat(HeartbeatStatus::Stopped).await?;
    b.heartbeat(HeartbeatStatus::Stopped).await?;
    sqlx::raw_sql("CREATE FUNCTION reject_transition() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected event failure'; END $$; CREATE TRIGGER reject_transition BEFORE INSERT ON qbit_prism_policy_transitions FOR EACH ROW EXECUTE FUNCTION reject_transition();")
        .execute(&a.pool).await?;
    let before = state(&a).await?;
    assert!(a
        .transition_policy(&config, &changed_fee(&config))
        .await
        .unwrap_err()
        .to_string()
        .contains("injected event failure"));
    assert_eq!(state(&a).await?, before);
    db.close(vec![a, b]).await
}

#[tokio::test]
async fn historical_audits_and_ctv_artifacts_remain_byte_identical() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (a, b, _node, config) = setup(&db).await?;
    prepare_mature_cpfp_fanouts(&a, 2).await?;
    let hash: String = sqlx::query_scalar("SELECT block_hash FROM qbit_pool_audit_bundles")
        .fetch_one(&a.pool)
        .await?;
    let bundle: AuditBundle = serde_json::from_value(a.audit_bundle(&hash).await?.unwrap())?;
    let bytes = qbit_prism::canonical_audit_bundle_bytes(&bundle)?;
    let artifacts = |pool: PgPool| async move {
        sqlx::query_scalar::<_, Value>("SELECT jsonb_build_object('sets',(SELECT jsonb_agg(to_jsonb(s) ORDER BY block_hash) FROM qbit_ctv_fanout_sets s),'artifacts',(SELECT jsonb_agg(to_jsonb(a) ORDER BY fanout_txid) FROM qbit_ctv_fanout_artifacts a))")
            .fetch_one(&pool).await
    };
    let before = artifacts(a.pool.clone()).await?;
    assert_eq!(before["artifacts"].as_array().unwrap().len(), 2);
    a.heartbeat(HeartbeatStatus::Stopped).await?;
    b.heartbeat(HeartbeatStatus::Stopped).await?;
    a.transition_policy(&config, &changed_fee(&config)).await?;
    assert_eq!(artifacts(a.pool.clone()).await?, before);
    let after: AuditBundle = serde_json::from_value(a.audit_bundle(&hash).await?.unwrap())?;
    assert_eq!(qbit_prism::canonical_audit_bundle_bytes(&after)?, bytes);
    verify_audit_bundle_with_ledger_public_key(&after, &config.ledger_public_key)?;
    db.close(vec![a, b]).await
}

#[tokio::test]
async fn policy_transition_cannot_clear_a_fatal_state() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (a, b, _node, config) = setup(&db).await?;
    a.heartbeat(HeartbeatStatus::Stopped).await?;
    b.heartbeat(HeartbeatStatus::Stopped).await?;
    sqlx::query("UPDATE qbit_prism_cluster SET fatal_error='unresolved reorg' WHERE singleton")
        .execute(&a.pool)
        .await?;
    let before = state(&a).await?;
    assert!(a
        .transition_policy(&config, &changed_fee(&config))
        .await
        .unwrap_err()
        .to_string()
        .contains("cluster halted"));
    assert_eq!(state(&a).await?, before);
    db.close(vec![a, b]).await
}

fn cli(db: &Database, node: &fake::FakeNode, path: &std::path::Path) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_qbit-prism-server"));
    command
        .env_clear()
        .kill_on_drop(true)
        .args(["policy-transition", "--to"])
        .arg(path)
        .env("PRISM_DATABASE_URL", &db.url)
        .env("QBIT_RPC_URL", &node.url)
        .env("QBIT_CHAIN", "testnet")
        .env("PRISM_ALLOW_TEST_SIGNING_SEEDS", "1")
        .env("PRISM_MANIFEST_SIGNING_SEED_HEX", "42".repeat(32))
        .env("PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX", "43".repeat(32))
        .env(
            "PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX",
            keys().1.public_key_hex(),
        )
        .env("PRISM_RUNTIME_WORKERS", "2");
    command.env("PRISM_USERNAME_FALLBACK_ADDRESS", "policy-test-fallback");
    command
}

#[tokio::test]
async fn transition_waits_for_both_accounting_locks() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (a, b, _node, mut config) = setup(&db).await?;
    a.heartbeat(HeartbeatStatus::Stopped).await?;
    b.heartbeat(HeartbeatStatus::Stopped).await?;
    for key in [0x505249534d000003i64, 0x505249534d000002i64] {
        let mut blocked = a.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(key)
            .execute(&mut *blocked)
            .await?;
        let mut next = changed_fee(&config);
        next.payout_policy.pool_fee_policy.as_mut().unwrap().fee_bps = config
            .payout_policy
            .pool_fee_policy
            .as_ref()
            .map_or(200, |fee| fee.fee_bps + 100);
        let before = state(&a).await?;
        let task = tokio::spawn({
            let ledger = a.clone();
            let current = config.clone();
            let target = next.clone();
            async move { ledger.transition_policy(&current, &target).await }
        });
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND classid=$1::bigint::oid AND objid=$2::bigint::oid)")
                    .bind(key >> 32).bind(key & 0xffff_ffff).fetch_one(&a.pool).await?;
                if waiting { return Ok::<_, anyhow::Error>(()); }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await??;
        assert!(!task.is_finished());
        assert_eq!(state(&a).await?, before);
        blocked.commit().await?;
        task.await??;
        config = next;
    }
    db.close(vec![a, b]).await
}

#[tokio::test]
async fn registration_racing_the_stopped_scan_is_not_missed() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (a, b, _node, config) = setup(&db).await?;
    a.heartbeat(HeartbeatStatus::Stopped).await?;
    b.heartbeat(HeartbeatStatus::Stopped).await?;
    let before = state(&a).await?;
    let mut registering = a.pool.begin().await?;
    sqlx::query("INSERT INTO qbit_prism_instances(instance_id,status) VALUES('racing-frontend','{\"state\":\"starting\",\"candidate_offer_lifecycle\":1}')")
        .execute(&mut *registering).await?;
    let task = tokio::spawn({
        let ledger = a.clone();
        let next = changed_fee(&config);
        async move { ledger.transition_policy(&config, &next).await }
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE relation='qbit_prism_instances'::regclass AND NOT granted)")
                .fetch_one(&a.pool).await?;
            if waiting { return Ok::<_, anyhow::Error>(()); }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await??;
    registering.commit().await?;
    assert!(task
        .await?
        .unwrap_err()
        .to_string()
        .contains("racing-frontend"));
    assert_eq!(state(&a).await?, before);
    db.close(vec![a, b]).await
}

#[tokio::test]
async fn cli_loads_env_overrides_and_resolves_fee_addresses_without_registering() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (a, b, node, config) = setup(&db).await?;
    a.heartbeat(HeartbeatStatus::Stopped).await?;
    b.heartbeat(HeartbeatStatus::Stopped).await?;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("next.env");
    std::fs::write(
        &path,
        "export PRISM_POOL_FEE_ENABLED=1\nPRISM_POOL_FEE_BPS='200'\nPRISM_POOL_FEE_ADDRESS=fee\n",
    )?;
    let output =
        tokio::time::timeout(Duration::from_secs(30), cli(&db, &node, &path).output()).await??;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let event: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(
        event["config_fingerprint"],
        changed_fee(&config).fingerprint(&"00".repeat(32))?
    );
    assert_eq!(
        event["policy"]["payout_policy"]["pool_fee_policy"]["fee_bps"],
        200
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM qbit_prism_instances")
            .fetch_one(&a.pool)
            .await?,
        2
    );
    assert!(!String::from_utf8_lossy(&output.stdout).contains(&config.manifest_seed));
    db.close(vec![a, b]).await
}

#[tokio::test]
async fn cli_rejects_invalid_target_without_exposing_env_source() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let (a, b, node, _config) = setup(&db).await?;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("bad.env");
    let before = state(&a).await?;
    for input in [
        "PRISM_POOL_FEE_BPS='secret-unclosed",
        "PRISM_POOL_FEE_ENABLED=1\nPRISM_POOL_FEE_ADDRESS=fee\nPRISM_POOL_FEE_BPS=10001",
        "PRISM_DATABASE_URL=postgresql://elsewhere/other",
        "PRISM_POOL_FEE_ENABLED=maybe",
    ] {
        std::fs::write(&path, input)?;
        let output = cli(&db, &node, &path).output().await?;
        assert!(!output.status.success());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("secret-unclosed"));
        assert_eq!(state(&a).await?, before);
    }
    db.close(vec![a, b]).await
}
