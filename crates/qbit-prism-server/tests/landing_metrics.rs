//! Landing telemetry through actual candidate offering, PostgreSQL and sockets.
use anyhow::{ensure, Context, Result};
use qbit_prism_server::{coordinator::Coordinator, metrics::Metrics, stratum::MiningBackend};
use qbit_prism_test_gate as gate;
use std::{sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{watch, Semaphore},
    time::timeout,
};

#[allow(dead_code)]
#[path = "support/compact_runtime_e2e/mod.rs"]
mod support;
use support::{
    run,
    socket::{Client, Listener},
    Fixture, DIFFICULTY,
};

const PENDING: &str = "qbit_prism_accepted_block_revision_work_pending_seconds";
const TIMEOUTS: &str = "qbit_prism_revision_work_build_timeouts_total";

const UNKNOWN: &str = "qbit_prism_accepted_block_revision_work_tracking_unknown";
const UNLANDED: &str = "qbit_prism_accepted_block_unlanded_seconds";
const ORPHANED: &str = "qbit_prism_block_candidates_orphaned_total";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_and_failed_terminal_collection_preserves_wait_until_durable_read() -> Result<()>
{
    run(gate::site!(), |f| {
        Box::pin(async move {
            let hash = offered_without_active_proof(f).await?;
            let claim =
                f.b.ledger
                    .claim_candidate(60)
                    .await?
                    .context("peer orphan claim")?;
            f.b.process_candidate(&claim).await?;
            durable_orphan(f, &hash).await?;
            let mut blocker = f.b.ledger.pool.begin().await?;
            sqlx::query("LOCK TABLE qbit_block_candidate_outbox IN ACCESS EXCLUSIVE MODE")
                .execute(&mut *blocker)
                .await?;
            let pool = f.a.ledger.pool.clone();
            let metrics = f.a.metrics.clone();
            let collection = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
                qbit_prism_server::metrics::collectors::database(&pool, &metrics).await
            }));
            timeout(Duration::from_secs(5), async {
                loop {
                    // PostgreSQL may wait for the relation lock during Parse,
                // before the wire proxy can observe an Execute record.
                let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%SELECT count(*)%' AND pid<>pg_backend_pid())")
                    .fetch_one(f.pool()).await?;
                if waiting {
                    break Ok::<_, anyhow::Error>(());
                }
                tokio::task::yield_now().await;
                }
            })
            .await??;
            collection.abort();
            ensure!(matches!(collection.await, Err(error) if error.is_cancelled()));
            blocker.rollback().await?;
            // The unlanded acceptance stays visible, and the failed terminal
            // read leaves the empty known state unknown rather than zero.
            ensure!(
                sample(&f.a.metrics, UNLANDED) > 0.,
                "cancelled read fabricated empty state"
            );
            ensure!(sample(&f.a.metrics, PENDING) == -1.);
            ensure!(sample(&f.a.metrics, UNKNOWN) == 1.);
            let closed = sqlx::postgres::PgPoolOptions::new()
                .connect_lazy_with(f.pool().connect_options().as_ref().clone());
            closed.close().await;
            ensure!(
                qbit_prism_server::metrics::collectors::database(&closed, &f.a.metrics)
                    .await
                    .is_err()
            );
            ensure!(sample(&f.a.metrics, UNLANDED) > 0.);
            ensure!(sample(&f.a.metrics, PENDING) == -1.);
            ensure!(sample(&f.a.metrics, UNKNOWN) == 1.);
            qbit_prism_server::metrics::collectors::database(f.pool(), &f.a.metrics).await?;
            ensure!(sample(&f.a.metrics, UNLANDED) == 0.);
            ensure!(sample(&f.a.metrics, PENDING) == 0.);
            ensure!(sample(&f.a.metrics, UNKNOWN) == 0.);
            no_delivery(&f.a.metrics)?;
            Ok(())
        })
    })
    .await
}

async fn offered_without_active_proof(f: &Fixture) -> Result<String> {
    f.refresh(true).await?;
    let claim = queue_block(&f.a).await?;
    let hash = claim.candidate.block_hash.clone();
    // A definitive null reply can accept a side-chain block. No active proof
    // is fabricated: the fake node still reports the original tip.
    f.node.set_reply(
        "submitblock",
        serde_json::json!([hex::encode(&claim.candidate.block_bytes)]),
        serde_json::Value::Null,
    );
    f.a.process_candidate(&claim).await?;
    let state: String =
        sqlx::query_scalar("SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$1")
            .bind(&hash)
            .fetch_one(f.pool())
            .await?;
    ensure!(state == "reconciliation");
    // A block the node accepted but does not hold on its active chain is an
    // unlanded acceptance, never a known pending delivery wait (#493).
    ensure!(
        sample(&f.a.metrics, PENDING) == 0.,
        "lost race counted as a known pending wait"
    );
    ensure!(
        sample(&f.a.metrics, UNLANDED) > 0.,
        "accepted offer without active proof is invisible"
    );
    ensure!(sample(&f.a.metrics, UNKNOWN) == 0.);
    ensure!(sample(&f.b.metrics, PENDING) == 0.);
    ensure!(sample(&f.b.metrics, UNLANDED) == 0.);
    // A successful read without terminal evidence must preserve the wait.
    qbit_prism_server::metrics::collectors::database(f.pool(), &f.a.metrics).await?;
    ensure!(sample(&f.a.metrics, UNLANDED) > 0.);
    ensure!(sample(&f.a.metrics, PENDING) == 0.);
    let height = claim.candidate.found_block.block_height;
    let competitor = "66".repeat(32);
    f.node
        .set_tip(&competitor, &"77".repeat(32), height + 5, "9999");
    f.node.set_reply(
        "getblockhash",
        serde_json::json!([height]),
        serde_json::json!(competitor),
    );
    sqlx::query("UPDATE qbit_block_candidate_outbox SET next_attempt_at=clock_timestamp() WHERE block_hash=$1")
        .bind(&hash).execute(f.pool()).await?;
    Ok(hash)
}

async fn orphan_marker(f: &Fixture) -> Result<()> {
    sqlx::raw_sql("CREATE FUNCTION orphan_reply_marker() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'prism-execution-marker qbit_block_candidate_outbox UPDATE'; RETURN NEW; END $$; CREATE TRIGGER orphan_reply_marker AFTER UPDATE ON qbit_block_candidate_outbox FOR EACH ROW WHEN (NEW.state='orphaned' AND OLD.state<>'orphaned') EXECUTE FUNCTION orphan_reply_marker();")
        .execute(f.pool()).await?;
    Ok(())
}

async fn durable_orphan(f: &Fixture, hash: &str) -> Result<()> {
    let state: String =
        sqlx::query_scalar("SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$1")
            .bind(hash)
            .fetch_one(f.pool())
            .await?;
    ensure!(state == "orphaned", "orphan did not commit: {state}");
    ensure!(
        f.b.ledger.claim_candidate(60).await?.is_none(),
        "terminal outbox was retried"
    );
    Ok(())
}

fn no_delivery(metrics: &Metrics) -> Result<()> {
    for result in ["published", "degraded", "superseded"] {
        ensure!(
            count(metrics, result) == 0.,
            "orphan invented {result} delivery"
        );
    }
    ensure!(sample(metrics, TIMEOUTS) == 0.);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_orphan_evidence_closes_the_accepting_frontend_without_delivery() -> Result<()> {
    run(gate::site!(), |f| {
        Box::pin(async move {
            let hash = offered_without_active_proof(f).await?;
            let claim =
                f.b.ledger
                    .claim_candidate(60)
                    .await?
                    .context("peer orphan claim")?;
            f.b.process_candidate(&claim).await?;
            durable_orphan(f, &hash).await?;
            ensure!(
                sample(&f.a.metrics, UNLANDED) > 0.,
                "peer invented local knowledge"
            );
            ensure!(sample(&f.a.metrics, PENDING) == 0.);
            for _ in 0..2 {
                qbit_prism_server::metrics::collectors::database(f.pool(), &f.a.metrics).await?;
                ensure!(
                    sample(&f.a.metrics, UNLANDED) == 0.,
                    "peer terminal evidence left a permanent wait"
                );
                ensure!(sample(&f.a.metrics, PENDING) == 0.);
                ensure!(sample(&f.a.metrics, UNKNOWN) == 0.);
                no_delivery(&f.a.metrics)?;
                no_delivery(&f.b.metrics)?;
            }
            // Later genuine active-chain proof may still credit the block, but
            // the retired local acceptance must never reopen or emit a sample.
            let height = claim.candidate.found_block.block_height;
            f.node.set_tip(&"88".repeat(32), &hash, height + 6, "ffff");
            f.node.set_reply(
                "getblockhash",
                serde_json::json!([height]),
                serde_json::json!(hash),
            );
            f.a.refresh_once().await?;
            deliver(&f.a).await?;
            ensure!(sample(&f.a.metrics, PENDING) == 0.);
            ensure!(sample(&f.a.metrics, UNLANDED) == 0.);
            no_delivery(&f.a.metrics)?;
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_orphan_commit_reply_is_unknown_until_terminal_evidence() -> Result<()> {
    run(gate::site!(), |f| {
        Box::pin(async move {
            let hash = offered_without_active_proof(f).await?;
            let claim =
                f.a.ledger
                    .claim_candidate(60)
                    .await?
                    .context("orphan claim")?;
            let revision = f.a.ledger.payout_revision().await?;
            ensure!(f
                .a
                .ledger
                .orphan_candidate_at_revision(&claim, "stale proof", revision - 1)
                .await
                .is_err());
            ensure!(sample(&f.a.metrics, UNLANDED) > 0.);
            ensure!(sample(&f.a.metrics, PENDING) == 0.);
            ensure!(
                sample(&f.a.metrics, UNKNOWN) == 0.,
                "precommit rejection invented uncertainty"
            );
            orphan_marker(f).await?;
            f.proxy.plan(support::execution::Fault {
                table: "qbit_block_candidate_outbox".into(),
                op: "UPDATE".into(),
                phase: support::execution::FaultPhase::AfterCommit,
            });
            ensure!(
                timeout(Duration::from_secs(5), f.a.process_candidate(&claim))
                    .await?
                    .is_err()
            );
            ensure!(f.proxy.fired().is_some(), "lost-reply fault did not fire");
            durable_orphan(f, &hash).await?;
            ensure!(
                sample(&f.a.metrics, PENDING) == -1.,
                "lost orphan outcome remained a known wait"
            );
            ensure!(sample(&f.a.metrics, UNKNOWN) == 1.);
            ensure!(
                sample(&f.a.metrics, UNLANDED) > 0.,
                "lost orphan outcome hid the unlanded acceptance"
            );
            no_delivery(&f.a.metrics)?;
            qbit_prism_server::metrics::collectors::database(f.pool(), &f.a.metrics).await?;
            ensure!(sample(&f.a.metrics, PENDING) == 0.);
            ensure!(sample(&f.a.metrics, UNKNOWN) == 0.);
            ensure!(sample(&f.a.metrics, UNLANDED) == 0.);
            no_delivery(&f.a.metrics)?;
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_orphan_commit_reply_reconciles_without_reopening_tombstone() -> Result<()> {
    run(gate::site!(), |f| {
        Box::pin(async move {
            let hash = offered_without_active_proof(f).await?;
            let claim =
                f.a.ledger
                    .claim_candidate(60)
                    .await?
                    .context("orphan claim")?;
            orphan_marker(f).await?;
            let reply = f
                .proxy
                .pause_after_commit("qbit_block_candidate_outbox", "UPDATE")?;
            let frontend = f.a.clone();
            let processing = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
                frontend.process_candidate(&claim).await
            }));
            timeout(Duration::from_secs(5), reply.entered()).await?;
            durable_orphan(f, &hash).await?;
            processing.abort();
            ensure!(processing.await.unwrap_err().is_cancelled());
            ensure!(
                sample(&f.a.metrics, PENDING) == -1.,
                "cancelled orphan outcome remained a known wait"
            );
            ensure!(sample(&f.a.metrics, UNKNOWN) == 1.);
            ensure!(sample(&f.a.metrics, UNLANDED) > 0.);
            qbit_prism_server::metrics::collectors::database(f.pool(), &f.a.metrics).await?;
            reply.release();
            ensure!(sample(&f.a.metrics, PENDING) == 0.);
            ensure!(sample(&f.a.metrics, UNKNOWN) == 0.);
            ensure!(sample(&f.a.metrics, UNLANDED) == 0.);
            no_delivery(&f.a.metrics)?;
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_fatal_reconcile_preserves_processed_peer_revision() -> Result<()> {
    run(gate::site!(), |f| Box::pin(async move {
        f.refresh(true).await?;
        land(f).await?;
        f.a.refresh_once().await?;
        let disconnected = land(f).await?;
        let height: i64 = sqlx::query_scalar("SELECT block_height FROM qbit_pool_blocks WHERE block_hash=$1")
            .bind(&disconnected).fetch_one(f.pool()).await?;
        sqlx::query("UPDATE qbit_pool_blocks SET maturity_state='mature',matured_at=clock_timestamp() WHERE block_hash=$1")
            .bind(&disconnected).execute(f.pool()).await?;
        let replacement = "77".repeat(32);
        f.node.set_tip(&replacement, &"88".repeat(32), height as u64, "ffff");
        f.node.set_reply("getblockhash", serde_json::json!([height]), serde_json::json!(replacement));
        let revision = f.b.ledger.payout_revision().await?;
        let error = f.b.reconcile(&replacement, height as u64, revision).await.unwrap_err();
        ensure!(format!("{error:#}").contains("mature pool block disconnected"));
        let fatal: Option<String> = sqlx::query_scalar("SELECT fatal_error FROM qbit_prism_cluster WHERE singleton")
            .fetch_one(f.pool()).await?;
        ensure!(fatal.is_some(), "fatal transaction did not commit");
        ensure!(sample(&f.b.metrics, PENDING) > 0., "postcommit fatal discarded a processed peer target");
        ensure!(count(&f.b.metrics, "published") == 0.);
        ensure!(sample(&f.b.metrics, TIMEOUTS) == 0.);
        Ok(())
    })).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_reconcile_before_commit_does_not_poison_peer_landing() -> Result<()> {
    run(gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let stale_revision = f.b.ledger.payout_revision().await?;
            let hash = land(f).await?;
            let height: i64 =
                sqlx::query_scalar("SELECT block_height FROM qbit_pool_blocks WHERE block_hash=$1")
                    .bind(&hash)
                    .fetch_one(f.pool())
                    .await?;
            // A peer confirmed after this frontend read its revision, before
            // its coherent active-chain proof reaches revision validation.
            let error = f.b.reconcile(&hash, height as u64, stale_revision).await;
            ensure!(
                error.is_err(),
                "stale reconciliation unexpectedly committed"
            );
            ensure!(
                sample(&f.b.metrics, PENDING) > 0.,
                "refused transaction invented a lost commit"
            );
            f.b.refresh_once().await?;
            deliver(&f.b).await?;
            ensure!(sample(&f.b.metrics, PENDING) == 0.);
            ensure!(count(&f.b.metrics, "published") == 1.);
            ensure!(sample(&f.b.metrics, TIMEOUTS) == 0.);
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn operator_recovery_binds_first_confirmation_until_real_delivery() -> Result<()> {
    run(gate::site!(), |f| Box::pin(async move {
        f.refresh(true).await?;
        f.node.accept_blocks();
        let claim = queue_block(&f.a).await?;
        let hash = claim.candidate.block_hash.clone();
        sqlx::raw_sql("CREATE FUNCTION landing_insert_marker() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'prism-execution-marker qbit_pool_blocks INSERT'; RETURN NEW; END $$; CREATE TRIGGER landing_insert_marker AFTER INSERT ON qbit_pool_blocks FOR EACH ROW EXECUTE FUNCTION landing_insert_marker();")
            .execute(f.pool()).await?;
        let reply = f.proxy.pause_after_commit("qbit_pool_blocks", "INSERT")?;
        let frontend = f.a.clone();
        let owned_claim = claim.clone();
        let processing = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
            frontend.process_candidate(&owned_claim).await
        }));
        timeout(Duration::from_secs(5), reply.entered()).await?;
        processing.abort();
        ensure!(processing.await.unwrap_err().is_cancelled());
        reply.release();
        ensure!(f.a.ledger.release_recovery_claim(&claim, "interrupted before confirmation").await?);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let qbit_prism_server::ledger::RecoveryClaim::Claimed(recovery) =
            f.a.claim_candidate_for_recovery(&hash, deadline).await? else {
                anyhow::bail!("operator recovery claim refused")
            };
        let parent: Vec<_> = recovery.candidate.block_bytes[4..36].iter().rev().copied().collect();
        f.node.set_reply("getblockheader", serde_json::json!([hash]), serde_json::json!({
            "height": recovery.candidate.found_block.block_height,
            "previousblockhash": hex::encode(parent),
        }));
        let stale_revision = f.a.ledger.payout_revision().await? - 1;
        ensure!(f.a.ledger.finish_candidate_at_revision(&recovery, true, None, stale_revision).await.is_err());
        ensure!(sample(&f.a.metrics, PENDING) > 0., "refused settlement invented an unknown commit");
        f.a.recover_candidate(&recovery, deadline).await?;
        ensure!(count(&f.a.metrics, "published") == 0., "recovery completion is not delivery");
        ensure!(sample(&f.a.metrics, PENDING) > 0.);
        f.a.refresh_once().await?;
        deliver(&f.a).await?;
        ensure!(sample(&f.a.metrics, PENDING) == 0., "recovery discarded its confirmation revision");
        ensure!(count(&f.a.metrics, "published") == 1.);
        ensure!(sample(&f.a.metrics, TIMEOUTS) == 0.);
        f.a.refresh_once().await?;
        deliver(&f.a).await?;
        ensure!(count(&f.a.metrics, "published") == 1.);
        Ok(())
    })).await
}

fn sample(metrics: &Metrics, name: &str) -> f64 {
    metrics
        .render()
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{name} ")))
        .unwrap_or_else(|| panic!("missing sample {name}"))
        .parse()
        .unwrap()
}

fn count(metrics: &Metrics, result: &str) -> f64 {
    sample(
        metrics,
        &format!("qbit_prism_accepted_block_to_revision_work_seconds_count{{result=\"{result}\"}}"),
    )
}

async fn land(f: &Fixture) -> Result<String> {
    land_from(f, &f.a).await
}

async fn land_from(f: &Fixture, frontend: &Arc<Coordinator>) -> Result<String> {
    f.node.accept_blocks();
    let claim = queue_block(frontend).await?;
    frontend.process_candidate(&claim).await?;
    let state: String =
        sqlx::query_scalar("SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$1")
            .bind(&claim.candidate.block_hash)
            .fetch_one(f.pool())
            .await?;
    ensure!(state == "submitted", "offer did not land: {state}");
    ensure!(
        sample(&frontend.metrics, UNLANDED) == 0.,
        "a landed offer stayed unlanded"
    );
    Ok(claim.candidate.block_hash)
}

async fn queue_block(
    frontend: &Arc<Coordinator>,
) -> Result<qbit_prism_server::ledger::CandidateClaim> {
    let worker = frontend.authorize("landing.rig").await?;
    let job = frontend
        .build_job(&worker, "1a2b3c4d", DIFFICULTY, 0.)
        .await?;
    frontend
        .persist_issued_job(&worker, &job, 0, Duration::from_secs(30))
        .await?;
    for nonce in 0..10_000u32 {
        let proof = job.wire.assemble_submission(
            &"00".repeat(job.wire.extranonce2_size),
            &format!("{:08x}", job.wire.ntime),
            &format!("{nonce:08x}"),
            None,
            0,
        )?;
        if proof.block_pass {
            frontend.submit(&worker, &job, proof, false.into()).await?;
            return frontend
                .ledger
                .claim_candidate(60)
                .await?
                .context("candidate missing");
        }
    }
    anyhow::bail!("no block proof in bounded fixture search")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_template_and_second_acceptance_keep_oldest_wait_until_delivery() -> Result<()> {
    run(gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            land(f).await?;
            f.b.refresh_once().await?;
            let first_age = sample(&f.b.metrics, PENDING);
            ensure!(first_age > 0.);
            let second = queue_block(&f.b).await?;
            f.node.set_template(Some(serde_json::json!({})));
            ensure!(f.a.refresh_once().await.is_err());
            ensure!(f.b.refresh_once().await.is_err());
            f.b.process_candidate(&second).await?;
            ensure!(
                sample(&f.b.metrics, PENDING) >= first_age,
                "second acceptance reset oldest age"
            );
            let before = sample(&f.a.metrics, PENDING);
            tokio::time::sleep(Duration::from_millis(20)).await;
            ensure!(
                sample(&f.a.metrics, PENDING) > before,
                "scrape did not age stalled work"
            );
            ensure!(
                sample(&f.a.metrics, TIMEOUTS) == 0.,
                "ordinary failure was called timeout"
            );
            f.node.set_template(None);
            for frontend in [&f.a, &f.b] {
                frontend.refresh_once().await?;
                ensure!(sample(&frontend.metrics, PENDING) > 0.);
                ensure!(count(&frontend.metrics, "published") == 0.);
                ensure!(
                    count(&frontend.metrics, "superseded") == 0.,
                    "supersession fabricated delivery"
                );
                deliver(frontend).await?;
                ensure!(sample(&frontend.metrics, PENDING) == 0.);
                ensure!(count(&frontend.metrics, "superseded") == 1.);
                ensure!(count(&frontend.metrics, "published") == 1.);
            }
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_refresh_keeps_acceptance_without_fabricating_timeout_or_delivery() -> Result<()>
{
    run(gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            land(f).await?;
            let before = sample(&f.a.metrics, PENDING);
            let mut pause = f.node.pause_next("getblocktemplate")?;
            let frontend = f.a.clone();
            let task = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
                frontend.refresh_once().await
            }));
            tokio::time::timeout(Duration::from_secs(5), pause.entered()).await??;
            task.abort();
            ensure!(task.await.unwrap_err().is_cancelled());
            pause.release();
            ensure!(sample(&f.a.metrics, PENDING) >= before);
            ensure!(sample(&f.a.metrics, TIMEOUTS) == 0.);
            ensure!(count(&f.a.metrics, "published") == 0.);
            f.a.refresh_once().await?;
            deliver(&f.a).await?;
            ensure!(count(&f.a.metrics, "published") == 1.);
            Ok(())
        })
    })
    .await
}

async fn wait_sample(metrics: &Metrics, name: &str, expected: f64) -> Result<()> {
    timeout(Duration::from_secs(5), async {
        while sample(metrics, name) != expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("metric did not reach expected value")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_build_deadline_counts_once_and_later_notify_is_degraded() -> Result<()> {
    run(gate::site!(), |f| Box::pin(async move {
        f.refresh(true).await?;
        land(f).await?;
        f.a.refresh_once().await?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let limit = Arc::new(Semaphore::new(0));
        let config = qbit_prism_server::stratum::StratumConfig {
            startup_difficulty: DIFFICULTY,
            vardiff: qbit_prism_server::vardiff::VardiffConfig { enabled: false, minimum: DIFFICULTY, ..Default::default() },
            initial_job_limit: limit.clone(),
            initial_job_timeout_seconds: 0.05,
            ..Default::default()
        };
        let (shutdown, receiver) = watch::channel(false);
        let task = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(qbit_prism_server::stratum::run_listener(
            listener, config, f.a.clone(), f.a.refresh.subscribe(), receiver, f.a.metrics.clone(),
        )));
        let mut client = Client::connect(address).await?;
        client.send(serde_json::json!({"id":1,"method":"mining.subscribe","params":[]})).await?;
        client.response(1).await?;
        client.send(serde_json::json!({"id":2,"method":"mining.authorize","params":["deadline.rig","x"]})).await?;
        ensure!(client.response(2).await?["result"] == true);
        wait_sample(&f.a.metrics, TIMEOUTS, 1.).await?;
        ensure!(sample(&f.a.metrics, PENDING) > 0.);
        ensure!(count(&f.a.metrics, "degraded") == 0., "timeout fabricated delivery");
        limit.add_permits(1);
        client.send(serde_json::json!({"id":3,"method":"mining.get_health","params":[]})).await?;
        loop {
            if client.read().await?["method"] == "mining.notify" { break; }
        }
        wait_sample(&f.a.metrics, PENDING, 0.).await?;
        ensure!(count(&f.a.metrics, "degraded") == 1.);
        ensure!(count(&f.a.metrics, "published") == 0.);
        ensure!(sample(&f.a.metrics, TIMEOUTS) == 1., "success was called a deadline hit");
        shutdown.send_replace(true);
        timeout(Duration::from_secs(5), task).await???;
        Ok(())
    })).await
}

async fn deliver(frontend: &Arc<Coordinator>) -> Result<()> {
    let mut listener = Listener::start(frontend, DIFFICULTY).await?;
    for name in ["delivery-one.rig", "delivery-two.rig"] {
        let mut client = Client::connect(listener.address).await?;
        client.login(name).await?;
        let worker = frontend.authorize(name).await?;
        let job = frontend
            .resume_job(
                &worker,
                client.notify["params"][0].as_str().context("job id")?,
            )
            .await?
            .context("delivered job")?;
        ensure!(
            job.wire.payout_revision == frontend.ledger.payout_revision().await?,
            "old revision delivered"
        );
    }
    listener.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_offer_and_peer_proof_finish_once_only_after_notify_delivery() -> Result<()> {
    run(gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            land(f).await?;
            ensure!(
                sample(&f.a.metrics, PENDING) > 0.,
                "acceptance was not observed"
            );
            for frontend in [&f.a, &f.b] {
                frontend.refresh_once().await?;
                ensure!(
                    sample(&frontend.metrics, PENDING) > 0.,
                    "preparation cleared delivery wait"
                );
                ensure!(count(&frontend.metrics, "published") == 0.);
                deliver(frontend).await?;
                ensure!(sample(&frontend.metrics, PENDING) == 0.);
                ensure!(count(&frontend.metrics, "published") == 1.);
                ensure!(sample(&frontend.metrics, TIMEOUTS) == 0.);
                frontend.refresh_once().await?;
                deliver(frontend).await?;
                ensure!(
                    count(&frontend.metrics, "published") == 1.,
                    "duplicate proof or delivery recounted"
                );
            }
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_settlement_reply_cannot_publish_an_older_acceptance_at_a_newer_revision(
) -> Result<()> {
    run(gate::site!(), |f| Box::pin(async move {
        f.refresh(true).await?;
        f.node.accept_blocks();
        let first = queue_block(&f.a).await?;
        sqlx::raw_sql("CREATE FUNCTION landing_reply_marker() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'prism-execution-marker qbit_block_candidate_outbox UPDATE'; RETURN NEW; END $$; CREATE TRIGGER landing_reply_marker AFTER UPDATE ON qbit_block_candidate_outbox FOR EACH ROW WHEN (NEW.state='submitted' AND OLD.state<>'submitted') EXECUTE FUNCTION landing_reply_marker();")
            .execute(f.pool()).await?;
        let reply = f.proxy.pause_after_commit("qbit_block_candidate_outbox", "UPDATE")?;
        let frontend = f.a.clone();
        let processing = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move { frontend.process_candidate(&first).await }));
        timeout(Duration::from_secs(5), reply.entered()).await?;
        f.b.refresh_once().await?;
        land_from(f, &f.b).await?;
        f.a.refresh_once().await?;
        deliver(&f.a).await?;
        ensure!(count(&f.a.metrics, "published") == 1., "current proof misattributed the older local acceptance");
        ensure!(sample(&f.a.metrics, PENDING) > 0., "unknown committed revision was treated as delivered");
        reply.release();
        timeout(Duration::from_secs(5), processing).await???;
        ensure!(sample(&f.a.metrics, PENDING) == 0.);
        ensure!(count(&f.a.metrics, "published") == 1.);
        ensure!(count(&f.a.metrics, "superseded") == 1.);
        f.a.refresh_once().await?;
        deliver(&f.a).await?;
        ensure!(count(&f.a.metrics, "published") == 1.);
        ensure!(count(&f.a.metrics, "superseded") == 1.);
        Ok(())
    })).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_confirmation_during_audit_reply_wait_cannot_invent_the_original_target() -> Result<()>
{
    run(gate::site!(), |f| Box::pin(async move {
        f.refresh(true).await?;
        f.node.accept_blocks();
        let claim = queue_block(&f.a).await?;
        sqlx::raw_sql("CREATE FUNCTION landing_insert_marker() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'prism-execution-marker qbit_pool_blocks INSERT'; RETURN NEW; END $$; CREATE TRIGGER landing_insert_marker AFTER INSERT ON qbit_pool_blocks FOR EACH ROW EXECUTE FUNCTION landing_insert_marker();")
            .execute(f.pool()).await?;
        let reply = f.proxy.pause_after_commit("qbit_pool_blocks", "INSERT")?;
        let frontend = f.a.clone();
        let processing = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move { frontend.process_candidate(&claim).await }));
        timeout(Duration::from_secs(5), reply.entered()).await?;
        f.b.refresh_once().await?;
        land_from(f, &f.b).await?;
        f.a.refresh_once().await?;
        deliver(&f.a).await?;
        ensure!(sample(&f.a.metrics, PENDING) == -1.);
        ensure!(count(&f.a.metrics, "published") == 1., "later proof fabricated an original target");
        ensure!(count(&f.a.metrics, "superseded") == 0., "unknown original revision was guessed");
        reply.release();
        timeout(Duration::from_secs(5), processing).await???;
        ensure!(sample(&f.a.metrics, PENDING) == -1.);
        ensure!(count(&f.a.metrics, "published") == 1.);
        ensure!(count(&f.a.metrics, "superseded") == 0.);
        Ok(())
    })).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn active_proof_confirmation_and_maturity_bind_the_same_committed_revision() -> Result<()> {
    run(gate::site!(), |f| {
        Box::pin(async move {
            // Bootstrap pays the solver immediately, so maturity updates a real
            // payout row and takes the existing second revision-bump branch.
            f.refresh(false).await?;
            f.node.accept_blocks();
            let claim = queue_block(&f.a).await?;
            let hash = claim.candidate.block_hash.clone();
            sqlx::raw_sql("CREATE FUNCTION landing_insert_marker() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'prism-execution-marker qbit_pool_blocks INSERT'; RETURN NEW; END $$; CREATE TRIGGER landing_insert_marker AFTER INSERT ON qbit_pool_blocks FOR EACH ROW EXECUTE FUNCTION landing_insert_marker();")
                .execute(f.pool()).await?;
            let reply = f.proxy.pause_after_commit("qbit_pool_blocks", "INSERT")?;
            let frontend = f.a.clone();
            let processing = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
                frontend.process_candidate(&claim).await
            }));
            timeout(Duration::from_secs(5), reply.entered()).await?;
            processing.abort();
            ensure!(processing.await.unwrap_err().is_cancelled());
            reply.release();
            // The real offer and audit landing committed, but confirmation has
            // not run. This frontend's first proof now confirms and matures it.
            f.node.set_tip(&"66".repeat(32), &hash, 1101, "9999");
            let before = f.b.ledger.payout_revision().await?;
            f.b.refresh_once().await?;
            ensure!(f.b.ledger.payout_revision().await? >= before + 2);
            let maturity: String = sqlx::query_scalar(
                "SELECT maturity_state FROM qbit_pool_blocks WHERE block_hash=$1",
            )
            .bind(&hash)
            .fetch_one(f.pool())
            .await?;
            ensure!(maturity == "mature");
            ensure!(sample(&f.b.metrics, PENDING) > 0.);
            deliver(&f.b).await?;
            ensure!(sample(&f.b.metrics, PENDING) == 0.);
            ensure!(count(&f.b.metrics, "published") == 1.);
            ensure!(
                count(&f.b.metrics, "superseded") == 0.,
                "an intermediate uncommitted revision was used"
            );
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_commit_reply_keeps_unknown_without_fabricated_histogram() -> Result<()> {
    run(gate::site!(), |f| Box::pin(async move {
        f.refresh(true).await?;
        f.node.accept_blocks();
        let claim = queue_block(&f.a).await?;
        sqlx::raw_sql("CREATE FUNCTION landing_reply_marker() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'prism-execution-marker qbit_block_candidate_outbox UPDATE'; RETURN NEW; END $$; CREATE TRIGGER landing_reply_marker AFTER UPDATE ON qbit_block_candidate_outbox FOR EACH ROW WHEN (NEW.state='submitted' AND OLD.state<>'submitted') EXECUTE FUNCTION landing_reply_marker();")
            .execute(f.pool()).await?;
        let reply = f.proxy.pause_after_commit("qbit_block_candidate_outbox", "UPDATE")?;
        let frontend = f.a.clone();
        let processing = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move { frontend.process_candidate(&claim).await }));
        timeout(Duration::from_secs(5), reply.entered()).await?;
        processing.abort();
        ensure!(processing.await.unwrap_err().is_cancelled());
        reply.release();
        ensure!(sample(&f.a.metrics, PENDING) == -1.);
        f.a.refresh_once().await?;
        deliver(&f.a).await?;
        ensure!(sample(&f.a.metrics, PENDING) == -1., "current revision guessed the lost committed revision");
        for result in ["published", "degraded", "superseded"] {
            ensure!(count(&f.a.metrics, result) == 0.);
        }
        ensure!(sample(&f.a.metrics, TIMEOUTS) == 0.);
        // A different frontend starts at its own current active-chain proof.
        f.b.refresh_once().await?;
        ensure!(sample(&f.b.metrics, PENDING) > 0.);
        deliver(&f.b).await?;
        ensure!(sample(&f.b.metrics, PENDING) == 0.);
        ensure!(count(&f.b.metrics, "published") == 1.);
        Ok(())
    })).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_commit_reply_timeouts_are_not_revision_work_failures() -> Result<()> {
    run(gate::site!(), |f| Box::pin(async move {
        f.refresh(true).await?;
        f.node.accept_blocks();
        let claim = queue_block(&f.a).await?;
        sqlx::raw_sql("CREATE FUNCTION landing_reply_marker() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'prism-execution-marker qbit_block_candidate_outbox UPDATE'; RETURN NEW; END $$; CREATE TRIGGER landing_reply_marker AFTER UPDATE ON qbit_block_candidate_outbox FOR EACH ROW WHEN (NEW.state='submitted' AND OLD.state<>'submitted') EXECUTE FUNCTION landing_reply_marker();")
            .execute(f.pool()).await?;
        let reply = f.proxy.pause_after_commit("qbit_block_candidate_outbox", "UPDATE")?;
        let frontend = f.a.clone();
        let processing = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move { frontend.process_candidate(&claim).await }));
        timeout(Duration::from_secs(5), reply.entered()).await?;
        processing.abort();
        ensure!(processing.await.unwrap_err().is_cancelled());
        reply.release();
        ensure!(sample(&f.a.metrics, PENDING) == -1.);
        ensure!(sample(&f.a.metrics, UNKNOWN) == 1.);
        f.a.refresh_once().await?;
        // A real initial-job build held at admission until its deadline, while
        // only the lost settlement's unknown tracking remains on this frontend.
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let limit = Arc::new(Semaphore::new(0));
        let config = qbit_prism_server::stratum::StratumConfig {
            startup_difficulty: DIFFICULTY,
            vardiff: qbit_prism_server::vardiff::VardiffConfig { enabled: false, minimum: DIFFICULTY, ..Default::default() },
            initial_job_limit: limit.clone(),
            initial_job_timeout_seconds: 0.05,
            ..Default::default()
        };
        let stats = config.stats.clone();
        let (shutdown, receiver) = watch::channel(false);
        let task = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(qbit_prism_server::stratum::run_listener(
            listener, config, f.a.clone(), f.a.refresh.subscribe(), receiver, f.a.metrics.clone(),
        )));
        let mut client = Client::connect(address).await?;
        client.send(serde_json::json!({"id":1,"method":"mining.subscribe","params":[]})).await?;
        client.response(1).await?;
        client.send(serde_json::json!({"id":2,"method":"mining.authorize","params":["deadline.rig","x"]})).await?;
        ensure!(client.response(2).await?["result"] == true);
        timeout(Duration::from_secs(5), async {
            while stats.snapshot(0).job_delivery_failures == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("the held build never hit its deadline")?;
        ensure!(
            sample(&f.a.metrics, TIMEOUTS) == 0.,
            "a deadline hit while only unknown tracking remained was counted as a revision-work failure"
        );
        ensure!(sample(&f.a.metrics, PENDING) == -1., "unknown tracking was reported as zero");
        ensure!(sample(&f.a.metrics, UNKNOWN) == 1.);
        limit.add_permits(1);
        client.send(serde_json::json!({"id":3,"method":"mining.get_health","params":[]})).await?;
        loop {
            if client.read().await?["method"] == "mining.notify" { break; }
        }
        ensure!(sample(&f.a.metrics, PENDING) == -1., "current revision guessed the lost committed revision");
        for result in ["published", "degraded", "superseded"] {
            ensure!(count(&f.a.metrics, result) == 0., "unknown tracking fabricated a {result} delivery");
        }
        ensure!(sample(&f.a.metrics, TIMEOUTS) == 0.);
        ensure!(sample(&f.a.metrics, UNKNOWN) == 1.);
        shutdown.send_replace(true);
        timeout(Duration::from_secs(5), task).await???;
        Ok(())
    })).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_offer_reply_starts_at_active_proof_and_is_not_a_work_build_timeout() -> Result<()> {
    run(gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            f.node.accept_blocks();
            let claim = queue_block(&f.a).await?;
            let hash = claim.candidate.block_hash.clone();
            let mut reply = f.node.pause_next("submitblock")?;
            let frontend = f.a.clone();
            let processing = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
                frontend.process_candidate(&claim).await
            }));
            timeout(Duration::from_secs(5), reply.entered()).await??;
            ensure!(
                sample(&f.a.metrics, PENDING) == 0.,
                "withheld reply invented local acceptance knowledge"
            );
            timeout(Duration::from_secs(5), processing).await???;
            reply.release();
            let outcome: String = sqlx::query_scalar(
                "SELECT offer_outcome FROM qbit_block_candidate_outbox WHERE block_hash=$1",
            )
            .bind(&hash)
            .fetch_one(f.pool())
            .await?;
            ensure!(outcome == "unknown");
            ensure!(sample(&f.a.metrics, PENDING) > 0.);
            ensure!(sample(&f.a.metrics, UNLANDED) == 0.);
            f.a.refresh_once().await?;
            deliver(&f.a).await?;
            ensure!(count(&f.a.metrics, "published") == 1.);
            ensure!(
                sample(&f.a.metrics, TIMEOUTS) == 0.,
                "node offer timeout was attributed to new-work build"
            );
            let rendered = f.a.metrics.render();
            for identity in [&hash, "landing.rig", "delivery-one.rig", "delivery-two.rig"] {
                ensure!(!rendered.contains(identity), "identity entered metrics");
            }
            Ok(())
        })
    })
    .await
}

/// #493 point 1: a routine lost race. The node accepts the offer, a competitor
/// holds the height, and the orphan is proven after the configured
/// confirmations. On no frontend is the pending age ever a known wait, so the
/// paging rule cannot fire; the unlanded gauge reports the acceptance until
/// the proof. The positive control lands an active block on the same
/// fixture: its wait is a known pending age that grows until real delivery.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_race_stays_unlanded_until_its_orphan_proof_and_an_active_block_still_pages(
) -> Result<()> {
    run(gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let claim = queue_block(&f.a).await?;
            let hash = claim.candidate.block_hash.clone();
            let height = claim.candidate.found_block.block_height;
            f.node.set_reply(
                "submitblock",
                serde_json::json!([hex::encode(&claim.candidate.block_bytes)]),
                serde_json::Value::Null,
            );
            f.a.process_candidate(&claim).await?;
            ensure!(row_state(f, &hash).await? == "reconciliation");
            ensure!(
                sample(&f.a.metrics, PENDING) == 0.,
                "lost race counted as a known pending wait"
            );
            let accepted_age = sample(&f.a.metrics, UNLANDED);
            ensure!(accepted_age > 0., "accepted offer is invisible");
            ensure!(sample(&f.a.metrics, UNKNOWN) == 0.);
            ensure!(sample(&f.b.metrics, PENDING) == 0.);
            ensure!(sample(&f.b.metrics, UNLANDED) == 0.);
            // The competitor advances to one confirmation short of the proof.
            // Every frontend reconciles the block as not active and keeps
            // delivering ordinary work; nothing lands, closes or resets it.
            let competitor = "66".repeat(32);
            f.node.set_reply(
                "getblockhash",
                serde_json::json!([height]),
                serde_json::json!(competitor),
            );
            f.node
                .set_tip(&"68".repeat(32), &"67".repeat(32), height + 4, "9999");
            for frontend in [&f.a, &f.b] {
                frontend.refresh_once().await?;
                deliver(frontend).await?;
                ensure!(sample(&frontend.metrics, PENDING) == 0.);
                no_delivery(&frontend.metrics)?;
            }
            ensure!(
                sample(&f.a.metrics, UNLANDED) >= accepted_age,
                "reconciliation reset the unlanded age"
            );
            ensure!(sample(&f.b.metrics, UNLANDED) == 0.);
            retry_reconciliation(f, &hash).await?;
            ensure!(row_state(f, &hash).await? == "reconciliation");
            ensure!(sample(&f.a.metrics, PENDING) == 0.);
            ensure!(sample(&f.a.metrics, UNLANDED) >= accepted_age);
            ensure!(sample(&f.a.metrics, ORPHANED) == 0.);
            // The configured sixth confirmation proves the orphan on the
            // offering frontend itself. The wait closes without a sample.
            f.node
                .set_tip(&"69".repeat(32), &"68".repeat(32), height + 5, "aaaa");
            retry_reconciliation(f, &hash).await?;
            durable_orphan(f, &hash).await?;
            ensure!(sample(&f.a.metrics, ORPHANED) == 1.);
            ensure!(sample(&f.a.metrics, UNLANDED) == 0.);
            ensure!(sample(&f.a.metrics, PENDING) == 0.);
            ensure!(sample(&f.a.metrics, UNKNOWN) == 0.);
            for frontend in [&f.a, &f.b] {
                frontend.refresh_once().await?;
                deliver(frontend).await?;
                ensure!(sample(&frontend.metrics, PENDING) == 0.);
                ensure!(sample(&frontend.metrics, UNLANDED) == 0.);
                no_delivery(&frontend.metrics)?;
            }
            // Positive control: an active block's wait is a known pending age
            // on every frontend, grows until real delivery, and is never
            // unlanded once the node holds it.
            land(f).await?;
            ensure!(sample(&f.a.metrics, UNLANDED) == 0.);
            let before = sample(&f.a.metrics, PENDING);
            ensure!(before > 0., "an active block's wait is not a known age");
            tokio::time::sleep(Duration::from_millis(20)).await;
            ensure!(
                sample(&f.a.metrics, PENDING) > before,
                "an active block's known wait did not age"
            );
            for frontend in [&f.a, &f.b] {
                frontend.refresh_once().await?;
                ensure!(sample(&frontend.metrics, PENDING) > 0.);
                ensure!(sample(&frontend.metrics, UNLANDED) == 0.);
                deliver(frontend).await?;
                ensure!(sample(&frontend.metrics, PENDING) == 0.);
                ensure!(count(&frontend.metrics, "published") == 1.);
                ensure!(sample(&frontend.metrics, TIMEOUTS) == 0.);
            }
            Ok(())
        })
    })
    .await
}

async fn row_state(f: &Fixture, hash: &str) -> Result<String> {
    Ok(
        sqlx::query_scalar("SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$1")
            .bind(hash)
            .fetch_one(f.pool())
            .await?,
    )
}

/// Make the reconciliation row due now and process it on the offering
/// frontend, as its ordinary retry would.
async fn retry_reconciliation(f: &Fixture, hash: &str) -> Result<()> {
    sqlx::query("UPDATE qbit_block_candidate_outbox SET next_attempt_at=clock_timestamp() WHERE block_hash=$1")
        .bind(hash).execute(f.pool()).await?;
    let claim =
        f.a.ledger
            .claim_candidate(60)
            .await?
            .context("reconciliation retry claim")?;
    ensure!(claim.candidate.block_hash == hash);
    f.a.process_candidate(&claim).await
}

/// #493: the candidate collector's paging measurement. A node-accepted lost
/// race in reconciliation is unfinished but acknowledged, so it ages only the
/// all-unfinished gauge; a pending row and an offer whose one submitblock
/// outcome is unknown age the unacknowledged gauge the paging rule reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn candidate_collector_keeps_an_acknowledged_lost_race_out_of_the_paging_age() -> Result<()> {
    run(gate::site!(), |f| {
        Box::pin(async move {
            let hash = offered_without_active_proof(f).await?;
            let census =
                qbit_prism_server::metrics::collectors::database(f.pool(), &f.a.metrics).await?;
            ensure!(census.candidates == 1);
            ensure!(census.candidate_oldest > Duration::ZERO);
            ensure!(
                census.candidate_oldest_unacknowledged == Duration::ZERO,
                "a node-accepted lost race counted as unacknowledged"
            );
            // A found block the node has not been offered yet is unacknowledged.
            f.b.refresh_once().await?;
            let waiting = queue_block(&f.b).await?;
            let census =
                qbit_prism_server::metrics::collectors::database(f.pool(), &f.a.metrics).await?;
            ensure!(census.candidates == 2);
            ensure!(census.candidate_oldest_unacknowledged > Duration::ZERO);
            ensure!(census.candidate_oldest_unacknowledged <= census.candidate_oldest);
            // Its one offer fails without a definitive reply: the outcome is
            // unknown, the row is reconciled against the chain (not active,
            // no competitor at its height yet) and stays unacknowledged.
            f.b.process_candidate(&waiting).await?;
            let (state, outcome): (String, Option<String>) = sqlx::query_as(
                "SELECT state,offer_outcome FROM qbit_block_candidate_outbox WHERE block_hash=$1",
            )
            .bind(&waiting.candidate.block_hash)
            .fetch_one(f.pool())
            .await?;
            ensure!(state == "reconciliation" && outcome.as_deref() == Some("unknown"));
            ensure!(row_state(f, &hash).await? == "reconciliation");
            let census =
                qbit_prism_server::metrics::collectors::database(f.pool(), &f.a.metrics).await?;
            ensure!(census.candidates == 2);
            ensure!(
                census.candidate_oldest_unacknowledged > Duration::ZERO,
                "an unknown offer outcome was treated as acknowledged"
            );
            ensure!(census.candidate_oldest_unacknowledged < census.candidate_oldest);
            // Without a definitive acceptance nothing is unlanded or pending.
            ensure!(sample(&f.b.metrics, PENDING) == 0.);
            ensure!(sample(&f.b.metrics, UNLANDED) == 0.);
            // Published, the gauges render together and invalidate together.
            f.a.metrics.publish_database(Some(census));
            ensure!(
                sample(
                    &f.a.metrics,
                    "qbit_prism_block_candidate_oldest_unacknowledged_seconds"
                ) > 0.
            );
            ensure!(
                sample(
                    &f.a.metrics,
                    "qbit_prism_block_candidate_oldest_pending_seconds"
                ) > 0.
            );
            f.a.metrics.publish_database(None);
            ensure!(
                sample(
                    &f.a.metrics,
                    "qbit_prism_block_candidate_oldest_unacknowledged_seconds"
                ) == -1.
            );
            ensure!(
                sample(
                    &f.a.metrics,
                    "qbit_prism_block_candidate_oldest_pending_seconds"
                ) == -1.
            );
            Ok(())
        })
    })
    .await
}

/// #493 review B1: a definitive node rejection is the pool building an
/// invalid block, and it keeps paging through the unacknowledged age; a
/// side-chain reply (`inconclusive`) is a lost tip race and does not. Neither
/// opens a frontend identity.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_rejected_block_pages_as_unacknowledged_but_a_side_chain_reply_does_not() -> Result<()>
{
    run(gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let mut hashes = Vec::new();
            // One block per frontend: each extranonce space yields its own proof.
            for (frontend, reply, unacknowledged) in
                [(&f.a, "inconclusive", false), (&f.b, "bad-cb-payee", true)]
            {
                let claim = queue_block(frontend).await?;
                let hash = claim.candidate.block_hash.clone();
                f.node.set_reply(
                    "submitblock",
                    serde_json::json!([hex::encode(&claim.candidate.block_bytes)]),
                    serde_json::json!(reply),
                );
                frontend.process_candidate(&claim).await?;
                let (state, outcome, recorded): (String, Option<String>, Option<String>) =
                    sqlx::query_as(
                        "SELECT state,offer_outcome,offer_reply FROM qbit_block_candidate_outbox WHERE block_hash=$1",
                    )
                    .bind(&hash)
                    .fetch_one(f.pool())
                    .await?;
                ensure!(
                    state == "reconciliation"
                        && outcome.as_deref() == Some("rejected")
                        && recorded.as_deref() == Some(reply),
                    "{state} {outcome:?} {recorded:?}"
                );
                let census =
                    qbit_prism_server::metrics::collectors::database(f.pool(), &f.a.metrics)
                        .await?;
                ensure!(census.candidates == hashes.len() as u64 + 1);
                ensure!(census.candidate_oldest > Duration::ZERO);
                ensure!(
                    (census.candidate_oldest_unacknowledged > Duration::ZERO) == unacknowledged,
                    "reply {reply}: unacknowledged age {:?}",
                    census.candidate_oldest_unacknowledged
                );
                ensure!(census.candidate_oldest_landing_failed == Duration::ZERO);
                ensure!(sample(&frontend.metrics, PENDING) == 0.);
                ensure!(sample(&frontend.metrics, UNLANDED) == 0.);
                ensure!(sample(&frontend.metrics, UNKNOWN) == 0.);
                hashes.push(hash);
            }
            // The rejected block is the younger row: the paging age is its
            // own age, not the older side-chain row's.
            let census =
                qbit_prism_server::metrics::collectors::database(f.pool(), &f.a.metrics).await?;
            ensure!(census.candidate_oldest_unacknowledged < census.candidate_oldest);
            Ok(())
        })
    })
    .await
}

/// #493 review M1: a won block whose audit landing fails after the offer is
/// reported by the landing-failed age (never by the unacknowledged age), and
/// leaves it as soon as a retry lands. Here the landing transaction is
/// aborted through the wire proxy on the first attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_audit_landing_after_the_offer_is_reported_until_a_retry_lands() -> Result<()> {
    run(gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let claim = queue_block(&f.a).await?;
            let hash = claim.candidate.block_hash.clone();
            f.node.set_reply(
                "submitblock",
                serde_json::json!([hex::encode(&claim.candidate.block_bytes)]),
                serde_json::Value::Null,
            );
            sqlx::raw_sql("CREATE FUNCTION landing_insert_marker() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'prism-execution-marker qbit_pool_blocks INSERT'; RETURN NEW; END $$; CREATE TRIGGER landing_insert_marker AFTER INSERT ON qbit_pool_blocks FOR EACH ROW EXECUTE FUNCTION landing_insert_marker();")
                .execute(f.pool()).await?;
            f.proxy.plan(support::execution::Fault {
                table: "qbit_pool_blocks".into(),
                op: "INSERT".into(),
                phase: support::execution::FaultPhase::AfterExecution,
            });
            let outcome = timeout(Duration::from_secs(10), f.a.process_candidate(&claim)).await?;
            ensure!(f.proxy.fired().is_some(), "landing fault did not fire");
            let (state, error): (String, Option<String>) = sqlx::query_as(
                "SELECT state,last_error FROM qbit_block_candidate_outbox WHERE block_hash=$1",
            )
            .bind(&hash)
            .fetch_one(f.pool())
            .await?;
            ensure!(
                state == "reconciliation"
                    && error.as_deref().is_some_and(|error| {
                        error.starts_with(qbit_prism_server::ledger::LANDING_FAILED_REASON_PREFIX)
                    }),
                "{state} {error:?} (processing: {outcome:?})"
            );
            let census =
                qbit_prism_server::metrics::collectors::database(f.pool(), &f.a.metrics).await?;
            ensure!(census.candidates == 1);
            ensure!(
                census.candidate_oldest_landing_failed > Duration::ZERO,
                "a failed landing was silent"
            );
            ensure!(census.candidate_oldest_unacknowledged == Duration::ZERO);
            f.a.metrics.publish_database(Some(census));
            ensure!(
                sample(&f.a.metrics, "qbit_prism_block_candidate_oldest_landing_failed_seconds")
                    > 0.
            );
            // The retry lands the audit; the row waits for the chain again.
            retry_reconciliation(f, &hash).await?;
            let (state, error): (String, Option<String>) = sqlx::query_as(
                "SELECT state,last_error FROM qbit_block_candidate_outbox WHERE block_hash=$1",
            )
            .bind(&hash)
            .fetch_one(f.pool())
            .await?;
            ensure!(
                state == "reconciliation"
                    && !error.as_deref().unwrap_or_default().starts_with(
                        qbit_prism_server::ledger::LANDING_FAILED_REASON_PREFIX
                    ),
                "{state} {error:?}"
            );
            let census =
                qbit_prism_server::metrics::collectors::database(f.pool(), &f.a.metrics).await?;
            ensure!(census.candidate_oldest_landing_failed == Duration::ZERO);
            ensure!(census.candidate_oldest_unacknowledged == Duration::ZERO);
            ensure!(census.candidate_oldest > Duration::ZERO);
            Ok(())
        })
    })
    .await
}
