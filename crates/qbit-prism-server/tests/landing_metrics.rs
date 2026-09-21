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
