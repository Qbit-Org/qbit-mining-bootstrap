//! Exercise accounting retries through the real refresh scheduler and ledger.
use anyhow::{ensure, Context, Result};
use qbit_prism_server::{coordinator::Coordinator, metrics::Metrics, stratum::MiningBackend};
use qbit_prism_test_gate as gate;
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::watch,
    time::{sleep, timeout, timeout_at, Instant},
};

#[path = "support/fake_qbitd.rs"]
#[allow(dead_code)]
mod fake_qbitd;
#[path = "support/ledger_database.rs"]
#[allow(dead_code)]
mod ledger_database;

const ISSUE_BOUND: Duration = Duration::from_secs(2);

async fn wait_for_error(coordinator: &Coordinator, expected: &str) -> Result<()> {
    timeout(ISSUE_BOUND, async {
        loop {
            if coordinator.last_error.read().await.as_deref() == Some(expected) {
                return;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .with_context(|| format!("scheduler did not report {expected}"))
}

async fn issue_current(coordinator: &Coordinator, tip: &str) -> Result<()> {
    let worker = coordinator.authorize("alice.scheduled").await?;
    timeout(ISSUE_BOUND, async {
        loop {
            if let Ok(job) = coordinator.build_job(&worker, "55667788", 1e-12, 0.0).await {
                if job.wire.previousblockhash == tip {
                    coordinator
                        .persist_issued_job(&worker, &job, 0x0000_e000, Duration::from_secs(30))
                        .await?;
                    ensure!(coordinator.ledger.job(&job.wire.job_id).await?.is_some());
                    return Ok::<_, anyhow::Error>(());
                }
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .context("scheduled work was not issued")?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn immediate_retry_budget_resets_only_after_an_external_tick() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = ledger_database::FixtureDatabase::open(&raw, "retry_budget_").await?;
    let node = fake_qbitd::FakeNode::open().await?;
    let mut config = fake_qbitd::coordinator_config(db.url.clone(), &node, "retry-budget")?;
    config.poll_interval = ISSUE_BOUND;
    let coordinator = Coordinator::new(config, Arc::new(Metrics::default())).await?;
    let result = async {
        coordinator.refresh_once().await?;
        let original = coordinator.ledger.chain_observation_state().await?;
        let replacement = "ef".repeat(32);
        node.set_tip(&replacement, &"cd".repeat(32), 100, "01");
        let mut first = node.pause_next("getblockchaininfo")?;
        let (stop, stopped) = watch::channel(false);
        let observer = coordinator.clone();
        let started = Instant::now();
        let scheduler = tokio::spawn(async move { observer.refresh_loop(stopped).await });
        let run = async {
            timeout(ISSUE_BOUND, first.entered()).await??;
            let mut second = node.pause_next("getblockchaininfo")?;
            sqlx::query(
                "UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1 WHERE singleton",
            )
            .execute(&coordinator.ledger.pool)
            .await?;
            first.release();
            timeout_at(started + ISSUE_BOUND, second.entered()).await??;
            let mut third = node.pause_next("getblockchaininfo")?;
            sqlx::query(
                "UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1 WHERE singleton",
            )
            .execute(&coordinator.ledger.pool)
            .await?;
            second.release();
            // Await the next actual request, then prove it waited for the
            // external tick. No third attempt may replenish its own budget.
            timeout(ISSUE_BOUND * 2, third.entered()).await??;
            ensure!(
                started.elapsed() >= ISSUE_BOUND,
                "a third immediate attempt bypassed the external tick"
            );
            ensure!(
                coordinator
                    .ledger
                    .chain_observation_state()
                    .await?
                    .chain_epoch
                    == original.chain_epoch
            );
            // A new external trigger may earn one new retry; it still carries
            // the very first witness epoch through these accounting changes.
            let mut fourth = node.pause_next("getblockchaininfo")?;
            sqlx::query(
                "UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1 WHERE singleton",
            )
            .execute(&coordinator.ledger.pool)
            .await?;
            third.release();
            timeout_at(started + ISSUE_BOUND * 2, fourth.entered()).await??;
            fourth.release();
            issue_current(&coordinator, &replacement).await?;
            let after = coordinator.ledger.chain_observation_state().await?;
            ensure!(after.chain_epoch == original.chain_epoch + 1);
            ensure!(after.payout_revision == original.payout_revision + 4);
            Ok(())
        }
        .await;
        let _ = stop.send(true);
        scheduler.abort();
        let _ = scheduler.await;
        run
    }
    .await;
    coordinator.ledger.pool.close().await;
    db.close(result).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_after_refusal_prevents_the_immediate_attempt() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = ledger_database::FixtureDatabase::open(&raw, "retry_shutdown_").await?;
    let node = fake_qbitd::FakeNode::open().await?;
    let mut config = fake_qbitd::coordinator_config(db.url.clone(), &node, "retry-shutdown")?;
    config.poll_interval = ISSUE_BOUND;
    let coordinator = Coordinator::new(config, Arc::new(Metrics::default())).await?;
    let result = async {
        coordinator.refresh_once().await?;
        let original = coordinator.ledger.chain_observation_state().await?;
        node.set_tip(&"ef".repeat(32), &"cd".repeat(32), 100, "01");
        let mut first = node.pause_next("getblockchaininfo")?;
        let (stop, stopped) = watch::channel(false);
        let observer = coordinator.clone();
        let mut scheduler = tokio::spawn(async move { observer.refresh_loop(stopped).await });
        let run = async {
            timeout(ISSUE_BOUND, first.entered()).await??;
            let mut forbidden = node.pause_next("getblockchaininfo")?;
            sqlx::query(
                "UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1 WHERE singleton",
            )
            .execute(&coordinator.ledger.pool)
            .await?;
            stop.send(true)?;
            first.release();
            timeout(ISSUE_BOUND, &mut scheduler).await??;
            ensure!(
                timeout(Duration::from_millis(50), forbidden.entered())
                    .await
                    .is_err(),
                "shutdown started another RPC proof"
            );
            let after = coordinator.ledger.chain_observation_state().await?;
            ensure!(
                after.chain_epoch == original.chain_epoch
                    && after.best_tip_hash == original.best_tip_hash
            );
            ensure!(after.payout_revision == original.payout_revision + 1);
            wait_for_error(&coordinator, "chain observation revision changed").await?;
            Ok(())
        }
        .await;
        scheduler.abort();
        if !scheduler.is_finished() {
            let _ = scheduler.await;
        }
        run
    }
    .await;
    coordinator.ledger.pool.close().await;
    db.close(result).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_aba_during_the_immediate_proof_consumes_the_original_witness() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = ledger_database::FixtureDatabase::open(&raw, "retry_peer_").await?;
    let node = fake_qbitd::FakeNode::open().await?;
    let peer_node = fake_qbitd::FakeNode::open().await?;
    let mut config = fake_qbitd::coordinator_config(db.url.clone(), &node, "retry-peer-old")?;
    config.poll_interval = ISSUE_BOUND;
    let coordinator = Coordinator::new(config, Arc::new(Metrics::default())).await?;
    let peer = Coordinator::new(
        fake_qbitd::coordinator_config(db.url.clone(), &peer_node, "retry-peer-new")?,
        Arc::new(Metrics::default()),
    )
    .await?;
    let result = async {
        coordinator.refresh_once().await?;
        peer.refresh_once().await?;
        node.set_tip(&"ef".repeat(32), &"cd".repeat(32), 100, "01");
        let mut first = node.pause_next("getblockchaininfo")?;
        let (stop, stopped) = watch::channel(false);
        let observer = coordinator.clone();
        let scheduler = tokio::spawn(async move { observer.refresh_loop(stopped).await });
        let run = async {
            timeout(ISSUE_BOUND, first.entered()).await??;
            let mut second = node.pause_next("getblockchaininfo")?;
            sqlx::query(
                "UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1 WHERE singleton",
            )
            .execute(&coordinator.ledger.pool)
            .await?;
            first.release();
            timeout(ISSUE_BOUND, second.entered()).await??;
            peer_node.set_tip(&"12".repeat(32), &"cd".repeat(32), 100, "01");
            peer.refresh_once().await?;
            peer_node.set_tip(&"ab".repeat(32), &"cd".repeat(32), 100, "01");
            peer.refresh_once().await?;
            let checkpoint = peer.ledger.chain_observation_state().await?;
            second.release();
            wait_for_error(&coordinator, "chain observation epoch changed").await?;
            for _ in 0..2 {
                ensure!(
                    coordinator.refresh_once().await.is_err(),
                    "unchanged poll revived a consumed witness"
                );
            }
            let after = coordinator.ledger.chain_observation_state().await?;
            ensure!(
                after.chain_epoch == checkpoint.chain_epoch
                    && after.payout_revision == checkpoint.payout_revision
                    && after.best_tip_hash == checkpoint.best_tip_hash
            );
            Ok(())
        }
        .await;
        let _ = stop.send(true);
        scheduler.abort();
        let _ = scheduler.await;
        run
    }
    .await;
    coordinator.ledger.pool.close().await;
    peer.ledger.pool.close().await;
    db.close(result).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_retry_epoch_refusal_keeps_the_normal_poll_cadence() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = ledger_database::FixtureDatabase::open(&raw, "no_epoch_retry_").await?;
    let node = fake_qbitd::FakeNode::open().await?;
    let peer_node = fake_qbitd::FakeNode::open().await?;
    let mut config = fake_qbitd::coordinator_config(db.url.clone(), &node, "no-epoch-retry")?;
    config.poll_interval = ISSUE_BOUND;
    let coordinator = Coordinator::new(config, Arc::new(Metrics::default())).await?;
    let peer = Coordinator::new(
        fake_qbitd::coordinator_config(db.url.clone(), &peer_node, "epoch-peer")?,
        Arc::new(Metrics::default()),
    )
    .await?;
    let result = async {
        coordinator.refresh_once().await?;
        peer.refresh_once().await?;
        node.set_tip(&"ef".repeat(32), &"cd".repeat(32), 100, "01");
        let mut first = node.pause_next("getblockchaininfo")?;
        let (stop, stopped) = watch::channel(false);
        let observer = coordinator.clone();
        let started = Instant::now();
        let scheduler = tokio::spawn(async move { observer.refresh_loop(stopped).await });
        let run = async {
            timeout(ISSUE_BOUND, first.entered()).await??;
            let mut next = node.pause_next("getblockchaininfo")?;
            peer_node.set_tip(&"12".repeat(32), &"cd".repeat(32), 100, "01");
            peer.refresh_once().await?;
            peer_node.set_tip(&"ab".repeat(32), &"cd".repeat(32), 100, "01");
            peer.refresh_once().await?;
            let checkpoint = peer.ledger.chain_observation_state().await?;
            first.release();
            wait_for_error(&coordinator, "chain observation epoch changed").await?;
            timeout(ISSUE_BOUND * 2, next.entered()).await??;
            ensure!(
                started.elapsed() >= ISSUE_BOUND,
                "a non-Retry failure received an immediate attempt"
            );
            let after = coordinator.ledger.chain_observation_state().await?;
            ensure!(
                after.chain_epoch == checkpoint.chain_epoch
                    && after.payout_revision == checkpoint.payout_revision
                    && after.best_tip_hash == checkpoint.best_tip_hash
            );
            Ok(())
        }
        .await;
        let _ = stop.send(true);
        scheduler.abort();
        let _ = scheduler.await;
        run
    }
    .await;
    coordinator.ledger.pool.close().await;
    peer.ledger.pool.close().await;
    db.close(result).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fresh_observation_after_completed_peer_aba_cannot_repeat_unchanged() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    // An unobserved node switch before versus after the peer's round trip
    // presents exactly the same fresh RPC proof and cluster token. Neither
    // case has an in-flight or retained witness from before that round trip.
    for switch_before_peer in [true, false] {
        let db = ledger_database::FixtureDatabase::open(&raw, "inter_poll_").await?;
        let node = fake_qbitd::FakeNode::open().await?;
        let peer_node = fake_qbitd::FakeNode::open().await?;
        let old = Coordinator::new(
            fake_qbitd::coordinator_config(db.url.clone(), &node, "inter-poll-old")?,
            Arc::new(Metrics::default()),
        )
        .await?;
        let peer = Coordinator::new(
            fake_qbitd::coordinator_config(db.url.clone(), &peer_node, "inter-poll-peer")?,
            Arc::new(Metrics::default()),
        )
        .await?;
        let result = async {
            old.refresh_once().await?;
            peer.refresh_once().await?;
            let initial = old.ledger.chain_observation_state().await?;
            let replacement = "ef".repeat(32);
            if switch_before_peer { node.set_tip(&replacement, &"cd".repeat(32), 100, "01"); }
            peer_node.set_tip(&"12".repeat(32), &"cd".repeat(32), 100, "01");
            peer.refresh_once().await?;
            peer_node.set_tip(&"ab".repeat(32), &"cd".repeat(32), 100, "01");
            peer.refresh_once().await?;
            let checkpoint = old.ledger.chain_observation_state().await?;
            ensure!(checkpoint.chain_epoch == initial.chain_epoch + 2);
            ensure!(checkpoint.best_tip_hash == initial.best_tip_hash);
            if !switch_before_peer { node.set_tip(&replacement, &"cd".repeat(32), 100, "01"); }
            timeout(ISSUE_BOUND, async {
                old.refresh_once().await?;
                issue_current(&old, &replacement).await
            }).await??;
            let accepted = old.ledger.chain_observation_state().await?;
            ensure!(accepted.chain_epoch == checkpoint.chain_epoch + 1);
            ensure!(accepted.payout_revision == checkpoint.payout_revision + 1);
            for _ in 0..4 {
                ensure!(peer.refresh_once().await.is_err());
                old.refresh_once().await?;
            }
            let stable = old.ledger.chain_observation_state().await?;
            ensure!(stable.chain_epoch == accepted.chain_epoch && stable.payout_revision == accepted.payout_revision && stable.best_tip_hash == accepted.best_tip_hash);
            eprintln!("inter-poll switch_before_peer={switch_before_peer}: fresh epoch {} -> {}, unchanged polls added zero revisions", checkpoint.chain_epoch, accepted.chain_epoch);
            // A genuinely observed convergence followed by a new transition
            // remains live; no historical epoch is silently rebound on retry.
            node.set_tip(&"ab".repeat(32), &"cd".repeat(32), 100, "01");
            old.refresh_once().await?;
            peer.refresh_once().await?;
            node.set_tip(&replacement, &"cd".repeat(32), 100, "01");
            old.refresh_once().await?;
            issue_current(&old, &replacement).await?;
            Ok(())
        }.await;
        old.ledger.pool.close().await;
        peer.ledger.pool.close().await;
        db.close(result).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scheduled_equal_work_retry_issues_within_the_original_bound() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    // The greater-work control encounters the identical accounting race.
    for work in ["02", "01"] {
        let db = ledger_database::FixtureDatabase::open(&raw, "scheduled_retry_").await?;
        let node = fake_qbitd::FakeNode::open().await?;
        let mut config = fake_qbitd::coordinator_config(db.url.clone(), &node, "scheduled-retry")?;
        config.poll_interval = ISSUE_BOUND;
        let coordinator = Coordinator::new(config, Arc::new(Metrics::default())).await?;
        let result = async {
            coordinator.refresh_once().await?;
            let worker = coordinator.authorize("alice.scheduler").await?;
            let original = coordinator.ledger.chain_observation_state().await?;
            let replacement = "ef".repeat(32);
            node.set_tip(&replacement, &"cd".repeat(32), 100, work);
            let mut pause = node.pause_next("getblockchaininfo")?;
            let (stop, stopped) = watch::channel(false);
            let observer = coordinator.clone();
            let visible = Instant::now();
            let deadline = visible + ISSUE_BOUND;
            let scheduler = tokio::spawn(async move { observer.refresh_loop(stopped).await });
            let issued = timeout_at(deadline, async {
                pause.entered().await?;
                sqlx::query("UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1 WHERE singleton")
                    .execute(&coordinator.ledger.pool).await?;
                pause.release();
                // build_job does not wake or run refresh. Only the real
                // scheduler can publish the replacement in this interval.
                loop {
                    if let Ok(job) = coordinator.build_job(&worker, "1a2b3c4d", 1e-12, 0.0).await {
                        if job.wire.previousblockhash == replacement {
                            coordinator.persist_issued_job(&worker, &job, 0x0000_e000, Duration::from_secs(30)).await?;
                            return Ok::<_, anyhow::Error>(job);
                        }
                    }
                    sleep(Duration::from_millis(5)).await;
                }
            }).await.context("scheduled accounting retry missed the original two-second issuance bound")
                .and_then(|result| result);
            let _ = stop.send(true);
            scheduler.abort();
            let _ = scheduler.await;
            let issued = issued?;
            eprintln!("scheduled replacement work={work}: issued and persisted in {:?}", visible.elapsed());
            ensure!(issued.wire.payout_revision == original.payout_revision + 2);
            ensure!(coordinator.ledger.job(&issued.wire.job_id).await?.is_some());
            let after = coordinator.ledger.chain_observation_state().await?;
            ensure!(after.chain_epoch == original.chain_epoch + 1);
            ensure!(after.best_tip_hash.as_deref() == Some(replacement.as_str()));
            Ok(())
        }.await;
        coordinator.ledger.pool.close().await;
        db.close(result).await?;
    }
    Ok(())
}
