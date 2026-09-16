//! Equal-work fork choice must reach issued work without weakening the ledger's
//! stale-observation and payout fences. Each test owns a disposable database.
use anyhow::{ensure, Context, Result};
use qbit_prism_server::{
    coordinator::Coordinator, ledger::Ledger, metrics::Metrics, stratum::MiningBackend,
};
use qbit_prism_test_gate as gate;
use std::{sync::Arc, time::Duration};
use tokio::time::{timeout, Instant};

#[path = "support/fake_qbitd.rs"]
#[allow(dead_code)]
mod fake_qbitd;
#[path = "support/ledger_database.rs"]
#[allow(dead_code)]
mod ledger_database;

const ISSUE_BOUND: Duration = Duration::from_secs(2);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_coherent_poll_cannot_reverse_a_newer_equal_work_tip() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = ledger_database::FixtureDatabase::open(&raw, "equal_work_poll_").await?;
    let node = fake_qbitd::FakeNode::open().await?;
    let a = Coordinator::new(
        fake_qbitd::coordinator_config(db.url.clone(), &node, "older-poll")?,
        Arc::new(Metrics::default()),
    )
    .await?;
    let b = Coordinator::new(
        fake_qbitd::coordinator_config(db.url.clone(), &node, "newer-poll")?,
        Arc::new(Metrics::default()),
    )
    .await?;
    let result = async {
        a.refresh_once().await?;
        b.refresh_once().await?;
        let revision = a.ledger.payout_revision().await?;
        node.set_tip(&"ef".repeat(32), &"cd".repeat(32), 100, "01");
        // Capture a coherent info/template/best-hash proof, then delay only
        // its predecessor lookup immediately before ledger publication.
        let mut pause = node.pause_next("getblockheader")?;
        let older = a.clone();
        let mut pending = tokio::spawn(async move { older.refresh_once().await });
        let abort = pending.abort_handle();
        let raced = async {
            timeout(ISSUE_BOUND, pause.entered()).await??;
            let newest = "12".repeat(32);
            node.set_tip(&newest, &"cd".repeat(32), 100, "01");
            b.refresh_once().await?;
            pause.release();
            let old = timeout(ISSUE_BOUND, &mut pending).await??;
            ensure!(old
                .unwrap_err()
                .to_string()
                .contains("chain observation revision changed"));
            let tip: String =
                sqlx::query_scalar("SELECT best_tip_hash FROM qbit_prism_cluster WHERE singleton")
                    .fetch_one(&b.ledger.pool)
                    .await?;
            ensure!(
                tip == newest,
                "late observation reverted accepted fork choice"
            );
            ensure!(b.ledger.payout_revision().await? == revision + 1);
            let worker = b.authorize("alice.rig").await?;
            ensure!(
                b.build_job(&worker, "1a2b3c4d", 1e-12, 0.0)
                    .await?
                    .wire
                    .previousblockhash
                    == newest
            );
            Ok(())
        }
        .await;
        // On timeout/error, stop the old task before tearing down its database.
        abort.abort();
        if !pending.is_finished() {
            let _ = pending.await;
        }
        raced
    }
    .await;
    a.ledger.pool.close().await;
    b.ledger.pool.close().await;
    db.close(result).await
}

#[tokio::test]
async fn equal_work_replacement_preserves_strict_and_revision_fences() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = ledger_database::FixtureDatabase::open(&raw, "equal_work_fence_").await?;
    let ledger = Ledger::connect(&db.url, "equal-work-fence".into(), 4, true).await?;
    let result = async {
        let original = "ab".repeat(32);
        let replacement = "ef".repeat(32);
        let before = ledger.observe_chain_view(&original, 100, "100").await?;
        // Unsequenced candidate and settlement observers remain conservative.
        ensure!(ledger
            .observe_chain_view(&replacement, 100, "100")
            .await
            .is_err());
        for (height, work) in [(99, "100"), (101, "100"), (100, "ff")] {
            ensure!(ledger
                .observe_chain_view_at_revision(&replacement, height, work, before)
                .await
                .is_err());
            ensure!(ledger.payout_revision().await? == before);
        }
        let current = ledger
            .observe_chain_view_at_revision(&replacement, 100, "0100", before)
            .await?;
        ensure!(current == before + 1);
        ensure!(
            ledger
                .observe_chain_view_at_revision(&replacement, 100, "100", current)
                .await?
                == current
        );
        ensure!(ledger
            .observe_chain_view_at_revision(&original, 100, "100", before)
            .await
            .is_err());
        ensure!(ledger
            .observe_chain_view_at_revision(&"12".repeat(32), 100, "100", before)
            .await
            .is_err());
        ensure!(ledger
            .reconcile_blocks_at_revision(&[], 100, before)
            .await
            .is_err());
        ensure!(ledger
            .save_job("stale", &serde_json::json!({}), before, &original, 60)
            .await
            .is_err());
        ensure!(ledger.job("stale").await?.is_none());
        // A fresh proof can return to a previously seen tip. The revision
        // orders observations; it does not blacklist hashes.
        let returned = ledger
            .observe_chain_view_at_revision(&original, 100, "100", current)
            .await?;
        ensure!(returned == current + 1);
        // Existing greater-work semantics permit a shorter, heavier chain.
        ensure!(
            ledger
                .observe_chain_view(&"34".repeat(32), 99, "101")
                .await?
                == returned + 1
        );
        ensure!(ledger
            .observe_chain_view_at_revision(&replacement, 100, "100", returned + 1)
            .await
            .is_err());
        Ok(())
    }
    .await;
    ledger.pool.close().await;
    db.close(result).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn equal_work_same_height_replacement_issues_within_two_seconds() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = ledger_database::FixtureDatabase::open(&raw, "equal_work_").await?;
    let node = fake_qbitd::FakeNode::open().await?;
    let coordinator = Coordinator::new(
        fake_qbitd::coordinator_config(db.url.clone(), &node, "equal-work")?,
        Arc::new(Metrics::default()),
    )
    .await?;
    let result = async {
        coordinator.refresh_once().await?;
        let worker = coordinator.authorize("alice.rig").await?;
        let original = coordinator.build_job(&worker, "1a2b3c4d", 1e-12, 0.0).await?;
        let original_revision = original.wire.payout_revision;
        // Same parent, height and cumulative work; only the authoritative
        // node's selected sibling changes. No extra block rescues the refresh.
        let replacement = "ef".repeat(32);
        node.set_tip(&replacement, &"cd".repeat(32), 100, "01");
        let visible = Instant::now();
        let issued = timeout(ISSUE_BOUND, async {
            coordinator.refresh_once().await?;
            let job = coordinator.build_job(&worker, "55667788", 1e-12, 0.0).await?;
            coordinator.persist_issued_job(&worker, &job, 0x0000_e000, Duration::from_secs(30)).await?;
            Ok::<_, anyhow::Error>(job)
        }).await.context("equal-work replacement missed issuance bound")??;
        eprintln!("equal-work tip visible to persisted issued work: {:?}", visible.elapsed());
        ensure!(issued.wire.previousblockhash == replacement);
        ensure!(issued.wire.payout_revision == original_revision + 1);
        ensure!(coordinator.ledger.job(&issued.wire.job_id).await?.is_some());
        let (tip, height, work): (String, i64, String) = sqlx::query_as(
            "SELECT best_tip_hash,best_tip_height,best_chainwork::text FROM qbit_prism_cluster WHERE singleton",
        ).fetch_one(&coordinator.ledger.pool).await?;
        ensure!(tip == replacement && height == 100 && work == "1");
        ensure!(coordinator.ledger.reconcile_blocks_at_revision(&[], 100, original_revision).await.is_err(),
            "replacement retained stale payout authority");
        Ok(())
    }.await;
    coordinator.ledger.pool.close().await;
    db.close(result).await
}
