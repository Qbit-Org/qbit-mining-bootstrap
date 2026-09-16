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
async fn accounting_revision_refusal_retains_only_an_uncommitted_transition() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = ledger_database::FixtureDatabase::open(&raw, "equal_work_retry_").await?;
    let node = fake_qbitd::FakeNode::open().await?;
    let coordinator = Coordinator::new(
        fake_qbitd::coordinator_config(db.url.clone(), &node, "accounting-race")?,
        Arc::new(Metrics::default()),
    )
    .await?;
    let result = async {
        coordinator.refresh_once().await?;
        let replacement = "ef".repeat(32);
        node.set_tip(&replacement, &"cd".repeat(32), 100, "01");
        let mut pause = node.pause_next("getblockheader")?;
        let observer = coordinator.clone();
        let mut pending = tokio::spawn(async move { observer.refresh_once().await });
        let abort = pending.abort_handle();
        let raced = async {
            timeout(ISSUE_BOUND, pause.entered()).await??;
            // Isolate accounting-only revision drift from the independently
            // covered confirmation path; the accepted chain tip stays fixed.
            let accounting: i64 = sqlx::query_scalar(
                "UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1 WHERE singleton RETURNING payout_revision",
            ).fetch_one(&coordinator.ledger.pool).await?;
            pause.release();
            let error = timeout(ISSUE_BOUND, &mut pending).await??.unwrap_err();
            ensure!(error.to_string().contains("chain observation revision changed"));
            let tip: String = sqlx::query_scalar(
                "SELECT best_tip_hash FROM qbit_prism_cluster WHERE singleton",
            ).fetch_one(&coordinator.ledger.pool).await?;
            ensure!(tip == "ab".repeat(32));
            timeout(ISSUE_BOUND, coordinator.refresh_once()).await??;
            ensure!(coordinator.ledger.payout_revision().await? == accounting + 1);
            let worker = coordinator.authorize("alice.retry").await?;
            let job = coordinator.build_job(&worker, "1a2b3c4d", 1e-12, 0.0).await?;
            ensure!(job.wire.previousblockhash == replacement);
            Ok(())
        }.await;
        abort.abort();
        if !pending.is_finished() {
            let _ = pending.await;
        }
        raced
    }.await;
    coordinator.ledger.pool.close().await;
    db.close(result).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_publication_and_cold_start_cannot_recreate_a_transition() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = ledger_database::FixtureDatabase::open(&raw, "equal_work_cancel_").await?;
    let node_a = fake_qbitd::FakeNode::open().await?;
    let node_b = fake_qbitd::FakeNode::open().await?;
    let a = Coordinator::new(
        fake_qbitd::coordinator_config(db.url.clone(), &node_a, "returning-node")?,
        Arc::new(Metrics::default()),
    )
    .await?;
    let b = Coordinator::new(
        fake_qbitd::coordinator_config(db.url.clone(), &node_b, "cancelled-node")?,
        Arc::new(Metrics::default()),
    )
    .await?;
    // This observer has no coherent local predecessor, even though its node
    // will later prefer B. Construction alone grants no transition witness.
    let cold = Coordinator::new(
        fake_qbitd::coordinator_config(db.url.clone(), &node_b, "cold-node")?,
        Arc::new(Metrics::default()),
    )
    .await?;
    let result = async {
        a.refresh_once().await?;
        b.refresh_once().await?;
        let original = "ab".repeat(32);
        let replacement = "ef".repeat(32);
        node_b.set_tip(&replacement, &"cd".repeat(32), 100, "01");
        // Reconciliation's previous-height probe follows the chain-view COMMIT
        // but precedes prepared publication. No database lock is held here.
        let mut pause = node_b.pause_next("getblockhash")?;
        let observer = b.clone();
        let mut pending = tokio::spawn(async move { observer.refresh_once().await });
        let abort = pending.abort_handle();
        let raced = async {
            timeout(ISSUE_BOUND, pause.entered()).await??;
            let accepted: String =
                sqlx::query_scalar("SELECT best_tip_hash FROM qbit_prism_cluster WHERE singleton")
                    .fetch_one(&a.ledger.pool)
                    .await?;
            ensure!(
                accepted == replacement,
                "pause did not follow chain-view commit"
            );
            pending.abort();
            ensure!((&mut pending).await.unwrap_err().is_cancelled());
            pause.release();
            node_a.set_tip(&replacement, &"cd".repeat(32), 100, "01");
            a.refresh_once().await?;
            node_a.set_tip(&original, &"cd".repeat(32), 100, "01");
            a.refresh_once().await?;
            let returned = a.ledger.payout_revision().await?;
            for observer in [&b, &cold] {
                for _ in 0..2 {
                    let error = observer.refresh_once().await.unwrap_err();
                    ensure!(error.to_string().contains("conflicting equal-work"));
                    ensure!(a.ledger.payout_revision().await? == returned);
                }
            }
            // A cold observer recovers through convergence, with no new format,
            // configuration, designated node or startup override.
            node_b.set_tip(&original, &"cd".repeat(32), 100, "01");
            cold.refresh_once().await?;
            b.refresh_once().await?;
            ensure!(a.ledger.payout_revision().await? == returned);
            Ok(())
        }
        .await;
        abort.abort();
        if !pending.is_finished() {
            let _ = pending.await;
        }
        raced
    }
    .await;
    a.ledger.pool.close().await;
    b.ledger.pool.close().await;
    cold.ledger.pool.close().await;
    db.close(result).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unchanged_opposing_nodes_cannot_repeat_equal_work_replacements() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = ledger_database::FixtureDatabase::open(&raw, "equal_work_ha_").await?;
    let node_a = fake_qbitd::FakeNode::open().await?;
    let node_b = fake_qbitd::FakeNode::open().await?;
    let a = Coordinator::new(
        fake_qbitd::coordinator_config(db.url.clone(), &node_a, "unchanged-node")?,
        Arc::new(Metrics::default()),
    )
    .await?;
    let b = Coordinator::new(
        fake_qbitd::coordinator_config(db.url.clone(), &node_b, "changed-node")?,
        Arc::new(Metrics::default()),
    )
    .await?;
    let result = async {
        a.refresh_once().await?;
        b.refresh_once().await?;
        let replacement = "ef".repeat(32);
        node_b.set_tip(&replacement, &"cd".repeat(32), 100, "01");
        let worker = b.authorize("alice.rig").await?;
        let job = timeout(ISSUE_BOUND, async {
            b.refresh_once().await?;
            let job = b.build_job(&worker, "55667788", 1e-12, 0.0).await?;
            b.persist_issued_job(&worker, &job, 0, Duration::from_secs(30)).await?;
            Ok::<_, anyhow::Error>(job)
        }).await.context("two-node replacement missed issuance bound")??;
        let accepted = b.ledger.payout_revision().await?;
        let mut conflicting_polls_accepted = 0;
        for _ in 0..4 {
            conflicting_polls_accepted += usize::from(a.refresh_once().await.is_ok());
            b.refresh_once().await?;
        }
        let after = b.ledger.payout_revision().await?;
        eprintln!(
            "unchanged opposing nodes: {conflicting_polls_accepted} conflicting polls accepted, {} revision increments",
            after - accepted
        );
        ensure!(after == accepted, "unchanged opposing polls changed payout revision");
        ensure!(conflicting_polls_accepted == 0, "unchanged conflicting node was accepted");
        let tip: String =
            sqlx::query_scalar("SELECT best_tip_hash FROM qbit_prism_cluster WHERE singleton")
                .fetch_one(&b.ledger.pool)
                .await?;
        ensure!(tip == replacement);
        // The job predates all opposing polls. Both ordinary credit and a
        // block-solving submission must retain their original payout authority.
        for block in [false, true] {
            let proof = (0..10_000u32).find_map(|nonce| {
                let proof = job.wire.assemble_submission(
                    &"00".repeat(8), &format!("{:08x}", job.wire.ntime),
                    &format!("{nonce:08x}"), None, 0,
                ).ok()?;
                (proof.share_pass && proof.block_pass == block).then_some(proof)
            }).context("no qualifying proof in bounded fixture search")?;
            let block_hash = proof.block_hash_hex.clone();
            b.submit(&worker, &job, proof, false.into()).await?;
            if block {
                let candidate: serde_json::Value = sqlx::query_scalar(
                    "SELECT candidate FROM qbit_block_candidate_outbox WHERE block_hash=$1",
                ).bind(&block_hash).fetch_one(&b.ledger.pool).await?;
                let candidate: qbit_prism_server::ledger::Candidate = serde_json::from_value(candidate)?;
                ensure!(candidate.payout_revision == accepted);
                ensure!(b.ledger.candidate_revision_valid(&candidate).await?);
            }
        }
        let shares: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger")
            .fetch_one(&b.ledger.pool).await?;
        ensure!(shares == 2, "opposing polls prevented durable share credit");
        Ok(())
    }
    .await;
    a.ledger.pool.close().await;
    b.ledger.pool.close().await;
    db.close(result).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_coherent_poll_cannot_reverse_a_newer_equal_work_tip() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = ledger_database::FixtureDatabase::open(&raw, "equal_work_poll_").await?;
    let node = fake_qbitd::FakeNode::open().await?;
    let newer_node = fake_qbitd::FakeNode::open().await?;
    let a = Coordinator::new(
        fake_qbitd::coordinator_config(db.url.clone(), &node, "older-poll")?,
        Arc::new(Metrics::default()),
    )
    .await?;
    let b = Coordinator::new(
        fake_qbitd::coordinator_config(db.url.clone(), &newer_node, "newer-poll")?,
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
            newer_node.set_tip(&newest, &"cd".repeat(32), 100, "01");
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
            // The losing genuine A->B transition was consumed. Fresh polls of
            // unchanged B must not re-arm it against the winner's A->C choice.
            for _ in 0..3 {
                ensure!(a.refresh_once().await.is_err());
                b.refresh_once().await?;
                ensure!(b.ledger.payout_revision().await? == revision + 1);
            }
            let worker = b.authorize("alice.rig").await?;
            ensure!(
                b.build_job(&worker, "1a2b3c4d", 1e-12, 0.0)
                    .await?
                    .wire
                    .previousblockhash
                    == newest
            );
            // Convergence establishes a new local predecessor. A later real
            // C->D change is eligible, while the unchanged C peer stays fenced.
            node.set_tip(&newest, &"cd".repeat(32), 100, "01");
            a.refresh_once().await?;
            let subsequent = "34".repeat(32);
            node.set_tip(&subsequent, &"cd".repeat(32), 100, "01");
            a.refresh_once().await?;
            ensure!(a.ledger.payout_revision().await? == revision + 2);
            ensure!(b.refresh_once().await.is_err());
            ensure!(a.ledger.payout_revision().await? == revision + 2);
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
                .observe_chain_transition(&original, &replacement, height, work, before)
                .await
                .is_err());
            ensure!(ledger.payout_revision().await? == before);
        }
        let current = ledger
            .observe_chain_transition(&original, &replacement, 100, "0100", before)
            .await?;
        ensure!(current == before + 1);
        // A peer can finish observing the same replacement after another peer
        // accepts it. This is a no-op, not a conflicting stale replacement.
        ensure!(
            ledger
                .observe_chain_transition(&original, &replacement, 100, "100", before)
                .await?
                == current
        );
        ensure!(
            ledger
                .observe_chain_transition(&original, &replacement, 100, "100", current)
                .await?
                == current
        );
        ensure!(ledger
            .observe_chain_transition(&replacement, &original, 100, "100", before)
            .await
            .is_err());
        ensure!(ledger
            .observe_chain_transition(&original, &"12".repeat(32), 100, "100", before)
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
            .observe_chain_transition(&replacement, &original, 100, "100", current)
            .await?;
        ensure!(returned == current + 1);
        // Returning to the initial hash does not make the original revision
        // current again: a third sibling from that old poll still loses.
        ensure!(ledger
            .observe_chain_transition(&original, &"12".repeat(32), 100, "100", before)
            .await
            .is_err());
        // Existing greater-work semantics permit a shorter, heavier chain.
        ensure!(
            ledger
                .observe_chain_transition(&original, &"34".repeat(32), 99, "101", before)
                .await?
                == returned + 1
        );
        ensure!(ledger
            .observe_chain_transition(&"34".repeat(32), &replacement, 100, "100", returned + 1)
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
