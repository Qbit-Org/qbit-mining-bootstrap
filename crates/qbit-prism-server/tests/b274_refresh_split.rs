//! #274 preparation on frozen PR397: actual runtime controls and expected-red
//! contracts, deliberately excluded from required CI execution until integrated.
//! Select an ignored test explicitly with required integration inputs; see
//! docs/b274-refresh-split-preparation.md for expected outcomes and promotion.
use anyhow::{ensure, Context, Result};
use qbit_prism_server::{coordinator::Coordinator, metrics::Metrics};
use std::{sync::Arc, time::Duration};
use tokio::time::{sleep_until, timeout, Instant};

#[path = "support/b274_refresh.rs"]
mod b274;
#[path = "support/compact_runtime_e2e/mod.rs"]
#[allow(dead_code)]
mod runtime;
use b274::run;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual frozen-397 baseline; not the #274 acceptance contract"]
async fn baseline_397_transaction_churn_reads_the_full_window_each_time() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            for nonce in 1..=3 {
                let before = b274::published(f).await?;
                b274::churn(f, &before, nonce);
                let mark = f.proxy.mark();
                f.a.refresh_once().await?;
                let measured = b274::reads(f, mark, &format!("baseline-churn-{nonce}"))?;
                let after = b274::published(f).await?;
                b274::changed_template(&before, &after)?;
                ensure!(
                    measured == (runtime::SHARES, 1),
                    "frozen baseline changed: {measured:?}"
                );
                ensure!(
                    before.snapshot.share_seq == after.snapshot.share_seq
                        && before.snapshot.payout_revision == after.snapshot.payout_revision,
                    "churn accidentally changed shares or revision"
                );
                ensure!(
                    after.window.anchor_ms > before.window.anchor_ms,
                    "baseline did not reanchor"
                );
                b274::native_artifacts(f).await?;
            }
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "EXPECTED RED on frozen PR397: #274 window reuse not implemented"]
async fn pending_transaction_churn_reuses_original_window_and_native_artifacts() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let original = b274::published(f).await?;
            for nonce in 1..=3 {
                let before = b274::published(f).await?;
                b274::churn(f, &before, nonce);
                let mark = f.proxy.mark();
                f.a.refresh_once().await?;
                let measured = b274::reads(f, mark, &format!("pending-churn-{nonce}"))?;
                let after = b274::published(f).await?;
                b274::changed_template(&before, &after)?;
                ensure!(
                    original.created.elapsed() < f.a.config.snapshot_interval,
                    "fixture crossed reanchor interval before testing reuse"
                );
                b274::native_artifacts(f).await?;
                ensure!(
                    measured == (0, 0),
                    "#274 pending: template-only refresh reread the window: {measured:?}"
                );
                ensure!(
                    after.window == original.window
                        && after.snapshot.share_seq == original.snapshot.share_seq
                        && after.snapshot.payout_revision == original.snapshot.payout_revision,
                    "template-only refresh replaced the original window identity"
                );
            }
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "EXPECTED RED on frozen PR397: cached work does not check for new shares"]
async fn pending_new_share_from_other_frontend_invalidates_unchanged_template() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let before = b274::published(f).await?;
            f.node.set_template(Some(before.template.clone()));
            b274::append_from_other_frontend(f).await?;
            let mark = f.proxy.mark();
            f.a.refresh_once().await?;
            let measured = b274::reads(f, mark, "pending-new-share")?;
            let after = b274::published(f).await?;
            ensure!(
                before.created.elapsed() < f.a.config.snapshot_interval,
                "reanchor hid the new-share trigger"
            );
            ensure!(
                before.fingerprint == after.fingerprint,
                "template changed during new-share test"
            );
            ensure!(
                before.snapshot.payout_revision == after.snapshot.payout_revision,
                "revision hid the new-share trigger"
            );
            ensure!(
                after.snapshot.share_seq == runtime::SHARES + 1,
                "#274 pending: new committed share was not incorporated; watermark={}",
                after.snapshot.share_seq
            );
            ensure!(
                measured == (runtime::SHARES, 1),
                "new-share refresh did not take one full snapshot: {measured:?}"
            );
            ensure!(
                after.window != before.window,
                "new share reused the old window"
            );
            b274::native_artifacts(f).await
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual preparation control; promote with #274 integration manifest"]
async fn control_unchanged_template_reuses_publication_without_share_rows() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let before = b274::published(f).await?;
            let mark = f.proxy.mark();
            f.a.refresh_once().await?;
            ensure!(
                b274::reads(f, mark, "unchanged")? == (0, 0),
                "unchanged refresh read window"
            );
            ensure!(
                Arc::ptr_eq(&before, &b274::published(f).await?),
                "unchanged publication was replaced"
            );
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual preparation control; promote with #274 integration manifest"]
async fn control_revision_invalidates_unchanged_template() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| Box::pin(async move {
        f.refresh(true).await?;
        let before = b274::published(f).await?;
        f.node.set_template(Some(before.template.clone()));
        let revision: i64 = sqlx::query_scalar("UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1 WHERE singleton RETURNING payout_revision")
            .fetch_one(f.pool()).await?;
        let mark = f.proxy.mark();
        f.a.refresh_once().await?;
        ensure!(b274::reads(f, mark, "revision")? == (runtime::SHARES, 1), "revision did not rebuild window");
        let after = b274::published(f).await?;
        ensure!(before.fingerprint == after.fingerprint && before.snapshot.share_seq == after.snapshot.share_seq,
            "revision test changed template or shares");
        ensure!(after.snapshot.payout_revision == revision && after.window.anchor_ms > before.window.anchor_ms,
            "revision reused the original window");
        b274::native_artifacts(f).await
    })).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual preparation control; real elapsed reanchor clock, no scale claim"]
async fn control_reanchor_interval_rebuilds_without_new_shares() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let mut config = (*f.a.config).clone();
            config.instance_id = "b274-reanchor".into();
            config.snapshot_interval = Duration::from_secs(2);
            let c = Coordinator::new(config, Arc::new(Metrics::default())).await?;
            let result = async {
                c.refresh_once().await?;
                let before = c
                    .prepared
                    .read()
                    .await
                    .clone()
                    .context("initial publication missing")?;
                f.node.set_template(Some(before.template.clone()));
                sleep_until(Instant::from_std(
                    before.created + c.config.snapshot_interval + Duration::from_millis(50),
                ))
                .await;
                let mark = f.proxy.mark();
                c.refresh_once().await?;
                ensure!(
                    b274::reads(f, mark, "reanchor")? == (runtime::SHARES, 1),
                    "reanchor did not rebuild window"
                );
                let after = c
                    .prepared
                    .read()
                    .await
                    .clone()
                    .context("reanchor publication missing")?;
                ensure!(
                    after.window.anchor_ms > before.window.anchor_ms
                        && after.storage_key != before.storage_key,
                    "expired window was reused"
                );
                ensure!(
                    after.fingerprint == before.fingerprint
                        && after.snapshot.share_seq == before.snapshot.share_seq
                        && after.snapshot.payout_revision == before.snapshot.payout_revision,
                    "reanchor test changed another trigger"
                );
                Ok(())
            }
            .await;
            c.ledger.pool.close().await;
            result
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "EXPECTED RED on frozen PR397: transaction refresh resets the reanchor clock"]
async fn pending_template_churn_does_not_extend_original_reanchor_deadline() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let mut config = (*f.a.config).clone();
            config.instance_id = "b274-churn-reanchor".into();
            config.snapshot_interval = Duration::from_secs(4);
            let c = Coordinator::new(config, Arc::new(Metrics::default())).await?;
            let result = async {
                c.refresh_once().await?;
                let original = c.prepared.read().await.clone().context("initial publication missing")?;
                let mut observed = Vec::new();
                let mut windows = Vec::new();
                for second in [1, 2] {
                    sleep_until(Instant::from_std(original.created + Duration::from_secs(second))).await;
                    let before = c.prepared.read().await.clone().context("publication missing")?;
                    b274::churn(f, &before, second as u32);
                    let mark = f.proxy.mark();
                    c.refresh_once().await?;
                    observed.push(b274::reads(f, mark, &format!("churn-before-reanchor-{second}"))?);
                    let after = c.prepared.read().await.clone().context("churn publication missing")?;
                    b274::changed_template(&before, &after)?;
                    ensure!(original.created.elapsed() < c.config.snapshot_interval,
                        "fixture crossed reanchor interval during transaction churn");
                    windows.push(after.window.clone());
                }
                sleep_until(Instant::from_std(original.created + c.config.snapshot_interval + Duration::from_millis(50))).await;
                let latest = c.prepared.read().await.clone().context("latest template missing")?;
                // A new template's publication clock is still young. Only the
                // original window clock should expire at this point.
                ensure!(latest.created.elapsed() < c.config.snapshot_interval,
                    "fixture also expired the template clock; cannot distinguish window lifetime");
                let mark = f.proxy.mark();
                c.refresh_once().await?;
                observed.push(b274::reads(f, mark, "original-reanchor-deadline")?);
                let after = c.prepared.read().await.clone().context("reanchor publication missing")?;
                ensure!(after.fingerprint == latest.fingerprint
                    && after.snapshot.share_seq == original.snapshot.share_seq
                    && after.snapshot.payout_revision == original.snapshot.payout_revision,
                    "reanchor stimulus changed another trigger");
                ensure!(observed == [(0, 0), (0, 0), (runtime::SHARES, 1)],
                    "#274 pending: expected two window reuses then one original-deadline snapshot: {observed:?}");
                ensure!(windows.iter().all(|window| window == &original.window)
                    && after.window.anchor_ms > original.window.anchor_ms,
                    "window identity did not survive until its original reanchor deadline");
                Ok(())
            }.await;
            c.ledger.pool.close().await;
            result
        })
    }).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual preparation control; preserve the post-reservation tip fence"]
async fn control_tip_change_after_reservation_commit_prevents_publication() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let before = b274::published(f).await?;
            b274::churn(f, &before, 1);
            let pause = f.proxy.pause_after_commit("qbit_prism_jobs", "INSERT")?;
            let mark = f.proxy.mark();
            let change_tip = async {
                let result = async {
                    let commit = timeout(Duration::from_secs(5), pause.entered())
                        .await
                        .context("reservation COMMIT did not reach barrier")?;
                    ensure!(
                        f.proxy
                            .executions_since(mark)?
                            .iter()
                            .any(|execution| execution.seq == commit
                                && execution.is_commit()
                                && !execution.delivered()),
                        "barrier was not an actual completed, undelivered COMMIT"
                    );
                    f.node
                        .set_tip(&"ef".repeat(32), &"ab".repeat(32), 101, "02");
                    Ok::<_, anyhow::Error>(())
                }
                .await;
                pause.release();
                result
            };
            let (refresh, changed) = tokio::join!(f.a.refresh_once(), change_tip);
            changed?;
            let error = refresh
                .err()
                .context("old-tip work published after reservation wait")?;
            ensure!(
                format!("{error:#}").contains("tip changed"),
                "unrelated refresh failure: {error:#}"
            );
            ensure!(
                Arc::ptr_eq(&before, &b274::published(f).await?),
                "stale refresh replaced coupled publication identity"
            );
            b274::reads(f, mark, "tip-rejection")?;
            Ok(())
        })
    })
    .await
}
