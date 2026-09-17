//! Observe actual PostgreSQL share rows while the public refresh path runs.
use anyhow::{ensure, Context, Result};
use qbit_prism_server::{
    coordinator::Coordinator, ledger::BalanceSource, metrics::Metrics, stratum::MiningBackend,
};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::time::{sleep, timeout};

#[allow(dead_code)]
#[path = "support/compact_runtime_e2e/mod.rs"]
mod support;
use support::{run, Fixture};

async fn prepared(c: &Coordinator) -> Result<Arc<qbit_prism_server::coordinator::Prepared>> {
    c.prepared
        .read()
        .await
        .clone()
        .context("refresh did not publish")
}

fn churn(mut template: Value, sequence: u32) -> Value {
    // Structurally valid transaction with distinct locktime, supplied by the
    // fake node. This changes real transaction and witness merkle inputs.
    let data = format!(
        "0200000001{}0000000000ffffffff0101000000000000000151{sequence:08x}",
        "11".repeat(32)
    );
    template["transactions"] = json!([{"data": data}]);
    template["curtime"] = json!(chrono::Utc::now().timestamp());
    template
}

async fn one_read(f: &Fixture, mark: u64, rows: u64) -> Result<()> {
    ensure!(
        f.returned_share_rows(mark)? == rows,
        "expected {rows} returned share rows, got {}",
        f.returned_share_rows(mark)?
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unchanged_empty_and_nonempty_refresh_probe_execution_counts() -> Result<()> {
    for has_shares in [false, true] {
        run(qbit_prism_test_gate::site!(), |f| Box::pin(async move {
            sqlx::query("INSERT INTO qbit_payout_carry_forward_current(miner_id,payout_order_key,p2mr_program,balance_sats,active_row_count) VALUES('prior','prior',decode(repeat('22',32),'hex'),12345,1)")
                .execute(f.pool()).await?;
            f.refresh(has_shares).await?;
            let first = prepared(&f.a).await?;
            let mark = f.proxy.mark();
            let started = std::time::Instant::now();
            f.a.refresh_once().await?;
            let wall = started.elapsed();
            ensure!(Arc::ptr_eq(&first, &prepared(&f.a).await?), "idle refresh replaced immutable work");
            one_read(f, mark, 0).await?;
            let executions = f.proxy.executions_since(mark)?;
            let mut transactions = std::collections::HashMap::<u64, Vec<&support::execution::Execution>>::new();
            let mut probes = Vec::new();
            let mut cutoff_queries = 0;
            for execution in &executions {
                ensure!(execution.complete_response(), "incomplete SQL observation: {execution:?}");
                if execution.sql == "SELECT COALESCE(max(share_seq),0) FROM qbit_share_ledger WHERE accepted" {
                    cutoff_queries += 1;
                }
                if execution.sql == "BEGIN" {
                    transactions.insert(execution.connection, Vec::new());
                }
                if let Some(transaction) = transactions.get_mut(&execution.connection) {
                    transaction.push(execution);
                }
                if execution.is_commit() {
                    if let Some(transaction) = transactions.remove(&execution.connection) {
                        if transaction.iter().any(|e| e.sql == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                            && transaction.iter().any(|e| e.sql.contains("qbit_current_carry_forward_balances()")) {
                            probes.push(transaction);
                        }
                    }
                }
            }
            let probe_sql: usize = probes.iter().map(Vec::len).sum();
            let balance_reads: Vec<_> = probes.iter().flatten().filter(|e| e.sql.contains("qbit_current_carry_forward_balances()")).collect();
            ensure!(probes.len() == 2 && probe_sql == 10 && cutoff_queries == 0,
                "idle probe cost changed: {} probe transactions / {probe_sql} executions / {cutoff_queries} standalone cutoffs", probes.len());
            ensure!(balance_reads.len() == 2, "idle balance-read count differs");
            for read in balance_reads { ensure!(read.returned_rows()? == 1, "balance DataRows differ"); }
            eprintln!("idle has_shares={has_shares}: payout_component_sql={} balance_reads=2 balance_rows=2 refresh_wall_us={}", probe_sql + cutoff_queries, wall.as_micros());
            if has_shares {
                // Sensitivity control: counting metadata as zero must not hide
                // an actual payload read on the same observed connection.
                let mark = f.proxy.mark();
                let rows = sqlx::query("SELECT share_seq,payout_order_key,share_difficulty FROM qbit_share_ledger WHERE accepted LIMIT 1")
                    .fetch_all(f.pool()).await?;
                ensure!(rows.len() == 1, "payload control has no accepted share");
                one_read(f, mark, 1).await?;
            }
            Ok(())
        })).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transaction_churn_reuses_window_and_preserves_as_issued_work() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let first = prepared(&f.a).await?;
            let worker = f.a.authorize("alice.rig").await?;
            let issued = f.issue(&worker, Duration::from_secs(30)).await?;
            let original_payload = f.payload(&issued.wire.job_id).await?;
            let mark = f.proxy.mark();
            let mut previous = first.clone();
            for n in 1..=3 {
                sleep(Duration::from_secs(2)).await;
                f.node.set_template(Some(churn(first.template.clone(), n)));
                f.a.refresh_once().await?;
                let current = prepared(&f.a).await?;
                ensure!(
                    current.window == first.window,
                    "transaction churn reanchored the window"
                );
                ensure!(
                    current.storage_key != previous.storage_key
                        && current.fingerprint != previous.fingerprint,
                    "template was not rebuilt"
                );
                ensure!(
                    current.base_wire.as_ref().unwrap().merkle_branch
                        != previous.base_wire.as_ref().unwrap().merkle_branch,
                    "transaction merkle branch was not rebuilt"
                );
                previous = current;
            }
            ensure!(
                f.returned_share_rows(mark)? == 0,
                "template churn reread accepted shares"
            );
            ensure!(
                f.a.build_slots.available_permits() == f.a.config.build_workers,
                "idle cache retained build admission"
            );
            ensure!(
                f.payload(&issued.wire.job_id).await? == original_payload,
                "refresh rewrote issued economics or expiry"
            );
            let resumed =
                f.b.resume_job(&worker, &issued.wire.job_id)
                    .await?
                    .context("original work did not resume")?;
            support::assertions::same_job(&issued, &resumed)?;
            let latest = f.issue(&worker, Duration::from_secs(30)).await?;
            support::assertions::compact_storage(f, &latest).await
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_share_and_payout_revision_each_invalidate_once() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let first = prepared(&f.a).await?;
            let mut share =
                f.a.ledger
                    .read_window(&first.window, BalanceSource::AsIssued)
                    .await?
                    .shares
                    .pop()
                    .context("fixture share missing")?;
            let mut changes = f.a.refresh.subscribe();
            let mut latest_share_seq = first.snapshot.share_seq;
            // With a share on every poll and an unchanged template, baseline
            // work stays issued until a real rebuild trigger. Dirty inputs
            // must not cause full-window reads or generation fanout per poll.
            for n in 1..=3 {
                share.share_id = format!("refresh-new-share-{n}");
                latest_share_seq =
                    f.a.ledger
                        .append(share.clone(), None)
                        .await?
                        .share
                        .share_seq;
                let mark = f.proxy.mark();
                f.a.refresh_once().await?;
                let current = prepared(&f.a).await?;
                ensure!(
                    f.returned_share_rows(mark)? == 0,
                    "shares alone reread the window"
                );
                ensure!(
                    Arc::ptr_eq(&first, &current),
                    "shares alone replaced published work"
                );
                ensure!(
                    current.generation == first.generation && !changes.has_changed()?,
                    "shares alone notified miners of a new generation"
                );
            }
            f.node.set_template(Some(churn(first.template.clone(), 1)));
            let mark = f.proxy.mark();
            f.a.refresh_once().await?;
            one_read(f, mark, support::SHARES + 3).await?;
            let next = prepared(&f.a).await?;
            ensure!(
                next.snapshot.share_seq == latest_share_seq && next.window != first.window,
                "next template did not capture all newly eligible shares"
            );
            ensure!(
                next.generation > first.generation && changes.has_changed()?,
                "changed template did not notify miners"
            );
            changes.borrow_and_update();
            let mark = f.proxy.mark();
            f.a.refresh_once().await?;
            ensure!(
                f.returned_share_rows(mark)? == 0,
                "unchanged share cutoff reread the window"
            );
            sqlx::query(
                "UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1 WHERE singleton",
            )
            .execute(f.pool())
            .await?;
            let mark = f.proxy.mark();
            f.a.refresh_once().await?;
            one_read(f, mark, support::SHARES + 3).await?;
            ensure!(
                prepared(&f.a).await?.snapshot.payout_revision == next.snapshot.payout_revision + 1,
                "revision was not refreshed"
            );
            let mark = f.proxy.mark();
            f.a.refresh_once().await?;
            ensure!(
                f.returned_share_rows(mark)? == 0,
                "unchanged revision reread the window"
            );
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transaction_churn_does_not_extend_original_reanchor_interval() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let mut config = (*f.a.config).clone();
            config.instance_id = "refresh-reanchor".into();
            config.snapshot_interval = Duration::from_millis(800);
            let c = Coordinator::new(config, Arc::new(Metrics::default())).await?;
            let result = async {
                c.refresh_once().await?;
                let first = prepared(&c).await?;
                let mark = f.proxy.mark();
                f.node.set_template(Some(churn(first.template.clone(), 1)));
                c.refresh_once().await?;
                ensure!(
                    f.returned_share_rows(mark)? == 0,
                    "early template churn reread the window"
                );
                let mut share = c
                    .ledger
                    .read_window(&first.window, BalanceSource::AsIssued)
                    .await?
                    .shares
                    .pop()
                    .context("fixture share missing")?;
                share.share_id = "refresh-before-reanchor".into();
                let latest_share_seq = c.ledger.append(share, None).await?.share.share_seq;
                let before = prepared(&c).await?;
                let mark = f.proxy.mark();
                c.refresh_once().await?;
                ensure!(
                    f.returned_share_rows(mark)? == 0 && Arc::ptr_eq(&before, &prepared(&c).await?),
                    "share alone rebuilt before the original reanchor"
                );
                sleep(Duration::from_millis(850)).await;
                let mark = f.proxy.mark();
                c.refresh_once().await?;
                one_read(f, mark, support::SHARES + 1).await?;
                ensure!(
                    prepared(&c).await?.window.anchor_ms > first.window.anchor_ms
                        && prepared(&c).await?.snapshot.share_seq == latest_share_seq,
                    "original reanchor did not capture the latest shares"
                );
                let mark = f.proxy.mark();
                f.node.set_template(Some(churn(first.template.clone(), 3)));
                c.refresh_once().await?;
                ensure!(
                    f.returned_share_rows(mark)? == 0,
                    "reanchor did not renew the window cache"
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
async fn delayed_snapshot_read_does_not_restart_reanchor_age() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let mut config = (*f.a.config).clone();
            config.instance_id = "refresh-delayed-anchor".into();
            config.snapshot_interval = Duration::from_millis(200);
            let c = Coordinator::new(config, Arc::new(Metrics::default())).await?;
            let result = async {
                sqlx::raw_sql("CREATE FUNCTION refresh_observe_anchor() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'prism-execution-marker qbit_prism_cluster UPDATE'; RETURN NULL; END $$; CREATE TRIGGER refresh_observe_anchor AFTER UPDATE OF ledger_clock_ms ON qbit_prism_cluster FOR EACH STATEMENT EXECUTE FUNCTION refresh_observe_anchor();")
                    .execute(f.pool()).await?;
                let pause = f.proxy.pause_after_commit("qbit_prism_cluster", "UPDATE")?;
                let coordinator = c.clone();
                let pending = tokio::spawn(async move { coordinator.refresh_once().await });
                timeout(Duration::from_secs(5), pause.entered()).await
                    .context("snapshot did not reach COMMIT barrier")?;
                sleep(Duration::from_millis(250)).await;
                pause.release();
                timeout(Duration::from_secs(5), pending).await???;
                let first = prepared(&c).await?;
                let mark = f.proxy.mark();
                c.refresh_once().await?;
                one_read(f, mark, support::SHARES).await?;
                ensure!(prepared(&c).await?.window.anchor_ms > first.window.anchor_ms,
                    "snapshot response wait renewed the original reanchor interval");
                Ok(())
            }.await;
            c.ledger.pool.close().await;
            result
        })
    }).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shares_after_capture_enter_the_next_window_without_rewriting_the_selected_one(
) -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let first = prepared(&f.a).await?;
            let mut share =
                f.a.ledger
                    .read_window(&first.window, BalanceSource::AsIssued)
                    .await?
                    .shares
                    .pop()
                    .context("fixture share missing")?;
            share.share_id = "accepted-during-reservation".into();
            f.node.set_template(Some(churn(first.template.clone(), 1)));
            let pause = f.proxy.pause_after_commit("qbit_prism_jobs", "INSERT")?;
            let c = f.a.clone();
            let pending = tokio::spawn(async move { c.refresh_once().await });
            timeout(Duration::from_secs(5), pause.entered())
                .await
                .context("refresh did not reach reservation barrier")?;
            let appended = f.a.ledger.append(share, None).await?;
            pause.release();
            timeout(Duration::from_secs(5), pending).await???;
            let selected = prepared(&f.a).await?;
            ensure!(
                selected.window == first.window
                    && selected.snapshot.share_seq == first.snapshot.share_seq,
                "publication rewrote the already-selected anchored window"
            );
            f.node.set_template(Some(churn(first.template.clone(), 2)));
            let mark = f.proxy.mark();
            f.a.refresh_once().await?;
            one_read(f, mark, support::SHARES + 1).await?;
            ensure!(
                prepared(&f.a).await?.snapshot.share_seq == appended.share.share_seq,
                "the next build did not capture the share committed during reservation"
            );
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn difficulty_change_invalidates_the_retained_window_weight() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let first = prepared(&f.a).await?;
            let mut template = churn(first.template.clone(), 1);
            template["bits"] = json!("200fffff");
            f.node.set_template(Some(template));
            let mark = f.proxy.mark();
            f.a.refresh_once().await?;
            one_read(f, mark, support::SHARES).await?;
            let current = prepared(&f.a).await?;
            ensure!(
                current
                    .bundle
                    .as_ref()
                    .unwrap()
                    .found_block
                    .network_difficulty
                    != first
                        .bundle
                        .as_ref()
                        .unwrap()
                        .found_block
                        .network_difficulty,
                "difficulty fixture did not change weight"
            );
            ensure!(
                current.window.anchor_ms > first.window.anchor_ms,
                "difficulty change reused an old anchor"
            );
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn older_refresh_cannot_publish_after_newer_tip_observation() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let first = prepared(&f.a).await?;
            f.node.set_template(Some(churn(first.template.clone(), 1)));
            // Pause an actual completed prepared COMMIT, after the cached build.
            let pause = f.proxy.pause_after_commit("qbit_prism_jobs", "INSERT")?;
            let c = f.a.clone();
            let pending = tokio::spawn(async move { c.refresh_once().await });
            timeout(Duration::from_secs(5), pause.entered())
                .await
                .context("refresh did not reach COMMIT barrier")?;
            ensure!(
                f.a.build_slots.available_permits() == f.a.config.build_workers,
                "reservation wait held admission needed by existing work"
            );
            f.node.set_template(None);
            f.node
                .set_tip(&"ef".repeat(32), &"ab".repeat(32), 101, "02");
            f.b.refresh_once().await?;
            pause.release();
            let stale = timeout(Duration::from_secs(5), pending).await??;
            ensure!(
                stale.is_err(),
                "older refresh published after tip supersession"
            );
            ensure!(
                prepared(&f.a).await?.storage_key == first.storage_key,
                "stale completion replaced prepared work"
            );
            f.a.refresh_once().await?;
            ensure!(
                prepared(&f.a).await?.template["previousblockhash"] == "ef".repeat(32),
                "fresh tip could not publish after stale refusal"
            );
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_reservation_retains_bounded_cache_without_holding_build_admission() -> Result<()>
{
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let first = prepared(&f.a).await?;
            sqlx::query(
                "UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1 WHERE singleton",
            )
            .execute(f.pool())
            .await?;
            let pause = f.proxy.pause_after_commit("qbit_prism_jobs", "INSERT")?;
            let c = f.a.clone();
            let pending = tokio::spawn(async move { c.refresh_once().await });
            timeout(Duration::from_secs(5), pause.entered())
                .await
                .context("refresh did not reach reservation wait")?;
            ensure!(
                f.a.build_slots.available_permits() == f.a.config.build_workers,
                "completed build kept admission while reservation waited"
            );
            pending.abort();
            ensure!(
                pending.await.unwrap_err().is_cancelled(),
                "refresh did not cancel"
            );
            pause.release();
            let all = timeout(
                Duration::from_secs(5),
                f.a.build_slots
                    .clone()
                    .acquire_many_owned(u32::try_from(f.a.config.build_workers)?),
            )
            .await??;
            ensure!(
                prepared(&f.a).await?.storage_key == first.storage_key,
                "cancelled refresh published its replacement"
            );
            drop(all);
            // Aborting a SQLx transaction queues rollback on its connection.
            // Cycle every checkout before marking the next refresh, so a late
            // cleanup response cannot be mistaken for that refresh's SQL.
            let drained = timeout(Duration::from_secs(5), async {
                let mut checkouts = Vec::new();
                for _ in 0..f.a.config.database_connections {
                    let mut connection = f.pool().acquire().await?;
                    sqlx::query("SELECT 1").execute(&mut *connection).await?;
                    checkouts.push(connection);
                }
                Ok::<_, anyhow::Error>(checkouts)
            })
            .await
            .context("cancelled checkout did not finish cleanup")??;
            drop(drained);
            let mark = f.proxy.mark();
            f.a.refresh_once().await?;
            ensure!(
                f.returned_share_rows(mark)? == 0,
                "cancelled reservation discarded reusable cached inputs"
            );
            ensure!(
                prepared(&f.a).await?.snapshot.payout_revision
                    == first.snapshot.payout_revision + 1,
                "refresh did not recover after cancellation cleanup"
            );
            Ok(())
        })
    })
    .await
}
