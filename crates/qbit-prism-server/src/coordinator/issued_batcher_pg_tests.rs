//! The real collector and PostgreSQL transaction, with observable SQL gates.
use super::*;
use crate::{coordinator::compact_runtime::PreparedReservation, ledger::Ledger};
use anyhow::ensure;
use sqlx::PgPool;

fn enqueue_pg(
    batcher: Arc<IssuedBatcher>,
    reservation: Arc<PreparedReservation>,
    id: &'static str,
    expiry: i64,
) -> tokio::task::JoinHandle<Result<IssuedJobSave>> {
    tokio::spawn(async move {
        batcher
            .save(
                id,
                &json!({"prepared_key":"prepared", "expires_at_ms":expiry}),
                0,
                &reservation.record.parent_hash,
                expiry,
                reservation.dependency("prepared"),
                Instant::now() + Duration::from_secs(10),
            )
            .await
    })
}

#[tokio::test]
async fn active_cancellation_keeps_live_peers_and_reconciles_all_gone() -> Result<()> {
    let Some(raw) = qbit_prism_test_gate::database_url(qbit_prism_test_gate::site!())? else {
        return Ok(());
    };
    let admin = PgPool::connect(&raw).await?;
    let (_fixture, prepared) = fixture().await;
    for committing in [false, true] {
        for live_peer in [true, false] {
            let schema = format!("prism_batch_cancel_{}", uuid::Uuid::new_v4().simple());
            sqlx::query(&format!("CREATE SCHEMA {schema}"))
                .execute(&admin)
                .await?;
            let mut url = url::Url::parse(&raw)?;
            url.query_pairs_mut()
                .append_pair("options", &format!("-csearch_path={schema}"));
            let ledger =
                Arc::new(Ledger::connect(url.as_str(), "batch-cancel".into(), 4, true).await?);
            let batcher = Arc::new(IssuedBatcher::new(ledger.clone(), IN_FLIGHT));
            let result: Result<()> = async {
                let original_expiry = ledger.now_ms().await? + 60_000;
                // This fresh PostgreSQL schema has no historical shares.
                // Seed a bootstrap reservation rather than the fake's window.
                let mut record = prepared.reservation.record.clone();
                record.window.shares = None;
                record.share_seq = 0;
                record.audit_hashes = None;
                let reservation = Arc::new(PreparedReservation {
                    record,
                    template: prepared.reservation.template.clone(),
                    balances: prepared.reservation.balances.clone(),
                    original_expires_at_ms: original_expiry,
                });
                ledger.save_compact_prepared("prepared", &reservation.record,
                    &reservation.template, &reservation.balances, 0, original_expiry).await?;
                let before = ledger.job("prepared").await?;
                let expiry = original_expiry + 60_000;
                let mut hold = ledger.pool.begin().await?;
                let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                    .fetch_one(&mut *hold).await?;
                let lock = 0x2750_0000_0000_i64 + i64::from(pid);
                sqlx::query("SELECT pg_advisory_xact_lock($1)").bind(lock)
                    .execute(&mut *hold).await?;
                let trigger = if committing {
                    "CONSTRAINT TRIGGER gate_batch AFTER INSERT ON qbit_prism_jobs DEFERRABLE INITIALLY DEFERRED"
                } else { "TRIGGER gate_batch AFTER INSERT ON qbit_prism_jobs" };
                sqlx::raw_sql(&format!("CREATE FUNCTION gate_batch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock({lock}); RETURN NEW; END; $$; CREATE {trigger} FOR EACH ROW WHEN (NEW.job_id='pg-peer') EXECUTE FUNCTION gate_batch();"))
                    .execute(&ledger.pool).await?;
                let first = enqueue_pg(batcher.clone(), reservation.clone(), "pg-cancel", expiry);
                let peer = enqueue_pg(batcher.clone(), reservation.clone(), "pg-peer", expiry + 1);
                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND query LIKE $2)")
                            .bind(pid).bind(if committing { "COMMIT%" } else { "INSERT INTO qbit_prism_jobs%" })
                            .fetch_one(&admin).await?;
                        if blocked { break Ok::<_, anyhow::Error>(()); }
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                }).await.context("collector did not reach the SQL gate")??;
                ensure!(!first.is_finished() && !peer.is_finished(), "children did not share the active batch");
                let count: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_prism_jobs WHERE job_id LIKE 'pg-%'")
                    .fetch_one(&ledger.pool).await?;
                ensure!(count == 0, "child visible before COMMIT acknowledgement");
                first.abort();
                ensure!(first.await.unwrap_err().is_cancelled());
                if !live_peer { peer.abort(); }
                tokio::task::yield_now().await;
                if live_peer { ensure!(!peer.is_finished(), "canceled member failed its live peer"); }
                hold.rollback().await?;
                if live_peer {
                    ensure!(peer.await?? == IssuedJobSave::Saved);
                } else {
                    ensure!(peer.await.unwrap_err().is_cancelled());
                }
                settled(&batcher).await;
                if !live_peer {
                    let (count, retention): (i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM qbit_prism_jobs WHERE job_id LIKE 'pg-%'), floor(extract(epoch FROM expires_at)*1000)::bigint FROM qbit_prism_jobs WHERE job_id='prepared'")
                        .fetch_one(&ledger.pool).await?;
                    if committing {
                        // Observe uncertainty before any explicit reconciliation:
                        // the complete commit retains both original children and
                        // their dependency, or rollback leaves no effects.
                        ensure!((count == 2 && retention == expiry + 60_001)
                            || (count == 0 && retention == original_expiry),
                            "canceled COMMIT left partial children or retention");
                    } else {
                        ensure!(count == 0 && retention == original_expiry, "canceled precommit attempt persisted a child or renewal");
                    }
                }
                if !live_peer {
                    // All-gone during COMMIT is uncertain. Reconcile only the
                    // original immutable IDs and expiries after cleanup.
                    for (id, absolute) in [("pg-cancel", expiry), ("pg-peer", expiry + 1)] {
                        ensure!(enqueue_pg(batcher.clone(), reservation.clone(), id, absolute).await?? == IssuedJobSave::Saved);
                    }
                    settled(&batcher).await;
                }
                let (count, transactions): (i64, i64) = sqlx::query_as("SELECT count(*), count(DISTINCT xmin::text) FROM qbit_prism_jobs WHERE job_id LIKE 'pg-%'")
                    .fetch_one(&ledger.pool).await?;
                ensure!(count == 2);
                if live_peer { ensure!(transactions == 1, "live peers committed separately"); }
                for (id, absolute) in [("pg-cancel", expiry), ("pg-peer", expiry + 1)] {
                    let (payload, stored_expiry): (serde_json::Value, i64) = sqlx::query_as("SELECT payload, floor(extract(epoch FROM expires_at)*1000)::bigint FROM qbit_prism_jobs WHERE job_id=$1")
                        .bind(id).fetch_one(&ledger.pool).await?;
                    ensure!(payload == json!({"prepared_key":"prepared", "expires_at_ms":absolute}) && stored_expiry == absolute);
                }
                ensure!(ledger.job("prepared").await? == before);
                let retention: i64 = sqlx::query_scalar("SELECT floor(extract(epoch FROM expires_at)*1000)::bigint FROM qbit_prism_jobs WHERE job_id='prepared'")
                    .fetch_one(&ledger.pool).await?;
                // The existing headroom can cover the second exact retry.
                ensure!(retention == expiry + 60_001 || (!live_peer && retention == expiry + 60_000));
                Ok(())
            }.await;
            drop(batcher);
            ledger.pool.close().await;
            let cleanup = sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
                .execute(&admin)
                .await;
            result?;
            cleanup?;
        }
    }
    admin.close().await;
    Ok(())
}

/// #860: two batches of one dependency group overlap in PostgreSQL. The first
/// is held inside its transaction at its child INSERT, before its COMMIT and
/// after any renewal of the dependency row.
///
/// The common case is a new tip's fan-out: the refresh stores the prepared
/// row with its shared retention (165 s by default) and each child expires
/// with the session's job retention (30 s), so no batch renews the row and
/// each takes only FOR KEY SHARE on the cluster, prepared and blob rows. The
/// second batch commits while the first is held.
///
/// A batch renews only for work issued more than about 135 s after its
/// publication. When two such batches both renew, the second waits only on
/// the first's update lock of the dependency row; both commit once the first
/// does, with no deadlock, and the dependency is renewed once.
#[tokio::test]
async fn overlapping_batches_of_one_dependency_commit_without_deadlock() -> Result<()> {
    let Some(raw) = qbit_prism_test_gate::database_url(qbit_prism_test_gate::site!())? else {
        return Ok(());
    };
    let admin = PgPool::connect(&raw).await?;
    let (_fixture, prepared) = fixture().await;
    for renewal in [false, true] {
        let schema = format!("prism_batch_overlap_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut url = url::Url::parse(&raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        let ledger =
            Arc::new(Ledger::connect(url.as_str(), "batch-overlap".into(), 4, true).await?);
        let batcher = Arc::new(IssuedBatcher::new(ledger.clone(), IN_FLIGHT));
        let result: Result<()> = async {
            let now = ledger.now_ms().await?;
            // A refresh's default prepared retention: ceil(30 s job retention
            // + 60 s reanchor + 15 s tip-poll age + 60 s).
            let original_expiry = now + 165_000;
            let mut record = prepared.reservation.record.clone();
            record.window.shares = None;
            record.share_seq = 0;
            record.audit_hashes = None;
            let reservation = Arc::new(PreparedReservation {
                record,
                template: prepared.reservation.template.clone(),
                balances: prepared.reservation.balances.clone(),
                original_expires_at_ms: original_expiry,
            });
            ledger.save_compact_prepared("prepared", &reservation.record,
                &reservation.template, &reservation.balances, 0, original_expiry).await?;
            // A session's default 30 s job retention renews nothing; work
            // issued past the prepared retention renews it.
            let (held_expiry, overlap_expiry) = if renewal {
                (original_expiry + 1, original_expiry + 2)
            } else {
                (now + 30_000, now + 30_001)
            };
            let mut hold = ledger.pool.begin().await?;
            let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut *hold).await?;
            let lock = 0x2760_0000_0000_i64 + i64::from(pid);
            sqlx::query("SELECT pg_advisory_xact_lock($1)").bind(lock)
                .execute(&mut *hold).await?;
            sqlx::raw_sql(&format!("CREATE FUNCTION gate_overlap() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock({lock}); RETURN NEW; END; $$; CREATE TRIGGER gate_overlap AFTER INSERT ON qbit_prism_jobs FOR EACH ROW WHEN (NEW.job_id='pg-held') EXECUTE FUNCTION gate_overlap();"))
                .execute(&ledger.pool).await?;
            let held = enqueue_pg(batcher.clone(), reservation.clone(), "pg-held", held_expiry);
            let held_pid: i32 = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let blocked: Option<i32> = sqlx::query_scalar("SELECT pid FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND query LIKE 'INSERT INTO qbit_prism_jobs%'")
                        .bind(pid).fetch_optional(&admin).await?;
                    if let Some(blocked) = blocked { break Ok::<_, anyhow::Error>(blocked); }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            }).await.context("held batch did not reach the SQL gate")??;
            let mut overlap = enqueue_pg(batcher.clone(), reservation.clone(), "pg-overlap", overlap_expiry);
            if renewal {
                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND query LIKE 'UPDATE qbit_prism_jobs SET expires_at%')")
                            .bind(held_pid).fetch_one(&admin).await?;
                        if blocked { break Ok::<_, anyhow::Error>(()); }
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                }).await.context("second batch did not wait on the dependency renewal")??;
                ensure!(!overlap.is_finished(), "second renewal did not wait for the first");
            } else {
                let saved = tokio::time::timeout(Duration::from_secs(5), &mut overlap)
                    .await
                    .context("second batch waited for the held one")???;
                ensure!(saved == IssuedJobSave::Saved);
            }
            ensure!(!held.is_finished(), "held batch finished before its gate opened");
            hold.rollback().await?;
            ensure!(held.await?? == IssuedJobSave::Saved);
            if renewal {
                ensure!(overlap.await?? == IssuedJobSave::Saved);
            }
            settled(&batcher).await;
            let (count, transactions): (i64, i64) = sqlx::query_as("SELECT count(*), count(DISTINCT xmin::text) FROM qbit_prism_jobs WHERE job_id LIKE 'pg-%'")
                .fetch_one(&ledger.pool).await?;
            ensure!(count == 2 && transactions == 2, "{count} children in {transactions} transactions");
            let retention: i64 = sqlx::query_scalar("SELECT floor(extract(epoch FROM expires_at)*1000)::bigint FROM qbit_prism_jobs WHERE job_id='prepared'")
                .fetch_one(&ledger.pool).await?;
            let renewed = if renewal { held_expiry + 60_000 } else { original_expiry };
            ensure!(retention == renewed, "dependency retention {retention}, expected {renewed}");
            Ok(())
        }
        .await;
        drop(batcher);
        ledger.pool.close().await;
        let cleanup = sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
            .execute(&admin)
            .await;
        result?;
        cleanup?;
    }
    admin.close().await;
    Ok(())
}
