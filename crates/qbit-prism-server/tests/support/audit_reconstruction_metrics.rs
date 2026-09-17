//! Only fresh checkouts in ledger-owned audit reconstruction are observations.
use super::*;
use acquire_metrics::{counts, ledger};
use qbit_prism_server::metrics::Metrics;
use sqlx::postgres::PgPoolOptions;
use std::{
    future::{poll_fn, Future},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::Poll,
};
use tokio_util::task::AbortOnDropHandle;

const WAIT: Duration = Duration::from_secs(10);

fn family(metrics: &Metrics) -> Vec<String> {
    metrics
        .render()
        .lines()
        .filter(|line| line.contains("qbit_prism_database_pool_acquire_seconds"))
        .map(str::to_owned)
        .collect()
}

async fn wait_for_counts(metrics: &Metrics, expected: (f64, f64)) -> Result<()> {
    tokio::time::timeout(WAIT, async {
        while counts(metrics) != expected {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .context("audit read did not reach checkout boundary")
}

async fn assert_read(
    ledger: &Ledger,
    metrics: &Metrics,
    block: &Landed,
    checkouts: f64,
) -> Result<()> {
    let before = counts(metrics);
    assert_eq!(
        tokio::time::timeout(WAIT, ledger.audit_bundle(block.hash())).await??,
        Some(block.logical.clone())
    );
    assert_eq!(counts(metrics), (before.0 + checkouts, before.1));
    let observed = family(metrics);
    // The pool-only public helper has no metrics owner. Its bytes and policy
    // remain unchanged, even when the pool also belongs to a metered ledger.
    assert_eq!(
        audit_canonical_bytes(&ledger.pool, block.hash()).await?,
        Some(block.canonical.clone())
    );
    assert_eq!(family(metrics), observed);
    Ok(())
}

#[tokio::test]
async fn representations_count_only_their_fresh_checkouts_and_preserve_bytes() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let metrics = Arc::new(Metrics::default());
    let ledger = ledger(&db, &metrics).await?;
    let result = representation_cases(&ledger, &metrics).await;
    result.and(db.close(vec![ledger]).await)
}

async fn representation_cases(ledger: &Ledger, metrics: &Metrics) -> Result<()> {
    let block = land_small_block(ledger, 35230).await?;
    // The fixture appends a later share: equality also pins the original
    // anchored range rather than present-day payout state.
    assert_read(ledger, metrics, &block, 3.).await?;
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=jsonb_set(audit_bundle,'{reward_manifest,shares}',$2) WHERE block_hash=$1")
        .bind(block.hash()).bind(serde_json::to_value(&block.bundle.reward_manifest.shares)?)
        .execute(&ledger.pool).await?;
    assert_read(ledger, metrics, &block, 3.).await?;
    sqlx::query("UPDATE qbit_prism_audit_snapshots SET inline_shares=$1")
        .bind(serde_json::to_value(&block.bundle.shares)?)
        .execute(&ledger.pool)
        .await?;
    assert_read(ledger, metrics, &block, 2.).await?;
    // Restore the normalized manifest shape while keeping inline shares.
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=audit_bundle #- '{reward_manifest,shares}'")
        .execute(&ledger.pool).await?;
    assert_read(ledger, metrics, &block, 2.).await?;
    sqlx::query("UPDATE qbit_pool_audit_bundles SET share_snapshot_sha256=NULL,canonical_audit_bytes=$1,audit_bundle='{}'")
        .bind(&block.canonical).execute(&ledger.pool).await?;
    assert_read(ledger, metrics, &block, 1.).await?;
    sqlx::query("UPDATE qbit_pool_audit_bundles SET canonical_audit_bytes=NULL,audit_bundle=$1")
        .bind(&block.logical)
        .execute(&ledger.pool)
        .await?;
    let before = counts(metrics);
    assert_eq!(
        ledger.audit_bundle(block.hash()).await?,
        Some(block.logical.clone())
    );
    assert_eq!(counts(metrics), (before.0 + 1., before.1));
    let before = counts(metrics);
    assert!(ledger.audit_bundle("missing").await?.is_none());
    assert_eq!(counts(metrics), (before.0 + 1., before.1));
    Ok(())
}

#[tokio::test]
async fn missing_data_and_decode_or_sql_errors_keep_successful_checkouts() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let metrics = Arc::new(Metrics::default());
    let ledger = ledger(&db, &metrics).await?;
    let result = error_cases(&ledger, &metrics).await;
    result.and(db.close(vec![ledger]).await)
}

async fn error_cases(ledger: &Ledger, metrics: &Metrics) -> Result<()> {
    let block = land_small_block(ledger, 35231).await?;
    for (table, column, checkouts) in [
        ("qbit_pool_audit_bundles", "block_hash", 1.),
        ("qbit_prism_audit_snapshots", "inline_shares", 2.),
        ("qbit_share_ledger", "ntime", 3.),
    ] {
        sqlx::query(&format!(
            "ALTER TABLE {table} RENAME COLUMN {column} TO reconstruction_hidden"
        ))
        .execute(&ledger.pool)
        .await?;
        let before = counts(metrics);
        let error = ledger.audit_bundle(block.hash()).await.unwrap_err();
        assert_eq!(
            error
                .downcast_ref::<sqlx::Error>()
                .and_then(sqlx::Error::as_database_error)
                .and_then(|error| error.code())
                .as_deref(),
            Some("42703")
        );
        assert_eq!(counts(metrics), (before.0 + checkouts, before.1));
        sqlx::query(&format!(
            "ALTER TABLE {table} RENAME COLUMN reconstruction_hidden TO {column}"
        ))
        .execute(&ledger.pool)
        .await?;
    }
    // Corrupt only this disposable fixture, and restore before the next case.
    for (statement, restore, checkouts, message) in [
        ("UPDATE qbit_prism_audit_snapshots SET inline_shares='{}'", "UPDATE qbit_prism_audit_snapshots SET inline_shares=NULL", 2., "invalid type"),
        ("UPDATE qbit_prism_audit_snapshots SET first_share_seq=last_share_seq+1,last_share_seq=last_share_seq+1", "UPDATE qbit_prism_audit_snapshots SET first_share_seq=first_share_seq-1,last_share_seq=last_share_seq-1", 3., "audit share history is incomplete"),
        ("UPDATE qbit_pool_audit_bundles SET audit_bundle_sha256=repeat('0',64)", "", 3., "materialized audit body digest mismatch"),
    ] {
        sqlx::query(statement).execute(&ledger.pool).await?;
        let before = counts(metrics);
        let error = ledger.audit_bundle(block.hash()).await.unwrap_err();
        assert!(error.to_string().contains(message), "{error:#}");
        assert_eq!(counts(metrics), (before.0 + checkouts, before.1));
        if !restore.is_empty() { sqlx::query(restore).execute(&ledger.pool).await?; }
    }
    // A range row decoding failure must not relabel its completed checkout.
    sqlx::query("ALTER TABLE qbit_share_ledger DISABLE TRIGGER qbit_prism_immutable_share_history")
        .execute(&ledger.pool)
        .await?;
    sqlx::query("UPDATE qbit_share_ledger SET ntime=4294967296")
        .execute(&ledger.pool)
        .await?;
    let before = counts(metrics);
    let error = ledger.audit_bundle(block.hash()).await.unwrap_err();
    assert!(
        error.downcast_ref::<std::num::TryFromIntError>().is_some(),
        "{error:#}"
    );
    assert_eq!(counts(metrics), (before.0 + 3., before.1));
    sqlx::query("ALTER TABLE qbit_pool_audit_bundles DROP CONSTRAINT qbit_pool_audit_bundles_share_snapshot_sha256_fkey")
        .execute(&ledger.pool).await?;
    sqlx::query("UPDATE qbit_pool_audit_bundles SET share_snapshot_sha256=repeat('0',64)")
        .execute(&ledger.pool)
        .await?;
    let before = counts(metrics);
    let error = ledger.audit_bundle(block.hash()).await.unwrap_err();
    assert!(matches!(
        error.downcast_ref::<sqlx::Error>(),
        Some(sqlx::Error::RowNotFound)
    ));
    assert_eq!(counts(metrics), (before.0 + 2., before.1));
    Ok(())
}

#[tokio::test]
async fn cancellation_distinguishes_pending_checkout_from_each_running_query() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let metrics = Arc::new(Metrics::default());
    let mut ledger = ledger(&db, &metrics).await?;
    let result = cancellation_cases(&db, &mut ledger, &metrics).await;
    result.and(db.close(vec![ledger]).await)
}

async fn cancellation_cases(db: &Database, ledger: &mut Ledger, metrics: &Metrics) -> Result<()> {
    let block = land_small_block(ledger, 35232).await?;
    let before = family(metrics);
    drop(ledger.audit_bundle(block.hash()));
    assert_eq!(family(metrics), before, "unpolled future");
    let held = ledger.pool.acquire().await?;
    let before = counts(metrics);
    let mut queued = Box::pin(ledger.audit_bundle(block.hash()));
    assert!(poll_fn(|cx| Poll::Ready(queued.as_mut().poll(cx)))
        .await
        .is_pending());
    assert_eq!(counts(metrics), before);
    assert!(tokio::time::timeout(Duration::from_millis(50), queued)
        .await
        .is_err());
    assert_eq!(counts(metrics), (before.0, before.1 + 1.));
    drop(held);
    assert_read(ledger, metrics, &block, 3.).await?;

    // Stop each subsequent checkout inside its real before_acquire hook. The
    // first N-1 SQL statements have finished, but checkout N has not succeeded.
    for stop_at in [2, 3] {
        let calls = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let hooked = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(WAIT)
            .before_acquire({
                let calls = calls.clone();
                let gate = gate.clone();
                move |_, _| {
                    let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
                    let gate = gate.clone();
                    Box::pin(async move {
                        if call == stop_at {
                            let _permit = gate.acquire().await.unwrap();
                        }
                        Ok(true)
                    })
                }
            })
            .connect(&db.url)
            .await?;
        let old = std::mem::replace(&mut ledger.pool, hooked);
        old.close().await;
        let before = counts(metrics);
        let task = AbortOnDropHandle::new(tokio::spawn({
            let ledger = ledger.clone();
            let hash = block.hash().to_owned();
            async move { ledger.audit_bundle(&hash).await }
        }));
        tokio::time::timeout(WAIT, async {
            while calls.load(Ordering::SeqCst) < stop_at {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert_eq!(counts(metrics), (before.0 + (stop_at - 1) as f64, before.1));
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(
            counts(metrics),
            (before.0 + (stop_at - 1) as f64, before.1 + 1.)
        );
        gate.add_permits(1);
        assert_read(ledger, metrics, &block, 3.).await?;
    }
    for (table, completed) in [
        ("qbit_pool_audit_bundles", 1.),
        ("qbit_prism_audit_snapshots", 2.),
        ("qbit_share_ledger", 3.),
    ] {
        let mut lock = db.admin.begin().await?;
        sqlx::query(&format!("LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE"))
            .execute(&mut *lock)
            .await?;
        let before = counts(metrics);
        let task = AbortOnDropHandle::new(tokio::spawn({
            let ledger = ledger.clone();
            let hash = block.hash().to_owned();
            async move { ledger.audit_bundle(&hash).await }
        }));
        wait_for_counts(metrics, (before.0 + completed, before.1)).await?;
        let acquired = family(metrics);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!task.is_finished(), "expected {table} SQL wait");
        assert_eq!(family(metrics), acquired);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(
            family(metrics),
            acquired,
            "SQL cancellation is not checkout failure"
        );
        lock.rollback().await?;
        assert_read(ledger, metrics, &block, 3.).await?;
    }
    ledger.pool.close().await;
    let before = counts(metrics);
    assert!(matches!(
        ledger
            .audit_bundle(block.hash())
            .await
            .unwrap_err()
            .downcast_ref::<sqlx::Error>(),
        Some(sqlx::Error::PoolClosed)
    ));
    assert_eq!(counts(metrics), (before.0, before.1 + 1.));
    Ok(())
}

// Disconnecting the channel also unblocks the worker on assertion failure.
struct BlockingGate(Option<std::sync::mpsc::Sender<()>>);
impl Drop for BlockingGate {
    fn drop(&mut self) {
        self.0.take();
    }
}

#[test]
fn reconstruction_releases_sole_connection_before_blocking_and_without_metrics() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()?
        .block_on(async {
            let Some(db) = Database::open().await? else {
                return Ok(());
            };
            let metrics = Arc::new(Metrics::default());
            let ledger = ledger(&db, &metrics).await?;
            let plain = db.ledger("reconstruction-no-metrics").await?;
            let result = blocking_cases(&ledger, &plain, &metrics).await;
            result.and(db.close(vec![ledger, plain]).await)
        })
}

async fn blocking_cases(ledger: &Ledger, plain: &Ledger, metrics: &Metrics) -> Result<()> {
    let block = land_small_block(ledger, 35233).await?;
    for (inline, checkouts) in [(false, 3.), (true, 2.)] {
        if inline {
            sqlx::query("UPDATE qbit_prism_audit_snapshots SET inline_shares=$1")
                .bind(serde_json::to_value(&block.bundle.shares)?)
                .execute(&ledger.pool)
                .await?;
        }
        let before = family(metrics);
        assert_eq!(
            plain.audit_bundle(block.hash()).await?,
            Some(block.logical.clone())
        );
        assert_eq!(family(metrics), before, "ledger without metrics");
        let (release, blocked) = std::sync::mpsc::channel();
        let gate = BlockingGate(Some(release));
        let (started, ready) = tokio::sync::oneshot::channel();
        let worker = tokio::task::spawn_blocking(move || {
            let _ = started.send(());
            let _ = blocked.recv();
        });
        tokio::time::timeout(WAIT, ready).await??;
        let before = counts(metrics);
        let task = AbortOnDropHandle::new(tokio::spawn({
            let ledger = ledger.clone();
            let hash = block.hash().to_owned();
            async move { ledger.audit_bundle(&hash).await }
        }));
        wait_for_counts(metrics, (before.0 + checkouts, before.1)).await?;
        let acquired = family(metrics);
        // A retained snapshot prevents the range checkout; a retained final
        // connection prevents this acquire until the blocked fold finishes.
        let connection = tokio::time::timeout(WAIT, ledger.pool.acquire()).await??;
        assert!(!task.is_finished());
        assert_eq!(family(metrics), acquired);
        drop(connection);
        drop(gate);
        worker.await?;
        assert_eq!(
            tokio::time::timeout(WAIT, task).await???,
            Some(block.logical.clone())
        );
        assert_eq!(family(metrics), acquired);
    }
    Ok(())
}
