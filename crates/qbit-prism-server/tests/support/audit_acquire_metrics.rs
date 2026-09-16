//! Checkout observations at the audit/migration/reconciliation callers (#352).
use super::*;
use qbit_prism_server::metrics::Metrics;
use sqlx::postgres::PgPoolOptions;
use std::future::{poll_fn, Future};
use std::sync::Arc;
use std::task::Poll;
use tokio_util::task::AbortOnDropHandle;

const WAIT: Duration = Duration::from_secs(10);

fn sample(metrics: &Metrics, outcome: &str, suffix: &str) -> f64 {
    let prefix =
        format!("qbit_prism_database_pool_acquire_seconds_{suffix}{{result=\"{outcome}\"}} ");
    metrics
        .render()
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .map_or(0., |value| value.parse().unwrap())
}

pub(super) fn counts(metrics: &Metrics) -> (f64, f64) {
    (
        sample(metrics, "success", "count"),
        sample(metrics, "failure", "count"),
    )
}

fn family(metrics: &Metrics) -> Vec<String> {
    metrics
        .render()
        .lines()
        .filter(|line| line.contains("qbit_prism_database_pool_acquire_seconds"))
        .map(str::to_owned)
        .collect()
}

pub(super) async fn ledger(db: &Database, metrics: &Arc<Metrics>) -> Result<Ledger> {
    let mut ledger = Ledger::connect_with_metrics(
        &db.url,
        "audit-acquire".into(),
        2,
        true,
        Some(metrics.clone()),
    )
    .await?;
    // A retained checkout cannot hide behind a second pool slot.
    let single = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(WAIT)
        .connect(&db.url)
        .await?;
    let old = std::mem::replace(&mut ledger.pool, single);
    old.close().await;
    Ok(ledger)
}

#[derive(Clone, Copy, Debug)]
enum Caller {
    Audit,
    Source,
    Import,
    Backfill,
    Reconcile,
}

impl Caller {
    const ALL: [Self; 5] = [
        Self::Audit,
        Self::Source,
        Self::Import,
        Self::Backfill,
        Self::Reconcile,
    ];

    async fn run(self, ledger: &Ledger) -> Result<()> {
        match self {
            Self::Audit => assert!(ledger.audit_bundle("missing").await?.is_none()),
            Self::Source => assert_eq!(
                ledger
                    .migration_source()
                    .await?
                    .context("source")?
                    .source_state,
                "fresh"
            ),
            Self::Import => assert_eq!(
                ledger
                    .import_legacy_audits(None, &ledger_public_key())
                    .await?,
                0
            ),
            Self::Backfill => assert_eq!(ledger.backfill_ctv(&ledger_public_key()).await?, 0),
            Self::Reconcile => assert!(ledger.pool_blocks_for_reconcile().await?.is_empty()),
        }
        Ok(())
    }

    fn table(self) -> &'static str {
        match self {
            Self::Source => "qbit_prism_migration_source",
            Self::Reconcile => "qbit_pool_blocks",
            _ => "qbit_pool_audit_bundles",
        }
    }

    fn column(self) -> &'static str {
        match self {
            Self::Source => "migrated_by",
            _ => "block_hash",
        }
    }
}

async fn wait_for_counts(metrics: &Metrics, expected: (f64, f64)) -> Result<()> {
    tokio::time::timeout(WAIT, async {
        while counts(metrics) != expected {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .context("caller did not reach the expected checkout boundary")
}

#[tokio::test]
async fn direct_callers_count_checkout_outcomes_and_recover() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let metrics = Arc::new(Metrics::default());
    let ledger = ledger(&db, &metrics).await?;
    let result = direct_cases(&db, &ledger, &metrics).await;
    result.and(db.close(vec![ledger]).await)
}

async fn direct_cases(db: &Database, ledger: &Ledger, metrics: &Metrics) -> Result<()> {
    let side = PgPool::connect(&db.url).await?;
    for caller in Caller::ALL {
        let before = counts(metrics);
        drop(caller.run(ledger));
        assert_eq!(counts(metrics), before, "{caller:?}: unpolled");
        tokio::time::timeout(WAIT, caller.run(ledger)).await??;
        assert_eq!(
            counts(metrics),
            (before.0 + 1., before.1),
            "{caller:?}: success"
        );

        // Schema errors are SQL errors after checkout, retaining their SQLSTATE.
        let rename = format!(
            "ALTER TABLE {} RENAME COLUMN {} TO acquire_test_hidden",
            caller.table(),
            caller.column()
        );
        sqlx::query(&rename).execute(&side).await?;
        let before = counts(metrics);
        let error = caller.run(ledger).await.unwrap_err();
        assert_eq!(
            error
                .downcast_ref::<sqlx::Error>()
                .and_then(sqlx::Error::as_database_error)
                .and_then(|error| error.code())
                .as_deref(),
            Some("42703"),
            "{caller:?}: {error:#}"
        );
        assert_eq!(
            counts(metrics),
            (before.0 + 1., before.1),
            "{caller:?}: SQL error"
        );
        sqlx::query(&format!(
            "ALTER TABLE {} RENAME COLUMN acquire_test_hidden TO {}",
            caller.table(),
            caller.column()
        ))
        .execute(&side)
        .await?;

        let held = ledger.pool.acquire().await?;
        let before = counts(metrics);
        let failure_sum = sample(metrics, "failure", "sum");
        // Reach the held pool slot before starting the cancellation deadline.
        // Caller setup (including import key derivation) is outside checkout
        // timing and may consume an arbitrary part of an earlier deadline.
        let mut acquiring = Box::pin(caller.run(ledger));
        assert!(
            poll_fn(|cx| Poll::Ready(acquiring.as_mut().poll(cx)))
                .await
                .is_pending(),
            "{caller:?}: expected checkout wait"
        );
        assert_eq!(counts(metrics), before, "{caller:?}: pending checkout");
        assert!(tokio::time::timeout(Duration::from_millis(50), acquiring)
            .await
            .is_err());
        assert_eq!(
            counts(metrics),
            (before.0, before.1 + 1.),
            "{caller:?}: checkout cancellation"
        );
        let observed = sample(metrics, "failure", "sum") - failure_sum;
        assert!(observed >= 0.04, "{caller:?}: observed {observed} seconds");
        drop(held);
        tokio::time::timeout(WAIT, caller.run(ledger)).await??;
        assert_eq!(
            counts(metrics),
            (before.0 + 1., before.1 + 1.),
            "{caller:?}: recovery"
        );

        // Wait in the caller's real SQL after a successful checkout. The entire
        // family must freeze before that wait and remain frozen after abort.
        let mut lock = side.begin().await?;
        sqlx::query(&format!(
            "LOCK TABLE {} IN ACCESS EXCLUSIVE MODE",
            caller.table()
        ))
        .execute(&mut *lock)
        .await?;
        let before = counts(metrics);
        let task = AbortOnDropHandle::new(tokio::spawn({
            let ledger = ledger.clone();
            async move { caller.run(&ledger).await }
        }));
        wait_for_counts(metrics, (before.0 + 1., before.1)).await?;
        let acquired = family(metrics);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!task.is_finished(), "{caller:?}: expected SQL lock wait");
        assert_eq!(family(metrics), acquired);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(family(metrics), acquired, "{caller:?}: SQL cancellation");
        lock.rollback().await?;
        tokio::time::timeout(WAIT, caller.run(ledger)).await??;
        assert_eq!(
            counts(metrics),
            (before.0 + 2., before.1),
            "{caller:?}: SQL recovery"
        );
    }
    side.close().await;
    ledger.pool.close().await;
    for caller in Caller::ALL {
        let before = counts(metrics);
        let error = caller.run(ledger).await.unwrap_err();
        assert!(
            matches!(
                error.downcast_ref::<sqlx::Error>(),
                Some(sqlx::Error::PoolClosed)
            ),
            "{caller:?}: {error:#}"
        );
        assert_eq!(counts(metrics), (before.0, before.1 + 1.));
    }
    Ok(())
}

// Dropping the sender also unblocks the worker if an assertion/test fails.
struct BlockingGate(Option<std::sync::mpsc::Sender<()>>);
impl Drop for BlockingGate {
    fn drop(&mut self) {
        self.0.take();
    }
}

#[test]
fn landing_counts_each_page_and_releases_before_blocking_work() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let Some(db) = Database::open().await? else {
            return Ok(());
        };
        let metrics = Arc::new(Metrics::default());
        let ledger = ledger(&db, &metrics).await?;
        let result = landing_cases(&db, &ledger, &metrics).await;
        result.and(db.close(vec![ledger]).await)
    })
}

async fn landing_cases(db: &Database, ledger: &Ledger, metrics: &Metrics) -> Result<()> {
    let plan = WindowPlan::new(PROOF_WINDOW_SHARES)?;
    plan.load(&ledger.pool, "audit-acquire").await?;
    let snapshot = ledger.snapshot(plan.window_network_difficulty()).await?;
    let candidate = signed_candidate(snapshot.shares.clone(), &snapshot, &plan, 3520)?;
    let canonical = canonical_audit_bundle_bytes(&candidate.bundle)?;
    let claim = claim_enqueued(ledger, candidate).await?;

    // Both the landing probe and the first page can fail SQL after checkout.
    let side = PgPool::connect(&db.url).await?;
    for (table, column, acquired) in [
        ("qbit_pool_audit_bundles", "block_hash", 1.),
        ("qbit_share_ledger", "ntime", 2.),
    ] {
        sqlx::query(&format!(
            "ALTER TABLE {table} RENAME COLUMN {column} TO acquire_test_hidden"
        ))
        .execute(&side)
        .await?;
        let before = counts(metrics);
        let error = ledger
            .land_candidate(&claim, &ledger_public_key())
            .await
            .unwrap_err();
        assert_eq!(
            error
                .downcast_ref::<sqlx::Error>()
                .and_then(sqlx::Error::as_database_error)
                .and_then(|error| error.code())
                .as_deref(),
            Some("42703")
        );
        assert_eq!(counts(metrics), (before.0 + acquired, before.1));
        sqlx::query(&format!(
            "ALTER TABLE {table} RENAME COLUMN acquire_test_hidden TO {column}"
        ))
        .execute(&side)
        .await?;
    }

    // Stall the first page's SQL so landing preparation has already finished
    // before we occupy the runtime's only blocking thread.
    let mut lock = side.begin().await?;
    sqlx::query("LOCK TABLE qbit_share_ledger IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await?;
    let before = counts(metrics);
    let task = AbortOnDropHandle::new(tokio::spawn({
        let ledger = ledger.clone();
        let claim = claim.clone();
        async move { ledger.land_candidate(&claim, &ledger_public_key()).await }
    }));
    wait_for_counts(metrics, (before.0 + 2., before.1)).await?;
    let acquired = family(metrics);
    let (release, blocked) = std::sync::mpsc::channel();
    let gate = BlockingGate(Some(release));
    let (started, ready) = tokio::sync::oneshot::channel();
    let worker = tokio::task::spawn_blocking(move || {
        let _ = started.send(());
        let _ = blocked.recv();
    });
    tokio::time::timeout(WAIT, ready).await??;
    lock.rollback().await?;

    // A page must release its sole connection while its comparison is queued
    // behind the gate. Retaining a connection across spawn_blocking deadlocks
    // this acquisition, even if counts would eventually look correct.
    let connection = tokio::time::timeout(WAIT, ledger.pool.acquire()).await??;
    assert!(!task.is_finished());
    assert_eq!(
        family(metrics),
        acquired,
        "SQL/decode waiting extended the checkout"
    );
    drop(connection);
    drop(gate);
    worker.await?;
    tokio::time::timeout(WAIT, task).await???;
    // Probe + three independent pages + newest boundary + BEGIN.
    assert_eq!(counts(metrics), (before.0 + 6., before.1));
    assert_eq!(
        audit_canonical_bytes(&ledger.pool, &claim.candidate.block_hash)
            .await?
            .as_deref(),
        Some(canonical.as_slice())
    );

    // Existing audit rows skip the page proof: only probe + BEGIN.
    let before = counts(metrics);
    ledger.land_candidate(&claim, &ledger_public_key()).await?;
    assert_eq!(counts(metrics), (before.0 + 2., before.1));
    side.close().await;
    Ok(())
}

#[tokio::test]
async fn legacy_import_pages_release_before_writes_and_audit_decode() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let metrics = Arc::new(Metrics::default());
    let ledger = ledger(&db, &metrics).await?;
    let result = legacy_cases(&ledger, &metrics).await;
    result.and(db.close(vec![ledger]).await)
}

async fn legacy_cases(ledger: &Ledger, metrics: &Metrics) -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut landed = Vec::new();
    for nonce in [3521, 3522] {
        let block = land_small_block(ledger, nonce).await?;
        ledger.finish_candidate(&block.claim, true, None).await?;
        let path = dir.path().join(format!("{}.json", block.hash()));
        std::fs::write(&path, &block.canonical)?;
        sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=NULL,share_snapshot_sha256=NULL,body_uri=$2 WHERE block_hash=$1")
            .bind(block.hash()).bind(path.to_str().context("utf-8 temp path")?)
            .execute(&ledger.pool).await?;
        landed.push(block);
    }
    let before = counts(metrics);
    let imported = tokio::time::timeout(
        WAIT,
        ledger.import_legacy_audits(Some(dir.path()), &ledger_public_key()),
    )
    .await??;
    assert_eq!(imported, 2);
    // Two one-row reads, two write transactions, and the final empty read.
    assert_eq!(counts(metrics), (before.0 + 5., before.1));
    let before = counts(metrics);
    assert_eq!(
        ledger
            .import_legacy_audits(Some(dir.path()), &ledger_public_key())
            .await?,
        0
    );
    assert_eq!(counts(metrics), (before.0 + 1., before.1));
    for block in &landed {
        let before = counts(metrics);
        assert_eq!(
            tokio::time::timeout(WAIT, ledger.audit_bundle(block.hash())).await??,
            Some(block.logical.clone())
        );
        assert_eq!(counts(metrics), (before.0 + 1., before.1));
    }
    let before = counts(metrics);
    assert_eq!(
        tokio::time::timeout(WAIT, ledger.backfill_ctv(&ledger_public_key())).await??,
        0
    );
    // One list read and one audit read per block; these bundles have no CTV set.
    assert_eq!(counts(metrics), (before.0 + 3., before.1));
    Ok(())
}
