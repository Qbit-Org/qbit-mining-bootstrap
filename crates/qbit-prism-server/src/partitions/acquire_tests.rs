use super::*;
use crate::{ledger::Ledger, ledger_test_database::FixtureDatabase};
use anyhow::Context;
use futures_util::{future::LocalBoxFuture, FutureExt};
use sqlx::postgres::PgPoolOptions;
use std::panic::{resume_unwind, AssertUnwindSafe};

const BOUND: Duration = Duration::from_secs(5);

fn sample(metrics: &Metrics, result: &str, suffix: &str) -> f64 {
    let key = format!("qbit_prism_database_pool_acquire_seconds_{suffix}{{result=\"{result}\"}} ");
    metrics
        .render()
        .lines()
        .find_map(|line| line.strip_prefix(&key))
        .unwrap()
        .parse()
        .unwrap()
}

fn counts(metrics: &Metrics) -> (f64, f64) {
    (
        sample(metrics, "success", "count"),
        sample(metrics, "failure", "count"),
    )
}

async fn with_database<F>(case: F) -> Result<()>
where
    F: for<'a> FnOnce(&'a Ledger, &'a PgPool, &'a Arc<Metrics>) -> LocalBoxFuture<'a, Result<()>>,
{
    use qbit_prism_test_gate as gate;
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let database = FixtureDatabase::open(&raw, "partition_acquire_").await?;
    let metrics = Arc::new(Metrics::default());
    let mut pools = Vec::new();
    let result = AssertUnwindSafe(async {
        let mut ledger = Ledger::connect_with_metrics(
            &database.url,
            "partition-acquire".into(),
            2,
            true,
            Some(metrics.clone()),
        )
        .await?;
        pools.push(ledger.pool.clone());
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_millis(250))
            .connect(&database.url)
            .await?;
        pools.push(pool.clone());
        let admin = std::mem::replace(&mut ledger.pool, pool);
        case(&ledger, &admin, &metrics).await
    })
    .catch_unwind()
    .await;
    for pool in pools {
        pool.close().await;
    }
    let cleanup = database.close(Ok(())).await;
    match result {
        Ok(result) => result.and(cleanup),
        Err(panic) => {
            if let Err(error) = cleanup {
                eprintln!("partition fixture cleanup failed: {error}");
            }
            resume_unwind(panic)
        }
    }
}

async fn reusable(pool: &PgPool) -> Result<()> {
    tokio::time::timeout(BOUND, async {
        let mut tx = pool.begin().await?;
        assert_eq!(
            sqlx::query_scalar::<_, i32>("SELECT 42")
                .fetch_one(&mut *tx)
                .await?,
            42
        );
        tx.commit().await
    })
    .await
    .context("partition attempt retained the only pool slot")??;
    Ok(())
}

#[tokio::test]
async fn unpolled_and_closed_pool_outcomes() -> Result<()> {
    let pool = PgPoolOptions::new().connect_lazy("postgresql://fixture@127.0.0.1:1/fixture")?;
    pool.close().await;
    let metrics = Metrics::default();
    drop(ensure_with_metrics(&pool, Some(&metrics)));
    assert_eq!(counts(&metrics), (0., 0.));
    let error = ensure_with_metrics(&pool, Some(&metrics))
        .await
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<sqlx::Error>(),
        Some(sqlx::Error::PoolClosed)
    ));
    assert_eq!(counts(&metrics), (0., 1.));
    assert!(ensure(&pool).await.is_err());
    assert_eq!(counts(&metrics), (0., 1.));
    Ok(())
}

#[tokio::test]
async fn postgres_checkout_wait_error_cancel_and_reuse() -> Result<()> {
    with_database(|ledger, _, metrics| {
        Box::pin(async move {
            let pool = &ledger.pool;
            let before = counts(metrics);
            let held = pool.acquire().await?;
            let mut pending = Box::pin(ensure_with_metrics(pool, Some(metrics)));
            assert!(futures_util::poll!(&mut pending).is_pending());
            tokio::time::sleep(Duration::from_millis(40)).await;
            assert_eq!(counts(metrics), before);
            drop(pending);
            assert_eq!(counts(metrics), (before.0, before.1 + 1.));
            assert!(sample(metrics, "failure", "sum") >= 0.04);

            let error = ensure_with_metrics(pool, Some(metrics)).await.unwrap_err();
            assert!(matches!(
                error.downcast_ref::<sqlx::Error>(),
                Some(sqlx::Error::PoolTimedOut)
            ));
            assert_eq!(counts(metrics), (before.0, before.1 + 2.));

            let sum = sample(metrics, "success", "sum");
            let mut pending = Box::pin(ensure_with_metrics(pool, Some(metrics)));
            assert!(futures_util::poll!(&mut pending).is_pending());
            tokio::time::sleep(Duration::from_millis(40)).await;
            drop(held);
            assert_eq!(tokio::time::timeout(BOUND, pending).await??, 0);
            assert_eq!(counts(metrics), (before.0 + 1., before.1 + 2.));
            assert!(sample(metrics, "success", "sum") - sum >= 0.04);
            reusable(pool).await?;
            assert_eq!(ensure(pool).await?, 0);
            assert_eq!(counts(metrics), (before.0 + 1., before.1 + 2.));

            // The real maintenance loop's shutdown cancels a pending checkout.
            let held = pool.acquire().await?;
            let (shutdown, receiver) = watch::channel(false);
            let mut task = Box::pin(run_with_metrics(
                pool.clone(),
                Settings { interval: BOUND },
                receiver,
                Some(metrics.clone()),
            ));
            tokio::select! {
                result = &mut task => panic!("maintenance ended early: {result:?}"),
                _ = tokio::time::sleep(Duration::from_millis(40)) => {},
            }
            assert_eq!(counts(metrics), (before.0 + 1., before.1 + 2.));
            shutdown.send(true)?;
            tokio::time::timeout(BOUND, task).await??;
            assert_eq!(counts(metrics), (before.0 + 1., before.1 + 3.));
            drop(held);
            reusable(pool).await
        })
    })
    .await
}

async fn wait_for_partition_sql(admin: &PgPool) -> Result<()> {
    tokio::time::timeout(BOUND, async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT FROM pg_stat_activity WHERE datname=current_database() AND query='SELECT qbit_prism_share_partition_ensure()' AND wait_event_type='Lock')")
                .fetch_one(admin).await?;
            if waiting { return Ok::<_, anyhow::Error>(()); }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.context("partition ensure never reached the locked SQL statement")?
}

#[tokio::test]
async fn postgres_sql_wait_error_cancel_and_shutdown_keep_checkout_success() -> Result<()> {
    with_database(|ledger, admin, metrics| {
        Box::pin(async move {
            let pool = &ledger.pool;
            for cancel in [false, true] {
                let mut blocker = admin.begin().await?;
                sqlx::query("LOCK TABLE qbit_prism_share_partitioning IN ACCESS EXCLUSIVE MODE")
                    .execute(&mut *blocker)
                    .await?;
                let before = counts(metrics);
                let mut attempt = Box::pin(ensure_with_metrics(pool, Some(metrics)));
                tokio::select! {
                    result = &mut attempt => panic!("ensure bypassed lock: {result:?}"),
                    result = wait_for_partition_sql(admin) => result?,
                }
                assert_eq!(counts(metrics), (before.0 + 1., before.1));
                let sum = sample(metrics, "success", "sum");
                tokio::time::sleep(Duration::from_millis(40)).await;
                if cancel {
                    drop(attempt);
                } else {
                    blocker.rollback().await?;
                    assert_eq!(tokio::time::timeout(BOUND, attempt).await??, 0);
                    assert_eq!(sample(metrics, "success", "sum"), sum);
                    reusable(pool).await?;
                    continue;
                }
                blocker.rollback().await?;
                reusable(pool).await?;
                assert_eq!(counts(metrics), (before.0 + 1., before.1));
                assert_eq!(sample(metrics, "success", "sum"), sum);
            }

            let mut blocker = admin.begin().await?;
            sqlx::query("LOCK TABLE qbit_prism_share_partitioning IN ACCESS EXCLUSIVE MODE")
                .execute(&mut *blocker)
                .await?;
            let before = counts(metrics);
            let (shutdown, receiver) = watch::channel(false);
            let mut task = Box::pin(run_with_metrics(
                pool.clone(),
                Settings { interval: BOUND },
                receiver,
                Some(metrics.clone()),
            ));
            tokio::select! {
                result = &mut task => panic!("maintenance ended early: {result:?}"),
                result = wait_for_partition_sql(admin) => result?,
            }
            let sum = sample(metrics, "success", "sum");
            shutdown.send(true)?;
            tokio::time::timeout(BOUND, task).await??;
            blocker.rollback().await?;
            reusable(pool).await?;
            assert_eq!(counts(metrics), (before.0 + 1., before.1));
            assert_eq!(sample(metrics, "success", "sum"), sum);

            // A post-checkout SQL error rolls back, but checkout remains success.
            sqlx::query("ALTER TABLE qbit_prism_share_partitioning RENAME TO hidden_partitioning")
                .execute(admin)
                .await?;
            let before = counts(metrics);
            let error = ensure_with_metrics(pool, Some(metrics)).await.unwrap_err();
            assert!(error
                .downcast_ref::<sqlx::Error>()
                .and_then(sqlx::Error::as_database_error)
                .is_some());
            assert_eq!(counts(metrics), (before.0 + 1., before.1));
            reusable(pool).await?;
            sqlx::query("ALTER TABLE hidden_partitioning RENAME TO qbit_prism_share_partitioning")
                .execute(admin)
                .await?;
            assert_eq!(ensure_with_metrics(pool, Some(metrics)).await?, 0);
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn postgres_real_23514_fallback_counts_three_checkouts_and_one_effect() -> Result<()> {
    with_database(|ledger, admin, metrics| Box::pin(async move {
        let bound: i64 = sqlx::query_scalar("SELECT upper_seq FROM qbit_prism_share_partitions WHERE partition_name='qbit_share_ledger_p0'").fetch_one(admin).await?;
        let names: Vec<String> = sqlx::query_scalar("SELECT partition_name FROM qbit_prism_share_partitions WHERE partition_name<>'qbit_share_ledger_p0'").fetch_all(admin).await?;
        for name in names {
            crate::ledger_test_database::identifier(&name)?;
            sqlx::raw_sql(&format!("ALTER TABLE qbit_share_ledger DETACH PARTITION {name}; DROP TABLE {name}" )).execute(admin).await?;
            sqlx::query("DELETE FROM qbit_prism_share_partitions WHERE partition_name=$1").bind(&name).execute(admin).await?;
        }
        sqlx::query("SELECT setval('qbit_share_ledger_share_seq_seq',$1)").bind(bound).execute(admin).await?;
        let share = qbit_prism::AcceptedShare {
            share_seq: 0, share_id: format!("partition.rig:{:064x}", 1), miner_id: "partition".into(),
            order_key: "partition".into(), p2mr_program_hex: "11".repeat(32), share_difficulty: 7,
            network_difficulty: 100, template_height: 100, job_id: "job".into(), job_issued_at_ms: 1,
            accepted_at_ms: 0, ntime: 1_800_000_000, credit_policy: None,
        };
        let before = counts(metrics);
        let commits = std::sync::atomic::AtomicUsize::new(0);
        let pre_commit = || {
            commits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            true
        };
        let landed = tokio::time::timeout(
            BOUND,
            ledger.append_at_revision_gated(share.clone(), None, 0, &pre_commit),
        ).await??;
        assert!(landed.inserted);
        assert_eq!(commits.load(std::sync::atomic::Ordering::Relaxed), 1);
        // The failed INSERT consumes a sequence value; its transaction rolls
        // back, ensure checks out once, then the original append retries once.
        assert_eq!(landed.share.share_seq, u64::try_from(bound + 2)?);
        assert_eq!(counts(metrics), (before.0 + 3., before.1));
        let credited: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger WHERE share_id=$1").bind(&share.share_id).fetch_one(admin).await?;
        assert_eq!(credited, 1);
        assert!(!ledger.append(share, None).await?.inserted);
        assert_eq!(counts(metrics), (before.0 + 4., before.1));
        reusable(&ledger.pool).await
    })).await
}
