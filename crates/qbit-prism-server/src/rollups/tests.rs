use super::*;
use crate::ledger::Ledger;
use anyhow::Context;
use futures_util::{future::LocalBoxFuture, FutureExt};
use sqlx::postgres::PgPoolOptions;
use std::panic::{resume_unwind, AssertUnwindSafe};

const BOUND: Duration = Duration::from_secs(5);

fn sample(metrics: &Metrics, outcome: &str, suffix: &str) -> f64 {
    let key = format!("qbit_prism_database_pool_acquire_seconds_{suffix}{{result=\"{outcome}\"}} ");
    let body = metrics.render();
    let values: Vec<_> = body
        .lines()
        .filter_map(|line| line.strip_prefix(&key))
        .collect();
    assert_eq!(values.len(), 1, "expected one rendered series for {key}");
    values[0].parse().unwrap()
}

fn counts(metrics: &Metrics) -> (f64, f64) {
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

// Each case owns its schema and all its pools, including during setup. Cleanup
// runs after errors and assertion panics; none of these tests share metrics.
async fn with_database<F>(case: F) -> Result<()>
where
    F: for<'a> FnOnce(&'a PgPool, &'a PgPool, &'a str) -> LocalBoxFuture<'a, Result<()>>,
{
    use qbit_prism_test_gate as gate;
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let schema = format!("prism_rollup_acquire_{}", uuid::Uuid::new_v4().simple());
    let admin = PgPool::connect(&raw).await?;
    let mut pools = Vec::new();
    let result = AssertUnwindSafe(async {
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut url = url::Url::parse(&raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"))
            .append_pair("application_name", &schema);
        let ledger = Ledger::connect(url.as_str(), "rollup-test".into(), 2, true).await?;
        pools.push(ledger.pool.clone());
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(2))
            .connect(url.as_str())
            .await?;
        pools.push(pool.clone());
        case(&pool, &ledger.pool, &schema).await
    })
    .catch_unwind()
    .await;
    for pool in pools {
        pool.close().await;
    }
    let cleanup = sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(&admin)
        .await;
    admin.close().await;
    if let Err(error) = &cleanup {
        eprintln!("rollup checkout fixture cleanup failed: {error}");
    }
    match result {
        Ok(result) => result.and(cleanup.map(|_| ()).map_err(Into::into)),
        Err(panic) => resume_unwind(panic),
    }
}

async fn insert_share(pool: &PgPool) -> Result<()> {
    sqlx::query("INSERT INTO qbit_share_ledger(share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,accepted,writer_id,writer_epoch) VALUES('rollup-checkout','miner','miner',decode(repeat('11',32),'hex'),7,1,100,'fixture',to_timestamp(1),1,to_timestamp(1800000000),true,'rollup-test',0)")
        .execute(pool).await?;
    Ok(())
}

async fn assert_reusable(pool: &PgPool) -> Result<()> {
    tokio::time::timeout(BOUND, async {
        let mut transaction = pool.begin().await?;
        let value: i32 = sqlx::query_scalar("SELECT 42")
            .fetch_one(&mut *transaction)
            .await?;
        assert_eq!(value, 42);
        transaction.commit().await
    })
    .await
    .context("only pool slot was not released")??;
    Ok(())
}

async fn assert_empty(pool: &PgPool) -> Result<()> {
    for table in [
        "qbit_hashrate_rollup_progress",
        "qbit_hashrate_rollup_pool",
        "qbit_hashrate_rollup_miner",
    ] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(pool)
            .await?;
        assert_eq!(count, 0, "{table} survived rollback");
    }
    Ok(())
}

async fn wait_for_sql(admin: &PgPool, application: &str) -> Result<()> {
    wait_for_query(admin, application, "WITH progress AS%").await
}

async fn wait_for_query(admin: &PgPool, application: &str, query: &str) -> Result<()> {
    tokio::time::timeout(BOUND, async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS (SELECT FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE $2)")
                .bind(application).bind(query).fetch_one(admin).await?;
            if waiting { return Ok::<_, anyhow::Error>(()); }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.context("rollup did not reach SQL lock")?
}

#[tokio::test]
async fn invalid_batch_and_unpolled_calls_emit_nothing() -> Result<()> {
    let pool = PgPoolOptions::new().connect_lazy("postgresql://fixture@127.0.0.1:1/fixture")?;
    pool.close().await;
    let metrics = Arc::new(Metrics::default());
    let before = family(&metrics);
    for batch in [0, 100_001, u32::MAX] {
        let error = advance_with_metrics(&pool, batch, Some(&metrics))
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "rollup batch must be 1..100000");
        assert_eq!(
            advance(&pool, batch).await.unwrap_err().to_string(),
            error.to_string()
        );
        assert_eq!(family(&metrics), before);
    }
    drop(advance_with_metrics(&pool, 1, Some(&metrics)));
    drop(advance(&pool, 1));
    let (_stop, shutdown) = watch::channel(false);
    drop(run_with_metrics(
        pool,
        Settings {
            batch: 1,
            interval: Duration::from_secs(15),
        },
        shutdown,
        Some(metrics.clone()),
    ));
    assert_eq!(family(&metrics), before);
    Ok(())
}

#[tokio::test]
async fn closed_pool_preserves_acquire_error_and_counts_once() -> Result<()> {
    let pool = PgPoolOptions::new().connect_lazy("postgresql://fixture@127.0.0.1:1/fixture")?;
    pool.close().await;
    let metrics = Metrics::default();
    let error = advance_with_metrics(&pool, 1, Some(&metrics))
        .await
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<sqlx::Error>(),
        Some(sqlx::Error::PoolClosed)
    ));
    assert_eq!(counts(&metrics), (0., 1.));
    let before = family(&metrics);
    let error = advance(&pool, 1).await.unwrap_err();
    assert!(matches!(
        error.downcast_ref::<sqlx::Error>(),
        Some(sqlx::Error::PoolClosed)
    ));
    assert_eq!(family(&metrics), before);
    Ok(())
}

#[tokio::test]
async fn postgres_batch_and_no_work_each_count_one_checkout() -> Result<()> {
    with_database(|pool, admin, _| Box::pin(async move {
        insert_share(admin).await?;
        for expected_scanned in [1, 0] {
            let metrics = Metrics::default();
            let progress = advance_with_metrics(pool, 10, Some(&metrics)).await?;
            assert_eq!(progress.scanned, expected_scanned);
            assert!(progress.advanced);
            assert!(progress.last_share_seq > 0);
            assert_eq!(counts(&metrics), (1., 0.), "BEGIN, SET LOCAL, rollup SQL and COMMIT share one checkout");
            let acquired = family(&metrics);
            assert_reusable(pool).await?;
            assert_eq!(family(&metrics), acquired);
        }
        for table in ["qbit_hashrate_rollup_pool", "qbit_hashrate_rollup_miner"] {
            let rows: Vec<(i32, i64, String)> = sqlx::query_as(&format!("SELECT grain_seconds, accepted_share_count, accepted_share_difficulty::text FROM {table} ORDER BY grain_seconds"))
                .fetch_all(pool).await?;
            assert_eq!(rows, vec![(300, 1, "7".into()), (3600, 1, "7".into()), (86400, 1, "7".into())]);
        }
        let metrics = Metrics::default();
        let before = family(&metrics);
        assert_eq!(advance(pool, 10).await?.scanned, 0);
        assert_eq!(family(&metrics), before);
        Ok(())
    })).await
}

#[tokio::test]
async fn postgres_exhausted_checkout_errors_cancel_and_reuse() -> Result<()> {
    with_database(|pool, _, _| {
        Box::pin(async move {
            let held = pool.acquire().await?;
            let metrics = Metrics::default();
            let error = advance_with_metrics(pool, 1, Some(&metrics))
                .await
                .unwrap_err();
            assert!(matches!(
                error.downcast_ref::<sqlx::Error>(),
                Some(sqlx::Error::PoolTimedOut)
            ));
            assert_eq!(counts(&metrics), (0., 1.));
            let cancelled = Metrics::default();
            let mut pending = Box::pin(advance_with_metrics(pool, 1, Some(&cancelled)));
            assert!(futures_util::poll!(&mut pending).is_pending());
            assert_eq!(counts(&cancelled), (0., 0.));
            drop(pending);
            assert_eq!(counts(&cancelled), (0., 1.));
            drop(held);
            let failed = family(&metrics);
            let dropped = family(&cancelled);
            assert_reusable(pool).await?;
            assert_eq!(family(&metrics), failed);
            assert_eq!(family(&cancelled), dropped);
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn postgres_begin_delay_error_and_cancel_keep_checkout_success() -> Result<()> {
    with_database(|pool, admin, application| Box::pin(async move {
        // Reuse the shared checkout-boundary test pattern: substitute only the
        // BEGIN statement to hold/refuse SQLx's round trip. Production uses None.
        for phase in ["delay", "error", "cancel"] {
            let metrics = Metrics::default();
            let connection = time_pool_acquire(Some(&metrics), pool.acquire()).await?;
            assert_eq!(counts(&metrics), (1., 0.));
            let acquired = family(&metrics);
            match phase {
                "delay" | "cancel" => {
                    let mut blocker = admin.begin().await?;
                    // A live backend PID uniquely owns this fixture lock.
                    let lock: i64 = sqlx::query_scalar("SELECT pg_backend_pid()::bigint").fetch_one(&mut *blocker).await?;
                    sqlx::query("SELECT pg_advisory_xact_lock($1)").bind(lock).execute(&mut *blocker).await?;
                    let statement = format!("BEGIN; SELECT pg_advisory_xact_lock({lock})");
                    let mut begin = Box::pin(Transaction::begin(connection, Some(statement.into())));
                    tokio::select! {
                        result = &mut begin => anyhow::bail!("BEGIN escaped its barrier: {result:?}"),
                        result = wait_for_query(admin, application, "BEGIN; SELECT pg_advisory_xact_lock%") => result?,
                    }
                    assert_eq!(family(&metrics), acquired);
                    if phase == "cancel" {
                        drop(begin);
                        blocker.commit().await?;
                    } else {
                        blocker.commit().await?;
                        tokio::time::timeout(BOUND, begin).await??.rollback().await?;
                    }
                }
                "error" => {
                    let error = Transaction::begin(connection, Some("BEGIN ISOLATION LEVEL invalid_test_level".into())).await.unwrap_err();
                    assert_eq!(error.as_database_error().and_then(|error| error.code()).as_deref(), Some("42601"));
                }
                _ => unreachable!(),
            }
            assert_eq!(family(&metrics), acquired);
            assert_reusable(pool).await?;
            assert_eq!(family(&metrics), acquired);
        }
        Ok(())
    })).await
}

#[tokio::test]
async fn postgres_sql_wait_and_failure_keep_completed_checkout() -> Result<()> {
    with_database(|pool, admin, application| Box::pin(async move {
        insert_share(admin).await?;
        sqlx::raw_sql("CREATE FUNCTION fail_rollup() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected rollup SQL failure'; END $$; CREATE TRIGGER fail_rollup BEFORE INSERT ON qbit_hashrate_rollup_miner FOR EACH ROW EXECUTE FUNCTION fail_rollup();")
            .execute(admin).await?;
        let mut blocker = admin.begin().await?;
        sqlx::query("LOCK TABLE qbit_hashrate_rollup_pool IN ACCESS EXCLUSIVE MODE").execute(&mut *blocker).await?;
        let metrics = Metrics::default();
        let mut attempt = Box::pin(advance_with_metrics(pool, 10, Some(&metrics)));
        tokio::select! {
            result = &mut attempt => anyhow::bail!("rollup completed before SQL barrier: {result:?}"),
            result = wait_for_sql(admin, application) => result?,
        }
        assert_eq!(counts(&metrics), (1., 0.));
        let acquired = family(&metrics);
        tokio::select! {
            result = &mut attempt => anyhow::bail!("rollup escaped SQL barrier: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(75)) => {}
        }
        assert_eq!(family(&metrics), acquired, "SQL wait must not extend the checkout sum");
        blocker.commit().await?;
        let error = tokio::time::timeout(BOUND, attempt).await?.unwrap_err();
        let error = error.downcast_ref::<sqlx::Error>().unwrap().as_database_error().unwrap();
        assert_eq!(error.code().as_deref(), Some("P0001"));
        assert_eq!(error.message(), "injected rollup SQL failure");
        assert_reusable(pool).await?;
        assert_empty(pool).await?;
        assert_eq!(family(&metrics), acquired);
        Ok(())
    })).await
}

#[tokio::test]
async fn postgres_sql_cancellation_retains_success_and_rolls_back() -> Result<()> {
    with_database(|pool, admin, application| Box::pin(async move {
        insert_share(admin).await?;
        let mut blocker = admin.begin().await?;
        sqlx::query("LOCK TABLE qbit_hashrate_rollup_pool IN ACCESS EXCLUSIVE MODE").execute(&mut *blocker).await?;
        let metrics = Metrics::default();
        let mut attempt = Box::pin(advance_with_metrics(pool, 10, Some(&metrics)));
        tokio::select! {
            result = &mut attempt => anyhow::bail!("rollup completed before SQL barrier: {result:?}"),
            result = wait_for_sql(admin, application) => result?,
        }
        assert_eq!(counts(&metrics), (1., 0.));
        let acquired = family(&metrics);
        drop(attempt);
        blocker.commit().await?;
        assert_reusable(pool).await?;
        assert_empty(pool).await?;
        assert_eq!(family(&metrics), acquired);
        Ok(())
    })).await
}

#[tokio::test]
async fn postgres_run_shutdown_cancels_checkout_once() -> Result<()> {
    with_database(|pool, _, _| Box::pin(async move {
        let held = pool.acquire().await?;
        let metrics = Arc::new(Metrics::default());
        let (stop, shutdown) = watch::channel(false);
        let mut task = Box::pin(run_with_metrics(pool.clone(), Settings { batch: 1, interval: Duration::from_secs(15) }, shutdown, Some(metrics.clone())));
        tokio::select! {
            result = &mut task => anyhow::bail!("rollup loop exited before shutdown: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(25)) => {}
        }
        assert_eq!(counts(&metrics), (0., 0.));
        stop.send_replace(true);
        tokio::time::timeout(BOUND, task).await??;
        assert_eq!(counts(&metrics), (0., 1.));
        let cancelled = family(&metrics);
        drop(held);
        assert_reusable(pool).await?;
        assert_eq!(family(&metrics), cancelled);
        Ok(())
    })).await
}

#[tokio::test]
async fn postgres_run_shutdown_during_sql_preserves_success_and_compatibility() -> Result<()> {
    with_database(|pool, admin, application| Box::pin(async move {
        insert_share(admin).await?;
        for attached in [true, false] {
            let mut blocker = admin.begin().await?;
            sqlx::query("LOCK TABLE qbit_hashrate_rollup_pool IN ACCESS EXCLUSIVE MODE").execute(&mut *blocker).await?;
            let metrics = Arc::new(Metrics::default());
            let (stop, shutdown) = watch::channel(false);
            let settings = Settings { batch: 10, interval: Duration::from_secs(15) };
            let mut task: LocalBoxFuture<'_, Result<()>> = if attached {
                Box::pin(run_with_metrics(pool.clone(), settings, shutdown, Some(metrics.clone())))
            } else {
                Box::pin(run(pool.clone(), settings, shutdown))
            };
            tokio::select! {
                result = &mut task => anyhow::bail!("rollup loop exited before SQL barrier: {result:?}"),
                result = wait_for_sql(admin, application) => result?,
            }
            assert_eq!(counts(&metrics), (if attached { 1. } else { 0. }, 0.));
            let acquired = family(&metrics);
            stop.send_replace(true);
            tokio::time::timeout(BOUND, task).await??;
            blocker.commit().await?;
            assert_reusable(pool).await?;
            assert_empty(pool).await?;
            assert_eq!(family(&metrics), acquired);
        }
        Ok(())
    })).await
}
