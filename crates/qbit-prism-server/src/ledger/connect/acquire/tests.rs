use super::*;
use std::{sync::Arc, time::Duration};

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

#[tokio::test]
async fn success_preserves_value_and_stops_before_later_work() {
    let metrics = Metrics::default();
    let value = Box::new(42);
    let address = std::ptr::from_ref(&*value);
    let result = observe(Some(&metrics), async { Ok(value) }).await.unwrap();
    assert_eq!(std::ptr::from_ref(&*result), address);
    assert_eq!(counts(&metrics), (1., 0.));
    let sum = sample(&metrics, "success", "sum");
    tokio::time::sleep(Duration::from_millis(10)).await;
    drop(result);
    assert_eq!(counts(&metrics), (1., 0.));
    assert_eq!(sample(&metrics, "success", "sum"), sum);
}

#[tokio::test]
async fn failure_preserves_original_error() {
    let metrics = Metrics::default();
    let error = sqlx::Error::Io(std::io::Error::from_raw_os_error(123));
    let error = observe::<()>(Some(&metrics), async { Err(error) })
        .await
        .unwrap_err();
    assert!(matches!(error, sqlx::Error::Io(ref error) if error.raw_os_error() == Some(123)));
    assert_eq!(counts(&metrics), (0., 1.));
    let sum = sample(&metrics, "failure", "sum");
    drop(error);
    assert_eq!(sample(&metrics, "failure", "sum"), sum);
}

#[tokio::test]
async fn dropping_a_polled_wait_records_once_and_drops_the_operation() {
    let metrics = Metrics::default();
    let (sender, receiver) = tokio::sync::oneshot::channel::<()>();
    let started = std::time::Instant::now();
    let mut future = Box::pin(observe(Some(&metrics), async move {
        receiver.await.unwrap();
        Ok(())
    }));
    assert!(futures_util::poll!(&mut future).is_pending());
    let waiting = std::time::Instant::now();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let waited = waiting.elapsed();
    drop(future);
    let elapsed = started.elapsed();
    assert!(
        sender.is_closed(),
        "cancelled acquisition must release its resources"
    );
    assert_eq!(counts(&metrics), (0., 1.));
    let sum = sample(&metrics, "failure", "sum");
    assert!((waited.as_secs_f64()..=elapsed.as_secs_f64()).contains(&sum));
    assert_eq!(counts(&metrics), (0., 1.));
    assert_eq!(sample(&metrics, "failure", "sum"), sum);
}

#[tokio::test]
async fn dropping_an_unpolled_wait_records_nothing() {
    let metrics = Metrics::default();
    let future = observe(Some(&metrics), std::future::pending::<sqlx::Result<()>>());
    drop(future);
    assert_eq!(counts(&metrics), (0., 0.));
}

#[tokio::test]
async fn unattached_observation_preserves_both_results() {
    assert_eq!(observe(None, async { Ok(42) }).await.unwrap(), 42);
    assert!(matches!(
        observe::<()>(None, async { Err(sqlx::Error::PoolClosed) }).await,
        Err(sqlx::Error::PoolClosed)
    ));
}

#[tokio::test]
async fn concurrent_observations_keep_every_outcome() {
    let metrics = Arc::new(Metrics::default());
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..32 {
        let metrics = metrics.clone();
        tasks.spawn(async move {
            observe(Some(&metrics), async {
                tokio::task::yield_now().await;
                if index % 2 == 0 {
                    Ok(())
                } else {
                    Err(sqlx::Error::PoolClosed)
                }
            })
            .await
        });
    }
    while let Some(result) = tasks.join_next().await {
        let _ = result.unwrap();
    }
    assert_eq!(counts(&metrics), (16., 16.));
}

#[tokio::test]
async fn postgres_checkout_boundaries_and_callers() -> anyhow::Result<()> {
    use qbit_prism_test_gate as gate;
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let admin = PgPool::connect(&raw).await?;
    let schema = format!("prism_acquire_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await?;
    let mut url = url::Url::parse(&raw)?;
    url.query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let mut pools = Vec::new();
    let result = postgres_cases(url.as_str(), &mut pools).await;
    for pool in pools {
        pool.close().await;
    }
    let cleanup = sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await;
    admin.close().await;
    result.and(cleanup.map(|_| ()).map_err(Into::into))
}

async fn postgres_cases(url: &str, pools: &mut Vec<PgPool>) -> anyhow::Result<()> {
    let metrics = Arc::new(Metrics::default());
    let mut ledger =
        Ledger::connect_with_metrics(url, "acquire-test".into(), 2, true, Some(metrics.clone()))
            .await?;
    pools.push(ledger.pool.clone());
    // A single slot makes any accidental retained checkout block the next
    // transaction instead of being hidden by another available connection.
    let single = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await?;
    pools.push(single.clone());
    let old = std::mem::replace(&mut ledger.pool, single);
    old.close().await;

    let before = counts(&metrics);
    tokio::time::timeout(Duration::from_secs(3), async {
        assert!(ledger
            .worker_difficulty("test", "missing", 60)
            .await?
            .is_none());
        assert!(ledger.share_accepted_at_ms("missing").await?.is_none());
        assert!(ledger.cpfp_package("missing").await?.is_none());
        assert!(ledger.retired_cpfp_funding("missing").await?.is_empty());
        assert_eq!(ledger.fatal_state().await?["halted"], false);
        ledger
            .heartbeat(crate::ledger::HeartbeatStatus::Starting)
            .await?;
        ledger.release_session_owner_reservations().await?;
        assert_eq!(ledger.payout_revision().await?, 0);
        assert_eq!(counts(&metrics), (before.0 + 8., before.1));
        let signer_keys = crate::ledger::SignerKeys {
            manifest_key_hex: "11".repeat(32),
            ledger_key_hex: "22".repeat(32),
        };
        ledger.configure("acquire-test", &signer_keys).await?;
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    assert_eq!(counts(&metrics), (before.0 + 9., before.1));

    let held = ledger.pool.acquire().await?;
    let before = counts(&metrics);
    let before_sum = sample(&metrics, "success", "sum");
    let mut acquisition = Box::pin(ledger.acquire());
    assert!(futures_util::poll!(&mut acquisition).is_pending());
    let waiting = std::time::Instant::now();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let waited = waiting.elapsed();
    drop(held);
    let mut connection = tokio::time::timeout(Duration::from_secs(3), acquisition).await??;
    assert_eq!(counts(&metrics), (before.0 + 1., before.1));
    assert!(sample(&metrics, "success", "sum") - before_sum >= waited.as_secs_f64());

    // Slow SQL and query failure must not extend acquisition timing or turn a
    // successful checkout into a failed one.
    let acquired_sum = sample(&metrics, "success", "sum");
    sqlx::query("SELECT pg_sleep(0.1)")
        .execute(&mut *connection)
        .await?;
    let error = sqlx::query("SELECT 1 / 0")
        .execute(&mut *connection)
        .await
        .unwrap_err();
    assert_eq!(
        error
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("22012")
    );
    assert_eq!(counts(&metrics), (before.0 + 1., before.1));
    assert_eq!(sample(&metrics, "success", "sum"), acquired_sum);
    drop(connection);

    let held = ledger.pool.acquire().await?;
    let before = counts(&metrics);
    let before_sum = sample(&metrics, "failure", "sum");
    let started = std::time::Instant::now();
    let error = tokio::time::timeout(Duration::from_millis(75), ledger.acquire())
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "deadline has elapsed");
    let elapsed = started.elapsed();
    assert_eq!(counts(&metrics), (before.0, before.1 + 1.));
    let failed_sum = sample(&metrics, "failure", "sum");
    assert!((0.05..=elapsed.as_secs_f64()).contains(&(failed_sum - before_sum)));
    drop(held);
    drop(tokio::time::timeout(Duration::from_secs(3), ledger.acquire()).await??);
    assert_eq!(counts(&metrics), (before.0 + 1., before.1 + 1.));
    assert_eq!(sample(&metrics, "failure", "sum"), failed_sum);

    // Cancelling a query after checkout retains its successful acquisition
    // outcome, and dropping the connection leaves the pool usable.
    let before = counts(&metrics);
    let mut connection = ledger.acquire().await?;
    let acquired_sum = sample(&metrics, "success", "sum");
    assert!(tokio::time::timeout(
        Duration::from_millis(50),
        sqlx::query("SELECT pg_sleep(0.2)").execute(&mut *connection)
    )
    .await
    .is_err());
    drop(connection);
    assert_eq!(counts(&metrics), (before.0 + 1., before.1));
    assert_eq!(sample(&metrics, "success", "sum"), acquired_sum);
    let mut transaction = tokio::time::timeout(Duration::from_secs(3), ledger.begin()).await??;
    let before = counts(&metrics);
    let acquired_sum = sample(&metrics, "success", "sum");
    sqlx::query("SELECT pg_sleep(0.05)")
        .execute(&mut *transaction)
        .await?;
    transaction.rollback().await?;
    assert_eq!(counts(&metrics), before);
    assert_eq!(sample(&metrics, "success", "sum"), acquired_sum);

    ledger.pool.close().await;
    let before = counts(&metrics);
    assert!(matches!(
        ledger.acquire().await,
        Err(sqlx::Error::PoolClosed)
    ));
    assert_eq!(counts(&metrics), (before.0, before.1 + 1.));
    Ok(())
}
