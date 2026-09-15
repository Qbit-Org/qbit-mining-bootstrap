use super::*;

fn sample(metrics: &Metrics, outcome: &str, suffix: &str) -> f64 {
    let key = format!("qbit_prism_database_pool_acquire_seconds_{suffix}{{result=\"{outcome}\"}} ");
    let body = metrics.render();
    let values: Vec<_> = body
        .lines()
        .filter_map(|line| line.strip_prefix(&key))
        .collect();
    assert_eq!(values.len(), 1, "expected one sample for {key}");
    values[0].parse().unwrap()
}

fn assert_observations(metrics: &Metrics, success: (f64, f64), failure: (f64, f64)) {
    for (outcome, (count, sum)) in [("success", success), ("failure", failure)] {
        assert_eq!(sample(metrics, outcome, "count"), count);
        assert_eq!(sample(metrics, outcome, "sum"), sum);
    }
}

fn counts(metrics: &Metrics) -> (f64, f64) {
    (
        sample(metrics, "success", "count"),
        sample(metrics, "failure", "count"),
    )
}

#[tokio::test(start_paused = true)]
async fn cancelled_acquisition_records_one_failure_with_elapsed_wait() {
    let metrics = Metrics::default();
    let error = tokio::time::timeout(
        Duration::from_secs(3),
        time_pool_acquire(Some(&metrics), std::future::pending::<sqlx::Result<()>>()),
    )
    .await
    .unwrap_err();
    assert_eq!(error.to_string(), "deadline has elapsed");
    assert_observations(&metrics, (0., 0.), (1., 3.));
    tokio::time::advance(Duration::from_secs(7)).await;
    assert_observations(&metrics, (0., 0.), (1., 3.));
}

#[tokio::test(start_paused = true)]
async fn successful_acquisition_records_once_and_preserves_returned_value() {
    let metrics = Metrics::default();
    let value = Box::new(42);
    let address = std::ptr::from_ref(&*value);
    let value = time_pool_acquire(Some(&metrics), async {
        tokio::time::sleep(Duration::from_millis(125)).await;
        Ok(value)
    })
    .await
    .unwrap();
    assert_eq!(*value, 42);
    assert_eq!(std::ptr::from_ref(&*value), address);
    assert_observations(&metrics, (1., 0.125), (0., 0.));
    tokio::time::advance(Duration::from_secs(7)).await;
    drop(value);
    assert_observations(&metrics, (1., 0.125), (0., 0.));
}

#[tokio::test(start_paused = true)]
async fn failed_acquisition_records_once_and_preserves_returned_error() {
    let metrics = Metrics::default();
    let error = sqlx::Error::Io(std::io::Error::from_raw_os_error(123));
    let error = time_pool_acquire(Some(&metrics), async {
        tokio::time::sleep(Duration::from_millis(250)).await;
        Err::<(), _>(error)
    })
    .await
    .unwrap_err();
    assert!(matches!(error, sqlx::Error::Io(ref error) if error.raw_os_error() == Some(123)));
    assert_observations(&metrics, (0., 0.), (1., 0.25));
    tokio::time::advance(Duration::from_secs(7)).await;
    drop(error);
    assert_observations(&metrics, (0., 0.), (1., 0.25));
}

#[tokio::test(start_paused = true)]
async fn cancellation_after_acquisition_does_not_extend_wait_or_record_failure() {
    let metrics = Metrics::default();
    assert!(tokio::time::timeout(Duration::from_secs(3), async {
        time_pool_acquire(Some(&metrics), async {
            tokio::time::sleep(Duration::from_millis(125)).await;
            Ok(())
        })
        .await
        .unwrap();
        std::future::pending::<()>().await;
    })
    .await
    .is_err());
    assert_observations(&metrics, (1., 0.125), (0., 0.));
}

#[tokio::test(start_paused = true)]
async fn unpolled_acquisition_does_not_fabricate_an_observation() {
    let metrics = Metrics::default();
    let acquisition = time_pool_acquire(Some(&metrics), std::future::pending::<sqlx::Result<()>>());
    tokio::time::advance(Duration::from_secs(3)).await;
    drop(acquisition);
    assert_observations(&metrics, (0., 0.), (0., 0.));
}

#[tokio::test(start_paused = true)]
async fn acquisition_clock_starts_at_first_poll_not_future_construction() {
    let metrics = Metrics::default();
    let acquisition = time_pool_acquire(Some(&metrics), async {
        tokio::time::sleep(Duration::from_millis(125)).await;
        Ok(42)
    });
    tokio::time::advance(Duration::from_secs(30)).await;
    assert_observations(&metrics, (0., 0.), (0., 0.));
    assert_eq!(acquisition.await.unwrap(), 42);
    assert_observations(&metrics, (1., 0.125), (0., 0.));
}

#[tokio::test]
async fn cancelling_an_unattached_acquisition_drops_its_resources() {
    let (sender, receiver) = tokio::sync::oneshot::channel::<()>();
    let mut acquisition = Box::pin(time_pool_acquire(None, async move {
        receiver.await.unwrap();
        Ok(())
    }));
    assert!(futures_util::poll!(&mut acquisition).is_pending());
    drop(acquisition);
    assert!(sender.is_closed());
}

#[tokio::test]
async fn dropping_a_polled_wait_records_once_and_drops_the_operation() {
    let metrics = Metrics::default();
    let (sender, receiver) = tokio::sync::oneshot::channel::<()>();
    let started = std::time::Instant::now();
    let mut future = Box::pin(time_pool_acquire(Some(&metrics), async move {
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
async fn unattached_observation_preserves_both_results() {
    assert_eq!(time_pool_acquire(None, async { Ok(42) }).await.unwrap(), 42);
    assert!(matches!(
        time_pool_acquire::<()>(None, async { Err(sqlx::Error::PoolClosed) }).await,
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
            time_pool_acquire(Some(&metrics), async {
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
