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

#[tokio::test(start_paused = true)]
async fn cancelled_acquisition_records_one_failure_with_elapsed_wait() {
    let metrics = Metrics::default();
    let error = tokio::time::timeout(
        Duration::from_secs(3),
        observe_pool_acquire(Some(&metrics), std::future::pending::<sqlx::Result<()>>()),
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
    let value = observe_pool_acquire(Some(&metrics), async {
        tokio::time::sleep(Duration::from_millis(125)).await;
        Ok(Box::new(42))
    })
    .await
    .unwrap();
    assert_eq!(*value, 42);
    assert_observations(&metrics, (1., 0.125), (0., 0.));
    tokio::time::advance(Duration::from_secs(7)).await;
    drop(value);
    assert_observations(&metrics, (1., 0.125), (0., 0.));
}

#[tokio::test(start_paused = true)]
async fn failed_acquisition_records_once_and_preserves_returned_error() {
    let metrics = Metrics::default();
    let error = observe_pool_acquire(Some(&metrics), async {
        tokio::time::sleep(Duration::from_millis(250)).await;
        Err::<(), _>(sqlx::Error::PoolClosed)
    })
    .await
    .unwrap_err();
    assert!(matches!(error, sqlx::Error::PoolClosed));
    assert_observations(&metrics, (0., 0.), (1., 0.25));
    tokio::time::advance(Duration::from_secs(7)).await;
    drop(error);
    assert_observations(&metrics, (0., 0.), (1., 0.25));
}

#[tokio::test(start_paused = true)]
async fn cancellation_after_acquisition_does_not_extend_wait_or_record_failure() {
    let metrics = Metrics::default();
    assert!(tokio::time::timeout(Duration::from_secs(3), async {
        observe_pool_acquire(Some(&metrics), async {
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
    let acquisition =
        observe_pool_acquire(Some(&metrics), std::future::pending::<sqlx::Result<()>>());
    tokio::time::advance(Duration::from_secs(3)).await;
    drop(acquisition);
    assert_observations(&metrics, (0., 0.), (0., 0.));
}

#[tokio::test(start_paused = true)]
async fn acquisition_clock_starts_at_first_poll_not_future_construction() {
    let metrics = Metrics::default();
    let acquisition = observe_pool_acquire(Some(&metrics), async {
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
    let mut acquisition = Box::pin(observe_pool_acquire(None, async move {
        receiver.await.unwrap();
        Ok(())
    }));
    assert!(futures_util::poll!(&mut acquisition).is_pending());
    drop(acquisition);
    assert!(sender.is_closed());
}
