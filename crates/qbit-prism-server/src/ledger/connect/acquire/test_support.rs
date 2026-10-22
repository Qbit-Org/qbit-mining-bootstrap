//! Shared assertions and explicit database ownership for checkout tests.
use crate::metrics::Metrics;
use anyhow::Result;
use futures_util::{future::LocalBoxFuture, FutureExt};
use sqlx::PgPool;
use std::panic::{resume_unwind, AssertUnwindSafe};

pub(super) fn sample(metrics: &Metrics, outcome: &str, suffix: &str) -> f64 {
    let key = format!("qbit_prism_database_pool_acquire_seconds_{suffix}{{result=\"{outcome}\"}} ");
    let body = metrics.render();
    let values: Vec<_> = body
        .lines()
        .filter_map(|line| line.strip_prefix(&key))
        .collect();
    assert_eq!(values.len(), 1, "expected one rendered series for {key}");
    values[0].parse().unwrap()
}

pub(super) fn counts(metrics: &Metrics) -> (f64, f64) {
    (
        sample(metrics, "success", "count"),
        sample(metrics, "failure", "count"),
    )
}

pub(super) fn family(metrics: &Metrics) -> Vec<String> {
    metrics
        .render()
        .lines()
        .filter(|line| line.contains("qbit_prism_database_pool_acquire_seconds"))
        .map(str::to_owned)
        .collect()
}

pub(super) async fn with_database<F>(case: F) -> Result<()>
where
    F: for<'a> FnOnce(&'a str, &'a mut Vec<PgPool>) -> LocalBoxFuture<'a, Result<()>>,
{
    use qbit_prism_test_gate as gate;
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let schema = format!("prism_acquire_{}", uuid::Uuid::new_v4().simple());
    with_schema(&raw, &schema, case).await
}

// Register each pool immediately after opening it, including during fixture
// setup. The owner closes all registered pools and drops the schema after the
// case has unwound, preserving the original error or panic if cleanup fails.
async fn with_schema<F>(raw: &str, schema: &str, case: F) -> Result<()>
where
    F: for<'a> FnOnce(&'a str, &'a mut Vec<PgPool>) -> LocalBoxFuture<'a, Result<()>>,
{
    let mut url = url::Url::parse(raw)?;
    url.query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let admin = PgPool::connect(raw).await?;
    let mut pools = Vec::new();
    let result = AssertUnwindSafe(async {
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        case(url.as_str(), &mut pools).await
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
        eprintln!("checkout test schema cleanup failed for {schema}: {error}");
    }
    match result {
        Ok(result) => result.and(cleanup.map(|_| ()).map_err(Into::into)),
        Err(panic) => resume_unwind(panic),
    }
}

#[tokio::test]
async fn postgres_fixture_cleans_up_after_setup_error_and_body_panic() -> Result<()> {
    use qbit_prism_test_gate as gate;
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let admin = PgPool::connect(&raw).await?;
    for panic in [false, true] {
        let schema = format!("prism_acquire_cleanup_{}", uuid::Uuid::new_v4().simple());
        let result = AssertUnwindSafe(with_schema(&raw, &schema, |url, pools| {
            Box::pin(async move {
                let pool = PgPool::connect(url).await?;
                pools.push(pool.clone());
                if !panic {
                    // Fail before fixture initialization is complete.
                    anyhow::bail!("injected setup error");
                }
                sqlx::query("CREATE TABLE cleanup_probe (expires_at bigint)")
                    .execute(&pool)
                    .await?;
                sqlx::query("ALTER TABLE cleanup_probe RENAME COLUMN expires_at TO hidden_expiry")
                    .execute(&pool)
                    .await?;
                panic!("injected body assertion");
            })
        }))
        .catch_unwind()
        .await;
        if panic {
            assert_eq!(
                result.unwrap_err().downcast_ref::<&str>(),
                Some(&"injected body assertion")
            );
        } else {
            assert_eq!(
                result.unwrap().unwrap_err().to_string(),
                "injected setup error"
            );
        }
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT FROM pg_namespace WHERE nspname=$1)")
                .bind(&schema)
                .fetch_one(&admin)
                .await?;
        assert!(!exists, "fixture left schema {schema} behind");
    }
    admin.close().await;
    Ok(())
}
