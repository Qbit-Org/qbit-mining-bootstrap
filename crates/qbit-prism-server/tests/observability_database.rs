//! Selected by the required PostgreSQL job with --ignored. No missing-DSN pass.
use anyhow::{Context, Result};
use qbit_prism_server::{
    ledger::Ledger,
    metrics::{collectors, Metrics},
};
use sqlx::PgPool;
use std::time::{Duration, Instant};

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; required CI runs --test observability_database -- --ignored"]
async fn collector_uses_real_schema_pending_rows_and_failed_read_semantics() -> Result<()> {
    let raw = std::env::var("PRISM_TEST_DATABASE_URL").context(
        "PRISM_TEST_DATABASE_URL is required for this explicitly selected database test",
    )?;
    let admin = PgPool::connect(&raw).await?;
    let schema = format!("prism_metrics_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await?;
    let mut url = url::Url::parse(&raw)?;
    url.query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let ledger = Ledger::connect(url.as_str(), "metrics-test".into(), 4, true).await?;
    let result = check(&ledger).await;
    ledger.pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await?;
    admin.close().await;
    result
}

async fn check(ledger: &Ledger) -> Result<()> {
    let metrics = Metrics::default();
    assert_eq!(
        sample(&metrics.render(), "qbit_prism_block_candidates_pending"),
        -1.
    );
    metrics.publish_database(collectors::database(&ledger.pool, &metrics).await.ok());
    assert_eq!(
        sample(&metrics.render(), "qbit_prism_block_candidates_pending"),
        0.
    );
    assert_eq!(
        sample(
            &metrics.render(),
            "qbit_prism_collector_success{collector=\"database\"}"
        ),
        1.
    );
    // Only metadata enters the collector; it must never decode candidate JSON.
    sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,created_at) VALUES($1,'{}',$1,clock_timestamp()-interval '5 seconds')")
        .bind("11".repeat(32)).execute(&ledger.pool).await?;
    for (hash, state) in [
        ("22".repeat(32), "submitted"),
        ("33".repeat(32), "abandoned"),
    ] {
        sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,state,completed_at) VALUES($1,NULL,$1,$2,clock_timestamp())")
            .bind(hash).bind(state).execute(&ledger.pool).await?;
    }
    let snapshot = collectors::database(&ledger.pool, &metrics).await?;
    assert_eq!(snapshot.candidates, 1);
    assert!(snapshot.candidate_oldest >= Duration::from_secs(5));
    metrics.publish_database(Some(snapshot));
    let good = metrics.render();
    assert_eq!(sample(&good, "qbit_prism_block_candidates_pending"), 1.);
    let mut blocker = ledger.pool.begin().await?;
    sqlx::query("LOCK TABLE qbit_block_candidate_outbox IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await?;
    let start = Instant::now();
    let error = collectors::database(&ledger.pool, &metrics)
        .await
        .err()
        .context("collector should fail its lock timeout")?;
    assert!(start.elapsed() < Duration::from_secs(3));
    let sql_error = error
        .downcast_ref::<sqlx::Error>()
        .context("expected PostgreSQL lock timeout")?;
    assert_eq!(
        sql_error
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("55P03")
    );
    metrics.publish_database(None);
    let failed = metrics.render();
    assert_eq!(sample(&failed, "qbit_prism_block_candidates_pending"), -1.);
    assert_eq!(
        sample(&failed, "qbit_prism_block_candidate_oldest_pending_seconds"),
        -1.
    );
    assert!(
        sample(
            &failed,
            "qbit_prism_collector_age_seconds{collector=\"database\"}"
        ) >= 0.4
    );
    assert_eq!(
        sample(
            &failed,
            "qbit_prism_collector_success{collector=\"database\"}"
        ),
        0.
    );
    blocker.rollback().await?;
    sqlx::query("UPDATE qbit_block_candidate_outbox SET state='abandoned',candidate=NULL,completed_at=clock_timestamp() WHERE state='pending'").execute(&ledger.pool).await?;
    let empty = collectors::database(&ledger.pool, &metrics).await?;
    assert_eq!(empty.candidates, 0);
    assert_eq!(empty.candidate_oldest, Duration::ZERO);
    metrics.publish_database(Some(empty));
    assert_eq!(
        sample(&metrics.render(), "qbit_prism_block_candidates_pending"),
        0.
    );
    assert_eq!(
        sample(
            &metrics.render(),
            "qbit_prism_collector_success{collector=\"database\"}"
        ),
        1.
    );
    // Exhaust the pool so the overall deadline cancels acquisition, then
    // prove cancellation released its waiter and the pool remains usable.
    let mut held = Vec::new();
    for _ in 0..4 {
        held.push(ledger.pool.acquire().await?);
    }
    let attempt = metrics.begin_collection(qbit_prism_server::metrics::Collector::Database);
    let started = Instant::now();
    assert!(collectors::database(&ledger.pool, &metrics).await.is_err());
    assert!(started.elapsed() < Duration::from_secs(4));
    attempt.publish_database(None);
    assert_eq!(
        sample(&metrics.render(), "qbit_prism_block_candidates_pending"),
        -1.
    );
    assert_eq!(
        sample(
            &metrics.render(),
            "qbit_prism_collector_success{collector=\"database\"}"
        ),
        0.
    );
    drop(held);
    let recovered = tokio::time::timeout(
        Duration::from_secs(1),
        collectors::database(&ledger.pool, &metrics),
    )
    .await??;
    assert_eq!(recovered.candidates, 0);
    assert!(
        sample(
            &metrics.render(),
            "qbit_prism_database_pool_acquire_seconds_count{result=\"success\"}"
        ) >= 1.
    );
    Ok(())
}

fn sample(body: &str, key: &str) -> f64 {
    body.lines()
        .find_map(|line| line.strip_prefix(&format!("{key} ")))
        .unwrap()
        .parse()
        .unwrap()
}
