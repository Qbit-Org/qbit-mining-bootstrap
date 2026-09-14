//! Selected by the required PostgreSQL job with --ignored. No missing-DSN pass.
use anyhow::{ensure, Context, Result};
use qbit_prism_server::{
    api::{router, ApiConfig, ApiState},
    ledger::Ledger,
    metrics::{collectors, Metrics},
};
use qbit_prism_test_gate as gate;
use sqlx::PgPool;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; required CI runs --test observability_database -- --ignored"]
async fn live_pool_waits_survive_blocked_publication_through_http() -> Result<()> {
    let raw = gate::required_database_url(gate::site!())?;
    let admin = PgPool::connect(&raw).await?;
    let schema = format!("prism_live_pool_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await?;
    let mut url = url::Url::parse(&raw)?;
    url.query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let metrics = Arc::new(Metrics::default());
    let ledger = Ledger::connect_with_metrics(
        url.as_str(),
        "live-pool-test".into(),
        4,
        true,
        Some(metrics.clone()),
    )
    .await?;
    let result = check_live_pool_scrapes(&ledger, metrics).await;
    ledger.pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await?;
    admin.close().await;
    result
}

async fn check_live_pool_scrapes(ledger: &Ledger, metrics: Arc<Metrics>) -> Result<()> {
    let state = ApiState::new(ledger.pool.clone(), ApiConfig::default(), metrics.clone());
    metrics.publish_database(Some(collectors::database(&ledger.pool, &metrics).await?));
    state.publish_metrics(metrics.render())?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}/metrics", listener.local_addr()?);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()?;
    let mut held = Vec::new();
    for _ in 0..4 {
        held.push(ledger.pool.acquire().await?);
    }
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let app = router(state.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    let publisher_ledger = ledger.clone();
    let publisher_state = state.clone();
    let publisher_metrics = metrics.clone();
    // Exercise the real SQL dependencies in server::publish_health order:
    // health's payout_revision read, publication, heartbeat, then pruning.
    // Under exhaustion each acquisition waits up to 15 seconds. This isolates
    // that publisher dependency without requiring a node or mining work.
    let publisher = tokio::spawn(async move {
        let _ = publisher_ledger.payout_revision().await;
        publisher_state.publish_metrics(publisher_metrics.render())?;
        let _ = publisher_ledger
            .heartbeat(serde_json::json!({"ok": false}))
            .await;
        let _ = publisher_ledger.prune_expired_jobs().await;
        anyhow::Ok(())
    });
    let result = async {
        let initial = client.get(&endpoint).send().await?.error_for_status()?.text().await?;
        let baseline = unique_pool_sample(&initial, "failure", "count")?;
        let baseline_sum = unique_pool_sample(&initial, "failure", "sum")?;
        let mut saw_stale = false;
        // Real collector deadlines and cancelled instrumented ledger waits
        // continue for >30 seconds, across a cached-body stale boundary.
        for attempt in 1..=9 {
            ensure!(tokio::time::timeout(Duration::from_millis(750), ledger.configure("live-pool-test")).await.is_err(),
                "held pool must block the instrumented ledger acquisition");
            let collection = metrics.begin_collection(qbit_prism_server::metrics::Collector::Database);
            ensure!(collectors::database(&ledger.pool, &metrics).await.is_err(), "held pool must fail collection");
            collection.publish_database(None);
            ensure!(!publisher.is_finished(), "publisher dependencies should still be blocked");
            let response = client.get(&endpoint).send().await?.error_for_status()?;
            ensure!(response.headers()["cache-control"] == "no-store");
            let text = response.text().await?;
            let count = unique_pool_sample(&text, "failure", "count")?;
            ensure!(count == baseline + f64::from(attempt * 2), "live HTTP count must include each ledger and collector cancellation: {count}");
            ensure!(unique_pool_sample(&text, "failure", "sum")? >= baseline_sum + f64::from(attempt) * 3.75);
            ensure!(sample(&text, "qbit_prism_database_pool_acquire_seconds_bucket{result=\"failure\",le=\"+Inf\"}") == count);
            ensure!(sample(&text, "qbit_prism_collector_available{collector=\"database\"}") == 0.);
            ensure!(sample(&text, "qbit_prism_block_candidates_pending") == -1.);
            if sample(&text, "qbit_prism_metrics_snapshot_stale") == 1. {
                saw_stale = true;
                ensure!(sample(&text, "qbit_prism_metrics_snapshot_age_seconds") > 15.);
                ensure!(sample(&text, "qbit_prism_health_state") == 0.);
            }
        }
        ensure!(saw_stale, "real publication must age stale while HTTP still sees new pool observations");
        drop(held);
        // Abandoning failed acquisitions must leave the pool reusable, and a
        // successful zero-row collection must be a real zero after recovery.
        let recovered = tokio::time::timeout(Duration::from_secs(2), collectors::database(&ledger.pool, &metrics)).await??;
        metrics.publish_database(Some(recovered));
        state.publish_metrics(metrics.render())?;
        let text = client.get(&endpoint).send().await?.error_for_status()?.text().await?;
        ensure!(sample(&text, "qbit_prism_block_candidates_pending") == 0.);
        ensure!(sample(&text, "qbit_prism_collector_available{collector=\"database\"}") == 1.);
        ensure!(sample(&text, "qbit_prism_metrics_snapshot_stale") == 0.);
        ensure!(unique_pool_sample(&text, "failure", "count")? == baseline + 18.);
        Ok(())
    }.await;
    publisher.abort();
    let _ = publisher.await;
    let _ = stop.send(());
    server.await??;
    result
}

fn unique_pool_sample(body: &str, outcome: &str, suffix: &str) -> Result<f64> {
    let key = format!("qbit_prism_database_pool_acquire_seconds_{suffix}{{result=\"{outcome}\"}} ");
    let values: Vec<_> = body
        .lines()
        .filter_map(|line| line.strip_prefix(&key))
        .collect();
    ensure!(
        values.len() == 1,
        "expected exactly one live sample for {key}"
    );
    Ok(values[0].parse()?)
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; required CI runs --test observability_database -- --ignored"]
async fn collector_uses_real_schema_pending_rows_and_failed_read_semantics() -> Result<()> {
    let raw = gate::required_database_url(gate::site!())?;
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
    // Query failure follows a successful acquisition, and must not turn its
    // observation into a failure or include the half-second query lock wait.
    assert_eq!(pool_sample(&metrics, "success", "count"), 3.);
    assert_eq!(pool_sample(&metrics, "failure", "count"), 0.);
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
    let successful_acquires = pool_sample(&metrics, "success", "count");
    let successful_wait = pool_sample(&metrics, "success", "sum");
    assert_eq!(successful_acquires, 4.);
    assert_eq!(pool_sample(&metrics, "failure", "count"), 0.);
    let attempt = metrics.begin_collection(qbit_prism_server::metrics::Collector::Database);
    let started = Instant::now();
    let error = collectors::database(&ledger.pool, &metrics)
        .await
        .err()
        .context("collector should exhaust its acquisition deadline")?;
    let elapsed = started.elapsed();
    assert!(elapsed >= Duration::from_secs(3));
    assert!(elapsed < Duration::from_secs(4));
    assert_eq!(
        error.to_string(),
        "metrics database collection deadline exceeded"
    );
    assert!(error
        .downcast_ref::<tokio::time::error::Elapsed>()
        .is_some());
    assert_eq!(pool_sample(&metrics, "failure", "count"), 1.);
    let failed_wait = pool_sample(&metrics, "failure", "sum");
    assert!(failed_wait >= 3.);
    assert!(failed_wait <= elapsed.as_secs_f64());
    assert_eq!(
        pool_sample(&metrics, "success", "count"),
        successful_acquires
    );
    assert_eq!(pool_sample(&metrics, "success", "sum"), successful_wait);
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
    assert_eq!(
        pool_sample(&metrics, "success", "count"),
        successful_acquires + 1.
    );
    assert!(pool_sample(&metrics, "success", "sum") >= successful_wait);
    assert_eq!(pool_sample(&metrics, "failure", "count"), 1.);
    assert_eq!(pool_sample(&metrics, "failure", "sum"), failed_wait);

    // A completed SQLx acquisition error is preserved and also recorded once.
    let closed_pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://unused@127.0.0.1:1/unused")?;
    closed_pool.close().await;
    let started = Instant::now();
    let error = collectors::database(&closed_pool, &metrics)
        .await
        .err()
        .context("collector should preserve a closed pool error")?;
    assert!(matches!(
        error.downcast_ref::<sqlx::Error>(),
        Some(sqlx::Error::PoolClosed)
    ));
    assert_eq!(pool_sample(&metrics, "failure", "count"), 2.);
    let completed_failure_wait = pool_sample(&metrics, "failure", "sum") - failed_wait;
    assert!(completed_failure_wait >= 0.);
    assert!(completed_failure_wait <= started.elapsed().as_secs_f64());
    assert_eq!(
        pool_sample(&metrics, "success", "count"),
        successful_acquires + 1.
    );
    Ok(())
}

fn pool_sample(metrics: &Metrics, outcome: &str, suffix: &str) -> f64 {
    sample(
        &metrics.render(),
        &format!("qbit_prism_database_pool_acquire_seconds_{suffix}{{result=\"{outcome}\"}}"),
    )
}

fn sample(body: &str, key: &str) -> f64 {
    body.lines()
        .find_map(|line| line.strip_prefix(&format!("{key} ")))
        .unwrap()
        .parse()
        .unwrap()
}
