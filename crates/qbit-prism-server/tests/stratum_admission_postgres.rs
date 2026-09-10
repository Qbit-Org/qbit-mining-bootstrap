//! Explicit real-PostgreSQL acceptance: never silently passes without a database.
#[path = "support/stratum_admission.rs"]
mod support;
use anyhow::{Context, Result};
use qbit_prism_server::{ledger::Ledger, stratum::StratumConfig};
use serde_json::json;
use std::sync::{atomic::Ordering, Arc};
use support::{Backend, Client, Server};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable PRISM_TEST_DATABASE_URL; run explicitly in database CI"]
async fn ten_thousand_unsubscribed_connections_do_not_advance_postgres_sequence() -> Result<()> {
    let raw = std::env::var("PRISM_TEST_DATABASE_URL")
        .context("this explicit acceptance test requires disposable PRISM_TEST_DATABASE_URL")?;
    let admin = sqlx::PgPool::connect(&raw).await?;
    let schema = format!("prism_admission_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await?;
    let mut url = url::Url::parse(&raw)?;
    url.query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let ledger = Ledger::connect(url.as_str(), "admission-test".into(), 4, true).await?;
    let backend = Arc::new(Backend {
        ledger: Some(ledger.clone()),
        ..Default::default()
    });
    let server = Server::start(StratumConfig::default(), backend).await;
    let before: (i64, bool) =
        sqlx::query_as("SELECT last_value, is_called FROM qbit_prism_session_sequence")
            .fetch_one(&ledger.pool)
            .await?;
    for _ in 0..10_000 {
        let silent = Client::connect(&server).await;
        server.connections(1).await;
        drop(silent);
        server.connections(0).await;
    }
    let after: (i64, bool) =
        sqlx::query_as("SELECT last_value, is_called FROM qbit_prism_session_sequence")
            .fetch_one(&ledger.pool)
            .await?;
    let calls = server.backend.allocation_calls.load(Ordering::SeqCst);
    let mut subscribed = Client::connect(&server).await;
    let response = subscribed
        .request(json!({"id":1,"method":"mining.subscribe","params":[]}))
        .await;
    let allocated: (i64, bool) =
        sqlx::query_as("SELECT last_value, is_called FROM qbit_prism_session_sequence")
            .fetch_one(&ledger.pool)
            .await?;
    drop(subscribed);
    server.stop().await;
    ledger.pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await?;
    admin.close().await;

    assert_eq!(
        after, before,
        "unsubscribed sockets consumed durable session values"
    );
    assert_eq!(calls, 0);
    assert_eq!(response["result"], json!([[], "00000001", 8]));
    assert_eq!(
        allocated,
        (1, true),
        "the positive control must use the real ledger allocator"
    );
    eprintln!("10,000 accepted/closed unsubscribed sockets: sequence {before:?} -> {after:?}; subscribed positive control {allocated:?}");
    Ok(())
}
