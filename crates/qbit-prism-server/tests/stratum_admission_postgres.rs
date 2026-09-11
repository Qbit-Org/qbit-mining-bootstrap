//! Explicit real-PostgreSQL acceptance: never silently passes without a database.
#[path = "support/stratum_admission.rs"]
mod support;
use anyhow::{Context, Result};
use qbit_prism_server::{ledger::Ledger, stratum::StratumConfig};
use serde_json::json;
use std::sync::{atomic::Ordering, Arc};
use support::{Backend, Client, Server};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrap_exhaustion_leaves_subscription_retryable_and_disconnect_releases_guard() -> Result<()>
{
    let Ok(raw) = std::env::var("PRISM_TEST_DATABASE_URL") else {
        eprintln!("set PRISM_TEST_DATABASE_URL for the wrap subscription test");
        return Ok(());
    };
    let admin = sqlx::PgPool::connect(&raw).await?;
    let schema = format!("prism_wrap_admission_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await?;
    let mut url = url::Url::parse(&raw)?;
    url.query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let ledger = Ledger::connect(url.as_str(), "wrap-subscriber".into(), 4, true).await?;
    ledger
        .save_job(
            "occupied-second-value",
            &json!({"extranonce1":"00000002"}),
            0,
            "parent",
            3600,
        )
        .await?;
    sqlx::query("ALTER SEQUENCE qbit_prism_session_sequence MAXVALUE 2")
        .execute(&ledger.pool)
        .await?;
    let held = ledger.new_session_id().await?;
    let backend = Arc::new(Backend {
        ledger: Some(ledger.clone()),
        ..Default::default()
    });
    let server = Server::start(StratumConfig::default(), backend).await;
    let mut client = Client::connect(&server).await;
    let failed = client
        .request(json!({"id":1,"method":"mining.subscribe","params":[]}))
        .await;
    assert!(failed["result"].is_null());
    assert_eq!(
        failed["error"][2]["reason_id"],
        "session-allocation-exhausted"
    );
    assert!(!failed["error"][1]
        .as_str()
        .unwrap()
        .contains("database unavailable"));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_prism_session_reservations")
        .fetch_one(&ledger.pool)
        .await?;
    assert_eq!(count, 1, "failed subscribe must preserve the other session");
    held.release().await?;
    let retried = client
        .request(json!({"id":2,"method":"mining.subscribe","params":[]}))
        .await;
    assert_eq!(retried["result"], json!([[], "00000001", 8]));
    let token: String =
        sqlx::query_scalar("SELECT reservation_token FROM qbit_prism_session_reservations")
            .fetch_one(&ledger.pool)
            .await?;
    let repeated = client
        .request(json!({"id":3,"method":"mining.subscribe","params":[]}))
        .await;
    assert_eq!(repeated["result"], retried["result"]);
    let same_token: String =
        sqlx::query_scalar("SELECT reservation_token FROM qbit_prism_session_reservations")
            .fetch_one(&ledger.pool)
            .await?;
    assert_eq!(
        token, same_token,
        "repeated subscribe must retain the same guard"
    );
    drop(client);
    server.connections(0).await;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let count: i64 =
                sqlx::query_scalar("SELECT count(*) FROM qbit_prism_session_reservations")
                    .fetch_one(&ledger.pool)
                    .await?;
            if count == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    server.stop().await;
    ledger.pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await?;
    admin.close().await;
    Ok(())
}

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
