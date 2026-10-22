//! Explicit real-PostgreSQL acceptance: never silently passes without a database.
#[path = "support/ledger_execution_proxy.rs"]
mod proxy;
#[path = "support/stratum_admission.rs"]
mod support;
use anyhow::{Context, Result};
use proxy::ExecutionProxy;
use qbit_prism_server::{ledger::Ledger, stratum::StratumConfig};
use qbit_prism_test_gate as gate;
use serde_json::json;
use std::sync::{atomic::Ordering, Arc};
use support::{assert_socket_refused, refusal_total, Backend, Client, Server};

/// The `qbit_prism_jobs` lookup an unknown-job submit makes, named by its text
/// rather than by a position in the execution order.
const JOB_LOOKUP: &str = "FROM qbit_prism_jobs WHERE job_id=";

fn job_lookups(executions: &[proxy::Execution]) -> usize {
    executions
        .iter()
        .filter(|execution| execution.sql.contains(JOB_LOOKUP))
        .count()
}

fn unknown_submit(id: u64, job_id: &str) -> serde_json::Value {
    json!({"id":id,"method":"mining.submit",
        "params":["budget.worker",job_id,"0".repeat(16),"00000000","00000000"]})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrap_exhaustion_leaves_subscription_retryable_and_disconnect_releases_guard() -> Result<()>
{
    let Some(raw) = gate::database_url(gate::site!())? else {
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
    let raw = gate::required_database_url(gate::site!())?;
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

/// #276 C1 and C3 against a real database, through the wire-protocol proxy.
/// The proxy attributes executions per PostgreSQL pool connection, not per
/// Stratum session, so this test runs its one session in isolation and asserts
/// the global count in each window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_job_budget_bounds_ledger_queries_and_a_capped_source_runs_none() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let admin = sqlx::PgPool::connect(&raw).await?;
    let schema = format!("prism_budget_admission_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await?;
    let mut url = url::Url::parse(&raw)?;
    url.query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let upstream = tokio::net::lookup_host((
        url.host_str().unwrap_or("127.0.0.1").to_string(),
        url.port().unwrap_or(5432),
    ))
    .await?
    .next()
    .expect("database host resolves");
    let observer = ExecutionProxy::start(upstream).await?;
    let ledger = Ledger::connect(
        &observer.rewrite_url(url.as_str())?,
        "budget-subscriber".into(),
        4,
        true,
    )
    .await?;
    let backend = Arc::new(Backend {
        ledger: Some(ledger.clone()),
        ..Default::default()
    });
    let budget = 32u32;
    let metrics = Arc::new(qbit_prism_server::metrics::Metrics::default());
    let server = Server::start_with_metrics(
        StratumConfig {
            max_unknown_jobs_per_interval: budget,
            session_budget_interval_seconds: 600.0,
            ..Default::default()
        },
        backend,
        metrics.clone(),
    )
    .await;
    let mut client = Client::connect(&server).await;
    assert_eq!(
        client
            .request(json!({"id":1,"method":"mining.subscribe","params":[]}))
            .await["result"],
        json!([[], "00000001", 8])
    );
    assert_eq!(
        client
            .request(json!({"id":2,"method":"mining.authorize","params":["budget.worker","x"]}))
            .await["result"],
        true
    );

    // 1,000 unknown-job submits from one session, sent as fast as the socket
    // takes them, all inside one budget window. Ten IDs repeat, so a cache
    // that skipped repeated lookups would show up as fewer than budget + 1.
    let flood = observer.mark();
    let started = std::time::Instant::now();
    for id in 0..1_000u64 {
        client
            .send(unknown_submit(100 + id, &format!("absent-{}", id % 10)))
            .await;
    }
    client.expect_closed().await;
    let elapsed = started.elapsed();
    server.connections(0).await;
    let lookups = job_lookups(&observer.executions_since(flood)?);
    // Every miss is charged, repeated IDs included, so the lookup count is
    // exactly the budget plus the one lookup whose miss spent it.
    assert_eq!(
        lookups,
        budget as usize + 1,
        "qbit_prism_jobs lookups for 1,000 unknown submits at a budget of {budget}"
    );
    assert_eq!(refusal_total(&metrics, "unknown_job_budget"), 1.);
    // The disconnected session released its reservation guard. The release
    // is the session task's own cleanup after the socket closed, so it lands
    // shortly after `expect_closed` rather than before it; wait for it the
    // way the wrap-exhaustion test does instead of reading the table once.
    let reservations = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let reservations: i64 =
                sqlx::query_scalar("SELECT count(*) FROM qbit_prism_session_reservations")
                    .fetch_one(&ledger.pool)
                    .await?;
            if reservations == 0 {
                return anyhow::Ok(reservations);
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .context("a budget disconnect must release the guard within five seconds")??;
    assert_eq!(
        reservations, 0,
        "a budget disconnect must release the guard"
    );
    server.stop().await;

    // C3: a connection beyond the per-source cap runs no statement at all.
    let capped = Server::start_with_metrics(
        StratumConfig {
            max_connections_per_ip: 1,
            ..Default::default()
        },
        Arc::new(Backend {
            ledger: Some(ledger.clone()),
            ..Default::default()
        }),
        metrics.clone(),
    )
    .await;
    let mut held = Client::connect(&capped).await;
    assert_eq!(
        held.request(json!({"id":1,"method":"mining.subscribe","params":[]}))
            .await["result"],
        json!([[], "00000002", 8])
    );
    capped.connections(1).await;
    let refusal = observer.mark();
    assert_socket_refused(capped.address).await;
    let refused_executions = observer.executions_since(refusal)?;
    assert!(
        refused_executions.is_empty(),
        "the refused connection ran {refused_executions:?}"
    );
    assert_eq!(refusal_total(&metrics, "ip_limit"), 1.);
    assert_eq!(capped.backend.allocation_calls.load(Ordering::SeqCst), 1);

    // Zero disables the cap: the same address is admitted repeatedly.
    drop(held);
    capped.connections(0).await;
    capped.stop().await;
    let open = Server::start_with_metrics(
        StratumConfig::default(),
        Arc::new(Backend {
            ledger: Some(ledger.clone()),
            ..Default::default()
        }),
        metrics.clone(),
    )
    .await;
    let mut admitted = Vec::new();
    for _ in 0..4 {
        admitted.push(Client::connect(&open).await);
    }
    open.connections(4).await;
    assert_eq!(refusal_total(&metrics, "ip_limit"), 1.);
    drop(admitted);
    open.connections(0).await;
    open.stop().await;

    eprintln!(
        "1,000 unknown-job submits in {elapsed:?}: {lookups} qbit_prism_jobs lookups, \
         budget {budget}; refused connection ran 0 statements"
    );
    ledger.pool.close().await;
    observer.finish().await?;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await?;
    admin.close().await;
    Ok(())
}
