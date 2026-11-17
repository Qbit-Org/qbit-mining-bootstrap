//! The difficulty-hint writer against the coordinator and a real PostgreSQL
//! (#872): a hint statement held past the write's bound is ended by the
//! server at the bound the listener's configuration sets, counted as
//! `timed_out`, and leaves nothing running and its connection as the pool
//! keeps it.
use super::*;
use crate::coordinator::miner_tests::Fixture as Frontend;
use crate::ledger::Ledger;
use anyhow::{ensure, Context, Result};
use futures_util::FutureExt;
use qbit_prism_test_gate as gate;
use sqlx::PgPool;
use std::panic::{resume_unwind, AssertUnwindSafe};

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

/// The backend of `application` waiting for a lock, once one is.
async fn waiting(admin: &PgPool, application: &str) -> Result<i32> {
    timeout(Duration::from_secs(5), async {
        loop {
            let pid: Option<i32> = sqlx::query_scalar(
                "SELECT pid FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock'",
            )
            .bind(application)
            .fetch_optional(admin)
            .await?;
            if let Some(pid) = pid {
                return Ok::<_, anyhow::Error>(pid);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .context("the hint's statement never waited for the row lock")?
}

/// How many of `application`'s backends are in `states`.
async fn backends(admin: &PgPool, application: &str, states: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(&format!(
        "SELECT count(*) FROM pg_stat_activity WHERE application_name=$1 AND state {states}"
    ))
    .bind(application)
    .fetch_one(admin)
    .await?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_a_hint_held_past_a_non_default_bound_is_ended_by_the_server_and_counted_timed_out(
) -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let admin = PgPool::connect(&raw).await?;
    let schema = format!("prism_hint_writer_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await?;
    // The schema is dropped whether the case fails or panics.
    let result = AssertUnwindSafe(held_hint(&raw, &admin, &schema))
        .catch_unwind()
        .await;
    let cleanup = sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await;
    admin.close().await;
    match result {
        Err(panic) => resume_unwind(panic),
        Ok(Ok(())) => cleanup.map(|_| ()).map_err(Into::into),
        Ok(Err(error)) => match cleanup {
            Ok(_) => Err(error),
            Err(cleanup) => Err(error.context(format!("schema cleanup also failed: {cleanup}"))),
        },
    }
}

async fn held_hint(raw: &str, admin: &PgPool, schema: &str) -> Result<()> {
    let application = format!("hint-writer-{}", &schema[18..30]);
    let mut url = url::Url::parse(raw)?;
    // The coordinator's ledger sessions start at 15 s, the pool's own
    // `statement_timeout`, as `Ledger::connect` sets its sessions'.
    url.query_pairs_mut()
        .append_pair(
            "options",
            &format!("-csearch_path={schema} -cstatement_timeout=15000"),
        )
        .append_pair("application_name", &application);
    let ledger = Ledger::connect(url.as_str(), "hint-writer".into(), 2, true).await?;
    let result = async {
        ledger
            .record_worker_difficulty("primary", "rig", 64.0, now_ms())
            .await?;
        let frontend =
            Frontend::build_with_ledger(Duration::from_secs(60), |_| {}, None, url.as_str())
                .await;
        let pool = frontend.coordinator.ledger.pool.clone();
        let outcome = async {
            // Not the 500 ms default: the server must be given this one.
            let config = StratumConfig {
                listener_name: "primary".into(),
                difficulty_hint_timeout_seconds: 1.0,
                ..Default::default()
            };
            let metrics = Arc::new(Metrics::default());
            let hints = DifficultyHints::new();
            let mut locker = admin.begin().await?;
            sqlx::query(&format!("SELECT 1 FROM {schema}.qbit_worker_difficulty WHERE listener='primary' AND worker_username='rig' FOR UPDATE"))
                .fetch_one(&mut *locker)
                .await?;
            let started = tokio::time::Instant::now();
            let (pid, elapsed) = hints
                .beside(
                    frontend.coordinator.clone(),
                    config.clone(),
                    metrics.clone(),
                    WriterEnd::Abandon,
                    async {
                        lower(&hints, &config, &metrics, "rig", 32.0);
                        let pid = waiting(admin, &application).await?;
                        until(|| count(&metrics, DifficultyHintOutcome::TimedOut) == 1.).await;
                        Ok::<_, anyhow::Error>((pid, started.elapsed()))
                    },
                )
                .await?;
            // PostgreSQL ended the decrease at what was left of the 1 s
            // bound less its 100 ms margin, before the writer's own bound:
            // not the default bound's 450 ms, nor the pool's 15 s.
            ensure!(
                elapsed >= Duration::from_millis(850) && elapsed < Duration::from_secs(1),
                "the held hint ended {elapsed:?} in"
            );
            for outcome in [DifficultyHintOutcome::Failed, DifficultyHintOutcome::Written] {
                ensure!(count(&metrics, outcome) == 0., "{}", outcome.as_str());
            }
            // Nothing of the hint is still running while the row is held.
            ensure!(backends(admin, &application, "='active'").await? == 0);
            let mut settled = false;
            for _ in 0..100 {
                if backends(admin, &application, "<>'idle'").await? == 0 {
                    settled = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            ensure!(settled, "the hint's transaction stayed open");
            locker.rollback().await?;
            // The same pooled connection, idle outside any transaction (as
            // the server reports it above) and back at the pool's own timeout.
            for _ in 0..100 {
                if pool.num_idle() > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let mut connection = pool.acquire().await?;
            let (again, setting): (i32, String) =
                sqlx::query_as("SELECT pg_backend_pid(),current_setting('statement_timeout')")
                    .fetch_one(&mut *connection)
                    .await?;
            ensure!(again == pid, "the hint ran on {pid}, the pool offered {again}");
            ensure!(setting == "15s", "the connection kept the hint's timeout: {setting}");
            drop(connection);
            // The rolled-back decrease changed nothing.
            let kept = ledger.worker_difficulty("primary", "rig", 900).await?;
            ensure!(kept.as_ref().map(|hint| hint.difficulty) == Some(64.0), "{kept:?}");
            Ok(())
        }
        .await;
        pool.close().await;
        outcome
    }
    .await;
    ledger.pool.close().await;
    result
}
