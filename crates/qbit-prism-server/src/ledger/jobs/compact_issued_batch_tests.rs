use super::*;
use sqlx::Connection;
use std::time::Duration;

#[tokio::test]
async fn canceled_begin_cleans_untracked_transaction_before_pool_reuse() -> Result<()> {
    let Some(url) = qbit_prism_test_gate::database_url(qbit_prism_test_gate::site!())? else {
        return Ok(());
    };
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await?;
    let mut observer = sqlx::PgConnection::connect(&url).await?;
    let mut acquired = pool.acquire().await?;
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *acquired)
        .await?;
    let lock = i64::from(pid);
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(lock)
        .execute(&mut observer)
        .await?;
    let attempt = Arc::new(CompactBatchAttempt::new(
        Instant::now() + Duration::from_secs(5),
    ));
    attempt.cleanup.pending.store(true, Ordering::Release);
    let mut guarded = BatchConnection {
        connection: Some(acquired),
        clean: false,
        cleanup: attempt.cleanup.clone(),
    };
    let writer: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
        // Delay the BEGIN acknowledgement after the server entered a
        // transaction, before SQLx increments its tracked transaction depth.
        let statement = format!("BEGIN; CREATE TEMP TABLE canceled_batch_begin(id integer); SELECT pg_advisory_xact_lock({lock})");
        let _tx = guarded
            .connection
            .as_mut()
            .unwrap()
            .begin_with(statement)
            .await?;
        anyhow::bail!("BEGIN unexpectedly completed before cancellation")
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event='advisory')")
                .bind(pid).fetch_one(&mut observer).await?;
            if blocked { return Ok::<_, anyhow::Error>(()); }
            tokio::task::yield_now().await;
        }
    }).await??;
    writer.abort();
    ensure!(writer.await.unwrap_err().is_cancelled());
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(lock)
        .execute(&mut observer)
        .await?;
    tokio::time::timeout(Duration::from_secs(2), attempt.wait_for_cleanup()).await?;
    let mut reused = pool.acquire().await?;
    let (same_backend, temporary_table): (bool, Option<String>) = sqlx::query_as(
        "SELECT pg_backend_pid()=$1, to_regclass('pg_temp.canceled_batch_begin')::text",
    )
    .bind(pid)
    .fetch_one(&mut *reused)
    .await?;
    // Reuse must follow rollback, not merely a successful protocol ping.
    let clean = same_backend && temporary_table.is_none();
    sqlx::raw_sql("ROLLBACK").execute(&mut *reused).await?;
    drop(reused);
    pool.close().await;
    observer.close().await?;
    ensure!(
        clean,
        "canceled BEGIN returned an open transaction to the pool"
    );
    Ok(())
}
