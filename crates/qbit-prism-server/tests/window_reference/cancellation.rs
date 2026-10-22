//! Explicit scale evidence for cancellation of a partially accumulated window.
use super::*;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

#[tokio::test(flavor = "current_thread")]
#[ignore = "400k partial-read cancellation measurement; requires disposable PostgreSQL"]
async fn cancelling_400k_read_after_96_pages_measures_runtime_stall() -> Result<()> {
    require_database()?;
    run(|db| Box::pin(async move {
        const COUNT: u64 = 400_000;
        const ACCUMULATED: u64 = 96 * 4096;
        let mut ledger = db.ledger().await?;
        ledger.pool = db.pool.clone(); // cancellation must return this sole connection
        window_fixture::WindowPlan::new(COUNT)?.load(&db.pool, "window-cancel-scale").await?;
        let gate_key: i64 = sqlx::query_scalar("SELECT oid::bigint FROM pg_namespace WHERE nspname=$1")
            .bind(&db.schema).fetch_one(&db.pool).await?;
        sqlx::raw_sql(&format!(
            "ALTER TABLE qbit_share_ledger RENAME TO window_cancel_source;
             CREATE FUNCTION cancel_after_page(seq bigint, value text) RETURNS text
             LANGUAGE plpgsql VOLATILE AS 'BEGIN IF seq > {ACCUMULATED} THEN
                 PERFORM pg_advisory_xact_lock({gate_key}); END IF; RETURN value; END;'"
        )).execute(&db.pool).await?;
        // Keep every real column and its type. Only the projected share ID is
        // gated, so endpoint probes and the first 96 keyset pages can finish.
        let columns: String = sqlx::query_scalar("SELECT string_agg(CASE WHEN attname='share_id' THEN 'cancel_after_page(share_seq,share_id) AS share_id' ELSE quote_ident(attname) END, ',' ORDER BY attnum) FROM pg_attribute WHERE attrelid='window_cancel_source'::regclass AND attnum>0 AND NOT attisdropped")
            .fetch_one(&db.pool).await?;
        // Keep the view ordered so PostgreSQL cannot evaluate the delayed
        // projection for all 400k rows in a sort before the first page's LIMIT.
        sqlx::raw_sql(&format!("CREATE VIEW qbit_share_ledger AS SELECT {columns} FROM window_cancel_source ORDER BY share_seq"))
            .execute(&db.pool).await?;
        // This is a cancellation benchmark, not a query-plan/throughput one.
        // The test-only connection must stream to the gate, never evaluate the
        // delayed projection while materializing an entire range for a sort.
        sqlx::raw_sql("ANALYZE window_cancel_source; SET enable_bitmapscan=off; SET enable_seqscan=off; SET enable_sort=off")
            .execute(&db.pool).await?;
        let plan: Value = sqlx::query_scalar(&format!(
            "EXPLAIN (FORMAT JSON) SELECT * FROM qbit_share_ledger WHERE accepted AND share_seq>0 AND share_seq<={COUNT} AND accepted_at<=to_timestamp({}::double precision/1000) AND job_issued_at<=to_timestamp({}::double precision/1000) ORDER BY share_seq LIMIT 4096",
            ANCHOR + COUNT as i64 + 1, ANCHOR + COUNT as i64 + 1,
        )).fetch_one(&db.pool).await?;
        require_streaming_plan(&plan[0]["Plan"])?;
        let mut gate = db.admin.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)").bind(gate_key).execute(&mut *gate).await?;
        let window = WindowRef {
            anchor_ms: ANCHOR + COUNT as i64 + 1,
            prior_balances_digest: qbit_prism::prior_balances_digest(&[]),
            shares: Some(ShareRange {
                first_share_seq: 1, last_share_seq: COUNT,
                share_count: COUNT,
                // Completion is impossible while the final page is gated.
                snapshot_sha256: [0; 32],
            }),
        };
        let source = ledger.clone();
        let reader = tokio::spawn(async move { source.read_window(&window, BalanceSource::Current).await });
        timeout(Duration::from_secs(120), async {
            loop {
                let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND classid=0 AND objid::bigint=$1)")
                    .bind(gate_key).fetch_one(&db.admin).await?;
                if blocked { return Ok::<_, anyhow::Error>(()); }
                ensure!(!reader.is_finished(), "reader finished before the final-page gate");
                sleep(Duration::from_millis(10)).await;
            }
        }).await.context("400k reader never reached its final page")??;

        let max_tick_ns = Arc::new(AtomicU64::new(0));
        let ticks = max_tick_ns.clone();
        let (ready, started) = tokio::sync::oneshot::channel();
        let ticker = tokio::spawn(async move {
            let mut previous = std::time::Instant::now();
            ready.send(()).unwrap();
            loop {
                sleep(Duration::from_millis(1)).await;
                let now = std::time::Instant::now();
                ticks.fetch_max(now.duration_since(previous).as_nanos() as u64, Ordering::Relaxed);
                previous = now;
            }
        });
        started.await?;
        let cancel_at = std::time::Instant::now();
        reader.abort();
        ensure!(reader.await.unwrap_err().is_cancelled(), "400k reader was not cancelled");
        let abort_join = cancel_at.elapsed();
        gate.rollback().await?;
        let recovered: i32 = timeout(Duration::from_secs(5), sqlx::query_scalar("SELECT 1").fetch_one(&ledger.pool)).await??;
        ensure!(recovered == 1, "cancelled reader retained the sole connection");
        sleep(Duration::from_millis(250)).await;
        ticker.abort();
        ensure!(ticker.await.unwrap_err().is_cancelled(), "ticker did not stop");
        println!("window_cancel_only fixture_rows={COUNT} completed_pages=96 accumulated_shares={ACCUMULATED} abort_join_us={} max_1ms_tick_gap_us={} connection_recovered=true",
            abort_join.as_micros(), max_tick_ns.load(Ordering::Relaxed) / 1000);
        // Report both measurements; no unapproved timing threshold or claim
        // about the full refresh/resume deadline follows from this sample.
        Ok(())
    })).await
}

fn require_streaming_plan(plan: &Value) -> Result<()> {
    let kind = plan["Node Type"].as_str().context("missing EXPLAIN node")?;
    ensure!(
        matches!(
            kind,
            "Limit" | "Subquery Scan" | "Index Scan" | "Index Only Scan" | "Result"
        ),
        "cancellation gate could run before page accumulation: {plan}"
    );
    if let Some(children) = plan["Plans"].as_array() {
        for child in children {
            require_streaming_plan(child)?;
        }
    }
    Ok(())
}
