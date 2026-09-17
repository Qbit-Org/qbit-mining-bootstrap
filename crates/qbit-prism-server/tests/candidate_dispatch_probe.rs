//! Executed canonical dispatch-probe regressions for #432, on real PostgreSQL.
//! The fixtures have exactly N unfinished rows, including all four states.
//! Plans report buffer *accesses* (hits and reads separately), not unique pages
//! or physical I/O savings. No latency/page threshold or universal work bound
//! is asserted: live claims, sparse eligibility and dead entries can cost more.

use anyhow::{ensure, Context, Result};
use qbit_prism_server::ledger::{CandidateState, Ledger};
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{Connection, PgConnection};
use std::{sync::Arc, time::Duration};

#[allow(dead_code)]
#[path = "support/ledger_database.rs"]
mod ledger_database;
use ledger_database::FixtureDatabase;

struct Database {
    fixture: FixtureDatabase,
    ledger: Arc<Ledger>,
}

impl Database {
    async fn open(raw: &str) -> Result<Self> {
        let fixture = FixtureDatabase::open(raw, "prism_dispatch_").await?;
        match Ledger::connect(&fixture.url, "dispatch-probe".into(), 1, true).await {
            Ok(ledger) => Ok(Self {
                fixture,
                ledger: Arc::new(ledger),
            }),
            Err(error) => Err(fixture.abandon(error).await),
        }
    }

    async fn close(self, outcome: Result<()>) -> Result<()> {
        self.ledger.pool.close().await;
        self.fixture.close(outcome).await
    }
}

async fn seed(conn: &mut PgConnection, unfinished: i64, retained: i64) -> Result<()> {
    sqlx::raw_sql("TRUNCATE qbit_block_candidate_outbox CASCADE")
        .execute(&mut *conn)
        .await?;
    sqlx::query(
        "INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,state,attempt_count,created_at,next_attempt_at,completed_at) \
         SELECT lpad(to_hex(i),64,'0'),NULL,lpad(to_hex(i),64,'0'),CASE WHEN i%7=0 THEN 'abandoned' ELSE 'submitted' END,1+(i%3), \
                clock_timestamp()-(i||' seconds')::interval,clock_timestamp()-(i||' seconds')::interval,clock_timestamp()-(i||' seconds')::interval \
         FROM generate_series(1,$1::bigint) AS g(i)",
    ).bind(retained).execute(&mut *conn).await?;
    // Synthetic payloads satisfy storage constraints; only the probe reads
    // these rows. Native claim/decoding/fairness remain in the existing suites.
    sqlx::query(
        "INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,block_bytes,window_anchor_ms,window_prior_balances_sha256,state,attempt_count,created_at,next_attempt_at,offer_reserved_at,offer_reserved_by,offered_at_ms,offer_outcome,last_error) \
         SELECT 'b'||lpad(to_hex(i),63,'0'),'{\"k\":1}'::jsonb,lpad(to_hex(i),64,'0'),decode('00','hex'),1,repeat('b1',32), \
                CASE i WHEN $1-2 THEN 'offer_reserved' WHEN $1-1 THEN 'offered' WHEN $1 THEN 'reconciliation' ELSE 'pending' END, \
                CASE WHEN i>$1-3 THEN 1 ELSE 0 END, \
                '2020-01-01'::timestamptz+i*interval '1 second','2020-01-01'::timestamptz+i*interval '1 second', \
                CASE WHEN i>$1-3 THEN '2020-01-01'::timestamptz END,CASE WHEN i>$1-3 THEN 'fixture' END, \
                CASE WHEN i=$1-1 THEN 1700000000456::bigint END,CASE WHEN i=$1-1 THEN 'accepted' WHEN i=$1 THEN 'unknown' END,CASE WHEN i=$1 THEN 'delivery unknown' END \
         FROM generate_series(1,$1::bigint) AS g(i)",
    ).bind(unfinished).execute(&mut *conn).await?;
    Ok(())
}

async fn population(conn: &mut PgConnection, name: &str, unfinished: i64) -> Result<bool> {
    let (due, expiry, eligible) = match name {
        "busy" => ("'2020-01-01'::timestamptz", "NULL::timestamptz", true),
        "backoff" => (
            "clock_timestamp()+interval '1 day'",
            "NULL::timestamptz",
            false,
        ),
        "live_claimed" | "sparse_last" => (
            "'2020-01-01'::timestamptz",
            "clock_timestamp()+interval '1 day'",
            name == "sparse_last",
        ),
        "expired" => (
            "'2020-01-01'::timestamptz",
            "'2020-01-01'::timestamptz",
            true,
        ),
        _ => anyhow::bail!("unknown fixture population {name}"),
    };
    sqlx::raw_sql(&format!(
        "UPDATE qbit_block_candidate_outbox SET next_attempt_at={due},claim_expires_at={expiry}, \
         claim_token=CASE WHEN {expiry} IS NULL THEN NULL ELSE 'fixture-token' END, \
         claim_instance_id=CASE WHEN {expiry} IS NULL THEN NULL ELSE 'fixture' END WHERE state IN {}",
        CandidateState::UNFINISHED_SQL
    )).execute(&mut *conn).await?;
    if name == "sparse_last" {
        sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_expires_at=NULL,claim_token=NULL,claim_instance_id=NULL WHERE block_hash='b'||lpad(to_hex($1::bigint),63,'0')")
            .bind(unfinished).execute(&mut *conn).await?;
    }
    Ok(eligible)
}

async fn sequence(conn: &mut PgConnection) -> Result<(i64, bool)> {
    Ok(
        sqlx::query_as("SELECT last_value,is_called FROM qbit_prism_candidate_dispatch_sequence")
            .fetch_one(conn)
            .await?,
    )
}

fn advanced(before: (i64, bool), after: (i64, bool), eligible: bool) {
    assert_eq!(
        after,
        if eligible {
            (before.0 + i64::from(before.1), true)
        } else {
            before
        }
    );
}

async fn probe(conn: &mut PgConnection, statement: &str, eligible: bool) -> Result<()> {
    let before = sequence(conn).await?;
    let rows: Vec<i64> = sqlx::query_scalar(statement).fetch_all(&mut *conn).await?;
    assert_eq!(rows.len(), usize::from(eligible));
    if eligible {
        assert_eq!(rows[0], before.0 + i64::from(before.1));
    }
    advanced(before, sequence(conn).await?, eligible);
    Ok(())
}

fn nodes<'a>(node: &'a Value, all: &mut Vec<&'a Value>) {
    all.push(node);
    if let Some(children) = node["Plans"].as_array() {
        for child in children {
            nodes(child, all);
        }
    }
}

async fn environment(conn: &mut PgConnection) -> Result<()> {
    let settings: Value = sqlx::query_scalar(
        "SELECT jsonb_object_agg(name,setting) FROM pg_settings WHERE name LIKE 'enable_%' OR name IN \
         ('server_version','shared_buffers','work_mem','random_page_cost','seq_page_cost','default_statistics_target','plan_cache_mode','autovacuum')"
    ).fetch_one(&mut *conn).await?;
    for setting in [
        "enable_seqscan",
        "enable_sort",
        "enable_indexscan",
        "enable_indexonlyscan",
        "enable_bitmapscan",
    ] {
        ensure!(
            settings[setting] == "on",
            "planner method forced: {settings}"
        );
    }
    for (name, expected) in [
        ("random_page_cost", "4"),
        ("seq_page_cost", "1"),
        ("default_statistics_target", "100"),
        ("plan_cache_mode", "auto"),
    ] {
        ensure!(settings[name] == expected, "non-default {name}: {settings}");
    }
    let query = Ledger::due_work_probe_sql();
    println!(
        "dispatch-probe-environment {}",
        json!({"settings":settings,"query":query,"query_sha256":format!("{:x}",Sha256::digest(query.as_bytes()))})
    );
    Ok(())
}

async fn explain(
    conn: &mut PgConnection,
    label: &str,
    statement: &str,
    eligible: bool,
    unfinished: i64,
    retained: i64,
) -> Result<()> {
    let counts: (i64, i64) = sqlx::query_as(&format!(
        "SELECT count(*) FILTER (WHERE state IN {}),count(*) FILTER (WHERE state NOT IN {}) FROM qbit_block_candidate_outbox",
        CandidateState::UNFINISHED_SQL, CandidateState::UNFINISHED_SQL
    )).fetch_one(&mut *conn).await?;
    assert_eq!(counts, (unfinished, retained));
    let before = sequence(conn).await?;
    let plan: Value = sqlx::query_scalar(&format!(
        "EXPLAIN (ANALYZE, BUFFERS, SETTINGS, TIMING OFF, FORMAT JSON) {statement}"
    ))
    .fetch_one(&mut *conn)
    .await?;
    advanced(before, sequence(conn).await?, eligible);
    let mut all = Vec::new();
    nodes(&plan[0]["Plan"], &mut all);
    assert_eq!(
        plan[0]["Plan"]["Actual Rows"].as_f64(),
        Some(f64::from(u8::from(eligible)))
    );
    let scans: Vec<_> = all
        .iter()
        .filter(|n| n["Relation Name"] == "qbit_block_candidate_outbox")
        .collect();
    ensure!(scans.len() == 1, "expected one outbox scan: {plan}");
    ensure!(
        scans[0]["Index Name"] == "qbit_block_candidate_outbox_unfinished_idx",
        "retained history scan in {label}: {plan}"
    );
    ensure!(
        !all.iter()
            .any(|n| n["Node Type"] == "Sort" || n["Node Type"] == "Seq Scan"),
        "unexpected sort/scan in {label}: {plan}"
    );
    // EXPLAIN reports per-loop row averages. Require one scan execution so
    // this counts visible tuples visited, not dead entries or buffer accesses.
    assert_eq!(scans[0]["Actual Loops"].as_f64(), Some(1.0));
    let visible = scans[0]["Actual Rows"].as_f64().unwrap_or(0.0)
        + scans[0]["Rows Removed by Filter"].as_f64().unwrap_or(0.0);
    ensure!(
        visible <= unfinished as f64,
        "examined retained visible tuples: {plan}"
    );
    if !eligible {
        assert_eq!(visible, unfinished as f64);
    }
    let sizes: (i64, i64) = sqlx::query_as("SELECT pg_relation_size('qbit_block_candidate_outbox'),pg_relation_size('qbit_block_candidate_outbox_unfinished_idx')")
        .fetch_one(&mut *conn).await?;
    println!(
        "dispatch-probe-plan {}",
        json!({"label":label,"unfinished":unfinished,"retained":retained,"heap_bytes":sizes.0,"index_bytes":sizes.1,"plan":plan})
    );
    Ok(())
}

async fn direct_and_prepared(
    conn: &mut PgConnection,
    label: &str,
    eligible: bool,
    unfinished: i64,
    retained: i64,
) -> Result<()> {
    let query = Ledger::due_work_probe_sql();
    explain(
        conn,
        &format!("{label}/direct"),
        &query,
        eligible,
        unfinished,
        retained,
    )
    .await?;
    sqlx::raw_sql(&format!("PREPARE dispatch_probe AS {query}"))
        .execute(&mut *conn)
        .await?;
    for _ in 0..6 {
        probe(conn, "EXECUTE dispatch_probe", eligible).await?;
    }
    explain(
        conn,
        &format!("{label}/prepared"),
        "EXECUTE dispatch_probe",
        eligible,
        unfinished,
        retained,
    )
    .await?;
    let prepared: (i64, i64, i32) = sqlx::query_as("SELECT generic_plans,custom_plans,cardinality(parameter_types) FROM pg_prepared_statements WHERE name='dispatch_probe'")
        .fetch_one(&mut *conn).await?;
    assert_eq!(
        prepared,
        (7, 0, 0),
        "parameter-free server prepared execution"
    );
    sqlx::raw_sql("DEALLOCATE dispatch_probe")
        .execute(&mut *conn)
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canonical_probe_plans_cover_exact_unfinished_and_retained_populations() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = Database::open(&raw).await?;
    let outcome = async {
        let mut conn = db.ledger.pool.acquire().await?;
        environment(&mut conn).await?;
        for retained in [50_000, 100_000] {
            for unfinished in [24, 100, 3_120] {
                seed(&mut conn, unfinished, retained).await?;
                for name in ["busy", "backoff", "live_claimed", "expired", "sparse_last"] {
                    let eligible = population(&mut conn, name, unfinished).await?;
                    sqlx::raw_sql("ANALYZE qbit_block_candidate_outbox")
                        .execute(&mut *conn)
                        .await?;
                    direct_and_prepared(
                        &mut conn,
                        &format!("n{unfinished}/r{retained}/{name}"),
                        eligible,
                        unfinished,
                        retained,
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }
    .await;
    db.close(outcome).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canonical_probe_handles_churn_dead_entries_and_stale_statistics() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = Database::open(&raw).await?;
    let outcome = async {
        let mut conn = db.ledger.pool.acquire().await?;
        environment(&mut conn).await?;
        seed(&mut conn, 3_120, 100_000).await?;
        // Start with statistics describing a much larger unfinished set.
        sqlx::raw_sql("UPDATE qbit_block_candidate_outbox SET state='pending',candidate='{}'::jsonb,completed_at=NULL WHERE block_hash<'b'; ANALYZE qbit_block_candidate_outbox")
            .execute(&mut *conn).await?;
        // Hold a real old snapshot so pruning cannot erase the old versions
        // before the measurement. Disable automatic maintenance only for this
        // private table so ANALYZE cannot silently freshen the stale control.
        sqlx::raw_sql("ALTER TABLE qbit_block_candidate_outbox SET (autovacuum_enabled=false)").execute(&mut *conn).await?;
        let mut old = PgConnection::connect(&db.fixture.url).await?;
        sqlx::raw_sql("BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT count(*) FROM qbit_block_candidate_outbox").execute(&mut old).await?;
        sqlx::raw_sql("UPDATE qbit_block_candidate_outbox SET state='submitted',candidate=NULL,completed_at=clock_timestamp() WHERE block_hash<'b'; \
            UPDATE qbit_block_candidate_outbox SET next_attempt_at=next_attempt_at+interval '1 second' WHERE block_hash>='b'; \
            DELETE FROM qbit_block_candidate_outbox WHERE block_hash<'b' AND right(block_hash,1)='0'")
            .execute(&mut *conn).await?;
        sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate_sha256,state,completed_at) SELECT lpad(to_hex(i),64,'0'),lpad(to_hex(i),64,'0'),'submitted',clock_timestamp() FROM generate_series(16,100000,16) g(i)")
            .execute(&mut *conn).await?;
        let stale: f32 = sqlx::query_scalar("SELECT reltuples FROM pg_class WHERE oid='qbit_block_candidate_outbox_unfinished_idx'::regclass")
            .fetch_one(&mut *conn).await?;
        ensure!(stale > 100_000.0, "stale statistics control was refreshed");
        for name in ["backoff", "live_claimed", "sparse_last"] {
            let eligible = population(&mut conn, name, 3_120).await?;
            direct_and_prepared(&mut conn, &format!("churn/stale/{name}"), eligible, 3_120, 100_000).await?;
        }
        let old_count: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_block_candidate_outbox WHERE state='pending'").fetch_one(&mut old).await?;
        assert_eq!(old_count, 103_117, "old tuple versions must still be visible");
        sqlx::raw_sql("ROLLBACK").execute(&mut old).await?;
        old.close().await?;
        sqlx::raw_sql("VACUUM (ANALYZE) qbit_block_candidate_outbox").execute(&mut *conn).await?;
        for name in ["backoff", "live_claimed", "sparse_last"] {
            let eligible = population(&mut conn, name, 3_120).await?;
            direct_and_prepared(&mut conn, &format!("churn/vacuumed/{name}"), eligible, 3_120, 100_000).await?;
        }
        Ok(())
    }.await;
    db.close(outcome).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canonical_probe_preserves_slots_clocks_and_candidate_ownership() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = Database::open(&raw).await?;
    let outcome = async {
        let mut conn = db.ledger.pool.acquire().await?;
        let query = Ledger::due_work_probe_sql();
        for (unfinished, retained) in [(0, 0), (0, 24)] {
            seed(&mut conn, unfinished, retained).await?;
            for _ in 0..5 { probe(&mut conn, &query, false).await?; }
        }
        seed(&mut conn, 4, 0).await?;
        sqlx::raw_sql("UPDATE qbit_block_candidate_outbox SET next_attempt_at='infinity'")
            .execute(&mut *conn).await?;
        for _ in 0..5 { probe(&mut conn, &query, false).await?; }
        for state in ["pending", "offer_reserved", "offered", "reconciliation"] {
            population(&mut conn, "backoff", 4).await?;
            for _ in 0..5 { probe(&mut conn, &query, false).await?; }
            // Persist a database-sampled equality boundary; by execution it
            // is due/expired. The fixed-cutoff truth table below checks exact
            // comparator equality without pretending to freeze volatile time.
            sqlx::query("UPDATE qbit_block_candidate_outbox SET next_attempt_at=clock_timestamp(),claim_expires_at=clock_timestamp() WHERE state=$1")
                .bind(state).execute(&mut *conn).await?;
            let before: Value = sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(o) ORDER BY block_hash) FROM qbit_block_candidate_outbox o").fetch_one(&mut *conn).await?;
            probe(&mut conn, &query, true).await?;
            let after: Value = sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(o) ORDER BY block_hash) FROM qbit_block_candidate_outbox o").fetch_one(&mut *conn).await?;
            assert_eq!(before, after, "the probe grants no candidate ownership");
        }
        // Freeze only the two clock expressions in the canonical SQL for an
        // exact +/-1 microsecond comparator model. This is not an executed
        // volatile-query plan and does not claim a frozen real-world instant.
        assert_eq!(query.matches("clock_timestamp()").count(), 2);
        sqlx::raw_sql("CREATE TEMP TABLE dispatch_probe_cutoff AS SELECT clock_timestamp() t")
            .execute(&mut *conn).await?;
        let boundary_query = query.replace("clock_timestamp()", "(SELECT t FROM dispatch_probe_cutoff)");
        for state in ["pending", "offer_reserved", "offered", "reconciliation"] {
            population(&mut conn, "backoff", 4).await?;
            for due in [-1_i32, 0, 1] {
                for expiry in [None, Some(-1_i32), Some(0), Some(1)] {
                    sqlx::query("UPDATE qbit_block_candidate_outbox SET next_attempt_at=t+$2*interval '1 microsecond',claim_expires_at=t+$3*interval '1 microsecond' FROM dispatch_probe_cutoff WHERE state=$1")
                        .bind(state).bind(due).bind(expiry).execute(&mut *conn).await?;
                    probe(&mut conn, &boundary_query, due<=0 && expiry.is_none_or(|e| e<=0)).await?;
                }
            }
        }
        population(&mut conn, "backoff", 4).await?;
        let mut tx = conn.begin().await?;
        sqlx::raw_sql("UPDATE qbit_block_candidate_outbox SET next_attempt_at=clock_timestamp()+interval '100 milliseconds',claim_expires_at=clock_timestamp()+interval '100 milliseconds' WHERE state='pending'")
            .execute(&mut *tx).await?;
        // Wait on the database clock, not a client-clock boundary or a
        // duration assertion. now() in this transaction remains too old.
        sqlx::raw_sql("DO $$ BEGIN WHILE EXISTS (SELECT 1 FROM qbit_block_candidate_outbox WHERE state='pending' AND (next_attempt_at>clock_timestamp() OR claim_expires_at>clock_timestamp())) LOOP PERFORM pg_sleep(0.01); END LOOP; END $$")
            .execute(&mut *tx).await?;
        let transaction_due: bool = sqlx::query_scalar("SELECT next_attempt_at<=now() FROM qbit_block_candidate_outbox WHERE state='pending'").fetch_one(&mut *tx).await?;
        assert!(!transaction_due);
        let before = sequence(&mut tx).await?;
        probe(&mut tx, &query, true).await?;
        tx.rollback().await?;
        advanced(before, sequence(&mut conn).await?, true);
        Ok(())
    }.await;
    db.close(outcome).await
}

async fn wait_for_probe_lock(conn: &mut PgConnection) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query=$1)")
                .bind(Ledger::due_work_probe_sql()).fetch_one(&mut *conn).await?;
            if waiting { return anyhow::Ok(()); }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.context("claim never reached the locked canonical probe")??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canonical_probe_errors_remain_failures_and_pool_recovers() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = Database::open(&raw).await?;
    let outcome = async {
        let mut control = PgConnection::connect(&db.fixture.url).await?;
        // The empty outbox isolates error propagation and pool recovery.
        // A cancelled future can leave its sent probe running on the server;
        // due work could consume a slot even though its transaction rolls back.
        // These empty-outbox checks establish no guarantee against replay or
        // unknown sequence allocation after cancellation or a lost response.
        let before = sequence(&mut control).await?;
        // Ledger raises this fixture's requested pool limit of one to two.
        // Hold one so SET and the claim use the same other pooled session.
        let _spare = db.ledger.pool.acquire().await?;
        let mut blocker = PgConnection::connect(&db.fixture.url).await?;
        sqlx::raw_sql("BEGIN; LOCK qbit_block_candidate_outbox IN ACCESS EXCLUSIVE MODE").execute(&mut blocker).await?;
        sqlx::raw_sql("SET statement_timeout='100ms'").execute(&db.ledger.pool).await?;
        let error = db.ledger.claim_candidate(60).await.err().context("timeout became idle")?;
        let code = error.downcast_ref::<sqlx::Error>().and_then(|e| e.as_database_error()).and_then(|e| e.code());
        ensure!(code.as_deref() == Some("57014"), "expected query cancellation: {error:#}");
        sqlx::raw_sql("SET statement_timeout='15s'").execute(&db.ledger.pool).await?;
        let ledger = db.ledger.clone();
        let cancelled = tokio::spawn(async move { ledger.claim_candidate(60).await });
        wait_for_probe_lock(&mut control).await?;
        cancelled.abort();
        ensure!(cancelled.await.unwrap_err().is_cancelled());
        sqlx::raw_sql("ROLLBACK").execute(&mut blocker).await?;
        // Pool acquisition/transaction rollback must recover after cancellation.
        ensure!(tokio::time::timeout(Duration::from_secs(10), db.ledger.claim_candidate(60)).await??.is_none());
        assert_eq!(sequence(&mut control).await?, before);
        sqlx::raw_sql("BEGIN; LOCK qbit_block_candidate_outbox IN ACCESS EXCLUSIVE MODE").execute(&mut blocker).await?;
        let ledger = db.ledger.clone();
        let disconnected = tokio::spawn(async move { ledger.claim_candidate(60).await });
        wait_for_probe_lock(&mut control).await?;
        let killed: bool = sqlx::query_scalar("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query=$1")
            .bind(Ledger::due_work_probe_sql()).fetch_one(&mut control).await?;
        ensure!(killed);
        ensure!(tokio::time::timeout(Duration::from_secs(10), disconnected).await??.is_err(), "lost connection became idle");
        sqlx::raw_sql("ROLLBACK").execute(&mut blocker).await?;
        ensure!(db.ledger.claim_candidate(60).await?.is_none());
        assert_eq!(sequence(&mut control).await?, before);
        Ok(())
    }.await;
    db.close(outcome).await
}
