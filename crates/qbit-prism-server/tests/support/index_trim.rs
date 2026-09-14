//! Migration 012 (#153): the share ledger index trim, applied online.
//!
//! The migration replaces two covering indexes with narrower ones and drops
//! two without a consumer. It is the first migration the migrator applies
//! outside its transaction, with `CREATE INDEX CONCURRENTLY` and `DROP
//! INDEX CONCURRENTLY`, so these tests cover what that changes: the index
//! set a fresh and a 2.x.x source end up with, that appends keep landing
//! while a build waits, and that an interrupted, pre-built or foreign index
//! under a reserved name is rebuilt, kept or refused.
use super::*;
use anyhow::ensure;
use qbit_prism_server::ledger::REQUIRED_SCHEMA_VERSIONS;
use sqlx::Row;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::{sleep, timeout};

const SEQ_WALK: &str = "qbit_share_ledger_accepted_seq_walk_idx";
const MINER_HISTORY: &str = "qbit_share_ledger_accepted_miner_history_idx";
const SEQ_WALK_DEFINITION: &str = "CREATE INDEX qbit_share_ledger_accepted_seq_walk_idx ON qbit_share_ledger USING btree (share_seq DESC) INCLUDE (job_issued_at, accepted_at, share_difficulty) WHERE accepted";
const MINER_HISTORY_DEFINITION: &str = "CREATE INDEX qbit_share_ledger_accepted_miner_history_idx ON qbit_share_ledger USING btree (miner_id, accepted_at DESC) INCLUDE (share_difficulty, share_seq, share_id) WHERE accepted";
/// The release indexes 012 drops, and the 001 DDL that restores them.
const REPLACED: [(&str, &str); 4] = [
    (
        "qbit_share_ledger_accepted_seq_window_idx",
        "CREATE INDEX qbit_share_ledger_accepted_seq_window_idx ON qbit_share_ledger (share_seq DESC) INCLUDE (job_issued_at, accepted_at, miner_id, payout_order_key, p2mr_program, share_difficulty, share_id) WHERE accepted",
    ),
    (
        "qbit_share_ledger_accepted_miner_recent_idx",
        "CREATE INDEX qbit_share_ledger_accepted_miner_recent_idx ON qbit_share_ledger (miner_id, accepted_at DESC) INCLUDE (share_difficulty, share_seq, share_id, payout_order_key) WHERE accepted",
    ),
    (
        "qbit_share_ledger_accepted_window_idx",
        "CREATE INDEX qbit_share_ledger_accepted_window_idx ON qbit_share_ledger (job_issued_at, share_seq DESC) WHERE accepted",
    ),
    (
        "qbit_share_ledger_template_height_idx",
        "CREATE INDEX qbit_share_ledger_template_height_idx ON qbit_share_ledger (template_height, share_seq) WHERE accepted",
    ),
];
const KEPT: [&str; 4] = [
    "qbit_share_ledger_accepted_block_suffix_idx",
    "qbit_share_ledger_accepted_recent_idx",
    "qbit_share_ledger_pkey",
    "qbit_share_ledger_share_id_key",
];

/// Every index on the share ledger: name, definition as the server renders
/// it (without the schema it always puts on the table), validity and the
/// index relation's OID, ordered by name.
async fn ledger_indexes(pool: &PgPool) -> Result<Vec<(String, String, bool, String)>> {
    let rows = sqlx::query("SELECT i.relname::text AS name,replace(pg_get_indexdef(x.indexrelid),current_schema()||'.','') AS definition,x.indisvalid AS valid,x.indexrelid::text AS oid FROM pg_index x JOIN pg_class i ON i.oid=x.indexrelid WHERE x.indrelid='qbit_share_ledger'::regclass ORDER BY 1")
        .fetch_all(pool)
        .await?;
    rows.iter()
        .map(|row| {
            Ok((
                row.try_get("name")?,
                row.try_get("definition")?,
                row.try_get("valid")?,
                row.try_get("oid")?,
            ))
        })
        .collect()
}

async fn schema_versions(pool: &PgPool) -> Result<Vec<i32>> {
    Ok(
        sqlx::query_scalar("SELECT version FROM qbit_prism_schema_migrations ORDER BY version")
            .fetch_all(pool)
            .await?,
    )
}

/// The index set 012 leaves: the kept release indexes and the two
/// replacements, every one valid, with 12 recorded.
async fn assert_trimmed(pool: &PgPool) -> Result<Vec<(String, String, bool, String)>> {
    let indexes = ledger_indexes(pool).await?;
    let mut expected: Vec<&str> = KEPT.to_vec();
    expected.extend([MINER_HISTORY, SEQ_WALK]);
    expected.sort_unstable();
    let names: Vec<&str> = indexes.iter().map(|(name, ..)| name.as_str()).collect();
    assert_eq!(names, expected);
    for (name, definition, valid, _) in &indexes {
        assert!(valid, "{name} is not valid");
        if name == SEQ_WALK {
            assert_eq!(definition, SEQ_WALK_DEFINITION);
        }
        if name == MINER_HISTORY {
            assert_eq!(definition, MINER_HISTORY_DEFINITION);
        }
    }
    assert_eq!(schema_versions(pool).await?, REQUIRED_SCHEMA_VERSIONS);
    Ok(indexes)
}

/// Put a migrated database back to 11: the release indexes as 001 creates
/// them, without the replacements and without the record.
pub(super) async fn undo_012(pool: &PgPool) -> Result<()> {
    assert_eq!(schema_versions(pool).await?, REQUIRED_SCHEMA_VERSIONS);
    let mut sql = format!(
        "DELETE FROM qbit_prism_schema_migrations WHERE version=12; DROP INDEX {SEQ_WALK}; DROP INDEX {MINER_HISTORY};"
    );
    for (_, restore) in REPLACED {
        sql.push_str(restore);
        sql.push(';');
    }
    sqlx::raw_sql(&sql).execute(pool).await?;
    assert_eq!(schema_versions(pool).await?, [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    Ok(())
}

/// A share row as a 2.x.x or native writer leaves it, without the native
/// ordering lock: enough for index builds and for qbit_prism_window.
async fn insert_share<'e>(
    executor: impl sqlx::Executor<'e, Database = sqlx::Postgres>,
    id: u64,
    miner: &str,
) -> Result<()> {
    sqlx::query("INSERT INTO qbit_share_ledger(share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,accepted,writer_id,writer_epoch) VALUES($1,$2,$2,decode(repeat('11',32),'hex'),1,100,100,'job',to_timestamp(1),1,clock_timestamp(),true,'index-trim',0)")
        .bind(format!("{miner}:{id:064x}"))
        .bind(miner)
        .execute(executor)
        .await?;
    Ok(())
}

async fn share_count(pool: &PgPool) -> Result<i64> {
    Ok(sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger")
        .fetch_one(pool)
        .await?)
}

/// An empty ledger has no append to block, so 012 is applied inside the
/// migration transaction like every other migration; the online runner is
/// for a ledger with rows (the tests below).
#[tokio::test]
async fn migration_012_trims_the_indexes_of_an_empty_ledger_in_the_transaction_and_a_restart_keeps_them(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    let a = db.ledger("a").await?;
    let trimmed = assert_trimmed(&pool).await?;
    // Recorded after every other migration of the same run.
    let recorded_last: bool = sqlx::query_scalar("SELECT (SELECT applied_at FROM qbit_prism_schema_migrations WHERE version=12) >= (SELECT max(applied_at) FROM qbit_prism_schema_migrations WHERE version<12)")
        .fetch_one(&pool)
        .await?;
    assert!(recorded_last);
    a.append(share(1), None).await?;
    a.append(share(2), None).await?;
    // The frozen 001 window function still walks the ledger on the new
    // index set, and the native snapshot reads the same rows.
    let counted: i64 =
        sqlx::query_scalar("SELECT count(*) FROM qbit_prism_window(clock_timestamp(), 1600)")
            .fetch_one(&pool)
            .await?;
    assert_eq!(counted, 2);
    assert_eq!(a.snapshot(100).await?.shares.len(), 2);
    // A second start applies nothing and keeps the same indexes.
    let b = db.ledger("b").await?;
    assert_eq!(ledger_indexes(&pool).await?, trimmed);
    db.close(vec![a, b]).await
}

#[tokio::test]
async fn migration_012_replaces_the_release_indexes_of_a_frozen_2x_source_and_keeps_its_rows(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    two_x::apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    for id in 1..=3 {
        insert_share(&pool, id, "alice").await?;
    }
    let release = ledger_indexes(&pool).await?;
    for (name, _) in REPLACED {
        assert!(
            release.iter().any(|(found, ..)| found == name),
            "{name} missing from the frozen release schema"
        );
    }
    let ledger = db.ledger("cutover").await?;
    assert_trimmed(&pool).await?;
    assert_eq!(share_count(&pool).await?, 3);
    let counted: i64 =
        sqlx::query_scalar("SELECT count(*) FROM qbit_prism_window(clock_timestamp(), 1600)")
            .fetch_one(&pool)
            .await?;
    assert_eq!(counted, 3);
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn migration_012_builds_its_indexes_without_blocking_appends() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    let first = db.ledger("first").await?;
    undo_012(&pool).await?;
    // A writer in the middle of its transaction. A plain CREATE INDEX
    // would queue behind its row lock, and every later append behind that.
    let mut writer = pool.begin().await?;
    insert_share(&mut *writer, 1, "alice").await?;
    // The migration runs on this task, polled alongside the observer below;
    // `finished` says whether it returned before the writer committed.
    let finished = AtomicBool::new(false);
    let migrate = async {
        let ledger = Ledger::connect(&db.url, "online".into(), 8, true).await;
        finished.store(true, Ordering::SeqCst);
        ledger
    };
    let observe = async {
        // The concurrent build waits for the open writer before it can
        // finish.
        timeout(Duration::from_secs(60), async {
            loop {
                let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE query LIKE 'CREATE INDEX CONCURRENTLY%' AND wait_event_type='Lock')")
                    .fetch_one(&pool)
                    .await?;
                if waiting {
                    return Ok::<_, anyhow::Error>(());
                }
                ensure!(
                    !finished.load(Ordering::SeqCst),
                    "the online migration finished while a writer transaction was open"
                );
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .context("the concurrent build never waited for the open writer")??;
        // Appends keep landing while it waits.
        timeout(Duration::from_secs(5), async {
            let mut tx = pool.begin().await?;
            insert_share(&mut *tx, 2, "bob").await?;
            tx.commit().await?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("an append blocked behind the online index build")??;
        ensure!(!finished.load(Ordering::SeqCst));
        writer.commit().await?;
        Ok::<_, anyhow::Error>(())
    };
    let (online, observed) = tokio::join!(migrate, observe);
    observed?;
    let online = online?;
    assert_trimmed(&pool).await?;
    assert_eq!(share_count(&pool).await?, 2);
    db.close(vec![first, online]).await
}

#[tokio::test]
async fn migration_012_resumes_an_interrupted_build_keeps_its_own_index_and_refuses_another(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    let first = db.ledger("first").await?;
    insert_share(&pool, 1, "alice").await?;
    insert_share(&pool, 2, "alice").await?;
    undo_012(&pool).await?;
    // Without initialize, a start refuses the database, naming the gap.
    let error = Ledger::connect(&db.url, "cold".into(), 8, false)
        .await
        .err()
        .context("a non-initializing start accepted a database at 10")?
        .to_string();
    assert!(error.contains("missing migration(s) 12"), "{error}");
    // An interrupted build: a unique build over duplicate miners fails and
    // leaves the reserved name behind as an invalid index.
    let failed = sqlx::raw_sql(&format!(
        "CREATE UNIQUE INDEX CONCURRENTLY {SEQ_WALK} ON qbit_share_ledger (miner_id)"
    ))
    .execute(&pool)
    .await;
    assert!(failed.is_err(), "duplicate miners must fail a unique build");
    let leftover = ledger_indexes(&pool)
        .await?
        .into_iter()
        .find(|(name, ..)| name == SEQ_WALK)
        .context("the failed build left no index")?;
    assert!(!leftover.2, "the failed build's index must be invalid");
    let resumed = db.ledger("resumed").await?;
    assert_trimmed(&pool).await?;
    // A valid index under a reserved name with another definition is
    // refused, naming it; nothing is built, dropped or recorded.
    undo_012(&pool).await?;
    sqlx::raw_sql(&format!(
        "CREATE INDEX {SEQ_WALK} ON qbit_share_ledger (share_seq)"
    ))
    .execute(&pool)
    .await?;
    let before = ledger_indexes(&pool).await?;
    let error = db
        .ledger("refused")
        .await
        .err()
        .context("the online migration adopted an index it did not declare")?
        .to_string();
    assert!(
        error.contains(&format!(
            "index {SEQ_WALK} on qbit_share_ledger already exists with a different definition"
        )),
        "{error}"
    );
    assert!(error.contains("migrate again"), "{error}");
    assert_eq!(ledger_indexes(&pool).await?, before);
    assert_eq!(schema_versions(&pool).await?, [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    // The declared definition under the reserved name is an earlier build
    // of the migration's own: kept as it is, not rebuilt.
    sqlx::raw_sql(&format!("DROP INDEX {SEQ_WALK}; {SEQ_WALK_DEFINITION}"))
        .execute(&pool)
        .await?;
    let prebuilt = ledger_indexes(&pool)
        .await?
        .into_iter()
        .find(|(name, ..)| name == SEQ_WALK)
        .context("the pre-built index is missing")?;
    let adopted = db.ledger("adopted").await?;
    let trimmed = assert_trimmed(&pool).await?;
    let kept = trimmed
        .iter()
        .find(|(name, ..)| name == SEQ_WALK)
        .context("the adopted index is missing")?;
    assert_eq!(kept.3, prebuilt.3, "the pre-built index was rebuilt");
    assert_eq!(share_count(&pool).await?, 2);
    db.close(vec![first, resumed, adopted]).await
}
