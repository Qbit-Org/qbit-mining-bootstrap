//! Migration 016 (#144): the share ledger partition conversion.
//!
//! The conversion attaches the release table as the first partition of a
//! new partitioned parent after a validated bound, inside the migration
//! transaction where the ledger is empty and through the resumable online
//! runner otherwise. These tests cover the shape both paths leave, that
//! rows and the sequence survive the online path from every stage it can
//! resume at, that a reserved name is refused before any DDL, that the
//! swap waits for an open writer with a lock timeout rather than queuing
//! every append behind it, that a bound whose headroom ran out is moved,
//! and what per-leaf share_id uniqueness means for the append path.
//!
//! `undo_016` and `undo_015` put a migrated database back to what a build
//! without those migrations left, for the older upgrade fixtures that
//! simulate such builds.
use super::*;
use anyhow::ensure;
use qbit_prism_server::ledger::REQUIRED_SCHEMA_VERSIONS;
use sqlx::Row;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::{sleep, timeout};

/// The default partition width, `qbit_prism_share_partitioning.partition_rows`.
pub(super) const WIDTH: i64 = 16_777_216;

const PARENT_INDEXES: [&str; 5] = [
    "qbit_share_ledger_accepted_block_suffix_idx",
    "qbit_share_ledger_accepted_miner_history_idx",
    "qbit_share_ledger_accepted_recent_idx",
    "qbit_share_ledger_accepted_seq_walk_idx",
    "qbit_share_ledger_pkey",
];
const LEAF_SUFFIXES: [&str; 6] = [
    "_accepted_block_suffix_idx",
    "_accepted_miner_history_idx",
    "_accepted_recent_idx",
    "_accepted_seq_walk_idx",
    "_pkey",
    "_share_id_key",
];

pub(super) async fn schema_versions(pool: &PgPool) -> Result<Vec<i32>> {
    Ok(
        sqlx::query_scalar("SELECT version FROM qbit_prism_schema_migrations ORDER BY version")
            .fetch_all(pool)
            .await?,
    )
}

async fn relkind(pool: &PgPool, name: &str) -> Result<Option<String>> {
    Ok(
        sqlx::query_scalar("SELECT c.relkind::text FROM pg_class c WHERE c.oid=to_regclass($1)")
            .bind(name)
            .fetch_optional(pool)
            .await?,
    )
}

/// The index names on one relation, sorted.
async fn index_names(pool: &PgPool, table: &str) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(
        "SELECT i.relname::text FROM pg_index x JOIN pg_class i ON i.oid=x.indexrelid WHERE x.indrelid=to_regclass($1) ORDER BY 1",
    )
    .bind(table)
    .fetch_all(pool)
    .await?)
}

/// The partitions PostgreSQL has attached, by name with the bound it holds.
async fn attached(pool: &PgPool) -> Result<Vec<(String, String)>> {
    let rows = sqlx::query("SELECT c.relname::text AS name,pg_get_expr(c.relpartbound,c.oid) AS bound FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid WHERE i.inhparent='qbit_share_ledger'::regclass ORDER BY 1")
        .fetch_all(pool)
        .await?;
    rows.iter()
        .map(|row| Ok((row.try_get("name")?, row.try_get("bound")?)))
        .collect()
}

/// The catalog rows: name, lower (None is MINVALUE), upper, state.
pub(super) async fn catalog(pool: &PgPool) -> Result<Vec<(String, Option<i64>, i64, String)>> {
    let rows = sqlx::query("SELECT partition_name,lower_seq,upper_seq,state FROM qbit_prism_share_partitions ORDER BY upper_seq")
        .fetch_all(pool)
        .await?;
    rows.iter()
        .map(|row| {
            Ok((
                row.try_get("partition_name")?,
                row.try_get("lower_seq")?,
                row.try_get("upper_seq")?,
                row.try_get("state")?,
            ))
        })
        .collect()
}

async fn conversion_bound(pool: &PgPool) -> Result<Option<i64>> {
    Ok(sqlx::query_scalar(
        "SELECT conversion_bound FROM qbit_prism_share_partitioning WHERE singleton",
    )
    .fetch_one(pool)
    .await?)
}

async fn share_count(pool: &PgPool) -> Result<i64> {
    Ok(sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger")
        .fetch_one(pool)
        .await?)
}

/// A share row as a writer leaves it, at an explicit `share_seq` when one
/// is given, without the native ordering lock.
async fn insert_share<'e>(
    executor: impl sqlx::Executor<'e, Database = sqlx::Postgres>,
    seq: Option<i64>,
    id: u64,
    miner: &str,
) -> Result<()> {
    sqlx::query("INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,accepted,writer_id,writer_epoch) VALUES(COALESCE($3,nextval('qbit_share_ledger_share_seq_seq')),$1,$2,$2,decode(repeat('11',32),'hex'),1,100,100,'job',to_timestamp(1),1,clock_timestamp(),true,'share-partitions',0)")
        .bind(format!("{miner}:{id:064x}"))
        .bind(miner)
        .bind(seq)
        .execute(executor)
        .await?;
    Ok(())
}

/// Everything the conversion leaves, on either path: a partitioned parent
/// with the release index names, the release table as `_p0` with the leaf
/// set (its own share_id uniqueness included), every attached partition in
/// the catalog and the catalog's attached rows attached, the immutability
/// trigger on the parent and every leaf, the release function returning
/// the parent's row type, and 16 recorded after every other version.
pub(super) async fn assert_converted(pool: &PgPool) -> Result<()> {
    assert_eq!(
        relkind(pool, "qbit_share_ledger").await?.as_deref(),
        Some("p")
    );
    assert_eq!(
        index_names(pool, "qbit_share_ledger").await?,
        PARENT_INDEXES
    );
    let partitions = attached(pool).await?;
    assert!(
        partitions
            .iter()
            .any(|(name, bound)| name == "qbit_share_ledger_p0" && bound.contains("MINVALUE")),
        "{partitions:?}"
    );
    let rows = catalog(pool).await?;
    let attached_rows: Vec<&str> = rows
        .iter()
        .filter(|(_, _, _, state)| state == "attached")
        .map(|(name, ..)| name.as_str())
        .collect();
    let mut attached_names: Vec<&str> = partitions.iter().map(|(name, _)| name.as_str()).collect();
    attached_names.sort_unstable();
    let mut expected = attached_rows.clone();
    expected.sort_unstable();
    assert_eq!(attached_names, expected, "catalog and pg_inherits disagree");
    for (name, _) in &partitions {
        let expected: Vec<String> = LEAF_SUFFIXES
            .iter()
            .map(|suffix| format!("{name}{suffix}"))
            .collect();
        assert_eq!(index_names(pool, name).await?, expected);
        let trigger: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=to_regclass($1) AND tgname='qbit_prism_immutable_share_history')")
            .bind(name).fetch_one(pool).await?;
        assert!(trigger, "{name} has no immutability trigger");
    }
    let parent_trigger: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='qbit_share_ledger'::regclass AND tgname='qbit_prism_immutable_share_history')")
        .fetch_one(pool).await?;
    assert!(parent_trigger);
    let returns_parent: bool = sqlx::query_scalar("SELECT (SELECT prorettype FROM pg_proc WHERE oid='qbit_shares_since_template_height(bigint)'::regprocedure)=(SELECT reltype FROM pg_class WHERE oid='qbit_share_ledger'::regclass)")
        .fetch_one(pool).await?;
    assert!(
        returns_parent,
        "qbit_shares_since_template_height returns the old row type"
    );
    let converted: bool = sqlx::query_scalar(
        "SELECT converted_at IS NOT NULL FROM qbit_prism_share_partitioning WHERE singleton",
    )
    .fetch_one(pool)
    .await?;
    assert!(converted);
    assert_eq!(schema_versions(pool).await?, REQUIRED_SCHEMA_VERSIONS);
    let recorded_last: bool = sqlx::query_scalar("SELECT (SELECT applied_at FROM qbit_prism_schema_migrations WHERE version=16) >= (SELECT max(applied_at) FROM qbit_prism_schema_migrations WHERE version<16)")
        .fetch_one(pool)
        .await?;
    assert!(recorded_last);
    Ok(())
}

/// Undo 016 on a converted ledger: detach and drop the empty lead
/// partitions, detach the release table, give it the sequence back before
/// the parent (which owns it) is dropped, drop the parent and the release
/// function bound to its row type, rename the release table and its six
/// indexes back, drop the bound, recreate the function on the plain table,
/// clear the catalog and remove the version row. The 015 objects stay.
pub(super) async fn undo_016(pool: &PgPool) -> Result<()> {
    ensure!(
        relkind(pool, "qbit_share_ledger").await?.as_deref() == Some("p"),
        "undo_016 needs a converted ledger"
    );
    let versions = schema_versions(pool).await?;
    ensure!(versions.contains(&16));
    sqlx::raw_sql(
        "DO $$
         DECLARE part text;
         BEGIN
           FOR part IN SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent='qbit_share_ledger'::regclass AND inhrelid<>'qbit_share_ledger_p0'::regclass LOOP
             EXECUTE format('ALTER TABLE qbit_share_ledger DETACH PARTITION %I', part);
             EXECUTE format('DROP TABLE %I', part);
           END LOOP;
         END $$;
         ALTER TABLE qbit_share_ledger DETACH PARTITION qbit_share_ledger_p0;
         ALTER SEQUENCE qbit_share_ledger_share_seq_seq OWNED BY qbit_share_ledger_p0.share_seq;
         DROP FUNCTION qbit_shares_since_template_height(bigint);
         DROP TABLE qbit_share_ledger;
         ALTER TABLE qbit_share_ledger_p0 RENAME TO qbit_share_ledger;
         ALTER INDEX qbit_share_ledger_p0_pkey RENAME TO qbit_share_ledger_pkey;
         ALTER INDEX qbit_share_ledger_p0_share_id_key RENAME TO qbit_share_ledger_share_id_key;
         ALTER INDEX qbit_share_ledger_p0_accepted_recent_idx RENAME TO qbit_share_ledger_accepted_recent_idx;
         ALTER INDEX qbit_share_ledger_p0_accepted_block_suffix_idx RENAME TO qbit_share_ledger_accepted_block_suffix_idx;
         ALTER INDEX qbit_share_ledger_p0_accepted_seq_walk_idx RENAME TO qbit_share_ledger_accepted_seq_walk_idx;
         ALTER INDEX qbit_share_ledger_p0_accepted_miner_history_idx RENAME TO qbit_share_ledger_accepted_miner_history_idx;
         ALTER TABLE qbit_share_ledger DROP CONSTRAINT qbit_share_ledger_p0_bound;
         CREATE FUNCTION qbit_shares_since_template_height(min_template_height bigint)
         RETURNS SETOF qbit_share_ledger LANGUAGE sql STABLE AS $$
             SELECT * FROM qbit_share_ledger WHERE accepted AND template_height >= min_template_height ORDER BY share_seq ASC;
         $$;
         DELETE FROM qbit_prism_share_partitions;
         UPDATE qbit_prism_share_partitioning SET converted_at=NULL, conversion_bound=NULL, updated_at=clock_timestamp();
         DELETE FROM qbit_prism_schema_migrations WHERE version=16",
    )
    .execute(pool)
    .await?;
    assert_eq!(
        relkind(pool, "qbit_share_ledger").await?.as_deref(),
        Some("r")
    );
    assert_eq!(
        schema_versions(pool).await?,
        versions
            .into_iter()
            .filter(|version| *version != 16)
            .collect::<Vec<_>>()
    );
    Ok(())
}

/// Undo 015 on a ledger 016 has been undone on: the catalog, the
/// functions, the solver columns and the version row go, and the two
/// release foreign keys onto share_id come back under their release names.
pub(super) async fn undo_015(pool: &PgPool) -> Result<()> {
    ensure!(
        relkind(pool, "qbit_share_ledger").await?.as_deref() == Some("r"),
        "undo 016 before 015"
    );
    let versions = schema_versions(pool).await?;
    ensure!(versions.contains(&15) && !versions.contains(&16));
    sqlx::raw_sql(
        "DELETE FROM qbit_prism_schema_migrations WHERE version=15;
         DROP FUNCTION qbit_prism_share_ledger_convert_swap();
         DROP FUNCTION qbit_prism_share_ledger_convert_validate();
         DROP FUNCTION qbit_prism_share_ledger_convert_prepare(bigint);
         DROP FUNCTION qbit_prism_share_partition_ensure();
         DROP FUNCTION qbit_prism_share_partition_create(text,bigint,bigint);
         DROP FUNCTION qbit_prism_share_probe_floor();
         DROP FUNCTION qbit_prism_share_next_seq();
         DROP TABLE qbit_prism_share_partitions;
         DROP TABLE qbit_prism_share_partitioning;
         ALTER TABLE qbit_pool_blocks DROP COLUMN solver_miner_id, DROP COLUMN solver_share_id, DROP COLUMN solver_share_difficulty, DROP COLUMN solver_network_difficulty;
         ALTER TABLE qbit_block_candidate_outbox ADD CONSTRAINT qbit_block_candidate_outbox_share_id_fkey FOREIGN KEY (share_id) REFERENCES qbit_share_ledger(share_id);
         ALTER TABLE qbit_prism_share_hashes ADD CONSTRAINT qbit_prism_share_hashes_share_id_fkey FOREIGN KEY (share_id) REFERENCES qbit_share_ledger(share_id)",
    )
    .execute(pool)
    .await?;
    assert_eq!(
        schema_versions(pool).await?,
        versions
            .into_iter()
            .filter(|version| *version != 15)
            .collect::<Vec<_>>()
    );
    Ok(())
}

/// The tableoid a share landed in.
async fn partition_of(pool: &PgPool, share_seq: i64) -> Result<String> {
    Ok(sqlx::query_scalar(
        "SELECT tableoid::regclass::text FROM qbit_share_ledger WHERE share_seq=$1",
    )
    .bind(share_seq)
    .fetch_one(pool)
    .await?)
}

/// A fresh ledger is converted inside the migration transaction, the
/// cutover locks excluding writers: its first partition is one cell,
/// the lead is attached above it, appends route into the first cell, and
/// a second start applies nothing.
#[tokio::test]
async fn migration_016_converts_an_empty_ledger_in_the_transaction_and_appends_route_to_the_first_cell(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    let a = db.ledger("a").await?;
    assert_converted(&pool).await?;
    let rows = catalog(&pool).await?;
    assert_eq!(
        rows.iter()
            .map(|(name, lower, upper, _)| (name.as_str(), *lower, *upper))
            .collect::<Vec<_>>(),
        [
            ("qbit_share_ledger_p0", None, WIDTH),
            ("qbit_share_ledger_p1", Some(WIDTH), 2 * WIDTH),
            ("qbit_share_ledger_p2", Some(2 * WIDTH), 3 * WIDTH),
            ("qbit_share_ledger_p3", Some(3 * WIDTH), 4 * WIDTH),
            ("qbit_share_ledger_p4", Some(4 * WIDTH), 5 * WIDTH),
        ]
    );
    assert_eq!(conversion_bound(&pool).await?, Some(WIDTH));
    let first = a.append(share(1), None).await?;
    let second = a.append(share(2), None).await?;
    assert_eq!(
        (first.share.share_seq, second.share.share_seq),
        (1, 2),
        "the sequence starts at 1 and is never restarted"
    );
    assert_eq!(partition_of(&pool, 1).await?, "qbit_share_ledger_p0");
    assert_eq!(a.snapshot(100).await?.shares.len(), 2);
    let counted: i64 =
        sqlx::query_scalar("SELECT count(*) FROM qbit_prism_window(clock_timestamp(), 1600)")
            .fetch_one(&pool)
            .await?;
    assert_eq!(counted, 2);
    let b = db.ledger("b").await?;
    assert_eq!(catalog(&pool).await?, rows);
    assert_converted(&pool).await?;
    db.close(vec![a, b]).await
}

/// A ledger with rows is converted after the commit by the online runner,
/// resumable from every stage: nothing prepared, the bound pending, the
/// bound validated. The rows and the sequence survive each time, the first
/// partition's bound is two widths above the sequence on the grid, and the
/// lead partitions follow it.
#[tokio::test]
async fn migration_016_converts_a_populated_ledger_online_and_resumes_from_every_stage(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    let first = db.ledger("first").await?;
    for id in 1..=5 {
        first.append(share(id), None).await?;
    }
    let mut ledgers = vec![first];
    for prepared in ["nothing", "prepare", "prepare+validate"] {
        // Each round below appends one more share after its conversion.
        let rows = 4 + i64::try_from(ledgers.len())?;
        undo_016(&pool).await?;
        assert_eq!(share_count(&pool).await?, rows);
        if prepared != "nothing" {
            let bound: i64 = sqlx::query_scalar("SELECT qbit_prism_share_ledger_convert_prepare()")
                .fetch_one(&pool)
                .await?;
            assert_eq!(bound, 3 * WIDTH, "next_seq 6 plus two widths, on the grid");
        }
        if prepared == "prepare+validate" {
            let validated: Vec<String> =
                sqlx::query_scalar("SELECT qbit_prism_share_ledger_convert_validate()")
                    .fetch_one(&pool)
                    .await?;
            assert_eq!(validated, ["qbit_share_ledger_p0_bound"]);
        }
        // Without initialize, a start refuses the database, naming the gap.
        let error = Ledger::connect(&db.url, "cold".into(), 8, false)
            .await
            .err()
            .context("a non-initializing start accepted a database missing 016")?
            .to_string();
        assert!(error.contains("missing migration(s) 16"), "{error}");
        let resumed = db.ledger(&format!("resumed-{prepared}")).await?;
        assert_converted(&pool).await?;
        assert_eq!(share_count(&pool).await?, rows);
        let rows = catalog(&pool).await?;
        assert_eq!(
            rows[0],
            (
                "qbit_share_ledger_p0".to_owned(),
                None,
                3 * WIDTH,
                "attached".to_owned()
            )
        );
        assert_eq!(rows[1].0, "qbit_share_ledger_p3");
        assert_eq!((rows[1].1, rows[1].2), (Some(3 * WIDTH), 4 * WIDTH));
        assert_eq!(conversion_bound(&pool).await?, Some(3 * WIDTH));
        let next = resumed
            .append(share(100 + ledgers.len() as u64), None)
            .await?;
        assert_eq!(next.share.share_seq, 5 + ledgers.len() as u64);
        assert_eq!(
            partition_of(&pool, i64::try_from(next.share.share_seq)?).await?,
            "qbit_share_ledger_p0",
            "rows keep landing in the release table until the sequence reaches the bound"
        );
        ledgers.push(resumed);
    }
    db.close(ledgers).await
}

/// A relation under a name the partitions take is refused before the
/// bound is prepared: the table stays plain, no constraint is added and
/// nothing is recorded; once the name is free the conversion goes through.
#[tokio::test]
async fn migration_016_refuses_a_reserved_partition_name_before_any_ddl() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    let first = db.ledger("first").await?;
    first.append(share(1), None).await?;
    undo_016(&pool).await?;
    let versions = schema_versions(&pool).await?;
    sqlx::raw_sql("CREATE VIEW qbit_share_ledger_p7 AS SELECT 1 AS one")
        .execute(&pool)
        .await?;
    let error = db
        .ledger("collision")
        .await
        .err()
        .context("the conversion ran with a reserved name taken")?
        .to_string();
    assert!(error.contains("view qbit_share_ledger_p7"), "{error}");
    assert!(error.contains("nothing was changed"), "{error}");
    assert_eq!(
        relkind(&pool, "qbit_share_ledger").await?.as_deref(),
        Some("r")
    );
    let bound_present: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='qbit_share_ledger'::regclass AND conname='qbit_share_ledger_p0_bound')")
        .fetch_one(&pool).await?;
    assert!(!bound_present, "the bound was prepared before the refusal");
    assert_eq!(schema_versions(&pool).await?, versions);
    assert_eq!(conversion_bound(&pool).await?, None);
    sqlx::raw_sql("DROP VIEW qbit_share_ledger_p7")
        .execute(&pool)
        .await?;
    let converted = db.ledger("converted").await?;
    assert_converted(&pool).await?;
    assert_eq!(share_count(&pool).await?, 1);
    db.close(vec![first, converted]).await
}

/// The prepare and swap steps take the table lock with a short timeout
/// and retry, so an open writer delays the conversion without holding the
/// appends queued behind an ACCESS EXCLUSIVE request; once the writer
/// commits, the conversion completes with its row.
#[tokio::test]
async fn migration_016_waits_for_an_open_writer_without_blocking_appends() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    let first = db.ledger("first").await?;
    first.append(share(1), None).await?;
    undo_016(&pool).await?;
    let mut writer = pool.begin().await?;
    insert_share(&mut *writer, None, 2, "alice").await?;
    let finished = AtomicBool::new(false);
    let migrate = async {
        let ledger = Ledger::connect(&db.url, "online".into(), 8, true).await;
        finished.store(true, Ordering::SeqCst);
        ledger
    };
    let observe = async {
        // The prepare step's ACCESS EXCLUSIVE request waits behind the
        // writer's row lock on the table, and is retried with a timeout.
        timeout(Duration::from_secs(60), async {
            loop {
                let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity a JOIN pg_locks l ON l.pid=a.pid WHERE a.query LIKE 'SELECT qbit_prism_share_ledger_convert_prepare%' AND a.wait_event_type='Lock' AND l.locktype='relation' AND l.relation='qbit_share_ledger'::regclass AND NOT l.granted)")
                    .fetch_one(&pool)
                    .await?;
                if waiting {
                    return Ok::<_, anyhow::Error>(());
                }
                ensure!(
                    !finished.load(Ordering::SeqCst),
                    "the conversion finished while a writer transaction was open"
                );
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .context("the conversion never waited for the open writer")??;
        // Appends keep landing while it retries: the request is dropped at
        // each lock timeout rather than queued.
        timeout(Duration::from_secs(10), async {
            let mut tx = pool.begin().await?;
            insert_share(&mut *tx, None, 3, "bob").await?;
            tx.commit().await?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("an append blocked behind the conversion's lock request")??;
        ensure!(!finished.load(Ordering::SeqCst));
        writer.commit().await?;
        Ok::<_, anyhow::Error>(())
    };
    let (online, observed) = tokio::join!(migrate, observe);
    observed?;
    let online = online?;
    assert_converted(&pool).await?;
    assert_eq!(share_count(&pool).await?, 3);
    db.close(vec![first, online]).await
}

/// A bound prepared by an earlier, interrupted run is moved further out
/// when the sequence has come within one width of it, then validated and
/// swapped; the first partition's bound is the new one.
#[tokio::test]
async fn migration_016_moves_a_bound_whose_headroom_ran_out() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    let first = db.ledger("first").await?;
    first.append(share(1), None).await?;
    undo_016(&pool).await?;
    let bound: i64 = sqlx::query_scalar("SELECT qbit_prism_share_ledger_convert_prepare(10)")
        .fetch_one(&pool)
        .await?;
    assert_eq!(bound, WIDTH);
    // The writers ran on: the sequence is now within one width of the bound.
    sqlx::query("SELECT setval('qbit_share_ledger_share_seq_seq',$1)")
        .bind(WIDTH - 5)
        .execute(&pool)
        .await?;
    let resumed = db.ledger("resumed").await?;
    assert_converted(&pool).await?;
    assert_eq!(conversion_bound(&pool).await?, Some(3 * WIDTH));
    assert_eq!(catalog(&pool).await?[0].2, 3 * WIDTH);
    let validated: bool = sqlx::query_scalar("SELECT convalidated FROM pg_constraint WHERE conrelid='qbit_share_ledger_p0'::regclass AND conname='qbit_share_ledger_p0_bound'")
        .fetch_one(&pool).await?;
    assert!(validated);
    let next = resumed.append(share(2), None).await?;
    assert_eq!(next.share.share_seq, u64::try_from(WIDTH - 4)?);
    db.close(vec![first, resumed]).await
}

/// share_id uniqueness is per leaf after the conversion. The append path
/// is what keeps it global: it consults qbit_prism_share_hashes first, so
/// an exact replay of a share in an older partition is matched (bounded
/// probe first, unbounded on a miss), a payload mismatch is refused, and
/// another share_id with the same header hash is refused. A row inserted
/// around the append path with a share_id another leaf holds is the one
/// duplicate PostgreSQL no longer refuses, which the design record states.
#[tokio::test]
async fn share_id_uniqueness_is_per_leaf_and_the_append_path_keeps_it_global() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    let ledger = db.ledger("a").await?;
    let original = ledger.append(share(1), None).await?;
    assert_eq!(original.share.share_seq, 1);
    // Move the sequence into the second cell, well past the probe floor of
    // the first row.
    sqlx::query("SELECT setval('qbit_share_ledger_share_seq_seq',$1)")
        .bind(3 * WIDTH + 10)
        .execute(&pool)
        .await?;
    let created: i32 = sqlx::query_scalar("SELECT qbit_prism_share_partition_ensure()")
        .fetch_one(&pool)
        .await?;
    assert!(created >= 1);
    let floor: i64 = sqlx::query_scalar("SELECT qbit_prism_share_probe_floor()")
        .fetch_one(&pool)
        .await?;
    assert!(
        floor > 1,
        "the first row is below the probe floor ({floor})"
    );
    let later = ledger.append(share(2), None).await?;
    assert_eq!(later.share.share_seq, u64::try_from(3 * WIDTH + 11)?);
    assert_eq!(
        partition_of(&pool, 3 * WIDTH + 11).await?,
        "qbit_share_ledger_p3"
    );
    // An exact replay of the first share is found past the bounded probe.
    let replay = ledger.append(share(1), None).await?;
    assert!(!replay.inserted);
    assert_eq!(replay.share, original.share);
    // The same share_id with another payload is refused.
    let mut altered = share(1);
    altered.share_difficulty = 2;
    let error = ledger.append(altered, None).await.unwrap_err().to_string();
    assert!(
        error.contains("duplicate share_id payload mismatch"),
        "{error}"
    );
    // Another identity submitting the same header is refused globally.
    let mut other = share(1);
    other.share_id = format!("other:{:064x}", 1);
    other.miner_id = "other".into();
    let error = ledger.append(other, None).await.unwrap_err().to_string();
    assert!(
        error.contains("header already credited globally"),
        "{error}"
    );
    assert_eq!(share_count(&pool).await?, 2);
    // Around the append path, another leaf accepts the first share_id: the
    // per-leaf index cannot see the other partition.
    insert_share(&pool, Some(3 * WIDTH + 12), 1, "worker").await?;
    let copies: i64 =
        sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger WHERE share_id=$1")
            .bind(format!("worker:{:064x}", 1))
            .fetch_one(&pool)
            .await?;
    assert_eq!(copies, 2);
    let same_leaf = insert_share(&pool, Some(3 * WIDTH + 13), 1, "worker").await;
    assert!(
        same_leaf
            .unwrap_err()
            .to_string()
            .contains("qbit_share_ledger_p3_share_id_key"),
        "the leaf's own uniqueness holds"
    );
    db.close(vec![ledger]).await
}
