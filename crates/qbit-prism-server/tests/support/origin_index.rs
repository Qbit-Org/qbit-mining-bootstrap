//! Migration 031 (CONTRACT D-14): the share ledger's (origin_node,
//! share_seq) index, which the 3.1 dual writer's peer pulls and window cuts
//! read, applied online.
//!
//! The ledger is partitioned since 017, and PostgreSQL builds no index of a
//! partitioned table CONCURRENTLY. On an existing ledger the online runner
//! therefore builds one leaf per partition with `CREATE INDEX CONCURRENTLY`,
//! then creates the index ON ONLY the parent and attaches each leaf, which
//! makes the parent valid. These tests cover what that changes: the index a
//! fresh ledger gets in the migration transaction, that appends keep landing
//! while a leaf's build waits for an open one, that every start refuses the
//! database until 31 is recorded, once, and that an interrupted run resumes
//! from what it finds (an invalid leaf built again, an earlier run's leaves
//! and parent kept, a partition attached meanwhile given its leaf) after
//! refusing a foreign index under a leaf's name before any DDL.
//! `migrate --offline-indexes` builds the same leaves plainly
//! (`index_trim.rs`).
use super::*;
use qbit_prism_server::ledger::REQUIRED_SCHEMA_VERSIONS;
use sqlx::Row;
use tokio::time::{sleep, timeout};

pub(super) const ORIGIN: &str = "qbit_share_ledger_origin_seq_idx";
pub(super) const ORIGIN_DEFINITION: &str = "CREATE INDEX qbit_share_ledger_origin_seq_idx ON ONLY qbit_share_ledger USING btree (origin_node, share_seq)";

/// The leaf of 031's index on `partition`: its name, after the partition as
/// `qbit_prism_share_partition_create` names every leaf, and its rendering.
pub(super) fn leaf(partition: &str) -> (String, String) {
    let name = format!("{partition}_origin_seq_idx");
    let definition =
        format!("CREATE INDEX {name} ON {partition} USING btree (origin_node, share_seq)");
    (name, definition)
}

/// Undo 031 where it is recorded: drop the index, which drops every leaf
/// attached to it, and its version record. 017's and 027's undos call this
/// first, since the index lives on 017's partitions and reads 027's column;
/// the next migrate applies 031 again after them.
pub(super) async fn undo_031(pool: &PgPool) -> Result<()> {
    let recorded: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM qbit_prism_schema_migrations WHERE version=31)",
    )
    .fetch_one(pool)
    .await?;
    if recorded {
        sqlx::raw_sql(&format!(
            "DELETE FROM qbit_prism_schema_migrations WHERE version=31; DROP INDEX {ORIGIN}"
        ))
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// The share ledger's partitions, oldest first, as the runner visits them.
pub(super) async fn partitions(pool: &PgPool) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(
        "SELECT c.relname::text FROM pg_inherits h JOIN pg_class c ON c.oid=h.inhrelid WHERE h.inhparent='qbit_share_ledger'::regclass ORDER BY c.oid",
    )
    .fetch_all(pool)
    .await?)
}

/// Every index named after 031's, on the parent and on the partitions: name,
/// rendering (without the schema), validity, OID and the index it is
/// attached to, ordered by name.
async fn origin_indexes(pool: &PgPool) -> Result<Vec<(String, String, bool, String, String)>> {
    let rows = sqlx::query("SELECT i.relname::text AS name,replace(pg_get_indexdef(x.indexrelid),current_schema()||'.','') AS definition,x.indisvalid AS valid,x.indexrelid::text AS oid,coalesce((SELECT h.inhparent::regclass::text FROM pg_inherits h WHERE h.inhrelid=x.indexrelid),'') AS parent FROM pg_index x JOIN pg_class i ON i.oid=x.indexrelid WHERE i.relnamespace=current_schema()::regnamespace AND i.relname LIKE '%origin_seq_idx' ORDER BY 1")
        .fetch_all(pool)
        .await?;
    rows.iter()
        .map(|row| {
            Ok((
                row.try_get("name")?,
                row.try_get("definition")?,
                row.try_get("valid")?,
                row.try_get("oid")?,
                row.try_get("parent")?,
            ))
        })
        .collect()
}

/// The index 031 leaves: valid on the parent with its declared rendering,
/// and on every partition exactly one valid leaf, named after the
/// partition and attached to it; nothing else under such a name. 31 is
/// recorded once with every other required version. Returns each index's
/// name and OID, ordered by name.
async fn assert_origin_index(pool: &PgPool) -> Result<Vec<(String, String)>> {
    let found = origin_indexes(pool).await?;
    let mut expected = vec![(
        ORIGIN.to_owned(),
        ORIGIN_DEFINITION.to_owned(),
        true,
        String::new(),
    )];
    for partition in partitions(pool).await? {
        let (name, definition) = leaf(&partition);
        expected.push((name, definition, true, ORIGIN.to_owned()));
    }
    expected.sort();
    let seen: Vec<(String, String, bool, String)> = found
        .iter()
        .map(|(name, definition, valid, _, parent)| {
            (name.clone(), definition.clone(), *valid, parent.clone())
        })
        .collect();
    ensure!(
        seen == expected,
        "031's index set is {seen:#?}, expected {expected:#?}"
    );
    let versions: Vec<i32> =
        sqlx::query_scalar("SELECT version FROM qbit_prism_schema_migrations ORDER BY version")
            .fetch_all(pool)
            .await?;
    ensure!(
        versions == REQUIRED_SCHEMA_VERSIONS,
        "recorded {versions:?}"
    );
    Ok(found
        .into_iter()
        .map(|(name, _, _, oid, _)| (name, oid))
        .collect())
}

fn oid<'a>(indexes: &'a [(String, String)], name: &str) -> &'a str {
    indexes
        .iter()
        .find(|(found, _)| found == name)
        .map(|(_, oid)| oid.as_str())
        .unwrap_or_default()
}

async fn schema_versions(pool: &PgPool) -> Result<Vec<i32>> {
    Ok(
        sqlx::query_scalar("SELECT version FROM qbit_prism_schema_migrations ORDER BY version")
            .fetch_all(pool)
            .await?,
    )
}

/// A share row as a native writer leaves it, inserted in the caller's
/// transaction without the native ordering lock: enough to hold a lock on
/// the first partition while a build waits.
async fn insert_share<'e>(
    executor: impl sqlx::Executor<'e, Database = sqlx::Postgres>,
    id: u64,
) -> Result<()> {
    sqlx::query("INSERT INTO qbit_share_ledger(share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,accepted,writer_id,writer_epoch) VALUES($1,'alice','alice',decode(repeat('11',32),'hex'),1,100,100,'job',to_timestamp(1),1,clock_timestamp(),true,'origin-index',0)")
        .bind(format!("alice:{id:064x}"))
        .execute(executor)
        .await?;
    Ok(())
}

/// A fresh ledger gets 031 in the migration transaction. On a ledger at 27
/// it is applied online after the transaction: each leaf's build is a
/// `CREATE INDEX CONCURRENTLY`, which waits for an append already open on
/// its partition while the appends that arrive meanwhile land at once,
/// where a plain build of the partitioned index would queue behind the open
/// append and every later one behind it. Until 31 is recorded a start
/// refuses the database; then the index is valid on the parent and every
/// partition, 31 is recorded once, and a second start builds nothing.
#[tokio::test]
async fn migration_031_builds_the_origin_index_leaf_by_leaf_without_blocking_appends() -> Result<()>
{
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let fresh = assert_origin_index(&a.pool).await?;
    ensure!(
        fresh.len() > 2,
        "a fresh ledger has partitions beyond its first: {fresh:?}"
    );
    for id in 1..=3 {
        a.append(share(id), None).await?;
    }
    undo_031(&a.pool).await?;
    ensure!(origin_indexes(&a.pool).await?.is_empty());
    // Without initialize, a start refuses the database, naming the gap.
    let error = Ledger::connect(&db.url, "cold".into(), 8, false)
        .await
        .err()
        .context("a non-initializing start accepted a database missing 031")?
        .to_string();
    ensure!(error.contains("missing migration(s) 31"), "{error}");
    let mut writer = a.pool.begin().await?;
    insert_share(&mut *writer, 1).await?;
    let mut migrate = Box::pin(db.ledger("online"));
    // The relation OID scopes this to this test's own schema.
    let waiting = timeout(Duration::from_secs(60), async {
        while !sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity a WHERE a.query LIKE 'CREATE INDEX CONCURRENTLY%' AND a.wait_event_type='Lock' AND EXISTS(SELECT 1 FROM pg_locks l WHERE l.pid=a.pid AND l.locktype='relation' AND l.relation='qbit_share_ledger_p0'::regclass AND l.granted))",
        )
        .fetch_one(&a.pool)
        .await?
        {
            sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    });
    tokio::select! {
        result = &mut migrate => match result {
            Ok(_) => bail!("031 was applied while an append was open"),
            Err(error) => {
                return Err(error.context("031 failed before its first leaf's build waited for the open append"))
            }
        },
        result = waiting => result.context("031's first leaf build never waited for the open append")??,
    }
    timeout(Duration::from_secs(5), a.append(share(10), None))
        .await
        .context("an append blocked behind 031's build")??;
    ensure!(
        futures_util::poll!(&mut migrate).is_pending(),
        "031 finished before the open append committed"
    );
    // Still no index on the parent: it is created once the leaves are built.
    ensure!(
        !origin_indexes(&a.pool)
            .await?
            .iter()
            .any(|(name, ..)| name == ORIGIN),
        "031 created the parent index before its leaves were built"
    );
    writer.commit().await?;
    let online = timeout(Duration::from_secs(120), migrate).await??;
    let built = assert_origin_index(&a.pool).await?;
    ensure!(
        oid(&built, ORIGIN) != oid(&fresh, ORIGIN),
        "031 was not built again: {fresh:?} then {built:?}"
    );
    let recorded: i64 =
        sqlx::query_scalar("SELECT count(*) FROM qbit_prism_schema_migrations WHERE version=31")
            .fetch_one(&a.pool)
            .await?;
    ensure!(recorded == 1, "31 recorded {recorded} times");
    // A second start finds 31 recorded and builds nothing.
    let again = db.ledger("again").await?;
    ensure!(assert_origin_index(&a.pool).await? == built);
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger")
        .fetch_one(&a.pool)
        .await?;
    ensure!(rows == 5, "{rows} shares");
    db.close(vec![a, online, again]).await
}

/// An interrupted run resumes from what it finds. The first partition holds
/// the invalid leaf a build stopped behind an open append leaves, the second
/// a valid leaf not yet attached, the third a leaf attached to the parent the
/// run had created, and a partition attached since then has its leaf from
/// the parent's definition. Before any of that is touched, a foreign index
/// under a leaf's name is refused, naming it, with nothing built, dropped or
/// recorded. Once it is gone the run drops and builds the invalid leaf
/// again, keeps the other three and the parent, builds the rest and
/// records 31.
#[tokio::test]
async fn migration_031_resumes_an_interrupted_build_and_refuses_a_foreign_leaf() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    let first = db.ledger("first").await?;
    for id in 1..=3 {
        first.append(share(id), None).await?;
    }
    undo_031(&pool).await?;
    let before_lead = partitions(&pool).await?;
    ensure!(
        before_lead.len() >= 4,
        "the fresh ledger has {before_lead:?}"
    );
    let (p0, p1, p2, p3) = (
        before_lead[0].as_str(),
        before_lead[1].as_str(),
        before_lead[2].as_str(),
        before_lead[3].as_str(),
    );
    // The first partition's leaf, cancelled behind an open append.
    let mut writer = pool.begin().await?;
    insert_share(&mut *writer, 1).await?;
    let mut builder = pool.acquire().await?;
    sqlx::query("SET statement_timeout='500ms'")
        .execute(&mut *builder)
        .await?;
    let failed = sqlx::raw_sql(&leaf(p0).1.replacen(
        "CREATE INDEX ",
        "CREATE INDEX CONCURRENTLY ",
        1,
    ))
    .execute(&mut *builder)
    .await
    .expect_err("the leaf's concurrent build must time out behind the append");
    ensure!(
        failed
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref()
            == Some("57014"),
        "{failed}"
    );
    sqlx::query("SET statement_timeout=0")
        .execute(&mut *builder)
        .await?;
    drop(builder);
    writer.rollback().await?;
    // The second's, built and not attached; the third's, attached to the
    // parent the interrupted run had created.
    sqlx::raw_sql(&format!(
        "{}; {ORIGIN_DEFINITION}; {}; ALTER INDEX {ORIGIN} ATTACH PARTITION {}",
        leaf(p1).1,
        leaf(p2).1,
        leaf(p2).0
    ))
    .execute(&pool)
    .await?;
    // A partition attached since then gets its leaf as it is attached.
    sqlx::raw_sql("UPDATE qbit_prism_share_partitioning SET lead_partitions=lead_partitions+1; SELECT qbit_prism_share_partition_ensure()")
        .execute(&pool)
        .await?;
    let with_lead = partitions(&pool).await?;
    let added = with_lead
        .last()
        .filter(|name| !before_lead.contains(name))
        .context("no partition was attached")?
        .clone();
    let interrupted = origin_indexes(&pool).await?;
    let state = |name: &str| {
        interrupted
            .iter()
            .find(|(found, ..)| found == name)
            .map(|(_, _, valid, oid, parent)| (*valid, oid.clone(), parent.clone()))
    };
    ensure!(
        state(&leaf(p0).0).is_some_and(|(valid, _, parent)| !valid && parent.is_empty()),
        "{interrupted:#?}"
    );
    ensure!(
        state(&leaf(p1).0).is_some_and(|(valid, _, parent)| valid && parent.is_empty()),
        "{interrupted:#?}"
    );
    ensure!(
        state(&leaf(p2).0).is_some_and(|(valid, _, parent)| valid && parent == ORIGIN),
        "{interrupted:#?}"
    );
    ensure!(
        state(&leaf(&added).0).is_some_and(|(valid, _, parent)| valid && parent == ORIGIN),
        "{interrupted:#?}"
    );
    ensure!(
        state(ORIGIN).is_some_and(|(valid, ..)| !valid),
        "{interrupted:#?}"
    );
    // An index of the operator's under the fourth's leaf name.
    sqlx::raw_sql(&format!("CREATE INDEX {} ON {p3} (share_seq)", leaf(p3).0))
        .execute(&pool)
        .await?;
    let refused_with = origin_indexes(&pool).await?;
    let error = db
        .ledger("refused")
        .await
        .err()
        .context("031 adopted or replaced an index it did not declare")?
        .to_string();
    ensure!(
        error.contains(&format!(
            "index {} on {p3} already exists with a different definition",
            leaf(p3).0
        )) && error.contains("nothing was changed by it"),
        "{error}"
    );
    ensure!(origin_indexes(&pool).await? == refused_with);
    ensure!(
        schema_versions(&pool).await?
            == REQUIRED_SCHEMA_VERSIONS
                .iter()
                .copied()
                .filter(|version| *version != 31)
                .collect::<Vec<_>>()
    );
    sqlx::raw_sql(&format!("DROP INDEX {}", leaf(p3).0))
        .execute(&pool)
        .await?;
    let resumed = db.ledger("resumed").await?;
    let built = assert_origin_index(&pool).await?;
    ensure!(
        oid(&built, ORIGIN) == state(ORIGIN).unwrap().1,
        "the interrupted run's parent was rebuilt"
    );
    ensure!(
        oid(&built, &leaf(p0).0) != state(&leaf(p0).0).unwrap().1,
        "the invalid leaf was not built again"
    );
    for kept in [leaf(p1).0, leaf(p2).0, leaf(&added).0] {
        ensure!(
            oid(&built, &kept) == state(&kept).unwrap().1,
            "{kept} was built again"
        );
    }
    db.close(vec![first, resumed]).await
}
