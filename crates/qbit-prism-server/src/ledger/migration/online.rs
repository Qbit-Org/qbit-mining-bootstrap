//! Native migrations applied outside the migration transaction.
//!
//! `CREATE INDEX CONCURRENTLY` and `DROP INDEX CONCURRENTLY` cannot run in a
//! transaction block, and a plain `CREATE INDEX` on the share ledger holds a
//! SHARE lock on the table for the whole build, so every append would queue
//! behind it. A migration listed in `ONLINE_MIGRATIONS` is therefore applied
//! in two parts. Its file is applied transactionally to the scratch schema
//! with every other native migration, so the migrator learns the index
//! definitions it declares as this server's PostgreSQL renders them; nothing
//! here parses SQL. After the migration transaction has committed, the
//! runner below builds each new index with `CREATE INDEX CONCURRENTLY`,
//! drops each replaced one with `DROP INDEX CONCURRENTLY`, and records the
//! version last, on a dedicated connection with no statement or lock
//! timeout, under a session-level advisory lock keyed by the ledger's
//! schema, so two starting frontends never build the same index twice.
//! Existing native ledgers always use this runner, even without visible
//! shares: writers do not take the migration lock. Only fresh or empty
//! 2.x.x sources apply the file inside `migrate_schema`'s transaction,
//! while its cutover locks exclude writers.
//!
//! The runner is resumable. An interrupted build leaves an invalid index
//! behind, still maintained by every insert; the next run drops it and
//! builds again. An index that already exists under a reserved name is
//! adopted when it is valid and its definition is exactly the one the file
//! declares (an earlier run built it), and refused, naming it, when it is
//! anything else: an operator's index under that name is theirs to judge,
//! as with every other native collision. Until the version is recorded,
//! every start refuses the database, as for every other required migration.
use super::*;
use sqlx::{Connection, PgConnection};
use std::time::{Duration, Instant};

/// What one online migration changes on the source, derived from the
/// scratch apply: the indexes it creates, rendered by `pg_get_indexdef`,
/// and the indexes it drops, each with the table it must be on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OnlineMigration {
    pub(super) version: i32,
    pub(super) creates: BTreeMap<String, IndexDefinition>,
    pub(super) drops: BTreeMap<String, String>,
}

/// The change an online migration made to the scratch schema, refused if
/// it is anything but creating and dropping indexes that back no
/// constraint: a table, column, function, trigger, sequence or constraint
/// in that file would reach the scratch schema and never the source, and
/// an index redefined under its old name would be adopted as it is.
pub(super) fn derive(
    version: i32,
    before: &SchemaFingerprint,
    after: &SchemaFingerprint,
) -> Result<OnlineMigration> {
    ensure!(
        before.tables == after.tables
            && before.constraints == after.constraints
            && before.incoming_foreign_keys == after.incoming_foreign_keys
            && before.triggers == after.triggers
            && before.rules == after.rules
            && before.policies == after.policies
            && before.functions == after.functions
            && before.sequences == after.sequences
            && before.other_relations == after.other_relations,
        "migration {version} is applied online and may only create and drop indexes, but its file changes other objects too; move those into a transactional migration"
    );
    let mut creates = BTreeMap::new();
    for (name, definition) in &after.indexes {
        match before.indexes.get(name) {
            None => {
                creates.insert(name.clone(), definition.clone());
            }
            Some(previous) => ensure!(
                previous == definition,
                "migration {version} is applied online and redefines index {name} under its old name; a replaced index must take a new name, or an earlier build under the old name would be adopted as it is"
            ),
        }
    }
    let drops: BTreeMap<String, String> = before
        .indexes
        .iter()
        .filter(|(name, _)| !after.indexes.contains_key(*name))
        .map(|(name, definition)| (name.clone(), definition.table.clone()))
        .collect();
    ensure!(
        !creates.is_empty() || !drops.is_empty(),
        "migration {version} is applied online but creates and drops no index"
    );
    Ok(OnlineMigration {
        version,
        creates,
        drops,
    })
}

/// Apply one online migration to the source and record it. Idempotent:
/// what an earlier run built or dropped is kept, and a version another
/// instance recorded meanwhile is not applied again.
pub(crate) async fn apply_online_migration(
    pool: &PgPool,
    migration: &OnlineMigration,
    metrics: Option<&crate::metrics::Metrics>,
) -> Result<()> {
    let version = migration.version;
    // A connection of its own, never returned to the pool: the session
    // settings and the session-level lock below end with it.
    let mut connection = pool.acquire().await?.detach();
    let outcome = apply(&mut connection, migration, metrics).await;
    let closed = connection.close().await;
    outcome?;
    closed.with_context(|| format!("closing the connection that applied migration {version}"))?;
    Ok(())
}

async fn apply(
    connection: &mut PgConnection,
    migration: &OnlineMigration,
    metrics: Option<&crate::metrics::Metrics>,
) -> Result<()> {
    let version = migration.version;
    // A build on a large ledger runs for hours, and CONCURRENTLY waits for
    // every transaction that could use the index; the pool's per-statement
    // and lock timeouts would abort it. Session-level, on this connection.
    sqlx::query(
        "SELECT set_config('statement_timeout','0',false),set_config('lock_timeout','0',false)",
    )
    .execute(&mut *connection)
    .await?;
    // A blocking advisory-lock SELECT keeps its statement snapshot while
    // waiting. A concurrent partial-index build can wait for that snapshot
    // to end, deadlocking with the next runner waiting for this lock. End
    // each attempt before sleeping, outside any database transaction.
    while !sqlx::query_scalar::<_, bool>(
        "SELECT pg_try_advisory_lock($1,hashtext(current_schema()))",
    )
    .bind(ONLINE_DDL_LOCK_CLASS)
    .fetch_one(&mut *connection)
    .await?
    {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if recorded(connection, version).await? {
        tracing::info!(version, "online migration already recorded");
        return Ok(());
    }
    // Every reserved name is inspected before any DDL, so a refusal leaves
    // the database as it was: an index built before a refusal would be
    // adopted on the next run, but the operator is told that nothing
    // changed, and that must be true.
    let mut plan = Vec::new();
    for (name, expected) in &migration.creates {
        match live_relation(connection, name).await? {
            LiveRelation::Absent => plan.push(Step::Build(name, expected)),
            LiveRelation::Index {
                valid: true,
                definition,
                ..
            } if definition == expected.definition => plan.push(Step::Keep(name)),
            LiveRelation::Index { valid: false, .. } => plan.push(Step::Rebuild(name, expected)),
            LiveRelation::Index {
                definition, table, ..
            } => bail!(
                "refusing to apply migration {version}: index {name} on {table} already exists with a different definition ({definition}); the migration declares {}. The migration is not recorded and nothing was changed by it. Check what that index serves, then rename or drop it and migrate again",
                expected.definition
            ),
            LiveRelation::Other(kind) => bail!(
                "refusing to apply migration {version}: a {kind} named {name} holds the name of an index this migration creates. The migration is not recorded and nothing was changed by it. Check what it holds, then rename or move it aside and migrate again"
            ),
        }
    }
    for (name, table) in &migration.drops {
        match live_relation(connection, name).await? {
            LiveRelation::Absent => plan.push(Step::Dropped(name)),
            LiveRelation::Index { table: found, .. } if found == *table => {
                plan.push(Step::Drop(name, table));
            }
            LiveRelation::Index { table: found, .. } => bail!(
                "refusing to apply migration {version}: index {name} is on {found}, not on {table} where the release created it, so this migration does not own it and will not drop it. The migration is not recorded and nothing was changed by it. Check what it serves, then rename or drop it and migrate again"
            ),
            LiveRelation::Other(kind) => bail!(
                "refusing to apply migration {version}: a {kind} named {name} holds the name of an index this migration drops, so this migration does not own it and will not drop it. The migration is not recorded and nothing was changed by it. Check what it holds, then rename or move it aside and migrate again"
            ),
        }
    }
    for step in plan {
        match step {
            Step::Keep(name) => tracing::info!(
                version,
                index = %name,
                "index already built with the declared definition; keeping it"
            ),
            Step::Build(name, expected) => build(connection, version, name, expected).await?,
            Step::Rebuild(name, expected) => {
                tracing::warn!(
                    version,
                    index = %name,
                    "dropping the invalid index an interrupted build left, then building again"
                );
                drop_concurrently(connection, name).await?;
                build(connection, version, name, expected).await?;
            }
            Step::Drop(name, table) => {
                drop_concurrently(connection, name).await?;
                tracing::info!(version, index = %name, table = %table, "dropped replaced index");
            }
            Step::Dropped(name) => tracing::info!(version, index = %name, "index already dropped"),
        }
    }
    let mut tx = connection.begin().await?;
    lock(&mut tx, MIGRATION_LOCK, metrics).await?;
    sqlx::query(
        "INSERT INTO qbit_prism_schema_migrations(version) VALUES($1) ON CONFLICT (version) DO NOTHING",
    )
    .bind(version)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    tracing::info!(version, "online migration recorded");
    Ok(())
}

/// One index change of the plan, decided before any DDL.
enum Step<'a> {
    Keep(&'a str),
    Build(&'a str, &'a IndexDefinition),
    Rebuild(&'a str, &'a IndexDefinition),
    Drop(&'a str, &'a str),
    Dropped(&'a str),
}

/// A relation found under a name the migration reserves, in the current
/// schema only: the migrator's DDL creates there, and the source checks
/// already refused a `qbit_` object resolved from any other schema.
enum LiveRelation {
    Absent,
    Index {
        definition: String,
        valid: bool,
        table: String,
    },
    Other(String),
}

async fn live_relation(connection: &mut PgConnection, name: &str) -> Result<LiveRelation> {
    let row = sqlx::query("SELECT c.relkind::text AS kind,current_schema()::text AS schema,CASE WHEN c.relkind='i' THEN pg_get_indexdef(c.oid) END AS definition,x.indisvalid AS valid,t.relname::text AS table_name FROM pg_class c LEFT JOIN pg_index x ON x.indexrelid=c.oid LEFT JOIN pg_class t ON t.oid=x.indrelid WHERE c.relnamespace=current_schema()::regnamespace AND c.relname=$1")
        .bind(name)
        .fetch_optional(&mut *connection)
        .await?;
    let Some(row) = row else {
        return Ok(LiveRelation::Absent);
    };
    let kind: String = row.try_get("kind")?;
    if kind != "i" {
        return Ok(LiveRelation::Other(relation_kind(&kind).to_owned()));
    }
    let schema: String = row.try_get("schema")?;
    let definition: String = row.try_get("definition")?;
    Ok(LiveRelation::Index {
        definition: strip_schema_qualification(&definition, &schema),
        valid: row.try_get("valid")?,
        table: row.try_get("table_name")?,
    })
}

fn relation_kind(kind: &str) -> &'static str {
    match kind {
        "r" => "table",
        "S" => "sequence",
        "v" => "view",
        "m" => "materialized view",
        "p" => "partitioned table",
        "I" => "partitioned index",
        "f" => "foreign table",
        "c" => "composite type",
        "t" => "TOAST table",
        _ => "relation",
    }
}

async fn build(
    connection: &mut PgConnection,
    version: i32,
    name: &str,
    expected: &IndexDefinition,
) -> Result<()> {
    let statement = concurrent_create(&expected.definition)
        .with_context(|| format!("migration {version}, index {name}"))?;
    tracing::info!(
        version,
        index = %name,
        table = %expected.table,
        "building index concurrently; appends continue, and on a large ledger this takes about two table scans"
    );
    let started = Instant::now();
    sqlx::raw_sql(&statement)
        .execute(&mut *connection)
        .await
        .with_context(|| format!("building index {name} for migration {version}; if the build was interrupted the index is invalid, and the next migrate drops and rebuilds it"))?;
    match live_relation(connection, name).await? {
        LiveRelation::Index {
            valid: true,
            definition,
            ..
        } if definition == expected.definition => {}
        LiveRelation::Index {
            valid, definition, ..
        } => bail!(
            "migration {version} built index {name}, but it reads back as {definition} (valid: {valid}), not the declared {}; migrate again",
            expected.definition
        ),
        _ => bail!("migration {version} built index {name}, but it is not an index of the current schema afterwards; migrate again"),
    }
    tracing::info!(
        version,
        index = %name,
        elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "index built"
    );
    Ok(())
}

/// The `CREATE INDEX` statement `pg_get_indexdef` renders, as a concurrent
/// build. The rendering is what the migration file declared, applied by
/// this server, so it is complete DDL and needs no parsing beyond the
/// leading keywords.
fn concurrent_create(definition: &str) -> Result<String> {
    for prefix in ["CREATE UNIQUE INDEX ", "CREATE INDEX "] {
        if let Some(rest) = definition.strip_prefix(prefix) {
            return Ok(format!("{prefix}CONCURRENTLY {rest}"));
        }
    }
    bail!("index definition does not start with CREATE INDEX: {definition}")
}

async fn drop_concurrently(connection: &mut PgConnection, name: &str) -> Result<()> {
    sqlx::raw_sql(&format!(
        "DROP INDEX CONCURRENTLY {}",
        quote_identifier(name)
    ))
    .execute(&mut *connection)
    .await
    .with_context(|| format!("dropping index {name} concurrently"))?;
    Ok(())
}

fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

async fn recorded(connection: &mut PgConnection, version: i32) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM qbit_prism_schema_migrations WHERE version=$1)",
    )
    .bind(version)
    .fetch_one(&mut *connection)
    .await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index(table: &str, definition: &str) -> IndexDefinition {
        IndexDefinition {
            table: table.into(),
            definition: definition.into(),
            valid: true,
            unique: false,
            expression: false,
            partial: true,
        }
    }

    #[test]
    fn concurrent_create_inserts_the_keyword_after_the_index_kind() {
        assert_eq!(
            concurrent_create("CREATE INDEX a ON t USING btree (x)").unwrap(),
            "CREATE INDEX CONCURRENTLY a ON t USING btree (x)"
        );
        assert_eq!(
            concurrent_create("CREATE UNIQUE INDEX a ON t USING btree (x)").unwrap(),
            "CREATE UNIQUE INDEX CONCURRENTLY a ON t USING btree (x)"
        );
        assert!(concurrent_create("ALTER TABLE t ADD COLUMN c int")
            .unwrap_err()
            .to_string()
            .contains("does not start with CREATE INDEX"));
    }

    #[test]
    fn identifiers_are_quoted_for_ddl() {
        assert_eq!(quote_identifier("plain_idx"), "\"plain_idx\"");
        assert_eq!(quote_identifier("odd\"name"), "\"odd\"\"name\"");
    }

    #[test]
    fn derivation_lists_new_indexes_and_dropped_ones_and_nothing_else() {
        let mut before = SchemaFingerprint::default();
        before
            .indexes
            .insert("old".into(), index("t", "CREATE INDEX old ON t (a)"));
        before
            .indexes
            .insert("kept".into(), index("t", "CREATE INDEX kept ON t (k)"));
        let mut after = SchemaFingerprint::default();
        after
            .indexes
            .insert("kept".into(), index("t", "CREATE INDEX kept ON t (k)"));
        after
            .indexes
            .insert("new".into(), index("t", "CREATE INDEX new ON t (b)"));
        let migration = derive(12, &before, &after).unwrap();
        assert_eq!(migration.version, 12);
        assert_eq!(migration.creates.keys().collect::<Vec<_>>(), vec!["new"]);
        assert_eq!(
            migration.drops,
            BTreeMap::from([("old".to_owned(), "t".to_owned())])
        );
        assert!(derive(12, &before, &before)
            .unwrap_err()
            .to_string()
            .contains("creates and drops no index"));
    }

    #[test]
    fn derivation_refuses_a_redefinition_under_the_old_name() {
        let mut before = SchemaFingerprint::default();
        before
            .indexes
            .insert("same".into(), index("t", "CREATE INDEX same ON t (a)"));
        let mut after = SchemaFingerprint::default();
        after
            .indexes
            .insert("same".into(), index("t", "CREATE INDEX same ON t (b)"));
        let error = derive(12, &before, &after).unwrap_err().to_string();
        assert!(
            error.contains("redefines index same under its old name"),
            "{error}"
        );
    }

    #[test]
    fn derivation_refuses_a_change_that_is_not_an_index() {
        let mut before = SchemaFingerprint::default();
        before
            .indexes
            .insert("old".into(), index("t", "CREATE INDEX old ON t (a)"));
        let mut after = SchemaFingerprint::default();
        after.sequences.insert(
            "t_seq".into(),
            SequenceDefinition {
                persistence: "p".into(),
                data_type: "bigint".into(),
                start: 1,
                increment: 1,
                min: 1,
                max: i64::MAX,
                cache: 1,
                cycle: false,
            },
        );
        let error = derive(12, &before, &after).unwrap_err().to_string();
        assert!(
            error.contains("may only create and drop indexes"),
            "{error}"
        );
    }
}
