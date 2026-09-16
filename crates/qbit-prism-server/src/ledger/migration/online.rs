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
//! builds again only if its table and definition match the migration.
//! An index that already exists under a reserved name is
//! adopted when it is valid and its definition is exactly the one the file
//! declares (an earlier run built it), and refused, naming it, when it is
//! anything else: an operator's index under that name is theirs to judge,
//! as with every other native collision. Until the version is recorded,
//! every start refuses the database, as for every other required migration.
//!
//! The plan those inspections produce is decided before any DDL and is
//! not trusted past the builds: each takes hours on a large ledger, the
//! advisory lock orders only runners, and an operator can rename an index
//! while a build holds its table. So the invalid index a rebuild replaces
//! and every index the migration drops are looked up again as their step
//! is reached, and a name that holds something else by then stops the run
//! before the drop, saying what the run had already built and dropped.
//! A kept index, and one built before a later build, are exposed the same
//! way, so the whole declared set (every created index valid with its
//! definition, every dropped name absent) is verified once more inside
//! the transaction that records the version, after its lock; a kept or
//! already-built index that moved while a later build ran is caught there
//! before the version is recorded. The next start plans afresh from what
//! it finds and keeps that.
use super::*;
use sqlx::{Connection, PgConnection};
use std::time::{Duration, Instant};

/// What one online migration changes on the source, derived from the
/// scratch apply: the indexes it creates, rendered by `pg_get_indexdef`,
/// and the original definitions of the indexes it drops.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OnlineMigration {
    pub(super) version: i32,
    pub(super) creates: BTreeMap<String, IndexDefinition>,
    pub(super) drops: BTreeMap<String, IndexDefinition>,
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
    let drops: BTreeMap<String, IndexDefinition> = before
        .indexes
        .iter()
        .filter(|(name, _)| !after.indexes.contains_key(*name))
        .map(|(name, definition)| (name.clone(), definition.clone()))
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
                valid,
                definition,
                table,
            } if table == expected.table && definition == expected.definition => {
                plan.push(if valid {
                    Step::Keep(name)
                } else {
                    Step::Rebuild(name, expected)
                });
            }
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
    for (name, expected) in &migration.drops {
        match live_relation(connection, name).await? {
            LiveRelation::Absent => plan.push(Step::Dropped(name)),
            LiveRelation::Index {
                table, definition, ..
            } if table == expected.table && definition == expected.definition => {
                plan.push(Step::Drop(name, expected));
            }
            LiveRelation::Index {
                table, definition, ..
            } => bail!(
                "refusing to apply migration {version}: index {name} on {table} has a different definition ({definition}) from the release's {} on {}, so this migration does not own it and will not drop it. The migration is not recorded and nothing was changed by it. Check what it serves, then rename or drop it and migrate again",
                expected.definition,
                expected.table
            ),
            LiveRelation::Other(kind) => bail!(
                "refusing to apply migration {version}: a {kind} named {name} holds the name of an index this migration drops, so this migration does not own it and will not drop it. The migration is not recorded and nothing was changed by it. Check what it holds, then rename or move it aside and migrate again"
            ),
        }
    }
    let mut progress = Progress::default();
    for step in plan {
        match step {
            Step::Keep(name) => tracing::info!(
                version,
                index = %name,
                "index already built with the declared definition; keeping it"
            ),
            Step::Build(name, expected) => {
                build(connection, version, name, expected).await?;
                progress.built.push(name);
            }
            Step::Rebuild(name, expected) => {
                drop_planned(
                    connection,
                    version,
                    name,
                    expected,
                    Planned::InvalidBuild,
                    &progress,
                )
                .await?;
                tracing::warn!(
                    version,
                    index = %name,
                    "dropped the invalid index an interrupted build left; building again"
                );
                build(connection, version, name, expected).await?;
                progress.built.push(name);
            }
            Step::Drop(name, expected) => {
                drop_planned(
                    connection,
                    version,
                    name,
                    expected,
                    Planned::Release,
                    &progress,
                )
                .await?;
                progress.dropped.push(name);
                tracing::info!(version, index = %name, table = %expected.table, "dropped replaced index");
            }
            Step::Dropped(name) => tracing::info!(version, index = %name, "index already dropped"),
        }
    }
    let mut tx = connection.begin().await?;
    lock(&mut tx, MIGRATION_LOCK, metrics).await?;
    verify_declared(&mut tx, migration, &progress).await?;
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

/// One index change of the plan, decided before any DDL. The two that drop
/// something look their target up again when they are reached
/// (`drop_planned`), and every name the plan covers is looked up once more
/// before the version is recorded (`verify_declared`): a step that runs or
/// finishes early is otherwise exposed for the hours a later build takes.
enum Step<'a> {
    Keep(&'a str),
    Build(&'a str, &'a IndexDefinition),
    Rebuild(&'a str, &'a IndexDefinition),
    Drop(&'a str, &'a IndexDefinition),
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

impl LiveRelation {
    /// What `name` holds now, for a refusal after the plan was made.
    fn describe(&self, name: &str) -> String {
        match self {
            LiveRelation::Index {
                valid,
                definition,
                table,
            } => format!("index {name} on {table} now reads as {definition} (valid: {valid})"),
            LiveRelation::Other(kind) => format!("a {kind} named {name} now holds the name"),
            LiveRelation::Absent => format!("index {name} no longer exists"),
        }
    }
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

/// What the plan saw under a name it is about to drop.
#[derive(Clone, Copy)]
enum Planned {
    /// The release's index the migration replaces, whatever its validity.
    Release,
    /// The migration's own definition, left invalid by an interrupted
    /// build and dropped to build again.
    InvalidBuild,
}

/// What this run has changed so far. A refusal after the plan was made
/// says so: unlike the preflight refusals, "nothing was changed" is only
/// true until the first build or drop.
#[derive(Default)]
struct Progress<'a> {
    built: Vec<&'a str>,
    dropped: Vec<&'a str>,
}

impl Progress<'_> {
    /// The sentence for a refusal, which must stay true of the next run: a
    /// built index is valid with the declared definition, so it is kept,
    /// and a dropped one is absent, so it is skipped.
    fn describe(&self) -> String {
        let built = self.built.join(", ");
        let dropped = self.dropped.join(", ");
        match (self.built.is_empty(), self.dropped.is_empty()) {
            (true, true) => "Nothing was changed by it.".to_owned(),
            (false, true) => {
                format!("It had already built {built}; what it built stays, and the next run keeps it.")
            }
            (true, false) => format!(
                "It had already dropped {dropped}; what it dropped stays dropped, and the next run skips it."
            ),
            (false, false) => format!(
                "It had already built {built} and dropped {dropped}; both stay as they are, and the next run keeps what was built and skips what was dropped."
            ),
        }
    }
}

/// Drop the index the plan found under `name`, after looking the name up
/// again. The plan was decided before any DDL, and the builds between it
/// and this step take hours on a large ledger, in which nothing stops an
/// operator's DDL on the name: the advisory lock orders only runners, an
/// index is renamed under a lock on itself alone while a build holds its
/// table, and `DROP INDEX CONCURRENTLY` resolves the name when it runs.
/// The look-up and the drop are still two statements, since CONCURRENTLY
/// refuses a transaction block, so a change that lands between them is
/// not caught; PostgreSQL offers nothing here to close that, and what the
/// check leaves open is the round trip between two statements, not the
/// hours of a build.
async fn drop_planned(
    connection: &mut PgConnection,
    version: i32,
    name: &str,
    expected: &IndexDefinition,
    planned: Planned,
    progress: &Progress<'_>,
) -> Result<()> {
    let found = match live_relation(connection, name).await? {
        LiveRelation::Index {
            valid,
            definition,
            table,
        } if table == expected.table
            && definition == expected.definition
            && (!valid || matches!(planned, Planned::Release)) =>
        {
            return drop_concurrently(connection, name).await;
        }
        other => other.describe(name),
    };
    let planned = match planned {
        Planned::Release => format!(
            "the release's {} on {}",
            expected.definition, expected.table
        ),
        Planned::InvalidBuild => format!(
            "the invalid {} on {} an interrupted build left",
            expected.definition, expected.table
        ),
    };
    bail!(
        "refusing to continue migration {version}: {found}, but when this run planned its changes the name held {planned}. The plan is stale past that change, so this run will not drop anything by that name. The migration is not recorded. {} Check what happened under that name, then migrate again; the next run plans afresh from what it finds",
        progress.describe()
    )
}

/// Look every name the migration declares up once more, inside the
/// transaction that records the version and after its lock, and refuse to
/// record unless each created index is valid on its table with exactly
/// the declared definition and each dropped name is absent. The preflight
/// saw a kept index once, and a build checks its own index as it finishes,
/// but the builds after either take hours, in which the DDL that moves a
/// drop target moves these just as well; recorded over that, the version
/// would let every later start trust an index set the database no longer
/// holds. The look-up and the INSERT share a transaction, so the record
/// commits with what the check saw as far as PostgreSQL allows: a rename
/// that commits between the two statements is not seen, since nothing
/// here can lock an index against DDL by another session, but what stays
/// open is that round trip, not the hours of a build.
async fn verify_declared(
    connection: &mut PgConnection,
    migration: &OnlineMigration,
    progress: &Progress<'_>,
) -> Result<()> {
    let version = migration.version;
    let refuse = |found: String, declared: String| -> Result<()> {
        bail!(
            "refusing to record migration {version}: {found}, but the migration declares {declared}. The name changed after this run's step for it, and every start would trust the record over what the database holds. The migration is not recorded. {} Check what happened under that name, then migrate again; the next run plans afresh from what it finds",
            progress.describe()
        )
    };
    for (name, expected) in &migration.creates {
        match live_relation(connection, name).await? {
            LiveRelation::Index {
                valid: true,
                definition,
                table,
            } if table == expected.table && definition == expected.definition => {}
            other => refuse(
                other.describe(name),
                format!(
                    "a valid {} on {} under that name",
                    expected.definition, expected.table
                ),
            )?,
        }
    }
    for (name, expected) in &migration.drops {
        match live_relation(connection, name).await? {
            LiveRelation::Absent => {}
            other => refuse(
                other.describe(name),
                format!(
                    "nothing under that name, the release's {} on {} having been dropped",
                    expected.definition, expected.table
                ),
            )?,
        }
    }
    Ok(())
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
    fn a_refusal_after_the_plan_says_what_the_run_changed() {
        let mut progress = Progress::default();
        assert_eq!(progress.describe(), "Nothing was changed by it.");
        progress.built.push("a");
        progress.built.push("b");
        assert_eq!(
            progress.describe(),
            "It had already built a, b; what it built stays, and the next run keeps it."
        );
        progress.dropped.push("c");
        assert_eq!(
            progress.describe(),
            "It had already built a, b and dropped c; both stay as they are, and the next run keeps what was built and skips what was dropped."
        );
        progress.built.clear();
        assert_eq!(
            progress.describe(),
            "It had already dropped c; what it dropped stays dropped, and the next run skips it."
        );
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
        let migration = derive(13, &before, &after).unwrap();
        assert_eq!(migration.version, 13);
        assert_eq!(migration.creates.keys().collect::<Vec<_>>(), vec!["new"]);
        assert_eq!(
            migration.drops,
            BTreeMap::from([("old".to_owned(), index("t", "CREATE INDEX old ON t (a)"))])
        );
        assert!(derive(13, &before, &before)
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
        let error = derive(13, &before, &after).unwrap_err().to_string();
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
        let error = derive(13, &before, &after).unwrap_err().to_string();
        assert!(
            error.contains("may only create and drop indexes"),
            "{error}"
        );
    }
}
