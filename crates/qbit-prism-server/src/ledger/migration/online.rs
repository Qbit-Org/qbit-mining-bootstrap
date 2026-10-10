//! Native migrations applied outside the migration transaction.
//!
//! `CREATE INDEX CONCURRENTLY` and `DROP INDEX CONCURRENTLY` cannot run in a
//! transaction block, and a plain `CREATE INDEX` on the share ledger holds a
//! SHARE lock on the table for the whole build, so every append would queue
//! behind it, as every write to the CTV fanout table would, a found block's
//! landing included, behind 024's. A migration listed in `ONLINE_MIGRATIONS` is
//! therefore applied in two parts. Its file is applied transactionally to the
//! scratch schema with every other native migration, so the migrator learns the
//! index definitions it declares as this server's PostgreSQL renders them;
//! nothing here parses SQL. After the migration transaction has committed, the
//! runner below builds each new index with `CREATE INDEX CONCURRENTLY`, drops
//! each replaced one with `DROP INDEX CONCURRENTLY` (with plain statements in
//! one transaction instead under `migrate --offline-indexes`, below), and
//! records the version last, on a dedicated connection with no statement or
//! lock timeout, under a session-level advisory lock keyed by the ledger's
//! schema, so two starting frontends never build the same index twice.
//! Existing native ledgers always use this runner, even without visible
//! shares: writers do not take the migration lock. Only fresh or empty 2.x.x
//! sources apply the file inside `migrate_schema`'s transaction, while its
//! cutover locks exclude writers.
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
//!
//! An index of a partitioned table, 031's on the share ledger 017
//! partitions, cannot be built CONCURRENTLY, and a plain build of it holds
//! every partition for the whole build. The scratch apply renders it as
//! `CREATE INDEX <name> ON ONLY <table> ...`, and the runner builds it the
//! way PostgreSQL documents for partitioned tables (`build_partitioned`):
//! first one leaf index per partition with `CREATE INDEX CONCURRENTLY`, named
//! `<partition><suffix>` (the suffix being the index's name past the
//! table's), as `qbit_prism_share_partition_create` names every leaf; then
//! the parent index ON ONLY, catalog work under a SHARE lock on the parent;
//! then each leaf attached to it, catalog work under an ACCESS EXCLUSIVE lock
//! on that leaf index alone. Both locks are taken with a short lock timeout
//! and retried, as 017's swap takes its own, so no append or read queues
//! behind a request that waits for a long transaction. PostgreSQL marks the
//! parent valid when the last partition's leaf is attached. A partition
//! created before the parent exists gets its leaf from the runner's next
//! pass, and one created after it gets its leaf as it is attached, from the
//! parent's definition. A partition that left while the parent waited for
//! its leaf would leave the parent invalid for good, so the runner refuses,
//! before any DDL, a partition that is still detaching, and holds the share
//! archive's lifecycle lock for the whole build: no `share-archive` command
//! runs until it has finished. Interrupted, it resumes from what it finds: a
//! valid leaf of the declared definition is kept, an invalid one is dropped
//! and built again, an attached one is skipped, and the parent, valid or
//! not, is kept.
//!
//! `migrate --offline-indexes` (`IndexBuildMode::Offline`) builds 013's,
//! 024's and 031's indexes the other way, for a migrate with no instance
//! live, such as a cutover's (`apply_offline`). One transaction takes the
//! migration lock,
//! refuses an instance that has not reported drained or stopped and a live
//! legacy writer lease before any DDL, and locks the tables ACCESS EXCLUSIVE
//! with a 5 s lock timeout. It runs the same plan with a plain, parallel
//! `CREATE INDEX` of the rendered definition and a plain `DROP INDEX`, a
//! partitioned index's leaves each with a plain `CREATE INDEX` of their own,
//! verifies the declared set and records the version as it commits. A plain
//! build reads the table once and waits for no other transaction, where a
//! concurrent one reads it twice and waits for every transaction that could
//! use the index. An interrupted migration rolls back whole, leaving no
//! invalid index and no partial drop, while the migrations the run recorded
//! before it stay recorded, and a rerun without the flag builds
//! concurrently. Frontends never build offline: they run this loop too, with
//! `PRISM_POSTGRES_INIT_SCHEMA=1`.
//!
//! The same entry point runs 002's share-hash backfill on a populated 2.x.x
//! source (`share_hashes.rs`, #582). 002's file is applied in the migration
//! transaction like any other; only the mapping of the legacy shares is
//! left for after the commit, in batches, and that run records 2. It runs
//! before 013, 017, 024 and 031, so every version is still recorded after
//! the ones below it. `migrate --defer-share-hashes` runs the backfill's
//! recent range in that slot instead, before 013 drops the index that serves
//! it, and 013, 017, 024 and 031 follow with the backfill pending. Once
//! serving is permitted, plain `migrate` maps the rest after them, throttled
//! as `backfill-share-hashes` maps it, and 2 is recorded last.
use super::*;
use sqlx::{Connection, PgConnection};
use std::time::{Duration, Instant};

/// One migration applied after the commit, by the runner its kind names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OnlineMigration {
    /// Index creates and drops, applied with `CONCURRENTLY` (013, 024, and
    /// 031 leaf by leaf), or with plain statements in one transaction under
    /// `migrate --offline-indexes` (`IndexBuildMode`).
    Indexes(IndexMigration),
    /// The share ledger partition conversion (017, `partition.rs`).
    Partitions(super::partition::PartitionMigration),
    /// 002's share-hash backfill on a populated 2.x.x source, in batches
    /// (`share_hashes.rs`, #582). Its file is applied in the transaction;
    /// only the backfill runs here, and it records 2. Whose connect
    /// scheduled it decides whether it maps a backfill that permits serving.
    ShareHashes(super::ShareHashBackfill),
    /// The recent range of 002's share-hash backfill, which permits serving
    /// with the rest pending (`migrate --defer-share-hashes`). It records
    /// nothing in the migration history.
    ShareHashesRecent,
}

impl OnlineMigration {
    pub(super) fn version(&self) -> i32 {
        match self {
            OnlineMigration::Indexes(migration) => migration.version,
            OnlineMigration::Partitions(migration) => migration.version,
            OnlineMigration::ShareHashes(_) | OnlineMigration::ShareHashesRecent => 2,
        }
    }
}

/// What one index migration changes on the source, derived from the
/// scratch apply: the indexes it creates, rendered by `pg_get_indexdef`,
/// and the original definitions of the indexes it drops.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IndexMigration {
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
) -> Result<IndexMigration> {
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
    // Each partition by its partitioned table. The scratch schema's
    // partitions are not the source's: an index created on a partitioned
    // table is declared by its parent alone, and the runner builds one leaf
    // for each partition it finds on the source.
    let partition_of: BTreeMap<&str, &str> = after
        .tables
        .iter()
        .filter_map(|(name, table)| {
            table
                .parents
                .first()
                .map(|parent| (name.as_str(), parent.as_str()))
        })
        .collect();
    let mut creates = BTreeMap::new();
    for (name, definition) in &after.indexes {
        match before.indexes.get(name) {
            None => {
                if let Some(parent) = partition_of.get(definition.table.as_str()) {
                    ensure!(
                        after.indexes.iter().any(|(other, declared)| {
                            !before.indexes.contains_key(other)
                                && declared.table == *parent
                                && partitioned_rest(other, declared).is_some()
                        }),
                        "migration {version} is applied online and creates index {name} on the partition {} directly; an online migration creates an index of a partitioned table on the table, and the runner builds its partitions' leaves",
                        definition.table
                    );
                    continue;
                }
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
    for (name, definition) in &drops {
        ensure!(
            partitioned_rest(name, definition).is_none()
                && !partition_of.contains_key(definition.table.as_str()),
            "migration {version} is applied online and drops index {name} of a partitioned table, which DROP INDEX CONCURRENTLY cannot do; move the drop into a transactional migration"
        );
    }
    ensure!(
        !creates.is_empty() || !drops.is_empty(),
        "migration {version} is applied online but creates and drops no index"
    );
    Ok(IndexMigration {
        version,
        creates,
        drops,
    })
}

/// How an index migration's builds and drops reach the source (013, 024,
/// 031).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum IndexBuildMode {
    /// `CREATE INDEX CONCURRENTLY` and `DROP INDEX CONCURRENTLY`, statement
    /// by statement, while appends continue: every connect's, a frontend's
    /// with `PRISM_POSTGRES_INIT_SCHEMA=1` included.
    #[default]
    Concurrent,
    /// A plain `CREATE INDEX` and `DROP INDEX` in the transaction that
    /// records the version, each build with up to `workers` parallel
    /// maintenance workers, once no instance is live
    /// (`migrate --offline-indexes`, `apply_offline`).
    Offline {
        workers: u16,
        /// maintenance_work_mem for the transaction, in kilobytes
        /// (`--index-build-memory`). `None` takes 2GB, or the server's
        /// setting when that is higher.
        memory_kb: Option<u32>,
    },
}

/// Apply one online migration to the source and record it. Idempotent:
/// what an earlier run built or dropped is kept, and a version another
/// instance recorded meanwhile is not applied again. `index_build` is how
/// an index migration builds; the other runners ignore it.
pub(crate) async fn apply_online_migration(
    pool: &PgPool,
    migration: &OnlineMigration,
    index_build: IndexBuildMode,
    metrics: Option<&crate::metrics::Metrics>,
) -> Result<()> {
    let version = migration.version();
    // A connection of its own, never returned to the pool: the session
    // settings and the session-level lock below end with it.
    let mut connection = crate::metrics::time_pool_acquire(metrics, pool.acquire())
        .await?
        .detach();
    let mut run_connection = super::share_hashes::RunConnection::Kept;
    let outcome = match migration {
        OnlineMigration::Indexes(migration) => {
            apply(&mut connection, migration, index_build, metrics).await
        }
        OnlineMigration::Partitions(migration) => {
            super::partition::apply(&mut connection, migration, metrics).await
        }
        OnlineMigration::ShareHashes(backfill) => {
            super::share_hashes::apply(
                &mut connection,
                &pool.connect_options(),
                *backfill,
                metrics,
                &mut run_connection,
            )
            .await
        }
        OnlineMigration::ShareHashesRecent => {
            super::share_hashes::map_recent(&mut connection, metrics).await
        }
    };
    // A connection the share-hash record lost is dropped, not closed,
    // whether the migration finished or stopped.
    let closed = super::share_hashes::close_unless_lost(connection, run_connection).await;
    outcome?;
    closed.with_context(|| format!("closing the connection that applied migration {version}"))?;
    Ok(())
}

/// Take the session-level runner lock on this connection, keyed by the
/// ledger's schema. A blocking advisory-lock SELECT keeps its statement
/// snapshot while waiting. A concurrent partial-index build can wait for
/// that snapshot to end, deadlocking with the next runner waiting for this
/// lock. End each attempt before sleeping, outside any database
/// transaction.
pub(super) async fn acquire_runner_lock(connection: &mut PgConnection) -> Result<()> {
    while !sqlx::query_scalar::<_, bool>(
        "SELECT pg_try_advisory_lock($1,hashtext(current_schema()))",
    )
    .bind(ONLINE_DDL_LOCK_CLASS)
    .fetch_one(&mut *connection)
    .await?
    {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}

async fn apply(
    connection: &mut PgConnection,
    migration: &IndexMigration,
    index_build: IndexBuildMode,
    metrics: Option<&crate::metrics::Metrics>,
) -> Result<()> {
    let version = migration.version;
    // A build on a large ledger runs for hours, and CONCURRENTLY waits for
    // every transaction that could use the index; the pool's per-statement
    // and lock timeouts would abort it. Session-level, on this connection.
    // An offline run sets its own lock timeout, for its transaction.
    sqlx::query(
        "SELECT set_config('statement_timeout','0',false),set_config('lock_timeout','0',false)",
    )
    .execute(&mut *connection)
    .await?;
    acquire_runner_lock(connection).await?;
    if recorded(connection, version).await? {
        tracing::info!(version, "online migration already recorded");
        return Ok(());
    }
    match index_build {
        IndexBuildMode::Concurrent => apply_concurrently(connection, migration, metrics).await,
        IndexBuildMode::Offline { workers, memory_kb } => {
            apply_offline(connection, migration, workers, memory_kb, metrics).await
        }
    }
}

/// The plan for `migration`, from what each name it reserves holds now.
async fn plan<'a>(
    connection: &mut PgConnection,
    migration: &'a IndexMigration,
) -> Result<Vec<Step<'a>>> {
    let version = migration.version;
    // Every reserved name is inspected before any DDL, so a refusal leaves
    // the database as it was: an index built before a refusal would be
    // adopted on the next run, but the operator is told that nothing
    // changed, and that must be true.
    let mut plan = Vec::new();
    for (name, expected) in &migration.creates {
        let partitioned = partitioned_rest(name, expected).is_some();
        match live_relation(connection, name).await? {
            LiveRelation::Absent => {
                if partitioned {
                    inspect_partitions(connection, version, name, expected).await?;
                }
                plan.push(Step::Build(name, expected));
            }
            LiveRelation::Index {
                valid,
                definition,
                table,
            } if table == expected.table && definition == expected.definition => {
                plan.push(if valid {
                    Step::Keep(name)
                } else if partitioned {
                    // An interrupted run's parent, still waiting for leaves:
                    // dropping it would drop every leaf attached to it.
                    inspect_partitions(connection, version, name, expected).await?;
                    Step::Build(name, expected)
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
    Ok(plan)
}

/// Run the plan statement by statement with `CONCURRENTLY`, appends
/// continuing, then record the version.
async fn apply_concurrently(
    connection: &mut PgConnection,
    migration: &IndexMigration,
    metrics: Option<&crate::metrics::Metrics>,
) -> Result<()> {
    if migration
        .creates
        .iter()
        .any(|(name, expected)| partitioned_rest(name, expected).is_some())
    {
        // Only the share archive detaches a partition, and a partition that
        // leaves while a partitioned index waits for its leaf leaves that
        // index invalid for good. Held, on this connection, until the run
        // ends; a share-archive command already running finishes first.
        tracing::info!(
            version = migration.version,
            "taking the share archive lifecycle lock: no share-archive command runs until the partitioned index is built"
        );
        // A share-archive command can hold the lock for long; say so while
        // the build waits for it, rather than appear stuck.
        let lifecycle = super::super::connect::session_lock(
            connection,
            super::super::archive::LIFECYCLE_LOCK,
            metrics,
        );
        tokio::pin!(lifecycle);
        let started = Instant::now();
        let mut waiting = tokio::time::interval(Duration::from_secs(30));
        waiting.tick().await;
        loop {
            tokio::select! {
                taken = &mut lifecycle => {
                    taken?;
                    break;
                }
                _ = waiting.tick() => tracing::warn!(
                    version = migration.version,
                    waited_s = started.elapsed().as_secs(),
                    "still waiting for the share archive lifecycle lock: a share-archive command holds it, and the partitioned index is built once it ends"
                ),
            }
        }
    }
    let plan = plan(connection, migration).await?;
    let mut progress = Progress::default();
    run_plan(
        connection,
        migration.version,
        plan,
        IndexBuildMode::Concurrent,
        &mut progress,
    )
    .await?;
    let mut tx = connection.begin().await?;
    lock(&mut tx, MIGRATION_LOCK, metrics).await?;
    record(tx, migration, &progress).await
}

/// `migrate --offline-indexes`: the plan run in one transaction that
/// records the version, with a plain `CREATE INDEX` of each rendered
/// definition and a plain `DROP INDEX`, once no instance is live (`apply`
/// has set the session up, taken the runner lock and found the version
/// unrecorded). Every append and read of the tables waits for the build,
/// so before any DDL the transaction refuses an instance that has not
/// reported drained or stopped and a live legacy writer lease, keeping
/// the instance and lease tables locked until it commits so that none
/// registers meanwhile, and it takes the tables' locks with a 5 s lock
/// timeout: a writer or reader still open on them, or one that slipped
/// past the instance check, ends the run there instead of queuing every
/// later statement behind it.
///
/// Anything that ends the transaction before its commit, a refusal, an
/// error or an interruption, rolls this migration back whole: no invalid
/// index stays behind, nothing is dropped and the version is not recorded,
/// so every refusal says the migration changed nothing. The migrations
/// this `migrate` recorded before it stay recorded. A rerun without the
/// flag builds concurrently.
async fn apply_offline(
    connection: &mut PgConnection,
    migration: &IndexMigration,
    workers: u16,
    memory_kb: Option<u32>,
    metrics: Option<&crate::metrics::Metrics>,
) -> Result<()> {
    let version = migration.version;
    let mode = IndexBuildMode::Offline { workers, memory_kb };
    let mut tx = connection.begin().await?;
    lock(&mut tx, MIGRATION_LOCK, metrics).await?;
    // For this transaction only. A parallel build divides
    // maintenance_work_mem among its leader and workers: the operator's
    // `--index-build-memory`, or at least 2GB unless the server's is higher.
    let memory = memory_kb.map(|kb| format!("{kb}kB"));
    sqlx::query("SELECT set_config('lock_timeout','5s',true),set_config('max_parallel_maintenance_workers',$1,true),set_config('maintenance_work_mem',COALESCE($2,CASE WHEN pg_size_bytes(current_setting('maintenance_work_mem'))<pg_size_bytes('2GB') THEN '2GB' ELSE current_setting('maintenance_work_mem') END),true)")
        .bind(workers.to_string())
        .bind(&memory)
        .execute(&mut *tx)
        .await
        .with_context(|| {
            format!(
                "PostgreSQL refused migration {version}'s offline build settings ({workers} workers, maintenance_work_mem {}); pass --index-build-workers and --index-build-memory values it accepts",
                memory.as_deref().unwrap_or("2GB or the server's")
            )
        })?;
    // Only the guards' own refusals name a live instance or writer. An
    // error from their statements, such as a lock timeout behind a
    // heartbeat, is reported as itself.
    let guarded = |outcome: Result<()>| -> Result<()> {
        outcome.map_err(|error| {
            if error.downcast_ref::<sqlx::Error>().is_some() {
                error.context(format!("could not check for live instances and legacy writers before migration {version}'s offline index build"))
            } else {
                error.context(format!("refusing to build migration {version}'s indexes offline, before any DDL: --offline-indexes holds their tables for the whole build, so no instance or legacy writer may be live. The migration is not recorded and nothing was changed by it. Stop every frontend and legacy writer and migrate again, or run plain `qbit-prism-server migrate`, which builds CONCURRENTLY and records without stopping frontends"))
            }
        })
    };
    guarded(refuse_unquiesced_instances(&mut tx, version).await)?;
    guarded(refuse_live_legacy_lease(&mut tx).await)?;
    let tables: BTreeSet<&str> = migration
        .creates
        .values()
        .chain(migration.drops.values())
        .map(|index| index.table.as_str())
        .collect();
    let quoted: Vec<String> = tables.iter().map(|table| quote_identifier(table)).collect();
    let names = tables.iter().copied().collect::<Vec<_>>().join(", ");
    sqlx::raw_sql(&format!(
        "LOCK TABLE {} IN ACCESS EXCLUSIVE MODE",
        quoted.join(",")
    ))
    .execute(&mut *tx)
    .await
    .map_err(|error| {
        let context = lock_failure(
            version,
            &names,
            error
                .as_database_error()
                .and_then(|error| error.code())
                .as_deref(),
        );
        anyhow::Error::new(error).context(context)
    })?;
    // A build takes its workers out of these two, or builds with fewer.
    let (maintenance_work_mem, max_parallel_workers, max_worker_processes): (String, String, String) =
        sqlx::query_as("SELECT current_setting('maintenance_work_mem'),current_setting('max_parallel_workers'),current_setting('max_worker_processes')")
            .fetch_one(&mut *tx)
            .await?;
    tracing::info!(
        version,
        workers,
        max_parallel_workers,
        max_worker_processes,
        maintenance_work_mem,
        "building the migration's indexes offline: no instance is live, and its tables stay locked until the migration is recorded"
    );
    let plan = plan(&mut tx, migration).await?;
    let mut progress = Progress::default();
    run_plan(&mut tx, version, plan, mode, &mut progress).await?;
    record(tx, migration, &progress).await
}

/// The context of an offline run's failed `LOCK TABLE`. Only a lock
/// timeout (55P03) means a transaction still holds a table, which the run
/// refuses to wait behind; any other failure, a deadlock, a cancel, a
/// missing table, is reported as itself.
fn lock_failure(version: i32, tables: &str, code: Option<&str>) -> String {
    if code == Some("55P03") {
        format!("refusing to build migration {version}'s indexes offline, before any DDL: could not lock {tables} within the run's 5 s lock timeout. A transaction still uses the table (an open writer, a running export or another reader), or a frontend is starting. The migration is not recorded and nothing was changed by it. Let it finish or stop it and migrate again, or run plain `qbit-prism-server migrate`, which builds CONCURRENTLY and records without stopping frontends")
    } else {
        format!("could not lock {tables} for migration {version}'s offline index build")
    }
}

/// Run the plan's steps in order, each built or dropped as `mode` says,
/// and keep in `progress` what a concurrent step changed: that stays when
/// a later step refuses, while an offline step's change rolls back with
/// the migration's transaction, so an offline refusal says nothing was
/// changed.
async fn run_plan<'a>(
    connection: &mut PgConnection,
    version: i32,
    plan: Vec<Step<'a>>,
    mode: IndexBuildMode,
    progress: &mut Progress,
) -> Result<()> {
    let lasting = mode == IndexBuildMode::Concurrent;
    for step in plan {
        match step {
            Step::Keep(name) => tracing::info!(
                version,
                index = %name,
                "index already built with the declared definition; keeping it"
            ),
            Step::Build(name, expected) => {
                if partitioned_rest(name, expected).is_some() {
                    build_partitioned(connection, version, name, expected, mode, progress).await?;
                } else {
                    build(connection, version, name, expected, mode).await?;
                }
                if lasting {
                    progress.built.push(name.to_owned());
                }
            }
            Step::Rebuild(name, expected) => {
                drop_planned(
                    connection,
                    version,
                    name,
                    expected,
                    Planned::InvalidBuild,
                    progress,
                    mode,
                )
                .await?;
                tracing::warn!(
                    version,
                    index = %name,
                    "dropped the invalid index an interrupted build left; building again"
                );
                build(connection, version, name, expected, mode).await?;
                if lasting {
                    progress.built.push(name.to_owned());
                }
            }
            Step::Drop(name, expected) => {
                drop_planned(
                    connection,
                    version,
                    name,
                    expected,
                    Planned::Release,
                    progress,
                    mode,
                )
                .await?;
                if lasting {
                    progress.dropped.push(name.to_owned());
                }
                tracing::info!(version, index = %name, table = %expected.table, "dropped replaced index");
            }
            Step::Dropped(name) => tracing::info!(version, index = %name, "index already dropped"),
        }
    }
    Ok(())
}

/// Verify the declared set once more and record the version in `tx`,
/// which holds the migration lock, then commit.
async fn record(
    mut tx: sqlx::Transaction<'_, Postgres>,
    migration: &IndexMigration,
    progress: &Progress,
) -> Result<()> {
    let version = migration.version;
    verify_declared(&mut tx, migration, progress).await?;
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

/// What `name` holds in the current schema. An index of a partitioned
/// table (`I`) is an index here too, rendered `... ON ONLY <table> ...`, and
/// valid once every partition's leaf is attached to it.
async fn live_relation(connection: &mut PgConnection, name: &str) -> Result<LiveRelation> {
    let row = sqlx::query("SELECT c.relkind::text AS kind,current_schema()::text AS schema,CASE WHEN c.relkind IN ('i','I') THEN pg_get_indexdef(c.oid) END AS definition,x.indisvalid AS valid,t.relname::text AS table_name FROM pg_class c LEFT JOIN pg_index x ON x.indexrelid=c.oid LEFT JOIN pg_class t ON t.oid=x.indrelid WHERE c.relnamespace=current_schema()::regnamespace AND c.relname=$1")
        .bind(name)
        .fetch_optional(&mut *connection)
        .await?;
    let Some(row) = row else {
        return Ok(LiveRelation::Absent);
    };
    let kind: String = row.try_get("kind")?;
    if kind != "i" && kind != "I" {
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

pub(super) fn relation_kind(kind: &str) -> &'static str {
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
    mode: IndexBuildMode,
) -> Result<()> {
    let statement = create_statement(&expected.definition, mode)
        .with_context(|| format!("migration {version}, index {name}"))?;
    match mode {
        IndexBuildMode::Concurrent => tracing::info!(
            version,
            index = %name,
            table = %expected.table,
            "building index concurrently; appends continue, and on a large ledger this takes about two table scans"
        ),
        IndexBuildMode::Offline { workers, .. } => tracing::info!(
            version,
            index = %name,
            table = %expected.table,
            workers,
            "building index offline; appends wait for the migration's commit, and on a large ledger this takes one table scan"
        ),
    }
    let started = Instant::now();
    sqlx::raw_sql(&statement)
        .execute(&mut *connection)
        .await
        .with_context(|| match mode {
            IndexBuildMode::Concurrent => format!("building index {name} for migration {version}; if the build was interrupted the index is invalid, and the next migrate drops and rebuilds it"),
            IndexBuildMode::Offline { .. } => format!("building index {name} for migration {version} offline; this migration's changes roll back with its transaction, the migrations this run recorded before it stay recorded, and the next migrate plans afresh"),
        })?;
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
    for prefix in CREATE_INDEX {
        if let Some(rest) = definition.strip_prefix(prefix) {
            return Ok(format!("{prefix}CONCURRENTLY {rest}"));
        }
    }
    bail!("index definition does not start with CREATE INDEX: {definition}")
}

/// How a `pg_get_indexdef` rendering starts.
const CREATE_INDEX: [&str; 2] = ["CREATE UNIQUE INDEX ", "CREATE INDEX "];

/// The statement a build runs in `mode`: the concurrent build, or offline
/// the rendering itself, verbatim, a plain build inside the transaction
/// that records the version. Either way the rendering must be a
/// `CREATE INDEX`.
fn create_statement(definition: &str, mode: IndexBuildMode) -> Result<String> {
    match mode {
        IndexBuildMode::Concurrent => concurrent_create(definition),
        IndexBuildMode::Offline { .. } => {
            ensure!(
                CREATE_INDEX
                    .iter()
                    .any(|prefix| definition.starts_with(prefix)),
                "index definition does not start with CREATE INDEX: {definition}"
            );
            Ok(definition.to_owned())
        }
    }
}

/// The kind (`CREATE INDEX ` or `CREATE UNIQUE INDEX `) and the rest of the
/// rendering of an index of a partitioned table, `<kind><name> ON ONLY
/// <table> <rest>`, which is how the scratch apply renders an index created
/// on one; `None` for any other rendering.
fn partitioned_rest<'a>(
    name: &str,
    declared: &'a IndexDefinition,
) -> Option<(&'static str, &'a str)> {
    CREATE_INDEX.iter().find_map(|kind| {
        declared
            .definition
            .strip_prefix(format!("{kind}{name} ON ONLY {} ", declared.table).as_str())
            .map(|rest| (*kind, rest))
    })
}

/// A name PostgreSQL renders, and accepts, without quotes.
fn plain_identifier(name: &str) -> bool {
    name.starts_with(|first: char| first.is_ascii_lowercase() || first == '_')
        && name.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
        })
}

/// One partition's leaf of a partitioned index: its name, and the plain
/// index of the partition it must read back as.
struct Leaf {
    name: String,
    declared: IndexDefinition,
}

/// The leaf of `name`, declared on a partitioned table, for `partition`:
/// named `<partition><suffix>`, the suffix being `name` past the table's
/// name, as `qbit_prism_share_partition_create` names the leaves of every
/// partition it creates, with the parent's rendering on the partition.
fn leaf_of(name: &str, declared: &IndexDefinition, partition: &str) -> Result<Leaf> {
    let (kind, rest) = partitioned_rest(name, declared)
        .with_context(|| format!("index {name} is not declared on a partitioned table"))?;
    let suffix = name.strip_prefix(declared.table.as_str()).with_context(|| {
        format!(
            "index {name} of the partitioned table {} does not begin with the table's name, so its leaves cannot be named after their partitions",
            declared.table
        )
    })?;
    let leaf = format!("{partition}{suffix}");
    ensure!(
        plain_identifier(partition) && plain_identifier(&leaf) && leaf.len() <= 63,
        "the leaf of index {name} for partition {partition} would be named {leaf}, which is not a plain identifier of at most 63 bytes"
    );
    Ok(Leaf {
        declared: IndexDefinition {
            table: partition.to_owned(),
            definition: format!("{kind}{leaf} ON {partition} {rest}"),
            ..declared.clone()
        },
        name: leaf,
    })
}

/// One partition of the table a partitioned index is declared on, oldest
/// first, and the valid leaf of that index attached for it, if any.
struct Partition {
    name: String,
    attached: Option<String>,
}

/// The partitions of the table `name` is declared on, in the current
/// schema, refusing one this runner cannot give a leaf that PostgreSQL then
/// counts: a partition that is itself partitioned, one still detaching, and
/// one whose attached leaf is not valid. PostgreSQL keeps the parent invalid
/// for as long as any of these remains.
async fn partitions(
    connection: &mut PgConnection,
    version: i32,
    name: &str,
    declared: &IndexDefinition,
    progress: &Progress,
) -> Result<Vec<Partition>> {
    let table = &declared.table;
    let rows = sqlx::query("SELECT c.relname::text AS name,c.relkind::text AS kind,h.inhdetachpending AS detaching,leaf.relname::text AS leaf,leaf.valid FROM pg_class p JOIN pg_inherits h ON h.inhparent=p.oid JOIN pg_class c ON c.oid=h.inhrelid LEFT JOIN LATERAL (SELECT i.relname,x.indisvalid AS valid FROM pg_class pi JOIN pg_inherits ih ON ih.inhparent=pi.oid JOIN pg_class i ON i.oid=ih.inhrelid JOIN pg_index x ON x.indexrelid=i.oid WHERE pi.relnamespace=p.relnamespace AND pi.relname=$2 AND x.indrelid=c.oid) leaf ON true WHERE p.relnamespace=current_schema()::regnamespace AND p.relname=$1 ORDER BY c.oid")
        .bind(table)
        .bind(name)
        .fetch_all(&mut *connection)
        .await?;
    let mut found = Vec::with_capacity(rows.len());
    for row in rows {
        let partition: String = row.try_get("name")?;
        let kind: String = row.try_get("kind")?;
        ensure!(
            kind == "r",
            "refusing to apply migration {version}: partition {partition} of {table} is a {}, and the runner builds the leaves of {name} on table partitions only. The migration is not recorded. {}",
            relation_kind(&kind),
            progress.describe()
        );
        ensure!(
            !row.try_get::<bool, _>("detaching")?,
            "refusing to apply migration {version}: partition {partition} of {table} is still being detached (an interrupted DETACH PARTITION ... CONCURRENTLY), and PostgreSQL never marks {name} valid while it is. The migration is not recorded. {} Finish the detach with `qbit-prism-server share-archive detach {partition}`, then migrate again",
            progress.describe()
        );
        let leaf: Option<String> = row.try_get("leaf")?;
        let valid: Option<bool> = row.try_get("valid")?;
        if let Some(leaf) = &leaf {
            ensure!(
                valid == Some(true),
                "refusing to apply migration {version}: index {leaf} of partition {partition} is attached to {name} but is not valid, and PostgreSQL never marks {name} valid while it is. The migration is not recorded. {} Drop {name} with `DROP INDEX {name}`, which drops every leaf attached to it, then migrate again",
                progress.describe()
            );
        }
        found.push(Partition {
            name: partition,
            attached: leaf,
        });
    }
    Ok(found)
}

/// Before any DDL, for a partitioned index the plan builds or resumes:
/// every partition can take its leaf, and the leaf name of each partition
/// with none attached is free or holds the leaf the migration declares, an
/// earlier run's, valid or not. So a refusal leaves the database as it was.
async fn inspect_partitions(
    connection: &mut PgConnection,
    version: i32,
    name: &str,
    declared: &IndexDefinition,
) -> Result<()> {
    let nothing = Progress::default();
    for partition in partitions(connection, version, name, declared, &nothing).await? {
        if partition.attached.is_some() {
            continue;
        }
        let leaf = leaf_of(name, declared, &partition.name)?;
        match live_relation(connection, &leaf.name).await? {
            LiveRelation::Absent => {}
            LiveRelation::Index {
                definition, table, ..
            } if table == leaf.declared.table && definition == leaf.declared.definition => {}
            LiveRelation::Index {
                definition, table, ..
            } => bail!(
                "refusing to apply migration {version}: index {} on {table} already exists with a different definition ({definition}); the migration declares {} as the leaf of {name} for partition {}. The migration is not recorded and nothing was changed by it. Check what that index serves, then rename or drop it and migrate again",
                leaf.name,
                leaf.declared.definition,
                partition.name
            ),
            LiveRelation::Other(kind) => bail!(
                "refusing to apply migration {version}: a {kind} named {} holds the name of the leaf of {name} this migration builds for partition {}. The migration is not recorded and nothing was changed by it. Check what it holds, then rename or move it aside and migrate again",
                leaf.name,
                partition.name
            ),
        }
    }
    Ok(())
}

/// Build the partitioned index `name` (see the module docs), in passes
/// until PostgreSQL has marked it valid. Each pass builds, or keeps, the
/// leaf of every partition with none attached, creates the parent ON ONLY
/// when it is missing, and attaches those leaves. A partition attached
/// before the parent exists has no leaf until the next pass; one attached
/// after it gets its leaf from the parent's definition as it is attached.
async fn build_partitioned(
    connection: &mut PgConnection,
    version: i32,
    name: &str,
    declared: &IndexDefinition,
    mode: IndexBuildMode,
    progress: &mut Progress,
) -> Result<()> {
    let started = Instant::now();
    loop {
        let pending = partitions(connection, version, name, declared, progress)
            .await?
            .into_iter()
            .filter(|partition| partition.attached.is_none())
            .map(|partition| leaf_of(name, declared, &partition.name))
            .collect::<Result<Vec<_>>>()?;
        for leaf in &pending {
            build_leaf(connection, version, name, leaf, mode, progress).await?;
        }
        match live_relation(connection, name).await? {
            LiveRelation::Absent => create_parent(connection, version, name, declared, mode).await?,
            LiveRelation::Index {
                definition, table, ..
            } if table == declared.table && definition == declared.definition => {}
            other => bail!(
                "refusing to continue migration {version}: {}, but the migration declares {} on {}. The migration is not recorded. {} Check what happened under that name, then migrate again; the next run plans afresh from what it finds",
                other.describe(name),
                declared.definition,
                declared.table,
                progress.describe()
            ),
        }
        for leaf in &pending {
            attach_leaf(connection, version, name, leaf, mode, progress).await?;
        }
        match live_relation(connection, name).await? {
            LiveRelation::Index {
                valid,
                definition,
                table,
            } if table == declared.table && definition == declared.definition => {
                if valid {
                    break;
                }
                // Unless a partition was attached after this pass listed
                // them, before the parent existed, whose leaf the next pass
                // builds, every partition's leaf is attached.
                ensure!(
                    !pending.is_empty(),
                    "refusing to record migration {version}: index {name} on {} is still not valid with every partition's leaf attached. PostgreSQL marks a partitioned index valid only as it attaches the last missing leaf, so a partition that had none left {} while this run built. The migration is not recorded. {} Drop {name} with `DROP INDEX {name}`, which drops every leaf attached to it, then migrate again",
                    declared.table,
                    declared.table,
                    progress.describe()
                );
            }
            other => bail!(
                "refusing to continue migration {version}: {}, but the migration declares {} on {}. The name changed after this run's step for it. The migration is not recorded. {} Check what happened under that name, then migrate again; the next run plans afresh from what it finds",
                other.describe(name),
                declared.definition,
                declared.table,
                progress.describe()
            ),
        }
    }
    tracing::info!(
        version,
        index = %name,
        table = %declared.table,
        elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "partitioned index built: every partition's leaf is attached"
    );
    Ok(())
}

/// Build the leaf of `name` for one partition, or keep an earlier run's.
/// The preflight saw its name free or holding this leaf, but the builds
/// before it can take hours, so the name is looked up again: a valid leaf
/// of the declared definition is kept, an invalid one is dropped and built
/// again, and anything else is refused.
async fn build_leaf(
    connection: &mut PgConnection,
    version: i32,
    name: &str,
    leaf: &Leaf,
    mode: IndexBuildMode,
    progress: &mut Progress,
) -> Result<()> {
    match live_relation(connection, &leaf.name).await? {
        LiveRelation::Absent => {}
        LiveRelation::Index {
            valid: true,
            definition,
            table,
        } if table == leaf.declared.table && definition == leaf.declared.definition => {
            tracing::info!(
                version,
                index = %leaf.name,
                table = %leaf.declared.table,
                "leaf already built with the declared definition; keeping it"
            );
            return Ok(());
        }
        LiveRelation::Index {
            valid: false,
            definition,
            table,
        } if table == leaf.declared.table && definition == leaf.declared.definition => {
            drop_planned(
                connection,
                version,
                &leaf.name,
                &leaf.declared,
                Planned::InvalidBuild,
                progress,
                mode,
            )
            .await?;
            tracing::warn!(
                version,
                index = %leaf.name,
                "dropped the invalid leaf an interrupted build left; building again"
            );
        }
        other => bail!(
            "refusing to continue migration {version}: {}, where the migration builds {} as the leaf of {name}. The migration is not recorded. {} Check what that index serves, then rename or drop it and migrate again",
            other.describe(&leaf.name),
            leaf.declared.definition,
            progress.describe()
        ),
    }
    build(connection, version, &leaf.name, &leaf.declared, mode).await?;
    if mode == IndexBuildMode::Concurrent {
        progress.built.push(leaf.name.clone());
    }
    Ok(())
}

/// Create the partitioned index ON ONLY its table, from the rendering as it
/// is. Catalog work, but under a SHARE lock on the table, which waits for
/// every open append: concurrently the lock is taken with a short lock
/// timeout and retried; offline the run already holds the table.
async fn create_parent(
    connection: &mut PgConnection,
    version: i32,
    name: &str,
    declared: &IndexDefinition,
    mode: IndexBuildMode,
) -> Result<()> {
    match mode {
        IndexBuildMode::Concurrent => {
            super::partition::with_lock_retries(
                connection,
                version,
                &format!("creating index {name} ON ONLY {}", declared.table),
                &declared.table,
                &declared.definition,
            )
            .await?;
        }
        IndexBuildMode::Offline { .. } => {
            sqlx::raw_sql(&declared.definition)
                .execute(&mut *connection)
                .await
                .with_context(|| format!("creating index {name} for migration {version} offline; this migration's changes roll back with its transaction, and the next migrate plans afresh"))?;
        }
    }
    tracing::info!(
        version,
        index = %name,
        table = %declared.table,
        "partitioned index created ON ONLY its table; attaching the leaves"
    );
    Ok(())
}

/// Attach a leaf to the partitioned index, after looking it up again: it
/// was built or kept earlier in this pass, possibly hours before. Catalog
/// work, but under an ACCESS EXCLUSIVE lock on the leaf index, which waits
/// for every open read of its partition: concurrently the lock is taken with
/// a short lock timeout and retried; offline no read is open.
async fn attach_leaf(
    connection: &mut PgConnection,
    version: i32,
    name: &str,
    leaf: &Leaf,
    mode: IndexBuildMode,
    progress: &Progress,
) -> Result<()> {
    match live_relation(connection, &leaf.name).await? {
        LiveRelation::Index {
            valid: true,
            definition,
            table,
        } if table == leaf.declared.table && definition == leaf.declared.definition => {}
        other => bail!(
            "refusing to continue migration {version}: {}, but this run left a valid {} there to attach to {name}. The name changed after this run's step for it. The migration is not recorded. {} Check what happened under that name, then migrate again; the next run plans afresh from what it finds",
            other.describe(&leaf.name),
            leaf.declared.definition,
            progress.describe()
        ),
    }
    let statement = format!(
        "ALTER INDEX {} ATTACH PARTITION {}",
        quote_identifier(name),
        quote_identifier(&leaf.name)
    );
    match mode {
        IndexBuildMode::Concurrent => {
            super::partition::with_lock_retries(
                connection,
                version,
                &format!("attaching leaf {} to {name}", leaf.name),
                &leaf.declared.table,
                &statement,
            )
            .await?;
        }
        IndexBuildMode::Offline { .. } => {
            sqlx::raw_sql(&statement)
                .execute(&mut *connection)
                .await
                .with_context(|| {
                    format!(
                        "attaching leaf {} to {name} for migration {version} offline",
                        leaf.name
                    )
                })?;
        }
    }
    tracing::info!(version, index = %name, leaf = %leaf.name, "leaf attached");
    Ok(())
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
/// true until the first build or drop. A partitioned index's leaves are
/// named after partitions the run finds, so names are owned.
#[derive(Default)]
struct Progress {
    built: Vec<String>,
    dropped: Vec<String>,
}

impl Progress {
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
/// hours of a build. Offline, both run in the build's transaction, whose
/// table lock does not cover a rename either, so the same round trip
/// stays open.
async fn drop_planned(
    connection: &mut PgConnection,
    version: i32,
    name: &str,
    expected: &IndexDefinition,
    planned: Planned,
    progress: &Progress,
    mode: IndexBuildMode,
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
            return drop_index(connection, name, mode).await;
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
    migration: &IndexMigration,
    progress: &Progress,
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

async fn drop_index(connection: &mut PgConnection, name: &str, mode: IndexBuildMode) -> Result<()> {
    sqlx::raw_sql(&drop_statement(name, mode))
        .execute(&mut *connection)
        .await
        .with_context(|| match mode {
            IndexBuildMode::Concurrent => format!("dropping index {name} concurrently"),
            IndexBuildMode::Offline { .. } => format!("dropping index {name} offline"),
        })?;
    Ok(())
}

/// The statement a drop runs in `mode`: `DROP INDEX CONCURRENTLY`, or
/// offline a plain `DROP INDEX` inside the transaction that records the
/// version.
fn drop_statement(name: &str, mode: IndexBuildMode) -> String {
    match mode {
        IndexBuildMode::Concurrent => format!("DROP INDEX CONCURRENTLY {}", quote_identifier(name)),
        IndexBuildMode::Offline { .. } => format!("DROP INDEX {}", quote_identifier(name)),
    }
}

fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub(super) async fn recorded(connection: &mut PgConnection, version: i32) -> Result<bool> {
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

    /// `migrate --offline-indexes` runs the rendering as it is, a plain
    /// build, and drops plainly; every other connect keeps CONCURRENTLY.
    #[test]
    fn an_offline_build_runs_the_rendered_definition_verbatim() {
        let offline = IndexBuildMode::Offline {
            workers: 4,
            memory_kb: None,
        };
        for definition in [
            "CREATE INDEX a ON t USING btree (x DESC) INCLUDE (y) WHERE accepted",
            "CREATE UNIQUE INDEX a ON t USING btree (x)",
        ] {
            assert_eq!(create_statement(definition, offline).unwrap(), definition);
            assert_eq!(
                create_statement(definition, IndexBuildMode::Concurrent).unwrap(),
                concurrent_create(definition).unwrap()
            );
        }
        for mode in [offline, IndexBuildMode::Concurrent] {
            assert!(create_statement("ALTER TABLE t ADD COLUMN c int", mode)
                .unwrap_err()
                .to_string()
                .contains("does not start with CREATE INDEX"));
        }
        assert_eq!(
            drop_statement("odd\"name", offline),
            "DROP INDEX \"odd\"\"name\""
        );
        assert_eq!(
            drop_statement("plain_idx", IndexBuildMode::Concurrent),
            "DROP INDEX CONCURRENTLY \"plain_idx\""
        );
        assert_eq!(
            MigrateOptions::default().index_build,
            IndexBuildMode::Concurrent
        );
    }

    /// Only a lock timeout reads as a transaction still holding the table;
    /// a deadlock, a cancel, a termination or a missing table keeps its own
    /// error under a neutral context.
    #[test]
    fn only_a_lock_timeout_reads_as_a_transaction_holding_the_table() {
        let timeout = lock_failure(13, "qbit_share_ledger", Some("55P03"));
        assert!(
            timeout.starts_with("refusing to build migration 13's indexes offline, before any DDL: could not lock qbit_share_ledger within the run's 5 s lock timeout."),
            "{timeout}"
        );
        assert!(timeout.contains("nothing was changed"), "{timeout}");
        for code in [
            Some("40P01"),
            Some("57014"),
            Some("57P01"),
            Some("42P01"),
            None,
        ] {
            assert_eq!(
                lock_failure(13, "qbit_share_ledger", code),
                "could not lock qbit_share_ledger for migration 13's offline index build",
                "{code:?}"
            );
        }
    }

    #[test]
    fn a_refusal_after_the_plan_says_what_the_run_changed() {
        let mut progress = Progress::default();
        assert_eq!(progress.describe(), "Nothing was changed by it.");
        progress.built.push("a".into());
        progress.built.push("b".into());
        assert_eq!(
            progress.describe(),
            "It had already built a, b; what it built stays, and the next run keeps it."
        );
        progress.dropped.push("c".into());
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

    fn table(parents: &[&str], children: &[&str]) -> TableDefinition {
        TableDefinition {
            persistence: "p".into(),
            row_security: false,
            force_row_security: false,
            parents: parents.iter().map(|name| (*name).to_owned()).collect(),
            children: children.iter().map(|name| (*name).to_owned()).collect(),
            columns: BTreeMap::new(),
        }
    }

    /// A ledger partitioned as 017 leaves it, in the scratch schema.
    fn partitioned() -> SchemaFingerprint {
        let mut schema = SchemaFingerprint::default();
        schema
            .tables
            .insert("t".into(), table(&[], &["t_p0", "t_p1"]));
        for partition in ["t_p0", "t_p1"] {
            schema.tables.insert(partition.into(), table(&["t"], &[]));
        }
        schema
    }

    const ORIGIN: &str = "CREATE INDEX t_origin_idx ON ONLY t USING btree (node, seq)";

    /// 031's shape: an index created on a partitioned table is declared by
    /// its parent alone, whatever leaves the scratch's partitions got.
    #[test]
    fn derivation_declares_an_index_of_a_partitioned_table_by_its_parent_alone() {
        let before = partitioned();
        let mut after = partitioned();
        after
            .indexes
            .insert("t_origin_idx".into(), index("t", ORIGIN));
        for partition in ["t_p0", "t_p1"] {
            after.indexes.insert(
                format!("{partition}_origin_idx"),
                index(
                    partition,
                    &format!("CREATE INDEX {partition}_origin_idx ON {partition} USING btree (node, seq)"),
                ),
            );
        }
        let migration = derive(31, &before, &after).unwrap();
        assert_eq!(
            migration.creates,
            BTreeMap::from([("t_origin_idx".to_owned(), index("t", ORIGIN))])
        );
        assert!(migration.drops.is_empty());
        // A leaf without its parent is an index created on a partition.
        after.indexes.remove("t_origin_idx");
        let error = derive(31, &before, &after).unwrap_err().to_string();
        assert!(
            error.contains("creates index t_p0_origin_idx on the partition t_p0 directly"),
            "{error}"
        );
        // DROP INDEX CONCURRENTLY refuses a partitioned index and its leaves.
        let error = derive(31, &after, &before).unwrap_err().to_string();
        assert!(
            error.contains("drops index t_p0_origin_idx of a partitioned table"),
            "{error}"
        );
        let mut with_parent = partitioned();
        with_parent
            .indexes
            .insert("t_origin_idx".into(), index("t", ORIGIN));
        let error = derive(31, &with_parent, &before).unwrap_err().to_string();
        assert!(
            error.contains("drops index t_origin_idx of a partitioned table"),
            "{error}"
        );
    }

    /// Each leaf is named after its partition and the parent past the
    /// table's name, as qbit_prism_share_partition_create names leaves, and
    /// reads back as the parent's rendering on the partition.
    #[test]
    fn a_leaf_is_named_and_rendered_after_its_partition() {
        let declared = index(
            "qbit_share_ledger",
            "CREATE INDEX qbit_share_ledger_origin_seq_idx ON ONLY qbit_share_ledger USING btree (origin_node, share_seq)",
        );
        assert_eq!(
            partitioned_rest("qbit_share_ledger_origin_seq_idx", &declared),
            Some(("CREATE INDEX ", "USING btree (origin_node, share_seq)"))
        );
        let leaf = leaf_of(
            "qbit_share_ledger_origin_seq_idx",
            &declared,
            "qbit_share_ledger_p12",
        )
        .unwrap();
        assert_eq!(leaf.name, "qbit_share_ledger_p12_origin_seq_idx");
        assert_eq!(leaf.declared.table, "qbit_share_ledger_p12");
        assert_eq!(
            leaf.declared.definition,
            "CREATE INDEX qbit_share_ledger_p12_origin_seq_idx ON qbit_share_ledger_p12 USING btree (origin_node, share_seq)"
        );
        // A plain table's index is not partitioned; a parent whose name does
        // not begin with its table's cannot name its leaves; a partition
        // whose name needs quoting is refused rather than misrendered.
        let plain = index("t", "CREATE INDEX t_idx ON t USING btree (x)");
        assert_eq!(partitioned_rest("t_idx", &plain), None);
        assert!(leaf_of("t_idx", &plain, "t_p0").is_err());
        let foreign = index("t", "CREATE INDEX other_idx ON ONLY t USING btree (x)");
        let error = leaf_of("other_idx", &foreign, "t_p0")
            .err()
            .unwrap()
            .to_string();
        assert!(
            error.contains("does not begin with the table's name"),
            "{error}"
        );
        let error = leaf_of("qbit_share_ledger_origin_seq_idx", &declared, "Odd")
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("not a plain identifier"), "{error}");
        let unique = index("t", "CREATE UNIQUE INDEX t_key ON ONLY t USING btree (seq)");
        assert_eq!(
            leaf_of("t_key", &unique, "t_p3")
                .unwrap()
                .declared
                .definition,
            "CREATE UNIQUE INDEX t_p3_key ON t_p3 USING btree (seq)"
        );
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
