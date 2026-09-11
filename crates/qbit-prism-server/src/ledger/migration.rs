//! Schema migration from a pinned 2.x.x source, the schema gates every
//! start passes, and the explicit one-time migration of Python filesystem
//! artifacts. Validation is performed before any write; operator files and
//! historical rows are retained.
use super::*;
use std::io::Read;
use std::path::{Path, PathBuf};

/// The schema version every native start requires. Bump it with each new
/// migration file. `Ledger::connect` refuses any other version even without
/// `initialize`, so a newer binary never reaches the claim path on a database
/// it has not migrated, and an older binary never writes a schema it does
/// not know.
pub const REQUIRED_SCHEMA_VERSION: i32 = 6;

/// Capability rows this binary understands, with the highest value each may
/// carry. #258's `002_candidate_bodies.sql` declares
/// `candidate_storage_version = 2`; migration 006 declares 1 on every other
/// source. Any other row or value is a database newer than this binary.
const KNOWN_CAPABILITIES: &[(&str, i32)] = &[("candidate_storage_version", 2)];

/// How many blocking outbox rows a drain refusal names.
const BLOCKING_ROWS_NAMED: usize = 16;

/// One row of the source-state table migration 006 pins: what the migrator
/// looks for and what it does when it finds it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SourceStateRule {
    pub name: &'static str,
    pub evidence: &'static str,
    pub verdict: &'static str,
}

/// The source states migration 006 accepts or refuses, as data. Detection is
/// column-aware: it asks the catalog which 002 objects exist, so a fixed
/// predicate never errors on a source that lacks a column, and it looks at
/// outbox rows for the drain check because the capability row proves only
/// that 002 ran.
pub const SOURCE_STATES: [SourceStateRule; 5] = [
    SourceStateRule {
        name: "fresh",
        evidence: "no qbit_share_ledger at all",
        verdict: "accept",
    },
    SourceStateRule {
        name: "pre-#258",
        evidence: "no qbit_prism_schema_capabilities, no 002 object",
        verdict: "accept after the drain check",
    },
    SourceStateRule {
        name: "#258 applied",
        evidence: "candidate_storage_version = 2 and every 002 object present",
        verdict: "accept after the drain check",
    },
    SourceStateRule {
        name: "partial 002",
        evidence: "some 002 objects or the capability row, not all",
        verdict: "refuse, naming the missing object",
    },
    SourceStateRule {
        name: "newer",
        evidence: "candidate_storage_version > 2 or an unknown capability",
        verdict: "refuse before any DDL",
    },
];

/// An accepted source. Each 2.x.x variant names the frozen release SQL under
/// `tests/fixtures/schema_2x` that produces it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceState {
    /// No share ledger: a new native deployment.
    Fresh,
    /// `001_share_ledger.sql` only: v2.0.0 and v2.0.1, before #258.
    Pre258,
    /// 001 and `002_candidate_bodies.sql`: v2.0.2, #258 applied.
    Applied258,
}

impl SourceState {
    /// The value recorded in `qbit_prism_migration_source.source_state`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::Pre258 => "pre_258",
            Self::Applied258 => "258_applied",
        }
    }

    /// The 2.x.x release whose frozen schema this state is, with the commit
    /// that set its `VERSION`. v2.0.0 (`f6854a0`) ships the same 001 as
    /// v2.0.1 and migrates as `Pre258`; v2.0.1 is the minimum supported
    /// release because its offline recovery command is part of the drain.
    pub fn release(self) -> Option<(&'static str, &'static str)> {
        match self {
            Self::Fresh => None,
            Self::Pre258 => Some(("2.0.1", "95ffe063846d51f83999a66cc654da5f7476fdef")),
            Self::Applied258 => Some(("2.0.2", "504846cc0b72e8f86ed17f896d4ccbbe196a31dc")),
        }
    }

    fn rule(self) -> &'static SourceStateRule {
        match self {
            Self::Fresh => &SOURCE_STATES[0],
            Self::Pre258 => &SOURCE_STATES[1],
            Self::Applied258 => &SOURCE_STATES[2],
        }
    }
}

/// What `qbit_prism_migration_source` records after a successful migration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationSource {
    pub source_state: String,
    pub source_release: Option<String>,
    pub source_commit: Option<String>,
    pub candidate_storage_version: Option<i32>,
    pub prior_schema_version: i32,
    pub migrated_by: String,
    pub migrated_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ObjectKind {
    Table,
    Index,
    Function,
    Trigger(&'static str),
    Column(&'static str),
    Constraint(&'static str),
}

const OUTBOX: &str = "qbit_block_candidate_outbox";

/// Every object `002_candidate_bodies.sql` creates, in file order. The
/// "#258 applied" state needs all of them plus the capability row; any
/// subset is "partial 002".
const OBJECTS_002: &[(ObjectKind, &str)] = &[
    (ObjectKind::Table, "qbit_prism_schema_capabilities"),
    (ObjectKind::Table, "qbit_block_candidate_body"),
    (ObjectKind::Index, "qbit_block_candidate_body_orphan_idx"),
    (ObjectKind::Index, "qbit_block_candidate_body_retired_idx"),
    (ObjectKind::Table, "qbit_block_candidate_body_chunk"),
    (ObjectKind::Table, "qbit_block_candidate_body_span"),
    (ObjectKind::Table, "qbit_block_candidate_body_page"),
    (ObjectKind::Function, "qbit_prism_candidate_body_part_guard"),
    (ObjectKind::Function, "qbit_prism_candidate_body_guard"),
    (
        ObjectKind::Trigger("qbit_block_candidate_body_chunk"),
        "qbit_block_candidate_body_chunk_guard",
    ),
    (
        ObjectKind::Trigger("qbit_block_candidate_body_span"),
        "qbit_block_candidate_body_span_guard",
    ),
    (
        ObjectKind::Trigger("qbit_block_candidate_body_page"),
        "qbit_block_candidate_body_page_guard",
    ),
    (
        ObjectKind::Trigger("qbit_block_candidate_body"),
        "qbit_block_candidate_body_manifest_guard",
    ),
    (ObjectKind::Function, "qbit_prism_bounded_fact"),
    (ObjectKind::Function, "qbit_prism_fact_oversized"),
    (ObjectKind::Function, "qbit_prism_bounded_replay_header"),
    (ObjectKind::Column(OUTBOX), "storage_version"),
    (ObjectKind::Column(OUTBOX), "body_id"),
    (ObjectKind::Index, "qbit_block_candidate_outbox_body_idx"),
    (ObjectKind::Column(OUTBOX), "retired_body_id"),
    (ObjectKind::Column(OUTBOX), "replay_header"),
    (ObjectKind::Column(OUTBOX), "parent_hash"),
    (ObjectKind::Column(OUTBOX), "expected_height"),
    (
        ObjectKind::Function,
        "qbit_prism_candidate_publication_guard",
    ),
    (
        ObjectKind::Trigger(OUTBOX),
        "qbit_block_candidate_publication_guard",
    ),
    (
        ObjectKind::Constraint(OUTBOX),
        "qbit_block_candidate_outbox_storage_version_check",
    ),
    (
        ObjectKind::Constraint(OUTBOX),
        "qbit_block_candidate_outbox_dual_format_check",
    ),
];

/// What the catalog says about a database before any DDL.
#[derive(Debug)]
pub(super) struct SourceInventory {
    share_ledger: bool,
    outbox: bool,
    /// Parallel to `OBJECTS_002`.
    present: Vec<bool>,
    /// Rows of `qbit_prism_schema_capabilities`; `None` when the table is absent.
    capabilities: Option<Vec<(String, i32)>>,
}

impl SourceInventory {
    fn has_outbox_column(&self, column: &str) -> bool {
        OBJECTS_002
            .iter()
            .zip(&self.present)
            .any(|((kind, name), present)| {
                *present && *name == column && *kind == ObjectKind::Column(OUTBOX)
            })
    }

    fn capability(&self, name: &str) -> Option<i32> {
        self.capabilities
            .as_deref()?
            .iter()
            .find(|(capability, _)| capability == name)
            .map(|(_, value)| *value)
    }

    fn any_002(&self) -> bool {
        self.capabilities.is_some() || self.present.iter().any(|present| *present)
    }

    /// Every 002 object that is absent, named the way an operator finds it.
    fn missing(&self) -> Vec<String> {
        let mut missing: Vec<String> = OBJECTS_002
            .iter()
            .zip(&self.present)
            .filter(|(_, present)| !**present)
            .map(|((kind, name), _)| match kind {
                ObjectKind::Table => format!("table {name}"),
                ObjectKind::Index => format!("index {name}"),
                ObjectKind::Function => format!("function {name}"),
                ObjectKind::Trigger(table) => format!("trigger {name} on {table}"),
                ObjectKind::Column(table) => format!("column {table}.{name}"),
                ObjectKind::Constraint(table) => format!("constraint {name} on {table}"),
            })
            .collect();
        match self.capability("candidate_storage_version") {
            Some(2) => {}
            Some(value) => missing.push(format!(
                "row candidate_storage_version = 2 in qbit_prism_schema_capabilities (found {value})"
            )),
            None if self.capabilities.is_some() => missing.push(
                "row candidate_storage_version = 2 in qbit_prism_schema_capabilities".into(),
            ),
            None => {}
        }
        missing
    }
}

#[derive(Debug)]
pub(super) enum SourceVerdict {
    Accept(SourceState),
    /// "partial 002": the missing objects.
    Partial(Vec<String>),
    /// "newer": why the capability rows are beyond this binary.
    Newer(String),
}

async fn regclass_present(
    tx: &mut Transaction<'_, Postgres>,
    names: &[String],
) -> Result<Vec<bool>> {
    Ok(sqlx::query_scalar("SELECT to_regclass(name) IS NOT NULL FROM unnest($1::text[]) WITH ORDINALITY AS t(name,ord) ORDER BY ord")
        .bind(names).fetch_all(&mut **tx).await?)
}

async fn owned_present(
    tx: &mut Transaction<'_, Postgres>,
    catalog_predicate: &str,
    names: &[String],
    owners: &[String],
) -> Result<Vec<bool>> {
    let query = format!("SELECT EXISTS(SELECT 1 FROM {catalog_predicate}) FROM unnest($1::text[],$2::text[]) WITH ORDINALITY AS t(name,owner,ord) ORDER BY ord");
    Ok(sqlx::query_scalar(&query)
        .bind(names)
        .bind(owners)
        .fetch_all(&mut **tx)
        .await?)
}

/// Ask the catalog, not the data, which 002 objects exist. `to_regclass` and
/// `to_regproc` follow the connection's search path exactly as the DDL did.
pub(super) async fn inspect_source_schema(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<SourceInventory> {
    let mut present = vec![false; OBJECTS_002.len()];
    let mut by_kind: Vec<(&'static str, Vec<usize>)> = vec![
        ("regclass", Vec::new()),
        ("regproc", Vec::new()),
        ("trigger", Vec::new()),
        ("column", Vec::new()),
        ("constraint", Vec::new()),
    ];
    for (index, (kind, _)) in OBJECTS_002.iter().enumerate() {
        let bucket = match kind {
            ObjectKind::Table | ObjectKind::Index => 0,
            ObjectKind::Function => 1,
            ObjectKind::Trigger(_) => 2,
            ObjectKind::Column(_) => 3,
            ObjectKind::Constraint(_) => 4,
        };
        by_kind[bucket].1.push(index);
    }
    for (kind, indexes) in &by_kind {
        if indexes.is_empty() {
            continue;
        }
        let names: Vec<String> = indexes
            .iter()
            .map(|index| OBJECTS_002[*index].1.to_owned())
            .collect();
        let owners: Vec<String> = indexes
            .iter()
            .map(|index| match OBJECTS_002[*index].0 {
                ObjectKind::Trigger(owner)
                | ObjectKind::Column(owner)
                | ObjectKind::Constraint(owner) => owner.to_owned(),
                _ => String::new(),
            })
            .collect();
        let found = match *kind {
            "regclass" => regclass_present(tx, &names).await?,
            "regproc" => sqlx::query_scalar("SELECT to_regproc(name) IS NOT NULL FROM unnest($1::text[]) WITH ORDINALITY AS t(name,ord) ORDER BY ord")
                .bind(&names).fetch_all(&mut **tx).await?,
            "trigger" => owned_present(tx, "pg_trigger WHERE tgrelid=to_regclass(t.owner) AND tgname=t.name AND NOT tgisinternal", &names, &owners).await?,
            "column" => owned_present(tx, "pg_attribute WHERE attrelid=to_regclass(t.owner) AND attname=t.name AND attnum>0 AND NOT attisdropped", &names, &owners).await?,
            _ => owned_present(tx, "pg_constraint WHERE conrelid=to_regclass(t.owner) AND conname=t.name", &names, &owners).await?,
        };
        ensure!(
            found.len() == indexes.len(),
            "catalog inspection returned {} rows for {} objects",
            found.len(),
            indexes.len()
        );
        for (index, value) in indexes.iter().zip(found) {
            present[*index] = value;
        }
    }
    let base = regclass_present(
        tx,
        &[
            "qbit_share_ledger".to_owned(),
            OUTBOX.to_owned(),
            "qbit_prism_schema_capabilities".to_owned(),
        ],
    )
    .await?;
    let capabilities = if base[2] {
        Some(read_capabilities(&mut **tx).await?)
    } else {
        None
    };
    Ok(SourceInventory {
        share_ledger: base[0],
        outbox: base[1],
        present,
        capabilities,
    })
}

pub(super) fn classify_source(inventory: &SourceInventory) -> SourceVerdict {
    if let Some(rows) = &inventory.capabilities {
        if let Err(error) = refuse_unknown_capabilities(rows) {
            return SourceVerdict::Newer(error.to_string());
        }
    }
    if !inventory.any_002() {
        return SourceVerdict::Accept(if inventory.share_ledger {
            SourceState::Pre258
        } else {
            SourceState::Fresh
        });
    }
    let missing = inventory.missing();
    if missing.is_empty() {
        SourceVerdict::Accept(SourceState::Applied258)
    } else {
        SourceVerdict::Partial(missing)
    }
}

async fn read_capabilities<'e, E>(executor: E) -> Result<Vec<(String, i32)>>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{
    let rows = sqlx::query("SELECT capability,capability_value FROM qbit_prism_schema_capabilities ORDER BY capability")
        .fetch_all(executor).await?;
    rows.iter()
        .map(|row| Ok((row.try_get("capability")?, row.try_get("capability_value")?)))
        .collect()
}

/// A capability this binary does not know, or a known one beyond the value
/// it understands, means a newer PRISM release wrote the database.
pub(super) fn refuse_unknown_capabilities(rows: &[(String, i32)]) -> Result<()> {
    for (name, value) in rows {
        match KNOWN_CAPABILITIES.iter().find(|(known, _)| known == name) {
            None => bail!("database declares capability {name} = {value}, which this server does not understand: a newer PRISM release wrote this database; upgrade the server before starting it here"),
            Some((_, max)) => ensure!(
                (1..=*max).contains(value),
                "database declares {name} = {value}, but this server understands {name} 1 to {max} only: a newer PRISM release wrote this database; upgrade the server before starting it here"
            ),
        }
    }
    Ok(())
}

/// Refuse a pending 2.x.x row the native claim lane cannot replay, with the
/// predicate built from the outbox columns that exist. The capability row is
/// not consulted: 002 upserts it whatever the writer stored, so only rows say
/// whether v2 work is pending.
pub(super) async fn refuse_undrained_outbox(
    tx: &mut Transaction<'_, Postgres>,
    inventory: &SourceInventory,
) -> Result<()> {
    if !inventory.outbox {
        return Ok(());
    }
    let mut clauses = Vec::new();
    if inventory.has_outbox_column("storage_version") {
        clauses.push("storage_version <> 1");
    }
    clauses.push("candidate IS NULL");
    if inventory.has_outbox_column("body_id") {
        clauses.push("body_id IS NOT NULL");
    }
    clauses.push("NOT (candidate ?& ARRAY['payout_revision','bundle','block_hash'])");
    let predicate = clauses.join(" OR ");
    let version = if inventory.has_outbox_column("storage_version") {
        "storage_version"
    } else {
        "1"
    };
    let query = format!("SELECT block_hash,{version}::int AS storage_version,created_at::text AS created_at,attempt_count,last_error,count(*) OVER () AS total FROM qbit_block_candidate_outbox WHERE state='pending' AND ({predicate}) ORDER BY created_at,block_hash LIMIT {BLOCKING_ROWS_NAMED}");
    let rows = sqlx::query(&query).fetch_all(&mut **tx).await?;
    let Some(first) = rows.first() else {
        return Ok(());
    };
    let total: i64 = first.try_get("total")?;
    let listing = rows
        .iter()
        .map(|row| -> Result<String> {
            Ok(format!(
                "block_hash={} storage_version={} created_at={} attempts={} last_error={}",
                row.try_get::<String, _>("block_hash")?,
                row.try_get::<i32, _>("storage_version")?,
                row.try_get::<String, _>("created_at")?,
                row.try_get::<i32, _>("attempt_count")?,
                row.try_get::<Option<String>, _>("last_error")?
                    .as_deref()
                    .unwrap_or("none")
            ))
        })
        .collect::<Result<Vec<_>>>()?
        .join("; ");
    let more = match usize::try_from(total)? {
        named if named > rows.len() => format!(" and {} more", named - rows.len()),
        _ => String::new(),
    };
    bail!("legacy Python block outbox is not drained: {total} pending 2.x.x candidate row(s) cannot be replayed natively ({listing}{more}). Drain them with the pinned 2.x.x release before migrating: start the 2.x.x coordinator (v2.0.2 for storage_version 2 rows, v2.0.1 or later otherwise) and let its block submitter finish every pending candidate, or for a block already accepted on the active chain run `python3 -m lab.prism.recover_pending_blocks --block-hash <hash> --apply` from the 2.x.x image; then take the final backup and repeat the migration. Do not delete pending rows to bypass this check")
}

/// Apply the base schema and every native migration inside the caller's
/// transaction, which holds the migration lock throughout. Refusals happen
/// before any DDL, so a refused database is unchanged.
pub(super) async fn migrate_schema(
    tx: &mut Transaction<'_, Postgres>,
    instance_id: &str,
) -> Result<()> {
    lock(tx, MIGRATION_LOCK).await?;
    sqlx::raw_sql("CREATE TABLE IF NOT EXISTS qbit_prism_schema_migrations(version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT clock_timestamp())").execute(&mut **tx).await?;
    // Each applied migration is tracked on its own, not as a high-water mark:
    // 007 and 008 are reserved by independent workstreams, so a later number
    // must not hide an earlier gap. Every step runs when its own version is
    // missing, in order.
    let versions: Vec<i32> = sqlx::query_scalar("SELECT version FROM qbit_prism_schema_migrations")
        .fetch_all(&mut **tx)
        .await?;
    let prior_version = versions.iter().copied().max().unwrap_or(0);
    let mut source = None;
    if !versions.contains(&3) {
        // Existing native writers use this same lock order. Keep the
        // schema repair and cutover atomic with their accounting.
        lock(tx, SETTLEMENT_LOCK).await?;
        lock(tx, ORDER_LOCK).await?;
        let lease_exists: bool =
            sqlx::query_scalar("SELECT to_regclass('qbit_ledger_writer_lease') IS NOT NULL")
                .fetch_one(&mut **tx)
                .await?;
        if lease_exists {
            // The table lock also closes the race with a legacy process
            // trying to reacquire its lease during the cutover.
            sqlx::query("LOCK TABLE qbit_ledger_writer_lease IN ACCESS EXCLUSIVE MODE")
                .execute(&mut **tx)
                .await?;
            let live: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM qbit_ledger_writer_lease WHERE lease_expires_at > clock_timestamp())").fetch_one(&mut **tx).await?;
            ensure!(!live, "live legacy Python writer lease: stop the Python deployment and release or wait for its lease before Rust migration");
        }
        let inventory = inspect_source_schema(tx).await?;
        let state = match classify_source(&inventory) {
            SourceVerdict::Accept(state) => state,
            SourceVerdict::Newer(reason) => bail!(
                "refusing to migrate a {} source before any DDL: {reason}",
                SOURCE_STATES[4].name
            ),
            SourceVerdict::Partial(missing) => bail!(
                "refusing to migrate a {} source: 001_share_ledger.sql ran but 002_candidate_bodies.sql did not finish, missing {}. Finish it with the v2.0.2 release (PRISM_POSTGRES_INIT_SCHEMA=1 applies both files) or restore the pre-migration backup, then migrate again",
                SOURCE_STATES[3].name,
                missing.join(", ")
            ),
        };
        refuse_undrained_outbox(tx, &inventory).await?;
        let base_schema = base_schema_transaction_body(include_str!(
            "../../../qbit-prism/sql/001_share_ledger.sql"
        ))?;
        sqlx::raw_sql(&base_schema).execute(&mut **tx).await?;
        if !versions.contains(&2) {
            sqlx::raw_sql(include_str!("../../migrations/002_multi_instance.sql"))
                .execute(&mut **tx)
                .await?;
            sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(2)")
                .execute(&mut **tx)
                .await?;
        }
        sqlx::raw_sql(include_str!("../../migrations/003_2x_compatibility.sql"))
            .execute(&mut **tx)
            .await?;
        sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(3)")
            .execute(&mut **tx)
            .await?;
        source = Some((state, inventory.capability("candidate_storage_version")));
    }
    if !versions.contains(&4) {
        sqlx::raw_sql(include_str!(
            "../../migrations/004_cpfp_retired_funding.sql"
        ))
        .execute(&mut **tx)
        .await?;
        sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(4)")
            .execute(&mut **tx)
            .await?;
    }
    if !versions.contains(&5) {
        sqlx::raw_sql(include_str!("../../migrations/005_candidate_dispatch.sql"))
            .execute(&mut **tx)
            .await?;
        sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(5)")
            .execute(&mut **tx)
            .await?;
    }
    if !versions.contains(&6) {
        // A native database that predates 006 declared nothing; read what it
        // has before 006 declares version 1 for it.
        let (state, capability) = match source {
            Some((state, capability)) => (Some(state), capability),
            None => {
                let declared: bool = sqlx::query_scalar(
                    "SELECT to_regclass('qbit_prism_schema_capabilities') IS NOT NULL",
                )
                .fetch_one(&mut **tx)
                .await?;
                let capability = if declared {
                    read_capabilities(&mut **tx)
                        .await?
                        .into_iter()
                        .find(|(name, _)| name == "candidate_storage_version")
                        .map(|(_, value)| value)
                } else {
                    None
                };
                (None, capability)
            }
        };
        sqlx::raw_sql(include_str!("../../migrations/006_source_schema.sql"))
            .execute(&mut **tx)
            .await?;
        sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(6)")
            .execute(&mut **tx)
            .await?;
        let (release, commit) = state.and_then(SourceState::release).unzip();
        sqlx::query("INSERT INTO qbit_prism_migration_source(source_state,source_release,source_commit,candidate_storage_version,prior_schema_version,migrated_by) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT (singleton) DO NOTHING")
            .bind(state.map_or("native", SourceState::as_str)).bind(release).bind(commit).bind(capability).bind(prior_version).bind(instance_id)
            .execute(&mut **tx).await?;
        if let Some(state) = state {
            tracing::info!(source=state.rule().name, release=?release, "migrated PRISM database source");
        }
    }
    if !versions.contains(&9) {
        sqlx::raw_sql(include_str!("../../migrations/009_wrap_safe_sessions.sql"))
            .execute(&mut **tx)
            .await?;
        sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(9)")
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

/// The startup gate. Every start, with or without `initialize`, reads the
/// schema version and refuses anything but `REQUIRED_SCHEMA_VERSION`.
pub(super) async fn require_schema_version(pool: &PgPool) -> Result<i32> {
    let recorded: bool =
        sqlx::query_scalar("SELECT to_regclass('qbit_prism_schema_migrations') IS NOT NULL")
            .fetch_one(pool)
            .await?;
    ensure!(
        recorded,
        "database has no native PRISM schema (qbit_prism_schema_migrations is missing) and this server requires schema version {REQUIRED_SCHEMA_VERSION}: run `qbit-prism-server migrate`, or start with PRISM_POSTGRES_INIT_SCHEMA=1, after draining the 2.x.x deployment"
    );
    let version: Option<i32> =
        sqlx::query_scalar("SELECT max(version) FROM qbit_prism_schema_migrations")
            .fetch_one(pool)
            .await?;
    let version = version.unwrap_or(0);
    ensure!(
        version >= REQUIRED_SCHEMA_VERSION,
        "database schema version {version} is below the version {REQUIRED_SCHEMA_VERSION} this server requires: run `qbit-prism-server migrate` with this release, or start with PRISM_POSTGRES_INIT_SCHEMA=1"
    );
    ensure!(
        version == REQUIRED_SCHEMA_VERSION,
        "database schema version {version} is newer than the version {REQUIRED_SCHEMA_VERSION} this server supports: a newer PRISM release migrated this database; upgrade the server before starting it here"
    );
    Ok(version)
}

/// Refuse capabilities or storage versions the binary does not understand.
pub(super) async fn require_known_capabilities(pool: &PgPool) -> Result<()> {
    let declared: bool =
        sqlx::query_scalar("SELECT to_regclass('qbit_prism_schema_capabilities') IS NOT NULL")
            .fetch_one(pool)
            .await?;
    if declared {
        refuse_unknown_capabilities(&read_capabilities(pool).await?)?;
    }
    Ok(())
}

/// The standalone 2.x SQL remains atomic under plain psql. SQLx already owns
/// the encompassing transaction, which must also retain its migration/lease
/// locks through the native migrations that follow it.
pub(super) fn base_schema_transaction_body(schema: &str) -> Result<String> {
    let (comments, body) = schema
        .split_once("\nBEGIN;\n")
        .context("base schema transaction opening is missing")?;
    ensure!(
        comments
            .lines()
            .all(|line| line.trim().is_empty() || line.trim_start().starts_with("--")),
        "base schema has statements before its transaction opening"
    );
    let body = body
        .trim_end()
        .strip_suffix("\nCOMMIT;")
        .context("base schema transaction closing is missing")?;
    Ok(format!("{comments}\n{body}\n"))
}

impl Ledger {
    /// What the database came from, as recorded by the migration that
    /// accepted it; `None` before migration 006 has run.
    pub async fn migration_source(&self) -> Result<Option<MigrationSource>> {
        let row = sqlx::query("SELECT source_state,source_release,source_commit,candidate_storage_version,prior_schema_version,migrated_by,migrated_at FROM qbit_prism_migration_source WHERE singleton")
            .fetch_optional(&self.pool).await?;
        row.map(|row| {
            Ok(MigrationSource {
                source_state: row.try_get("source_state")?,
                source_release: row.try_get("source_release")?,
                source_commit: row.try_get("source_commit")?,
                candidate_storage_version: row.try_get("candidate_storage_version")?,
                prior_schema_version: row.try_get("prior_schema_version")?,
                migrated_by: row.try_get("migrated_by")?,
                migrated_at: row.try_get("migrated_at")?,
            })
        })
        .transpose()
    }

    pub async fn import_legacy_audits(
        &self,
        root_dir: Option<&Path>,
        ledger_key: &str,
    ) -> Result<usize> {
        let mut imported = 0;
        let mut cursor = String::new();
        loop {
            // Decode only one historical window at a time, even when importing
            // years of inline JSON and canonical sidecars.
            let row = sqlx::query("SELECT block_hash,body_uri,audit_bundle,audit_bundle_sha256,coinbase_tx_hex FROM qbit_pool_audit_bundles WHERE canonical_audit_bytes IS NULL AND share_snapshot_sha256 IS NULL AND block_hash>$1 ORDER BY block_hash LIMIT 1")
                .bind(&cursor).fetch_optional(&self.pool).await?;
            let Some(row) = row else { break };
            let hash: String = row.try_get("block_hash")?;
            cursor = hash.clone();
            let uri: Option<String> = row.try_get("body_uri")?;
            let inline: Option<Value> = row.try_get("audit_bundle")?;
            let expected_digest: String = row.try_get("audit_bundle_sha256")?;
            let coinbase: String = row.try_get("coinbase_tx_hex")?;
            let root = root_dir.map(Path::to_path_buf);
            let source_uri = uri.clone();
            let source_hash = hash.clone();
            let source_digest = expected_digest.clone();
            let key = ledger_key.to_owned();
            let (bundle, canonical_bytes) =
                tokio::task::spawn_blocking(move || -> Result<(AuditBundle, Vec<u8>)> {
                    let sidecar = legacy_canonical_sidecar(
                        root.as_deref(),
                        source_uri.as_deref(),
                        &source_hash,
                        &source_digest,
                    )?;
                    let (bundle, exact) = if let Some(path) = sidecar {
                        let mut exact = Vec::new();
                        flate2::read::GzDecoder::new(std::fs::File::open(&path)?)
                            .read_to_end(&mut exact)
                            .with_context(|| {
                                format!(
                                    "canonical audit sidecar cannot decompress: {}",
                                    path.display()
                                )
                            })?;
                        ensure!(
                            hex::encode(Sha256::digest(&exact)) == source_digest,
                            "canonical audit sidecar digest mismatch: {}",
                            path.display()
                        );
                        let bundle = qbit_prism::parse_audit_bundle_value(
                            serde_json::from_slice(&exact)?,
                            None,
                        )?;
                        (bundle, Some(exact))
                    } else if let Some(inline) = inline {
                        (
                            qbit_prism::parse_audit_bundle_value(inline, root.as_deref())?,
                            None,
                        )
                    } else {
                        let uri = source_uri
                            .as_deref()
                            .context("legacy audit has neither body nor canonical sidecar")?;
                        let path = resolve_import_path(root.as_deref(), uri)?;
                        (qbit_prism::load_audit_bundle_from_path(&path)?, None)
                    };
                    let report = qbit_prism::verify_audit_bundle_against_coinbase_tx_hex(
                        &bundle, &coinbase, &key,
                    )?;
                    ensure!(
                        report.audit_bundle_sha256_hex == source_digest,
                        "legacy audit digest mismatch for {source_hash}"
                    );
                    let canonical_bytes = match exact {
                        Some(bytes) => bytes,
                        None => qbit_prism::canonical_audit_bundle_bytes(&bundle)?,
                    };
                    ensure!(
                        hex::encode(Sha256::digest(&canonical_bytes)) == source_digest,
                        "legacy canonical audit bytes mismatch"
                    );
                    Ok((bundle, canonical_bytes))
                })
                .await??;
            let value = serde_json::to_value(&bundle)?;
            let mut tx = self.pool.begin().await?;
            lock(&mut tx, SETTLEMENT_LOCK).await?;
            writable(&mut tx).await?;
            // Retain legacy inline shape on import, including valid historical
            // snapshots whose ledger history was archived before Rust cutover.
            // Newly mined bodies use the normalized range-backed representation.
            let updated = sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=$2,schema_version=$3,found_block_network_difficulty=$4::text::numeric,found_block_coinbase_value_sats=$5,audit_commitment_leaves_hex=$6,witness_merkle_leaves_hex=$7,canonical_audit_bytes=$10 WHERE block_hash=$1 AND canonical_audit_bytes IS NULL AND body_uri IS NOT DISTINCT FROM $8 AND audit_bundle_sha256=$9")
                .bind(&hash).bind(value).bind(&bundle.schema).bind(bundle.found_block.network_difficulty.to_string()).bind(i64::try_from(bundle.found_block.coinbase_value_sats)?)
                .bind(serde_json::to_value(&bundle.audit_commitment_leaves_hex)?).bind(serde_json::to_value(&bundle.witness_merkle_leaves_hex)?).bind(&uri).bind(expected_digest).bind(canonical_bytes).execute(&mut *tx).await?.rows_affected();
            tx.commit().await?;
            imported += usize::try_from(updated)?;
        }
        Ok(imported)
    }

    /// Recover missing sets or individual fanouts from trusted audit evidence.
    /// Return the number of blocks repaired; matching existing rows are no-ops.
    pub async fn backfill_ctv(&self, ledger_key: &str) -> Result<usize> {
        let rows = sqlx::query("SELECT block_hash,audit_bundle_sha256,coinbase_tx_hex FROM qbit_pool_audit_bundles ORDER BY created_at,block_hash").fetch_all(&self.pool).await?;
        let mut repaired = 0;
        for row in rows {
            let hash: String = row.try_get("block_hash")?;
            let value = self
                .audit_bundle(&hash)
                .await?
                .with_context(|| format!("audit {hash} is external; import legacy audits first"))?;
            let coinbase: String = row.try_get("coinbase_tx_hex")?;
            let expected: String = row.try_get("audit_bundle_sha256")?;
            let key = ledger_key.to_owned();
            let bundle = tokio::task::spawn_blocking(move || -> Result<AuditBundle> {
                let bundle = qbit_prism::parse_audit_bundle_value(value, None)?;
                let report = qbit_prism::verify_audit_bundle_against_coinbase_tx_hex(
                    &bundle, &coinbase, &key,
                )?;
                ensure!(
                    report.audit_bundle_sha256_hex == expected,
                    "stored audit digest mismatch"
                );
                Ok(bundle)
            })
            .await??;
            let Some(set) = bundle.ctv_fanout_manifest_set else {
                continue;
            };
            let mut tx = self.pool.begin().await?;
            lock(&mut tx, SETTLEMENT_LOCK).await?;
            writable(&mut tx).await?;
            let before: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM qbit_ctv_fanout_artifacts WHERE block_hash=$1",
            )
            .bind(&hash)
            .fetch_one(&mut *tx)
            .await?;
            blocks::persist_fanouts(&mut tx, &hash, &set).await?;
            // Recovered artifacts inherit canonical parent state immediately.
            sqlx::query("UPDATE qbit_ctv_fanout_artifacts a SET settlement_status=CASE WHEN b.chain_state IN ('inactive','reversed','rejected') THEN 'reorged' WHEN b.chain_state='confirmed' AND b.maturity_state='mature' THEN 'broadcastable' ELSE 'awaiting_maturity' END,updated_at=clock_timestamp() FROM qbit_pool_blocks b WHERE a.block_hash=b.block_hash AND a.block_hash=$1 AND a.settlement_status='awaiting_maturity'").bind(&hash).execute(&mut *tx).await?;
            let after: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM qbit_ctv_fanout_artifacts WHERE block_hash=$1",
            )
            .bind(&hash)
            .fetch_one(&mut *tx)
            .await?;
            tx.commit().await?;
            if after > before {
                repaired += 1;
            }
        }
        Ok(repaired)
    }
}

fn legacy_canonical_sidecar(
    root: Option<&Path>,
    uri: Option<&str>,
    hash: &str,
    digest: &str,
) -> Result<Option<PathBuf>> {
    ensure!(
        hash.len() == 64
            && digest.len() == 64
            && hash
                .bytes()
                .chain(digest.bytes())
                .all(|byte| byte.is_ascii_hexdigit()),
        "invalid legacy audit hash identity"
    );
    let directory = root.map(Path::to_path_buf).or_else(|| {
        uri.and_then(|uri| {
            Path::new(uri.strip_prefix("file://").unwrap_or(uri))
                .parent()
                .map(Path::to_path_buf)
        })
    });
    let Some(directory) = directory else {
        return Ok(None);
    };
    let directory = if directory.is_absolute() {
        directory
    } else {
        std::env::current_dir()?.join(directory)
    };
    let path = directory.join(format!(
        "prism-audit-bundle-canonical-{hash}-{digest}.json.gz"
    ));
    match path.symlink_metadata() {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    // Present-but-corrupt/unreadable canonical files never silently fall back
    // to a logical reconstruction with different bytes.
    Ok(Some(resolve_import_path(
        root,
        path.to_str().context("legacy audit path is not UTF-8")?,
    )?))
}

fn resolve_import_path(root_dir: Option<&Path>, uri: &str) -> Result<PathBuf> {
    let path = if let Some(uri) = uri.strip_prefix("file://") {
        PathBuf::from(uri)
    } else {
        PathBuf::from(uri)
    };
    let path = if path.is_relative() {
        if let Some(root) = root_dir {
            root.join(path)
        } else {
            path
        }
    } else {
        path
    };
    let canonical = path
        .canonicalize()
        .with_context(|| format!("cannot read legacy audit body {}", path.display()))?;
    if let Some(root) = root_dir {
        let root = root.canonicalize()?;
        ensure!(
            canonical.starts_with(&root),
            "legacy audit body escapes configured artifact root"
        );
    }
    Ok(canonical)
}
