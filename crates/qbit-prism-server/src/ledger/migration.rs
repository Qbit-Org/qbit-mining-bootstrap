//! Schema migration from a pinned 2.x.x source, the schema gates every
//! start passes, and the explicit one-time migration of Python filesystem
//! artifacts. Validation is performed before any write; operator files and
//! historical rows are retained.
use super::*;
use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};

/// The schema version every native start requires. Bump it with each new
/// migration file. `Ledger::connect` refuses an older schema even without
/// `initialize`, so a newer binary never reaches the claim path on a database
/// it has not migrated. A newer schema is accepted with a warning: native
/// migrations are additive, and a release whose format an older binary must
/// not touch declares a capability, which `require_known_capabilities`
/// refuses.
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

/// The names of the source states. Code cites a state by its name and finds
/// its row with `source_rule`, never by position, so the table can be
/// reordered or grown without a refusal silently citing the wrong row.
const STATE_FRESH: &str = "fresh";
const STATE_PARTIAL_001: &str = "partial 001";
const STATE_PRE_258: &str = "pre-#258";
const STATE_APPLIED_258: &str = "#258 applied";
const STATE_PARTIAL_002: &str = "partial 002";
const STATE_NEWER: &str = "newer";
const STATE_DRIFTED_001: &str = "drifted 001";

/// The source states migration 006 accepts or refuses, as data. Detection is
/// column-aware: it asks the catalog which 002 objects exist, so a fixed
/// predicate never errors on a source that lacks a column, and it looks at
/// outbox rows for the drain check because the capability row proves only
/// that 002 ran. The release definitions come from applying the frozen
/// release SQL to a scratch schema under a savepoint, before any DDL touches
/// the source. A database without `qbit_share_ledger` is fresh only if it
/// has nothing else that 001 creates; otherwise it is a partial 001,
/// refused before any DDL. The last row is decided after 001 has run: the
/// release 001 is idempotent and repairs what it re-asserts, so the check
/// compares what its `IF NOT EXISTS` left alone against the same scratch
/// apply, and a refusal rolls the whole migration back.
pub const SOURCE_STATES: [SourceStateRule; 7] = [
    SourceStateRule {
        name: STATE_FRESH,
        evidence: "no 001 or 002 object at all",
        verdict: "accept",
    },
    SourceStateRule {
        name: STATE_PARTIAL_001,
        evidence: "no qbit_share_ledger, but some 001 object present",
        verdict: "refuse before any DDL, naming the objects present",
    },
    SourceStateRule {
        name: STATE_PRE_258,
        evidence: "no qbit_prism_schema_capabilities, no 002 object",
        verdict: "accept after the drain check",
    },
    SourceStateRule {
        name: STATE_APPLIED_258,
        evidence: "candidate_storage_version = 2 and every 002 object present",
        verdict: "accept after the drain check",
    },
    SourceStateRule {
        name: STATE_PARTIAL_002,
        evidence: "some 002 objects or the capability row, not all",
        verdict: "refuse, naming the missing object",
    },
    SourceStateRule {
        name: STATE_NEWER,
        evidence: "candidate_storage_version > 2 or an unknown capability",
        verdict: "refuse before any DDL",
    },
    SourceStateRule {
        name: STATE_DRIFTED_001,
        evidence: "a 001 (or 002) object whose definition, after 001 has run, differs from the frozen release",
        verdict: "refuse transactionally, naming the object",
    },
];

/// The row of `SOURCE_STATES` with this name. Every name the code cites is
/// one of the `STATE_*` constants, each of which names a row above; the
/// unit tests check every one, so this cannot fail at run time.
fn source_rule(name: &str) -> &'static SourceStateRule {
    SOURCE_STATES
        .iter()
        .find(|rule| rule.name == name)
        .unwrap_or_else(|| panic!("{name} is not a row of SOURCE_STATES"))
}

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
        source_rule(match self {
            Self::Fresh => STATE_FRESH,
            Self::Pre258 => STATE_PRE_258,
            Self::Applied258 => STATE_APPLIED_258,
        })
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
/// whether v2 work is pending. `native_version` is the recorded schema
/// version when the database was migrated to native schema 3, 4 or 5 by an
/// earlier 3.x.x build, whose drain check never counted a v2 row; that path
/// gets its own wording and remedy. Native pending rows carry the native
/// `payout_revision`, `bundle` and `block_hash` fields, so the predicate
/// never flags them; only a v2 body, a `body_id`, a `storage_version` other
/// than 1, or a v1 body without those fields is refused.
pub(super) async fn refuse_undrained_outbox(
    tx: &mut Transaction<'_, Postgres>,
    inventory: &SourceInventory,
    native_version: Option<i32>,
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
    match native_version {
        None => bail!("legacy Python block outbox is not drained: {total} pending 2.x.x candidate row(s) cannot be replayed natively ({listing}{more}). Drain them with the pinned 2.x.x release before migrating: start the 2.x.x coordinator (v2.0.2 for storage_version 2 rows, v2.0.1 or later otherwise) and let its block submitter finish every pending candidate, or for a block already accepted on the active chain run `python3 -m lab.prism.recover_pending_blocks --block-hash <hash> --apply` from the 2.x.x image; then take the final backup and repeat the migration. Do not delete pending rows to bypass this check"),
        Some(version) => bail!("refusing to apply migration 006 to a native schema {version} database: an earlier 3.x.x build migrated it before the drain rule covered these rows, and the legacy Python block outbox is not drained: {total} pending 2.x.x candidate row(s) cannot be replayed natively ({listing}{more}). Nothing was changed. Restore the pre-migration 2.x.x backup and drain them with the pinned 2.x.x release (v2.0.2 for storage_version 2 rows, v2.0.1 or later otherwise): start its coordinator and let the block submitter finish every pending candidate, or for a block already accepted on the active chain run `python3 -m lab.prism.recover_pending_blocks --block-hash <hash> --apply` from the 2.x.x image; then take a new backup and migrate again with this release. The 2.x.x release is not supported against a native schema, so do not point it at this database. If native traffic was admitted after the earlier migration, that restore discards it: see the recovery section of docs/prism-rust-migration.md first. Do not delete pending rows to bypass this check"),
    }
}

/// How many drifted or extra objects a release-schema report names.
const DRIFT_OBJECTS_NAMED: usize = 16;

/// What both sides of the release-schema comparison call the schema an
/// object lives in, so the scratch apply and the source compare equal
/// whatever their schemas are named.
const SCHEMA_PLACEHOLDER: &str = "<schema>";

#[derive(Clone, Debug, PartialEq, Eq)]
struct ColumnDefinition {
    data_type: String,
    not_null: bool,
    default: Option<String>,
    identity: String,
    generated: String,
    collation: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct IndexDefinition {
    table: String,
    definition: String,
    valid: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TriggerDefinition {
    definition: String,
    enabled: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FunctionDefinition {
    arguments: String,
    result: Option<String>,
    language: String,
    body: Option<String>,
    volatility: String,
    strict: bool,
    security_definer: bool,
    leakproof: bool,
    parallel: String,
    kind: String,
    config: Vec<String>,
}

/// A table's persistence and its columns. `pg_class.relpersistence` is `p`
/// for an ordinary logged table, `u` for UNLOGGED and `t` for temporary. The
/// release creates logged tables only: an unlogged ledger or outbox is one
/// whose rows PostgreSQL truncates after a crash, so persistence is part of
/// the definition, not a tuning knob.
#[derive(Clone, Debug, PartialEq, Eq)]
struct TableDefinition {
    persistence: String,
    columns: BTreeMap<String, ColumnDefinition>,
}

/// The structure of a sequence, from `pg_sequence`: what a `serial` column
/// or `CREATE SEQUENCE` fixed, and its persistence (PostgreSQL 15 and later
/// have unlogged sequences, and a sequence owned by an unlogged table is
/// unlogged with it). Its current value (`last_value`, `is_called`) is data
/// the writers advance and is not read.
#[derive(Clone, Debug, PartialEq, Eq)]
struct SequenceDefinition {
    persistence: String,
    data_type: String,
    start: i64,
    increment: i64,
    min: i64,
    max: i64,
    cache: i64,
    cycle: bool,
}

/// Every table, column, constraint, index, trigger, function and sequence of
/// one schema, as the server renders them, without schema qualification.
/// Columns are keyed by name, so their physical order is irrelevant;
/// constraints are keyed per table by definition, so an auto-generated name
/// is irrelevant; comments are not read.
#[derive(Debug, Default, PartialEq, Eq)]
struct SchemaFingerprint {
    /// Keyed by name: each table's persistence and its columns.
    tables: BTreeMap<String, TableDefinition>,
    /// Table, then constraint definition, to the name the constraint carries.
    constraints: BTreeMap<String, BTreeMap<String, String>>,
    /// Indexes that do not back a constraint, by name; the constraint
    /// comparison covers the others under whatever name they were given.
    indexes: BTreeMap<String, IndexDefinition>,
    /// Keyed by table, then trigger name.
    triggers: BTreeMap<(String, String), TriggerDefinition>,
    /// Keyed by name, then identity arguments.
    functions: BTreeMap<(String, String), FunctionDefinition>,
    /// Keyed by name: the sequences behind `serial` columns and the ones
    /// created explicitly alike.
    sequences: BTreeMap<String, SequenceDefinition>,
}

/// What the source has that the release does not create (`extra`, kept and
/// logged) and what it lacks or defines differently (`drift`, refused).
#[derive(Debug, Default, PartialEq, Eq)]
struct SchemaComparison {
    drift: Vec<String>,
    extra: Vec<String>,
}

/// Remove `schema.` and `"schema".` where they qualify a name. Only a
/// qualifier is removed: the schema name followed by a dot and not preceded
/// by an identifier character, so an identifier that merely contains the
/// schema name is left alone.
fn strip_schema_qualification(text: &str, schema: &str) -> String {
    let quoted = format!("\"{}\"", schema.replace('"', "\"\""));
    let mut stripped = String::with_capacity(text.len());
    let mut rest = text;
    while !rest.is_empty() {
        let after_identifier = stripped
            .chars()
            .next_back()
            .is_some_and(|last| last.is_alphanumeric() || last == '_' || last == '$');
        let qualifier = [quoted.as_str(), schema].into_iter().find(|name| {
            !after_identifier
                && rest.len() > name.len()
                && rest.starts_with(name)
                && rest[name.len()..].starts_with('.')
        });
        match qualifier {
            Some(name) => rest = &rest[name.len() + 1..],
            None => {
                let character = rest.chars().next().unwrap_or_default();
                stripped.push(character);
                rest = &rest[character.len_utf8()..];
            }
        }
    }
    stripped
}

/// A function's `SET search_path` names its installation schema (001 pins
/// two functions that way); replace that name with the placeholder.
fn normalize_function_config(item: &str, schema: &str) -> String {
    let Some((key, value)) = item.split_once('=') else {
        return item.to_owned();
    };
    if key != "search_path" {
        return item.to_owned();
    }
    let quoted = format!("\"{}\"", schema.replace('"', "\"\""));
    let value = value
        .split(',')
        .map(str::trim)
        .map(|token| {
            if token == schema || token == quoted {
                SCHEMA_PLACEHOLDER
            } else {
                token
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("{key}={value}")
}

/// A CHECK the release added to an upgraded table with `NOT VALID` is the
/// same rule for every row the native writers produce; 001 itself compares
/// constraint definitions this way.
fn strip_not_valid(definition: &str) -> String {
    definition
        .strip_suffix(" NOT VALID")
        .unwrap_or(definition)
        .to_owned()
}

/// Read the catalog for every object in `namespace`. The `pg_get_*`
/// renderers qualify a name only when it is not visible on the search path,
/// except that an index or trigger always qualifies its table, so this runs
/// while `namespace` is the current schema and strips its name from what is
/// rendered.
async fn fingerprint_schema(
    tx: &mut Transaction<'_, Postgres>,
    namespace: &str,
) -> Result<SchemaFingerprint> {
    let mut fingerprint = SchemaFingerprint::default();
    let tables: Vec<(String, String)> = sqlx::query_as("SELECT c.relname::text,c.relpersistence::text FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relkind='r' ORDER BY 1")
        .bind(namespace).fetch_all(&mut **tx).await?;
    for (table, persistence) in tables {
        fingerprint.tables.insert(
            table,
            TableDefinition {
                persistence,
                columns: BTreeMap::new(),
            },
        );
    }
    let rows = sqlx::query("SELECT c.relname::text AS table_name,a.attname::text AS column_name,format_type(a.atttypid,a.atttypmod) AS data_type,a.attnotnull AS not_null,pg_get_expr(d.adbin,d.adrelid) AS default_expr,a.attidentity::text AS identity,a.attgenerated::text AS generated,CASE WHEN a.attcollation<>t.typcollation THEN col.collname::text END AS collation FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped JOIN pg_type t ON t.oid=a.atttypid LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum LEFT JOIN pg_collation col ON col.oid=a.attcollation WHERE n.nspname=$1 AND c.relkind='r' ORDER BY 1,2")
        .bind(namespace).fetch_all(&mut **tx).await?;
    for row in &rows {
        let table: String = row.try_get("table_name")?;
        let default: Option<String> = row.try_get("default_expr")?;
        let columns = &mut fingerprint
            .tables
            .get_mut(&table)
            .with_context(|| format!("column of {table} read without its table"))?
            .columns;
        columns.insert(
            row.try_get("column_name")?,
            ColumnDefinition {
                data_type: row.try_get("data_type")?,
                not_null: row.try_get("not_null")?,
                default: default.map(|expr| strip_schema_qualification(&expr, namespace)),
                identity: row.try_get("identity")?,
                generated: row.try_get("generated")?,
                collation: row.try_get("collation")?,
            },
        );
    }
    let rows = sqlx::query("SELECT c.relname::text AS table_name,k.conname::text AS name,pg_get_constraintdef(k.oid) AS definition FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relkind='r' ORDER BY 1,2")
        .bind(namespace).fetch_all(&mut **tx).await?;
    for row in &rows {
        let table: String = row.try_get("table_name")?;
        let definition: String = row.try_get("definition")?;
        fingerprint
            .constraints
            .entry(table)
            .or_default()
            .entry(strip_not_valid(&strip_schema_qualification(
                &definition,
                namespace,
            )))
            .or_insert(row.try_get("name")?);
    }
    let rows = sqlx::query("SELECT c.relname::text AS table_name,i.relname::text AS name,pg_get_indexdef(x.indexrelid) AS definition,x.indisvalid AS valid FROM pg_index x JOIN pg_class i ON i.oid=x.indexrelid JOIN pg_class c ON c.oid=x.indrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relkind='r' AND NOT EXISTS(SELECT 1 FROM pg_constraint k WHERE k.conindid=x.indexrelid AND k.contype IN ('p','u','x')) ORDER BY 1,2")
        .bind(namespace).fetch_all(&mut **tx).await?;
    for row in &rows {
        let definition: String = row.try_get("definition")?;
        fingerprint.indexes.insert(
            row.try_get("name")?,
            IndexDefinition {
                table: row.try_get("table_name")?,
                definition: strip_schema_qualification(&definition, namespace),
                valid: row.try_get("valid")?,
            },
        );
    }
    let rows = sqlx::query("SELECT c.relname::text AS table_name,t.tgname::text AS name,pg_get_triggerdef(t.oid) AS definition,t.tgenabled::text AS enabled FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND NOT t.tgisinternal ORDER BY 1,2")
        .bind(namespace).fetch_all(&mut **tx).await?;
    for row in &rows {
        let definition: String = row.try_get("definition")?;
        fingerprint.triggers.insert(
            (row.try_get("table_name")?, row.try_get("name")?),
            TriggerDefinition {
                definition: strip_schema_qualification(&definition, namespace),
                enabled: row.try_get("enabled")?,
            },
        );
    }
    let rows = sqlx::query("SELECT p.proname::text AS name,pg_get_function_identity_arguments(p.oid) AS identity,pg_get_function_arguments(p.oid) AS arguments,pg_get_function_result(p.oid) AS result,l.lanname::text AS language,p.prosrc AS body,p.provolatile::text AS volatility,p.proisstrict AS strict,p.prosecdef AS security_definer,p.proleakproof AS leakproof,p.proparallel::text AS parallel,p.prokind::text AS kind,coalesce(p.proconfig,'{}') AS config FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=$1 ORDER BY 1,2")
        .bind(namespace).fetch_all(&mut **tx).await?;
    for row in &rows {
        let config: Vec<String> = row.try_get("config")?;
        let result: Option<String> = row.try_get("result")?;
        fingerprint.functions.insert(
            (row.try_get("name")?, row.try_get("identity")?),
            FunctionDefinition {
                arguments: row.try_get("arguments")?,
                result: result.map(|result| strip_schema_qualification(&result, namespace)),
                language: row.try_get("language")?,
                body: row.try_get("body")?,
                volatility: row.try_get("volatility")?,
                strict: row.try_get("strict")?,
                security_definer: row.try_get("security_definer")?,
                leakproof: row.try_get("leakproof")?,
                parallel: row.try_get("parallel")?,
                kind: row.try_get("kind")?,
                config: config
                    .iter()
                    .map(|item| normalize_function_config(item, namespace))
                    .collect(),
            },
        );
    }
    let rows = sqlx::query("SELECT c.relname::text AS name,c.relpersistence::text AS persistence,format_type(s.seqtypid,NULL) AS data_type,s.seqstart AS start,s.seqincrement AS increment,s.seqmin AS min,s.seqmax AS max,s.seqcache AS cache,s.seqcycle AS cycle FROM pg_sequence s JOIN pg_class c ON c.oid=s.seqrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relkind='S' ORDER BY 1")
        .bind(namespace).fetch_all(&mut **tx).await?;
    for row in &rows {
        fingerprint.sequences.insert(
            row.try_get("name")?,
            SequenceDefinition {
                persistence: row.try_get("persistence")?,
                data_type: row.try_get("data_type")?,
                start: row.try_get("start")?,
                increment: row.try_get("increment")?,
                min: row.try_get("min")?,
                max: row.try_get("max")?,
                cache: row.try_get("cache")?,
                cycle: row.try_get("cycle")?,
            },
        );
    }
    Ok(fingerprint)
}

fn column_differences(expected: &ColumnDefinition, found: &ColumnDefinition) -> String {
    let nullability = |not_null: bool| if not_null { "NOT NULL" } else { "nullable" };
    let identity = |identity: &str| match identity {
        "a" => "always",
        "d" => "by default",
        _ => "none",
    };
    let generated = |generated: &str| match generated {
        "s" => "stored",
        _ => "none",
    };
    let optional = |value: &Option<String>| value.clone().unwrap_or_else(|| "none".to_owned());
    let mut parts = Vec::new();
    if expected.data_type != found.data_type {
        parts.push(format!(
            "type expected {}, found {}",
            expected.data_type, found.data_type
        ));
    }
    if expected.not_null != found.not_null {
        parts.push(format!(
            "expected {}, found {}",
            nullability(expected.not_null),
            nullability(found.not_null)
        ));
    }
    if expected.default != found.default {
        parts.push(format!(
            "default expected {}, found {}",
            optional(&expected.default),
            optional(&found.default)
        ));
    }
    if expected.identity != found.identity {
        parts.push(format!(
            "identity expected {}, found {}",
            identity(&expected.identity),
            identity(&found.identity)
        ));
    }
    if expected.generated != found.generated {
        parts.push(format!(
            "generated expected {}, found {}",
            generated(&expected.generated),
            generated(&found.generated)
        ));
    }
    if expected.collation != found.collation {
        parts.push(format!(
            "collation expected {}, found {}",
            expected.collation.as_deref().unwrap_or("default"),
            found.collation.as_deref().unwrap_or("default")
        ));
    }
    parts.join(", ")
}

fn trigger_state(enabled: &str) -> &str {
    match enabled {
        "O" => "enabled",
        "D" => "disabled",
        "R" => "enabled on replicas only",
        "A" => "always enabled",
        other => other,
    }
}

fn function_differences(expected: &FunctionDefinition, found: &FunctionDefinition) -> String {
    let volatility = |volatility: &str| match volatility {
        "i" => "IMMUTABLE",
        "s" => "STABLE",
        _ => "VOLATILE",
    };
    let parallel = |parallel: &str| match parallel {
        "s" => "PARALLEL SAFE",
        "r" => "PARALLEL RESTRICTED",
        _ => "PARALLEL UNSAFE",
    };
    let kind = |kind: &str| match kind {
        "p" => "procedure",
        "a" => "aggregate",
        "w" => "window function",
        _ => "function",
    };
    let optional = |value: &Option<String>| value.clone().unwrap_or_else(|| "none".to_owned());
    let mut parts = Vec::new();
    if expected.body != found.body {
        parts.push("body".to_owned());
    }
    if expected.arguments != found.arguments {
        parts.push(format!(
            "arguments expected ({}), found ({})",
            expected.arguments, found.arguments
        ));
    }
    if expected.result != found.result {
        parts.push(format!(
            "result expected {}, found {}",
            optional(&expected.result),
            optional(&found.result)
        ));
    }
    if expected.language != found.language {
        parts.push(format!(
            "language expected {}, found {}",
            expected.language, found.language
        ));
    }
    if expected.volatility != found.volatility {
        parts.push(format!(
            "expected {}, found {}",
            volatility(&expected.volatility),
            volatility(&found.volatility)
        ));
    }
    if expected.strict != found.strict {
        let strictness = |strict: bool| {
            if strict {
                "STRICT"
            } else {
                "CALLED ON NULL INPUT"
            }
        };
        parts.push(format!(
            "expected {}, found {}",
            strictness(expected.strict),
            strictness(found.strict)
        ));
    }
    if expected.security_definer != found.security_definer {
        let security = |definer: bool| {
            if definer {
                "SECURITY DEFINER"
            } else {
                "SECURITY INVOKER"
            }
        };
        parts.push(format!(
            "expected {}, found {}",
            security(expected.security_definer),
            security(found.security_definer)
        ));
    }
    if expected.leakproof != found.leakproof {
        let leakproof = |leakproof: bool| {
            if leakproof {
                "LEAKPROOF"
            } else {
                "NOT LEAKPROOF"
            }
        };
        parts.push(format!(
            "expected {}, found {}",
            leakproof(expected.leakproof),
            leakproof(found.leakproof)
        ));
    }
    if expected.parallel != found.parallel {
        parts.push(format!(
            "expected {}, found {}",
            parallel(&expected.parallel),
            parallel(&found.parallel)
        ));
    }
    if expected.kind != found.kind {
        parts.push(format!(
            "expected a {}, found a {}",
            kind(&expected.kind),
            kind(&found.kind)
        ));
    }
    if expected.config != found.config {
        parts.push(format!(
            "configuration expected [{}], found [{}]",
            expected.config.join(", "),
            found.config.join(", ")
        ));
    }
    parts.join(", ")
}

/// `relpersistence` the way `CREATE TABLE` spells it.
fn persistence(code: &str) -> &str {
    match code {
        "p" => "logged",
        "u" => "UNLOGGED",
        "t" => "TEMPORARY",
        other => other,
    }
}

fn sequence_differences(expected: &SequenceDefinition, found: &SequenceDefinition) -> String {
    let mut parts = Vec::new();
    if expected.persistence != found.persistence {
        parts.push(format!(
            "expected {}, found {}",
            persistence(&expected.persistence),
            persistence(&found.persistence)
        ));
    }
    if expected.data_type != found.data_type {
        parts.push(format!(
            "type expected {}, found {}",
            expected.data_type, found.data_type
        ));
    }
    for (property, expected, found) in [
        ("start", expected.start, found.start),
        ("increment", expected.increment, found.increment),
        ("minimum", expected.min, found.min),
        ("maximum", expected.max, found.max),
        ("cache", expected.cache, found.cache),
    ] {
        if expected != found {
            parts.push(format!("{property} expected {expected}, found {found}"));
        }
    }
    if expected.cycle != found.cycle {
        let cycle = |cycle: bool| if cycle { "CYCLE" } else { "NO CYCLE" };
        parts.push(format!(
            "expected {}, found {}",
            cycle(expected.cycle),
            cycle(found.cycle)
        ));
    }
    parts.join(", ")
}

/// Every object the release creates must have an equivalent in the source;
/// anything else in the source is extra. An object on a table the source
/// lacks is not reported twice. A table or sequence must also have the
/// release's persistence: UNLOGGED is drift, whatever its columns say. A
/// sequence is compared by its structure only: the value it has reached is
/// the source's data.
fn compare_fingerprints(
    expected: &SchemaFingerprint,
    found: &SchemaFingerprint,
) -> SchemaComparison {
    let mut comparison = SchemaComparison::default();
    for (table, definition) in &expected.tables {
        let Some(found_table) = found.tables.get(table) else {
            comparison.drift.push(format!("missing table {table}"));
            continue;
        };
        if found_table.persistence != definition.persistence {
            comparison.drift.push(format!(
                "table {table} differs: expected {}, found {}",
                persistence(&definition.persistence),
                persistence(&found_table.persistence)
            ));
        }
        let columns = &definition.columns;
        let found_columns = &found_table.columns;
        for (column, definition) in columns {
            match found_columns.get(column) {
                None => comparison
                    .drift
                    .push(format!("missing column {table}.{column}")),
                Some(actual) if actual != definition => comparison.drift.push(format!(
                    "column {table}.{column} differs: {}",
                    column_differences(definition, actual)
                )),
                Some(_) => {}
            }
        }
        for column in found_columns.keys() {
            if !columns.contains_key(column) {
                comparison.extra.push(format!("column {table}.{column}"));
            }
        }
    }
    for table in found.tables.keys() {
        if !expected.tables.contains_key(table) {
            comparison.extra.push(format!("table {table}"));
        }
    }
    for (name, sequence) in &expected.sequences {
        match found.sequences.get(name) {
            None => comparison.drift.push(format!("missing sequence {name}")),
            Some(actual) if actual != sequence => comparison.drift.push(format!(
                "sequence {name} differs: {}",
                sequence_differences(sequence, actual)
            )),
            Some(_) => {}
        }
    }
    for name in found.sequences.keys() {
        if !expected.sequences.contains_key(name) {
            comparison.extra.push(format!("sequence {name}"));
        }
    }
    let empty = BTreeMap::new();
    for (table, constraints) in &expected.constraints {
        if !found.tables.contains_key(table) {
            continue;
        }
        let found_constraints = found.constraints.get(table).unwrap_or(&empty);
        for (definition, name) in constraints {
            if !found_constraints.contains_key(definition) {
                comparison.drift.push(format!(
                    "missing constraint {name} on {table}: {definition}"
                ));
            }
        }
    }
    for (table, constraints) in &found.constraints {
        if !expected.tables.contains_key(table) {
            continue;
        }
        let expected_constraints = expected.constraints.get(table).unwrap_or(&empty);
        for (definition, name) in constraints {
            if !expected_constraints.contains_key(definition) {
                comparison
                    .extra
                    .push(format!("constraint {name} on {table}: {definition}"));
            }
        }
    }
    for (name, index) in &expected.indexes {
        if !found.tables.contains_key(&index.table) {
            continue;
        }
        match found.indexes.get(name) {
            None => comparison
                .drift
                .push(format!("missing index {name} on {}", index.table)),
            Some(actual) if actual.definition != index.definition => {
                comparison.drift.push(format!(
                    "index {name} differs: expected {}, found {}",
                    index.definition, actual.definition
                ))
            }
            Some(actual) if !actual.valid => comparison
                .drift
                .push(format!("index {name} on {} is not valid", index.table)),
            Some(_) => {}
        }
    }
    for (name, index) in &found.indexes {
        if expected.tables.contains_key(&index.table) && !expected.indexes.contains_key(name) {
            comparison
                .extra
                .push(format!("index {name} on {}", index.table));
        }
    }
    for ((table, name), trigger) in &expected.triggers {
        if !found.tables.contains_key(table) {
            continue;
        }
        match found.triggers.get(&(table.clone(), name.clone())) {
            None => comparison
                .drift
                .push(format!("missing trigger {name} on {table}")),
            Some(actual) if actual != trigger => {
                let mut parts = Vec::new();
                if actual.definition != trigger.definition {
                    parts.push(format!(
                        "expected {}, found {}",
                        trigger.definition, actual.definition
                    ));
                }
                if actual.enabled != trigger.enabled {
                    parts.push(format!(
                        "expected {}, found {}",
                        trigger_state(&trigger.enabled),
                        trigger_state(&actual.enabled)
                    ));
                }
                comparison.drift.push(format!(
                    "trigger {name} on {table} differs: {}",
                    parts.join(", ")
                ));
            }
            Some(_) => {}
        }
    }
    for (table, name) in found.triggers.keys() {
        if expected.tables.contains_key(table)
            && !expected
                .triggers
                .contains_key(&(table.clone(), name.clone()))
        {
            comparison.extra.push(format!("trigger {name} on {table}"));
        }
    }
    for ((name, identity), function) in &expected.functions {
        match found.functions.get(&(name.clone(), identity.clone())) {
            None => comparison
                .drift
                .push(format!("missing function {name}({identity})")),
            Some(actual) if actual != function => comparison.drift.push(format!(
                "function {name}({identity}) differs: {}",
                function_differences(function, actual)
            )),
            Some(_) => {}
        }
    }
    for (name, identity) in found.functions.keys() {
        if !expected
            .functions
            .contains_key(&(name.clone(), identity.clone()))
        {
            comparison
                .extra
                .push(format!("function {name}({identity})"));
        }
    }
    comparison
}

/// Up to `DRIFT_OBJECTS_NAMED` objects, then a count of the rest.
fn named_objects(objects: &[String]) -> String {
    let listing = objects
        .iter()
        .take(DRIFT_OBJECTS_NAMED)
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join("; ");
    match objects.len() {
        named if named > DRIFT_OBJECTS_NAMED => {
            format!("{listing} and {} more", named - DRIFT_OBJECTS_NAMED)
        }
        _ => listing,
    }
}

/// The frozen release's definitions and the name of the schema the source
/// lives in. The definitions come from applying the release SQL (001, plus
/// 002 for a #258 source) to a scratch schema inside this transaction,
/// under a savepoint that is rolled back before anything else happens, so
/// they are exact for this server's PostgreSQL version and nothing from the
/// scratch apply survives. Taken once, before any DDL touches the source,
/// and used both to decide whether a database without `qbit_share_ledger`
/// is really fresh and, after 001 has run, to check what it left alone.
async fn release_fingerprint(
    tx: &mut Transaction<'_, Postgres>,
    state: SourceState,
    base_schema: &str,
) -> Result<(SchemaFingerprint, String)> {
    let row = sqlx::query("SELECT current_schema()::text AS schema,current_setting('search_path') AS search_path,current_database()::text AS database,current_user::text AS role")
        .fetch_one(&mut **tx).await?;
    let source_schema: String = row.try_get("schema")?;
    let search_path: String = row.try_get("search_path")?;
    let database: String = row.try_get("database")?;
    let role: String = row.try_get("role")?;
    let scratch = format!("qbit_prism_scratch_{}", Uuid::new_v4().simple());
    sqlx::raw_sql("SAVEPOINT qbit_prism_release_schema")
        .execute(&mut **tx)
        .await?;
    if let Err(error) = sqlx::raw_sql(&format!("CREATE SCHEMA {scratch}"))
        .execute(&mut **tx)
        .await
    {
        if matches!(&error, sqlx::Error::Database(failure) if failure.code().as_deref() == Some("42501"))
        {
            bail!("cannot verify the source schema against the v2.0.x release: role {role} lacks the CREATE privilege on database {database}, which the check needs to apply the release SQL to a scratch schema inside the migration transaction (it is rolled back afterwards). Grant it with `GRANT CREATE ON DATABASE {database} TO {role}` and migrate again; nothing was changed");
        }
        return Err(error.into());
    }
    sqlx::raw_sql(&format!("SET LOCAL search_path TO {scratch}"))
        .execute(&mut **tx)
        .await?;
    sqlx::raw_sql(base_schema).execute(&mut **tx).await?;
    if state == SourceState::Applied258 {
        sqlx::raw_sql(include_str!(
            "../../../qbit-prism/sql/002_candidate_bodies.sql"
        ))
        .execute(&mut **tx)
        .await?;
    }
    let applied_in: String = sqlx::query_scalar("SELECT current_schema()::text")
        .fetch_one(&mut **tx)
        .await?;
    ensure!(
        applied_in == scratch,
        "release schema was applied in {applied_in}, not in scratch schema {scratch}"
    );
    let expected = fingerprint_schema(tx, &scratch).await?;
    sqlx::raw_sql("ROLLBACK TO SAVEPOINT qbit_prism_release_schema; RELEASE SAVEPOINT qbit_prism_release_schema")
        .execute(&mut **tx).await?;
    sqlx::query("SELECT set_config('search_path',$1,true)")
        .bind(&search_path)
        .execute(&mut **tx)
        .await?;
    let (restored, scratch_gone): (String, bool) =
        sqlx::query_as("SELECT current_schema()::text,to_regnamespace($1) IS NULL")
            .bind(&scratch)
            .fetch_one(&mut **tx)
            .await?;
    ensure!(
        restored == source_schema && scratch_gone,
        "release schema scratch apply did not roll back (current schema {restored}, expected {source_schema}; scratch schema {scratch} present: {})",
        !scratch_gone
    );
    Ok((expected, source_schema))
}

/// The source schema as it is now, without the migrator's own version
/// table: that was created before any source object was read and is native
/// bookkeeping, not part of the 2.x.x source.
async fn source_fingerprint(
    tx: &mut Transaction<'_, Postgres>,
    source_schema: &str,
) -> Result<SchemaFingerprint> {
    let mut found = fingerprint_schema(tx, source_schema).await?;
    found.tables.remove("qbit_prism_schema_migrations");
    found.constraints.remove("qbit_prism_schema_migrations");
    Ok(found)
}

/// Every table, sequence, index, trigger and function the release creates
/// that the source already has, named the way the drift report names them.
fn release_objects_present(expected: &SchemaFingerprint, found: &SchemaFingerprint) -> Vec<String> {
    let mut present = Vec::new();
    for table in expected.tables.keys() {
        if found.tables.contains_key(table) {
            present.push(format!("table {table}"));
        }
    }
    for name in expected.sequences.keys() {
        if found.sequences.contains_key(name) {
            present.push(format!("sequence {name}"));
        }
    }
    for name in expected.indexes.keys() {
        if let Some(actual) = found.indexes.get(name) {
            present.push(format!("index {name} on {}", actual.table));
        }
    }
    for (table, name) in expected.triggers.keys() {
        if found.triggers.contains_key(&(table.clone(), name.clone())) {
            present.push(format!("trigger {name} on {table}"));
        }
    }
    for (name, identity) in expected.functions.keys() {
        if found
            .functions
            .contains_key(&(name.clone(), identity.clone()))
        {
            present.push(format!("function {name}({identity})"));
        }
    }
    present
}

/// A database without `qbit_share_ledger` is fresh only when it has nothing
/// else the release 001 creates either. Anything else is a partial 001: a
/// selective restore, or a piece of the schema installed by hand, which
/// 001's `IF NOT EXISTS` would keep exactly as it is. Refused before any
/// DDL. Objects the release does not create, an operator's own table for
/// instance, do not disqualify a fresh database; they are logged as extras.
async fn require_fresh_source(
    tx: &mut Transaction<'_, Postgres>,
    expected: &SchemaFingerprint,
    source_schema: &str,
) -> Result<()> {
    let found = source_fingerprint(tx, source_schema).await?;
    let present = release_objects_present(expected, &found);
    ensure!(
        present.is_empty(),
        "refusing to migrate a {STATE_PARTIAL_001} source before any DDL: the database has no qbit_share_ledger but holds {} object(s) that the 2.x.x release's 001_share_ledger.sql creates ({}), so it is neither an empty database nor a 2.x.x ledger, and 001's IF NOT EXISTS would keep those objects whatever they hold. Nothing was changed. Restore the full pre-migration backup, or migrate into an empty database",
        present.len(),
        named_objects(&present)
    );
    let comparison = compare_fingerprints(expected, &found);
    if !comparison.extra.is_empty() {
        tracing::warn!(
            source = STATE_FRESH,
            extra = comparison.extra.len(),
            objects = %named_objects(&comparison.extra),
            "empty database has objects the 2.x.x release does not create; they are kept as they are"
        );
    }
    Ok(())
}

/// After 001 has run, refuse anything its `IF NOT EXISTS` left alone that
/// differs from the frozen release, whose definitions `release_fingerprint`
/// took before any DDL. On a fresh database 001 just created everything,
/// so this passes trivially; it runs there too, as a second guard. Extra
/// objects, columns, constraints, indexes and sequences are kept and
/// logged; a missing or different one fails the migration, which rolls back
/// whole, so the database is unchanged.
async fn require_release_schema(
    tx: &mut Transaction<'_, Postgres>,
    state: SourceState,
    expected: &SchemaFingerprint,
    source_schema: &str,
) -> Result<()> {
    let found = source_fingerprint(tx, source_schema).await?;
    let comparison = compare_fingerprints(expected, &found);
    let (release, files) = match state {
        SourceState::Applied258 => (
            "v2.0.2",
            "001_share_ledger.sql and 002_candidate_bodies.sql",
        ),
        _ => ("v2.0.1", "001_share_ledger.sql"),
    };
    if !comparison.extra.is_empty() {
        tracing::warn!(
            source = state.rule().name,
            release,
            extra = comparison.extra.len(),
            objects = %named_objects(&comparison.extra),
            "source schema has objects the 2.x.x release does not create; they are kept as they are"
        );
    }
    ensure!(
        comparison.drift.is_empty(),
        "refusing to migrate a {STATE_DRIFTED_001} source: after 001_share_ledger.sql ran, the database does not match the v2.0.x release schema ({release}, {files}), {} object(s) differ ({}). Nothing was changed: the migration rolled back. Restore the pre-migration backup, or bring the database to the release schema with the 2.x.x release (v2.0.1 or later; v2.0.2 for a #258 database) and take a new backup, then migrate again",
        comparison.drift.len(),
        named_objects(&comparison.drift)
    );
    tracing::info!(
        source = state.rule().name,
        release,
        tables = expected.tables.len(),
        indexes = expected.indexes.len(),
        triggers = expected.triggers.len(),
        functions = expected.functions.len(),
        sequences = expected.sequences.len(),
        extra = comparison.extra.len(),
        "source schema matches the 2.x.x release"
    );
    Ok(())
}

/// Apply the base schema and every native migration inside the caller's
/// transaction, which holds the migration lock throughout. Refusals happen
/// before any DDL or roll the transaction back, so a refused database is
/// unchanged.
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
                "refusing to migrate a {STATE_NEWER} source before any DDL: {reason}"
            ),
            SourceVerdict::Partial(missing) => bail!(
                "refusing to migrate a {STATE_PARTIAL_002} source: 001_share_ledger.sql ran but 002_candidate_bodies.sql did not finish, missing {}. Finish it with the v2.0.2 release (PRISM_POSTGRES_INIT_SCHEMA=1 applies both files) or restore the pre-migration backup, then migrate again",
                missing.join(", ")
            ),
        };
        let base_schema = base_schema_transaction_body(include_str!(
            "../../../qbit-prism/sql/001_share_ledger.sql"
        ))?;
        // The release definitions, taken once under a savepoint and rolled
        // back before the source is touched.
        let (expected, source_schema) = release_fingerprint(tx, state, &base_schema).await?;
        if state == SourceState::Fresh {
            // No share ledger and no 002 object: fresh only if nothing else
            // of 001 is there either, or 001 would keep it as it is.
            require_fresh_source(tx, &expected, &source_schema).await?;
        }
        refuse_undrained_outbox(tx, &inventory, None).await?;
        sqlx::raw_sql(&base_schema).execute(&mut **tx).await?;
        // 001 repaired what it re-asserts; what it skipped must already be
        // the release definition before any native DDL alters those tables.
        require_release_schema(tx, state, &expected, &source_schema).await?;
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
    } else if !versions.contains(&6) {
        // A database an earlier 3.x.x build migrated to native schema 3, 4
        // or 5. That build's drain check used the v1-only predicate, which
        // never counted a v2 row (`candidate ?& ...` is NULL for a NULL
        // body), so a pending v2 candidate can still be there. The
        // column-aware check runs here, before 004, 005 or 006 touch
        // anything, so a refusal on this path is before any DDL too.
        let inventory = inspect_source_schema(tx).await?;
        refuse_undrained_outbox(tx, &inventory, Some(prior_version)).await?;
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
/// schema version and refuses one below `REQUIRED_SCHEMA_VERSION`. A newer
/// one is accepted with a warning, so frontends on the previous release keep
/// starting while a rollout drains and replaces them one at a time; a format
/// an older binary must not touch is declared as a capability instead.
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
    if version > REQUIRED_SCHEMA_VERSION {
        tracing::warn!(
            schema_version = version,
            required_schema_version = REQUIRED_SCHEMA_VERSION,
            "database schema is newer than this server requires; a later release migrated it"
        );
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn column(data_type: &str, not_null: bool) -> ColumnDefinition {
        ColumnDefinition {
            data_type: data_type.to_owned(),
            not_null,
            default: None,
            identity: String::new(),
            generated: String::new(),
            collation: None,
        }
    }

    fn table(columns: &[(&str, ColumnDefinition)]) -> TableDefinition {
        TableDefinition {
            persistence: "p".into(),
            columns: columns
                .iter()
                .map(|(name, definition)| ((*name).to_owned(), definition.clone()))
                .collect(),
        }
    }

    #[test]
    fn schema_qualification_is_stripped_only_where_it_qualifies() {
        assert_eq!(
            strip_schema_qualification(
                "CREATE INDEX qbit_pool_blocks_public_recent_idx ON public.qbit_pool_blocks USING btree (a)",
                "public"
            ),
            "CREATE INDEX qbit_pool_blocks_public_recent_idx ON qbit_pool_blocks USING btree (a)"
        );
        assert_eq!(
            strip_schema_qualification(
                "ON \"Prism\".t FOR EACH ROW EXECUTE FUNCTION \"Prism\".f()",
                "Prism"
            ),
            "ON t FOR EACH ROW EXECUTE FUNCTION f()"
        );
        assert_eq!(
            strip_schema_qualification(
                "nextval('qbit_prism_scratch_ab.s'::regclass)",
                "qbit_prism_scratch_ab"
            ),
            "nextval('s'::regclass)"
        );
        assert_eq!(
            strip_schema_qualification("republic.x", "public"),
            "republic.x"
        );
        assert_eq!(strip_schema_qualification("public", "public"), "public");
        assert_eq!(
            normalize_function_config("search_path=pg_catalog, public, pg_temp", "public"),
            "search_path=pg_catalog, <schema>, pg_temp"
        );
        assert_eq!(
            normalize_function_config("search_path=pg_catalog, \"Prism\", pg_temp", "Prism"),
            "search_path=pg_catalog, <schema>, pg_temp"
        );
        assert_eq!(
            normalize_function_config("work_mem=public", "public"),
            "work_mem=public"
        );
        assert_eq!(
            strip_not_valid("CHECK ((a > 0)) NOT VALID"),
            "CHECK ((a > 0))"
        );
    }

    #[test]
    fn comparison_refuses_missing_or_different_and_tolerates_extra() {
        let mut expected = SchemaFingerprint::default();
        expected.tables.insert(
            "t".into(),
            table(&[("a", column("bigint", true)), ("b", column("text", false))]),
        );
        expected
            .tables
            .insert("gone".into(), table(&[("x", column("text", true))]));
        expected
            .constraints
            .entry("t".into())
            .or_default()
            .insert("CHECK ((a > 0))".into(), "t_a_check".into());
        expected.indexes.insert(
            "t_b_idx".into(),
            IndexDefinition {
                table: "t".into(),
                definition: "CREATE INDEX t_b_idx ON t USING btree (b)".into(),
                valid: true,
            },
        );
        let mut found = SchemaFingerprint::default();
        // Different column order, a widened type, an extra column, the same
        // constraint under another name, an extra index and an extra table.
        found.tables.insert(
            "t".into(),
            table(&[
                ("extra", column("text", false)),
                ("b", column("text", false)),
                ("a", column("numeric", true)),
            ]),
        );
        found
            .tables
            .insert("other".into(), table(&[("x", column("text", true))]));
        found
            .constraints
            .entry("t".into())
            .or_default()
            .insert("CHECK ((a > 0))".into(), "t_check1".into());
        found.indexes.insert(
            "t_b_idx".into(),
            IndexDefinition {
                table: "t".into(),
                definition: "CREATE INDEX t_b_idx ON t USING btree (b)".into(),
                valid: true,
            },
        );
        found.indexes.insert(
            "t_extra_idx".into(),
            IndexDefinition {
                table: "t".into(),
                definition: "CREATE INDEX t_extra_idx ON t USING btree (extra)".into(),
                valid: true,
            },
        );
        let comparison = compare_fingerprints(&expected, &found);
        assert_eq!(
            comparison.drift,
            vec![
                "missing table gone",
                "column t.a differs: type expected bigint, found numeric",
            ]
        );
        assert_eq!(
            comparison.extra,
            vec!["column t.extra", "table other", "index t_extra_idx on t"]
        );

        // A matching source has nothing to report.
        found
            .tables
            .insert("gone".into(), table(&[("x", column("text", true))]));
        found
            .tables
            .get_mut("t")
            .unwrap()
            .columns
            .insert("a".into(), column("bigint", true));
        let comparison = compare_fingerprints(&expected, &found);
        assert!(comparison.drift.is_empty(), "{:?}", comparison.drift);

        // A constraint by definition, not by name; an invalid index.
        found.constraints.get_mut("t").unwrap().clear();
        found.indexes.get_mut("t_b_idx").unwrap().valid = false;
        let comparison = compare_fingerprints(&expected, &found);
        assert_eq!(
            comparison.drift,
            vec![
                "missing constraint t_a_check on t: CHECK ((a > 0))",
                "index t_b_idx on t is not valid",
            ]
        );
        let long: Vec<String> = (0..DRIFT_OBJECTS_NAMED + 3)
            .map(|index| format!("object {index}"))
            .collect();
        assert!(named_objects(&long).ends_with("; object 15 and 3 more"));
    }

    #[test]
    fn every_cited_source_state_is_a_row_found_by_name() {
        for name in [
            STATE_FRESH,
            STATE_PARTIAL_001,
            STATE_PRE_258,
            STATE_APPLIED_258,
            STATE_PARTIAL_002,
            STATE_NEWER,
            STATE_DRIFTED_001,
        ] {
            assert_eq!(source_rule(name).name, name);
            assert_eq!(
                SOURCE_STATES
                    .iter()
                    .filter(|rule| rule.name == name)
                    .count(),
                1,
                "{name} must be exactly one row"
            );
        }
        assert_eq!(SOURCE_STATES.len(), 7, "every row has a name constant");
        for state in [
            SourceState::Fresh,
            SourceState::Pre258,
            SourceState::Applied258,
        ] {
            assert!(SOURCE_STATES.contains(state.rule()));
        }
        assert_eq!(SourceState::Fresh.rule().name, "fresh");
        assert_eq!(SourceState::Pre258.rule().name, "pre-#258");
        assert_eq!(SourceState::Applied258.rule().name, "#258 applied");
        assert_eq!(
            source_rule(STATE_PARTIAL_001).verdict,
            "refuse before any DDL, naming the objects present"
        );
    }

    #[test]
    fn a_leftover_release_object_disqualifies_fresh_and_an_operator_object_does_not() {
        let mut expected = SchemaFingerprint::default();
        expected.tables.insert(
            "qbit_pool_blocks".into(),
            table(&[("a", column("text", true))]),
        );
        expected.sequences.insert(
            "qbit_audit_publication_sequence_seq".into(),
            sequence("bigint", 1, i64::MAX),
        );
        expected.indexes.insert(
            "qbit_pool_blocks_maturity_idx".into(),
            IndexDefinition {
                table: "qbit_pool_blocks".into(),
                definition:
                    "CREATE INDEX qbit_pool_blocks_maturity_idx ON qbit_pool_blocks USING btree (a)"
                        .into(),
                valid: true,
            },
        );
        expected.triggers.insert(
            ("qbit_pool_blocks".into(), "qbit_pool_blocks_guard".into()),
            TriggerDefinition {
                definition: "CREATE TRIGGER ...".into(),
                enabled: "O".into(),
            },
        );
        expected.functions.insert(
            ("qbit_prism_window".into(), "w numeric".into()),
            FunctionDefinition {
                arguments: "w numeric".into(),
                result: None,
                language: "sql".into(),
                body: None,
                volatility: "v".into(),
                strict: false,
                security_definer: false,
                leakproof: false,
                parallel: "u".into(),
                kind: "f".into(),
                config: Vec::new(),
            },
        );
        let mut found = SchemaFingerprint::default();
        found.tables.insert(
            "operator_notes".into(),
            table(&[("note", column("text", true))]),
        );
        found.sequences.insert(
            "operator_notes_note_id_seq".into(),
            sequence("bigint", 1, i64::MAX),
        );
        assert!(release_objects_present(&expected, &found).is_empty());
        let comparison = compare_fingerprints(&expected, &found);
        assert_eq!(
            comparison.extra,
            vec![
                "table operator_notes",
                "sequence operator_notes_note_id_seq"
            ]
        );

        // Each kind of leftover is named, tables first.
        for (kind, name) in [
            ("table", "qbit_pool_blocks"),
            ("sequence", "qbit_audit_publication_sequence_seq"),
            ("function", "qbit_prism_window(w numeric)"),
        ] {
            let mut found = SchemaFingerprint::default();
            match kind {
                "table" => {
                    found
                        .tables
                        .insert(name.into(), table(&[("other", column("bigint", false))]));
                }
                "sequence" => {
                    found
                        .sequences
                        .insert(name.into(), sequence("integer", 1, 2_147_483_647));
                }
                _ => {
                    found.functions.insert(
                        ("qbit_prism_window".into(), "w numeric".into()),
                        expected.functions.values().next().unwrap().clone(),
                    );
                }
            }
            assert_eq!(
                release_objects_present(&expected, &found),
                vec![format!("{kind} {name}")]
            );
        }
        let mut found = SchemaFingerprint::default();
        found.tables.insert(
            "qbit_pool_blocks".into(),
            table(&[("a", column("text", true))]),
        );
        found.indexes.insert(
            "qbit_pool_blocks_maturity_idx".into(),
            expected.indexes["qbit_pool_blocks_maturity_idx"].clone(),
        );
        found.triggers.insert(
            ("qbit_pool_blocks".into(), "qbit_pool_blocks_guard".into()),
            expected.triggers[&(
                "qbit_pool_blocks".to_owned(),
                "qbit_pool_blocks_guard".to_owned(),
            )]
                .clone(),
        );
        assert_eq!(
            release_objects_present(&expected, &found),
            vec![
                "table qbit_pool_blocks",
                "index qbit_pool_blocks_maturity_idx on qbit_pool_blocks",
                "trigger qbit_pool_blocks_guard on qbit_pool_blocks",
            ]
        );
    }

    fn sequence(data_type: &str, increment: i64, max: i64) -> SequenceDefinition {
        SequenceDefinition {
            persistence: "p".into(),
            data_type: data_type.to_owned(),
            start: 1,
            increment,
            min: 1,
            max,
            cache: 1,
            cycle: false,
        }
    }

    #[test]
    fn sequence_comparison_refuses_structure_and_names_each_difference() {
        let release = sequence("bigint", 1, i64::MAX);
        let mut expected = SchemaFingerprint::default();
        expected
            .sequences
            .insert("t_id_seq".into(), release.clone());
        expected
            .sequences
            .insert("u_id_seq".into(), release.clone());
        expected
            .sequences
            .insert("gone_seq".into(), release.clone());
        let mut found = SchemaFingerprint::default();
        // A lowered maximum with a narrowed type, a changed increment, a
        // missing sequence, and one the release does not create.
        found
            .sequences
            .insert("t_id_seq".into(), sequence("integer", 1, 2_147_483_647));
        found
            .sequences
            .insert("u_id_seq".into(), sequence("bigint", 2, i64::MAX));
        found
            .sequences
            .insert("operator_seq".into(), release.clone());
        let comparison = compare_fingerprints(&expected, &found);
        assert_eq!(
            comparison.drift,
            vec![
                "missing sequence gone_seq",
                "sequence t_id_seq differs: type expected bigint, found integer, maximum expected 9223372036854775807, found 2147483647",
                "sequence u_id_seq differs: increment expected 1, found 2",
            ]
        );
        assert_eq!(comparison.extra, vec!["sequence operator_seq"]);

        // Every property is named; the current value is not a property.
        let mut advanced = release.clone();
        advanced.start = 5;
        advanced.min = 0;
        advanced.cache = 20;
        advanced.cycle = true;
        assert_eq!(
            sequence_differences(&release, &advanced),
            "start expected 1, found 5, minimum expected 1, found 0, cache expected 1, found 20, expected NO CYCLE, found CYCLE"
        );
        found.sequences.insert("gone_seq".into(), release.clone());
        found.sequences.insert("t_id_seq".into(), release.clone());
        found.sequences.insert("u_id_seq".into(), release);
        let comparison = compare_fingerprints(&expected, &found);
        assert!(comparison.drift.is_empty(), "{:?}", comparison.drift);
    }

    #[test]
    fn persistence_is_compared_for_tables_and_sequences() {
        let mut expected = SchemaFingerprint::default();
        expected.tables.insert(
            "qbit_share_ledger".into(),
            table(&[("share_id", column("text", true))]),
        );
        expected.tables.insert(
            "qbit_pool_blocks".into(),
            table(&[("block_hash", column("text", true))]),
        );
        expected.sequences.insert(
            "qbit_share_ledger_share_seq_seq".into(),
            sequence("bigint", 1, i64::MAX),
        );
        // The same columns and structure, but the ledger, its sequence and a
        // temporary blocks table are not the logged relations the release
        // creates; an unlogged table of the operator's own is only extra.
        let mut found = SchemaFingerprint::default();
        let mut unlogged = table(&[("share_id", column("text", true))]);
        unlogged.persistence = "u".into();
        found
            .tables
            .insert("qbit_share_ledger".into(), unlogged.clone());
        let mut temporary = table(&[("block_hash", column("text", true))]);
        temporary.persistence = "t".into();
        found.tables.insert("qbit_pool_blocks".into(), temporary);
        found.tables.insert("operator_scratch".into(), unlogged);
        let mut unlogged_sequence = sequence("bigint", 1, i64::MAX);
        unlogged_sequence.persistence = "u".into();
        found
            .sequences
            .insert("qbit_share_ledger_share_seq_seq".into(), unlogged_sequence);
        let comparison = compare_fingerprints(&expected, &found);
        assert_eq!(
            comparison.drift,
            vec![
                "table qbit_pool_blocks differs: expected logged, found TEMPORARY",
                "table qbit_share_ledger differs: expected logged, found UNLOGGED",
                "sequence qbit_share_ledger_share_seq_seq differs: expected logged, found UNLOGGED",
            ]
        );
        assert_eq!(comparison.extra, vec!["table operator_scratch"]);
        assert_eq!(
            release_objects_present(&expected, &found),
            vec![
                "table qbit_pool_blocks",
                "table qbit_share_ledger",
                "sequence qbit_share_ledger_share_seq_seq",
            ]
        );

        // Persistence is named alongside the other sequence properties.
        let mut changed = sequence("bigint", 2, i64::MAX);
        changed.persistence = "u".into();
        assert_eq!(
            sequence_differences(&sequence("bigint", 1, i64::MAX), &changed),
            "expected logged, found UNLOGGED, increment expected 1, found 2"
        );
        // Logged everywhere: nothing to report.
        found
            .tables
            .get_mut("qbit_share_ledger")
            .unwrap()
            .persistence = "p".into();
        found
            .tables
            .get_mut("qbit_pool_blocks")
            .unwrap()
            .persistence = "p".into();
        found
            .sequences
            .get_mut("qbit_share_ledger_share_seq_seq")
            .unwrap()
            .persistence = "p".into();
        let comparison = compare_fingerprints(&expected, &found);
        assert!(comparison.drift.is_empty(), "{:?}", comparison.drift);
    }
}
