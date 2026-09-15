//! Schema migration from a pinned 2.x.x source, the schema gates every
//! start passes, and the explicit one-time migration of Python filesystem
//! artifacts. Validation is performed before any write; operator files and
//! historical rows are retained.
use super::connect::lock;
use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::path::{Path, PathBuf};

/// The schema migrations every native start requires, each checked on its
/// own. Add every new migration file here. `Ledger::connect` refuses a
/// database missing any of them even without `initialize`, so a newer binary
/// never reaches the claim path on a database it has not migrated, and a
/// later number never hides an earlier gap: 007 landed after 008, 009 and 010. A migration this
/// binary does not know is accepted with a warning: native migrations are
/// additive, and a release whose format an older binary must not touch
/// declares a capability, which `migrate_schema` refuses before any DDL and
/// `require_known_capabilities` refuses again at connect.
pub const REQUIRED_SCHEMA_VERSIONS: &[i32] = &[2, 3, 4, 5, 6, 7, 8, 9, 10, 11];

/// Schema migration numbers as they appear in messages: `2, 3, 4`, or
/// `none`.
pub fn schema_version_list(versions: &[i32]) -> String {
    if versions.is_empty() {
        return "none".to_owned();
    }
    versions
        .iter()
        .map(i32::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Legacy source formats the cutover can validate and drain. Version 2 is
/// historical evidence from #258, never native runtime support.
const SOURCE_CAPABILITIES: &[(&str, i32)] = &[("candidate_storage_version", 2)];
/// Formats the native claim lane can process. Migration 006 records the
/// old declaration as provenance and declares this runtime format atomically.
const NATIVE_CAPABILITIES: &[(&str, i32)] = &[
    ("candidate_storage_version", 1),
    // 011: the offer-before-landing outbox lifecycle. A binary without this
    // entry refuses the migrated database at connect, which is what keeps a
    // pre-011 frontend off rows the reservation lifecycle owns.
    ("candidate_offer_lifecycle", 1),
];

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
const STATE_NATIVE_COLLISION: &str = "native collision";
const STATE_DRIFTED_001: &str = "drifted 001";

/// The source states migration 006 accepts or refuses, as data. Detection is
/// column-aware: it asks the catalog which 002 objects exist, so a fixed
/// predicate never errors on a source that lacks a column, and it looks at
/// outbox rows for the drain check because the capability row proves only
/// that 002 ran. The release definitions come from applying the frozen
/// release SQL to a scratch schema under a savepoint, before any DDL touches
/// the source. A database without `qbit_share_ledger` is fresh only if it
/// has nothing else that 001 creates; otherwise it is a partial 001,
/// refused before any DDL. The native migrations are applied to the same
/// scratch schema after the release SQL, so the objects and columns they
/// create and the release does not are known before any DDL too: one
/// already present is a native collision, refused before any DDL, because
/// a native migration's `IF NOT EXISTS` would keep it whatever it holds.
/// The last row is decided after 001 has run: the release 001 is
/// idempotent and repairs what it re-asserts, so the check compares what
/// its `IF NOT EXISTS` left alone against the same scratch apply, and a
/// refusal rolls the whole migration back.
pub const SOURCE_STATES: [SourceStateRule; 8] = [
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
        name: STATE_NATIVE_COLLISION,
        evidence: "a table, sequence, index, trigger, function or column a native migration creates is already present in a 2.x.x or empty database",
        verdict: "refuse before any DDL, naming the objects",
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

/// Unqualified reads can resolve past current_schema(), while unqualified
/// DDL creates in it. Refuse that mismatch before even creating the migration
/// record: otherwise an empty first schema can shadow a later ledger's data.
async fn require_source_schema_resolution(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    let schema: Option<String> = sqlx::query_scalar("SELECT current_schema()::text")
        .fetch_one(&mut **tx)
        .await?;
    let schema =
        schema.context("refusing to migrate before any DDL: search_path has no current schema")?;
    let foreign_objects: Vec<String> = sqlx::query_scalar("SELECT format('relation %I.%I',n.nspname,c.relname) AS object FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname<>$1 AND left(c.relname,5)='qbit_' AND pg_table_is_visible(c.oid) UNION ALL SELECT format('function %I.%I(%s)',n.nspname,p.proname,pg_get_function_identity_arguments(p.oid)) AS object FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname<>$1 AND left(p.proname,5)='qbit_' AND pg_function_is_visible(p.oid) ORDER BY 1")
        .bind(&schema).fetch_all(&mut **tx).await?;
    ensure!(foreign_objects.is_empty(),
        "refusing to migrate before any DDL: current schema {schema}, but search_path resolves {} qbit_ object(s) in another schema: {}. Unqualified DDL would create objects in {schema} and could hide existing accounting history. Nothing was changed. Set search_path to the intended ledger schema first, without qbit_ objects resolved from other schemas, then migrate again",
        foreign_objects.len(), named_objects(&foreign_objects));
    Ok(())
}

/// Ask the catalog, not the data, which 002 objects exist. `to_regclass` and
/// `to_regproc` follow search_path; migrate first verifies that visible
/// qbit_ objects resolve in current_schema(), where its DDL creates objects.
/// On a native database the same inventory carries the capability rows and
/// the outbox columns the native checks read, taken once, before any DDL.
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
    let base = regclass_present(tx, &["qbit_share_ledger".to_owned(), OUTBOX.to_owned()]).await?;
    let capabilities = read_capabilities(&mut **tx).await?;
    Ok(SourceInventory {
        share_ledger: base[0],
        outbox: base[1],
        present,
        capabilities,
    })
}

pub(super) fn classify_source(inventory: &SourceInventory) -> SourceVerdict {
    if let Some(rows) = &inventory.capabilities {
        if let Err(error) = refuse_unknown_capabilities(rows, SOURCE_CAPABILITIES) {
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

/// The capability rows current_schema() declares, or `None` when it has no
/// capability relation. The unqualified name follows search_path and can
/// resolve past a schema that lost its table to a later schema's, whose
/// declaration describes another ledger: `require_migration_history` bound
/// the history to current_schema(), and the declaration must come from the
/// same schema, or a version-1 row elsewhere would clear a database whose
/// own declaration is gone. Migrate refuses such resolution before any DDL;
/// connect refuses it here, naming both schemas.
async fn read_capabilities<'e, E>(executor: E) -> Result<Option<Vec<(String, i32)>>>
where
    E: sqlx::Acquire<'e, Database = Postgres>,
{
    let mut connection = executor.acquire().await?;
    let Some(relation) = sqlx::query(
        "SELECT n.nspname::text AS schema,current_schema()::text AS current,c.relkind::text AS kind,c.relrowsecurity AS enabled,c.relforcerowsecurity AS forced FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=to_regclass('qbit_prism_schema_capabilities')",
    )
    .fetch_optional(&mut *connection)
    .await?
    else {
        return Ok(None);
    };
    let schema: String = relation.try_get("schema")?;
    let current: Option<String> = relation.try_get("current")?;
    ensure!(
        current.as_deref() == Some(schema.as_str()),
        "qbit_prism_schema_capabilities resolves to {schema}.qbit_prism_schema_capabilities, outside the current schema {} whose migration history was verified; refusing to trust another schema's capability declaration for this database. Restore the current schema's capability table from the full backup, or, if every pending candidate is known to use this server's version 1 format, re-create it from migrations/006_source_schema.sql, or set search_path to the intended ledger schema, then start or migrate again",
        current.as_deref().unwrap_or("(none)")
    );
    let kind: String = relation.try_get("kind")?;
    let enabled: bool = relation.try_get("enabled")?;
    let forced: bool = relation.try_get("forced")?;
    ensure!(
        kind == "r",
        "qbit_prism_schema_capabilities must be an ordinary table (found relation kind {kind}); refusing to trust substituted capability rows. Restore the original capability table from the full backup before starting or migrating this database"
    );
    ensure!(
        !enabled && !forced,
        "qbit_prism_schema_capabilities has row-level security enabled or forced; refusing to trust possibly hidden capability rows. Review and disable row-level security before starting or migrating this database"
    );
    let rows = sqlx::query("SELECT capability,capability_value FROM qbit_prism_schema_capabilities ORDER BY capability")
        .fetch_all(&mut *connection).await?;
    let rows: Vec<(String, i32)> = rows
        .iter()
        .map(|row| Ok((row.try_get("capability")?, row.try_get("capability_value")?)))
        .collect::<Result<_>>()?;
    Ok(Some(rows))
}

/// A capability this binary does not know, or a known one beyond the value
/// it understands, means a newer PRISM release wrote the database. Refused
/// before any DDL on every migrate path, and again at connect.
pub(super) fn refuse_unknown_capabilities(
    rows: &[(String, i32)],
    supported: &[(&str, i32)],
) -> Result<()> {
    for (name, value) in rows {
        match supported.iter().find(|(known, _)| known == name) {
            None => bail!("database declares capability {name} = {value}, which this server does not understand: a newer PRISM release wrote this database; upgrade the server before starting it here"),
            Some((_, max)) => ensure!(
                (1..=*max).contains(value),
                "database declares {name} = {value}, but this server understands {name} 1 to {max} only: a newer PRISM release wrote this database; upgrade the server before starting it here"
            ),
        }
    }
    Ok(())
}

/// The capability every native database declares once 006 has run.
const DECLARED_CAPABILITY: &str = "candidate_storage_version";
/// The capability 011 declares, at the value it declares, once the offer
/// lifecycle owns the outbox.
const OFFER_LIFECYCLE_CAPABILITY: (&str, i32) = ("candidate_offer_lifecycle", 1);

/// A database at migration 6 declares `candidate_storage_version`, and one
/// at migration 11 declares `candidate_offer_lifecycle = 1` as well: 006
/// created the table and its row, 011 declared the lifecycle last, once
/// every object it owns existed, and nothing native removes or edits any of
/// them. An absent table or storage row is a dropped table or a deleted row,
/// after which the database can no longer say which PRISM release wrote it,
/// so it is refused rather than read as a legacy state, which has no meaning
/// once 006 has run. A missing or edited lifecycle row beside 011's record
/// is a selectively restored declaration, after which the database can no
/// longer say that every unfinished outbox row belongs to the offer
/// lifecycle, so it is refused rather than read as a pre-011 outbox, which
/// 011's record contradicts. Nothing here repairs a declaration: the remedy
/// is named for the operator, who knows what the rows are. `rows` is `None`
/// when the table is missing; `versions` is the recorded migration set, and
/// the caller has established that 6 or 11 is in it.
fn require_declared_capabilities(rows: Option<&[(String, i32)]>, versions: &[i32]) -> Result<()> {
    let (lifecycle, declared) = OFFER_LIFECYCLE_CAPABILITY;
    let Some(rows) = rows else {
        // At 11 the table also held 011's declaration: the remedy names both
        // rows, so the operator is not refused a second time for the other.
        let lifecycle_remedy = if versions.contains(&11) {
            format!(" and, once every unfinished row of qbit_block_candidate_outbox is known to have been written or quarantined by a post-011 frontend, declare the offer lifecycle 011 declared in it again with INSERT INTO qbit_prism_schema_capabilities(capability,capability_value) VALUES('{lifecycle}',{declared})")
        } else {
            String::new()
        };
        bail!("database is at schema migration 6 but has no qbit_prism_schema_capabilities: 006 created it and nothing native drops it, so the table was dropped or restored selectively and the database can no longer declare which PRISM release wrote it. Restore the full backup, or, if every pending candidate is known to use this server's version 1 format, re-create the table and its {DECLARED_CAPABILITY} row from migrations/006_source_schema.sql (1){lifecycle_remedy}, then start or migrate again");
    };
    ensure!(
        rows.iter().any(|(name, _)| name == DECLARED_CAPABILITY),
        "database is at schema migration 6 but qbit_prism_schema_capabilities has no {DECLARED_CAPABILITY} row: 006 declared it and nothing native deletes it, so the row was deleted and the database can no longer declare which PRISM release wrote it. Restore the full backup, or, if every pending candidate is known to use this server's version 1 format, declare it again with INSERT INTO qbit_prism_schema_capabilities(capability,capability_value) VALUES('{DECLARED_CAPABILITY}',1), then start or migrate again"
    );
    if versions.contains(&11) {
        match rows.iter().find(|(capability, _)| capability == lifecycle) {
            None => bail!("database is at schema migration 11 but qbit_prism_schema_capabilities has no {lifecycle} row: 011 declared it once the offer lifecycle owned the outbox and nothing native deletes it, so the row was deleted or the capability table was restored selectively, and the database can no longer declare that every unfinished block candidate belongs to the offer lifecycle. Restore the full backup, or, once every unfinished row of qbit_block_candidate_outbox is known to have been written or quarantined by a post-011 frontend, declare it again with INSERT INTO qbit_prism_schema_capabilities(capability,capability_value) VALUES('{lifecycle}',{declared}), then start or migrate again"),
            Some((_, value)) => ensure!(
                *value == declared,
                "database is at schema migration 11 but qbit_prism_schema_capabilities declares {lifecycle} = {value}: 011 declares {declared} and this server understands {declared} only, so either the row was edited or a newer PRISM release wrote this database. Restore the full backup, or upgrade the server if a newer release wrote it; nothing native writes another value"
            ),
        }
    }
    Ok(())
}

/// The native path's refusal of a database at 6 that no longer declares
/// its capabilities, or at 11 that no longer declares the offer lifecycle
/// as 011 declared it, before any DDL: otherwise a later migration would
/// run above the missing declaration and only the connect-time gate, after
/// the commit, would refuse it. `versions` is the recorded migration set,
/// named in the refusal.
fn refuse_undeclared_native_database(versions: &[i32], inventory: &SourceInventory) -> Result<()> {
    // Checked whenever 6 or 11 is recorded. 011 postdates 006 and every
    // native build records 6 first, so a record with 11 and not 6 was
    // restored selectively; it is not read as a pre-006 database, which
    // would let 006 declare a storage version above an outbox 011 already
    // owns without the lifecycle declaration being checked first.
    if !versions.contains(&6) && !versions.contains(&11) {
        return Ok(());
    }
    if let Err(reason) = require_declared_capabilities(inventory.capabilities.as_deref(), versions)
    {
        bail!(
            "refusing to migrate a native database at schema migrations {} before any DDL: {reason}",
            schema_version_list(versions)
        );
    }
    Ok(())
}

/// The native path's "newer" verdict: refuse a database an earlier 3.x.x
/// build migrated and a newer release then wrote, before any DDL, as
/// `classify_source` refuses a 2.x.x source. Without this, 004, 005 and
/// 006, or 008 and 009, would alter that database and record their
/// versions, and only `require_known_capabilities` would refuse it, after
/// the commit.
/// `versions` is the recorded migration set, named in the refusal.
fn refuse_newer_native_database(versions: &[i32], inventory: &SourceInventory) -> Result<()> {
    if let Some(rows) = &inventory.capabilities {
        let supported = if versions.contains(&6) {
            NATIVE_CAPABILITIES
        } else {
            SOURCE_CAPABILITIES
        };
        if let Err(reason) = refuse_unknown_capabilities(rows, supported) {
            bail!(
                "refusing to migrate a native database at schema migrations {} before any DDL: {reason}",
                schema_version_list(versions)
            );
        }
    }
    Ok(())
}

/// A native database whose record has 3 and not 2. Every native build
/// records both in the one transaction that applies them, so such a record
/// was edited or restored selectively. Only 2 can be hidden that way: 3
/// applies it, and every later migration is checked on its own.
/// `require_schema_version` refuses the gap at every start, and
/// `migrate_schema` neither re-runs 002 on a database at 3 nor records it
/// unseen, which would vouch for objects this run never checked, so nothing
/// would repair it. Refused before any DDL, naming the remedy.
fn refuse_inconsistent_native_record(versions: &[i32]) -> Result<()> {
    ensure!(
        !versions.contains(&3) || versions.contains(&2),
        "refusing to migrate a native database at schema migrations {} before any DDL: migration 3 is recorded and 2 is not, and every native build records both in one transaction, so the migration record was edited or restored selectively; every start refuses the gap, and no migrate repairs it, because 002_multi_instance.sql is not re-run on a database at 3 and recording it unseen would vouch for objects this run never checked. Nothing was changed. Restore the full pre-migration backup, or, once every object 002_multi_instance.sql creates is verified present, record it with INSERT INTO qbit_prism_schema_migrations(version) VALUES(2) and migrate again",
        schema_version_list(versions)
    );
    Ok(())
}

/// 006 adds this column only when absent. A pre-006 native database may
/// already have it from 002 or a partial/manual installation; accepting a
/// nullable or defaultless copy would strand later candidates, since native
/// inserts omit it and the claim lane decodes it as a non-null i32.
async fn require_pre_006_storage_version(
    tx: &mut Transaction<'_, Postgres>,
    versions: &[i32],
) -> Result<()> {
    let row = sqlx::query("SELECT format_type(a.atttypid,a.atttypmod) AS data_type,(SELECT typtype='d' FROM pg_type WHERE oid=a.atttypid) AS is_domain,a.attnotnull AS not_null,pg_get_expr(d.adbin,d.adrelid) AS default_expr,a.attidentity::text AS identity,a.attgenerated::text AS generated FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attrelid=to_regclass('qbit_block_candidate_outbox') AND a.attname='storage_version' AND a.attnum>0 AND NOT a.attisdropped")
        .fetch_optional(&mut **tx).await?;
    let Some(row) = row else {
        // The unmodified pre-258 native path: 006 creates the definition.
        return Ok(());
    };
    let expected = ColumnDefinition {
        data_type: "integer".into(),
        is_domain: false,
        not_null: true,
        default: Some("1".into()),
        identity: String::new(),
        generated: String::new(),
        collation: None,
    };
    let actual = ColumnDefinition {
        data_type: row.try_get("data_type")?,
        is_domain: row.try_get("is_domain")?,
        not_null: row.try_get("not_null")?,
        default: row.try_get("default_expr")?,
        identity: row.try_get("identity")?,
        generated: row.try_get("generated")?,
        collation: None,
    };
    ensure!(actual == expected,
        "refusing to migrate a native database at schema migrations {} before any DDL: column qbit_block_candidate_outbox.storage_version differs: {}; migration 006 preserves an existing column, while native inserts require integer NOT NULL DEFAULT 1 without an identity or generated expression. Nothing was changed. Restore the full pre-migration backup or bring this column to that definition after checking its data, then migrate again",
        schema_version_list(versions), column_differences(&expected, &actual));
    Ok(())
}

/// Refuse a pending 2.x.x row the native claim lane cannot replay, with the
/// predicate built from the outbox columns that exist. The capability row is
/// not consulted: 002 upserts it whatever the writer stored, so only rows say
/// whether v2 work is pending. `native_versions` is the recorded migration
/// set of a database an earlier 3.x.x build migrated to native schema 3, 4
/// or 5, with or without 008 and 009, whose drain check never counted a v2
/// row; that path gets its own wording and remedy. Native pending rows carry
/// `payout_revision`, `block_hash` and either an inline `bundle` (pre-007) or
/// a `window` reference (007 and later), so the predicate never flags them;
/// only a v2 body, a `body_id`, a `storage_version` other than 1, or a v1 body
/// without those fields is refused.
pub(super) async fn refuse_undrained_outbox(
    tx: &mut Transaction<'_, Postgres>,
    inventory: &SourceInventory,
    native_versions: Option<&[i32]>,
) -> Result<()> {
    if !inventory.outbox {
        return Ok(());
    }
    if let Some(versions) = native_versions {
        // The legacy path also fingerprints the release table, but this
        // native path does not. Its drain query must see every pending row.
        let (enabled, forced, policies): (bool, bool, Vec<String>) = sqlx::query_as(
            "SELECT c.relrowsecurity,c.relforcerowsecurity,ARRAY(SELECT p.polname::text FROM pg_policy p WHERE p.polrelid=c.oid ORDER BY p.polname) FROM pg_class c WHERE c.oid='qbit_block_candidate_outbox'::regclass",
        )
        .fetch_one(&mut **tx)
        .await?;
        ensure!(
            !enabled && !forced && policies.is_empty(),
            "refusing to apply migration 006 to a native database at schema migrations {} before any DDL: qbit_block_candidate_outbox has row-level security or policies (enabled={enabled}, forced={forced}, policies={}); the drain check cannot trust a possibly filtered outbox. Nothing was changed. Restore the full backup, or review and remove these policies and security flags before retrying the drain check",
            schema_version_list(versions), named_objects(&policies)
        );
    }
    let mut clauses = Vec::new();
    if inventory.has_outbox_column("storage_version") {
        clauses.push("storage_version <> 1");
    }
    clauses.push("candidate IS NULL");
    if inventory.has_outbox_column("body_id") {
        clauses.push("body_id IS NOT NULL");
    }
    // A native pending row is recognised by the fields its own writer stores.
    // Before 007 that was an inline `bundle`; since 007 the candidate holds a
    // `window` reference instead, so both shapes must count as native or this
    // check would refuse rows this server wrote itself as undrained 2.x.x work.
    clauses.push(
        "NOT (candidate ?& ARRAY['payout_revision','block_hash'] AND (candidate ? 'bundle' OR candidate ? 'window'))",
    );
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
    match native_versions {
        None => bail!("legacy Python block outbox is not drained: {total} pending 2.x.x candidate row(s) cannot be replayed natively ({listing}{more}). Drain them with the pinned 2.x.x release before migrating: start the 2.x.x coordinator (v2.0.2 for storage_version 2 rows, v2.0.1 or later otherwise) and let its block submitter finish every pending candidate, or for a block already accepted on the active chain run `python3 -m lab.prism.recover_pending_blocks --block-hash <hash> --apply` from the 2.x.x image; then take the final backup and repeat the migration. Do not delete pending rows to bypass this check"),
        Some(versions) => bail!("refusing to apply migration 006 to a native database at schema migrations {}: an earlier 3.x.x build migrated it before the drain rule covered these rows, and the legacy Python block outbox is not drained: {total} pending 2.x.x candidate row(s) cannot be replayed natively ({listing}{more}). Nothing was changed. Restore the pre-migration 2.x.x backup and drain them with the pinned 2.x.x release (v2.0.2 for storage_version 2 rows, v2.0.1 or later otherwise): start its coordinator and let the block submitter finish every pending candidate, or for a block already accepted on the active chain run `python3 -m lab.prism.recover_pending_blocks --block-hash <hash> --apply` from the 2.x.x image; then take a new backup and migrate again with this release. The 2.x.x release is not supported against a native schema, so do not point it at this database. If native traffic was admitted after the earlier migration, that restore discards it: see the recovery section of docs/prism-rust-migration.md first. Do not delete pending rows to bypass this check", schema_version_list(versions)),
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
    is_domain: bool,
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
    unique: bool,
    expression: bool,
    partial: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TriggerDefinition {
    definition: String,
    enabled: String,
}

/// A rewrite rule can replace or add to a statement before triggers run.
#[derive(Clone, Debug, PartialEq, Eq)]
struct RuleDefinition {
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

/// A table's persistence, its row-level security flags and its columns.
/// `pg_class.relpersistence` is `p` for an ordinary logged table, `u` for
/// UNLOGGED and `t` for temporary. The release creates logged tables only:
/// an unlogged ledger or outbox is one whose rows PostgreSQL truncates after
/// a crash, so persistence is part of the definition, not a tuning knob.
/// `relrowsecurity` (ENABLE ROW LEVEL SECURITY) and `relforcerowsecurity`
/// (FORCE ROW LEVEL SECURITY, applying it to the owner too) decide which
/// rows a role that does not bypass row-level security sees and may write;
/// the release creates none, and on the outbox they would decide what the
/// drain check and the native claim lane see. `parents` and `children` are
/// the table's `pg_inherits` relations, by name (schema-qualified when in
/// another schema): the release creates none either. A child created with
/// `INHERITS (qbit_share_ledger)` has its rows included in every query of
/// the ledger, the share reads included, without the release constraints
/// ever checking them, and a release table attached as a partition of or
/// inheriting from another table is no longer the relation the release
/// defined.
#[derive(Clone, Debug, PartialEq, Eq)]
struct TableDefinition {
    persistence: String,
    row_security: bool,
    force_row_security: bool,
    parents: Vec<String>,
    children: Vec<String>,
    columns: BTreeMap<String, ColumnDefinition>,
}

/// A row-level security policy, from `pg_policy`: the command it applies to
/// (`polcmd`: `r`, `a`, `w`, `d` or `*`), whether it is permissive or
/// restrictive, the roles it applies to (`PUBLIC` for oid 0, sorted), and
/// its USING and WITH CHECK expressions as the server renders them, without
/// schema qualification.
#[derive(Clone, Debug, PartialEq, Eq)]
struct PolicyDefinition {
    command: String,
    permissive: bool,
    roles: Vec<String>,
    using: Option<String>,
    with_check: Option<String>,
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

/// A foreign key can constrain a release table even when its owner lives in
/// another schema. Local release-owned constraints are compared by definition;
/// all other incoming references to release tables must be refused.
#[derive(Clone, Debug, PartialEq, Eq)]
struct IncomingForeignKey {
    owner: String,
    owner_in_schema: bool,
    name: String,
    referenced_table: String,
}

/// Every table, column, constraint, index, trigger, function, sequence,
/// rewrite rule and row-level security policy of one schema, as the server
/// renders them,
/// without schema qualification. Columns are keyed by name, so their
/// physical order is irrelevant; constraints are keyed per table by
/// definition, so an auto-generated name is irrelevant; comments are not
/// read.
#[derive(Debug, Default, PartialEq, Eq)]
struct SchemaFingerprint {
    /// Keyed by name: each table's persistence, its row-level security
    /// flags and its columns.
    tables: BTreeMap<String, TableDefinition>,
    /// Table, then constraint definition (`constraint_key`), to the name the
    /// constraint carries and its validation state.
    constraints: BTreeMap<String, BTreeMap<String, ConstraintDefinition>>,
    /// References into this schema, including owners in other schemas.
    incoming_foreign_keys: Vec<IncomingForeignKey>,
    /// Indexes that do not back a constraint, by name; the constraint
    /// comparison covers the others under whatever name they were given.
    indexes: BTreeMap<String, IndexDefinition>,
    /// Keyed by table, then trigger name.
    triggers: BTreeMap<(String, String), TriggerDefinition>,
    /// Keyed by table, then rewrite rule name.
    rules: BTreeMap<(String, String), RuleDefinition>,
    /// Keyed by table, then policy name.
    policies: BTreeMap<(String, String), PolicyDefinition>,
    /// Keyed by name, then identity arguments.
    functions: BTreeMap<(String, String), FunctionDefinition>,
    /// Keyed by name: the sequences behind `serial` columns and the ones
    /// created explicitly alike.
    sequences: BTreeMap<String, SequenceDefinition>,
    /// Every relation no other map models, by name: the indexes that back
    /// a constraint, views, materialized views, partitioned tables and
    /// indexes, foreign tables and composite types. Relations of every kind
    /// share one namespace, and `IF NOT EXISTS` looks at the name alone, so
    /// `objects_present` counts a reserved name held by any of them; the
    /// release comparison does not read this map, a constraint-backed index
    /// being compared through its constraint.
    other_relations: BTreeMap<String, OtherRelation>,
}

/// A relation of a kind no other map of `SchemaFingerprint` models.
#[derive(Clone, Debug, PartialEq, Eq)]
struct OtherRelation {
    /// How a refusal names it: `index x backing constraint k on t`, `view
    /// v`.
    description: String,
    /// For an index backing a constraint, the table the constraint is on.
    constraint_table: Option<String>,
}

impl SchemaFingerprint {
    /// Every relation by name, the way a refusal names it, with the table
    /// of an index that backs a constraint: the tables, sequences and
    /// indexes, and the relations of every other kind.
    fn relations(&self) -> BTreeMap<&str, (String, Option<&str>)> {
        let mut all = BTreeMap::new();
        for name in self.tables.keys() {
            all.insert(name.as_str(), (format!("table {name}"), None));
        }
        for name in self.sequences.keys() {
            all.insert(name.as_str(), (format!("sequence {name}"), None));
        }
        for (name, index) in &self.indexes {
            all.insert(
                name.as_str(),
                (format!("index {name} on {}", index.table), None),
            );
        }
        for (name, relation) in &self.other_relations {
            all.insert(
                name.as_str(),
                (
                    relation.description.clone(),
                    relation.constraint_table.as_deref(),
                ),
            );
        }
        all
    }
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

/// The constraints the frozen 2.x.x release adds with `NOT VALID` and never
/// validates, as (table, constraint name): 001 adds its credit-policy CHECK
/// that way on a `qbit_share_ledger` upgraded from before the column
/// existed, and a fresh apply creates the same constraint validated inside
/// `CREATE TABLE`. These are accepted in either validation state; every
/// other release constraint must be validated in the source, because a
/// `NOT VALID` foreign key or CHECK was never checked against the rows that
/// were there when it was added. The list is pinned to the release: a test
/// in `tests/support/ledger_2x.rs` derives the same set from the frozen 001
/// and 002 fixtures and fails if the two differ.
pub const NOT_VALID_EXEMPT: &[(&str, &str)] =
    &[("qbit_share_ledger", "qbit_share_ledger_credit_policy_check")];

fn not_valid_exempt(table: &str, name: &str) -> bool {
    NOT_VALID_EXEMPT
        .iter()
        .any(|(exempt_table, exempt_name)| *exempt_table == table && *exempt_name == name)
}

/// The key a constraint is compared under: its rendered definition without
/// the `NOT VALID` suffix, so a constraint that is not validated still finds
/// its release counterpart by definition. Validation itself is compared from
/// `pg_constraint.convalidated`, not from this text.
fn constraint_key(definition: &str) -> String {
    definition
        .strip_suffix(" NOT VALID")
        .unwrap_or(definition)
        .to_owned()
}

/// A constraint's name, whether PostgreSQL has checked every existing row
/// against it (`convalidated`), and the enabled state (`pg_trigger.tgenabled`,
/// sorted) of each internal trigger that enforces it: the four referential
/// triggers of a foreign key, on both of its tables, or the recheck trigger
/// of a deferrable unique constraint; none for a CHECK or an ordinary
/// primary key. `ALTER TABLE ... DISABLE TRIGGER` leaves the definition and
/// `convalidated` as they were while no new row is checked against the
/// constraint, so those states are part of it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ConstraintDefinition {
    name: String,
    validated: bool,
    enforcement: Vec<String>,
}

/// The enabled states of a constraint's enforcement triggers, counted:
/// `4 enabled`, `2 disabled, 2 enabled`, or `none`.
fn enforcement_summary(states: &[String]) -> String {
    if states.is_empty() {
        return "none".to_owned();
    }
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for state in states {
        *counts.entry(trigger_state(state)).or_default() += 1;
    }
    counts
        .iter()
        .map(|(state, count)| format!("{count} {state}"))
        .collect::<Vec<_>>()
        .join(", ")
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
    let rows = sqlx::query("SELECT c.relname::text AS table_name,c.relpersistence::text AS persistence,c.relrowsecurity AS row_security,c.relforcerowsecurity AS force_row_security,(SELECT coalesce(array_agg(i.inhparent::regclass::text ORDER BY i.inhseqno),'{}') FROM pg_inherits i WHERE i.inhrelid=c.oid) AS parents,(SELECT coalesce(array_agg(i.inhrelid::regclass::text ORDER BY i.inhrelid::regclass::text),'{}') FROM pg_inherits i WHERE i.inhparent=c.oid) AS children FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relkind='r' ORDER BY 1")
        .bind(namespace).fetch_all(&mut **tx).await?;
    for row in &rows {
        let parents: Vec<String> = row.try_get("parents")?;
        let children: Vec<String> = row.try_get("children")?;
        fingerprint.tables.insert(
            row.try_get("table_name")?,
            TableDefinition {
                persistence: row.try_get("persistence")?,
                row_security: row.try_get("row_security")?,
                force_row_security: row.try_get("force_row_security")?,
                parents: parents
                    .iter()
                    .map(|name| strip_schema_qualification(name, namespace))
                    .collect(),
                children: children
                    .iter()
                    .map(|name| strip_schema_qualification(name, namespace))
                    .collect(),
                columns: BTreeMap::new(),
            },
        );
    }
    let rows = sqlx::query("SELECT c.relname::text AS table_name,a.attname::text AS column_name,format_type(a.atttypid,a.atttypmod) AS data_type,(SELECT typtype='d' FROM pg_type WHERE oid=a.atttypid) AS is_domain,a.attnotnull AS not_null,pg_get_expr(d.adbin,d.adrelid) AS default_expr,a.attidentity::text AS identity,a.attgenerated::text AS generated,CASE WHEN a.attcollation<>t.typcollation THEN col.collname::text END AS collation FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped JOIN pg_type t ON t.oid=a.atttypid LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum LEFT JOIN pg_collation col ON col.oid=a.attcollation WHERE n.nspname=$1 AND c.relkind='r' ORDER BY 1,2")
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
                is_domain: row.try_get("is_domain")?,
                not_null: row.try_get("not_null")?,
                default: default.map(|expr| strip_schema_qualification(&expr, namespace)),
                identity: row.try_get("identity")?,
                generated: row.try_get("generated")?,
                collation: row.try_get("collation")?,
            },
        );
    }
    let rows = sqlx::query("SELECT c.relname::text AS table_name,k.conname::text AS name,pg_get_constraintdef(k.oid) AS definition,k.convalidated AS validated,(SELECT coalesce(array_agg(t.tgenabled::text ORDER BY t.tgenabled),'{}') FROM pg_trigger t WHERE t.tgconstraint=k.oid AND t.tgisinternal) AS enforcement FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relkind='r' ORDER BY 1,2")
        .bind(namespace).fetch_all(&mut **tx).await?;
    for row in &rows {
        let table: String = row.try_get("table_name")?;
        let definition: String = row.try_get("definition")?;
        fingerprint
            .constraints
            .entry(table)
            .or_default()
            .entry(constraint_key(&strip_schema_qualification(
                &definition,
                namespace,
            )))
            .or_insert(ConstraintDefinition {
                name: row.try_get("name")?,
                validated: row.try_get("validated")?,
                enforcement: row.try_get("enforcement")?,
            });
    }
    let rows = sqlx::query("SELECT CASE WHEN owner_ns.nspname=$1 THEN owner.relname::text ELSE quote_ident(owner_ns.nspname)||'.'||quote_ident(owner.relname) END AS owner,owner_ns.nspname=$1 AS owner_in_schema,k.conname::text AS name,target.relname::text AS referenced_table FROM pg_constraint k JOIN pg_class owner ON owner.oid=k.conrelid JOIN pg_namespace owner_ns ON owner_ns.oid=owner.relnamespace JOIN pg_class target ON target.oid=k.confrelid JOIN pg_namespace target_ns ON target_ns.oid=target.relnamespace WHERE k.contype='f' AND target_ns.nspname=$1 ORDER BY 1,3")
        .bind(namespace).fetch_all(&mut **tx).await?;
    for row in &rows {
        fingerprint.incoming_foreign_keys.push(IncomingForeignKey {
            owner: row.try_get("owner")?,
            owner_in_schema: row.try_get("owner_in_schema")?,
            name: row.try_get("name")?,
            referenced_table: row.try_get("referenced_table")?,
        });
    }
    let rows = sqlx::query("SELECT c.relname::text AS table_name,i.relname::text AS name,pg_get_indexdef(x.indexrelid) AS definition,x.indisvalid AS valid,x.indisunique AS unique,x.indexprs IS NOT NULL AS expression,x.indpred IS NOT NULL AS partial FROM pg_index x JOIN pg_class i ON i.oid=x.indexrelid JOIN pg_class c ON c.oid=x.indrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relkind='r' AND NOT EXISTS(SELECT 1 FROM pg_constraint k WHERE k.conindid=x.indexrelid AND k.contype IN ('p','u','x')) ORDER BY 1,2")
        .bind(namespace).fetch_all(&mut **tx).await?;
    for row in &rows {
        let definition: String = row.try_get("definition")?;
        fingerprint.indexes.insert(
            row.try_get("name")?,
            IndexDefinition {
                table: row.try_get("table_name")?,
                definition: strip_schema_qualification(&definition, namespace),
                valid: row.try_get("valid")?,
                unique: row.try_get("unique")?,
                expression: row.try_get("expression")?,
                partial: row.try_get("partial")?,
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
    let rows = sqlx::query("SELECT c.relname::text AS table_name,r.rulename::text AS name,pg_get_ruledef(r.oid) AS definition,r.ev_enabled::text AS enabled FROM pg_rewrite r JOIN pg_class c ON c.oid=r.ev_class JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 ORDER BY 1,2")
        .bind(namespace).fetch_all(&mut **tx).await?;
    for row in &rows {
        let definition: String = row.try_get("definition")?;
        fingerprint.rules.insert(
            (row.try_get("table_name")?, row.try_get("name")?),
            RuleDefinition {
                definition: strip_schema_qualification(&definition, namespace),
                enabled: row.try_get("enabled")?,
            },
        );
    }
    let rows = sqlx::query("SELECT c.relname::text AS table_name,p.polname::text AS name,p.polcmd::text AS command,p.polpermissive AS permissive,(SELECT coalesce(array_agg(CASE WHEN u.role_oid=0 THEN 'PUBLIC' ELSE pg_get_userbyid(u.role_oid)::text END),'{}') FROM unnest(p.polroles) AS u(role_oid))::text[] AS roles,pg_get_expr(p.polqual,p.polrelid) AS using_expr,pg_get_expr(p.polwithcheck,p.polrelid) AS with_check FROM pg_policy p JOIN pg_class c ON c.oid=p.polrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 ORDER BY 1,2")
        .bind(namespace).fetch_all(&mut **tx).await?;
    for row in &rows {
        let mut roles: Vec<String> = row.try_get("roles")?;
        roles.sort();
        let using: Option<String> = row.try_get("using_expr")?;
        let with_check: Option<String> = row.try_get("with_check")?;
        fingerprint.policies.insert(
            (row.try_get("table_name")?, row.try_get("name")?),
            PolicyDefinition {
                command: row.try_get("command")?,
                permissive: row.try_get("permissive")?,
                roles,
                using: using.map(|expr| strip_schema_qualification(&expr, namespace)),
                with_check: with_check.map(|expr| strip_schema_qualification(&expr, namespace)),
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
    // Every other relation that holds a name: the constraint-backed indexes
    // the index reading left out, and the kinds no map above models. TOAST
    // tables live in pg_toast and never here.
    let rows = sqlx::query("SELECT c.relname::text AS name,c.relkind::text AS kind,(SELECT t.relname::text FROM pg_index x JOIN pg_class t ON t.oid=x.indrelid WHERE x.indexrelid=c.oid) AS table_name,(SELECT k.conname::text FROM pg_constraint k WHERE k.conindid=c.oid AND k.contype IN ('p','u','x') ORDER BY k.conname LIMIT 1) AS constraint_name FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relkind IN ('i','I','v','m','p','f','c') ORDER BY 1")
        .bind(namespace).fetch_all(&mut **tx).await?;
    for row in &rows {
        let name: String = row.try_get("name")?;
        let kind: String = row.try_get("kind")?;
        let table: Option<String> = row.try_get("table_name")?;
        let constraint: Option<String> = row.try_get("constraint_name")?;
        let mut constraint_table = None;
        let description = match (kind.as_str(), table, constraint) {
            ("i" | "I", Some(table), Some(constraint)) => {
                let description =
                    format!("index {name} backing constraint {constraint} on {table}");
                constraint_table = Some(table);
                description
            }
            // A plain index is in `indexes`; a partitioned one is not.
            ("i", _, None) => continue,
            ("I", Some(table), None) => format!("partitioned index {name} on {table}"),
            ("v", ..) => format!("view {name}"),
            ("m", ..) => format!("materialized view {name}"),
            ("p", ..) => format!("partitioned table {name}"),
            ("f", ..) => format!("foreign table {name}"),
            ("c", ..) => format!("composite type {name}"),
            (kind, ..) => format!("relation {name} of kind {kind}"),
        };
        fingerprint.other_relations.insert(
            name,
            OtherRelation {
                description,
                constraint_table,
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

/// A table's two row-level security flags the way `ALTER TABLE` sets them.
/// FORCE without ENABLE is a state PostgreSQL keeps (it takes effect once
/// security is enabled) and is named as such.
fn row_security_state(table: &TableDefinition) -> &'static str {
    match (table.row_security, table.force_row_security) {
        (false, false) => "disabled",
        (true, false) => "enabled",
        (true, true) => "enabled and forced",
        (false, true) => "disabled but forced",
    }
}

/// A table's inheritance relations, one way: `no parent table`, or
/// `parent table(s) a, b`.
fn inheritance(kind: &str, names: &[String]) -> String {
    if names.is_empty() {
        format!("no {kind} table")
    } else {
        format!("{kind} table(s) {}", names.join(", "))
    }
}

/// What differs between two tables apart from their columns: persistence,
/// row-level security and inheritance.
fn table_differences(expected: &TableDefinition, found: &TableDefinition) -> String {
    let mut parts = Vec::new();
    if expected.persistence != found.persistence {
        parts.push(format!(
            "expected {}, found {}",
            persistence(&expected.persistence),
            persistence(&found.persistence)
        ));
    }
    if (expected.row_security, expected.force_row_security)
        != (found.row_security, found.force_row_security)
    {
        parts.push(format!(
            "expected row-level security {}, found {}",
            row_security_state(expected),
            row_security_state(found)
        ));
    }
    for (kind, expected_names, found_names) in [
        ("parent", &expected.parents, &found.parents),
        ("child", &expected.children, &found.children),
    ] {
        if expected_names != found_names {
            parts.push(format!(
                "expected {}, found {}",
                inheritance(kind, expected_names),
                inheritance(kind, found_names)
            ));
        }
    }
    parts.join(", ")
}

/// `pg_policy.polcmd` the way `CREATE POLICY ... FOR` spells it.
fn policy_command(code: &str) -> &str {
    match code {
        "r" => "SELECT",
        "a" => "INSERT",
        "w" => "UPDATE",
        "d" => "DELETE",
        "*" => "ALL",
        other => other,
    }
}

/// A policy the way `CREATE POLICY` would state it after its name and
/// table: the defaults (permissive, `TO PUBLIC`) are left out, so the text
/// names what the policy actually restricts.
fn policy_text(policy: &PolicyDefinition) -> String {
    let mut text = String::new();
    if !policy.permissive {
        text.push_str("AS RESTRICTIVE ");
    }
    text.push_str("FOR ");
    text.push_str(policy_command(&policy.command));
    if policy.roles != ["PUBLIC"] {
        text.push_str(" TO ");
        text.push_str(&policy.roles.join(", "));
    }
    if let Some(using) = &policy.using {
        text.push_str(&format!(" USING ({using})"));
    }
    if let Some(with_check) = &policy.with_check {
        text.push_str(&format!(" WITH CHECK ({with_check})"));
    }
    text
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
/// release table must have the release's row-level security flags, and
/// exactly the release's constraints, policies and triggers: an additional
/// constraint, policy or trigger on a release table is drift, not extra.
/// An extra constraint can reject native writes that satisfy the release
/// schema. A policy changes which rows the migrator and the native writers
/// see rather than adding to the schema; a trigger fires
/// on the rows they write and can refuse them (one that rejects
/// `writer_epoch = 0` lets the legacy writer's leased epochs through and
/// fails every native share insert, which writes epoch 0) or rewrite them.
/// Policies and triggers on tables the release does not create are not
/// reported. A release constraint must be validated in the source unless
/// it is one of `NOT_VALID_EXEMPT`. Extra columns must allow native inserts
/// to omit them without evaluating unverified expressions: only plain nullable
/// extra columns may stay; defaults, identities and generated columns are drift.
/// All extra indexes on release tables are drift, even when not query-valid:
/// a plain index can evaluate native writes through its access method or
/// operator class, just as unique, expression and partial indexes can.
/// A sequence is compared by its structure only: the value it has reached
/// is the source's data.
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
        let differences = table_differences(definition, found_table);
        if !differences.is_empty() {
            comparison
                .drift
                .push(format!("table {table} differs: {differences}"));
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
        for (column, definition) in found_columns {
            if !columns.contains_key(column) {
                if definition.is_domain {
                    comparison.drift.push(format!(
                        "column {table}.{column} has an extra domain type; domain constraints or defaults can affect omitted native values"
                    ));
                } else if definition.default.is_some()
                    || !definition.identity.is_empty()
                    || !definition.generated.is_empty()
                {
                    comparison.drift.push(format!(
                        "column {table}.{column} has an extra default, identity or generated expression; native writes can evaluate it"
                    ));
                } else if definition.not_null {
                    comparison.drift.push(format!(
                        "column {table}.{column} is an extra NOT NULL column without a default, identity or generated expression; native inserts omit it"
                    ));
                } else {
                    comparison.extra.push(format!("column {table}.{column}"));
                }
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
        for (definition, constraint) in constraints {
            let Some(actual) = found_constraints.get(definition) else {
                comparison.drift.push(format!(
                    "missing constraint {} on {table}: {definition}",
                    constraint.name
                ));
                continue;
            };
            // The release validates it; the source never checked its rows
            // against it. Only the pinned release exemptions are accepted
            // in either state.
            if constraint.validated
                && !actual.validated
                && !not_valid_exempt(table, &constraint.name)
            {
                comparison.drift.push(format!(
                    "constraint {} on {table} is NOT VALID; the release validates it",
                    actual.name
                ));
            }
            // The release enforces it; a disabled enforcement trigger
            // checks no new row against it, whatever the definition and
            // `convalidated` say, and 001 never re-enables one.
            if actual.enforcement != constraint.enforcement {
                comparison.drift.push(format!(
                    "constraint {} on {table} differs: enforcement triggers expected {}, found {}",
                    actual.name,
                    enforcement_summary(&constraint.enforcement),
                    enforcement_summary(&actual.enforcement)
                ));
            }
        }
    }
    for (table, constraints) in &found.constraints {
        if !expected.tables.contains_key(table) {
            continue;
        }
        let expected_constraints = expected.constraints.get(table).unwrap_or(&empty);
        for (definition, constraint) in constraints {
            if !expected_constraints.contains_key(definition) {
                comparison.drift.push(format!(
                    "constraint {} on {table}: {definition}; the release does not create it",
                    constraint.name
                ));
            }
        }
    }
    for foreign_key in &found.incoming_foreign_keys {
        if expected.tables.contains_key(&foreign_key.referenced_table)
            && !(foreign_key.owner_in_schema && expected.tables.contains_key(&foreign_key.owner))
        {
            comparison.drift.push(format!(
                "foreign key {} on {} references release table {}; its enforcement can affect native updates or deletes",
                foreign_key.name, foreign_key.owner, foreign_key.referenced_table
            ));
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
            let mut effects: Vec<_> = [
                (index.unique, "unique"),
                (index.expression, "expression"),
                (index.partial, "partial"),
            ]
            .into_iter()
            .filter_map(|(present, effect)| present.then_some(effect))
            .collect();
            if effects.is_empty() {
                effects.push("plain");
            }
            comparison.drift.push(format!(
                "index {name} on {} is an extra {} index; it can constrain or evaluate native writes",
                index.table, effects.join(", ")
            ));
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
    for ((table, name), trigger) in &found.triggers {
        if !expected.tables.contains_key(table)
            || expected
                .triggers
                .contains_key(&(table.clone(), name.clone()))
        {
            continue;
        }
        // Not an extra: it can refuse or rewrite rows the migrator and the
        // native writers put in a release table.
        let release_trigger_on_table = expected
            .triggers
            .keys()
            .any(|(release_table, _)| release_table == table);
        comparison.drift.push(format!(
            "trigger {name} on {table}: {}; the release {}",
            trigger.definition,
            if release_trigger_on_table {
                "does not create it"
            } else {
                "has no trigger on this table"
            }
        ));
    }
    for ((table, name), rule) in &expected.rules {
        if !found.tables.contains_key(table) {
            continue;
        }
        match found.rules.get(&(table.clone(), name.clone())) {
            None => comparison
                .drift
                .push(format!("missing rule {name} on {table}")),
            Some(actual) if actual != rule => comparison.drift.push(format!(
                "rule {name} on {table} differs: expected {} (enabled {}), found {} (enabled {})",
                rule.definition, rule.enabled, actual.definition, actual.enabled
            )),
            Some(_) => {}
        }
    }
    for ((table, name), rule) in &found.rules {
        if expected.tables.contains_key(table)
            && !expected.rules.contains_key(&(table.clone(), name.clone()))
        {
            comparison.drift.push(format!(
                "rule {name} on {table}: {}; the release does not create it",
                rule.definition
            ));
        }
    }
    for ((table, name), policy) in &expected.policies {
        if !found.tables.contains_key(table) {
            continue;
        }
        match found.policies.get(&(table.clone(), name.clone())) {
            None => comparison.drift.push(format!(
                "missing policy {name} on {table}: {}",
                policy_text(policy)
            )),
            Some(actual) if actual != policy => comparison.drift.push(format!(
                "policy {name} on {table} differs: expected {}, found {}",
                policy_text(policy),
                policy_text(actual)
            )),
            Some(_) => {}
        }
    }
    for ((table, name), policy) in &found.policies {
        if !expected.tables.contains_key(table)
            || expected
                .policies
                .contains_key(&(table.clone(), name.clone()))
        {
            continue;
        }
        let release_policy_on_table = expected
            .policies
            .keys()
            .any(|(release_table, _)| release_table == table);
        comparison.drift.push(format!(
            "policy {name} on {table}: {}; the release {}",
            policy_text(policy),
            if release_policy_on_table {
                "does not create it"
            } else {
                "has no row-level security policy on this table"
            }
        ));
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

/// The native migrations in the order `migrate_schema` applies them, each
/// with the version it records. The scratch apply of `release_fingerprint`
/// runs the same files in the same order, so the reserved set is derived
/// from what they create, never from a hand-maintained list.
const NATIVE_MIGRATIONS: &[(i32, &str)] = &[
    (2, include_str!("../../migrations/002_multi_instance.sql")),
    (3, include_str!("../../migrations/003_2x_compatibility.sql")),
    (
        4,
        include_str!("../../migrations/004_cpfp_retired_funding.sql"),
    ),
    (
        5,
        include_str!("../../migrations/005_candidate_dispatch.sql"),
    ),
    (6, include_str!("../../migrations/006_source_schema.sql")),
    (
        7,
        include_str!("../../migrations/007_candidate_window_reference.sql"),
    ),
    (
        8,
        include_str!("../../migrations/008_prepared_window_reference.sql"),
    ),
    (
        9,
        include_str!("../../migrations/009_wrap_safe_sessions.sql"),
    ),
    (
        10,
        include_str!("../../migrations/010_fatal_state_recovery.sql"),
    ),
    (
        11,
        include_str!("../../migrations/011_offer_before_landing.sql"),
    ),
];

/// The SQL of one native migration, by the version it records.
fn native_migration(version: i32) -> &'static str {
    NATIVE_MIGRATIONS
        .iter()
        .find(|(recorded, _)| *recorded == version)
        .map(|(_, sql)| *sql)
        .unwrap_or_else(|| panic!("migration {version} is not in NATIVE_MIGRATIONS"))
}

/// What the scratch apply established before any DDL touches the source.
struct ReleaseDefinitions {
    /// The frozen release's objects: 001, plus 002 for a #258 source.
    release: SchemaFingerprint,
    /// What the native migrations create and the release does not; a
    /// source that already has any of it is refused.
    reserved: ReservedObjects,
    /// Objects introduced by each requested, missing native migration.
    native_gaps: BTreeMap<i32, ReservedObjects>,
    /// The schema the source lives in.
    source_schema: String,
}

/// What the native migrations create and the release does not: the second
/// reading of the scratch schema minus the first.
#[derive(Debug, Default, PartialEq, Eq)]
struct ReservedObjects {
    /// The tables, sequences, indexes, triggers and functions whose names
    /// the native migrations take.
    objects: SchemaFingerprint,
    /// Per release table, the columns the native migrations add to it with
    /// `ADD COLUMN IF NOT EXISTS`, which would keep a column of that name
    /// whatever its type or default. Kept apart from `objects`: the
    /// partial-001 check shares `objects_present`, refuses on a present
    /// table, and would only be cluttered by that table's columns.
    columns: BTreeMap<String, BTreeSet<String>>,
    /// Named constraints added to existing tables, whose IF NOT EXISTS
    /// guards also preserve an unrelated constraint under the same name.
    constraints: BTreeMap<String, BTreeSet<String>>,
}

/// The frozen release's definitions, the objects the native migrations
/// reserve, and the name of the schema the source lives in. The definitions
/// come from applying the release SQL (001, plus 002 for a #258 source) to
/// a scratch schema inside this transaction, and then the native migrations
/// in order, under a savepoint that is rolled back before anything else
/// happens, so they are exact for this server's PostgreSQL version and
/// nothing from the scratch apply survives. Taken once, before any DDL
/// touches the source, and used to decide whether a database without
/// `qbit_share_ledger` is really fresh, whether a native object or column
/// is already present, and, after 001 has run, to check what it left alone.
async fn release_fingerprint(
    tx: &mut Transaction<'_, Postgres>,
    state: SourceState,
    base_schema: &str,
    missing_native_versions: &[i32],
) -> Result<ReleaseDefinitions> {
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
    let mut native_gaps = BTreeMap::new();
    for (version, sql) in NATIVE_MIGRATIONS {
        let before = if missing_native_versions.contains(version) {
            Some(fingerprint_schema(tx, &scratch).await?)
        } else {
            None
        };
        sqlx::raw_sql(sql).execute(&mut **tx).await?;
        if let Some(before) = before {
            let after = fingerprint_schema(tx, &scratch).await?;
            let mut reserved = reserved_objects(&after, &before);
            // These migrations leave new tables' constraint indexes and
            // identity/serial sequences unnamed. PostgreSQL chooses another
            // name if an operator's renamed table retained the default one;
            // unlike explicit CREATE names, those names cannot cause a skip.
            reserved.objects.other_relations.retain(|_, relation| {
                !relation
                    .constraint_table
                    .as_ref()
                    .is_some_and(|table| reserved.objects.tables.contains_key(table))
            });
            let owned_sequences: Vec<(String, String)> = sqlx::query_as(
                "SELECT s.relname::text,t.relname::text FROM pg_class s JOIN pg_depend d ON d.classid='pg_class'::regclass AND d.objid=s.oid JOIN pg_class t ON d.refclassid='pg_class'::regclass AND d.refobjid=t.oid WHERE s.relnamespace=$1::regnamespace AND s.relkind='S' AND d.refobjsubid>0 AND d.deptype IN ('a','i')",
            )
            .bind(&scratch)
            .fetch_all(&mut **tx)
            .await?;
            for (sequence, table) in owned_sequences {
                if reserved.objects.tables.contains_key(&table) {
                    reserved.objects.sequences.remove(&sequence);
                }
            }
            native_gaps.insert(*version, reserved);
        }
    }
    let native = fingerprint_schema(tx, &scratch).await?;
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
    let reserved = reserved_objects(&native, &expected);
    Ok(ReleaseDefinitions {
        release: expected,
        reserved,
        native_gaps,
        source_schema,
    })
}

/// The tables, sequences, indexes, triggers and functions the native
/// migrations create and the release does not, and, per release table, the
/// columns they add to it. An object in both sets (the capability table on
/// a #258 source, a function 003 re-asserts) stays governed by the release
/// checks, and so does a column in both: the outbox's `storage_version` on
/// a #258 source, where the release 002 added it (on a pre-#258 source 006
/// adds it, so it is reserved there). `qbit_prism_schema_migrations` is
/// the migrator's own, created before any source object is read, and is
/// never reserved.
fn reserved_objects(native: &SchemaFingerprint, release: &SchemaFingerprint) -> ReservedObjects {
    let mut objects = SchemaFingerprint::default();
    let mut columns = BTreeMap::new();
    let mut constraints = BTreeMap::new();
    for (table, definition) in &native.tables {
        if table == "qbit_prism_schema_migrations" {
            continue;
        }
        match release.tables.get(table) {
            None => {
                objects.tables.insert(table.clone(), definition.clone());
            }
            Some(released) => {
                let added: BTreeSet<String> = definition
                    .columns
                    .keys()
                    .filter(|column| !released.columns.contains_key(*column))
                    .cloned()
                    .collect();
                if !added.is_empty() {
                    columns.insert(table.clone(), added);
                }
            }
        }
    }
    for (name, definition) in &native.sequences {
        if !release.sequences.contains_key(name) {
            objects.sequences.insert(name.clone(), definition.clone());
        }
    }
    for (name, definition) in &native.indexes {
        if !release.indexes.contains_key(name) {
            objects.indexes.insert(name.clone(), definition.clone());
        }
    }
    for (key, definition) in &native.triggers {
        if !release.triggers.contains_key(key) {
            objects.triggers.insert(key.clone(), definition.clone());
        }
    }
    for (key, definition) in &native.functions {
        if !release.functions.contains_key(key) {
            objects.functions.insert(key.clone(), definition.clone());
        }
    }
    for (name, relation) in &native.other_relations {
        if !release.other_relations.contains_key(name) {
            objects
                .other_relations
                .insert(name.clone(), relation.clone());
        }
    }
    for (table, definitions) in &native.constraints {
        if !release.tables.contains_key(table) {
            continue;
        }
        let existing: BTreeSet<&str> = release
            .constraints
            .get(table)
            .into_iter()
            .flat_map(|definitions| {
                definitions
                    .values()
                    .map(|definition| definition.name.as_str())
            })
            .collect();
        let added: BTreeSet<String> = definitions
            .values()
            .filter(|definition| !existing.contains(definition.name.as_str()))
            .map(|definition| definition.name.clone())
            .collect();
        if !added.is_empty() {
            constraints.insert(table.clone(), added);
        }
    }
    ReservedObjects {
        objects,
        columns,
        constraints,
    }
}

/// A recorded native migration owns its objects, but a missing step must
/// not silently adopt a pre-existing object. Derive each missing step's
/// names from the same scratch replay used for fresh/2.x sources. Start
/// with the #258 release: its capability table and storage_version column
/// may legitimately predate 006, whose dedicated gates validate them.
async fn require_no_native_gap_collisions(
    tx: &mut Transaction<'_, Postgres>,
    versions: &[i32],
) -> Result<()> {
    let missing: Vec<i32> = NATIVE_MIGRATIONS
        .iter()
        .map(|(version, _)| *version)
        .filter(|version| !versions.contains(version))
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    let base_schema =
        base_schema_transaction_body(include_str!("../../../qbit-prism/sql/001_share_ledger.sql"))?;
    let definitions =
        release_fingerprint(tx, SourceState::Applied258, &base_schema, &missing).await?;
    let found = source_fingerprint(tx, &definitions.source_schema).await?;
    for (version, reserved) in definitions.native_gaps {
        let mut present = objects_present(&reserved.objects, &found);
        present.extend(columns_present(&reserved.columns, &found));
        for (table, names) in &reserved.constraints {
            if let Some(constraints) = found.constraints.get(table) {
                for definition in constraints.values() {
                    if names.contains(&definition.name) {
                        present.push(format!("constraint {} on {table}", definition.name));
                    }
                }
            }
        }
        ensure!(
            present.is_empty(),
            "refusing to repair native migration {version} before any DDL: the database already holds objects this missing migration creates ({}), so applying it could preserve objects it did not build. Nothing was changed. Restore the full backup, or review and move the existing objects aside before migrating again",
            named_objects(&present)
        );
    }
    Ok(())
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
    found
        .other_relations
        .remove("qbit_prism_schema_migrations_pkey");
    Ok(found)
}

/// Every table, sequence, index, trigger and function of `expected` that
/// the source already has, named the way the drift report names them. A
/// relation's name is taken whatever kind of relation holds it, because
/// `IF NOT EXISTS` looks at the name alone: a view under a table's name, or
/// an index backing an operator's constraint under an index's name, is
/// present and is named by what holds the name. An index backing a
/// constraint of an expected table goes with that table, which is named on
/// its own when present, so it is not named twice.
fn objects_present(expected: &SchemaFingerprint, found: &SchemaFingerprint) -> Vec<String> {
    let found_relations = found.relations();
    let mut present = Vec::new();
    let names = expected
        .tables
        .keys()
        .chain(expected.sequences.keys())
        .chain(expected.indexes.keys())
        .chain(expected.other_relations.keys());
    for name in names {
        let Some((description, constraint_table)) = found_relations.get(name.as_str()) else {
            continue;
        };
        if constraint_table.is_some_and(|table| expected.tables.contains_key(table)) {
            continue;
        }
        present.push(description.clone());
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
fn require_fresh_source(expected: &SchemaFingerprint, found: &SchemaFingerprint) -> Result<()> {
    let present = objects_present(expected, found);
    ensure!(
        present.is_empty(),
        "refusing to migrate a {STATE_PARTIAL_001} source before any DDL: the database has no qbit_share_ledger but holds {} object(s) that the 2.x.x release's 001_share_ledger.sql creates ({}), so it is neither an empty database nor a 2.x.x ledger, and 001's IF NOT EXISTS would keep those objects whatever they hold. Nothing was changed. Restore the full pre-migration backup, or migrate into an empty database",
        present.len(),
        named_objects(&present)
    );
    let comparison = compare_fingerprints(expected, found);
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

/// Every reserved column that the source's release table already has,
/// named the way the drift report names columns.
fn columns_present(
    reserved: &BTreeMap<String, BTreeSet<String>>,
    found: &SchemaFingerprint,
) -> Vec<String> {
    let mut present = Vec::new();
    for (table, columns) in reserved {
        let Some(found_table) = found.tables.get(table) else {
            continue;
        };
        for column in columns {
            if found_table.columns.contains_key(column) {
                present.push(format!("column {table}.{column}"));
            }
        }
    }
    present
}

/// A table, sequence, index, trigger, function or column that a native
/// migration creates and the release does not must not be there yet, on a
/// 2.x.x source or an empty database alike: a native migration's `IF NOT
/// EXISTS` would keep it whatever it holds, the migration would record a
/// schema it did not build, and the writers would fail only afterwards. A
/// reserved relation name held by a relation of another kind, a view or an
/// index backing an operator's constraint, is the same collision: `IF NOT
/// EXISTS` would skip the native object for it. Refused before any DDL,
/// naming the objects. Nothing is dropped: what such an object holds is
/// the operator's to judge.
fn require_no_native_collision(
    state: SourceState,
    reserved: &ReservedObjects,
    found: &SchemaFingerprint,
) -> Result<()> {
    let mut present = objects_present(&reserved.objects, found);
    present.extend(columns_present(&reserved.columns, found));
    ensure!(
        present.is_empty(),
        "refusing to migrate a {STATE_NATIVE_COLLISION} source before any DDL: the {} already holds {} object(s) that the native migrations create and the 2.x.x release does not ({}), so a native migration's IF NOT EXISTS would keep each such table, sequence, index, trigger, function or column whatever it holds, or skip its own object where a relation of another kind holds the name, and the migration would record a schema it did not build. Nothing was changed. Restore the full pre-migration backup, or check what those objects hold and remove them yourself, then migrate again",
        match state {
            SourceState::Fresh => "empty database",
            _ => "2.x.x database",
        },
        present.len(),
        named_objects(&present)
    );
    Ok(())
}

/// After 001 has run, refuse anything its `IF NOT EXISTS` left alone that
/// differs from the frozen release, whose definitions `release_fingerprint`
/// took before any DDL. On a fresh database 001 just created everything,
/// so this passes trivially; it runs there too, as a second guard. Extra
/// objects, columns, constraints, indexes and sequences are kept and
/// logged; a missing or different one, row-level security on a release
/// table, or a policy or trigger the release does not create on one, fails
/// the migration, which rolls back whole, so the database is unchanged.
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
        policies = expected.policies.len(),
        functions = expected.functions.len(),
        sequences = expected.sequences.len(),
        extra = comparison.extra.len(),
        "source schema matches the 2.x.x release"
    );
    Ok(())
}

/// Version rows are trusted only after their storage and behavior are checked.
/// Read catalogs before querying rows so RLS, views and rewrite rules cannot
/// influence source classification. Native history permits no extra behavior.
async fn require_migration_history(connection: &mut sqlx::PgConnection) -> Result<()> {
    let valid: bool = sqlx::query_scalar(
        r#"SELECT EXISTS (
            SELECT 1 FROM pg_class c JOIN pg_am am ON am.oid=c.relam
            WHERE c.oid=to_regclass('qbit_prism_schema_migrations')
              AND c.relnamespace=current_schema()::regnamespace
              AND c.relkind='r' AND c.relpersistence='p' AND am.amname='heap'
              AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
              AND NOT EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid)
              AND NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid=c.oid)
              AND NOT EXISTS (SELECT 1 FROM pg_rewrite WHERE ev_class=c.oid)
              AND NOT EXISTS (SELECT 1 FROM pg_policy WHERE polrelid=c.oid)
              AND (SELECT count(*) FROM pg_attribute WHERE attrelid=c.oid AND attnum>0 AND NOT attisdropped)=2
              AND EXISTS (
                  SELECT 1 FROM pg_attribute a
                  WHERE a.attrelid=c.oid AND a.attname='version' AND NOT a.attisdropped
                    AND a.atttypid='pg_catalog.int4'::regtype AND a.atttypmod=-1
                    AND a.attnotnull AND a.attidentity='' AND a.attgenerated=''
                    AND NOT EXISTS (SELECT 1 FROM pg_attrdef WHERE adrelid=c.oid AND adnum=a.attnum)
              )
              AND EXISTS (
                  SELECT 1 FROM pg_attribute a JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
                  WHERE a.attrelid=c.oid AND a.attname='applied_at' AND NOT a.attisdropped
                    AND a.atttypid='pg_catalog.timestamptz'::regtype AND a.atttypmod=-1
                    AND a.attnotnull AND a.attidentity='' AND a.attgenerated=''
                    AND pg_get_expr(d.adbin,d.adrelid) IN ('clock_timestamp()', 'pg_catalog.clock_timestamp()')
                    AND NOT EXISTS (
                        SELECT 1 FROM pg_depend dep WHERE dep.classid='pg_attrdef'::regclass
                          AND dep.objid=d.oid AND dep.refclassid='pg_proc'::regclass
                          AND dep.refobjid<>'pg_catalog.clock_timestamp()'::regprocedure
                    )
              )
              AND (SELECT count(*) FROM pg_constraint WHERE conrelid=c.oid OR confrelid=c.oid)=1
              AND (SELECT count(*) FROM pg_index WHERE indrelid=c.oid)=1
              AND EXISTS (
                  SELECT 1 FROM pg_constraint k JOIN pg_index i ON i.indexrelid=k.conindid
                  JOIN pg_attribute a ON a.attrelid=c.oid AND a.attname='version' AND NOT a.attisdropped
                  JOIN pg_opclass op ON op.oid=i.indclass[0]
                  JOIN pg_am iam ON iam.oid=op.opcmethod
                  WHERE k.conrelid=c.oid AND k.contype='p' AND k.convalidated
                    AND NOT k.condeferrable AND NOT k.condeferred AND k.conkey=ARRAY[a.attnum]
                    AND i.indrelid=c.oid AND i.indisprimary AND i.indisunique
                    AND i.indisvalid AND i.indisready AND i.indislive
                    AND i.indnatts=1 AND i.indnkeyatts=1 AND i.indkey[0]=a.attnum
                    AND i.indexprs IS NULL AND i.indpred IS NULL
                    AND op.opcnamespace='pg_catalog'::regnamespace
                    AND op.opcname='int4_ops' AND iam.amname='btree'
              )
        )"#,
    ).fetch_one(connection).await?;
    ensure!(valid,
        "refusing qbit_prism_schema_migrations before any DDL: migration history must have the native logged-table, version primary-key and applied_at definitions, without extra columns, constraints, indexes, triggers, rules, inheritance or row security. Nothing was changed. Restore the full backup or review and repair the history table before starting or migrating again"
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
    metrics: Option<&crate::metrics::Metrics>,
) -> Result<()> {
    lock(tx, MIGRATION_LOCK, metrics).await?;
    require_source_schema_resolution(tx).await?;
    let history_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('qbit_prism_schema_migrations') IS NOT NULL")
            .fetch_one(&mut **tx)
            .await?;
    if history_exists {
        sqlx::query("LOCK TABLE ONLY qbit_prism_schema_migrations IN ACCESS EXCLUSIVE MODE")
            .execute(&mut **tx)
            .await
            .context("refusing qbit_prism_schema_migrations before any DDL: cannot lock the migration-history table for validation")?;
        require_migration_history(&mut *tx).await?;
    } else {
        sqlx::raw_sql("CREATE TABLE qbit_prism_schema_migrations(version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT clock_timestamp())").execute(&mut **tx).await?;
    }
    // Each applied migration is tracked on its own, not as a high-water mark:
    // 007 is reserved by an independent workstream, so a later number must
    // not hide an earlier gap. Every step runs when its own version is
    // missing, in order.
    let versions: Vec<i32> =
        sqlx::query_scalar("SELECT version FROM qbit_prism_schema_migrations ORDER BY version")
            .fetch_all(&mut **tx)
            .await?;
    ensure!(
        !history_exists || versions.contains(&3),
        "refusing a non-native qbit_prism_schema_migrations before any DDL: a fresh or 2.x.x source cannot already contain native migration history without migration 3. Nothing was changed. Restore the full backup, or review and move the pre-existing object aside before migrating again"
    );
    // The highest migration recorded before this run, which the source
    // record keeps as `prior_schema_version`.
    let prior_version = versions.iter().copied().max().unwrap_or(0);
    // What 006 records when it runs: the accepted 2.x.x source state, or
    // `None` for a database that was already native, and the
    // `candidate_storage_version` the database declared before 006 declares
    // one for it.
    let mut source: (Option<SourceState>, Option<i32>) = (None, None);
    if !versions.contains(&3) {
        // Existing native writers use this same lock order. Keep the
        // schema repair and cutover atomic with their accounting.
        lock(tx, SETTLEMENT_LOCK, metrics).await?;
        lock(tx, ORDER_LOCK, metrics).await?;
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
        // The release definitions and the reserved native objects, taken
        // once under a savepoint and rolled back before the source is
        // touched.
        let ReleaseDefinitions {
            release: expected,
            reserved,
            source_schema,
            ..
        } = release_fingerprint(tx, state, &base_schema, &[]).await?;
        let found = source_fingerprint(tx, &source_schema).await?;
        if state == SourceState::Fresh {
            // No share ledger and no 002 object: fresh only if nothing else
            // of 001 is there either, or 001 would keep it as it is.
            require_fresh_source(&expected, &found)?;
        }
        // Nothing a native migration creates may be there yet, or its IF
        // NOT EXISTS would keep it as it is.
        require_no_native_collision(state, &reserved, &found)?;
        refuse_undrained_outbox(tx, &inventory, None).await?;
        sqlx::raw_sql(&base_schema).execute(&mut **tx).await?;
        // 001 repaired what it re-asserts; what it skipped must already be
        // the release definition before any native DDL alters those tables.
        require_release_schema(tx, state, &expected, &source_schema).await?;
        if !versions.contains(&2) {
            sqlx::raw_sql(native_migration(2))
                .execute(&mut **tx)
                .await?;
            sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(2)")
                .execute(&mut **tx)
                .await?;
        }
        sqlx::raw_sql(native_migration(3))
            .execute(&mut **tx)
            .await?;
        sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(3)")
            .execute(&mut **tx)
            .await?;
        source = (
            Some(state),
            inventory.capability("candidate_storage_version"),
        );
    } else {
        // A native database, one an earlier 3.x.x build migrated. Its
        // capability rows are refused first, before any DDL, exactly as
        // `classify_source` refuses them on a 2.x.x source: otherwise 004,
        // 005 and 006, or 008 and 009, would alter a database a newer
        // release wrote and record their versions, and only the connect-time
        // gate, after the commit, would refuse it. A record with 3 and not
        // 2, which no native build writes, is refused first: 004 to 009
        // must not run above a record every start refuses and no migrate
        // repairs.
        refuse_inconsistent_native_record(&versions)?;
        let inventory = inspect_source_schema(tx).await?;
        // A database at 6 that no longer declares its capabilities is
        // refused before 008 or 009 run above it, as connect refuses it.
        refuse_undeclared_native_database(&versions, &inventory)?;
        refuse_newer_native_database(&versions, &inventory)?;
        if versions.contains(&6) {
            // Startup reads this record after commit. Check it with the same
            // decoder before any later migration can be applied or recorded.
            if let Err(reason) = require_migration_source(&mut **tx).await {
                bail!(
                    "refusing to migrate a native database at schema migrations {} before any DDL: {reason:#}",
                    schema_version_list(&versions)
                );
            }
        }
        if !versions.contains(&6) {
            let source_absent: bool =
                sqlx::query_scalar("SELECT to_regclass('qbit_prism_migration_source') IS NULL")
                    .fetch_one(&mut **tx)
                    .await?;
            ensure!(
                source_absent,
                "refusing to migrate a native database at schema migrations {} before any DDL: qbit_prism_migration_source exists without migration 6, so 006 would preserve metadata it did not create or verify. Nothing was changed. Restore the full backup, or review and move the existing object aside before migrating again",
                schema_version_list(&versions)
            );
            // Native schema 3, 4 or 5, with or without 008 and 009. That
            // build's drain check used the v1-only predicate, which never
            // counted a v2 row (`candidate ?& ...` is NULL for a NULL body),
            // so a pending v2 candidate can still be there. The column-aware
            // check runs here, before 004, 005 or 006 touch anything, so a
            // refusal on this path is before any DDL too.
            require_pre_006_storage_version(tx, &versions).await?;
            refuse_undrained_outbox(tx, &inventory, Some(&versions)).await?;
            source = (None, inventory.capability("candidate_storage_version"));
        }
        require_no_native_gap_collisions(tx, &versions).await?;
    }
    if !versions.contains(&4) {
        sqlx::raw_sql(native_migration(4))
            .execute(&mut **tx)
            .await?;
        sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(4)")
            .execute(&mut **tx)
            .await?;
    }
    if !versions.contains(&5) {
        sqlx::raw_sql(native_migration(5))
            .execute(&mut **tx)
            .await?;
        sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(5)")
            .execute(&mut **tx)
            .await?;
    }
    if !versions.contains(&6) {
        // 006 declares native runtime version 1; the record keeps the source
        // declaration from before cutover, read with the inventory above.
        let (state, capability) = source;
        sqlx::raw_sql(native_migration(6))
            .execute(&mut **tx)
            .await?;
        require_known_capabilities(&mut **tx).await?;
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
    if !versions.contains(&7) {
        // 007's own drain check, which is not the one `refuse_undrained_outbox`
        // performs and cannot be folded into it. That helper exists to refuse a
        // row this server cannot decode at all, and its predicate deliberately
        // passes a native pending row carrying `payout_revision`, `bundle` and
        // `block_hash`: for 006 such a row is perfectly readable. 007 is what
        // removes the inline claim path, so for 007 that very row is the
        // blocking shape. Refusing it here, before any of 007's DDL runs,
        // leaves the schema exactly as it was.
        //
        // `candidate IS NULL` and `storage_version <> 1` are #258's chunked
        // version-2 shape, named both ways because native schemas have no
        // `storage_version` column; the disjunct is added only where it exists.
        let versioned: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='qbit_block_candidate_outbox' AND column_name='storage_version')",
        )
        .fetch_one(&mut **tx)
        .await?;
        let undrained: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT block_hash FROM qbit_block_candidate_outbox WHERE state='pending' AND (candidate IS NULL OR candidate ? 'bundle'{}) ORDER BY block_hash",
            if versioned { " OR storage_version<>1" } else { "" }
        ))
        .fetch_all(&mut **tx)
        .await?;
        ensure!(
            undrained.is_empty(),
            "migration 007 refuses undrained pre-007 block candidates: {}. \
             Drain the outbox with the pre-007 frontends running, then stop every \
             frontend, verify that SELECT count(*) FROM qbit_block_candidate_outbox \
             WHERE state='pending' is 0, and only then start the post-007 binary",
            named_objects(&undrained)
        );
        sqlx::raw_sql(native_migration(7))
            .execute(&mut **tx)
            .await?;
        sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(7)")
            .execute(&mut **tx)
            .await?;
    }
    if !versions.contains(&8) {
        sqlx::raw_sql(native_migration(8))
            .execute(&mut **tx)
            .await?;
        sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(8)")
            .execute(&mut **tx)
            .await?;
    }
    if !versions.contains(&9) {
        sqlx::raw_sql(native_migration(9))
            .execute(&mut **tx)
            .await?;
        sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(9)")
            .execute(&mut **tx)
            .await?;
    }
    if !versions.contains(&10) {
        sqlx::raw_sql(native_migration(10))
            .execute(&mut **tx)
            .await?;
        sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(10)")
            .execute(&mut **tx)
            .await?;
    }
    if !versions.contains(&11) {
        refuse_unquiesced_outbox(tx).await?;
        sqlx::raw_sql(native_migration(11))
            .execute(&mut **tx)
            .await?;
        sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(11)")
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

/// 011's own quiesce check, before any of its DDL runs. Every registered
/// instance must explicitly report shutdown, even if it holds no claim.
/// A pending row whose
/// claim is still live belongs to a pre-011 frontend that may be mid-offer:
/// the reservation lifecycle cannot take over a row an old frontend may
/// still send. And 011 quarantines every attempted pending row as an
/// unknown-delivery reconciliation row, which must carry the evidence the
/// lifecycle payload rule requires (the document, the block bytes and the
/// window reference); a parked chunked row or a pre-007 row cannot be
/// quarantined and is refused by name, exactly as 007 refused it.
async fn refuse_unquiesced_outbox(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    // Claim expiry proves nothing about an idle or paused frontend. Use the
    // same explicit shutdown markers as fatal-state recovery, without a
    // heartbeat-age cutoff. Serialize the scan with heartbeat registration
    // and retain the lock until the capability change commits.
    sqlx::query("LOCK TABLE qbit_prism_instances IN SHARE ROW EXCLUSIVE MODE")
        .execute(&mut **tx)
        .await?;
    let instances: Vec<String> = sqlx::query_scalar(
        "SELECT instance_id FROM qbit_prism_instances WHERE COALESCE(status->>'state','') NOT IN ('drained','stopped') ORDER BY instance_id",
    )
    .fetch_all(&mut **tx)
    .await?;
    ensure!(
        instances.is_empty(),
        "migration 011 requires every pre-011 instance to report drained or stopped; offending instances: {}. \
         Stop every pre-011 frontend gracefully and disable automatic restarts before migrating. \
         An empty outbox or an expired heartbeat does not prove shutdown; nothing was changed",
        named_objects(&instances)
    );
    let outbox: bool =
        sqlx::query_scalar("SELECT to_regclass('qbit_block_candidate_outbox') IS NOT NULL")
            .fetch_one(&mut **tx)
            .await?;
    if !outbox {
        return Ok(());
    }
    let live: Vec<String> = sqlx::query_scalar(
        "SELECT block_hash FROM qbit_block_candidate_outbox WHERE state='pending' AND claim_expires_at>clock_timestamp() ORDER BY block_hash",
    )
    .fetch_all(&mut **tx)
    .await?;
    ensure!(
        live.is_empty(),
        "migration 011 refuses to run while pending block candidates hold a live claim: {}. \
         A pre-011 frontend may be offering them. Stop every pre-011 frontend, wait for their \
         claims to expire (at most 120 s), then migrate again; nothing was changed",
        named_objects(&live)
    );
    let bare: Vec<String> = sqlx::query_scalar(
        "SELECT block_hash FROM qbit_block_candidate_outbox WHERE state='pending' AND attempt_count>0 AND (candidate IS NULL OR block_bytes IS NULL OR window_anchor_ms IS NULL OR window_prior_balances_sha256 IS NULL) ORDER BY block_hash",
    )
    .fetch_all(&mut **tx)
    .await?;
    ensure!(
        bare.is_empty(),
        "migration 011 refuses undrained attempted block candidates without a block and window reference: {}. \
         They were attempted by a pre-007 frontend or parked as chunked rows, and 011 cannot quarantine them. \
         Drain them with the frontend that wrote them (or the 2.x.x image for a parked chunked row), stop it, \
         then migrate again; nothing was changed",
        named_objects(&bare)
    );
    Ok(())
}

/// The startup gate. Every start, with or without `initialize`, reads the
/// applied migrations and refuses a database missing any of
/// `REQUIRED_SCHEMA_VERSIONS`, naming the gap. A migration this binary does
/// not know is accepted with a warning that names it, so frontends on the
/// previous release keep starting while a rollout drains and replaces them
/// one at a time; a format an older binary must not touch is declared as a
/// capability instead.
pub(super) async fn require_schema_version(pool: &PgPool) -> Result<()> {
    let recorded: bool =
        sqlx::query_scalar("SELECT to_regclass('qbit_prism_schema_migrations') IS NOT NULL")
            .fetch_one(pool)
            .await?;
    ensure!(
        recorded,
        "database has no native PRISM schema (qbit_prism_schema_migrations is missing) and this server requires schema migrations {}: run `qbit-prism-server migrate`, or start with PRISM_POSTGRES_INIT_SCHEMA=1, after draining the 2.x.x deployment",
        schema_version_list(REQUIRED_SCHEMA_VERSIONS)
    );
    let mut connection = pool.acquire().await?;
    require_migration_history(&mut connection).await?;
    let applied: Vec<i32> =
        sqlx::query_scalar("SELECT version FROM qbit_prism_schema_migrations ORDER BY version")
            .fetch_all(&mut *connection)
            .await?;
    let missing: Vec<i32> = REQUIRED_SCHEMA_VERSIONS
        .iter()
        .copied()
        .filter(|version| !applied.contains(version))
        .collect();
    ensure!(
        missing.is_empty(),
        "database schema is missing migration(s) {}; this server requires {} and found {}: run `qbit-prism-server migrate` with this release, or start with PRISM_POSTGRES_INIT_SCHEMA=1",
        schema_version_list(&missing),
        schema_version_list(REQUIRED_SCHEMA_VERSIONS),
        schema_version_list(&applied)
    );
    let unknown: Vec<i32> = applied
        .iter()
        .copied()
        .filter(|version| !REQUIRED_SCHEMA_VERSIONS.contains(version))
        .collect();
    if !unknown.is_empty() {
        tracing::warn!(
            unknown_migrations = %schema_version_list(&unknown),
            required_migrations = %schema_version_list(REQUIRED_SCHEMA_VERSIONS),
            "database has schema migrations this server does not know; a later release applied them"
        );
    }
    Ok(())
}

/// The connect-time gate: the database must declare the capabilities its
/// recorded migrations declared (006's storage version, and 011's offer
/// lifecycle once 11 is recorded), and every capability or storage version
/// it declares must be one the binary understands. Every start runs it,
/// with or without `initialize`, after `require_schema_version` has
/// established that 006 ran in current_schema(), so a missing table or row
/// is a dropped or deleted declaration, never a legacy state, and a table
/// resolved from another schema is refused rather than read;
/// `migrate_schema` refused the same database before any DDL, and runs this
/// again once 006 has declared a database it is migrating. The recorded
/// migrations are read on the connection the rows are read on, in the same
/// schema.
pub(super) async fn require_known_capabilities<'e, E>(executor: E) -> Result<()>
where
    E: sqlx::Acquire<'e, Database = Postgres>,
{
    let mut connection = executor.acquire().await?;
    let versions: Vec<i32> =
        sqlx::query_scalar("SELECT version FROM qbit_prism_schema_migrations ORDER BY version")
            .fetch_all(&mut *connection)
            .await?;
    let rows = read_capabilities(&mut *connection).await?;
    require_declared_capabilities(rows.as_deref(), &versions)?;
    refuse_unknown_capabilities(rows.as_deref().unwrap_or_default(), NATIVE_CAPABILITIES)
}

async fn read_migration_source<'e, E>(executor: E) -> Result<Option<MigrationSource>>
where
    E: sqlx::Acquire<'e, Database = Postgres>,
{
    let mut connection = executor.acquire().await?;
    read_migration_source_connection(&mut connection).await
}

// The concrete reader keeps migration_source's future Send for spawned callers;
// passing its borrowed checkout through generic Acquire fails that bound.
async fn read_migration_source_connection(
    connection: &mut sqlx::PgConnection,
) -> Result<Option<MigrationSource>> {
    // Keep resolution and the read on one connection, and qualify the read
    // with the verified schema so provenance cannot come from another ledger.
    let relation = sqlx::query("SELECT n.nspname::text AS schema,current_schema()::text AS current,c.relkind::text AS kind,format('%I.%I',n.nspname,c.relname) AS qualified FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=to_regclass('qbit_prism_migration_source')")
        .fetch_optional(&mut *connection).await?
        .context("qbit_prism_migration_source is missing from the current schema")?;
    let schema: String = relation.try_get("schema")?;
    let current: Option<String> = relation.try_get("current")?;
    ensure!(
        current.as_deref() == Some(schema.as_str()),
        "qbit_prism_migration_source resolves to {schema}.qbit_prism_migration_source, outside the current schema {} whose migration history was verified; refusing to trust another schema's migration provenance. Restore the full backup, including the current schema's original source metadata table and singleton row, then start or migrate again",
        current.as_deref().unwrap_or("(none)")
    );
    let kind: String = relation.try_get("kind")?;
    ensure!(
        kind == "r",
        "qbit_prism_migration_source must be an ordinary table (found relation kind {kind}); restore the original source metadata table from the full backup before starting or migrating this database"
    );
    let qualified: String = relation.try_get("qualified")?;
    let row = sqlx::query(&format!("SELECT source_state,source_release,source_commit,candidate_storage_version,prior_schema_version,migrated_by,migrated_at FROM {qualified} WHERE singleton"))
        .fetch_optional(&mut *connection).await?;
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

/// Once 006 is recorded, its source table and readable singleton row must
/// exist. Never reconstruct provenance from the already-migrated database.
/// Migrate checks inside its transaction before later DDL; startup checks
/// again before any accounting statement, with or without initialization.
pub(super) async fn require_migration_source<'e, E>(executor: E) -> Result<MigrationSource>
where
    E: sqlx::Acquire<'e, Database = Postgres>,
{
    read_migration_source(executor)
        .await
        .context("database is at schema migration 6 but qbit_prism_migration_source cannot be read. Restore the full backup, including the source metadata table and its original singleton row, then start or migrate again")?
        .context("database is at schema migration 6 but qbit_prism_migration_source has no singleton row. Restore the full backup, including the original source metadata row, then start or migrate again")
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
        let mut connection = self.acquire().await?;
        read_migration_source_connection(&mut connection).await
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
                .bind(&cursor).fetch_optional(&mut *self.acquire().await?).await?;
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
            let (metadata, canonical_bytes) =
                tokio::task::spawn_blocking(move || -> Result<(ImportedAudit, Vec<u8>)> {
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
                    // The parsed window is dropped here, off the runtime
                    // threads and before the write transaction opens.
                    Ok((
                        ImportedAudit {
                            schema: bundle.schema,
                            network_difficulty: bundle.found_block.network_difficulty.to_string(),
                            coinbase_value_sats: i64::try_from(
                                bundle.found_block.coinbase_value_sats,
                            )?,
                            audit_commitment_leaves_hex: serde_json::to_value(
                                &bundle.audit_commitment_leaves_hex,
                            )?,
                            witness_merkle_leaves_hex: serde_json::to_value(
                                &bundle.witness_merkle_leaves_hex,
                            )?,
                        },
                        canonical_bytes,
                    ))
                })
                .await??;
            let mut tx = self.begin().await?;
            self.lock(&mut tx, SETTLEMENT_LOCK).await?;
            writable(&mut tx).await?;
            // Store the exact canonical bytes and the non-share metadata only;
            // readers decode the bytes. A two-copy inline JSONB body would cross
            // PostgreSQL's container limit at production window sizes. Inline
            // bodies already stored on rows without a body_uri are left as they
            // are: the body-present CHECK needs one of the two.
            let updated = sqlx::query("UPDATE qbit_pool_audit_bundles SET schema_version=$2,found_block_network_difficulty=$3::text::numeric,found_block_coinbase_value_sats=$4,audit_commitment_leaves_hex=$5,witness_merkle_leaves_hex=$6,canonical_audit_bytes=$9 WHERE block_hash=$1 AND canonical_audit_bytes IS NULL AND body_uri IS NOT DISTINCT FROM $7 AND audit_bundle_sha256=$8")
                .bind(&hash).bind(&metadata.schema).bind(&metadata.network_difficulty).bind(metadata.coinbase_value_sats)
                .bind(&metadata.audit_commitment_leaves_hex).bind(&metadata.witness_merkle_leaves_hex).bind(&uri).bind(expected_digest).bind(canonical_bytes).execute(&mut *tx).await?.rows_affected();
            tx.commit().await?;
            imported += usize::try_from(updated)?;
        }
        Ok(imported)
    }

    /// Recover missing sets or individual fanouts from trusted audit evidence.
    /// Return the number of blocks repaired; matching existing rows are no-ops.
    pub async fn backfill_ctv(&self, ledger_key: &str) -> Result<usize> {
        let rows = sqlx::query("SELECT block_hash,audit_bundle_sha256,coinbase_tx_hex FROM qbit_pool_audit_bundles ORDER BY created_at,block_hash").fetch_all(&mut *self.acquire().await?).await?;
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
            let mut tx = self.begin().await?;
            self.lock(&mut tx, SETTLEMENT_LOCK).await?;
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

/// The non-share metadata the legacy import stores beside an audit's
/// canonical bytes, so the parsed window need not outlive verification.
struct ImportedAudit {
    schema: String,
    network_difficulty: String,
    coinbase_value_sats: i64,
    audit_commitment_leaves_hex: Value,
    witness_merkle_leaves_hex: Value,
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
            is_domain: false,
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
            row_security: false,
            force_row_security: false,
            parents: Vec::new(),
            children: Vec::new(),
            columns: columns
                .iter()
                .map(|(name, definition)| ((*name).to_owned(), definition.clone()))
                .collect(),
        }
    }

    fn function(language: &str, body: &str) -> FunctionDefinition {
        FunctionDefinition {
            arguments: String::new(),
            result: None,
            language: language.to_owned(),
            body: Some(body.to_owned()),
            volatility: "v".into(),
            strict: false,
            security_definer: false,
            leakproof: false,
            parallel: "u".into(),
            kind: "f".into(),
            config: Vec::new(),
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
            constraint_key("CHECK ((a > 0)) NOT VALID"),
            "CHECK ((a > 0))"
        );
        assert_eq!(constraint_key("CHECK ((a > 0))"), "CHECK ((a > 0))");
    }

    /// A relation of another kind: a view, say.
    fn other(description: &str) -> OtherRelation {
        OtherRelation {
            description: description.to_owned(),
            constraint_table: None,
        }
    }

    /// The index backing `constraint` on `table`, under `name`.
    fn backing_index(name: &str, constraint: &str, table: &str) -> OtherRelation {
        OtherRelation {
            description: format!("index {name} backing constraint {constraint} on {table}"),
            constraint_table: Some(table.to_owned()),
        }
    }

    fn constraint(name: &str, validated: bool) -> ConstraintDefinition {
        ConstraintDefinition {
            name: name.to_owned(),
            validated,
            enforcement: Vec::new(),
        }
    }

    /// A foreign key with its four referential triggers in `states`.
    fn foreign_key(name: &str, states: &str) -> ConstraintDefinition {
        ConstraintDefinition {
            name: name.to_owned(),
            validated: true,
            enforcement: states.chars().map(String::from).collect(),
        }
    }

    #[test]
    fn inheritance_involving_a_release_table_is_drift_and_among_extras_is_not() {
        let mut expected = SchemaFingerprint::default();
        expected.tables.insert(
            "qbit_share_ledger".into(),
            table(&[("share_id", column("bigint", true))]),
        );
        let mut found = SchemaFingerprint {
            tables: expected.tables.clone(),
            ..SchemaFingerprint::default()
        };
        // A child created with INHERITS: an extra table on its own, and
        // the release table it changes is drift, naming it.
        let mut child = table(&[("share_id", column("bigint", true))]);
        child.parents = vec!["qbit_share_ledger".into()];
        found.tables.insert("qbit_share_ledger_2025".into(), child);
        found.tables.get_mut("qbit_share_ledger").unwrap().children =
            vec!["qbit_share_ledger_2025".into()];
        let comparison = compare_fingerprints(&expected, &found);
        assert_eq!(
            comparison.drift,
            vec!["table qbit_share_ledger differs: expected no child table, found child table(s) qbit_share_ledger_2025"]
        );
        assert_eq!(comparison.extra, vec!["table qbit_share_ledger_2025"]);
        // The release table made to inherit from, or attached as a
        // partition of, a table in another schema.
        found.tables.remove("qbit_share_ledger_2025");
        let ledger = found.tables.get_mut("qbit_share_ledger").unwrap();
        ledger.children.clear();
        ledger.parents = vec!["archive.ledgers".into()];
        let comparison = compare_fingerprints(&expected, &found);
        assert_eq!(
            comparison.drift,
            vec!["table qbit_share_ledger differs: expected no parent table, found parent table(s) archive.ledgers"]
        );
        assert!(comparison.extra.is_empty(), "{:?}", comparison.extra);
        // Inheritance among the operator's own tables is theirs to keep.
        found
            .tables
            .get_mut("qbit_share_ledger")
            .unwrap()
            .parents
            .clear();
        let mut parent = table(&[("note", column("text", true))]);
        parent.children = vec!["operator_notes_2025".into()];
        let mut child = table(&[("note", column("text", true))]);
        child.parents = vec!["operator_notes".into()];
        found.tables.insert("operator_notes".into(), parent);
        found.tables.insert("operator_notes_2025".into(), child);
        let comparison = compare_fingerprints(&expected, &found);
        assert!(comparison.drift.is_empty(), "{:?}", comparison.drift);
        assert_eq!(
            comparison.extra,
            vec!["table operator_notes", "table operator_notes_2025"]
        );
    }

    #[test]
    fn disabled_enforcement_triggers_are_drift_for_a_release_constraint() {
        let definition = "FOREIGN KEY (share_id) REFERENCES qbit_share_ledger(share_id)";
        let mut expected = SchemaFingerprint::default();
        expected.tables.insert(
            "qbit_block_candidate_outbox".into(),
            table(&[("share_id", column("text", false))]),
        );
        expected
            .constraints
            .entry("qbit_block_candidate_outbox".into())
            .or_default()
            .insert(
                definition.into(),
                foreign_key("qbit_block_candidate_outbox_share_id_fkey", "OOOO"),
            );
        let mut found = SchemaFingerprint {
            tables: expected.tables.clone(),
            ..SchemaFingerprint::default()
        };
        let outbox = found
            .constraints
            .entry("qbit_block_candidate_outbox".into())
            .or_default();
        // The same definition, validated, under the source's name, with
        // every referential trigger disabled: drift, counted by state.
        outbox.insert(definition.into(), foreign_key("outbox_share_fk", "DDDD"));
        let comparison = compare_fingerprints(&expected, &found);
        assert_eq!(
            comparison.drift,
            vec!["constraint outbox_share_fk on qbit_block_candidate_outbox differs: enforcement triggers expected 4 enabled, found 4 disabled"]
        );
        // Half of them, on one of its tables; and one set to fire on
        // replicas only, which never fires on a primary.
        let outbox = found
            .constraints
            .get_mut("qbit_block_candidate_outbox")
            .unwrap();
        outbox.insert(definition.into(), foreign_key("outbox_share_fk", "DDOO"));
        let comparison = compare_fingerprints(&expected, &found);
        assert_eq!(
            comparison.drift,
            vec!["constraint outbox_share_fk on qbit_block_candidate_outbox differs: enforcement triggers expected 4 enabled, found 2 disabled, 2 enabled"]
        );
        let outbox = found
            .constraints
            .get_mut("qbit_block_candidate_outbox")
            .unwrap();
        outbox.insert(definition.into(), foreign_key("outbox_share_fk", "OOOR"));
        let comparison = compare_fingerprints(&expected, &found);
        assert_eq!(
            comparison.drift,
            vec!["constraint outbox_share_fk on qbit_block_candidate_outbox differs: enforcement triggers expected 4 enabled, found 3 enabled, 1 enabled on replicas only"]
        );
        // Disabled and NOT VALID at once: both are named.
        let outbox = found
            .constraints
            .get_mut("qbit_block_candidate_outbox")
            .unwrap();
        let mut both = foreign_key("outbox_share_fk", "DDDD");
        both.validated = false;
        outbox.insert(definition.into(), both);
        let comparison = compare_fingerprints(&expected, &found);
        assert_eq!(
            comparison.drift,
            vec![
                "constraint outbox_share_fk on qbit_block_candidate_outbox is NOT VALID; the release validates it",
                "constraint outbox_share_fk on qbit_block_candidate_outbox differs: enforcement triggers expected 4 enabled, found 4 disabled",
            ]
        );
        // Enabled again, the release constraint matches; an extra constraint
        // on its table is drift even with disabled enforcement triggers.
        let outbox = found
            .constraints
            .get_mut("qbit_block_candidate_outbox")
            .unwrap();
        outbox.insert(definition.into(), foreign_key("outbox_share_fk", "OOOO"));
        outbox.insert(
            "FOREIGN KEY (share_id) REFERENCES operator_shares(share_id)".into(),
            foreign_key("operator_fk", "DDDD"),
        );
        let comparison = compare_fingerprints(&expected, &found);
        assert_eq!(
            comparison.drift,
            vec!["constraint operator_fk on qbit_block_candidate_outbox: FOREIGN KEY (share_id) REFERENCES operator_shares(share_id); the release does not create it"]
        );
        assert!(comparison.extra.is_empty(), "{:?}", comparison.extra);
        assert_eq!(enforcement_summary(&[]), "none");
    }

    /// A user trigger as `pg_get_triggerdef` renders it, enabled.
    fn trigger(definition: &str) -> TriggerDefinition {
        TriggerDefinition {
            definition: definition.to_owned(),
            enabled: "O".into(),
        }
    }

    #[test]
    fn a_trigger_the_release_does_not_create_on_a_release_table_is_drift() {
        let release_sync = "CREATE TRIGGER qbit_pool_blocks_carry_forward_current_sync AFTER INSERT OR UPDATE OR DELETE ON qbit_pool_blocks FOR EACH ROW EXECUTE FUNCTION qbit_pool_blocks_carry_forward_current_sync()";
        let epoch_guard = "CREATE TRIGGER operator_epoch_guard BEFORE INSERT ON qbit_share_ledger FOR EACH ROW EXECUTE FUNCTION operator_reject_epoch_zero()";
        let blocks_audit = "CREATE TRIGGER operator_blocks_audit AFTER INSERT ON qbit_pool_blocks FOR EACH ROW EXECUTE FUNCTION operator_audit_blocks()";
        let sync_key = (
            "qbit_pool_blocks".to_owned(),
            "qbit_pool_blocks_carry_forward_current_sync".to_owned(),
        );
        let guard_key = (
            "qbit_share_ledger".to_owned(),
            "operator_epoch_guard".to_owned(),
        );
        let audit_key = (
            "qbit_pool_blocks".to_owned(),
            "operator_blocks_audit".to_owned(),
        );
        let mut expected = SchemaFingerprint::default();
        expected.tables.insert(
            "qbit_share_ledger".into(),
            table(&[("writer_epoch", column("bigint", true))]),
        );
        expected.tables.insert(
            "qbit_pool_blocks".into(),
            table(&[("block_hash", column("text", true))]),
        );
        expected
            .triggers
            .insert(sync_key.clone(), trigger(release_sync));
        // The release's own trigger; an operator's guard on the ledger,
        // which the release leaves without triggers; a second trigger on
        // the blocks table beside the release's; and a trigger on the
        // operator's own table. The two on release tables are drift, named
        // with what they run and why the release lacks them; the operator's
        // table is extra and its trigger is theirs.
        let mut found = SchemaFingerprint {
            tables: expected.tables.clone(),
            triggers: expected.triggers.clone(),
            ..SchemaFingerprint::default()
        };
        found.tables.insert(
            "operator_notes".into(),
            table(&[("note", column("text", true))]),
        );
        found
            .triggers
            .insert(guard_key.clone(), trigger(epoch_guard));
        found
            .triggers
            .insert(audit_key.clone(), trigger(blocks_audit));
        found.triggers.insert(
            ("operator_notes".into(), "operator_notes_stamp".into()),
            trigger("CREATE TRIGGER operator_notes_stamp BEFORE INSERT ON operator_notes FOR EACH ROW EXECUTE FUNCTION operator_notes_stamp()"),
        );
        let drift = vec![
            format!("trigger operator_blocks_audit on qbit_pool_blocks: {blocks_audit}; the release does not create it"),
            format!("trigger operator_epoch_guard on qbit_share_ledger: {epoch_guard}; the release has no trigger on this table"),
        ];
        let comparison = compare_fingerprints(&expected, &found);
        assert_eq!(comparison.drift, drift);
        assert_eq!(comparison.extra, vec!["table operator_notes"]);
        // Disabled, it is still not the release's table.
        found.triggers.get_mut(&guard_key).unwrap().enabled = "D".into();
        let comparison = compare_fingerprints(&expected, &found);
        assert_eq!(comparison.drift, drift);
        // A release trigger the source lacks is missing, named before the
        // ones the source should not have.
        found.triggers.remove(&sync_key);
        let comparison = compare_fingerprints(&expected, &found);
        assert_eq!(
            comparison.drift,
            [
                vec!["missing trigger qbit_pool_blocks_carry_forward_current_sync on qbit_pool_blocks".to_owned()],
                drift,
            ]
            .concat()
        );
        // The release's triggers and no other: nothing to report but the
        // operator's table.
        found.triggers.insert(sync_key, trigger(release_sync));
        found.triggers.remove(&guard_key);
        found.triggers.remove(&audit_key);
        let comparison = compare_fingerprints(&expected, &found);
        assert!(comparison.drift.is_empty(), "{:?}", comparison.drift);
        assert_eq!(comparison.extra, vec!["table operator_notes"]);
    }

    #[test]
    fn incoming_foreign_keys_from_nonrelease_tables_are_drift() {
        let mut expected = SchemaFingerprint::default();
        expected
            .tables
            .insert("t".into(), table(&[("a", column("bigint", true))]));
        let mut found = SchemaFingerprint {
            tables: expected.tables.clone(),
            ..SchemaFingerprint::default()
        };
        for (owner, local, target, drift) in [
            ("operator_notes", true, "t", true),
            ("other.t", false, "t", true),
            ("t", true, "t", false),
            ("operator_notes", true, "operator_other", false),
            ("other.notes", false, "operator_other", false),
        ] {
            found.incoming_foreign_keys = vec![IncomingForeignKey {
                owner: owner.into(),
                owner_in_schema: local,
                name: "operator_fk".into(),
                referenced_table: target.into(),
            }];
            let comparison = compare_fingerprints(&expected, &found);
            if drift {
                assert_eq!(comparison.drift, vec![format!("foreign key operator_fk on {owner} references release table {target}; its enforcement can affect native updates or deletes")]);
            } else {
                assert!(comparison.drift.is_empty(), "{:?}", comparison.drift);
            }
        }
    }

    #[test]
    fn rewrite_rules_are_compared_and_extra_release_table_rules_are_drift() {
        let mut expected = SchemaFingerprint::default();
        expected
            .tables
            .insert("t".into(), table(&[("a", column("bigint", true))]));
        let key = ("t".to_owned(), "operator_rule".to_owned());
        let rule = RuleDefinition {
            definition: "CREATE RULE operator_rule AS ON INSERT TO t DO INSTEAD NOTHING".into(),
            enabled: "O".into(),
        };
        let mut found = SchemaFingerprint {
            tables: expected.tables.clone(),
            ..SchemaFingerprint::default()
        };
        found
            .rules
            .insert(("operator_notes".into(), "own_rule".into()), rule.clone());
        assert!(compare_fingerprints(&expected, &found).drift.is_empty());
        for enabled in ["O", "D", "R", "A"] {
            found.rules.insert(
                key.clone(),
                RuleDefinition {
                    enabled: enabled.into(),
                    ..rule.clone()
                },
            );
            assert_eq!(
                compare_fingerprints(&expected, &found).drift,
                vec![format!(
                    "rule operator_rule on t: {}; the release does not create it",
                    rule.definition
                )]
            );
        }
        expected.rules.insert(key.clone(), rule.clone());
        found.rules.insert(key.clone(), rule.clone());
        assert!(compare_fingerprints(&expected, &found).drift.is_empty());
        found.rules.get_mut(&key).unwrap().enabled = "D".into();
        assert!(compare_fingerprints(&expected, &found).drift[0].contains("enabled D"));
        found.rules.insert(
            key.clone(),
            RuleDefinition {
                definition: "changed".into(),
                ..rule
            },
        );
        assert!(compare_fingerprints(&expected, &found).drift[0].contains("found changed"));
        found.rules.remove(&key);
        assert_eq!(
            compare_fingerprints(&expected, &found).drift,
            vec!["missing rule operator_rule on t"]
        );
    }

    #[test]
    fn extra_columns_must_allow_omission_without_executing_expressions() {
        let mut expected = SchemaFingerprint::default();
        expected
            .tables
            .insert("t".into(), table(&[("a", column("bigint", true))]));
        let mut found = SchemaFingerprint {
            tables: expected.tables.clone(),
            ..SchemaFingerprint::default()
        };
        let mut defaulted = column("bigint", false);
        defaulted.default = Some("(10 / 0)".into());
        let mut generated = column("bigint", false);
        generated.generated = "s".into();
        let mut identity = column("bigint", true);
        identity.identity = "a".into();
        for definition in [defaulted, generated, identity] {
            found
                .tables
                .get_mut("t")
                .unwrap()
                .columns
                .insert("extra".into(), definition.clone());
            found
                .tables
                .insert("operator_notes".into(), table(&[("extra", definition)]));
            let comparison = compare_fingerprints(&expected, &found);
            assert_eq!(comparison.drift, vec!["column t.extra has an extra default, identity or generated expression; native writes can evaluate it"]);
            assert_eq!(comparison.extra, vec!["table operator_notes"]);
        }
        let mut domain = column("operator_required", false);
        domain.is_domain = true;
        found
            .tables
            .get_mut("t")
            .unwrap()
            .columns
            .insert("extra".into(), domain);
        assert_eq!(compare_fingerprints(&expected, &found).drift,
            vec!["column t.extra has an extra domain type; domain constraints or defaults can affect omitted native values"]);
        found
            .tables
            .get_mut("t")
            .unwrap()
            .columns
            .insert("extra".into(), column("bigint", false));
        let comparison = compare_fingerprints(&expected, &found);
        assert!(comparison.drift.is_empty());
        assert_eq!(
            comparison.extra,
            vec!["column t.extra", "table operator_notes"]
        );
    }

    #[test]
    fn extra_indexes_that_can_constrain_or_evaluate_native_writes_are_drift() {
        let mut expected = SchemaFingerprint::default();
        expected.tables.insert(
            "qbit_share_ledger".into(),
            table(&[("writer_epoch", column("bigint", true))]),
        );
        let mut found = SchemaFingerprint {
            tables: expected.tables.clone(),
            ..SchemaFingerprint::default()
        };
        for (unique, expression, partial, reason) in [
            (true, false, false, "unique"),
            (false, true, false, "expression"),
            (false, false, true, "partial"),
            (true, true, true, "unique, expression, partial"),
            (false, false, false, "plain"),
        ] {
            for valid in [true, false] {
                found.indexes.insert(
                    "operator_epoch_idx".into(),
                    IndexDefinition {
                        table: "qbit_share_ledger".into(),
                        definition: "operator index".into(),
                        valid,
                        unique,
                        expression,
                        partial,
                    },
                );
                let comparison = compare_fingerprints(&expected, &found);
                assert_eq!(comparison.drift, vec![format!(
                    "index operator_epoch_idx on qbit_share_ledger is an extra {reason} index; it can constrain or evaluate native writes"
                )]);
                assert!(comparison.extra.is_empty(), "{:?}", comparison.extra);

                // The release's own indexes still compare by definition.
                expected.indexes = found.indexes.clone();
                found.indexes.get_mut("operator_epoch_idx").unwrap().valid = true;
                assert!(compare_fingerprints(&expected, &found).drift.is_empty());
                expected.indexes.clear();

                // Indexes on an operator-owned table do not govern native writes.
                found.indexes.get_mut("operator_epoch_idx").unwrap().table =
                    "operator_notes".into();
                assert!(compare_fingerprints(&expected, &found).drift.is_empty());
            }
        }
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
            .insert("CHECK ((a > 0))".into(), constraint("t_a_check", true));
        expected.indexes.insert(
            "t_b_idx".into(),
            IndexDefinition {
                table: "t".into(),
                definition: "CREATE INDEX t_b_idx ON t USING btree (b)".into(),
                valid: true,
                unique: false,
                expression: false,
                partial: false,
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
            .insert("CHECK ((a > 0))".into(), constraint("t_check1", true));
        found.indexes.insert(
            "t_b_idx".into(),
            IndexDefinition {
                table: "t".into(),
                definition: "CREATE INDEX t_b_idx ON t USING btree (b)".into(),
                valid: true,
                unique: false,
                expression: false,
                partial: false,
            },
        );
        found.indexes.insert(
            "t_extra_idx".into(),
            IndexDefinition {
                table: "t".into(),
                definition: "CREATE INDEX t_extra_idx ON t USING btree (extra)".into(),
                valid: true,
                unique: false,
                expression: false,
                partial: false,
            },
        );
        let comparison = compare_fingerprints(&expected, &found);
        assert_eq!(
            comparison.drift,
            vec![
                "missing table gone",
                "column t.a differs: type expected bigint, found numeric",
                "index t_extra_idx on t is an extra plain index; it can constrain or evaluate native writes",
            ]
        );
        assert_eq!(comparison.extra, vec!["column t.extra", "table other"]);

        // A matching source has nothing to report.
        found.indexes.remove("t_extra_idx");
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
            STATE_NATIVE_COLLISION,
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
        assert_eq!(SOURCE_STATES.len(), 8, "every row has a name constant");
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
                unique: false,
                expression: false,
                partial: false,
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
        assert!(objects_present(&expected, &found).is_empty());
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
                objects_present(&expected, &found),
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
            objects_present(&expected, &found),
            vec![
                "table qbit_pool_blocks",
                "index qbit_pool_blocks_maturity_idx on qbit_pool_blocks",
                "trigger qbit_pool_blocks_guard on qbit_pool_blocks",
            ]
        );
        // A release name held by a relation of another kind is a leftover
        // too: 001's IF NOT EXISTS looks at the name alone.
        let mut found = SchemaFingerprint::default();
        found
            .other_relations
            .insert("qbit_pool_blocks".into(), other("view qbit_pool_blocks"));
        found.indexes.insert(
            "qbit_audit_publication_sequence_seq".into(),
            IndexDefinition {
                table: "operator_notes".into(),
                definition: "CREATE INDEX qbit_audit_publication_sequence_seq ON operator_notes USING btree (note)".into(),
                valid: true,
                unique: false,
                expression: false,
                partial: false,
            },
        );
        assert_eq!(
            objects_present(&expected, &found),
            vec![
                "view qbit_pool_blocks",
                "index qbit_audit_publication_sequence_seq on operator_notes",
            ]
        );
    }

    #[test]
    fn a_reserved_name_held_by_a_relation_of_another_kind_is_a_collision() {
        let mut reserved = ReservedObjects::default();
        reserved.objects.tables.insert(
            "qbit_prism_jobs".into(),
            table(&[("job_id", column("text", true))]),
        );
        reserved.objects.sequences.insert(
            "qbit_prism_session_sequence".into(),
            sequence("bigint", 1, 4294967295),
        );
        reserved.objects.indexes.insert(
            "qbit_prism_candidate_claim_idx".into(),
            IndexDefinition {
                table: "qbit_block_candidate_outbox".into(),
                definition: "CREATE INDEX qbit_prism_candidate_claim_idx ON qbit_block_candidate_outbox USING btree (next_attempt_at)".into(),
                valid: true,
                unique: false,
                expression: false,
                partial: false,
            },
        );
        reserved.objects.other_relations.insert(
            "qbit_prism_jobs_pkey".into(),
            backing_index(
                "qbit_prism_jobs_pkey",
                "qbit_prism_jobs_pkey",
                "qbit_prism_jobs",
            ),
        );
        // A view under the table's name, a plain index under the sequence's,
        // an index backing an operator's constraint under the index's, and
        // a plain index under the primary key's: each named by what holds
        // the name, tables, sequences and indexes first.
        let mut found = SchemaFingerprint::default();
        found
            .other_relations
            .insert("qbit_prism_jobs".into(), other("view qbit_prism_jobs"));
        found.indexes.insert(
            "qbit_prism_session_sequence".into(),
            IndexDefinition {
                table: "operator_notes".into(),
                definition:
                    "CREATE INDEX qbit_prism_session_sequence ON operator_notes USING btree (note)"
                        .into(),
                valid: true,
                unique: false,
                expression: false,
                partial: false,
            },
        );
        found.other_relations.insert(
            "qbit_prism_candidate_claim_idx".into(),
            backing_index(
                "qbit_prism_candidate_claim_idx",
                "qbit_prism_candidate_claim_idx",
                "operator_notes",
            ),
        );
        found.indexes.insert(
            "qbit_prism_jobs_pkey".into(),
            IndexDefinition {
                table: "operator_notes".into(),
                definition:
                    "CREATE INDEX qbit_prism_jobs_pkey ON operator_notes USING btree (note_id)"
                        .into(),
                valid: true,
                unique: false,
                expression: false,
                partial: false,
            },
        );
        assert_eq!(
            objects_present(&reserved.objects, &found),
            vec![
                "view qbit_prism_jobs",
                "index qbit_prism_session_sequence on operator_notes",
                "index qbit_prism_candidate_claim_idx backing constraint qbit_prism_candidate_claim_idx on operator_notes",
                "index qbit_prism_jobs_pkey on operator_notes",
            ]
        );
        let error = require_no_native_collision(SourceState::Pre258, &reserved, &found)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("already holds 4 object(s)")
                && error.contains(
                    "skip its own object where a relation of another kind holds the name"
                ),
            "{error}"
        );
        // A stray native table is named once: the index backing its own
        // primary key goes with it.
        let mut found = SchemaFingerprint::default();
        found.tables.insert(
            "qbit_prism_jobs".into(),
            table(&[("job_id", column("text", true))]),
        );
        found.other_relations.insert(
            "qbit_prism_jobs_pkey".into(),
            backing_index(
                "qbit_prism_jobs_pkey",
                "qbit_prism_jobs_pkey",
                "qbit_prism_jobs",
            ),
        );
        assert_eq!(
            objects_present(&reserved.objects, &found),
            vec!["table qbit_prism_jobs"]
        );
        // The release's own constraint-backed index is in both readings and
        // never reserved; the native one is.
        let mut native = SchemaFingerprint::default();
        native.other_relations.insert(
            "qbit_share_ledger_pkey".into(),
            backing_index(
                "qbit_share_ledger_pkey",
                "qbit_share_ledger_pkey",
                "qbit_share_ledger",
            ),
        );
        native.other_relations.insert(
            "qbit_prism_jobs_pkey".into(),
            backing_index(
                "qbit_prism_jobs_pkey",
                "qbit_prism_jobs_pkey",
                "qbit_prism_jobs",
            ),
        );
        let mut release = SchemaFingerprint::default();
        release.other_relations.insert(
            "qbit_share_ledger_pkey".into(),
            backing_index(
                "qbit_share_ledger_pkey",
                "qbit_share_ledger_pkey",
                "qbit_share_ledger",
            ),
        );
        assert_eq!(
            reserved_objects(&native, &release)
                .objects
                .other_relations
                .keys()
                .collect::<Vec<_>>(),
            ["qbit_prism_jobs_pkey"]
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
    fn validation_state_is_compared_except_for_the_pinned_release_exemptions() {
        assert_eq!(
            NOT_VALID_EXEMPT,
            &[("qbit_share_ledger", "qbit_share_ledger_credit_policy_check")]
        );
        assert!(not_valid_exempt(
            "qbit_share_ledger",
            "qbit_share_ledger_credit_policy_check"
        ));
        assert!(!not_valid_exempt(
            "qbit_pool_blocks",
            "qbit_share_ledger_credit_policy_check"
        ));
        let mut expected = SchemaFingerprint::default();
        expected.tables.insert(
            "qbit_share_ledger".into(),
            table(&[("credit_policy", column("text", false))]),
        );
        expected.tables.insert(
            "qbit_block_candidate_outbox".into(),
            table(&[("share_id", column("text", false))]),
        );
        let exempt = "CHECK (((credit_policy IS NULL) OR (credit_policy = 'stale-grace'::text)))";
        let foreign_key = "FOREIGN KEY (share_id) REFERENCES qbit_share_ledger(share_id)";
        let check = "CHECK ((share_id <> ''::text))";
        expected
            .constraints
            .entry("qbit_share_ledger".into())
            .or_default()
            .insert(
                exempt.into(),
                constraint("qbit_share_ledger_credit_policy_check", true),
            );
        let outbox = expected
            .constraints
            .entry("qbit_block_candidate_outbox".into())
            .or_default();
        outbox.insert(
            foreign_key.into(),
            constraint("qbit_block_candidate_outbox_share_id_fkey", true),
        );
        outbox.insert(
            check.into(),
            constraint("qbit_block_candidate_outbox_share_id_check", true),
        );
        // The same definitions, none of them validated: the exempt CHECK is
        // accepted, the foreign key and the other CHECK are drift, named by
        // the name the source gives them.
        let mut found = SchemaFingerprint {
            tables: expected.tables.clone(),
            ..SchemaFingerprint::default()
        };
        found
            .constraints
            .entry("qbit_share_ledger".into())
            .or_default()
            .insert(
                exempt.into(),
                constraint("qbit_share_ledger_credit_policy_check", false),
            );
        let outbox = found
            .constraints
            .entry("qbit_block_candidate_outbox".into())
            .or_default();
        outbox.insert(
            foreign_key.into(),
            constraint("qbit_block_candidate_outbox_share_id_fkey", false),
        );
        outbox.insert(check.into(), constraint("outbox_share_id_check1", false));
        let comparison = compare_fingerprints(&expected, &found);
        assert_eq!(
            comparison.drift,
            vec![
                "constraint outbox_share_id_check1 on qbit_block_candidate_outbox is NOT VALID; the release validates it",
                "constraint qbit_block_candidate_outbox_share_id_fkey on qbit_block_candidate_outbox is NOT VALID; the release validates it",
            ]
        );
        assert!(comparison.extra.is_empty(), "{:?}", comparison.extra);

        // The exempt constraint in either state on either side: the scratch
        // apply may itself leave it NOT VALID.
        expected
            .constraints
            .get_mut("qbit_share_ledger")
            .unwrap()
            .get_mut(exempt)
            .unwrap()
            .validated = false;
        found
            .constraints
            .get_mut("qbit_share_ledger")
            .unwrap()
            .get_mut(exempt)
            .unwrap()
            .validated = true;
        // Validated in the source, the others are equivalent again.
        for constraint in found
            .constraints
            .get_mut("qbit_block_candidate_outbox")
            .unwrap()
            .values_mut()
        {
            constraint.validated = true;
        }
        let comparison = compare_fingerprints(&expected, &found);
        assert!(comparison.drift.is_empty(), "{:?}", comparison.drift);
        // An extra constraint is drift even when existing rows were not checked.
        found
            .constraints
            .get_mut("qbit_block_candidate_outbox")
            .unwrap()
            .insert(
                "CHECK ((share_id <> 'x'::text))".into(),
                constraint("operator_check", false),
            );
        let comparison = compare_fingerprints(&expected, &found);
        assert_eq!(
            comparison.drift,
            vec!["constraint operator_check on qbit_block_candidate_outbox: CHECK ((share_id <> 'x'::text)); the release does not create it"]
        );
        assert!(comparison.extra.is_empty(), "{:?}", comparison.extra);
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
            objects_present(&expected, &found),
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

    fn policy(command: &str, using: Option<&str>) -> PolicyDefinition {
        PolicyDefinition {
            command: command.to_owned(),
            permissive: true,
            roles: vec!["PUBLIC".into()],
            using: using.map(str::to_owned),
            with_check: None,
        }
    }

    #[test]
    fn row_level_security_and_policies_are_compared_for_release_tables() {
        let mut expected = SchemaFingerprint::default();
        expected.tables.insert(
            "qbit_block_candidate_outbox".into(),
            table(&[("state", column("text", true))]),
        );
        expected.tables.insert(
            "qbit_share_ledger".into(),
            table(&[("share_id", column("text", true))]),
        );
        // The same columns, but the outbox has forced row-level security
        // with a policy that hides pending rows, the ledger has it enabled
        // with no policy, and an operator table of its own has both, which
        // is only extra: the release does not create that table.
        let mut found = SchemaFingerprint::default();
        let mut outbox = table(&[("state", column("text", true))]);
        outbox.row_security = true;
        outbox.force_row_security = true;
        found
            .tables
            .insert("qbit_block_candidate_outbox".into(), outbox.clone());
        let mut ledger = table(&[("share_id", column("text", true))]);
        ledger.row_security = true;
        found.tables.insert("qbit_share_ledger".into(), ledger);
        found.tables.insert("operator_notes".into(), outbox);
        let hide_pending = policy("*", Some("(state <> 'pending'::text)"));
        found.policies.insert(
            ("qbit_block_candidate_outbox".into(), "hide_pending".into()),
            hide_pending.clone(),
        );
        found.policies.insert(
            ("operator_notes".into(), "mine".into()),
            policy("r", Some("(owner = CURRENT_USER)")),
        );
        let comparison = compare_fingerprints(&expected, &found);
        assert_eq!(
            comparison.drift,
            vec![
                "table qbit_block_candidate_outbox differs: expected row-level security disabled, found enabled and forced",
                "table qbit_share_ledger differs: expected row-level security disabled, found enabled",
                "policy hide_pending on qbit_block_candidate_outbox: FOR ALL USING ((state <> 'pending'::text)); the release has no row-level security policy on this table",
            ]
        );
        assert_eq!(comparison.extra, vec!["table operator_notes"]);
        assert_eq!(
            objects_present(&expected, &found),
            vec![
                "table qbit_block_candidate_outbox",
                "table qbit_share_ledger"
            ]
        );

        // Persistence and security are named on one line, in that order.
        let mut unlogged = expected.tables["qbit_share_ledger"].clone();
        unlogged.persistence = "u".into();
        unlogged.force_row_security = true;
        assert_eq!(
            table_differences(&expected.tables["qbit_share_ledger"], &unlogged),
            "expected logged, found UNLOGGED, expected row-level security disabled, found disabled but forced"
        );

        // Symmetric: a release policy the source lacks, or defines
        // differently, is drift too; one the release also has on that table
        // is named as not created rather than as the table's only policy.
        expected
            .tables
            .get_mut("qbit_block_candidate_outbox")
            .unwrap()
            .row_security = true;
        expected
            .tables
            .get_mut("qbit_block_candidate_outbox")
            .unwrap()
            .force_row_security = true;
        expected.policies.insert(
            ("qbit_block_candidate_outbox".into(), "release_only".into()),
            policy("r", Some("(state = 'pending'::text)")),
        );
        expected.policies.insert(
            (
                "qbit_block_candidate_outbox".into(),
                "release_writes".into(),
            ),
            policy("a", None),
        );
        found.policies.insert(
            (
                "qbit_block_candidate_outbox".into(),
                "release_writes".into(),
            ),
            PolicyDefinition {
                permissive: false,
                roles: vec!["operator".into(), "prism".into()],
                with_check: Some("(state = 'pending'::text)".into()),
                ..policy("a", None)
            },
        );
        let comparison = compare_fingerprints(&expected, &found);
        assert_eq!(
            comparison.drift,
            vec![
                "table qbit_share_ledger differs: expected row-level security disabled, found enabled",
                "missing policy release_only on qbit_block_candidate_outbox: FOR SELECT USING ((state = 'pending'::text))",
                "policy release_writes on qbit_block_candidate_outbox differs: expected FOR INSERT, found AS RESTRICTIVE FOR INSERT TO operator, prism WITH CHECK ((state = 'pending'::text))",
                "policy hide_pending on qbit_block_candidate_outbox: FOR ALL USING ((state <> 'pending'::text)); the release does not create it",
            ]
        );
        for (code, command) in [
            ("r", "SELECT"),
            ("a", "INSERT"),
            ("w", "UPDATE"),
            ("d", "DELETE"),
            ("*", "ALL"),
        ] {
            assert_eq!(policy_command(code), command);
        }

        // The same flags and policies on both sides: nothing to report.
        expected.policies.clear();
        expected.policies.insert(
            ("qbit_block_candidate_outbox".into(), "hide_pending".into()),
            hide_pending,
        );
        found.policies.remove(&(
            "qbit_block_candidate_outbox".to_owned(),
            "release_writes".to_owned(),
        ));
        found
            .tables
            .get_mut("qbit_share_ledger")
            .unwrap()
            .row_security = false;
        let comparison = compare_fingerprints(&expected, &found);
        assert!(comparison.drift.is_empty(), "{:?}", comparison.drift);
        assert_eq!(comparison.extra, vec!["table operator_notes"]);
    }

    #[test]
    fn a_record_with_3_and_not_2_is_refused_naming_the_record_and_the_remedy() {
        refuse_inconsistent_native_record(&[]).unwrap();
        refuse_inconsistent_native_record(&[2, 3, 4, 5, 6, 8, 9]).unwrap();
        // Without 3 the 2.x.x path runs, which applies 2 before 3.
        refuse_inconsistent_native_record(&[2]).unwrap();
        let error = refuse_inconsistent_native_record(&[3, 4, 5, 8, 9])
            .unwrap_err()
            .to_string();
        assert!(
            error.starts_with("refusing to migrate a native database at schema migrations 3, 4, 5, 8, 9 before any DDL: migration 3 is recorded and 2 is not"),
            "{error}"
        );
        assert!(
            error.contains("Nothing was changed")
                && error.contains("INSERT INTO qbit_prism_schema_migrations(version) VALUES(2)"),
            "{error}"
        );
    }

    /// `require_declared_capabilities` over the given rows, at the given
    /// recorded migrations.
    fn declared_at(rows: &[(&str, i32)], versions: &[i32]) -> Result<()> {
        let rows: Vec<(String, i32)> = rows
            .iter()
            .map(|(name, value)| ((*name).to_owned(), *value))
            .collect();
        require_declared_capabilities(Some(&rows), versions)
    }

    #[test]
    fn a_database_at_6_must_declare_its_candidate_storage_version() {
        let at_6 = &[2, 3, 4, 5, 6, 7, 8, 9, 10];
        let declared = |rows: &[(&str, i32)]| declared_at(rows, at_6);
        declared(&[("candidate_storage_version", 1)]).unwrap();
        declared(&[("candidate_storage_version", 2), ("sealed_share_pages", 1)]).unwrap();
        let error = require_declared_capabilities(None, at_6)
            .unwrap_err()
            .to_string();
        assert!(
            error.starts_with(
                "database is at schema migration 6 but has no qbit_prism_schema_capabilities"
            ),
            "{error}"
        );
        assert!(
            error.contains("006_source_schema.sql") && error.contains("start or migrate again"),
            "{error}"
        );
        let error = declared(&[("sealed_share_pages", 1)])
            .unwrap_err()
            .to_string();
        assert!(
            error.starts_with("database is at schema migration 6 but qbit_prism_schema_capabilities has no candidate_storage_version row"),
            "{error}"
        );
        assert!(
            error.contains("VALUES('candidate_storage_version',1)"),
            "{error}"
        );
        let error = declared(&[]).unwrap_err().to_string();
        assert!(
            error.contains("has no candidate_storage_version row"),
            "{error}"
        );
    }

    #[test]
    fn a_database_at_11_must_declare_the_offer_lifecycle_as_011_declared_it() {
        let at_11 = REQUIRED_SCHEMA_VERSIONS;
        let storage = ("candidate_storage_version", 1);
        let lifecycle = ("candidate_offer_lifecycle", 1);
        declared_at(&[storage, lifecycle], at_11).unwrap();
        declared_at(&[lifecycle, storage, ("sealed_share_pages", 1)], at_11).unwrap();
        // Before 011 there is no lifecycle row to require: the genuine
        // upgrade, and a #258 source 006 has just declared, carry the
        // storage version alone.
        declared_at(&[storage], &[2, 3, 4, 5, 6, 7, 8, 9, 10]).unwrap();
        declared_at(&[("candidate_storage_version", 2)], &[2, 3, 4, 5, 6]).unwrap();
        let error = declared_at(&[storage], at_11).unwrap_err().to_string();
        assert!(
            error.starts_with("database is at schema migration 11 but qbit_prism_schema_capabilities has no candidate_offer_lifecycle row"),
            "{error}"
        );
        assert!(
            error.contains("VALUES('candidate_offer_lifecycle',1)")
                && error.contains("start or migrate again"),
            "{error}"
        );
        for value in [0, -1, 2] {
            let error = declared_at(&[storage, ("candidate_offer_lifecycle", value)], at_11)
                .unwrap_err()
                .to_string();
            assert!(
                error.starts_with(&format!("database is at schema migration 11 but qbit_prism_schema_capabilities declares candidate_offer_lifecycle = {value}")),
                "{error}"
            );
            assert!(
                error.contains("newer PRISM release")
                    && error.contains("nothing native writes another value"),
                "{error}"
            );
        }
        // A record with 11 and not 6 was restored selectively: the row is
        // required all the same.
        let error = declared_at(&[storage], &[2, 3, 4, 5, 7, 8, 9, 10, 11])
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("has no candidate_offer_lifecycle row"),
            "{error}"
        );
        // The storage version is required first, at 11 as at 6, and so is
        // the table.
        let error = declared_at(&[lifecycle], at_11).unwrap_err().to_string();
        assert!(
            error.contains("has no candidate_storage_version row"),
            "{error}"
        );
        // A dropped table held both declarations at 11: the one refusal
        // names both remedies; before 011 it names 006's alone.
        let error = require_declared_capabilities(None, at_11)
            .unwrap_err()
            .to_string();
        assert!(
            error.starts_with(
                "database is at schema migration 6 but has no qbit_prism_schema_capabilities"
            ),
            "{error}"
        );
        assert!(
            error.contains("006_source_schema.sql")
                && error.contains("VALUES('candidate_offer_lifecycle',1)")
                && error.ends_with("then start or migrate again"),
            "{error}"
        );
        let error = require_declared_capabilities(None, &[2, 3, 4, 5, 6, 7, 8, 9, 10])
            .unwrap_err()
            .to_string();
        assert!(!error.contains("candidate_offer_lifecycle"), "{error}");
    }

    #[test]
    fn the_pre_ddl_declaration_fence_runs_whenever_6_or_11_is_recorded() {
        let inventory = |capabilities: Option<Vec<(&str, i32)>>| SourceInventory {
            share_ledger: true,
            outbox: true,
            present: vec![false; OBJECTS_002.len()],
            capabilities: capabilities.map(|rows| {
                rows.into_iter()
                    .map(|(name, value)| (name.to_owned(), value))
                    .collect()
            }),
        };
        let storage_only = inventory(Some(vec![("candidate_storage_version", 1)]));
        let both = inventory(Some(vec![
            ("candidate_storage_version", 1),
            ("candidate_offer_lifecycle", 1),
        ]));
        // Before 006 nothing is declared, and before 011 the storage
        // version alone is.
        refuse_undeclared_native_database(&[2, 3, 4, 5], &inventory(None)).unwrap();
        refuse_undeclared_native_database(&[2, 3, 4, 5, 6, 7, 8, 9, 10], &storage_only).unwrap();
        refuse_undeclared_native_database(REQUIRED_SCHEMA_VERSIONS, &both).unwrap();
        let error = refuse_undeclared_native_database(REQUIRED_SCHEMA_VERSIONS, &storage_only)
            .unwrap_err()
            .to_string();
        assert!(
            error.starts_with("refusing to migrate a native database at schema migrations 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 before any DDL: database is at schema migration 11 but"),
            "{error}"
        );
        // 11 without 6 was restored selectively: checked all the same, the
        // table included.
        let restored = [2, 3, 4, 5, 7, 8, 9, 10, 11];
        let error = refuse_undeclared_native_database(&restored, &storage_only)
            .unwrap_err()
            .to_string();
        assert!(
            error.starts_with("refusing to migrate a native database at schema migrations 2, 3, 4, 5, 7, 8, 9, 10, 11 before any DDL: database is at schema migration 11 but"),
            "{error}"
        );
        let error = refuse_undeclared_native_database(&restored, &inventory(None))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("has no qbit_prism_schema_capabilities"),
            "{error}"
        );
        refuse_undeclared_native_database(&restored, &both).unwrap();
    }

    #[test]
    fn native_migrations_are_the_applied_versions_in_order() {
        let versions: Vec<i32> = NATIVE_MIGRATIONS.iter().map(|(v, _)| *v).collect();
        assert_eq!(versions, REQUIRED_SCHEMA_VERSIONS);
        for (version, sql) in NATIVE_MIGRATIONS {
            assert!(!sql.trim().is_empty(), "migration {version} is empty");
            assert_eq!(native_migration(*version), *sql);
        }
    }

    #[test]
    fn reserved_objects_are_the_native_ones_the_release_lacks_without_the_migrator_table() {
        let mut release = SchemaFingerprint::default();
        release.tables.insert(
            "qbit_share_ledger".into(),
            table(&[("share_id", column("bigint", true))]),
        );
        // The outbox of a #258 source: the release 002 added storage_version.
        release.tables.insert(
            "qbit_block_candidate_outbox".into(),
            table(&[
                ("block_hash", column("text", true)),
                ("storage_version", column("integer", true)),
            ]),
        );
        release.sequences.insert(
            "qbit_share_ledger_share_seq".into(),
            sequence("bigint", 1, i64::MAX),
        );
        release.functions.insert(
            ("qbit_prism_window".into(), "w numeric".into()),
            function("sql", "1"),
        );
        let mut native = SchemaFingerprint::default();
        // Kept from the release, re-asserted by 003, and the migrator's own.
        native.tables.insert(
            "qbit_share_ledger".into(),
            table(&[("share_id", column("bigint", true))]),
        );
        native.sequences.insert(
            "qbit_share_ledger_share_seq".into(),
            sequence("bigint", 1, i64::MAX),
        );
        native.functions.insert(
            ("qbit_prism_window".into(), "w numeric".into()),
            function("sql", "2"),
        );
        native.tables.insert(
            "qbit_prism_schema_migrations".into(),
            table(&[("version", column("integer", true))]),
        );
        // The release's outbox columns kept, and the claim columns 002 adds.
        native.tables.insert(
            "qbit_block_candidate_outbox".into(),
            table(&[
                ("block_hash", column("text", true)),
                ("storage_version", column("integer", true)),
                ("claim_token", column("text", false)),
                ("next_attempt_at", column("timestamp with time zone", true)),
            ]),
        );
        // Created by the native migrations only.
        native.tables.insert(
            "qbit_prism_cluster".into(),
            table(&[
                ("singleton", column("boolean", true)),
                ("fatal_error", column("text", false)),
            ]),
        );
        native.sequences.insert(
            "qbit_prism_session_sequence".into(),
            sequence("bigint", 1, 4294967295),
        );
        native.indexes.insert(
            "qbit_prism_jobs_expiry_idx".into(),
            IndexDefinition {
                table: "qbit_prism_jobs".into(),
                definition: "CREATE INDEX qbit_prism_jobs_expiry_idx ON qbit_prism_jobs USING btree (expires_at)".into(),
                valid: true,
                unique: false,
                expression: false,
                partial: false,
            },
        );
        native.triggers.insert(
            ("qbit_share_ledger".into(), "qbit_prism_no_legacy_writer".into()),
            TriggerDefinition {
                definition: "CREATE TRIGGER qbit_prism_no_legacy_writer BEFORE INSERT ON qbit_share_ledger FOR EACH ROW EXECUTE FUNCTION qbit_prism_reject_legacy_writer()".into(),
                enabled: "O".into(),
            },
        );
        native.functions.insert(
            ("qbit_prism_reject_legacy_writer".into(), String::new()),
            function("plpgsql", "BEGIN RETURN NEW; END"),
        );
        let reserved = reserved_objects(&native, &release);
        assert_eq!(
            reserved.objects.tables.keys().collect::<Vec<_>>(),
            ["qbit_prism_cluster"]
        );
        assert_eq!(
            reserved.objects.sequences.keys().collect::<Vec<_>>(),
            ["qbit_prism_session_sequence"]
        );
        assert_eq!(
            reserved.objects.indexes.keys().collect::<Vec<_>>(),
            ["qbit_prism_jobs_expiry_idx"]
        );
        assert_eq!(
            reserved.objects.triggers.keys().collect::<Vec<_>>(),
            [&(
                "qbit_share_ledger".to_owned(),
                "qbit_prism_no_legacy_writer".to_owned()
            )]
        );
        assert_eq!(
            reserved.objects.functions.keys().collect::<Vec<_>>(),
            [&("qbit_prism_reject_legacy_writer".to_owned(), String::new())]
        );
        // The columns the native migrations add to a release table, and
        // nothing of a reserved table, whose columns come with it. A column
        // in both readings (storage_version on a #258 source) is not
        // reserved.
        assert_eq!(
            reserved.columns,
            BTreeMap::from([(
                "qbit_block_candidate_outbox".to_owned(),
                BTreeSet::from(["claim_token".to_owned(), "next_attempt_at".to_owned()])
            )])
        );
        // On a pre-#258 source the release lacks storage_version and 006
        // adds it, so it is reserved there (a real pre-#258 source that has
        // it is a partial 002, refused earlier).
        release
            .tables
            .get_mut("qbit_block_candidate_outbox")
            .unwrap()
            .columns
            .remove("storage_version");
        let reserved_pre_258 = reserved_objects(&native, &release);
        assert_eq!(reserved_pre_258.objects, reserved.objects);
        assert_eq!(
            reserved_pre_258.columns["qbit_block_candidate_outbox"],
            BTreeSet::from([
                "claim_token".to_owned(),
                "next_attempt_at".to_owned(),
                "storage_version".to_owned()
            ])
        );
        // A source that has any of them is named the way the drift report
        // names objects and columns; the release's own are not collisions.
        let mut found = SchemaFingerprint::default();
        found.tables.insert(
            "qbit_share_ledger".into(),
            table(&[("share_id", column("bigint", true))]),
        );
        found.tables.insert(
            "qbit_block_candidate_outbox".into(),
            table(&[
                ("block_hash", column("text", true)),
                ("storage_version", column("integer", true)),
                ("claim_token", column("integer", false)),
            ]),
        );
        found.tables.insert(
            "qbit_prism_cluster".into(),
            table(&[("singleton", column("boolean", true))]),
        );
        found.functions.insert(
            ("qbit_prism_reject_legacy_writer".into(), String::new()),
            function("plpgsql", "BEGIN RETURN NULL; END"),
        );
        assert_eq!(
            objects_present(&reserved.objects, &found),
            vec![
                "table qbit_prism_cluster",
                "function qbit_prism_reject_legacy_writer()"
            ]
        );
        assert_eq!(
            columns_present(&reserved.columns, &found),
            vec!["column qbit_block_candidate_outbox.claim_token"]
        );
        let error = require_no_native_collision(SourceState::Pre258, &reserved, &found)
            .unwrap_err()
            .to_string();
        assert!(error.starts_with("refusing to migrate a native collision source before any DDL: the 2.x.x database already holds 3 object(s)"), "{error}");
        assert!(
            error.contains("(table qbit_prism_cluster; function qbit_prism_reject_legacy_writer(); column qbit_block_candidate_outbox.claim_token)"),
            "{error}"
        );
        assert!(
            error.contains("table, sequence, index, trigger, function or column"),
            "{error}"
        );
        assert!(
            error.contains("Nothing was changed") && error.contains("remove them yourself"),
            "{error}"
        );
        let error = require_no_native_collision(SourceState::Fresh, &reserved, &found)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("the empty database already holds"),
            "{error}"
        );
        found.tables.remove("qbit_prism_cluster");
        found.functions.clear();
        let error = require_no_native_collision(SourceState::Applied258, &reserved, &found)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("already holds 1 object(s) that the native migrations create and the 2.x.x release does not (column qbit_block_candidate_outbox.claim_token)"),
            "{error}"
        );
        found
            .tables
            .get_mut("qbit_block_candidate_outbox")
            .unwrap()
            .columns
            .remove("claim_token");
        require_no_native_collision(SourceState::Applied258, &reserved, &found).unwrap();
    }
}
