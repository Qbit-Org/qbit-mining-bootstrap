//! Every table of the PRISM schema, classified for the 3.1 dual writer
//! (CONTRACT D-16), with what re-personalising a physical copy of the peer's
//! database (`qbit-prism-server node-identity repersonalise`) does with it:
//!
//! - **copied**: the peer sync pulls the rows the peer originated
//!   ([`COPIED_TABLES`]). The copy's rows stay. A row or column of such a
//!   table that is this node's own ([`LocalPart`]) is named, with its fate.
//! - **local**: never copied; this node's own state. Its entry says whether
//!   the copy's rows are cleared, kept or rewritten ([`Reset`]).
//! - **pair-wide**: never copied, and kept as the copy holds it: written by
//!   migrations and commands about the database itself, the same on both
//!   nodes of a pair that started from one ledger, or describing the
//!   physical database the copy brought along.
//!
//! The partitions of `qbit_share_ledger`, attached or detached, are covered
//! by their parent, and an extension's tables are not PRISM's. A gated test
//! fails on any table of a migrated schema that the inventory does not
//! classify and on any table or column it names that the schema lacks, so a
//! migration that adds a table classifies it here; re-personalising refuses
//! a database holding a table it does not classify. A column a migration adds
//! to a copied table is that table's to copy unless its entry names it here
//! as this node's own.
//!
//! A physical copy of the peer holds the peer's in-flight work (its
//! found-block candidates and their deferred shares, its sessions, claims and
//! wallet reservations, its frontends' registrations, its issued jobs), which
//! this node must neither settle nor credit a second time: that is cleared.
//! What is derived from rows both nodes hold (the balance summary, block and
//! payout states, chain view, rollups) or is history this database carries
//! (journals, divergences, broadcast history) is kept: clearing it would
//! change balances or audits, and this node's own workers move it on from
//! the copy's values as they would after any restart.
use anyhow::Result;
use serde::Serialize;
use sqlx::PgConnection;

use crate::peer_sync::COPIED_TABLES;

/// How a table relates to the two nodes of a pair.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableClass {
    /// One of [`COPIED_TABLES`], with the rows and columns of it that are
    /// this node's own.
    Copied(&'static [LocalPart]),
    /// Never copied: this node's own state, and what re-personalising does
    /// with the copy's rows.
    Local(Reset),
    /// Never copied, and the same for the pair: kept as the copy holds it.
    PairWide,
}

/// What re-personalising a physical copy of the peer does with a local table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reset {
    /// Every row is deleted, in inventory order (a table before the one it
    /// references).
    Clear,
    /// Kept as the copy holds it.
    Keep,
    /// Rewritten for this node by the command itself: the identity, the
    /// lineage and the sync cursors.
    Rewrite,
}

/// The part of a copied table that is this node's own, never copied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LocalPart {
    /// Rows matching `predicate` (SQL) are never copied: deleted.
    Rows { predicate: &'static str },
    /// Columns this node derives itself, kept at the copy's values.
    Kept(&'static [&'static str]),
    /// Columns reset on the rows matching `rows` (SQL) by the assignments
    /// `set` (SQL).
    Reset {
        columns: &'static [&'static str],
        rows: &'static str,
        set: &'static str,
    },
}

/// One table of the inventory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TableEntry {
    pub table: &'static str,
    pub class: TableClass,
    /// Created only on some databases: kept whenever present, and never
    /// required.
    pub optional: bool,
}

const fn copied(table: &'static str, local: &'static [LocalPart]) -> TableEntry {
    TableEntry {
        table,
        class: TableClass::Copied(local),
        optional: false,
    }
}

const fn local(table: &'static str, reset: Reset) -> TableEntry {
    TableEntry {
        table,
        class: TableClass::Local(reset),
        optional: false,
    }
}

const fn pair_wide(table: &'static str) -> TableEntry {
    TableEntry {
        table,
        class: TableClass::PairWide,
        optional: false,
    }
}

const fn optional_pair_wide(table: &'static str) -> TableEntry {
    TableEntry {
        table,
        class: TableClass::PairWide,
        optional: true,
    }
}

/// A fanout's claim, and the two columns handing it back writes: the claim's
/// own schedule, and the row's last write.
const FANOUT_CLAIM_COLUMNS: &[&str] = &[
    "claim_token",
    "claim_instance_id",
    "claim_expires_at",
    "claim_lease_seconds",
    "claim_renewals",
    "next_broadcast_attempt_at",
    "updated_at",
];

/// Every table of the PRISM schema. Its order is the order re-personalising
/// works in.
pub const TABLE_INVENTORY: &[TableEntry] = &[
    // Copied, in COPIED_TABLES order. The sync never updates an existing row.
    //
    // Immutable: a trigger refuses UPDATE, DELETE and TRUNCATE. Covers every
    // partition, attached or detached.
    copied("qbit_share_ledger", &[]),
    copied("qbit_prism_share_hashes", &[]),
    // The states this node's reconciler derives from its own chain view
    // (D-1). Kept: the copy's are the peer's view of the same chain, and the
    // balance summary and the publication order rest on them. Reset to
    // 'prepared', a block would drop out of every balance (the carry summary
    // counts confirmed blocks only) and never count again
    // (qbit_confirm_pool_block confirms at the tip height only), 'mature' and
    // 'reversed' are terminal, and the publication ordinal is assigned once.
    // This node's reconciler moves them on as its own chain view requires.
    copied(
        "qbit_pool_blocks",
        &[LocalPart::Kept(&[
            "chain_state",
            "maturity_state",
            "matured_at",
            "disconnected_at",
            "inactive_since",
            "audit_publication_sequence",
        ])],
    ),
    copied("qbit_prism_audit_snapshots", &[]),
    // Stored when this database seals a share partition (share-archive), so
    // it follows this database's partitions: kept with the partition catalog.
    copied(
        "qbit_pool_audit_bundles",
        &[LocalPart::Kept(&["canonical_audit_bytes"])],
    ),
    // Each follows its block's maturity: kept, as the block's state is.
    copied(
        "qbit_pool_payout_entries",
        &[LocalPart::Kept(&["maturity_state"])],
    ),
    copied(
        "qbit_payout_carry_forward",
        &[LocalPart::Kept(&["maturity_state"])],
    ),
    copied("qbit_ctv_fanout_sets", &[]),
    copied(
        "qbit_ctv_fanout_artifacts",
        &[
            // The peer's frontends' claims: none of them runs here. Handed
            // back as a holder's own release hands a claim back
            // (CLEAR_FANOUT_CLAIM_SQL, and the fanout due at once unless
            // held at infinity), so this node's lane takes the fanout without
            // first waiting out a lease nobody renews. An unclaimed fanout's
            // schedule and last write are kept.
            LocalPart::Reset {
                columns: FANOUT_CLAIM_COLUMNS,
                rows: "claim_token IS NOT NULL OR claim_instance_id IS NOT NULL \
                       OR claim_expires_at IS NOT NULL OR claim_lease_seconds IS NOT NULL \
                       OR claim_renewals<>0",
                set: "claim_token=NULL,claim_instance_id=NULL,claim_expires_at=NULL,\
                      claim_lease_seconds=NULL,claim_renewals=0,\
                      next_broadcast_attempt_at=CASE WHEN next_broadcast_attempt_at IS NULL \
                      OR next_broadcast_attempt_at='infinity'::timestamptz \
                      THEN next_broadcast_attempt_at \
                      ELSE LEAST(next_broadcast_attempt_at,clock_timestamp()) END,\
                      updated_at=clock_timestamp()",
            },
            // Settlement and confirmation, from the chain this node shares:
            // kept. Reset, every settled fanout would be broadcast again and
            // its confirmation scanned for again. This node's lane still
            // checks every fanout under 1,000 confirmations against its own
            // chain view.
            LocalPart::Kept(&[
                "settlement_status",
                "confirmed_block_hash",
                "confirmed_block_height",
                "confirmed_depth",
                "spend_scan_next_height",
                "spend_scan_anchor_height",
                "spend_scan_anchor_hash",
            ]),
            // The fanout's broadcast history and backoff: kept. A broadcast
            // repeated from this node sends the same transaction, and the
            // history is the fanout's, whichever node sent it.
            LocalPart::Kept(&[
                "broadcast_attempt_count",
                "broadcast_attempt_detail_count",
                "first_broadcast_attempt_at",
                "last_broadcast_attempt_at",
                "last_broadcast_attempt_status",
                "last_broadcast_package_tx_hexes",
                "last_broadcast_package_txids",
                "last_broadcast_submit_result",
                "last_broadcast_error",
                "broadcast_attempt_status_counts",
                "broadcast_retry_backoff_seconds",
            ]),
        ],
    ),
    copied("qbit_prism_templates", &[]),
    copied("qbit_prism_balance_snapshots", &[]),
    copied(
        "qbit_prism_jobs",
        &[
            // Only prepared records are copied (D-3, D-11). Issued jobs are
            // the peer's sessions' work. Kept, this node would resume a peer
            // miner's job (Coordinator::resume_job reads any stored job) with
            // the peer's extranonce1, and could credit a share the peer
            // already credited: deleted.
            LocalPart::Rows {
                predicate: "job_id NOT LIKE 'prepared:%'",
            },
            // A prepared record's retention, renewed while issued jobs
            // depend on it: kept.
            LocalPart::Kept(&["expires_at"]),
        ],
    ),
    // Append-only (a trigger refuses UPDATE, DELETE and TRUNCATE); D3's.
    copied("qbit_prism_node_roles", &[]),
    //
    // Local, never copied (CONTRACT section 1, D-2, D-9).
    //
    // Clear. The solving shares of the peer's found-block candidates,
    // credited when their block lands or confirms (credit_deferred_share).
    // Kept, this node's reconciler would credit a share the peer credits
    // too (D-2). Before the outbox, which they reference.
    local("qbit_prism_deferred_shares", Reset::Clear),
    // Clear. The peer's found-block candidates. Kept, this node would offer,
    // land and settle the peer's blocks beside it, under its own origin. The
    // peer settles them; if it dies first, adoption (S8) covers a block that
    // its prepared record proves.
    local("qbit_block_candidate_outbox", Reset::Clear),
    // Clear. The extranonce1 reservations of the peer's live sessions, owned
    // by its instances. This node allocates from its own half.
    local("qbit_prism_session_reservations", Reset::Clear),
    // Clear, once no row shows a frontend running on this database. The
    // peer's frontends' registrations: none of them runs here, and their
    // copied states, which nothing here would ever mark stopped, would refuse
    // every later migration and fatal-state clear.
    local("qbit_prism_instances", Reset::Clear),
    // Clear. CPFP funding the peer's node reserved, locked in its own wallet
    // and signed for. This node's wallet holds other coins: replaying or
    // unlocking these from here would fail. Its broadcaster funds its own.
    local("qbit_prism_cpfp_packages", Reset::Clear),
    // Clear. Wallet locks only the peer's node can release.
    local("qbit_prism_cpfp_retired_funding", Reset::Clear),
    // Clear. Conflicts the peer found while pulling this node's old rows.
    local("qbit_prism_peer_sync_conflicts", Reset::Clear),
    // Rewrite. This node's index.
    local("qbit_prism_node_identity", Reset::Rewrite),
    // Rewrite. Where this node's own rows start, above every key of its old
    // rows the peer has seen, and no own-log verification: the check runs
    // before this node serves (D-8, D-17).
    local("qbit_prism_node_lineage", Reset::Rewrite),
    // Rewrite. Each of the peer's streams, from where the copy ends:
    // ledger/node_identity.rs, Ledger::repersonalise_node_identity.
    local("qbit_prism_peer_sync_cursors", Reset::Rewrite),
    // Keep. The balance summary, maintained by triggers from the carry rows
    // and block states the copy keeps: it equals their recomputation here as
    // on the peer. Cleared, every miner's balance would read zero.
    local("qbit_payout_carry_forward_current", Reset::Keep),
    // Keep, every column. payout_revision and chain_epoch never go back
    // (prepared work and chain transitions are checked against them);
    // ledger_clock_ms stays at or above every share's stamp; best_chainwork,
    // best_tip_hash and best_tip_height are the chain view this node's
    // observer must not regress from (a lagging node is refused as behind,
    // never read as a reorganisation of the balances); config_fingerprint is
    // the pair's (D-6); a halt recorded on the peer (fatal_error) holds here
    // too until fatal-state clear.
    local("qbit_prism_cluster", Reset::Keep),
    // Keep. The debt this database's confirmations created (#478), which the
    // integrity report sums; cleared, the report would forget debt the
    // balances still show. A re-confirmation here replaces a block's row.
    local("qbit_prism_payout_divergences", Reset::Keep),
    local("qbit_prism_payout_divergence_accounts", Reset::Keep),
    // Keep. The newest 32 broadcast attempts of each copied fanout, which its
    // counters count.
    local("qbit_ctv_fanout_broadcast_attempts", Reset::Keep),
    // Keep. Rollups of the copied share ledger up to their watermark;
    // rebuilt, they would rescan the whole ledger.
    local("qbit_hashrate_rollup_pool", Reset::Keep),
    local("qbit_hashrate_rollup_miner", Reset::Keep),
    local("qbit_hashrate_rollup_progress", Reset::Keep),
    // Keep. Vardiff preload hints per worker, never accounting.
    local("qbit_worker_difficulty", Reset::Keep),
    // Keep. A block submission hold set on the peer stays set: cleared here,
    // it would release what an operator held (submission-hold clear does).
    local("qbit_prism_submission_hold", Reset::Keep),
    // Keep. Immutable operator journals (triggers refuse UPDATE, DELETE and
    // TRUNCATE): history this database carries.
    local("qbit_prism_submission_hold_events", Reset::Keep),
    local("qbit_prism_fatal_state_events", Reset::Keep),
    local("qbit_prism_policy_transitions", Reset::Keep),
    local("qbit_prism_signing_transitions", Reset::Keep),
    //
    // Pair-wide, never copied, kept.
    //
    // The schema record, its capabilities and the migration source.
    pair_wide("qbit_prism_schema_migrations"),
    pair_wide("qbit_prism_schema_capabilities"),
    pair_wide("qbit_prism_migration_source"),
    // The partition settings, the catalog of this database's share ledger
    // partitions and the registry of rejected IDs from partitions that left:
    // the copy's partitions are the ones they describe.
    pair_wide("qbit_prism_share_partitioning"),
    pair_wide("qbit_prism_share_partitions"),
    pair_wide("qbit_prism_rejected_share_ids"),
    // The 2.x.x writer lease, frozen by a trigger since migration 002.
    pair_wide("qbit_ledger_writer_lease"),
    // Optional. Migration 2's share-hash backfill state, present while a
    // deferred backfill runs.
    optional_pair_wide("qbit_prism_share_hash_backfill"),
    // Optional. #258's chunked candidate bodies, on a database migrated from
    // a 2.x.x release that had them; native PRISM never writes them.
    optional_pair_wide("qbit_block_candidate_body"),
    optional_pair_wide("qbit_block_candidate_body_chunk"),
    optional_pair_wide("qbit_block_candidate_body_span"),
    optional_pair_wide("qbit_block_candidate_body_page"),
];

const fn same_name(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }
    true
}

/// Whether the inventory's copied tables are exactly [`COPIED_TABLES`], in
/// its order.
const fn copied_tables_match() -> bool {
    let mut copied = 0;
    let mut i = 0;
    while i < TABLE_INVENTORY.len() {
        if let TableClass::Copied(_) = TABLE_INVENTORY[i].class {
            if copied == COPIED_TABLES.len()
                || !same_name(TABLE_INVENTORY[i].table, COPIED_TABLES[copied])
            {
                return false;
            }
            copied += 1;
        }
        i += 1;
    }
    copied == COPIED_TABLES.len()
}

// Checked when the crate compiles: a table the peer sync copies is classified
// copied here, and only those are.
const _: () = assert!(
    copied_tables_match(),
    "TABLE_INVENTORY's copied tables must be peer_sync::COPIED_TABLES, in its order"
);

/// The inventory's entry for `table`, if it has one.
pub fn entry(table: &str) -> Option<&'static TableEntry> {
    TABLE_INVENTORY.iter().find(|entry| entry.table == table)
}

/// How a database's PRISM schema (the first schema on its search path)
/// differs from the inventory. Empty everywhere when the inventory classifies
/// every table and the schema holds every table and column it names.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct SchemaCoverage {
    /// Tables the inventory does not classify. A partition of the share
    /// ledger, attached or detached, is its parent's.
    pub unclassified: Vec<String>,
    /// Tables the inventory requires that the schema lacks.
    pub missing_tables: Vec<String>,
    /// Columns the inventory names that their table lacks, as `table.column`.
    pub missing_columns: Vec<String>,
}

impl SchemaCoverage {
    pub fn is_complete(&self) -> bool {
        self.unclassified.is_empty()
            && self.missing_tables.is_empty()
            && self.missing_columns.is_empty()
    }
}

/// Every column the inventory names, as `(table, column)`.
fn named_columns() -> Vec<(&'static str, &'static str)> {
    let mut named = Vec::new();
    for entry in TABLE_INVENTORY {
        let TableClass::Copied(parts) = entry.class else {
            continue;
        };
        for part in parts {
            let columns = match part {
                LocalPart::Kept(columns) | LocalPart::Reset { columns, .. } => *columns,
                LocalPart::Rows { .. } => &[],
            };
            named.extend(columns.iter().map(|column| (entry.table, *column)));
        }
    }
    named
}

/// Compare the connection's PRISM schema with the inventory.
pub async fn schema_coverage(connection: &mut PgConnection) -> Result<SchemaCoverage> {
    let classified: Vec<&str> = TABLE_INVENTORY.iter().map(|entry| entry.table).collect();
    let required: Vec<&str> = TABLE_INVENTORY
        .iter()
        .filter(|entry| !entry.optional)
        .map(|entry| entry.table)
        .collect();
    let (tables, columns): (Vec<&str>, Vec<&str>) = named_columns().into_iter().unzip();
    // A table an extension created is the extension's, not PRISM's.
    let unclassified: Vec<String> = sqlx::query_scalar(
        "SELECT c.relname::text FROM pg_class c \
         WHERE c.relnamespace=(SELECT oid FROM pg_namespace WHERE nspname=current_schema()) \
         AND c.relkind IN ('r','p') AND NOT c.relispartition AND c.relname<>ALL($1::text[]) \
         AND NOT EXISTS (SELECT 1 FROM qbit_prism_share_partitions p WHERE p.partition_name=c.relname) \
         AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid='pg_class'::regclass \
         AND d.objid=c.oid AND d.deptype='e') \
         ORDER BY 1",
    )
    .bind(&classified)
    .fetch_all(&mut *connection)
    .await?;
    let missing_tables: Vec<String> = sqlx::query_scalar(
        "SELECT t FROM unnest($1::text[]) t WHERE NOT EXISTS (SELECT 1 FROM pg_class c \
         WHERE c.relnamespace=(SELECT oid FROM pg_namespace WHERE nspname=current_schema()) \
         AND c.relkind IN ('r','p') AND c.relname=t) ORDER BY 1",
    )
    .bind(&required)
    .fetch_all(&mut *connection)
    .await?;
    let missing_columns: Vec<String> = sqlx::query_scalar(
        "SELECT x.t||'.'||x.col FROM unnest($1::text[],$2::text[]) x(t,col) \
         WHERE NOT EXISTS (SELECT 1 FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid \
         WHERE c.relnamespace=(SELECT oid FROM pg_namespace WHERE nspname=current_schema()) \
         AND c.relname=x.t AND a.attname=x.col AND a.attnum>0 AND NOT a.attisdropped) \
         ORDER BY 1",
    )
    .bind(&tables)
    .bind(&columns)
    .fetch_all(&mut *connection)
    .await?;
    Ok(SchemaCoverage {
        unclassified,
        missing_tables,
        missing_columns,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn every_table_is_classified_once_and_the_copied_ones_are_the_peer_syncs() {
        let mut seen = HashSet::new();
        for entry in TABLE_INVENTORY {
            assert!(seen.insert(entry.table), "{} is listed twice", entry.table);
        }
        let copied: Vec<&str> = TABLE_INVENTORY
            .iter()
            .filter(|entry| matches!(entry.class, TableClass::Copied(_)))
            .map(|entry| entry.table)
            .collect();
        assert_eq!(
            copied, COPIED_TABLES,
            "the copied tables, in the sync's order"
        );
        assert_eq!(
            entry("qbit_share_ledger").map(|e| e.class),
            Some(TableClass::Copied(&[]))
        );
        assert_eq!(
            entry("qbit_share_ledger_p0"),
            None,
            "partitions are their parent's"
        );
    }

    #[test]
    fn only_pair_wide_tables_are_optional_and_only_the_identity_state_is_rewritten() {
        for entry in TABLE_INVENTORY.iter().filter(|entry| entry.optional) {
            assert_eq!(entry.class, TableClass::PairWide, "{}", entry.table);
        }
        let rewritten: Vec<&str> = TABLE_INVENTORY
            .iter()
            .filter(|entry| entry.class == TableClass::Local(Reset::Rewrite))
            .map(|entry| entry.table)
            .collect();
        // Ledger::repersonalise_node_identity writes exactly these.
        assert_eq!(
            rewritten,
            [
                "qbit_prism_node_identity",
                "qbit_prism_node_lineage",
                "qbit_prism_peer_sync_cursors"
            ]
        );
    }

    #[test]
    fn deferred_shares_are_cleared_before_the_outbox_they_reference() {
        let position = |table: &str| {
            TABLE_INVENTORY
                .iter()
                .position(|entry| entry.table == table)
                .unwrap()
        };
        for table in ["qbit_prism_deferred_shares", "qbit_block_candidate_outbox"] {
            assert_eq!(
                entry(table).unwrap().class,
                TableClass::Local(Reset::Clear),
                "{table}"
            );
        }
        assert!(position("qbit_prism_deferred_shares") < position("qbit_block_candidate_outbox"));
    }

    #[test]
    fn a_fanout_claim_is_handed_back_as_its_holder_releases_it() {
        let Some(TableClass::Copied(parts)) =
            entry("qbit_ctv_fanout_artifacts").map(|entry| entry.class)
        else {
            panic!("fanout artifacts are copied");
        };
        let reset: Vec<_> = parts
            .iter()
            .filter_map(|part| match part {
                LocalPart::Reset { columns, set, .. } => Some((*columns, *set)),
                _ => None,
            })
            .collect();
        let [(columns, set)] = reset[..] else {
            panic!("one reset: {reset:?}");
        };
        assert!(
            set.starts_with(super::super::fanout::CLEAR_FANOUT_CLAIM_SQL),
            "{set}"
        );
        for column in columns {
            assert!(set.contains(&format!("{column}=")), "{column} is reset");
        }
        let named = named_columns();
        let unique: HashSet<_> = named.iter().collect();
        assert_eq!(unique.len(), named.len(), "a column is named once");
    }
}
