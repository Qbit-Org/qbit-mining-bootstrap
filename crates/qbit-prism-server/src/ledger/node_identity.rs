//! Which 3.1 dual-writer node a database is (migration 027, CONTRACT D-9):
//! the never-copied `qbit_prism_node_identity` row, and the personalisation
//! `qbit-prism-server node-identity set --index N` performs when it writes
//! that row during the bootstrap or cutover. A frontend never writes either;
//! in dual mode it only checks them ([`Ledger::check_node_identity`]).
//!
//! Personalising changes no statement PRISM runs. It sets, in this database
//! only, what those statements draw on:
//! - every copied table's `origin_node` default, so each row this node writes
//!   carries its index;
//! - the parity of the sequences whose values are keys of copied tables
//!   (`share_seq`, `payout_entry_seq`, `carry_forward_seq`): node A allocates
//!   even values and node B odd, each above every value the database holds;
//! - the session sequence's range: node A hands out extranonce1 values in
//!   `[1, 2^31-1]` and node B in `[2^31, 2^32-1]`, cycling within its half,
//!   where migration 009's reservations keep a wrapped value from being reused;
//! - `qbit_prism_node_lineage`'s floors, where this node's own rows start.
//!
//! A database nobody personalised keeps 3.0's defaults and sequences.
//!
//! A node whose disk was replaced is rebuilt from a physical copy of its
//! peer's database, which says it is the peer. `qbit-prism-server
//! node-identity repersonalise --index N` makes such a copy node N's
//! (CONTRACT D-16, [`Ledger::repersonalise_node_identity`]), resetting what
//! the table inventory (`ledger/table_inventory.rs`) classifies as the
//! peer's own state.
use super::table_inventory::{self, LocalPart, TableClass, TABLE_INVENTORY};
use super::*;
use crate::metrics::OrderLockHolder;
use crate::node_identity::NodeIndex;
use crate::peer_sync::COPIED_TABLES;

/// The serial columns that are keys of copied tables, each drawn with its
/// node's parity once personalised.
pub(super) const PARITY_KEYS: &[(&str, &str)] = &[
    ("qbit_share_ledger", "share_seq"),
    ("qbit_pool_payout_entries", "payout_entry_seq"),
    ("qbit_payout_carry_forward", "carry_forward_seq"),
];
/// The sequence `new_session_id` draws extranonce1 values from.
const SESSION_SEQUENCE: &str = "qbit_prism_session_sequence";
/// The system identifier and WAL timeline of the server a connection runs
/// on ([`LineageEvidence`]).
const LINEAGE_EVIDENCE_SQL: &str = "SELECT (SELECT system_identifier FROM pg_control_system()),\
     ('x'||substr(pg_walfile_name(pg_current_wal_lsn()),1,8))::bit(32)::int";
/// How recent a heartbeat shows a frontend running on this database: an
/// instance row neither drained nor stopped whose heartbeat is younger, by
/// this database's clock, refuses re-personalisation. A frontend heartbeats
/// every `PRISM_HEALTH_REFRESH_SECONDS` (2 by default). A row a physical copy
/// carried from the peer's own frontends ages from the moment the copy
/// stopped following the peer.
const RUNNING_HEARTBEAT_SECONDS: f64 = 60.0;

/// The `qbit_prism_node_identity` row: which node this database is.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct NodeIdentityRecord {
    #[serde(rename = "node_index")]
    pub node: NodeIndex,
    pub recorded_at: DateTime<Utc>,
    pub recorded_by: String,
}

/// The server a database ran on when its own log was last proved complete
/// (CONTRACT D-17): a restore or promotion changes the timeline, an initdb
/// the identifier, and a plain restart neither.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct LineageEvidence {
    pub system_identifier: i64,
    pub timeline: i32,
}

/// The `qbit_prism_node_lineage` row: where this node's own rows start, and
/// when its own log was last proved complete.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct NodeLineage {
    pub share_seq_floor: i64,
    pub sync_seq_floor: i64,
    pub personalised_at: DateTime<Utc>,
    pub verified: Option<(LineageEvidence, DateTime<Utc>)>,
}

/// What a dual-writer frontend found its database to be.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdentityCheck {
    /// Personalised as this node, with nothing lost since.
    Ready(NodeIdentityRecord),
    /// No identity row: `node-identity set` has not run on this database.
    Unidentified,
    /// Personalised as the other node: this frontend's database URL names its
    /// peer's database, or the database is a copy of the peer's.
    OtherNode(NodeIdentityRecord),
    /// Personalised as this node, but an origin default, a key sequence's
    /// parity or the session range has been changed since; each named.
    Drifted(NodeIdentityRecord, Vec<String>),
}

/// The rows a re-personalisation deleted or reset in one table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TableRows {
    pub table: String,
    pub rows: u64,
}

/// A `qbit_prism_peer_sync_cursors` row: how far this node has pulled one
/// stream of the peer's rows, in the stream's key on the peer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PeerSyncCursor {
    pub stream: String,
    pub peer_node: NodeIndex,
    pub scanned_through: i64,
    pub ingested_through: Option<i64>,
}

/// What `node-identity repersonalise` made of a physical copy of the peer's
/// database.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Repersonalisation {
    /// The identity the copy carried: the peer's.
    pub copied_identity: NodeIdentityRecord,
    /// This node's identity, as now recorded.
    pub identity: NodeIdentityRecord,
    /// The lineage's floors: every `share_seq` and `sync_seq` this node
    /// draws from now on is above them.
    pub share_seq_floor: i64,
    pub sync_seq_floor: i64,
    /// Rows deleted, in the order deleted: every local table the inventory
    /// clears, and the local rows of copied tables.
    pub cleared: Vec<TableRows>,
    /// Rows of copied tables whose local columns were reset.
    pub reset: Vec<TableRows>,
    /// Where this node's pulls of the peer's streams start.
    pub cursors: Vec<PeerSyncCursor>,
}

const IDENTITY_COLUMNS: &str = "node_index,recorded_at,recorded_by";
const LINEAGE_COLUMNS: &str = "share_seq_floor,sync_seq_floor,personalised_at,\
     verified_system_identifier,verified_timeline,verified_at";

fn identity_from_row(row: &PgRow) -> Result<NodeIdentityRecord> {
    let index: i16 = row.try_get("node_index")?;
    Ok(NodeIdentityRecord {
        node: NodeIndex::from_index(index.into())
            .with_context(|| format!("qbit_prism_node_identity holds node index {index}"))?,
        recorded_at: row.try_get("recorded_at")?,
        recorded_by: row.try_get("recorded_by")?,
    })
}

fn lineage_from_row(row: &PgRow) -> Result<NodeLineage> {
    let identifier: Option<i64> = row.try_get("verified_system_identifier")?;
    let timeline: Option<i32> = row.try_get("verified_timeline")?;
    let at: Option<DateTime<Utc>> = row.try_get("verified_at")?;
    Ok(NodeLineage {
        share_seq_floor: row.try_get("share_seq_floor")?,
        sync_seq_floor: row.try_get("sync_seq_floor")?,
        personalised_at: row.try_get("personalised_at")?,
        verified: match (identifier, timeline, at) {
            (Some(system_identifier), Some(timeline), Some(at)) => Some((
                LineageEvidence {
                    system_identifier,
                    timeline,
                },
                at,
            )),
            _ => None,
        },
    })
}

impl Ledger {
    /// CONTRACT D-12: a single-writer frontend refuses a database that has
    /// run as a dual-writer node (its carry-owner journal holds rows), where
    /// it would pay carried balances beside the carry owner, unless
    /// `downgrade` (`PRISM_DUAL_WRITER_DOWNGRADE`) says this is the
    /// deliberate rollback to a single writer.
    pub async fn refuse_single_writer_on_dual_ledger(&self, downgrade: bool) -> Result<()> {
        let journal: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_prism_node_roles")
            .fetch_one(&mut *self.acquire().await?)
            .await?;
        if journal == 0 {
            return Ok(());
        }
        ensure!(
            downgrade,
            "this database has run as a dual-writer node (qbit_prism_node_roles holds {journal} \
             rows), so a single-writer frontend (PRISM_DUAL_WRITER off) would pay carried \
             balances beside the carry owner. Start it with PRISM_DUAL_WRITER=1, or set \
             PRISM_DUAL_WRITER_DOWNGRADE=1 for the deliberate rollback to one writer"
        );
        tracing::warn!(
            journal_rows = journal,
            "PRISM_DUAL_WRITER_DOWNGRADE: starting a single writer on a database that has run as \
             a dual-writer node"
        );
        Ok(())
    }

    /// The node this database was personalised as, if any.
    pub async fn recorded_node_identity(&self) -> Result<Option<NodeIdentityRecord>> {
        sqlx::query(&format!(
            "SELECT {IDENTITY_COLUMNS} FROM qbit_prism_node_identity WHERE singleton"
        ))
        .fetch_optional(&mut *self.acquire().await?)
        .await?
        .as_ref()
        .map(identity_from_row)
        .transpose()
    }

    /// Where this node's own rows start, and the last own-log verification.
    pub async fn node_lineage(&self) -> Result<Option<NodeLineage>> {
        sqlx::query(&format!(
            "SELECT {LINEAGE_COLUMNS} FROM qbit_prism_node_lineage WHERE singleton"
        ))
        .fetch_optional(&mut *self.acquire().await?)
        .await?
        .as_ref()
        .map(lineage_from_row)
        .transpose()
    }

    /// The server this database runs on now, as [`LineageEvidence`]. The
    /// timeline is the one WAL is written on, which a promotion changes at
    /// once; `pg_control_checkpoint()` reports it only after the next
    /// checkpoint.
    pub async fn lineage_evidence(&self) -> Result<LineageEvidence> {
        let (system_identifier, timeline): (i64, i32) = sqlx::query_as(LINEAGE_EVIDENCE_SQL)
            .fetch_one(&mut *self.acquire().await?)
            .await?;
        Ok(LineageEvidence {
            system_identifier,
            timeline,
        })
    }

    /// What a dual-writer frontend configured as `node` finds this database
    /// to be. Reads only; a frontend never personalises its database.
    pub async fn check_node_identity(&self, node: NodeIndex) -> Result<IdentityCheck> {
        let mut tx = self.begin().await?;
        let recorded = sqlx::query(&format!(
            "SELECT {IDENTITY_COLUMNS} FROM qbit_prism_node_identity WHERE singleton"
        ))
        .fetch_optional(&mut *tx)
        .await?
        .as_ref()
        .map(identity_from_row)
        .transpose()?;
        let check = match recorded {
            None => IdentityCheck::Unidentified,
            Some(record) if record.node != node => IdentityCheck::OtherNode(record),
            Some(record) => {
                let drift = drift(&mut tx, node).await?;
                if drift.is_empty() {
                    IdentityCheck::Ready(record)
                } else {
                    IdentityCheck::Drifted(record, drift)
                }
            }
        };
        tx.rollback().await?;
        Ok(check)
    }

    /// `qbit-prism-server node-identity set --index N`: make this database
    /// `node`'s, or confirm that it already is. The bootstrap and cutover steps
    /// run it once per database, before the node's first dual-writer start.
    /// Run again on a database that is already `node`'s, it restores anything
    /// lost since (an origin default, a key sequence's parity, the session
    /// range) and changes nothing else. A database personalised as the other
    /// node is refused: a physical copy of the peer is re-personalised
    /// deliberately, by its own command ([`Ledger::repersonalise_node_identity`]).
    ///
    /// Takes `SETTLEMENT_LOCK` and `ORDER_LOCK`, so no landing or share
    /// append draws a key while the sequences change.
    pub async fn set_node_identity(
        &self,
        node: NodeIndex,
        recorded_by: &str,
    ) -> Result<NodeIdentityRecord> {
        let mut tx = self.begin().await?;
        self.lock(&mut tx, SETTLEMENT_LOCK).await?;
        let _order = self.lock_order(&mut tx, OrderLockHolder::PeerSync).await?;
        let existing = sqlx::query(&format!(
            "SELECT {IDENTITY_COLUMNS} FROM qbit_prism_node_identity WHERE singleton FOR UPDATE"
        ))
        .fetch_optional(&mut *tx)
        .await?
        .as_ref()
        .map(identity_from_row)
        .transpose()?;
        if let Some(record) = &existing {
            ensure!(
                record.node == node,
                "this database is already dual-writer node {} (qbit_prism_node_identity, recorded \
                 at {} by {}); refusing to make it node {node}. A physical copy of the peer's \
                 database, rebuilt for this node, is re-personalised deliberately with \
                 `qbit-prism-server node-identity repersonalise --index {}`",
                record.node,
                record.recorded_at,
                record.recorded_by,
                node.index(),
            );
            if drift(&mut tx, node).await?.is_empty() {
                tx.rollback().await?;
                return Ok(existing.expect("checked above"));
            }
        }
        let share_seq_floor = personalise_keys(&mut tx, node, 0).await?;
        let record = match existing {
            Some(record) => {
                tracing::warn!(
                    node = %node,
                    "restored this dual-writer node's personalisation, which the database had lost"
                );
                record
            }
            None => {
                let sync_seq_floor: i64 = sqlx::query_scalar(
                    "SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM qbit_prism_sync_seq",
                )
                .fetch_one(&mut *tx)
                .await?;
                sqlx::query(
                    "INSERT INTO qbit_prism_node_lineage(share_seq_floor,sync_seq_floor) VALUES($1,$2)",
                )
                .bind(share_seq_floor)
                .bind(sync_seq_floor)
                .execute(&mut *tx)
                .await?;
                let row = sqlx::query(&format!(
                    "INSERT INTO qbit_prism_node_identity(node_index,recorded_by) VALUES($1,$2) \
                     RETURNING {IDENTITY_COLUMNS}"
                ))
                .bind(node.index())
                .bind(recorded_by)
                .fetch_one(&mut *tx)
                .await?;
                identity_from_row(&row)?
            }
        };
        tx.commit().await?;
        Ok(record)
    }

    /// `qbit-prism-server node-identity repersonalise --index N` (CONTRACT
    /// D-16): make a physical copy of the peer's database, promoted to rebuild
    /// node `node` after its disk was replaced, `node`'s. One transaction,
    /// under `SETTLEMENT_LOCK` and `ORDER_LOCK`, that:
    ///
    /// - refuses unless the database's identity names `node`'s peer (the copy
    ///   says it is the peer), while a frontend may be running on it (an
    ///   instance row neither drained nor stopped whose heartbeat is younger
    ///   than `RUNNING_HEARTBEAT_SECONDS`), when it is the server the peer
    ///   last proved its own log on (the peer's own database, not a promoted
    ///   copy), and when the table inventory does not classify every table;
    /// - resets the peer's own state as the inventory says: clears the local
    ///   tables it clears, deletes the local rows of copied tables and hands
    ///   back the peer's fanout claims, keeping everything else;
    /// - personalises the keys as `set` does, with every key above the
    ///   positions the peer reached in this node's old streams, so the peer
    ///   pulls each row this node writes from now on;
    /// - records the identity, the floors with no own-log verification (the
    ///   check runs before the node serves, D-8), and cursors from which this
    ///   node's first pull of each of the peer's streams starts: every peer
    ///   share at or below the shares cursor is in the copy (the peer's own
    ///   rows commit in `share_seq` order, D-14), and the block and prepared
    ///   streams, whose rows commit in no `sync_seq` order, are read again from
    ///   the peer's floor, each row the copy holds skipped.
    pub async fn repersonalise_node_identity(
        &self,
        node: NodeIndex,
        recorded_by: &str,
    ) -> Result<Repersonalisation> {
        let peer = node.peer();
        let mut tx = self.begin().await?;
        self.lock(&mut tx, SETTLEMENT_LOCK).await?;
        let _order = self.lock_order(&mut tx, OrderLockHolder::PeerSync).await?;
        // Registrations and heartbeats wait for this transaction, so the scan
        // below misses no frontend, as in fatal-state clear.
        sqlx::query("LOCK TABLE qbit_prism_instances IN SHARE ROW EXCLUSIVE MODE")
            .execute(&mut *tx)
            .await?;
        let copied_identity = sqlx::query(&format!(
            "SELECT {IDENTITY_COLUMNS} FROM qbit_prism_node_identity WHERE singleton FOR UPDATE"
        ))
        .fetch_optional(&mut *tx)
        .await?
        .as_ref()
        .map(identity_from_row)
        .transpose()?;
        let copied_identity = match copied_identity {
            None => bail!(
                "this database has no dual-writer node identity, so it is no physical copy of \
                 node {peer}'s database, which would say it is node {peer}; nothing was changed. \
                 A database nobody personalised is made node {node}'s with \
                 `qbit-prism-server node-identity set --index {}`",
                node.index()
            ),
            Some(record) if record.node == node => bail!(
                "this database is already dual-writer node {node} (recorded at {} by {}); there \
                 is nothing to repersonalise and nothing was changed. \
                 `qbit-prism-server node-identity set --index {}` restores anything its \
                 personalisation lost",
                record.recorded_at,
                record.recorded_by,
                node.index()
            ),
            Some(record) => record,
        };
        let peer_lineage = sqlx::query(&format!(
            "SELECT {LINEAGE_COLUMNS} FROM qbit_prism_node_lineage WHERE singleton FOR UPDATE"
        ))
        .fetch_optional(&mut *tx)
        .await?
        .as_ref()
        .map(lineage_from_row)
        .transpose()?
        .with_context(|| {
            format!(
                "this database is dual-writer node {peer}'s but has no qbit_prism_node_lineage \
                 row, which node-identity set records with the identity; refusing to \
                 repersonalise it, nothing was changed"
            )
        })?;

        let mut refusals = Vec::new();
        let coverage = table_inventory::schema_coverage(&mut tx).await?;
        if !coverage.unclassified.is_empty() {
            refusals.push(format!(
                "the dual-writer table inventory (ledger/table_inventory.rs) does not classify \
                 {}, so nothing says whether node {peer}'s own state is in it",
                coverage.unclassified.join(", ")
            ));
        }
        let missing: Vec<&String> = coverage
            .missing_tables
            .iter()
            .chain(&coverage.missing_columns)
            .collect();
        if !missing.is_empty() {
            refusals.push(format!(
                "the schema lacks {} that the table inventory names; migrate this database first",
                missing
                    .iter()
                    .map(|name| name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        let running: Vec<(String, Option<String>, f64)> = sqlx::query_as(
            "SELECT instance_id,status->>'state',\
             extract(epoch FROM clock_timestamp()-heartbeat_at)::float8 \
             FROM qbit_prism_instances \
             WHERE COALESCE(status->>'state','') NOT IN ('drained','stopped') \
             AND heartbeat_at>clock_timestamp()-$1*interval '1 second' ORDER BY instance_id",
        )
        .bind(RUNNING_HEARTBEAT_SECONDS)
        .fetch_all(&mut *tx)
        .await?;
        if !running.is_empty() {
            let named: Vec<String> = running
                .iter()
                .map(|(id, state, age)| {
                    format!(
                        "{id} ({}, heartbeat {age:.3} seconds old)",
                        state.as_deref().unwrap_or("running")
                    )
                })
                .collect();
            refusals.push(format!(
                "a frontend may be running on this database: {}. Stop every PRISM process that \
                 uses it. A row this copy carried from node {peer}'s own frontends stops counting \
                 once its heartbeat is {RUNNING_HEARTBEAT_SECONDS} seconds old by this \
                 database's clock",
                named.join(", ")
            ));
        }
        let (system_identifier, timeline): (i64, i32) = sqlx::query_as(LINEAGE_EVIDENCE_SQL)
            .fetch_one(&mut *tx)
            .await?;
        let here = LineageEvidence {
            system_identifier,
            timeline,
        };
        if let Some((verified, at)) = &peer_lineage.verified {
            if *verified == here {
                refusals.push(format!(
                    "this is the server node {peer} last proved its own log on (system identifier \
                     {system_identifier}, timeline {timeline}, at {at}): node {peer}'s own \
                     database, not a copy promoted for node {node}. Repersonalise only the rebuilt \
                     node's database, once it is promoted"
                ));
            }
        }
        ensure!(
            refusals.is_empty(),
            "node-identity repersonalise refused, nothing was changed: {}",
            refusals.join("; ")
        );

        // How far the peer got in this node's old streams: everything this
        // node writes from now on must be above it, or the peer would never
        // pull it.
        let (seen_shares, seen_sync): (Option<i64>, Option<i64>) = sqlx::query_as(
            "SELECT max(GREATEST(scanned_through,ingested_through)) FILTER (WHERE stream='shares'),\
             max(GREATEST(scanned_through,ingested_through)) FILTER (WHERE stream<>'shares') \
             FROM qbit_prism_peer_sync_cursors WHERE peer_node=$1",
        )
        .bind(node.index())
        .fetch_one(&mut *tx)
        .await?;
        // The peer's own rows commit in share_seq order (D-14), so the copy
        // holds every peer share at or below the newest it holds. Read in
        // the shape only 031's index serves, never a walk of this node's
        // rows under the locks held here.
        let peer_shares: Option<i64> = sqlx::query_scalar(super::peer_sync::HIGHEST_SHARE)
            .bind(peer.index())
            .fetch_optional(&mut *tx)
            .await?;

        let mut cleared = Vec::new();
        let mut reset = Vec::new();
        for entry in TABLE_INVENTORY {
            match entry.class {
                TableClass::Local(table_inventory::Reset::Clear) => {
                    let rows = sqlx::query(&format!("DELETE FROM {}", entry.table))
                        .execute(&mut *tx)
                        .await?
                        .rows_affected();
                    cleared.push(TableRows {
                        table: entry.table.into(),
                        rows,
                    });
                }
                TableClass::Copied(parts) => {
                    for part in parts {
                        match part {
                            LocalPart::Rows { predicate } => {
                                let rows = sqlx::query(&format!(
                                    "DELETE FROM {} WHERE {predicate}",
                                    entry.table
                                ))
                                .execute(&mut *tx)
                                .await?
                                .rows_affected();
                                cleared.push(TableRows {
                                    table: entry.table.into(),
                                    rows,
                                });
                            }
                            LocalPart::Reset {
                                rows: predicate,
                                set,
                                ..
                            } => {
                                let rows = sqlx::query(&format!(
                                    "UPDATE {} SET {set} WHERE {predicate}",
                                    entry.table
                                ))
                                .execute(&mut *tx)
                                .await?
                                .rows_affected();
                                reset.push(TableRows {
                                    table: entry.table.into(),
                                    rows,
                                });
                            }
                            LocalPart::Kept(_) => {}
                        }
                    }
                }
                TableClass::Local(_) | TableClass::PairWide => {}
            }
        }

        let share_seq_floor = personalise_keys(&mut tx, node, seen_shares.unwrap_or(0)).await?;
        let (position, sync_seq_floor): (i64, i64) = sqlx::query_as(
            "WITH sequence AS (SELECT CASE WHEN is_called THEN last_value ELSE 0 END AS position \
             FROM qbit_prism_sync_seq) \
             SELECT position,GREATEST(position,\
             (SELECT max(sync_seq) FROM qbit_pool_blocks WHERE origin_node=$1 AND sync_seq IS NOT NULL),\
             (SELECT max(sync_seq) FROM qbit_prism_jobs WHERE origin_node=$1 AND sync_seq IS NOT NULL \
             AND job_id LIKE 'prepared:%'),$2::bigint) FROM sequence",
        )
        .bind(node.index())
        .bind(seen_sync)
        .fetch_one(&mut *tx)
        .await?;
        if sync_seq_floor > position {
            sqlx::query("SELECT setval('qbit_prism_sync_seq',$1,true)")
                .bind(sync_seq_floor)
                .execute(&mut *tx)
                .await?;
        }

        let row = sqlx::query(&format!(
            "UPDATE qbit_prism_node_identity SET node_index=$1,recorded_at=clock_timestamp(),\
             recorded_by=$2 WHERE singleton RETURNING {IDENTITY_COLUMNS}"
        ))
        .bind(node.index())
        .bind(recorded_by)
        .fetch_one(&mut *tx)
        .await?;
        let identity = identity_from_row(&row)?;
        sqlx::query("DELETE FROM qbit_prism_node_lineage")
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "INSERT INTO qbit_prism_node_lineage(share_seq_floor,sync_seq_floor) VALUES($1,$2)",
        )
        .bind(share_seq_floor)
        .bind(sync_seq_floor)
        .execute(&mut *tx)
        .await?;
        let cursors = vec![
            PeerSyncCursor {
                stream: "shares".into(),
                peer_node: peer,
                scanned_through: peer_shares.map_or(peer_lineage.share_seq_floor, |held| {
                    held.max(peer_lineage.share_seq_floor)
                }),
                ingested_through: peer_shares,
            },
            PeerSyncCursor {
                stream: "blocks".into(),
                peer_node: peer,
                scanned_through: peer_lineage.sync_seq_floor,
                ingested_through: None,
            },
            PeerSyncCursor {
                stream: "prepared".into(),
                peer_node: peer,
                scanned_through: peer_lineage.sync_seq_floor,
                ingested_through: None,
            },
        ];
        sqlx::query("DELETE FROM qbit_prism_peer_sync_cursors")
            .execute(&mut *tx)
            .await?;
        for cursor in &cursors {
            sqlx::query(
                "INSERT INTO qbit_prism_peer_sync_cursors(stream,peer_node,scanned_through,\
                 ingested_through) VALUES($1,$2,$3,$4)",
            )
            .bind(&cursor.stream)
            .bind(cursor.peer_node.index())
            .bind(cursor.scanned_through)
            .bind(cursor.ingested_through)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        tracing::warn!(
            node = %node,
            copied_from = %peer,
            "repersonalised a physical copy of node {peer}'s database as dual-writer node {node}"
        );
        Ok(Repersonalisation {
            copied_identity,
            identity,
            share_seq_floor,
            sync_seq_floor,
            cleared,
            reset,
            cursors,
        })
    }
}

/// Personalise the keys of `node`'s database: every copied table's
/// `origin_node` default, the parity of the key sequences above every key
/// held and above `shares_above` for `share_seq`, and the session range.
/// Returns the `share_seq` floor: every share the node appends from now on is
/// above it.
async fn personalise_keys(
    tx: &mut Transaction<'_, Postgres>,
    node: NodeIndex,
    shares_above: i64,
) -> Result<i64> {
    let mut share_seq_floor = 0;
    for (table, column) in PARITY_KEYS {
        if *table == "qbit_share_ledger" {
            share_seq_floor = set_parity(tx, table, column, node, shares_above).await?;
        } else {
            set_parity(tx, table, column, node, 0).await?;
        }
    }
    set_session_range(tx, node).await?;
    for table in COPIED_TABLES {
        sqlx::raw_sql(&format!(
            "ALTER TABLE {table} ALTER COLUMN origin_node SET DEFAULT {}",
            node.index()
        ))
        .execute(&mut **tx)
        .await?;
    }
    Ok(share_seq_floor)
}

/// The sequence behind `table.column`.
async fn serial_sequence(
    tx: &mut Transaction<'_, Postgres>,
    table: &str,
    column: &str,
) -> Result<String> {
    sqlx::query_scalar::<_, Option<String>>("SELECT pg_get_serial_sequence($1,$2)")
        .bind(table)
        .bind(column)
        .fetch_one(&mut **tx)
        .await?
        .with_context(|| format!("{table}.{column} draws from no sequence"))
}

/// Give `table.column`'s sequence `node`'s parity: step 2, continuing from
/// the smallest value of that parity at or above what the sequence has
/// handed out, the largest key the table holds and `at_least`. Returns that
/// value; every key the node draws afterwards is above it.
async fn set_parity(
    tx: &mut Transaction<'_, Postgres>,
    table: &str,
    column: &str,
    node: NodeIndex,
    at_least: i64,
) -> Result<i64> {
    let sequence = serial_sequence(tx, table, column).await?;
    let (last_value, held): (i64, Option<i64>) = sqlx::query_as(&format!(
        "SELECT (SELECT last_value FROM {sequence}),(SELECT max({column}) FROM {table})"
    ))
    .fetch_one(&mut **tx)
    .await?;
    let floor = node.share_seq_at_or_above(last_value.max(held.unwrap_or(0)).max(at_least));
    sqlx::raw_sql(&format!("ALTER SEQUENCE {sequence} INCREMENT BY 2"))
        .execute(&mut **tx)
        .await?;
    sqlx::query("SELECT setval($1::regclass,$2,true)")
        .bind(&sequence)
        .bind(floor)
        .execute(&mut **tx)
        .await?;
    Ok(floor)
}

/// Raise `table.column`'s sequence, keeping `node`'s parity, to at least
/// `floor` and, when `scan` is set, above the largest key the table holds.
/// It only rises: a sequence already above stays where it is. Returns the
/// sequence's value afterwards. The caller holds the lock its writers
/// allocate under, so no allocation is in flight while it moves.
pub(super) async fn raise_parity(
    tx: &mut Transaction<'_, Postgres>,
    table: &str,
    column: &str,
    node: NodeIndex,
    floor: Option<i64>,
    scan: bool,
) -> Result<i64> {
    let sequence = serial_sequence(tx, table, column).await?;
    let held: Option<i64> = if scan {
        sqlx::query_scalar(&format!("SELECT max({column}) FROM {table}"))
            .fetch_one(&mut **tx)
            .await?
    } else {
        None
    };
    let (last_value, is_called): (i64, bool) =
        sqlx::query_as(&format!("SELECT last_value,is_called FROM {sequence}"))
            .fetch_one(&mut **tx)
            .await?;
    let wanted = node.share_seq_at_or_above(
        floor
            .unwrap_or(i64::MIN)
            .max(held.unwrap_or(i64::MIN))
            .max(last_value),
    );
    if is_called && wanted == last_value {
        return Ok(last_value);
    }
    sqlx::query("SELECT setval($1::regclass,$2,true)")
        .bind(&sequence)
        .bind(wanted)
        .execute(&mut **tx)
        .await?;
    Ok(wanted)
}

/// Confine the session sequence to `node`'s half of the extranonce1 space,
/// continuing from its current value when that is in the half.
async fn set_session_range(tx: &mut Transaction<'_, Postgres>, node: NodeIndex) -> Result<()> {
    let range = node.extranonce1_range();
    let (low, high) = (i64::from(*range.start()), i64::from(*range.end()));
    let (last_value, is_called): (i64, bool) = sqlx::query_as(&format!(
        "SELECT last_value,is_called FROM {SESSION_SEQUENCE}"
    ))
    .fetch_one(&mut **tx)
    .await?;
    let next = match (last_value, is_called) {
        (value, false) if (low..=high).contains(&value) => value,
        (value, true) if (low..high).contains(&value) => value + 1,
        _ => low,
    };
    sqlx::raw_sql(&format!(
        "ALTER SEQUENCE {SESSION_SEQUENCE} MINVALUE {low} MAXVALUE {high} START WITH {low} \
         RESTART WITH {next} CYCLE"
    ))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// What `node`'s personalisation would change in this database, each named.
async fn drift(tx: &mut Transaction<'_, Postgres>, node: NodeIndex) -> Result<Vec<String>> {
    let mut drift = Vec::new();
    let expected_default = node.index().to_string();
    for table in COPIED_TABLES {
        let default: Option<String> = sqlx::query_scalar(
            "SELECT pg_get_expr(d.adbin,d.adrelid) FROM pg_attribute a \
             LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum \
             WHERE a.attrelid=$1::regclass AND a.attname='origin_node' AND NOT a.attisdropped",
        )
        .bind(table)
        .fetch_optional(&mut **tx)
        .await?
        .with_context(|| format!("{table} has no origin_node column; migrate to 027 first"))?;
        if default.as_deref() != Some(expected_default.as_str()) {
            drift.push(format!("{table}.origin_node default"));
        }
    }
    for (table, column) in PARITY_KEYS {
        let sequence = serial_sequence(tx, table, column).await?;
        let (increment, last_value): (i64, i64) = sqlx::query_as(&format!(
            "SELECT s.seqincrement,(SELECT last_value FROM {sequence}) FROM pg_sequence s \
             WHERE s.seqrelid=$1::regclass"
        ))
        .bind(&sequence)
        .fetch_one(&mut **tx)
        .await?;
        if increment != 2 || last_value.rem_euclid(2) != node.share_seq_residue() {
            drift.push(format!("{table}.{column} parity"));
        }
    }
    let range = node.extranonce1_range();
    let (minimum, maximum, cycles): (i64, i64, bool) = sqlx::query_as(
        "SELECT seqmin,seqmax,seqcycle FROM pg_sequence WHERE seqrelid=$1::regclass",
    )
    .bind(SESSION_SEQUENCE)
    .fetch_one(&mut **tx)
    .await?;
    if minimum != i64::from(*range.start()) || maximum != i64::from(*range.end()) || !cycles {
        drift.push(format!("{SESSION_SEQUENCE} range"));
    }
    Ok(drift)
}
