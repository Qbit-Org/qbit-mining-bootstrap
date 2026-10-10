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
        let (system_identifier, timeline): (i64, i32) = sqlx::query_as(
            "SELECT (SELECT system_identifier FROM pg_control_system()),\
             ('x'||substr(pg_walfile_name(pg_current_wal_lsn()),1,8))::bit(32)::int",
        )
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
    /// deliberately, by its own command.
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
                 database is re-personalised deliberately, not with node-identity set",
                record.node,
                record.recorded_at,
                record.recorded_by,
            );
            if drift(&mut tx, node).await?.is_empty() {
                tx.rollback().await?;
                return Ok(existing.expect("checked above"));
            }
        }
        let mut share_seq_floor = 0;
        for (table, column) in PARITY_KEYS {
            let floor = set_parity(&mut tx, table, column, node).await?;
            if *table == "qbit_share_ledger" {
                share_seq_floor = floor;
            }
        }
        set_session_range(&mut tx, node).await?;
        for table in COPIED_TABLES {
            sqlx::raw_sql(&format!(
                "ALTER TABLE {table} ALTER COLUMN origin_node SET DEFAULT {}",
                node.index()
            ))
            .execute(&mut *tx)
            .await?;
        }
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
/// the smallest value of that parity at or above both what the sequence has
/// handed out and the largest key the table holds. Returns that value; every
/// key the node draws afterwards is above it.
async fn set_parity(
    tx: &mut Transaction<'_, Postgres>,
    table: &str,
    column: &str,
    node: NodeIndex,
) -> Result<i64> {
    let sequence = serial_sequence(tx, table, column).await?;
    let (last_value, held): (i64, Option<i64>) = sqlx::query_as(&format!(
        "SELECT (SELECT last_value FROM {sequence}),(SELECT max({column}) FROM {table})"
    ))
    .fetch_one(&mut **tx)
    .await?;
    let floor = node.share_seq_at_or_above(last_value.max(held.unwrap_or(0)));
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
