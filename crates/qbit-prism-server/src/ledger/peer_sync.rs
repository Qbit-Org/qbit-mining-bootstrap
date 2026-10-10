//! The SQL of the 3.1 peer sync (CONTRACT §3, D-1 to D-15): what is read on
//! the peer, as its read-only sync role, and how this node applies it by
//! identity. The engine that schedules it is `crate::peer_sync`.
//!
//! Rows travel as `to_jsonb` documents and are applied with
//! `jsonb_populate_record(set)`, so every column keeps its exact value and
//! type (numerics, bytea, timestamps to the microsecond). A pull never
//! updates or deletes a local row: an identity this node already holds is
//! compared, and a difference is recorded in `qbit_prism_peer_sync_conflicts`
//! and alerted, never written over.
//!
//! Three streams, each with its own cursor in `qbit_prism_peer_sync_cursors`:
//! - `shares`: `qbit_share_ledger` rows and their header mappings, pulled in
//!   `share_seq` order. The cursor is a position in the peer's `share_seq`
//!   space, scanned over every row the peer holds whatever its origin. That
//!   is safe because each node commits its own rows in `share_seq` order
//!   under ORDER_LOCK and raises its own sequence above every peer row
//!   before inserting it: once a row at position p is visible on the peer,
//!   no row of the peer's below p can commit later. The cursor is therefore
//!   the safe peer mark (D-14, `qbit_prism_peer_share_mark()`).
//! - `blocks`: a landed block with every row its landing wrote, pulled in
//!   `sync_seq` order of the block row and applied whole (D-1, D-10).
//! - `prepared`: a prepared job with its template and balance blob (D-3,
//!   D-11, D-19), pulled in `sync_seq` order of the job row.
//!
//! The two `sync_seq` streams have no commit-order lock, so their pulls stop
//! at the sync barrier's mark: every `sync_seq` default takes the barrier
//! shared until its transaction ends, and the puller reads the sequence while
//! it holds the barrier exclusively, so every row at or below what it read is
//! committed or never will be ([`peer::sync_barrier`]). The carry-owner journal
//! (`qbit_prism_node_roles`, D-4) is small and is re-read whole each pass.
use super::*;
use crate::metrics::OrderLockHolder;
use crate::node_identity::NodeIndex;
use crate::peer_sync::COPIED_TABLES;
use sqlx::PgConnection;
use std::collections::{BTreeMap, BTreeSet};

/// The sync barrier: `qbit_prism_next_sync_seq()` takes it shared for the
/// rest of its transaction (migration 027), the puller exclusively.
pub const SYNC_BARRIER_LOCK: i64 = 0x505249534d000008;

pub const SHARES: &str = "shares";
pub const BLOCKS: &str = "blocks";
pub const PREPARED: &str = "prepared";

/// Which columns a pull carries for one copied table.
enum Carried {
    /// Every column but these, which are this node's own derived or local
    /// state and take their defaults (D-1).
    AllBut(&'static [&'static str]),
    /// Only these, the immutable facts of a landing (D-1); every other
    /// column takes its default.
    Only(&'static [&'static str]),
}

/// What a pull carries of each copied table. A sibling that adds a derived
/// or local column to a copied table names it here; a copied column needs
/// nothing, except on the two tables listed by their immutable facts.
fn carried(table: &str) -> Carried {
    match table {
        "qbit_pool_blocks" => Carried::Only(&[
            "block_hash",
            "block_height",
            "parent_hash",
            "coinbase_txid",
            "payout_manifest_sha256",
            "as_issued_audit_sha256",
            "found_at",
            "solver_miner_id",
            "solver_share_id",
            "solver_share_difficulty",
            "solver_network_difficulty",
            "origin_node",
            "sync_seq",
        ]),
        "qbit_ctv_fanout_artifacts" => Carried::Only(&[
            "fanout_txid",
            "block_hash",
            "manifest_set_sha256",
            "manifest_json",
            "manifest",
            "manifest_sha256",
            "precommitment_sha256",
            "ctv_hash",
            "commitment_witness_leaf_hex",
            "chunk_index",
            "chunk_count",
            "parent_coinbase_txid",
            "parent_coinbase_vout",
            "fanout_tx_template_hex",
            "fanout_tx_hex",
            "anchor_vout",
            "covenant_output_value_sats",
            "fanout_output_sum_sats",
            "origin_node",
        ]),
        "qbit_pool_payout_entries" | "qbit_payout_carry_forward" => {
            Carried::AllBut(&["maturity_state"])
        }
        _ => Carried::AllBut(&[]),
    }
}

/// The immutable facts compared when a peer block's `block_hash` is
/// already held here (D-10), its audit digest beside them.
const BLOCK_FACTS: &[&str] = &[
    "block_hash",
    "block_height",
    "parent_hash",
    "coinbase_txid",
    "payout_manifest_sha256",
    "as_issued_audit_sha256",
];
/// The content-determined columns of an audit snapshot (D-15): windows with
/// one share array share one row, whose anchor, cut, origin and creation
/// time may differ.
const SNAPSHOT_FACTS: &[&str] = &[
    "snapshot_sha256",
    "first_share_seq",
    "last_share_seq",
    "share_count",
    "inline_shares",
];

/// The columns of `table` this database has, in table order.
async fn local_columns(connection: &mut PgConnection, table: &str) -> Result<Vec<String>> {
    let columns: Vec<String> = sqlx::query_scalar(
        "SELECT attname::text FROM pg_attribute WHERE attrelid=$1::regclass AND attnum>0 \
         AND NOT attisdropped ORDER BY attnum",
    )
    .bind(table)
    .fetch_all(&mut *connection)
    .await?;
    ensure!(!columns.is_empty(), "{table} has no columns");
    Ok(columns)
}

/// The columns a pull carries of every copied table, as this database's
/// catalog has them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CarriedColumns(BTreeMap<&'static str, Vec<String>>);

impl CarriedColumns {
    /// Read from `connection`'s catalog: this node's, or the peer's.
    pub async fn read(connection: &mut PgConnection) -> Result<Self> {
        let mut carried_columns = BTreeMap::new();
        for table in COPIED_TABLES {
            let columns = local_columns(connection, table).await?;
            let kept: Vec<String> = match carried(table) {
                Carried::AllBut(local) => columns
                    .into_iter()
                    .filter(|column| !local.contains(&column.as_str()))
                    .collect(),
                Carried::Only(facts) => {
                    for fact in facts {
                        ensure!(
                            columns.iter().any(|column| column == fact),
                            "{table} has no column {fact}"
                        );
                    }
                    facts.iter().map(|fact| (*fact).to_owned()).collect()
                }
            };
            carried_columns.insert(*table, kept);
        }
        Ok(Self(carried_columns))
    }

    fn of(&self, table: &str) -> &[String] {
        self.0.get(table).map(Vec::as_slice).unwrap_or_default()
    }

    /// The tables whose carried columns differ between two catalogs, each
    /// with the columns only one side has.
    pub fn differences(&self, peer: &Self) -> Vec<String> {
        let mut differences = Vec::new();
        for table in COPIED_TABLES {
            let ours: BTreeSet<&String> = self.of(table).iter().collect();
            let theirs: BTreeSet<&String> = peer.of(table).iter().collect();
            if ours != theirs {
                let only_ours: Vec<&&String> = ours.difference(&theirs).collect();
                let only_theirs: Vec<&&String> = theirs.difference(&ours).collect();
                differences.push(format!(
                    "{table} (only here: {only_ours:?}; only on the peer: {only_theirs:?})"
                ));
            }
        }
        differences
    }
}

/// What the peer says about itself, read each pass.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerFacts {
    pub node: Option<NodeIndex>,
    pub config_fingerprint: Option<String>,
    pub share_seq_floor: Option<i64>,
    pub sync_seq_floor: Option<i64>,
}

/// The peer's cursors over this node's streams: positions in this node's
/// key spaces that the peer has scanned through.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PeerCursors {
    pub shares: Option<i64>,
    pub blocks: Option<i64>,
    pub prepared: Option<i64>,
}

/// One pull of share rows: the peer rows in `(after, through]`, their header
/// mappings, and how far the scan reached.
#[derive(Clone, Debug, Default)]
pub struct ShareBatch {
    /// The highest position read; `None` when the peer holds nothing above
    /// `after`.
    pub through: Option<i64>,
    /// How many rows of any origin the scan read.
    pub scanned: i64,
    /// The rows to apply, as a JSON array of `to_jsonb` documents.
    pub rows: Value,
    pub hashes: Value,
    pub row_count: usize,
    pub highest: Option<i64>,
    pub highest_accepted_at_ms: Option<i64>,
}

/// A landed block and every row its landing wrote, as the peer holds them.
#[derive(Clone, Debug)]
pub struct BlockBundle {
    pub sync_seq: i64,
    pub block_hash: String,
    pub block: Value,
    pub bundle: Option<Value>,
    pub snapshot: Option<Value>,
    pub payouts: Value,
    pub carries: Value,
    pub fanout_set: Option<Value>,
    pub artifacts: Value,
}

/// Prepared jobs and the blobs they reference.
#[derive(Clone, Debug, Default)]
pub struct PreparedBatch {
    pub jobs: Value,
    pub templates: Value,
    pub balances: Value,
    pub count: usize,
    pub highest: Option<i64>,
}

/// How applying one pull went.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Applied {
    pub inserted: BTreeMap<&'static str, u64>,
    pub conflicts: BTreeMap<&'static str, u64>,
}

impl Applied {
    fn add(&mut self, table: &'static str, rows: u64) {
        if rows > 0 {
            *self.inserted.entry(table).or_default() += rows;
        }
    }

    fn conflict(&mut self, table: &'static str) {
        *self.conflicts.entry(table).or_default() += 1;
    }

    pub fn merge(&mut self, other: Applied) {
        for (table, rows) in other.inserted {
            *self.inserted.entry(table).or_default() += rows;
        }
        for (table, rows) in other.conflicts {
            *self.conflicts.entry(table).or_default() += rows;
        }
    }

    pub fn total_conflicts(&self) -> u64 {
        self.conflicts.values().sum()
    }
}

fn array_len(value: &Value) -> usize {
    value.as_array().map_or(0, Vec::len)
}

/// Reads on the peer's database, as its read-only sync role.
pub mod peer {
    use super::*;

    /// The peer's identity, fingerprint and floors.
    pub async fn facts(connection: &mut PgConnection) -> Result<PeerFacts> {
        let row = sqlx::query(
            "SELECT (SELECT node_index FROM qbit_prism_node_identity WHERE singleton) AS node_index,\
             (SELECT config_fingerprint FROM qbit_prism_cluster WHERE singleton) AS config_fingerprint,\
             (SELECT share_seq_floor FROM qbit_prism_node_lineage WHERE singleton) AS share_seq_floor,\
             (SELECT sync_seq_floor FROM qbit_prism_node_lineage WHERE singleton) AS sync_seq_floor",
        )
        .fetch_one(&mut *connection)
        .await?;
        let node: Option<i16> = row.try_get("node_index")?;
        Ok(PeerFacts {
            node: node.and_then(|index| NodeIndex::from_index(index.into())),
            config_fingerprint: row.try_get("config_fingerprint")?,
            share_seq_floor: row.try_get("share_seq_floor")?,
            sync_seq_floor: row.try_get("sync_seq_floor")?,
        })
    }

    /// How far the peer has pulled this node's streams.
    pub async fn cursors(connection: &mut PgConnection) -> Result<PeerCursors> {
        let rows: Vec<(String, i64)> =
            sqlx::query_as("SELECT stream,scanned_through FROM qbit_prism_peer_sync_cursors")
                .fetch_all(&mut *connection)
                .await?;
        let mut cursors = PeerCursors::default();
        for (stream, position) in rows {
            match stream.as_str() {
                SHARES => cursors.shares = Some(position),
                BLOCKS => cursors.blocks = Some(position),
                PREPARED => cursors.prepared = Some(position),
                _ => {}
            }
        }
        Ok(cursors)
    }

    /// The peer's rows above `after`, in `share_seq` order, scanning at
    /// most `limit` rows of any origin and returning those `origin` wrote.
    /// One repeatable-read snapshot, so the rows, their mappings and the
    /// position reached agree.
    pub async fn shares_scanned(
        connection: &mut PgConnection,
        after: i64,
        limit: i64,
        origin: NodeIndex,
    ) -> Result<ShareBatch> {
        let mut tx = repeatable_read(connection).await?;
        let (through, scanned): (Option<i64>, i64) = sqlx::query_as(
            "SELECT max(share_seq),count(*) FROM (SELECT share_seq FROM qbit_share_ledger \
             WHERE share_seq>$1 ORDER BY share_seq LIMIT $2) scanned",
        )
        .bind(after)
        .bind(limit)
        .fetch_one(&mut *tx)
        .await?;
        let mut batch = ShareBatch {
            through,
            scanned,
            rows: Value::Array(Vec::new()),
            hashes: Value::Array(Vec::new()),
            ..ShareBatch::default()
        };
        if let Some(through) = through {
            fill_shares(&mut tx, &mut batch, after, Some(through), origin, None).await?;
        }
        tx.commit().await?;
        Ok(batch)
    }

    /// The rows `origin` wrote above `after`, at most `limit`, in
    /// `share_seq` order: own-log recovery, which reads this node's own rows
    /// back from the peer. The peer inserted them in `share_seq` order, so
    /// what it holds of them is a prefix.
    pub async fn shares_of(
        connection: &mut PgConnection,
        after: i64,
        limit: i64,
        origin: NodeIndex,
    ) -> Result<ShareBatch> {
        let mut tx = repeatable_read(connection).await?;
        let mut batch = ShareBatch {
            rows: Value::Array(Vec::new()),
            hashes: Value::Array(Vec::new()),
            ..ShareBatch::default()
        };
        fill_shares(&mut tx, &mut batch, after, None, origin, Some(limit)).await?;
        batch.through = batch.highest;
        batch.scanned = i64::try_from(batch.row_count)?;
        tx.commit().await?;
        Ok(batch)
    }

    async fn fill_shares(
        tx: &mut Transaction<'_, Postgres>,
        batch: &mut ShareBatch,
        after: i64,
        through: Option<i64>,
        origin: NodeIndex,
        limit: Option<i64>,
    ) -> Result<()> {
        let row = sqlx::query(
            "WITH picked AS MATERIALIZED (SELECT s.* FROM qbit_share_ledger s WHERE s.origin_node=$1 \
                AND s.share_seq>$2 AND s.share_seq<=$3 ORDER BY s.share_seq LIMIT $4) \
             SELECT COALESCE((SELECT jsonb_agg(to_jsonb(p) ORDER BY p.share_seq) FROM picked p),'[]'::jsonb) AS rows,\
                COALESCE((SELECT jsonb_agg(to_jsonb(h) ORDER BY h.header_hash) FROM qbit_prism_share_hashes h \
                    WHERE h.share_id IN (SELECT share_id FROM picked WHERE accepted)),'[]'::jsonb) AS hashes,\
                (SELECT count(*) FROM picked) AS row_count,(SELECT max(share_seq) FROM picked) AS highest,\
                (SELECT floor(extract(epoch FROM max(accepted_at))*1000)::bigint FROM picked) AS accepted_ms",
        )
        .bind(origin.index())
        .bind(after)
        .bind(through.unwrap_or(i64::MAX))
        .bind(limit.unwrap_or(i64::MAX))
        .fetch_one(&mut **tx)
        .await?;
        batch.rows = row.try_get("rows")?;
        batch.hashes = row.try_get("hashes")?;
        batch.row_count = usize::try_from(row.try_get::<i64, _>("row_count")?)?;
        batch.highest = row.try_get("highest")?;
        batch.highest_accepted_at_ms = row.try_get("accepted_ms")?;
        Ok(())
    }

    /// The highest `share_seq` and `sync_seq` the peer holds of rows
    /// `origin` wrote, and of the journal's epochs.
    pub async fn highest_of(
        connection: &mut PgConnection,
        origin: NodeIndex,
    ) -> Result<(Option<i64>, Option<i64>, Option<i64>)> {
        Ok(sqlx::query_as(
            "SELECT (SELECT max(share_seq) FROM qbit_share_ledger WHERE origin_node=$1),\
             GREATEST((SELECT max(sync_seq) FROM qbit_pool_blocks WHERE origin_node=$1),\
                      (SELECT max(sync_seq) FROM qbit_prism_jobs WHERE origin_node=$1 AND job_id LIKE 'prepared:%')),\
             (SELECT max(epoch) FROM qbit_prism_node_roles WHERE origin_node=$1)",
        )
        .bind(origin.index())
        .fetch_one(&mut *connection)
        .await?)
    }

    /// Rows above the cursor the peer still holds for `origin`, counted up
    /// to `cap`.
    pub async fn shares_beyond(
        connection: &mut PgConnection,
        after: i64,
        origin: NodeIndex,
        cap: i64,
    ) -> Result<i64> {
        Ok(sqlx::query_scalar(
            "SELECT count(*) FROM (SELECT 1 FROM qbit_share_ledger WHERE origin_node=$1 AND share_seq>$2 LIMIT $3) beyond",
        )
        .bind(origin.index())
        .bind(after)
        .bind(cap)
        .fetch_one(&mut *connection)
        .await?)
    }

    /// The peer's sync sequence position at the sync barrier, or `None` when
    /// a transaction that drew a `sync_seq` is still open (the barrier is
    /// held shared): every `sync_seq` default holds the barrier shared until
    /// its transaction ends, so while this session holds it exclusively no
    /// row at or below the position read can still commit. Tried, never
    /// waited for, so no writer ever queues behind the puller.
    pub async fn sync_barrier(connection: &mut PgConnection) -> Result<Option<Option<i64>>> {
        let taken: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(SYNC_BARRIER_LOCK)
            .fetch_one(&mut *connection)
            .await?;
        if !taken {
            return Ok(None);
        }
        let position = sqlx::query_scalar(
            "SELECT CASE WHEN is_called THEN last_value END FROM qbit_prism_sync_seq",
        )
        .fetch_one(&mut *connection)
        .await;
        let released: bool = sqlx::query_scalar("SELECT pg_advisory_unlock($1)")
            .bind(SYNC_BARRIER_LOCK)
            .fetch_one(&mut *connection)
            .await?;
        ensure!(
            released,
            "the sync barrier was not held when it was released"
        );
        Ok(Some(position?))
    }

    /// Landed blocks `origin` wrote with `sync_seq` in `(after, through]`,
    /// at most `limit`, each with every row its landing wrote.
    pub async fn blocks(
        connection: &mut PgConnection,
        origin: NodeIndex,
        after: i64,
        through: Option<i64>,
        limit: i64,
    ) -> Result<Vec<BlockBundle>> {
        let mut tx = repeatable_read(connection).await?;
        let rows = sqlx::query(
            "SELECT b.sync_seq,b.block_hash,to_jsonb(b) AS block,\
             (SELECT to_jsonb(a) FROM qbit_pool_audit_bundles a WHERE a.block_hash=b.block_hash) AS bundle,\
             (SELECT to_jsonb(s) FROM qbit_prism_audit_snapshots s WHERE s.snapshot_sha256=\
                (SELECT a.share_snapshot_sha256 FROM qbit_pool_audit_bundles a WHERE a.block_hash=b.block_hash)) AS snapshot,\
             COALESCE((SELECT jsonb_agg(to_jsonb(p) ORDER BY p.payout_entry_seq) FROM qbit_pool_payout_entries p WHERE p.block_hash=b.block_hash),'[]'::jsonb) AS payouts,\
             COALESCE((SELECT jsonb_agg(to_jsonb(c) ORDER BY c.carry_forward_seq) FROM qbit_payout_carry_forward c WHERE c.block_hash=b.block_hash),'[]'::jsonb) AS carries,\
             (SELECT to_jsonb(f) FROM qbit_ctv_fanout_sets f WHERE f.block_hash=b.block_hash) AS fanout_set,\
             COALESCE((SELECT jsonb_agg(to_jsonb(r) ORDER BY r.chunk_index) FROM qbit_ctv_fanout_artifacts r WHERE r.block_hash=b.block_hash),'[]'::jsonb) AS artifacts \
             FROM qbit_pool_blocks b WHERE b.origin_node=$1 AND b.sync_seq>$2 \
             AND b.sync_seq<=$3 ORDER BY b.sync_seq LIMIT $4",
        )
        .bind(origin.index())
        .bind(after)
        .bind(through.unwrap_or(i64::MAX))
        .bind(limit)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        rows.iter()
            .map(|row| {
                Ok(BlockBundle {
                    sync_seq: row.try_get("sync_seq")?,
                    block_hash: row.try_get("block_hash")?,
                    block: row.try_get("block")?,
                    bundle: row.try_get("bundle")?,
                    snapshot: row.try_get("snapshot")?,
                    payouts: row.try_get("payouts")?,
                    carries: row.try_get("carries")?,
                    fanout_set: row.try_get("fanout_set")?,
                    artifacts: row.try_get("artifacts")?,
                })
            })
            .collect()
    }

    /// Prepared jobs `origin` wrote with `sync_seq` in `(after, through]`,
    /// at most `limit`, with the template and balance blobs they reference.
    pub async fn prepared(
        connection: &mut PgConnection,
        origin: NodeIndex,
        after: i64,
        through: Option<i64>,
        limit: i64,
    ) -> Result<PreparedBatch> {
        let mut tx = repeatable_read(connection).await?;
        let row = sqlx::query(
            "WITH picked AS MATERIALIZED (SELECT j.* FROM qbit_prism_jobs j WHERE j.origin_node=$1 \
                AND j.job_id LIKE 'prepared:%' AND j.sync_seq>$2 AND j.sync_seq<=$3 \
                ORDER BY j.sync_seq LIMIT $4) \
             SELECT COALESCE((SELECT jsonb_agg(to_jsonb(p) ORDER BY p.sync_seq) FROM picked p),'[]'::jsonb) AS jobs,\
                COALESCE((SELECT jsonb_agg(to_jsonb(t)) FROM qbit_prism_templates t \
                    WHERE t.template_sha256 IN (SELECT template_sha256 FROM picked)),'[]'::jsonb) AS templates,\
                COALESCE((SELECT jsonb_agg(to_jsonb(s)) FROM qbit_prism_balance_snapshots s \
                    WHERE s.prior_balances_digest IN (SELECT window_prior_balances_sha256 FROM picked)),'[]'::jsonb) AS balances,\
                (SELECT count(*) FROM picked) AS count,(SELECT max(sync_seq) FROM picked) AS highest",
        )
        .bind(origin.index())
        .bind(after)
        .bind(through.unwrap_or(i64::MAX))
        .bind(limit)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(PreparedBatch {
            jobs: row.try_get("jobs")?,
            templates: row.try_get("templates")?,
            balances: row.try_get("balances")?,
            count: usize::try_from(row.try_get::<i64, _>("count")?)?,
            highest: row.try_get("highest")?,
        })
    }

    /// Every journal row `origin` wrote.
    pub async fn node_roles(connection: &mut PgConnection, origin: NodeIndex) -> Result<Value> {
        Ok(sqlx::query_scalar(
            "SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY r.epoch),'[]'::jsonb) FROM qbit_prism_node_roles r WHERE r.origin_node=$1",
        )
        .bind(origin.index())
        .fetch_one(&mut *connection)
        .await?)
    }

    async fn repeatable_read(
        connection: &mut PgConnection,
    ) -> Result<Transaction<'_, Postgres>, sqlx::Error> {
        let mut tx = sqlx::Connection::begin(connection).await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *tx)
            .await?;
        Ok(tx)
    }
}

impl Ledger {
    /// The columns a pull carries of each copied table, from this
    /// database's catalog.
    pub async fn carried_columns(&self) -> Result<CarriedColumns> {
        CarriedColumns::read(&mut *self.acquire().await?).await
    }

    /// The cluster fingerprint this database is pinned to (D-6), as stored.
    pub async fn stored_config_fingerprint(&self) -> Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT config_fingerprint FROM qbit_prism_cluster WHERE singleton")
                .fetch_one(&mut *self.acquire().await?)
                .await?,
        )
    }

    /// This node's cursor over the peer's `stream`, if it has pulled it.
    pub async fn peer_sync_cursor(&self, stream: &str) -> Result<Option<(i64, Option<i64>)>> {
        Ok(sqlx::query_as(
            "SELECT scanned_through,ingested_through FROM qbit_prism_peer_sync_cursors WHERE stream=$1",
        )
        .bind(stream)
        .fetch_optional(&mut *self.acquire().await?)
        .await?)
    }

    /// The highest `share_seq`, and the highest root `sync_seq`, of rows
    /// `origin` wrote that this database holds, and the journal's highest
    /// epoch for it.
    pub async fn highest_held_of(
        &self,
        origin: NodeIndex,
    ) -> Result<(Option<i64>, Option<i64>, Option<i64>, Option<i64>)> {
        Ok(sqlx::query_as(
            "SELECT (SELECT max(share_seq) FROM qbit_share_ledger WHERE origin_node=$1),\
             (SELECT max(sync_seq) FROM qbit_pool_blocks WHERE origin_node=$1),\
             (SELECT max(sync_seq) FROM qbit_prism_jobs WHERE origin_node=$1 AND job_id LIKE 'prepared:%'),\
             (SELECT max(epoch) FROM qbit_prism_node_roles WHERE origin_node=$1)",
        )
        .bind(origin.index())
        .fetch_one(&mut *self.acquire().await?)
        .await?)
    }

    /// Record a conflict: the peer offered `key` of `table` with content
    /// this node already holds differently, or could not insert it.
    async fn record_conflict(
        tx: &mut Transaction<'_, Postgres>,
        table: &str,
        key: &str,
        origin: i16,
        detail: &str,
    ) -> Result<()> {
        tracing::error!(
            table,
            key,
            origin_node = origin,
            detail,
            "ALERT: the peer sync found a row this node already holds with other content, or \
             could not insert it; the local row is kept and the peer's is not applied"
        );
        sqlx::query(
            "INSERT INTO qbit_prism_peer_sync_conflicts(source_table,row_key,origin_node,detail) \
             VALUES($1,$2,$3,$4) ON CONFLICT(source_table,row_key) DO UPDATE SET \
             last_seen_at=clock_timestamp(),seen_count=qbit_prism_peer_sync_conflicts.seen_count+1,\
             detail=EXCLUDED.detail",
        )
        .bind(table)
        .bind(key)
        .bind(origin)
        .bind(detail)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    /// Record a conflict in a transaction of its own.
    async fn record_conflict_alone(
        &self,
        table: &str,
        key: &str,
        origin: i16,
        detail: &str,
    ) -> Result<()> {
        let mut tx = self.begin().await?;
        Self::record_conflict(&mut tx, table, key, origin, detail).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Advance `stream`'s cursor over the peer's rows to `scanned_through`,
    /// in the transaction that inserted the rows it covers. It never moves
    /// back.
    async fn advance_cursor(
        tx: &mut Transaction<'_, Postgres>,
        stream: &str,
        peer: NodeIndex,
        scanned_through: i64,
        ingested: Option<i64>,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO qbit_prism_peer_sync_cursors(stream,peer_node,scanned_through,ingested_through) \
             VALUES($1,$2,$3,$4) ON CONFLICT(stream) DO UPDATE SET \
             scanned_through=GREATEST(qbit_prism_peer_sync_cursors.scanned_through,EXCLUDED.scanned_through),\
             ingested_through=GREATEST(qbit_prism_peer_sync_cursors.ingested_through,EXCLUDED.ingested_through),\
             peer_node=EXCLUDED.peer_node,updated_at=clock_timestamp()",
        )
        .bind(stream)
        .bind(peer.index())
        .bind(scanned_through)
        .bind(ingested)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    /// Start `stream`'s cursor at `position` if it has none.
    pub async fn start_peer_sync_cursor(
        &self,
        stream: &str,
        peer: NodeIndex,
        position: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO qbit_prism_peer_sync_cursors(stream,peer_node,scanned_through) VALUES($1,$2,$3) \
             ON CONFLICT(stream) DO NOTHING",
        )
        .bind(stream)
        .bind(peer.index())
        .bind(position)
        .execute(&mut *self.acquire().await?)
        .await?;
        Ok(())
    }

    /// Advance a `sync_seq` stream's cursor past a mark the pull found no
    /// more rows below.
    pub async fn settle_peer_sync_cursor(
        &self,
        stream: &str,
        peer: NodeIndex,
        scanned_through: i64,
    ) -> Result<()> {
        let mut tx = self.begin().await?;
        Self::advance_cursor(&mut tx, stream, peer, scanned_through, None).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Raise this node's own key sequences, with their parity, above every
    /// key the database holds and above `share_seq_floor` and
    /// `sync_seq_floor`, and the ledger clock to at least `clock_ms`, under
    /// SETTLEMENT_LOCK and ORDER_LOCK so no landing or append draws a key
    /// meanwhile. Each only rises.
    pub async fn raise_own_sequences(
        &self,
        node: NodeIndex,
        share_seq_floor: Option<i64>,
        sync_seq_floor: Option<i64>,
        clock_ms: Option<i64>,
    ) -> Result<()> {
        let mut tx = self.begin().await?;
        self.lock(&mut tx, SETTLEMENT_LOCK).await?;
        let _order = self.lock_order(&mut tx, OrderLockHolder::PeerSync).await?;
        for (table, column) in super::node_identity::PARITY_KEYS {
            let floor = if *table == "qbit_share_ledger" {
                share_seq_floor
            } else {
                None
            };
            super::node_identity::raise_parity(&mut tx, table, column, node, floor, true).await?;
        }
        if let Some(floor) = sync_seq_floor {
            sqlx::query(
                "SELECT setval('qbit_prism_sync_seq',$1,true) FROM qbit_prism_sync_seq \
                 WHERE NOT is_called OR last_value<$1",
            )
            .bind(floor)
            .execute(&mut *tx)
            .await?;
        }
        if let Some(clock_ms) = clock_ms {
            sqlx::query(
                "UPDATE qbit_prism_cluster SET ledger_clock_ms=$1 WHERE singleton AND ledger_clock_ms<$1",
            )
            .bind(clock_ms)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Raise this node's share sequence above `highest`, a peer row about
    /// to be inserted, keeping its parity, under ORDER_LOCK so no append
    /// holds a lower value meanwhile. A no-op when it is already above.
    async fn raise_share_sequence_above(&self, node: NodeIndex, highest: i64) -> Result<()> {
        let mut tx = self.begin().await?;
        let _order = self.lock_order(&mut tx, OrderLockHolder::PeerSync).await?;
        super::node_identity::raise_parity(
            &mut tx,
            "qbit_share_ledger",
            "share_seq",
            node,
            Some(highest),
            false,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Apply one pull of share rows that `origin` wrote: the peer's own
    /// rows (`stream` is `Some(SHARES)`, which also advances its cursor and
    /// the safe peer mark to `batch.through`), or this node's own rows read
    /// back from the peer (`None`).
    ///
    /// A peer row is inserted only after this node's share sequence is
    /// above it and a partition holds it. An own row is inserted under
    /// ORDER_LOCK (D-14). A row whose header this node already credits to
    /// another share is refused whole (D-14), as is a row whose
    /// `share_seq` or `share_id` this node holds with other content, or one
    /// below every attached partition: each is a recorded conflict, and the
    /// stream moves past it.
    pub async fn apply_share_batch(
        &self,
        node: NodeIndex,
        origin: NodeIndex,
        batch: &ShareBatch,
        stream: Option<&str>,
    ) -> Result<Applied> {
        let mut applied = Applied::default();
        let own = origin == node;
        if batch.row_count > 0 {
            if let Some(highest) = batch.highest.filter(|_| !own) {
                self.raise_share_sequence_above(node, highest).await?;
            }
            crate::partitions::ensure_with_metrics(&self.pool, self.metrics.as_deref()).await?;
        }
        let mut tx = self.begin().await?;
        let _order = if own {
            Some(self.lock_order(&mut tx, OrderLockHolder::PeerSync).await?)
        } else {
            None
        };
        if batch.row_count > 0 {
            // Header mappings this node holds for another share, and rows below
            // every attached partition: refused whole.
            let refused: Vec<(String, String)> = sqlx::query_as(
                "WITH incoming AS (SELECT * FROM jsonb_populate_recordset(NULL::qbit_share_ledger,$1)),\
                 mappings AS (SELECT * FROM jsonb_populate_recordset(NULL::qbit_prism_share_hashes,$2)) \
                 SELECT m.share_id,'header '||m.header_hash||' is credited here to '||h.share_id \
                 FROM mappings m JOIN qbit_prism_share_hashes h ON h.header_hash=m.header_hash WHERE h.share_id<>m.share_id \
                 UNION ALL SELECT m.share_id,'share_id '||m.share_id||' maps header '||h.header_hash||' here, not '||m.header_hash \
                 FROM mappings m JOIN qbit_prism_share_hashes h ON h.share_id=m.share_id WHERE h.header_hash<>m.header_hash \
                 UNION ALL SELECT i.share_id,'share_seq '||i.share_seq||' is below every attached partition' FROM incoming i \
                 WHERE i.share_seq<(SELECT COALESCE(min(lower_seq),-1) FROM qbit_prism_share_partitions WHERE state='attached' AND lower_seq IS NOT NULL) \
                 AND NOT EXISTS (SELECT 1 FROM qbit_prism_share_partitions WHERE state='attached' AND lower_seq IS NULL)",
            )
            .bind(&batch.rows)
            .bind(&batch.hashes)
            .fetch_all(&mut *tx)
            .await?;
            let refused_ids: Vec<String> = refused.iter().map(|(id, _)| id.clone()).collect();
            for (share_id, detail) in &refused {
                Self::record_conflict(
                    &mut tx,
                    "qbit_share_ledger",
                    share_id,
                    origin.index(),
                    detail,
                )
                .await?;
                applied.conflict("qbit_share_ledger");
            }
            let columns = self
                .carried_columns_in(&mut tx, "qbit_share_ledger")
                .await?;
            let plain = columns.replace('"', "");
            let inserted: Vec<String> = sqlx::query_scalar(&format!(
                "INSERT INTO qbit_share_ledger({columns}) SELECT {columns} FROM \
                 jsonb_populate_recordset(NULL::qbit_share_ledger,$1) WHERE share_id<>ALL($2) \
                 ORDER BY share_seq ON CONFLICT DO NOTHING RETURNING share_id"
            ))
            .bind(&batch.rows)
            .bind(&refused_ids)
            .fetch_all(&mut *tx)
            .await?;
            applied.add("qbit_share_ledger", inserted.len() as u64);
            let mapping_columns = self
                .carried_columns_in(&mut tx, "qbit_prism_share_hashes")
                .await?;
            let mapped = sqlx::query(&format!(
                "INSERT INTO qbit_prism_share_hashes({mapping_columns}) SELECT {mapping_columns} FROM \
                 jsonb_populate_recordset(NULL::qbit_prism_share_hashes,$1) WHERE share_id=ANY($2) \
                 ON CONFLICT DO NOTHING"
            ))
            .bind(&batch.hashes)
            .bind(&inserted)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            applied.add("qbit_prism_share_hashes", mapped);
            // An inserted accepted row whose header this node now credits to
            // another share raced a local writer: undo the whole pull, and
            // the next one refuses the row before inserting it.
            let unmapped: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM jsonb_populate_recordset(NULL::qbit_prism_share_hashes,$1) m \
                 WHERE m.share_id=ANY($2) AND NOT EXISTS (SELECT 1 FROM qbit_prism_share_hashes h \
                 WHERE h.header_hash=m.header_hash AND h.share_id=m.share_id)",
            )
            .bind(&batch.hashes)
            .bind(&inserted)
            .fetch_one(&mut *tx)
            .await?;
            ensure!(
                unmapped == 0,
                "{unmapped} pulled share(s) lost their header to a concurrent writer; the pull is retried"
            );
            // Every row neither inserted nor refused is held here already:
            // identical, or a conflict.
            let differing: Vec<(i64, String)> = sqlx::query_as(&format!(
                "SELECT i.share_seq,i.share_id FROM jsonb_populate_recordset(NULL::qbit_share_ledger,$1) i \
                 WHERE i.share_id<>ALL($2) AND i.share_id<>ALL($3) AND NOT EXISTS (SELECT 1 FROM qbit_share_ledger l \
                 WHERE l.share_seq=i.share_seq AND ROW({local}) IS NOT DISTINCT FROM ROW({theirs}))",
                local = prefixed(&plain, "l."),
                theirs = prefixed(&plain, "i."),
            ))
            .bind(&batch.rows)
            .bind(&refused_ids)
            .bind(&inserted)
            .fetch_all(&mut *tx)
            .await?;
            for (share_seq, share_id) in &differing {
                Self::record_conflict(
                    &mut tx,
                    "qbit_share_ledger",
                    share_id,
                    origin.index(),
                    &format!(
                        "share_seq {share_seq} or its share_id is held here with other content"
                    ),
                )
                .await?;
                applied.conflict("qbit_share_ledger");
            }
        }
        if let (Some(stream), Some(through)) = (stream, batch.through) {
            Self::advance_cursor(&mut tx, stream, origin, through, batch.highest).await?;
        }
        tx.commit().await?;
        Ok(applied)
    }

    /// The carried columns of `table`, quoted, read in `tx`.
    async fn carried_columns_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        table: &str,
    ) -> Result<String> {
        let columns = local_columns(tx, table).await?;
        let kept: Vec<String> = match carried(table) {
            Carried::AllBut(local) => columns
                .into_iter()
                .filter(|column| !local.contains(&column.as_str()))
                .collect(),
            Carried::Only(facts) => facts.iter().map(|fact| (*fact).to_owned()).collect(),
        };
        Ok(kept
            .iter()
            .map(|column| format!("\"{column}\""))
            .collect::<Vec<_>>()
            .join(","))
    }

    /// Apply one landed block whole (D-1, D-10), in one transaction, block
    /// row first: if this node already holds a block of that hash (its own
    /// landing, or an adoption), nothing of it is applied and its immutable
    /// facts are compared; otherwise every row is inserted with only its
    /// carried columns, derived ones at their defaults (`prepared`,
    /// `immature`, no publication sequence, `awaiting_maturity`, no claim),
    /// so it counts only once this node's own reconciler confirms it. A row
    /// that cannot be inserted refuses the whole block, as a recorded
    /// conflict. `stream` names the cursor to advance, when the block is the
    /// peer's.
    pub async fn apply_block(
        &self,
        block: &BlockBundle,
        stream: Option<(&str, NodeIndex)>,
    ) -> Result<Applied> {
        let mut applied = Applied::default();
        let origin: i16 = block
            .block
            .get("origin_node")
            .and_then(Value::as_i64)
            .and_then(|origin| i16::try_from(origin).ok())
            .context("a peer block document has no origin_node")?;
        let mut tx = self.begin().await?;
        let facts = BLOCK_FACTS.join(",");
        let held: Option<(Value, Option<String>)> = sqlx::query_as(&format!(
            "SELECT to_jsonb(f),(SELECT audit_bundle_sha256 FROM qbit_pool_audit_bundles a WHERE a.block_hash=$1) \
             FROM (SELECT {facts} FROM qbit_pool_blocks WHERE block_hash=$1) f"
        ))
        .bind(&block.block_hash)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some((held_facts, held_audit)) = held {
            let theirs: Value = sqlx::query_scalar(&format!(
                "SELECT to_jsonb(f) FROM (SELECT {facts} FROM jsonb_populate_record(NULL::qbit_pool_blocks,$1)) f"
            ))
            .bind(&block.block)
            .fetch_one(&mut *tx)
            .await?;
            let their_audit = block
                .bundle
                .as_ref()
                .and_then(|bundle| bundle.get("audit_bundle_sha256"))
                .and_then(Value::as_str)
                .map(str::to_owned);
            if held_facts != theirs || held_audit != their_audit {
                Self::record_conflict(
                    &mut tx,
                    "qbit_pool_blocks",
                    &block.block_hash,
                    origin,
                    "the block is held here with other landing facts or audit digest",
                )
                .await?;
                applied.conflict("qbit_pool_blocks");
            }
        } else {
            let mut savepoint = sqlx::Connection::begin(&mut *tx as &mut PgConnection).await?;
            match self.insert_block(&mut savepoint, block).await {
                Ok(rows) => {
                    savepoint.commit().await?;
                    for (table, count) in rows {
                        applied.add(table, count);
                    }
                }
                Err(error) => {
                    savepoint.rollback().await?;
                    Self::record_conflict(
                        &mut tx,
                        "qbit_pool_blocks",
                        &block.block_hash,
                        origin,
                        &format!("the block could not be applied: {error:#}"),
                    )
                    .await?;
                    applied.conflict("qbit_pool_blocks");
                }
            }
        }
        if let Some((stream, peer)) = stream {
            Self::advance_cursor(&mut tx, stream, peer, block.sync_seq, Some(block.sync_seq))
                .await?;
        }
        tx.commit().await?;
        Ok(applied)
    }

    async fn insert_block(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        block: &BlockBundle,
    ) -> Result<Vec<(&'static str, u64)>> {
        let mut rows = Vec::new();
        let columns = self.carried_columns_in(tx, "qbit_pool_blocks").await?;
        let inserted = sqlx::query(&format!(
            "INSERT INTO qbit_pool_blocks({columns}) SELECT {columns} FROM jsonb_populate_record(NULL::qbit_pool_blocks,$1)"
        ))
        .bind(&block.block)
        .execute(&mut **tx)
        .await?
        .rows_affected();
        rows.push(("qbit_pool_blocks", inserted));
        if let Some(snapshot) = &block.snapshot {
            let columns = self
                .carried_columns_in(tx, "qbit_prism_audit_snapshots")
                .await?;
            let inserted = sqlx::query(&format!(
                "INSERT INTO qbit_prism_audit_snapshots({columns}) SELECT {columns} FROM \
                 jsonb_populate_record(NULL::qbit_prism_audit_snapshots,$1) ON CONFLICT DO NOTHING"
            ))
            .bind(snapshot)
            .execute(&mut **tx)
            .await?
            .rows_affected();
            if inserted == 0 {
                let facts = SNAPSHOT_FACTS.join(",");
                let same: bool = sqlx::query_scalar(&format!(
                    "SELECT EXISTS(SELECT 1 FROM qbit_prism_audit_snapshots l, jsonb_populate_record(NULL::qbit_prism_audit_snapshots,$1) i \
                     WHERE l.snapshot_sha256=i.snapshot_sha256 AND ROW({l}) IS NOT DISTINCT FROM ROW({i}))",
                    l = prefixed(&facts, "l."),
                    i = prefixed(&facts, "i."),
                ))
                .bind(snapshot)
                .fetch_one(&mut **tx)
                .await?;
                ensure!(
                    same,
                    "its audit snapshot is held here with other content (D-15)"
                );
            }
            rows.push(("qbit_prism_audit_snapshots", inserted));
        }
        if let Some(bundle) = &block.bundle {
            let columns = self
                .carried_columns_in(tx, "qbit_pool_audit_bundles")
                .await?;
            let inserted = sqlx::query(&format!(
                "INSERT INTO qbit_pool_audit_bundles({columns}) SELECT {columns} FROM \
                 jsonb_populate_record(NULL::qbit_pool_audit_bundles,$1)"
            ))
            .bind(bundle)
            .execute(&mut **tx)
            .await?
            .rows_affected();
            rows.push(("qbit_pool_audit_bundles", inserted));
        }
        for (table, documents) in [
            ("qbit_pool_payout_entries", &block.payouts),
            ("qbit_payout_carry_forward", &block.carries),
        ] {
            if array_len(documents) == 0 {
                continue;
            }
            let columns = self.carried_columns_in(tx, table).await?;
            let inserted = sqlx::query(&format!(
                "INSERT INTO {table}({columns}) SELECT {columns} FROM jsonb_populate_recordset(NULL::{table},$1)"
            ))
            .bind(documents)
            .execute(&mut **tx)
            .await?
            .rows_affected();
            rows.push((table, inserted));
        }
        if let Some(set) = &block.fanout_set {
            let columns = self.carried_columns_in(tx, "qbit_ctv_fanout_sets").await?;
            let inserted = sqlx::query(&format!(
                "INSERT INTO qbit_ctv_fanout_sets({columns}) SELECT {columns} FROM \
                 jsonb_populate_record(NULL::qbit_ctv_fanout_sets,$1)"
            ))
            .bind(set)
            .execute(&mut **tx)
            .await?
            .rows_affected();
            rows.push(("qbit_ctv_fanout_sets", inserted));
        }
        if array_len(&block.artifacts) > 0 {
            let columns = self
                .carried_columns_in(tx, "qbit_ctv_fanout_artifacts")
                .await?;
            let inserted = sqlx::query(&format!(
                "INSERT INTO qbit_ctv_fanout_artifacts({columns}) SELECT {columns} FROM \
                 jsonb_populate_recordset(NULL::qbit_ctv_fanout_artifacts,$1)"
            ))
            .bind(&block.artifacts)
            .execute(&mut **tx)
            .await?
            .rows_affected();
            rows.push(("qbit_ctv_fanout_artifacts", inserted));
        }
        Ok(rows)
    }

    /// Apply prepared jobs with their blobs, blobs first, in one transaction
    /// (D-19): a node that holds a record then holds both blobs it names. A
    /// content-addressed blob held here already must be byte-identical; a
    /// job held here already must match on everything but its expiry, which
    /// its writer renews. `stream` names the cursor to advance, when the
    /// jobs are the peer's.
    pub async fn apply_prepared(
        &self,
        batch: &PreparedBatch,
        origin: NodeIndex,
        stream: Option<&str>,
    ) -> Result<Applied> {
        let mut applied = Applied::default();
        let mut tx = self.begin().await?;
        for (table, key, documents, compared) in [
            (
                "qbit_prism_templates",
                "template_sha256",
                &batch.templates,
                "template_bytes",
            ),
            (
                "qbit_prism_balance_snapshots",
                "prior_balances_digest",
                &batch.balances,
                "balances",
            ),
        ] {
            if array_len(documents) == 0 {
                continue;
            }
            let columns = self.carried_columns_in(&mut tx, table).await?;
            let inserted = sqlx::query(&format!(
                "INSERT INTO {table}({columns}) SELECT {columns} FROM jsonb_populate_recordset(NULL::{table},$1) \
                 ON CONFLICT DO NOTHING"
            ))
            .bind(documents)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            applied.add(table, inserted);
            let differing: Vec<String> = sqlx::query_scalar(&format!(
                "SELECT i.{key} FROM jsonb_populate_recordset(NULL::{table},$1) i JOIN {table} l USING ({key}) \
                 WHERE l.{compared} IS DISTINCT FROM i.{compared}"
            ))
            .bind(documents)
            .fetch_all(&mut *tx)
            .await?;
            for digest in &differing {
                Self::record_conflict(
                    &mut tx,
                    table,
                    digest,
                    origin.index(),
                    "a content-addressed blob is held here with other bytes",
                )
                .await?;
                applied.conflict(table);
            }
        }
        if batch.count > 0 {
            let columns = self.carried_columns_in(&mut tx, "qbit_prism_jobs").await?;
            let inserted = sqlx::query(&format!(
                "INSERT INTO qbit_prism_jobs({columns}) SELECT {columns} FROM \
                 jsonb_populate_recordset(NULL::qbit_prism_jobs,$1) ON CONFLICT DO NOTHING"
            ))
            .bind(&batch.jobs)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            applied.add("qbit_prism_jobs", inserted);
            let compared = columns
                .replace('"', "")
                .split(',')
                .filter(|column| *column != "expires_at")
                .collect::<Vec<_>>()
                .join(",");
            let differing: Vec<String> = sqlx::query_scalar(&format!(
                "SELECT i.job_id FROM jsonb_populate_recordset(NULL::qbit_prism_jobs,$1) i JOIN qbit_prism_jobs l USING (job_id) \
                 WHERE ROW({l}) IS DISTINCT FROM ROW({i})",
                l = prefixed(&compared, "l."),
                i = prefixed(&compared, "i."),
            ))
            .bind(&batch.jobs)
            .fetch_all(&mut *tx)
            .await?;
            for job_id in &differing {
                Self::record_conflict(
                    &mut tx,
                    "qbit_prism_jobs",
                    job_id,
                    origin.index(),
                    "the prepared job is held here with other content",
                )
                .await?;
                applied.conflict("qbit_prism_jobs");
            }
        }
        if let (Some(stream), Some(highest)) = (stream, batch.highest) {
            Self::advance_cursor(&mut tx, stream, origin, highest, Some(highest)).await?;
        }
        tx.commit().await?;
        Ok(applied)
    }

    /// Apply journal rows `origin` wrote (D-4): append-only, so a row held
    /// here already must be identical.
    pub async fn apply_node_roles(&self, rows: &Value, origin: NodeIndex) -> Result<Applied> {
        let mut applied = Applied::default();
        if array_len(rows) == 0 {
            return Ok(applied);
        }
        let mut tx = self.begin().await?;
        let columns = self
            .carried_columns_in(&mut tx, "qbit_prism_node_roles")
            .await?;
        let inserted = sqlx::query(&format!(
            "INSERT INTO qbit_prism_node_roles({columns}) SELECT {columns} FROM \
             jsonb_populate_recordset(NULL::qbit_prism_node_roles,$1) ON CONFLICT DO NOTHING"
        ))
        .bind(rows)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        applied.add("qbit_prism_node_roles", inserted);
        let differing: Vec<i64> = sqlx::query_scalar(&format!(
            "SELECT i.epoch FROM jsonb_populate_recordset(NULL::qbit_prism_node_roles,$1) i \
             JOIN qbit_prism_node_roles l USING (origin_node,epoch) WHERE ROW({l}) IS DISTINCT FROM ROW({i})",
            l = prefixed(&columns.replace('"', ""), "l."),
            i = prefixed(&columns.replace('"', ""), "i."),
        ))
        .bind(rows)
        .fetch_all(&mut *tx)
        .await?;
        for epoch in &differing {
            Self::record_conflict(
                &mut tx,
                "qbit_prism_node_roles",
                &format!("{}:{epoch}", origin.index()),
                origin.index(),
                "the journal row is held here with other content",
            )
            .await?;
            applied.conflict("qbit_prism_node_roles");
        }
        tx.commit().await?;
        Ok(applied)
    }

    /// Record that this node's own log was proved complete on the server
    /// `evidence` names (D-8, D-17).
    pub async fn record_own_log_verified(
        &self,
        evidence: super::node_identity::LineageEvidence,
    ) -> Result<()> {
        let updated = sqlx::query(
            "UPDATE qbit_prism_node_lineage SET verified_system_identifier=$1,verified_timeline=$2,\
             verified_at=clock_timestamp() WHERE singleton",
        )
        .bind(evidence.system_identifier)
        .bind(evidence.timeline)
        .execute(&mut *self.acquire().await?)
        .await?
        .rows_affected();
        ensure!(
            updated == 1,
            "this database has no qbit_prism_node_lineage row; run node-identity set first"
        );
        Ok(())
    }

    /// A conflict found outside an apply transaction.
    pub async fn record_peer_sync_conflict(
        &self,
        table: &str,
        key: &str,
        origin: NodeIndex,
        detail: &str,
    ) -> Result<()> {
        self.record_conflict_alone(table, key, origin.index(), detail)
            .await
    }
}

/// `a,b` with each name prefixed: `l.a,l.b`.
fn prefixed(columns: &str, prefix: &str) -> String {
    columns
        .split(',')
        .map(|column| format!("{prefix}{}", column.trim()))
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derived_columns_are_never_carried_and_landing_facts_are_listed() {
        for table in COPIED_TABLES {
            if let Carried::AllBut(local) = carried(table) {
                for column in local {
                    assert!(
                        ["maturity_state"].contains(column),
                        "{table}.{column} is excluded but not a known derived column"
                    );
                }
            }
        }
        let Carried::Only(block) = carried("qbit_pool_blocks") else {
            panic!("pool blocks carry only their landing facts")
        };
        for derived in [
            "chain_state",
            "maturity_state",
            "matured_at",
            "disconnected_at",
            "inactive_since",
            "audit_publication_sequence",
        ] {
            assert!(!block.contains(&derived), "{derived} is carried");
        }
        for fact in BLOCK_FACTS {
            assert!(block.contains(fact), "{fact} is not carried");
        }
        let Carried::Only(artifact) = carried("qbit_ctv_fanout_artifacts") else {
            panic!("fanout artifacts carry only their manifest columns")
        };
        assert!(!artifact
            .iter()
            .any(|column| column.starts_with("claim_") || column.contains("broadcast")));
        assert!(!artifact.contains(&"settlement_status"));
    }

    #[test]
    fn column_lists_are_prefixed_one_by_one() {
        assert_eq!(prefixed("a, b,c", "l."), "l.a,l.b,l.c");
    }

    #[test]
    fn differences_name_each_side() {
        let mut ours = CarriedColumns::default();
        let mut theirs = CarriedColumns::default();
        ours.0.insert(
            "qbit_prism_jobs",
            vec!["job_id".into(), "origin_node".into()],
        );
        theirs.0.insert("qbit_prism_jobs", vec!["job_id".into()]);
        let differences = ours.differences(&theirs);
        assert_eq!(differences.len(), 1);
        assert!(differences[0].contains("only here: [\"origin_node\"]"));
        assert!(ours.differences(&ours.clone()).is_empty());
    }
}
