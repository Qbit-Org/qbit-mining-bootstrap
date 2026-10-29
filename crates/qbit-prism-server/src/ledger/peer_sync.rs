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
use sqlx::types::Json;
use sqlx::PgConnection;
use std::collections::{BTreeMap, BTreeSet};

/// The sync barrier: `qbit_prism_next_sync_seq()` takes it shared for the
/// rest of its transaction (migration 027), the puller exclusively.
pub const SYNC_BARRIER_LOCK: i64 = 0x505249534d000008;

/// The highest `share_seq` of `$1`'s rows, in the shape only the
/// (origin_node, share_seq) index of migration 031 can serve: with an
/// equality on origin_node the planner may walk the primary key backward
/// through the other node's rows instead.
pub const HIGHEST_SHARE: &str =
    "SELECT share_seq FROM qbit_share_ledger WHERE origin_node BETWEEN $1 AND $1 \
     ORDER BY origin_node DESC,share_seq DESC LIMIT 1";

/// The share ledger's (origin_node, share_seq) index (migration 031), which
/// every share read by origin needs: the sync runs only while it is a valid
/// btree on both databases.
pub const ORIGIN_INDEX: &str = "qbit_share_ledger_origin_seq_idx";

/// Whether [`ORIGIN_INDEX`] is a valid btree on the share ledger's
/// `(origin_node, share_seq)`, with no predicate or expression: false while
/// 031 has not built it, while its online build has not attached every
/// partition's leaf, or when an index of that name is something else.
const ORIGIN_INDEX_VALID: &str =
    "SELECT EXISTS(SELECT 1 FROM pg_index x JOIN pg_class i ON i.oid=x.indexrelid \
     JOIN pg_am am ON am.oid=i.relam \
     WHERE x.indexrelid=to_regclass('qbit_share_ledger_origin_seq_idx') AND x.indisvalid AND am.amname='btree' \
     AND x.indrelid=to_regclass('qbit_share_ledger') AND x.indpred IS NULL AND x.indexprs IS NULL \
     AND (SELECT array_agg(a.attname::text ORDER BY k.ord) FROM unnest(x.indkey::int2[]) WITH ORDINALITY AS k(attnum,ord) \
          JOIN pg_attribute a ON a.attrelid=x.indrelid AND a.attnum=k.attnum)=ARRAY['origin_node','share_seq'])";

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
        // Stored when this database seals a share partition, so it follows
        // this database's partitions (table_inventory.rs).
        "qbit_pool_audit_bundles" => Carried::AllBut(&["canonical_audit_bytes"]),
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
/// A digest of landed block `b`'s [`BLOCK_FACTS`] and its audit digest,
/// everything [`Ledger::apply_block`] compares for a block held already,
/// computed alike on either node: own-log recovery compares it to tell a
/// block held here with other facts from one held the same.
fn block_facts_digest() -> String {
    let pairs = BLOCK_FACTS
        .iter()
        .map(|fact| format!("'{fact}',b.{fact}"))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "md5(jsonb_build_array(jsonb_build_object({pairs}),(SELECT a.audit_bundle_sha256 \
         FROM qbit_pool_audit_bundles a WHERE a.block_hash=b.block_hash))::text)"
    )
}

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

    /// The carried columns of `table`.
    pub fn of(&self, table: &str) -> &[String] {
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
    /// [`ORIGIN_INDEX`] is a valid btree on the peer.
    pub origin_index_valid: bool,
}

/// The peer's cursors over this node's streams: positions in this node's
/// key spaces that the peer has scanned through, and, for shares and
/// prepared jobs, the highest row of this node's it refused as a conflict,
/// quarantined there for good. It holds every one it scanned above that.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PeerCursors {
    pub shares: Option<i64>,
    pub blocks: Option<i64>,
    pub prepared: Option<i64>,
    pub shares_refused: Option<i64>,
    pub prepared_refused: Option<i64>,
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

/// The most prepared jobs one transaction of [`Ledger::apply_prepared`]
/// applies under the blob-GC fence (#785).
pub const PREPARED_CHUNK_ROWS: usize = 1000;

/// The most JSON, of jobs and the blobs they name, one such transaction
/// applies. A fenced apply was measured at about 0.14 ms a job and 64 ms a
/// MiB of JSON, so a chunk holds the fence, and keeps the tip observation
/// (SETTLEMENT held) and the blob collector (SETTLEMENT and ORDER held)
/// waiting behind it, for about 250 ms. A job whose blobs alone are larger
/// goes alone (D-19), and its hold grows with them: a 4 MiB template held
/// the fence about 509 ms, an 8 MiB one about 1,049 ms.
pub const PREPARED_CHUNK_BYTES: usize = 2 << 20;

/// One transaction's part of a [`PreparedBatch`]: consecutive jobs with
/// every blob they name, so it applies alone (D-19).
struct PreparedChunk<'a> {
    jobs: &'a [Value],
    templates: Vec<&'a Value>,
    balances: Vec<&'a Value>,
    highest: Option<i64>,
}

/// The carried columns of the tables a [`PreparedChunk`] writes, read once a
/// batch, before the first fence.
struct PreparedColumns {
    templates: String,
    balances: String,
    jobs: String,
}

/// [`Ledger::apply_prepared`] stopped at a chunk that failed. The chunks
/// before it are committed, with the cursor past them, and `applied` counts
/// them; `pending` jobs, that chunk's and the later ones', are not applied.
#[derive(Debug)]
pub struct PreparedApplyError {
    pub applied: Applied,
    pub pending: u64,
    pub error: anyhow::Error,
}

impl std::fmt::Display for PreparedApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.error, f)
    }
}

impl std::error::Error for PreparedApplyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.error.source()
    }
}

impl PreparedBatch {
    /// The batch in runs of at most [`PREPARED_CHUNK_ROWS`] jobs and
    /// [`PREPARED_CHUNK_BYTES`], in `sync_seq` order. A blob several runs
    /// name goes in each; one no job names is not applied (a pull reads only
    /// the blobs its jobs name). Each run's `highest` is its last job's
    /// `sync_seq`, the last run's the batch's.
    fn chunks(&self) -> Vec<PreparedChunk<'_>> {
        let jobs = self.jobs.as_array().map_or(&[][..], Vec::as_slice);
        let templates = blobs_by_key(&self.templates, "template_sha256");
        let balances = blobs_by_key(&self.balances, "prior_balances_digest");
        let mut chunks = Vec::new();
        let (mut start, mut bytes) = (0, 0);
        let (mut named_templates, mut named_balances) = (BTreeSet::new(), BTreeSet::new());
        for (index, job) in jobs.iter().enumerate() {
            let template = named_blob(job, "template_sha256", &templates);
            let balance = named_blob(job, "window_prior_balances_sha256", &balances);
            let own = json_len(job);
            let size = |templates: &BTreeSet<&str>, balances: &BTreeSet<&str>| {
                own + template
                    .filter(|(key, _)| !templates.contains(key))
                    .map_or(0, |(_, size)| size)
                    + balance
                        .filter(|(key, _)| !balances.contains(key))
                        .map_or(0, |(_, size)| size)
            };
            if index > start
                && (index - start == PREPARED_CHUNK_ROWS
                    || bytes + size(&named_templates, &named_balances) > PREPARED_CHUNK_BYTES)
            {
                chunks.push(PreparedChunk {
                    jobs: &jobs[start..index],
                    templates: blob_documents(&templates, &std::mem::take(&mut named_templates)),
                    balances: blob_documents(&balances, &std::mem::take(&mut named_balances)),
                    highest: jobs[index - 1].get("sync_seq").and_then(Value::as_i64),
                });
                (start, bytes) = (index, 0);
            }
            bytes += size(&named_templates, &named_balances);
            named_templates.extend(template.map(|(key, _)| key));
            named_balances.extend(balance.map(|(key, _)| key));
        }
        if start < jobs.len() {
            chunks.push(PreparedChunk {
                jobs: &jobs[start..],
                templates: blob_documents(&templates, &named_templates),
                balances: blob_documents(&balances, &named_balances),
                highest: self.highest,
            });
        }
        chunks
    }
}

/// The length of `value` as JSON, counted as it is written, not built.
fn json_len(value: &Value) -> usize {
    struct Count(usize);
    impl std::io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 += bytes.len();
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    // Writing a `Value` cannot fail, and `Count` never does.
    serde_json::to_writer(&mut count, value).map_or(0, |()| count.0)
}

/// Each blob document of `documents` by its `key`, with its JSON length.
fn blobs_by_key<'a>(documents: &'a Value, key: &str) -> BTreeMap<&'a str, (&'a Value, usize)> {
    documents
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|document| Some((document.get(key)?.as_str()?, (document, json_len(document)))))
        .collect()
}

/// The key and JSON length of the blob `job` names in `column`, when the
/// batch carries it.
fn named_blob<'a>(
    job: &Value,
    column: &str,
    blobs: &BTreeMap<&'a str, (&'a Value, usize)>,
) -> Option<(&'a str, usize)> {
    let (key, (_, size)) = blobs.get_key_value(job.get(column)?.as_str()?)?;
    Some((*key, *size))
}

fn blob_documents<'a>(
    blobs: &BTreeMap<&'a str, (&'a Value, usize)>,
    keys: &BTreeSet<&str>,
) -> Vec<&'a Value> {
    keys.iter()
        .filter_map(|key| blobs.get(key).map(|(document, _)| *document))
        .collect()
}

/// A prepared job's carried `columns` compared when it is held already: all
/// but its expiry, which its writer renews.
fn compared_job_columns(columns: &str) -> String {
    columns
        .replace('"', "")
        .split(',')
        .filter(|column| *column != "expires_at")
        .collect::<Vec<_>>()
        .join(",")
}

/// Reads on the peer's database, as its read-only sync role.
pub mod peer {
    use super::*;

    /// The peer's identity, fingerprint and floors. The cluster row is read
    /// without a predicate: the table holds one row (its key is the
    /// singleton), and the sync role may read only `config_fingerprint`.
    pub async fn facts(connection: &mut PgConnection) -> Result<PeerFacts> {
        let row = sqlx::query(&format!(
            "SELECT (SELECT node_index FROM qbit_prism_node_identity WHERE singleton) AS node_index,\
             (SELECT config_fingerprint FROM qbit_prism_cluster) AS config_fingerprint,\
             (SELECT share_seq_floor FROM qbit_prism_node_lineage WHERE singleton) AS share_seq_floor,\
             (SELECT sync_seq_floor FROM qbit_prism_node_lineage WHERE singleton) AS sync_seq_floor,\
             ({ORIGIN_INDEX_VALID}) AS origin_index_valid"
        ))
        .fetch_one(&mut *connection)
        .await?;
        let node: Option<i16> = row.try_get("node_index")?;
        Ok(PeerFacts {
            node: node.and_then(|index| NodeIndex::from_index(index.into())),
            config_fingerprint: row.try_get("config_fingerprint")?,
            share_seq_floor: row.try_get("share_seq_floor")?,
            sync_seq_floor: row.try_get("sync_seq_floor")?,
            origin_index_valid: row.try_get("origin_index_valid")?,
        })
    }

    /// How far the peer has pulled this node's streams.
    pub async fn cursors(connection: &mut PgConnection) -> Result<PeerCursors> {
        let rows: Vec<(String, i64, Option<i64>)> = sqlx::query_as(
            "SELECT stream,scanned_through,ingested_through FROM qbit_prism_peer_sync_cursors",
        )
        .fetch_all(&mut *connection)
        .await?;
        let mut cursors = PeerCursors::default();
        for (stream, position, refused) in rows {
            match stream.as_str() {
                SHARES => (cursors.shares, cursors.shares_refused) = (Some(position), refused),
                BLOCKS => cursors.blocks = Some(position),
                PREPARED => {
                    (cursors.prepared, cursors.prepared_refused) = (Some(position), refused)
                }
                _ => {}
            }
        }
        Ok(cursors)
    }

    /// The peer's rows above `after`, in `share_seq` order, scanning at
    /// most `limit` rows of any origin and returning those `origin` wrote.
    /// One statement, so the rows, their mappings and the position reached
    /// share its snapshot, and no snapshot outlives it.
    pub async fn shares_scanned(
        connection: &mut PgConnection,
        after: i64,
        limit: i64,
        origin: NodeIndex,
    ) -> Result<ShareBatch> {
        let row = sqlx::query(&shares_scanned_sql())
            .bind(after)
            .bind(limit)
            .bind(origin.index())
            .fetch_one(&mut *connection)
            .await?;
        share_batch(&row)
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
        let row = sqlx::query(&shares_of_sql())
            .bind(origin.index())
            .bind(after)
            .bind(limit)
            .fetch_one(&mut *connection)
            .await?;
        share_batch(&row)
    }

    /// [`shares_scanned`]'s statement: `$1` after, `$2` limit, `$3` origin.
    /// The scan walks the primary key from the cursor under its limit; the
    /// pick reads the origin's rows in the scanned range through the
    /// (origin_node, share_seq) index of 031, bounded on both sides.
    pub fn shares_scanned_sql() -> String {
        format!(
            "WITH scanned AS MATERIALIZED (SELECT share_seq FROM qbit_share_ledger \
                WHERE share_seq>$1 ORDER BY share_seq LIMIT $2),\
             bounds AS MATERIALIZED (SELECT max(share_seq) AS through,count(*) AS scanned FROM scanned),\
             picked AS MATERIALIZED (SELECT s.* FROM qbit_share_ledger s WHERE s.origin_node BETWEEN $3 AND $3 \
                AND s.share_seq>$1 AND s.share_seq<=(SELECT through FROM bounds) \
                ORDER BY s.origin_node,s.share_seq) \
             SELECT (SELECT through FROM bounds) AS through,(SELECT scanned FROM bounds) AS scanned,{PICKED_SHARES}"
        )
    }

    /// [`shares_of`]'s statement: `$1` origin, `$2` after, `$3` limit. Only
    /// the (origin_node, share_seq) index of 031 can give its order, so the
    /// planner never walks the primary key through the other node's rows,
    /// and the read stops at the limit or the origin's last row.
    pub fn shares_of_sql() -> String {
        format!(
            "WITH picked AS MATERIALIZED (SELECT s.* FROM qbit_share_ledger s \
                WHERE s.origin_node BETWEEN $1 AND $1 AND s.share_seq>$2 \
                ORDER BY s.origin_node,s.share_seq LIMIT $3) \
             SELECT (SELECT max(share_seq) FROM picked) AS through,\
                (SELECT count(*) FROM picked) AS scanned,{PICKED_SHARES}"
        )
    }

    /// [`shares_beyond`]'s statement: `$1` origin, `$2` after, `$3` cap, in
    /// the shape of [`shares_of_sql`].
    pub const SHARES_BEYOND: &str =
        "SELECT count(*) FROM (SELECT 1 FROM qbit_share_ledger WHERE origin_node BETWEEN $1 AND $1 \
         AND share_seq>$2 ORDER BY origin_node,share_seq LIMIT $3) beyond";

    /// The columns both share reads return of their `picked` rows. The
    /// header mappings are looked up by an array of the picked share IDs, so
    /// the mapping table, which holds a row for every share, is probed by
    /// its share_id key: a semi-join on the CTE lets the planner hash it and
    /// scan the whole table.
    const PICKED_SHARES: &str = "COALESCE((SELECT jsonb_agg(to_jsonb(p) ORDER BY p.share_seq) FROM picked p),'[]'::jsonb) AS rows,\
        COALESCE((SELECT jsonb_agg(to_jsonb(h) ORDER BY h.header_hash) FROM qbit_prism_share_hashes h \
            WHERE h.share_id=ANY(ARRAY(SELECT share_id FROM picked WHERE accepted))),'[]'::jsonb) AS hashes,\
        (SELECT count(*) FROM picked) AS row_count,(SELECT max(share_seq) FROM picked) AS highest,\
        (SELECT floor(extract(epoch FROM max(accepted_at))*1000)::bigint FROM picked) AS accepted_ms";

    fn share_batch(row: &PgRow) -> Result<ShareBatch> {
        Ok(ShareBatch {
            through: row.try_get("through")?,
            scanned: row.try_get("scanned")?,
            rows: row.try_get("rows")?,
            hashes: row.try_get("hashes")?,
            row_count: usize::try_from(row.try_get::<i64, _>("row_count")?)?,
            highest: row.try_get("highest")?,
            highest_accepted_at_ms: row.try_get("accepted_ms")?,
        })
    }

    /// The highest `share_seq` and `sync_seq` the peer holds of rows
    /// `origin` wrote, and of the journal's epochs. The share read is
    /// [`HIGHEST_SHARE`].
    pub async fn highest_of(
        connection: &mut PgConnection,
        origin: NodeIndex,
    ) -> Result<(Option<i64>, Option<i64>, Option<i64>)> {
        Ok(sqlx::query_as(&format!(
            "SELECT ({HIGHEST_SHARE}),\
             GREATEST((SELECT max(sync_seq) FROM qbit_pool_blocks WHERE origin_node=$1),\
                      (SELECT max(sync_seq) FROM qbit_prism_jobs WHERE origin_node=$1 AND job_id LIKE 'prepared:%')),\
             (SELECT max(epoch) FROM qbit_prism_node_roles WHERE origin_node=$1)"
        ))
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
        Ok(sqlx::query_scalar(SHARES_BEYOND)
            .bind(origin.index())
            .bind(after)
            .bind(cap)
            .fetch_one(&mut *connection)
            .await?)
    }

    /// The peer's sync sequence position at the sync barrier, or `None` while
    /// a transaction that drew a `sync_seq` is still open (the barrier is
    /// held shared). One call to `qbit_prism_sync_barrier()` (migration 027),
    /// which tries the barrier exclusively for its own statement only and
    /// reads the sequence under it: every row at or below the position read
    /// has committed or never will. The lock ends with the statement, so a
    /// puller that stalls or vanishes can never leave it held, and the
    /// writers it shares the barrier with wait for that one statement at
    /// most.
    pub async fn sync_barrier(connection: &mut PgConnection) -> Result<Option<Option<i64>>> {
        let (taken, position): (bool, Option<i64>) =
            sqlx::query_as("SELECT taken,sync_position FROM qbit_prism_sync_barrier()")
                .fetch_one(&mut *connection)
                .await?;
        Ok(taken.then_some(position))
    }

    /// Each landed block `b` that `origin` ($1) wrote, with every row its
    /// landing wrote, as [`BlockBundle`] reads it; the caller completes the
    /// `WHERE` clause.
    const BLOCK_BUNDLE: &str = "SELECT b.sync_seq,b.block_hash,to_jsonb(b) AS block,\
             (SELECT to_jsonb(a) FROM qbit_pool_audit_bundles a WHERE a.block_hash=b.block_hash) AS bundle,\
             (SELECT to_jsonb(s) FROM qbit_prism_audit_snapshots s WHERE s.snapshot_sha256=\
                (SELECT a.share_snapshot_sha256 FROM qbit_pool_audit_bundles a WHERE a.block_hash=b.block_hash)) AS snapshot,\
             COALESCE((SELECT jsonb_agg(to_jsonb(p) ORDER BY p.payout_entry_seq) FROM qbit_pool_payout_entries p WHERE p.block_hash=b.block_hash),'[]'::jsonb) AS payouts,\
             COALESCE((SELECT jsonb_agg(to_jsonb(c) ORDER BY c.carry_forward_seq) FROM qbit_payout_carry_forward c WHERE c.block_hash=b.block_hash),'[]'::jsonb) AS carries,\
             (SELECT to_jsonb(f) FROM qbit_ctv_fanout_sets f WHERE f.block_hash=b.block_hash) AS fanout_set,\
             COALESCE((SELECT jsonb_agg(to_jsonb(r) ORDER BY r.chunk_index) FROM qbit_ctv_fanout_artifacts r WHERE r.block_hash=b.block_hash),'[]'::jsonb) AS artifacts \
             FROM qbit_pool_blocks b WHERE b.origin_node=$1 AND ";

    fn block_bundles(rows: &[sqlx::postgres::PgRow]) -> Result<Vec<BlockBundle>> {
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

    /// Landed blocks `origin` wrote with `sync_seq` in `(after, through]`,
    /// at most `limit`, each with every row its landing wrote.
    pub async fn blocks(
        connection: &mut PgConnection,
        origin: NodeIndex,
        after: i64,
        through: Option<i64>,
        limit: i64,
    ) -> Result<Vec<BlockBundle>> {
        let rows = sqlx::query(&format!(
            "{BLOCK_BUNDLE}b.sync_seq>$2 AND b.sync_seq<=$3 ORDER BY b.sync_seq LIMIT $4"
        ))
        .bind(origin.index())
        .bind(after)
        .bind(through.unwrap_or(i64::MAX))
        .bind(limit)
        .fetch_all(&mut *connection)
        .await?;
        block_bundles(&rows)
    }

    /// The landed blocks `origin` wrote among `hashes`, each with every row
    /// its landing wrote, in `sync_seq` order.
    pub async fn blocks_with_hashes(
        connection: &mut PgConnection,
        origin: NodeIndex,
        hashes: &[String],
    ) -> Result<Vec<BlockBundle>> {
        let rows = sqlx::query(&format!(
            "{BLOCK_BUNDLE}b.block_hash=ANY($2) ORDER BY b.sync_seq"
        ))
        .bind(origin.index())
        .bind(hashes)
        .fetch_all(&mut *connection)
        .await?;
        block_bundles(&rows)
    }

    /// The `sync_seq`, hash and facts digest of each landed block `origin`
    /// wrote with `sync_seq` above `after`, at most `limit`, in `sync_seq`
    /// order: what own-log recovery compares before it reads any block
    /// whole.
    pub async fn block_keys(
        connection: &mut PgConnection,
        origin: NodeIndex,
        after: i64,
        limit: i64,
    ) -> Result<Vec<(i64, String, String)>> {
        Ok(sqlx::query_as(&format!(
            "SELECT b.sync_seq,b.block_hash,{} FROM qbit_pool_blocks b WHERE b.origin_node=$1 \
             AND b.sync_seq>$2 ORDER BY b.sync_seq LIMIT $3",
            block_facts_digest()
        ))
        .bind(origin.index())
        .bind(after)
        .bind(limit)
        .fetch_all(&mut *connection)
        .await?)
    }

    /// Prepared jobs `origin` wrote with `sync_seq` in `(after, through]`,
    /// at most `limit`, with the template and balance blobs they reference.
    /// `unexpired` keeps only the jobs whose expiry has not passed, by the
    /// peer's clock.
    pub async fn prepared(
        connection: &mut PgConnection,
        origin: NodeIndex,
        after: i64,
        through: Option<i64>,
        limit: i64,
        unexpired: bool,
    ) -> Result<PreparedBatch> {
        let row = sqlx::query(
            "WITH picked AS MATERIALIZED (SELECT j.* FROM qbit_prism_jobs j WHERE j.origin_node=$1 \
                AND j.job_id LIKE 'prepared:%' AND j.sync_seq>$2 AND j.sync_seq<=$3 \
                AND (NOT $5 OR j.expires_at>statement_timestamp()) \
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
        .bind(unexpired)
        .fetch_one(&mut *connection)
        .await?;
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
}

impl Ledger {
    /// The columns a pull carries of each copied table, from this
    /// database's catalog.
    pub async fn carried_columns(&self) -> Result<CarriedColumns> {
        CarriedColumns::read(&mut *self.acquire().await?).await
    }

    /// The cluster fingerprint this database is pinned to (D-6), as stored.
    /// Whether [`ORIGIN_INDEX`] is a valid btree in this database.
    pub async fn origin_index_valid(&self) -> Result<bool> {
        Ok(sqlx::query_scalar(ORIGIN_INDEX_VALID)
            .fetch_one(&mut *self.acquire().await?)
            .await?)
    }

    pub async fn stored_config_fingerprint(&self) -> Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT config_fingerprint FROM qbit_prism_cluster WHERE singleton")
                .fetch_one(&mut *self.acquire().await?)
                .await?,
        )
    }

    /// The `sync_seq` of the prepared record a block found on the issued job
    /// `job_id` was built on, the one the job's `prepared_key` names, when
    /// this node originated it (D-19). `None` when the peer holds it anyway:
    /// it is the peer's own, or it predates migration 027, which both
    /// databases of a pair started from. An error when the job or its record
    /// is no longer held here: nothing then proves the peer holds it.
    pub async fn own_prepared_sync_seq(
        &self,
        node: NodeIndex,
        job_id: &str,
    ) -> Result<Option<i64>> {
        let record: Option<(i16, Option<i64>)> = sqlx::query_as(
            "SELECT p.origin_node,p.sync_seq FROM qbit_prism_jobs i JOIN qbit_prism_jobs p \
             ON p.job_id=i.payload->>'prepared_key' WHERE i.job_id=$1",
        )
        .bind(job_id)
        .fetch_optional(&mut *self.acquire().await?)
        .await?;
        let (origin, sync_seq) = record
            .context("the block's issued job or its prepared record is no longer held here")?;
        Ok(sync_seq.filter(|_| origin == node.index()))
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
        Ok(sqlx::query_as(&format!(
            "SELECT ({HIGHEST_SHARE}),\
             (SELECT max(sync_seq) FROM qbit_pool_blocks WHERE origin_node=$1),\
             (SELECT max(sync_seq) FROM qbit_prism_jobs WHERE origin_node=$1 AND job_id LIKE 'prepared:%'),\
             (SELECT max(epoch) FROM qbit_prism_node_roles WHERE origin_node=$1)"
        ))
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
        // A row this node originated meets a conflict only in own-log
        // recovery (a pull reads only the peer's rows), where it means the
        // own log has diverged: the verification goes in the same
        // transaction, so no start without the peer latches on it (D-17).
        sqlx::query(
            "UPDATE qbit_prism_node_lineage SET verified_system_identifier=NULL,verified_timeline=NULL,\
             verified_at=NULL WHERE singleton AND verified_at IS NOT NULL \
             AND $1=(SELECT node_index FROM qbit_prism_node_identity WHERE singleton)",
        )
        .bind(origin)
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
    /// back. For shares and prepared jobs, `ingested_through` is the highest
    /// peer row a pull refused as a conflict (`refused_through`; NULL while
    /// none was): quarantined for good, never inserted and never read again,
    /// while this node holds every peer row it scanned above it. The peer's
    /// found-block wait reads it.
    async fn advance_cursor(
        tx: &mut Transaction<'_, Postgres>,
        stream: &str,
        peer: NodeIndex,
        scanned_through: i64,
        refused_through: Option<i64>,
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
        .bind(refused_through)
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

    /// The highest `share_seq` this node accepts from a pull: its attached
    /// partitions, its own sequence and `above`, whichever reaches highest,
    /// plus `PULL_HEADROOM_PARTITIONS` partitions.
    async fn share_seq_ceiling(&self, above: i64) -> Result<i64> {
        let ceiling: i64 = sqlx::query_scalar(
            "SELECT GREATEST((SELECT COALESCE(max(upper_seq),0) FROM qbit_prism_share_partitions WHERE state='attached'),\
             (SELECT last_value FROM qbit_share_ledger_share_seq_seq),$2::bigint)\
             +$1*(SELECT partition_rows FROM qbit_prism_share_partitioning WHERE singleton)",
        )
        .bind(PULL_HEADROOM_PARTITIONS)
        .bind(above)
        .fetch_one(&mut *self.acquire().await?)
        .await?;
        Ok(ceiling)
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
    /// back from the peer (`None`). For those, `own_rows_through` is what
    /// bounds every own row the peer holds: the higher of its cursor over
    /// this node's shares and its own lineage floor.
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
        own_rows_through: Option<i64>,
    ) -> Result<Applied> {
        let mut applied = Applied::default();
        let own = origin == node;
        // A key this far above what this node can hold is no key a node
        // drew: raising the sequence to it would leave every local append
        // without a partition. Refused below as a conflict, never raised to.
        // This node's own rows, read back from the peer after a restore, can
        // lie far above its restored sequence (a backup node restored from
        // an old backup), but each came to the peer through a pull, which
        // its cursor over this node's shares covers, or was held when the
        // peer was personalised, below its floor (`node-identity
        // repersonalise` also sets that cursor above them): their ceiling
        // counts from the higher of the two.
        let above = own_rows_through.filter(|_| own).unwrap_or(0);
        let ceiling = self.share_seq_ceiling(above).await?;
        let acceptable = acceptable_highest(&batch.rows, ceiling)?;
        if batch.row_count > 0 {
            // Above every row about to be inserted, the peer's and this node's
            // own alike, before the lead is attached, so `ensure` covers them.
            if let Some(highest) = acceptable {
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
        let mut refused_through = None;
        if batch.row_count > 0 {
            // Header mappings this node holds for another share, a share this
            // node credits, or holds or retains as rejected, at another
            // share_seq (the ledger is unique on share_id per partition only,
            // and a rejected share has no mapping), and rows below every
            // attached partition: refused whole, one conflict per share
            // whatever number of these it breaks.
            let refused: Vec<(String, String)> = sqlx::query_as(
                "WITH incoming AS (SELECT * FROM jsonb_populate_recordset(NULL::qbit_share_ledger,$1)),\
                 mappings AS (SELECT * FROM jsonb_populate_recordset(NULL::qbit_prism_share_hashes,$2)) \
                 SELECT share_id,string_agg(detail,'; ' ORDER BY detail) FROM (\
                 SELECT m.share_id,'header '||m.header_hash||' is credited here to '||h.share_id \
                 FROM mappings m JOIN qbit_prism_share_hashes h ON h.header_hash=m.header_hash WHERE h.share_id<>m.share_id \
                 UNION ALL SELECT m.share_id,'share_id '||m.share_id||' maps header '||h.header_hash||' here, not '||m.header_hash \
                 FROM mappings m JOIN qbit_prism_share_hashes h ON h.share_id=m.share_id WHERE h.header_hash<>m.header_hash \
                 UNION ALL SELECT i.share_id,'share_id '||i.share_id||' is credited here at another share_seq than '||i.share_seq \
                 FROM incoming i WHERE EXISTS (SELECT 1 FROM qbit_prism_share_hashes h WHERE h.share_id=i.share_id) \
                 AND NOT EXISTS (SELECT 1 FROM qbit_share_ledger l WHERE l.share_seq=i.share_seq AND l.share_id=i.share_id) \
                 UNION ALL SELECT i.share_id,'share_id '||i.share_id||' is held here as a rejected share at another share_seq' \
                 FROM incoming i WHERE EXISTS (SELECT 1 FROM qbit_share_ledger l WHERE l.share_id=i.share_id AND l.share_seq<>i.share_seq AND NOT l.accepted) \
                 OR EXISTS (SELECT 1 FROM qbit_prism_rejected_share_ids r WHERE r.share_id=i.share_id AND r.share_seq<>i.share_seq) \
                 UNION ALL SELECT i.share_id,'share_seq '||i.share_seq||' is below every attached partition' FROM incoming i \
                 WHERE i.share_seq<(SELECT COALESCE(min(lower_seq),-1) FROM qbit_prism_share_partitions WHERE state='attached' AND lower_seq IS NOT NULL) \
                 AND NOT EXISTS (SELECT 1 FROM qbit_prism_share_partitions WHERE state='attached' AND lower_seq IS NULL) \
                 UNION ALL SELECT i.share_id,'share_seq '||i.share_seq||' is beyond any key a node draws here (above '||$3||')' \
                 FROM incoming i WHERE i.share_seq>$3\
                 ) refusal(share_id,detail) GROUP BY share_id",
            )
            .bind(&batch.rows)
            .bind(&batch.hashes)
            .bind(ceiling)
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
            let refused: BTreeSet<&str> = refused_ids.iter().map(String::as_str).collect();
            refused_through = batch
                .rows
                .as_array()
                .map(Vec::as_slice)
                .unwrap_or_default()
                .iter()
                // A refused rejected share is in no window, so it never holds
                // the found-block wait back.
                .filter(|row| {
                    row.get("accepted").and_then(Value::as_bool) != Some(false)
                        && row
                            .get("share_id")
                            .and_then(Value::as_str)
                            .is_some_and(|id| refused.contains(id))
                })
                .filter_map(|row| row.get("share_seq").and_then(Value::as_i64))
                .chain(differing.iter().map(|(share_seq, _)| *share_seq))
                .max();
            // Inserted below the hashrate rollups' watermark, which this
            // node's sweep has passed and never reads again: folded here, in
            // this transaction, under the sweep's progress row. A sweep
            // running now holds that row, so this waits for it, one sweep
            // statement at most. Before the first sweep there is no row: the
            // table is locked against that sweep until this pull commits, and
            // the fold runs again against any sweep that committed first.
            if !inserted.is_empty() {
                let (mut folded, held) = fold_late_peer_shares(&mut tx, batch, &inserted).await?;
                if !held {
                    sqlx::query(crate::rollups::PROGRESS_TABLE_LOCK)
                        .execute(&mut *tx)
                        .await?;
                    (folded, _) = fold_late_peer_shares(&mut tx, batch, &inserted).await?;
                }
                if folded > 0 {
                    tracing::debug!(folded, "peer shares below the rollup watermark folded");
                }
            }
        }
        if let (Some(stream), Some(through)) = (stream, batch.through) {
            Self::advance_cursor(&mut tx, stream, origin, through, refused_through).await?;
        }
        tx.commit().await?;
        Ok(applied)
    }

    /// The carried columns of `table`, quoted, read on `connection` (or in
    /// a transaction on it).
    async fn carried_columns_in(
        &self,
        connection: &mut PgConnection,
        table: &str,
    ) -> Result<String> {
        let columns = local_columns(connection, table).await?;
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
                // Only a row this node holds under the same identity with
                // other content skips the block, recorded and alerted. Any
                // other failure (a lock or statement timeout, an I/O stall)
                // rolls the whole transaction back with its cursor, so the
                // next pass applies the block again: it is never lost.
                Err(BlockRefused::Conflict(detail)) => {
                    savepoint.rollback().await?;
                    Self::record_conflict(
                        &mut tx,
                        "qbit_pool_blocks",
                        &block.block_hash,
                        origin,
                        &format!("the block could not be applied: {detail}"),
                    )
                    .await?;
                    applied.conflict("qbit_pool_blocks");
                }
                Err(BlockRefused::Failed(error)) => {
                    return Err(error.context(format!(
                        "applying peer block {}; it is retried on the next pass",
                        block.block_hash
                    )));
                }
            }
        }
        if let Some((stream, peer)) = stream {
            // No stop: nothing waits on the peer holding a block.
            Self::advance_cursor(&mut tx, stream, peer, block.sync_seq, None).await?;
        }
        tx.commit().await?;
        Ok(applied)
    }

    async fn insert_block(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        block: &BlockBundle,
    ) -> Result<Vec<(&'static str, u64)>, BlockRefused> {
        self.insert_block_rows(tx, block)
            .await
            .map_err(BlockRefused::classify)
    }

    async fn insert_block_rows(
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
                if !same {
                    return Err(anyhow::Error::new(HeldOtherwise(
                        "its audit snapshot is held here with other content (D-15)",
                    )));
                }
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

    /// Apply prepared jobs with their blobs (D-19): a node that holds a
    /// record then holds both blobs it names. A content-addressed blob held
    /// here already must be byte-identical; a job held here already must
    /// match on everything but its expiry, which its writer renews. `stream`
    /// names the cursor to advance, when the jobs are the peer's.
    ///
    /// The batch goes in [`PreparedBatch::chunks`], each in a transaction of
    /// its own under the blob-GC fence, blobs first, that advances the cursor
    /// through its last job (#785). A chunk that fails stops the batch: the
    /// chunks before it stay applied, with the cursor past them, and the
    /// error counts them ([`PreparedApplyError`]); applying them again
    /// changes nothing.
    pub async fn apply_prepared(
        &self,
        batch: &PreparedBatch,
        origin: NodeIndex,
        stream: Option<&str>,
    ) -> Result<Applied, PreparedApplyError> {
        let mut applied = Applied::default();
        let chunks = batch.chunks();
        let mut pending: u64 = chunks.iter().map(|chunk| chunk.jobs.len() as u64).sum();
        if chunks.is_empty() {
            return Ok(applied);
        }
        let columns = match self.prepared_columns().await {
            Ok(columns) => columns,
            Err(error) => {
                return Err(PreparedApplyError {
                    applied,
                    pending,
                    error,
                })
            }
        };
        // A blob conflict is recorded once for the batch, however many of
        // its chunks name the blob; each of them is refused.
        let mut recorded = BTreeSet::new();
        for chunk in &chunks {
            match self
                .apply_prepared_chunk(chunk, &columns, origin, stream, &mut recorded)
                .await
            {
                Ok(done) => applied.merge(done),
                Err(error) => {
                    return Err(PreparedApplyError {
                        applied,
                        pending,
                        error,
                    })
                }
            }
            pending -= chunk.jobs.len() as u64;
        }
        Ok(applied)
    }

    async fn prepared_columns(&self) -> Result<PreparedColumns> {
        let mut connection = self.acquire().await?;
        Ok(PreparedColumns {
            templates: self
                .carried_columns_in(&mut connection, "qbit_prism_templates")
                .await?,
            balances: self
                .carried_columns_in(&mut connection, "qbit_prism_balance_snapshots")
                .await?,
            jobs: self
                .carried_columns_in(&mut connection, "qbit_prism_jobs")
                .await?,
        })
    }

    async fn apply_prepared_chunk(
        &self,
        chunk: &PreparedChunk<'_>,
        columns: &PreparedColumns,
        origin: NodeIndex,
        stream: Option<&str>,
        recorded: &mut BTreeSet<(&'static str, String)>,
    ) -> Result<Applied> {
        let mut applied = Applied::default();
        let mut refused = false;
        let mut tx = self.begin().await?;
        // Fence blob GC as `save_compact_prepared` does, before any blob is
        // inserted or found held, through the commit: the collector deletes,
        // under the cluster row FOR UPDATE, every blob no committed job
        // names, and `ON CONFLICT DO NOTHING` locks no held blob. It then
        // either commits first, and the inserts below put the blobs back, or
        // scans after these jobs are visible. No advisory lock may follow
        // this fence (the collector takes SETTLEMENT and ORDER before it),
        // and none does: the job insert names `sync_seq`, so its
        // barrier-taking default never runs. No `writable()` check, as in
        // every peer-sync apply: a halted node still ingests the peer's log,
        // so it keeps D-19 for the blocks the peer finds.
        sqlx::query("SELECT singleton FROM qbit_prism_cluster WHERE singleton FOR KEY SHARE")
            .fetch_one(&mut *tx)
            .await?;
        for (table, key, documents, compared, columns) in [
            (
                "qbit_prism_templates",
                "template_sha256",
                &chunk.templates,
                "template_bytes",
                &columns.templates,
            ),
            (
                "qbit_prism_balance_snapshots",
                "prior_balances_digest",
                &chunk.balances,
                "balances",
                &columns.balances,
            ),
        ] {
            if documents.is_empty() {
                continue;
            }
            let inserted = sqlx::query(&format!(
                "INSERT INTO {table}({columns}) SELECT {columns} FROM jsonb_populate_recordset(NULL::{table},$1) \
                 ON CONFLICT DO NOTHING"
            ))
            .bind(Json(documents))
            .execute(&mut *tx)
            .await?
            .rows_affected();
            applied.add(table, inserted);
            let differing: Vec<String> = sqlx::query_scalar(&format!(
                "SELECT i.{key} FROM jsonb_populate_recordset(NULL::{table},$1) i JOIN {table} l USING ({key}) \
                 WHERE l.{compared} IS DISTINCT FROM i.{compared}"
            ))
            .bind(Json(documents))
            .fetch_all(&mut *tx)
            .await?;
            for digest in differing {
                refused = true;
                if !recorded.insert((table, digest.clone())) {
                    continue;
                }
                Self::record_conflict(
                    &mut tx,
                    table,
                    &digest,
                    origin.index(),
                    "a content-addressed blob is held here with other bytes",
                )
                .await?;
                applied.conflict(table);
            }
        }
        if !chunk.jobs.is_empty() {
            let columns = &columns.jobs;
            let inserted = sqlx::query(&format!(
                "INSERT INTO qbit_prism_jobs({columns}) SELECT {columns} FROM \
                 jsonb_populate_recordset(NULL::qbit_prism_jobs,$1) ON CONFLICT DO NOTHING"
            ))
            .bind(Json(chunk.jobs))
            .execute(&mut *tx)
            .await?
            .rows_affected();
            applied.add("qbit_prism_jobs", inserted);
            let compared = compared_job_columns(columns);
            let differing: Vec<String> = sqlx::query_scalar(&format!(
                "SELECT i.job_id FROM jsonb_populate_recordset(NULL::qbit_prism_jobs,$1) i JOIN qbit_prism_jobs l USING (job_id) \
                 WHERE ROW({l}) IS DISTINCT FROM ROW({i})",
                l = prefixed(&compared, "l."),
                i = prefixed(&compared, "i."),
            ))
            .bind(Json(chunk.jobs))
            .fetch_all(&mut *tx)
            .await?;
            for job_id in &differing {
                refused = true;
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
        if let (Some(stream), Some(highest)) = (stream, chunk.highest) {
            // A conflict anywhere in the chunk, a job or a blob it names:
            // refused through the chunk's highest job.
            let refused_through = refused.then_some(highest);
            Self::advance_cursor(&mut tx, stream, origin, highest, refused_through).await?;
        }
        tx.commit().await?;
        Ok(applied)
    }

    /// How this node holds `batch`: how many of its jobs it lacks, and
    /// whether it holds every job and blob in it as the batch has them, a
    /// job's expiry aside. Then applying it would insert nothing and find no
    /// conflict, so own-log recovery skips its fenced apply (#785).
    pub async fn prepared_held(&self, batch: &PreparedBatch) -> Result<(i64, bool)> {
        let mut connection = self.acquire().await?;
        let compared = compared_job_columns(
            &self
                .carried_columns_in(&mut connection, "qbit_prism_jobs")
                .await?,
        );
        Ok(sqlx::query_as(&format!(
            "SELECT (SELECT count(*) FROM jsonb_array_elements($1) j \
                WHERE NOT EXISTS (SELECT 1 FROM qbit_prism_jobs p WHERE p.job_id=j->>'job_id')), \
             NOT (EXISTS(SELECT 1 FROM jsonb_populate_recordset(NULL::qbit_prism_jobs,$1) i \
                LEFT JOIN qbit_prism_jobs l USING (job_id) WHERE l.job_id IS NULL OR ROW({l}) IS DISTINCT FROM ROW({i})) \
             OR EXISTS(SELECT 1 FROM jsonb_populate_recordset(NULL::qbit_prism_templates,$2) i \
                LEFT JOIN qbit_prism_templates l USING (template_sha256) \
                WHERE l.template_sha256 IS NULL OR l.template_bytes IS DISTINCT FROM i.template_bytes) \
             OR EXISTS(SELECT 1 FROM jsonb_populate_recordset(NULL::qbit_prism_balance_snapshots,$3) i \
                LEFT JOIN qbit_prism_balance_snapshots l USING (prior_balances_digest) \
                WHERE l.prior_balances_digest IS NULL OR l.balances IS DISTINCT FROM i.balances))",
            l = prefixed(&compared, "l."),
            i = prefixed(&compared, "i."),
        ))
        .bind(&batch.jobs)
        .bind(&batch.templates)
        .bind(&batch.balances)
        .fetch_one(&mut *connection)
        .await?)
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

    /// Forget the last own-log verification (D-17): own rows the peer holds
    /// are missing here, so the evidence it recorded no longer proves this
    /// database whole, as after a restore that kept the timeline. Until
    /// recovery records a new one, the latch never sets without the peer.
    /// Returns whether a verification was forgotten.
    pub async fn forget_own_log_verification(&self) -> Result<bool> {
        Ok(sqlx::query(
            "UPDATE qbit_prism_node_lineage SET verified_system_identifier=NULL,verified_timeline=NULL,\
             verified_at=NULL WHERE singleton AND verified_at IS NOT NULL",
        )
        .execute(&mut *self.acquire().await?)
        .await?
        .rows_affected()
            == 1)
    }

    /// The blocks among `keys` (hash and facts digest, as
    /// [`peer::block_keys`] reads them) this database lacks or holds with
    /// another digest, in their order, each with whether it is held here.
    pub async fn missing_blocks(&self, keys: &[(String, String)]) -> Result<Vec<(String, bool)>> {
        let (hashes, digests): (Vec<&str>, Vec<&str>) = keys
            .iter()
            .map(|(hash, digest)| (hash.as_str(), digest.as_str()))
            .unzip();
        Ok(sqlx::query_as(&format!(
            "SELECT u.h,b.block_hash IS NOT NULL FROM unnest($1::text[],$2::text[]) \
             WITH ORDINALITY AS u(h,d,n) LEFT JOIN qbit_pool_blocks b ON b.block_hash=u.h \
             WHERE b.block_hash IS NULL OR {} IS DISTINCT FROM u.d ORDER BY u.n",
            block_facts_digest()
        ))
        .bind(&hashes)
        .bind(&digests)
        .fetch_all(&mut *self.acquire().await?)
        .await?)
    }

    /// Record that this node's own log was proved complete on the server
    /// `evidence` names (D-8, D-17).
    ///
    /// `evidence` is what the server showed when recovery began; the update
    /// applies only while it still shows it, in the same statement. Each step
    /// of a recovery takes its own pooled connection, so a failover partway
    /// could leave the recovered rows on the old primary while this lands on
    /// the promoted one: then it refuses, and recovery runs again there.
    pub async fn record_own_log_verified(
        &self,
        evidence: super::node_identity::LineageEvidence,
    ) -> Result<()> {
        let mut connection = self.acquire().await?;
        let updated = sqlx::query(&format!(
            "UPDATE qbit_prism_node_lineage SET verified_system_identifier=$1,verified_timeline=$2,\
             verified_at=clock_timestamp() WHERE singleton AND ({})=($1::bigint,$2::int)",
            super::node_identity::LINEAGE_EVIDENCE_SQL.trim_start_matches("SELECT ")
        ))
        .bind(evidence.system_identifier)
        .bind(evidence.timeline)
        .execute(&mut *connection)
        .await?
        .rows_affected();
        if updated == 1 {
            return Ok(());
        }
        let lineage: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM qbit_prism_node_lineage WHERE singleton)",
        )
        .fetch_one(&mut *connection)
        .await?;
        ensure!(
            lineage,
            "this database has no qbit_prism_node_lineage row; run node-identity set first"
        );
        bail!(
            "this database's system identifier or WAL timeline changed during own-log recovery \
             (a failover or restore); the verification is not recorded, and recovery runs again"
        )
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

/// Why a peer block was not applied.
enum BlockRefused {
    /// A row of it is held here under the same identity with other content:
    /// it is skipped, recorded and alerted.
    Conflict(String),
    /// Anything else; the block is applied again on the next pass.
    Failed(anyhow::Error),
}

/// A row held here under the same identity with other content.
#[derive(Debug)]
struct HeldOtherwise(&'static str);

impl std::fmt::Display for HeldOtherwise {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for HeldOtherwise {}

impl BlockRefused {
    /// A unique or exclusion violation, or a held row compared unequal, is a
    /// conflict: the same key, other content. Every other error is not.
    fn classify(error: anyhow::Error) -> Self {
        if error.downcast_ref::<HeldOtherwise>().is_some() {
            return Self::Conflict(format!("{error:#}"));
        }
        let conflict = error
            .downcast_ref::<sqlx::Error>()
            .and_then(|error| error.as_database_error())
            .and_then(|error| error.code())
            .is_some_and(|code| matches!(code.as_ref(), "23505" | "23P01"));
        if conflict {
            Self::Conflict(format!("{error:#}"))
        } else {
            Self::Failed(error)
        }
    }
}

/// How many partitions above what it can already hold a node accepts a
/// pulled `share_seq`: at 2^24 rows a partition, days of the peer's appends
/// even at peak, and few enough that attaching them stays within one
/// `ensure`. Only a jump in the peer's own sequence can pass it, never a
/// backlog however long: the pull reads the peer's rows in order from its
/// cursor, a batch at a time, each node's rows are dense in its own
/// sequence, and this node raises its sequence above every batch it takes,
/// so the next batch starts within reach again.
const PULL_HEADROOM_PARTITIONS: i64 = 64;

/// The highest `share_seq` among `rows` at or below `ceiling`.
fn acceptable_highest(rows: &Value, ceiling: i64) -> Result<Option<i64>> {
    let mut highest = None;
    for row in rows.as_array().map(Vec::as_slice).unwrap_or_default() {
        let share_seq = row
            .get("share_seq")
            .and_then(Value::as_i64)
            .context("a pulled share has no share_seq")?;
        if share_seq <= ceiling {
            highest = highest.max(Some(share_seq));
        }
    }
    Ok(highest)
}

/// Run [`crate::rollups::LATE_PEER_SHARES`] for the `inserted` rows of
/// `batch`: the shares folded, and whether a progress row was there.
async fn fold_late_peer_shares(
    tx: &mut Transaction<'_, Postgres>,
    batch: &ShareBatch,
    inserted: &[String],
) -> Result<(i64, bool)> {
    Ok(sqlx::query_as(crate::rollups::LATE_PEER_SHARES)
        .bind(&batch.rows)
        .bind(inserted)
        .fetch_one(&mut **tx)
        .await?)
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

    /// The pull's lists and the table inventory's local columns agree
    /// without a database: every column a pull skips is local there, every
    /// local column is skipped (but a prepared job's `expires_at`, carried as
    /// the peer held it, D-11), and no fact a pull names is local.
    #[test]
    fn the_pull_and_the_table_inventory_agree_on_every_copied_column() {
        for table in COPIED_TABLES {
            let local = super::super::table_inventory::local_columns(table);
            match carried(table) {
                Carried::AllBut(skipped) => {
                    for column in skipped {
                        assert!(
                            local.contains(column),
                            "{table}.{column} is skipped but not local"
                        );
                    }
                    for column in &local {
                        assert!(
                            skipped.contains(column)
                                || (*table == "qbit_prism_jobs" && *column == "expires_at"),
                            "{table}.{column} is local but carried"
                        );
                    }
                }
                Carried::Only(facts) => {
                    for fact in facts {
                        assert!(!local.contains(fact), "{table}.{fact} is local but carried");
                    }
                }
            }
        }
    }

    #[test]
    fn derived_columns_are_never_carried_and_landing_facts_are_listed() {
        for table in COPIED_TABLES {
            if let Carried::AllBut(local) = carried(table) {
                for column in local {
                    assert!(
                        ["maturity_state", "canonical_audit_bytes"].contains(column),
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

    fn job(seq: i64, template: &str, balances: &str) -> Value {
        json!({"job_id": format!("prepared:{seq}"), "sync_seq": seq,
            "template_sha256": template, "window_prior_balances_sha256": balances})
    }

    fn blob(key: &str, column: &str, digest: &str, len: usize) -> Value {
        json!({key: digest, column: "x".repeat(len)})
    }

    fn keys(documents: &[&Value], key: &str) -> Vec<String> {
        documents
            .iter()
            .map(|document| document[key].as_str().unwrap().to_owned())
            .collect()
    }

    /// #785: chunks keep to the row and byte bounds, in order, a job whose
    /// blobs alone pass the byte bound goes alone, and each chunk carries
    /// every blob its jobs name, a shared one in each, and no other. An
    /// empty batch has none, and its JSON length is counted exactly.
    #[test]
    fn prepared_chunks_keep_their_bounds_and_carry_every_blob_their_jobs_name() {
        let total = 2 * PREPARED_CHUNK_ROWS + PREPARED_CHUNK_ROWS / 2;
        let rows = PreparedBatch {
            jobs: Value::Array((1..=total as i64).map(|seq| job(seq, "t", "b")).collect()),
            templates: json!([blob("template_sha256", "template_bytes", "t", 8)]),
            balances: json!([blob("prior_balances_digest", "balances", "b", 8)]),
            count: total,
            highest: Some(total as i64),
        };
        let chunks = rows.chunks();
        let rows_per_chunk = PREPARED_CHUNK_ROWS as i64;
        assert_eq!(
            chunks
                .iter()
                .map(|chunk| (chunk.jobs.len(), chunk.highest))
                .collect::<Vec<_>>(),
            [
                (PREPARED_CHUNK_ROWS, Some(rows_per_chunk)),
                (PREPARED_CHUNK_ROWS, Some(2 * rows_per_chunk)),
                (PREPARED_CHUNK_ROWS / 2, Some(total as i64)),
            ]
        );
        assert!(chunks
            .iter()
            .all(|chunk| chunk.templates.len() == 1 && chunk.balances.len() == 1));

        // Templates of two fifths of the byte bound: two jobs a chunk, then
        // one whose template alone passes it, alone. The unnamed blob is in
        // none.
        let fifth = PREPARED_CHUNK_BYTES / 5;
        let mut templates: Vec<Value> = (1..=5)
            .map(|n| {
                blob(
                    "template_sha256",
                    "template_bytes",
                    &format!("t{n}"),
                    2 * fifth,
                )
            })
            .collect();
        templates.push(blob("template_sha256", "template_bytes", "t6", 6 * fifth));
        templates.push(blob("template_sha256", "template_bytes", "unnamed", 8));
        let bytes = PreparedBatch {
            jobs: Value::Array((1..=6).map(|n| job(n, &format!("t{n}"), "b")).collect()),
            templates: Value::Array(templates),
            balances: json!([blob("prior_balances_digest", "balances", "b", 8)]),
            count: 6,
            highest: Some(6),
        };
        let chunks = bytes.chunks();
        assert_eq!(
            chunks
                .iter()
                .map(|chunk| (chunk.jobs.len(), chunk.highest))
                .collect::<Vec<_>>(),
            [(2, Some(2)), (2, Some(4)), (1, Some(5)), (1, Some(6))]
        );
        assert_eq!(
            chunks
                .iter()
                .map(|chunk| keys(&chunk.templates, "template_sha256"))
                .collect::<Vec<_>>(),
            [vec!["t1", "t2"], vec!["t3", "t4"], vec!["t5"], vec!["t6"]]
        );
        assert!(chunks
            .iter()
            .all(|chunk| keys(&chunk.balances, "prior_balances_digest") == ["b"]));
        for chunk in &chunks {
            let size: usize = chunk
                .jobs
                .iter()
                .chain(chunk.templates.iter().copied())
                .chain(chunk.balances.iter().copied())
                .map(json_len)
                .sum();
            assert!(
                chunk.jobs.len() == 1 || size <= PREPARED_CHUNK_BYTES,
                "{size}"
            );
        }
        assert!(PreparedBatch::default().chunks().is_empty());
        let document = json!({"a": [1, "\u{e9}\"", null], "b": {"c": 2.5}});
        assert_eq!(json_len(&document), document.to_string().len());
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
