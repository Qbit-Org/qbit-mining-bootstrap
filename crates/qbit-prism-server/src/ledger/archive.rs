//! Share ledger retention: seal, archive, verify, detach, drop and restore one
//! partition of `qbit_share_ledger` (#144, decision D6 of the design record
//! `docs/prism-share-ledger-partitioning.md`).
//!
//! Retention never deletes a share. A partition leaves the online ledger only
//! after every audit that depends on it holds its own canonical bytes, after
//! its rows have been written to an archive outside PostgreSQL, and after that
//! archive has been re-read and compared against the live rows. That comparison
//! counts only once the share sequence has passed the partition, so nothing
//! more can land in it, and because ledger rows are immutable the detach and
//! the drop each count the rows against the archive again before they act.
//! Only then is the partition detached, and only a detached, verified
//! partition is dropped. The archive is the copy of record from that point,
//! and `restore` builds the partition table back from it.
//!
//! Every step is idempotent and every step is resumable, because each is a
//! piece of DDL or a bounded write followed by one catalog transaction, and
//! PostgreSQL (`pg_inherits`, `to_regclass`) is the authority the catalog is
//! reconciled against rather than the other way round. A run that dies between
//! the DDL and the catalog update is repaired by running the same command
//! again.
//!
//! Rows are never materialized: every read walks the partition in `share_seq`
//! order in [`PAGE_ROWS`] pages, so one page and not one partition is the peak
//! memory of an archive, a verify or a restore, and each page is one statement
//! inside the connection's `statement_timeout`.
use super::*;
use chrono::SecondsFormat;
use sqlx::PgConnection;
use std::io::{BufRead, Read, Write};
use std::path::{Path, PathBuf};

/// The partitioned parent. Every name this module interpolates into SQL is
/// either this constant or a partition name proved by [`check_partition_name`].
const PARENT: &str = "qbit_share_ledger";

/// The only archive format this binary writes, and the only one it reads.
/// EP-COMPAT: the format is persisted outside the database and outlives the
/// binary that wrote it, so the version travels in the manifest and an
/// unknown value is refused by name instead of being parsed optimistically.
pub const ARCHIVE_SCHEMA_V1: &str = "qbit.prism.share-archive.v1";

/// Rows per page of every ordered walk here. The same page the window reader
/// and the durable-range proof use: about 2.5 MB of decoded rows, and one
/// statement well inside the 15 s `statement_timeout` the operator pool sets.
const PAGE_ROWS: i64 = 4096;

/// Rows per multi-row `INSERT` during a restore. Seventeen columns per row, so
/// this stays far below PostgreSQL's 65,535 bind parameters per statement.
/// Binding each value keeps the restore free of any text quoting of its own:
/// the bytes that come back out of the restored table have to re-stream to the
/// manifest's digest, and a quoting bug would be a silent corruption.
const RESTORE_BATCH_ROWS: usize = 512;

/// The whole ledger row, in the archive's fixed key order, with the exact
/// forms the design record specifies: timestamps as microseconds since the
/// epoch (the full resolution of `timestamptz`, so a restore reproduces the
/// identical instant), difficulties as decimal strings (`numeric(78, 0)` does
/// not fit any Rust integer), the program as hex, and `reject_reason` and
/// `credit_policy` as JSON `null` when the column is NULL rather than absent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveRow {
    pub share_seq: i64,
    pub share_id: String,
    pub miner_id: String,
    pub payout_order_key: String,
    pub p2mr_program_hex: String,
    pub share_difficulty: String,
    pub network_difficulty: String,
    pub template_height: i64,
    pub job_id: String,
    pub job_issued_at_us: i64,
    pub accepted_at_us: i64,
    pub ntime: i64,
    pub accepted: bool,
    pub reject_reason: Option<String>,
    pub credit_policy: Option<String>,
    pub writer_id: String,
    pub writer_epoch: i64,
}

/// The projection every stream of this module reads, in the archive's key
/// order. `floor(extract(epoch FROM ts) * 1e6)` is exact: `extract(epoch)`
/// returns `numeric` with the column's microsecond resolution, so no binary
/// floating point is involved and the value round-trips through
/// [`RESTORE_TIMESTAMP`].
const SELECT_ARCHIVE_ROW: &str = "SELECT share_seq,share_id,miner_id,payout_order_key,\
     encode(p2mr_program,'hex') AS p2mr_program_hex,\
     share_difficulty::text AS share_difficulty,\
     network_difficulty::text AS network_difficulty,\
     template_height,job_id,\
     floor(extract(epoch FROM job_issued_at)*1000000)::bigint AS job_issued_at_us,\
     floor(extract(epoch FROM accepted_at)*1000000)::bigint AS accepted_at_us,\
     ntime,accepted,reject_reason,credit_policy,writer_id,writer_epoch FROM ";

/// Microseconds back to `timestamptz` without passing through `double
/// precision`: an interval holds microseconds as a 64-bit integer, so the
/// addition is exact integer arithmetic and reproduces the archived instant.
const RESTORE_TIMESTAMP: &str = "(timestamptz 'epoch' + ($X::text || ' microseconds')::interval)";

impl ArchiveRow {
    fn from_row(row: &PgRow) -> Result<Self> {
        Ok(Self {
            share_seq: row.try_get("share_seq")?,
            share_id: row.try_get("share_id")?,
            miner_id: row.try_get("miner_id")?,
            payout_order_key: row.try_get("payout_order_key")?,
            p2mr_program_hex: row.try_get("p2mr_program_hex")?,
            share_difficulty: row.try_get("share_difficulty")?,
            network_difficulty: row.try_get("network_difficulty")?,
            template_height: row.try_get("template_height")?,
            job_id: row.try_get("job_id")?,
            job_issued_at_us: row.try_get("job_issued_at_us")?,
            accepted_at_us: row.try_get("accepted_at_us")?,
            ntime: row.try_get("ntime")?,
            accepted: row.try_get("accepted")?,
            reject_reason: row.try_get("reject_reason")?,
            credit_policy: row.try_get("credit_policy")?,
            writer_id: row.try_get("writer_id")?,
            writer_epoch: row.try_get("writer_epoch")?,
        })
    }

    /// One line of `rows.ndjson.gz`, terminated. The digest is taken over
    /// exactly these bytes, so nothing may reorder or reformat them.
    pub fn line(&self) -> Result<Vec<u8>> {
        let mut bytes = serde_json::to_vec(self)?;
        bytes.push(b'\n');
        Ok(bytes)
    }

    /// Parse one line back. The line must be exactly what [`Self::line`]
    /// writes for the parsed row: an archive whose lines re-encode to
    /// something else is not this format, whatever its digest says.
    pub fn parse_line(line: &[u8]) -> Result<Self> {
        let trimmed = line.strip_suffix(b"\n").unwrap_or(line);
        let row: Self = serde_json::from_slice(trimmed)
            .context("share archive row is not a v1 ndjson row object")?;
        ensure!(
            row.line()? == [trimmed, b"\n"].concat(),
            "share archive row {} is not in the canonical v1 key order",
            row.share_seq
        );
        Ok(row)
    }
}

/// The record beside the rows. Serialized by serde in declaration order with
/// no trailing newline, which is what makes `manifest.json` canonical and its
/// SHA-256 a stable name for the archive.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveManifest {
    pub schema: String,
    pub partition_name: String,
    /// NULL is MINVALUE: the release table attached as the first partition.
    pub lower_seq: Option<i64>,
    pub upper_seq: i64,
    pub row_count: i64,
    pub first_share_seq: Option<i64>,
    pub last_share_seq: Option<i64>,
    pub first_accepted_at_us: Option<i64>,
    pub last_accepted_at_us: Option<i64>,
    /// SHA-256 of the uncompressed ndjson stream, so a verifier can stream the
    /// file instead of materializing it.
    pub rows_sha256: String,
    /// SHA-256 of `rows.ndjson.gz` itself.
    pub rows_gz_sha256: String,
    pub rows_bytes: i64,
    pub rows_gz_bytes: i64,
    /// The archived partition with the next-lower `upper_seq`, or null for the
    /// first one. A `previous_upper_seq` that is not this partition's
    /// `lower_seq` means a partition is missing from the chain.
    pub previous_manifest_sha256: Option<String>,
    pub previous_upper_seq: Option<i64>,
    pub schema_versions: Vec<i32>,
    pub created_at: String,
    pub created_by: String,
}

impl ArchiveManifest {
    /// The canonical bytes and their digest, which is what the catalog and the
    /// next manifest record.
    pub fn canonical(&self) -> Result<(Vec<u8>, String)> {
        let bytes = serde_json::to_vec(self)?;
        let digest = hex::encode(Sha256::digest(&bytes));
        Ok((bytes, digest))
    }
}

/// Is `lower_seq` exactly where the previously archived partition ended?
///
/// A first partition (`lower_seq` NULL, MINVALUE) must have no predecessor,
/// and any other must start exactly at its predecessor's `upper_seq`. Anything
/// else is a gap: a partition between the two is missing from the chain, so
/// the chain no longer proves the archived history is contiguous.
pub fn chain_is_adjacent(previous_upper_seq: Option<i64>, lower_seq: Option<i64>) -> bool {
    match (previous_upper_seq, lower_seq) {
        (None, None) => true,
        (Some(previous), Some(lower)) => previous == lower,
        _ => false,
    }
}

/// Refuse any name that is not a share ledger partition. Partition names reach
/// SQL by interpolation (an identifier cannot be a bind parameter), so this is
/// the boundary that keeps that safe, and it doubles as a format check on a
/// manifest read from disk.
fn check_partition_name(name: &str) -> Result<()> {
    let grid = name.strip_prefix("qbit_share_ledger_p");
    ensure!(
        name.len() <= 63
            && grid.is_some_and(|cell| !cell.is_empty() && cell.bytes().all(|b| b.is_ascii_digit())),
        "{name} is not a share ledger partition name; partitions are named qbit_share_ledger_p<cell>, as listed by share-archive plan"
    );
    Ok(())
}

/// Refuse a network difficulty that is not a positive `numeric(78, 0)`. It is
/// multiplied server-side, so it never has to fit a Rust integer.
fn check_network_difficulty(difficulty: &str) -> Result<()> {
    ensure!(
        !difficulty.is_empty()
            && difficulty.len() <= 78
            && difficulty.bytes().all(|b| b.is_ascii_digit())
            && difficulty.bytes().any(|b| b != b'0'),
        "--network-difficulty must be a positive whole number of at most 78 digits, as printed by qbit-prism-server header-difficulty"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Streaming a partition: one digest, optionally one gzip file
// ---------------------------------------------------------------------------

/// A writer that digests and counts everything it passes through, so the file
/// digest costs one pass and never a re-read.
struct HashingWriter<W> {
    inner: W,
    hasher: Sha256,
    bytes: u64,
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buffer)?;
        self.hasher.update(&buffer[..written]);
        self.bytes += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

type GzSink = flate2::write::GzEncoder<HashingWriter<std::io::BufWriter<std::fs::File>>>;

/// What a stream of rows amounts to: the counts and endpoints the manifest
/// records, and the digest of the uncompressed ndjson bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
struct RowsSummary {
    row_count: i64,
    first_share_seq: Option<i64>,
    last_share_seq: Option<i64>,
    first_accepted_at_us: Option<i64>,
    last_accepted_at_us: Option<i64>,
    rows_sha256: String,
    rows_bytes: i64,
}

/// One pass over a partition's rows in `share_seq` order. The same type serves
/// the archive writer (with a gzip sink) and the verifier's re-read of the
/// live rows (without one), so the two can never disagree on how a row is
/// encoded or digested.
struct RowStream {
    hasher: Sha256,
    bytes: u64,
    row_count: i64,
    first_share_seq: Option<i64>,
    last_share_seq: Option<i64>,
    first_accepted_at_us: Option<i64>,
    last_accepted_at_us: Option<i64>,
    lower_seq: Option<i64>,
    upper_seq: i64,
    partition_name: String,
    sink: Option<GzSink>,
}

impl RowStream {
    fn new(
        partition_name: &str,
        lower_seq: Option<i64>,
        upper_seq: i64,
        sink: Option<GzSink>,
    ) -> Self {
        Self {
            hasher: Sha256::new(),
            bytes: 0,
            row_count: 0,
            first_share_seq: None,
            last_share_seq: None,
            first_accepted_at_us: None,
            last_accepted_at_us: None,
            lower_seq,
            upper_seq,
            partition_name: partition_name.to_owned(),
            sink,
        }
    }

    /// Append one row. Ascending order and the recorded bounds are checked
    /// here rather than assumed from the query: the bounds are what a restore
    /// recreates the `CHECK` from, and an out-of-bound row would make the
    /// restored table unattachable long after the archive was written.
    fn push(&mut self, row: &ArchiveRow) -> Result<()> {
        if let Some(last) = self.last_share_seq {
            ensure!(
                row.share_seq > last,
                "partition {} produced share_seq {} after {last}; the stream must ascend",
                self.partition_name,
                row.share_seq
            );
        }
        ensure!(
            row.share_seq < self.upper_seq
                && self.lower_seq.is_none_or(|lower| row.share_seq >= lower),
            "partition {} holds share_seq {} outside its recorded bounds [{}, {}); the catalog row and the partition disagree, so re-read the bounds with share-archive plan before archiving it",
            self.partition_name,
            row.share_seq,
            self.lower_seq.map(|l| l.to_string()).unwrap_or_else(|| "MINVALUE".into()),
            self.upper_seq
        );
        let line = row.line()?;
        self.hasher.update(&line);
        self.bytes += line.len() as u64;
        if let Some(sink) = self.sink.as_mut() {
            sink.write_all(&line)?;
        }
        self.row_count += 1;
        if self.first_share_seq.is_none() {
            self.first_share_seq = Some(row.share_seq);
            self.first_accepted_at_us = Some(row.accepted_at_us);
        }
        self.last_share_seq = Some(row.share_seq);
        self.last_accepted_at_us = Some(row.accepted_at_us);
        Ok(())
    }

    /// Close the sink and return what was streamed, plus the file's own digest
    /// and size when there was a file.
    fn finish(self) -> Result<(RowsSummary, Option<(String, i64)>)> {
        let summary = RowsSummary {
            row_count: self.row_count,
            first_share_seq: self.first_share_seq,
            last_share_seq: self.last_share_seq,
            first_accepted_at_us: self.first_accepted_at_us,
            last_accepted_at_us: self.last_accepted_at_us,
            rows_sha256: hex::encode(self.hasher.finalize()),
            rows_bytes: i64::try_from(self.bytes)?,
        };
        let file = match self.sink {
            None => None,
            Some(sink) => {
                let writer = sink.finish()?;
                let digest = hex::encode(writer.hasher.clone().finalize());
                let bytes = i64::try_from(writer.bytes)?;
                let mut inner = writer.inner;
                inner.flush()?;
                // The archive is the copy of record from the drop onwards, so
                // it reaches the platter before the catalog claims it exists.
                inner.get_ref().sync_all()?;
                Some((digest, bytes))
            }
        };
        Ok((summary, file))
    }
}

/// Walk one partition's rows in `share_seq` order, one page per statement.
///
/// Decoding and digesting a page is proportional to the page and runs on a
/// blocking thread; the stream moves in and out of that job, so no page is
/// ever copied and the runtime threads never carry one.
async fn stream_partition(
    connection: &mut PgConnection,
    table: &str,
    mut stream: RowStream,
) -> Result<RowStream> {
    let sql = format!("{SELECT_ARCHIVE_ROW}{table} WHERE share_seq>$1 ORDER BY share_seq LIMIT $2");
    let mut cursor = i64::MIN;
    loop {
        let rows = sqlx::query(&sql)
            .bind(cursor)
            .bind(PAGE_ROWS)
            .fetch_all(&mut *connection)
            .await
            .with_context(|| format!("reading a page of {table} from share_seq above {cursor}"))?;
        if rows.is_empty() {
            break;
        }
        let (moved, next) = tokio::task::spawn_blocking(move || -> Result<(RowStream, i64)> {
            let mut last = cursor;
            for row in &rows {
                let row = ArchiveRow::from_row(row)?;
                last = row.share_seq;
                stream.push(&row)?;
            }
            Ok((stream, last))
        })
        .await??;
        stream = moved;
        cursor = next;
    }
    Ok(stream)
}

// ---------------------------------------------------------------------------
// The archive on disk
// ---------------------------------------------------------------------------

fn partition_dir(root: &Path, partition_name: &str) -> PathBuf {
    root.join(PARENT).join(partition_name)
}

/// A temp file that removes itself unless it is renamed into place.
/// EP-ERRORS: a failed archive leaves no half-written file behind for the next
/// run, or for a verifier, to mistake for an archive.
struct TempFile {
    path: PathBuf,
    live: bool,
}

impl TempFile {
    fn create(directory: &Path, name: &str) -> Result<(Self, std::fs::File)> {
        let path = directory.join(format!("{name}.tmp-{}", Uuid::new_v4().simple()));
        let file =
            std::fs::File::create(&path).with_context(|| format!("creating {}", path.display()))?;
        Ok((Self { path, live: true }, file))
    }

    fn promote(mut self, destination: &Path) -> Result<()> {
        std::fs::rename(&self.path, destination).with_context(|| {
            format!(
                "renaming {} onto {}",
                self.path.display(),
                destination.display()
            )
        })?;
        self.live = false;
        Ok(())
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        if self.live {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Read `manifest.json` and return it with its digest and its exact bytes.
///
/// EP-COMPAT: the `schema` is read before anything else is parsed, so a
/// future version is refused by name instead of being half-understood, and the
/// parsed manifest must re-serialize to the bytes on disk, which is what makes
/// the recorded digest a digest of a canonical record rather than of one
/// particular formatting of it.
fn read_manifest(path: &Path) -> Result<(ArchiveManifest, String, Vec<u8>)> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("reading the share archive manifest {}", path.display()))?;
    let (manifest, digest) = parse_manifest(&bytes, &path.display().to_string())?;
    Ok((manifest, digest, bytes))
}

/// The manifest and its digest, or the reason the bytes are not one.
fn parse_manifest(bytes: &[u8], named: &str) -> Result<(ArchiveManifest, String)> {
    let probe: Value =
        serde_json::from_slice(bytes).with_context(|| format!("{named} is not JSON"))?;
    let schema = probe
        .get("schema")
        .and_then(Value::as_str)
        .unwrap_or("none");
    ensure!(
        schema == ARCHIVE_SCHEMA_V1,
        "{named} declares archive schema {schema}; this binary reads {ARCHIVE_SCHEMA_V1} only. Restore it with the release that wrote it"
    );
    let manifest: ArchiveManifest = serde_json::from_slice(bytes)
        .with_context(|| format!("{named} is not a v1 share archive manifest"))?;
    let (canonical, digest) = manifest.canonical()?;
    ensure!(
        canonical == bytes,
        "{named} is not the canonical encoding of its own manifest; it was rewritten or reformatted after it was archived"
    );
    check_partition_name(&manifest.partition_name)?;
    ensure!(
        manifest.row_count >= 0 && manifest.rows_bytes >= 0 && manifest.rows_gz_bytes >= 0,
        "{named} records a negative count or size"
    );
    Ok((manifest, digest))
}

/// SHA-256 and size of a file, streamed so nothing is held in memory.
fn digest_file(path: &Path) -> Result<(String, i64)> {
    let mut file = std::fs::File::open(path)
        .with_context(|| format!("reading the share archive rows {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 64 * 1024];
    let mut bytes = 0i64;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        bytes += i64::try_from(read)?;
    }
    Ok((hex::encode(hasher.finalize()), bytes))
}

/// Stream `rows.ndjson.gz`, handing each row to `sink`, and check both digests
/// and every count the manifest records against what the file actually holds.
/// A mismatch names the field that differed.
fn read_rows_file(
    path: &Path,
    manifest: &ArchiveManifest,
    mut sink: impl FnMut(ArchiveRow) -> Result<()>,
) -> Result<()> {
    // The file's own digest is taken first, in its own pass. An archive whose
    // bytes were altered has to be reported as an altered archive, not as a
    // decompression failure, and gzip fails its internal checks long before
    // the stream digest would be reached.
    let (rows_gz_sha256, rows_gz_bytes) = digest_file(path)?;
    ensure!(
        rows_gz_sha256 == manifest.rows_gz_sha256,
        "{}: rows_gz_sha256 is {rows_gz_sha256} but the manifest records {}; the archive file was altered",
        path.display(),
        manifest.rows_gz_sha256
    );
    ensure!(
        rows_gz_bytes == manifest.rows_gz_bytes,
        "{}: is {rows_gz_bytes} bytes but the manifest records {}",
        path.display(),
        manifest.rows_gz_bytes
    );
    let file = std::fs::File::open(path)
        .with_context(|| format!("reading the share archive rows {}", path.display()))?;
    let mut reader = std::io::BufReader::new(flate2::read::MultiGzDecoder::new(
        std::io::BufReader::new(file),
    ));
    let mut stream = RowStream::new(
        &manifest.partition_name,
        manifest.lower_seq,
        manifest.upper_seq,
        None,
    );
    let mut line = Vec::new();
    loop {
        line.clear();
        let read = reader
            .read_until(b'\n', &mut line)
            .with_context(|| format!("decompressing {}", path.display()))?;
        if read == 0 {
            break;
        }
        ensure!(
            line.last() == Some(&b'\n'),
            "{} ends without a line terminator; the archive is truncated",
            path.display()
        );
        let row = ArchiveRow::parse_line(&line)?;
        stream.push(&row)?;
        sink(row)?;
    }
    let (summary, _) = stream.finish()?;
    let named = path.display();
    ensure!(
        summary.rows_sha256 == manifest.rows_sha256,
        "{named}: rows_sha256 is {} but the manifest records {}; the archived rows were altered",
        summary.rows_sha256,
        manifest.rows_sha256
    );
    ensure!(
        summary.row_count == manifest.row_count,
        "{named}: holds {} rows but the manifest records {}",
        summary.row_count,
        manifest.row_count
    );
    ensure!(
        summary.rows_bytes == manifest.rows_bytes,
        "{named}: decompresses to {} bytes but the manifest records {}",
        summary.rows_bytes,
        manifest.rows_bytes
    );
    ensure!(
        summary.first_share_seq == manifest.first_share_seq
            && summary.last_share_seq == manifest.last_share_seq
            && summary.first_accepted_at_us == manifest.first_accepted_at_us
            && summary.last_accepted_at_us == manifest.last_accepted_at_us,
        "{named}: endpoints {:?}..{:?} differ from the manifest's {:?}..{:?}",
        summary.first_share_seq,
        summary.last_share_seq,
        manifest.first_share_seq,
        manifest.last_share_seq
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// The catalog, and what PostgreSQL says about it
// ---------------------------------------------------------------------------

/// One `qbit_prism_share_partitions` row.
#[derive(Clone, Debug, Serialize)]
pub struct PartitionRecord {
    pub partition_name: String,
    pub lower_seq: Option<i64>,
    pub upper_seq: i64,
    pub state: String,
    pub sealed_at: Option<DateTime<Utc>>,
    pub archive_uri: Option<String>,
    pub archive_manifest_sha256: Option<String>,
    pub archive_rows: Option<i64>,
    pub archive_rows_sha256: Option<String>,
    pub archived_at: Option<DateTime<Utc>>,
    pub archive_verified_at: Option<DateTime<Utc>>,
    pub detached_at: Option<DateTime<Utc>>,
    pub dropped_at: Option<DateTime<Utc>>,
}

const SELECT_PARTITION: &str = "SELECT partition_name,lower_seq,upper_seq,state,sealed_at,\
     archive_uri,archive_manifest_sha256,archive_rows,archive_rows_sha256,archived_at,\
     archive_verified_at,detached_at,dropped_at FROM qbit_prism_share_partitions";

impl PartitionRecord {
    // A six-plus-column `query_as` tuple is exactly the row type clippy's
    // `type_complexity` refuses, so every row here is decoded by name.
    fn from_row(row: &PgRow) -> Result<Self> {
        Ok(Self {
            partition_name: row.try_get("partition_name")?,
            lower_seq: row.try_get("lower_seq")?,
            upper_seq: row.try_get("upper_seq")?,
            state: row.try_get("state")?,
            sealed_at: row.try_get("sealed_at")?,
            archive_uri: row.try_get("archive_uri")?,
            archive_manifest_sha256: row.try_get("archive_manifest_sha256")?,
            archive_rows: row.try_get("archive_rows")?,
            archive_rows_sha256: row.try_get("archive_rows_sha256")?,
            archived_at: row.try_get("archived_at")?,
            archive_verified_at: row.try_get("archive_verified_at")?,
            detached_at: row.try_get("detached_at")?,
            dropped_at: row.try_get("dropped_at")?,
        })
    }

    fn bounds(&self) -> String {
        format!(
            "[{}, {})",
            self.lower_seq
                .map(|lower| lower.to_string())
                .unwrap_or_else(|| "MINVALUE".into()),
            self.upper_seq
        )
    }
}

async fn catalog_row(
    connection: &mut PgConnection,
    partition_name: &str,
) -> Result<PartitionRecord> {
    let row = sqlx::query(&format!("{SELECT_PARTITION} WHERE partition_name=$1"))
        .bind(partition_name)
        .fetch_optional(&mut *connection)
        .await?
        .with_context(|| {
            format!("{partition_name} has no qbit_prism_share_partitions row; only partitions the ledger created are managed here, and share-archive plan lists them")
        })?;
    PartitionRecord::from_row(&row)
}

async fn catalog_rows(connection: &mut PgConnection) -> Result<Vec<PartitionRecord>> {
    sqlx::query(&format!("{SELECT_PARTITION} ORDER BY upper_seq"))
        .fetch_all(&mut *connection)
        .await?
        .iter()
        .map(PartitionRecord::from_row)
        .collect()
}

/// What PostgreSQL holds for one partition name: whether it is a child of the
/// parent, whether a concurrent detach was interrupted halfway, and whether
/// the relation exists at all. `pg_inherits` is the authority; the catalog
/// records what happened to a partition afterwards and is reconciled to it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct AttachmentState {
    pub relation_present: bool,
    pub attached: bool,
    pub detach_pending: bool,
}

async fn attachment(
    connection: &mut PgConnection,
    partition_name: &str,
) -> Result<AttachmentState> {
    let row = sqlx::query(
        "SELECT to_regclass($1) IS NOT NULL AS present,\
         (SELECT i.inhdetachpending FROM pg_inherits i WHERE i.inhrelid=to_regclass($1) AND i.inhparent=to_regclass($2)) AS detach_pending",
    )
    .bind(partition_name)
    .bind(PARENT)
    .fetch_one(&mut *connection)
    .await?;
    let detach_pending: Option<bool> = row.try_get("detach_pending")?;
    Ok(AttachmentState {
        relation_present: row.try_get("present")?,
        attached: detach_pending.is_some(),
        detach_pending: detach_pending == Some(true),
    })
}

async fn attached_partitions(connection: &mut PgConnection) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(
        "SELECT c.relname::text FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid WHERE i.inhparent=to_regclass($1) ORDER BY 1",
    )
    .bind(PARENT)
    .fetch_all(&mut *connection)
    .await?)
}

/// The archived partition with the next-lower `upper_seq`: what a new
/// manifest links to, and what a verified one has to link to.
async fn archived_predecessor(
    connection: &mut PgConnection,
    upper_seq: i64,
) -> Result<Option<PartitionRecord>> {
    sqlx::query(&format!(
        "{SELECT_PARTITION} WHERE archive_manifest_sha256 IS NOT NULL AND upper_seq<$1 ORDER BY upper_seq DESC LIMIT 1"
    ))
    .bind(upper_seq)
    .fetch_optional(&mut *connection)
    .await?
    .as_ref()
    .map(PartitionRecord::from_row)
    .transpose()
}

/// Every archived partition above `upper_seq`, in order: the ones whose chain
/// passes through the manifest recorded at that position.
async fn archived_successors(
    connection: &mut PgConnection,
    upper_seq: i64,
) -> Result<Vec<PartitionRecord>> {
    sqlx::query(&format!(
        "{SELECT_PARTITION} WHERE archive_manifest_sha256 IS NOT NULL AND upper_seq>$1 ORDER BY upper_seq"
    ))
    .bind(upper_seq)
    .fetch_all(&mut *connection)
    .await?
    .iter()
    .map(PartitionRecord::from_row)
    .collect()
}

/// The next `share_seq` the sequence will hand out. A partition whose
/// `upper_seq` is above it can still receive appends, so no archive of it can
/// be complete, and no comparison with its live rows proves anything about the
/// rows still to come.
async fn next_share_seq(connection: &mut PgConnection) -> Result<i64> {
    Ok(sqlx::query_scalar("SELECT qbit_prism_share_next_seq()")
        .fetch_one(&mut *connection)
        .await?)
}

/// Refuse to archive or verify a partition the sequence has not passed.
fn check_sequence_passed(record: &PartitionRecord, next_share_seq: i64, what: &str) -> Result<()> {
    ensure!(
        next_share_seq >= record.upper_seq,
        "refusing to {what} {}: the share sequence stands at {next_share_seq}, below the partition's upper bound {}, so appends can still land in it and no archive of it can be complete. Wait until the sequence has passed {}",
        record.partition_name,
        record.upper_seq,
        record.upper_seq
    );
    Ok(())
}

/// Wait for every append that drew its `share_seq` before the bound was
/// passed. Sequences are nontransactional: an append takes its value inside
/// its transaction (`INSERT ... RETURNING share_seq`) and holds `ORDER_LOCK`
/// from before that insert until the transaction ends, so the sequence can
/// report the bound passed while a row below it is still uncommitted and
/// invisible. Taking the lock, outside any transaction so it lasts for the
/// statement only, returns once every such transaction has committed or
/// rolled back; an append that starts afterwards draws a value at or above
/// what the sequence reported, outside the partition. Only after this is a
/// stream of the live rows complete.
async fn drain_appends(ledger: &Ledger, connection: &mut PgConnection) -> Result<()> {
    super::connect::lock(connection, super::ORDER_LOCK, ledger.metrics.as_deref())
        .await
        .context("waiting for in-flight appends under the ledger's ordering lock")?;
    Ok(())
}

/// Every audit row whose snapshot intersects the partition, split by whether
/// its canonical bytes are stored. Snapshots with `inline_shares` are the
/// bootstrap window's synthetic share, which is not in the ledger and so does
/// not depend on any partition.
async fn intersecting_audits(
    connection: &mut PgConnection,
    record: &PartitionRecord,
) -> Result<(i64, i64)> {
    let row = sqlx::query(
        "SELECT count(*)::bigint AS intersecting,\
         count(*) FILTER (WHERE a.canonical_audit_bytes IS NULL)::bigint AS unsealed \
         FROM qbit_pool_audit_bundles a \
         JOIN qbit_prism_audit_snapshots s ON s.snapshot_sha256=a.share_snapshot_sha256 \
         WHERE s.inline_shares IS NULL AND s.first_share_seq<$1 \
           AND s.last_share_seq>=COALESCE($2,s.first_share_seq)",
    )
    .bind(record.upper_seq)
    .bind(record.lower_seq)
    .fetch_one(&mut *connection)
    .await?;
    Ok((row.try_get("intersecting")?, row.try_get("unsealed")?))
}

/// Unfinished outbox rows and deferred shares that name a `share_id` the
/// partition holds. Both probes go through the leaf's own `UNIQUE (share_id)`
/// index, one descent per referencing row.
async fn pending_references(
    connection: &mut PgConnection,
    record: &PartitionRecord,
) -> Result<(i64, i64)> {
    let row = sqlx::query(&format!(
        "SELECT (SELECT count(*)::bigint FROM qbit_block_candidate_outbox o \
           WHERE o.state IN {states} \
             AND (EXISTS(SELECT 1 FROM {partition_name} p WHERE p.share_id=o.share_id) \
               OR (o.window_first_share_seq IS NOT NULL AND o.window_last_share_seq IS NOT NULL \
                 AND o.window_first_share_seq<$1 \
                 AND o.window_last_share_seq>=COALESCE($2,o.window_first_share_seq)))) AS outbox,\
         (SELECT count(*)::bigint FROM qbit_prism_deferred_shares d \
           WHERE EXISTS(SELECT 1 FROM {partition_name} p WHERE p.share_id=d.share->>'share_id')) AS deferred",
        partition_name = record.partition_name,
        states = crate::ledger::CandidateState::UNFINISHED_SQL,
    ))
    .bind(record.upper_seq)
    .bind(record.lower_seq)
    .fetch_one(&mut *connection)
    .await?;
    Ok((row.try_get("outbox")?, row.try_get("deferred")?))
}

// ---------------------------------------------------------------------------
// plan
// ---------------------------------------------------------------------------

/// The retention rules `plan` evaluates and `detach` requires. Defaults are the
/// design record's: four times the requested window weight, thirty days.
#[derive(Clone, Debug)]
pub struct PlanOptions {
    pub network_difficulty: String,
    pub retention_days: i64,
    pub window_multiple: i64,
    pub check_duplicates: bool,
}

/// One of the five conditions of decision D6, for one partition.
///
/// EP-OBSERVABILITY: `status` separates `clear` from `unknown`. A condition
/// whose input is missing (no rollup watermark row, a catalog row whose
/// relation is gone) is never reported as clear because a query returned
/// nothing; it blocks the detach exactly as a real blocker does.
#[derive(Clone, Debug, Serialize)]
pub struct Condition {
    pub name: &'static str,
    pub status: &'static str,
    pub detail: String,
}

impl Condition {
    fn clear(name: &'static str, detail: impl Into<String>) -> Self {
        Self {
            name,
            status: "clear",
            detail: detail.into(),
        }
    }
    fn blocked(name: &'static str, detail: impl Into<String>) -> Self {
        Self {
            name,
            status: "blocked",
            detail: detail.into(),
        }
    }
    fn unknown(name: &'static str, detail: impl Into<String>) -> Self {
        Self {
            name,
            status: "unknown",
            detail: detail.into(),
        }
    }
    fn not_applicable(name: &'static str, detail: impl Into<String>) -> Self {
        Self {
            name,
            status: "not_applicable",
            detail: detail.into(),
        }
    }
}

const CONDITION_NAMES: [&str; 5] = [
    "payout_window",
    "retention_age",
    "rollup_watermark",
    "audits_sealed",
    "pending_references",
];

/// What `plan` reports for one partition.
#[derive(Clone, Debug, Serialize)]
pub struct PartitionPlan {
    #[serde(flatten)]
    pub record: PartitionRecord,
    #[serde(flatten)]
    pub attachment: AttachmentState,
    pub live_rows: Option<i64>,
    pub newest_accepted_at: Option<DateTime<Utc>>,
    pub conditions: Vec<Condition>,
    pub blockers: Vec<String>,
    pub unknowns: Vec<String>,
    pub eligible: bool,
}

/// What `plan` prints: one object per partition and one summary.
#[derive(Clone, Debug, Serialize)]
pub struct PlanReport {
    pub schema: &'static str,
    pub generated_at: String,
    pub network_difficulty: String,
    pub window_multiple: i64,
    pub retention_days: i64,
    pub window_floor_share_seq: Option<i64>,
    pub next_share_seq: i64,
    pub rollup_last_share_seq: Option<i64>,
    pub attached_count: i64,
    pub attached_upper_seq: Option<i64>,
    /// How far the attached partitions reach above the next `share_seq`.
    pub lead_rows_ahead: Option<i64>,
    /// `None` without `--check-duplicates`: not checked is not the same as
    /// none found, and the query is a full pass over the attached leaves.
    pub duplicate_share_ids: Option<Vec<String>>,
    pub unknowns: Vec<String>,
    pub eligible: Vec<String>,
    pub partitions: Vec<PartitionPlan>,
}

impl PartitionPlan {
    fn refusal(&self, what: &str) -> String {
        let mut reasons = self.blockers.clone();
        reasons.extend(self.unknowns.iter().cloned());
        format!(
            "refusing to {what} {}: {}. Clear each with share-archive plan and run it again; there is no override",
            self.record.partition_name,
            reasons.join("; ")
        )
    }
}

/// Evaluate every retention condition without changing anything.
pub async fn plan(ledger: &Ledger, options: &PlanOptions) -> Result<PlanReport> {
    check_network_difficulty(&options.network_difficulty)?;
    ensure!(
        (0..=36_500).contains(&options.retention_days),
        "--retention-days must be between 0 and 36500"
    );
    ensure!(
        (1..=1024).contains(&options.window_multiple),
        "--window-multiple must be between 1 and 1024"
    );
    let mut connection = ledger.acquire().await?;
    let records = catalog_rows(&mut connection).await?;
    let attached = attached_partitions(&mut connection).await?;
    let mut unknowns = Vec::new();

    // The online horizon's first term: the oldest share_seq the payout window
    // can still reach at four times the requested weight. An empty window is
    // only clear when the ledger holds no accepted share at all; otherwise the
    // floor is unknown and blocks every partition.
    let floor_row = sqlx::query(
        "SELECT (SELECT min(share_seq) FROM qbit_prism_window(clock_timestamp(),$1::text::numeric*8*$2::bigint)) AS floor,\
         EXISTS(SELECT 1 FROM qbit_share_ledger WHERE accepted) AS any_accepted,\
         qbit_prism_share_next_seq() AS next_seq,\
         (SELECT last_share_seq FROM qbit_hashrate_rollup_progress WHERE singleton) AS watermark",
    )
    .bind(&options.network_difficulty)
    .bind(options.window_multiple)
    .fetch_one(&mut *connection)
    .await
    .context("reading the payout window floor, the share sequence and the rollup watermark")?;
    let window_floor: Option<i64> = floor_row.try_get("floor")?;
    let any_accepted: bool = floor_row.try_get("any_accepted")?;
    let next_share_seq: i64 = floor_row.try_get("next_seq")?;
    let watermark: Option<i64> = floor_row.try_get("watermark")?;
    if window_floor.is_none() && any_accepted {
        unknowns.push(
            "the payout window returned no rows while the ledger holds accepted shares; the window floor is unknown".into(),
        );
    }
    if watermark.is_none() {
        unknowns.push(
            "qbit_hashrate_rollup_progress holds no row, so the rollup sweep has never run and no partition's contribution is known to be folded; start a frontend with PRISM_HASHRATE_ROLLUP_ENABLED and let it catch up".into(),
        );
    }
    for name in &attached {
        if !records.iter().any(|record| &record.partition_name == name) {
            unknowns.push(format!(
                "{name} is attached to {PARENT} but has no qbit_prism_share_partitions row; it was not created by the ledger and retention cannot reason about it"
            ));
        }
    }

    let duplicate_share_ids = if options.check_duplicates {
        Some(
            sqlx::query_scalar::<_, String>(
                "SELECT share_id FROM qbit_share_ledger GROUP BY share_id HAVING count(*)>1 ORDER BY share_id LIMIT 100",
            )
            .fetch_all(&mut *connection)
            .await
            .context("scanning the attached leaves for a share_id in more than one of them")?,
        )
    } else {
        None
    };
    if duplicate_share_ids
        .as_ref()
        .is_some_and(|ids| !ids.is_empty())
    {
        unknowns.push(format!(
            "{} share_id value(s) appear in more than one attached partition; a row was inserted around the append path. Reconcile them before any partition leaves",
            duplicate_share_ids.as_ref().map(Vec::len).unwrap_or(0)
        ));
    }

    let mut partitions = Vec::new();
    for record in records {
        partitions.push(
            partition_plan(
                &mut connection,
                record,
                options,
                window_floor,
                any_accepted,
                watermark,
            )
            .await?,
        );
    }
    let attached_upper_seq = partitions
        .iter()
        .filter(|plan| plan.attachment.attached)
        .map(|plan| plan.record.upper_seq)
        .max();
    let report = PlanReport {
        schema: "qbit.prism.share-archive-plan.v1",
        generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true),
        network_difficulty: options.network_difficulty.clone(),
        window_multiple: options.window_multiple,
        retention_days: options.retention_days,
        window_floor_share_seq: window_floor,
        next_share_seq,
        rollup_last_share_seq: watermark,
        attached_count: i64::try_from(attached.len())?,
        attached_upper_seq,
        lead_rows_ahead: attached_upper_seq.map(|upper| upper - next_share_seq),
        duplicate_share_ids,
        unknowns,
        eligible: partitions
            .iter()
            .filter(|plan| plan.eligible)
            .map(|plan| plan.record.partition_name.clone())
            .collect(),
        partitions,
    };
    Ok(report)
}

async fn partition_plan(
    connection: &mut PgConnection,
    record: PartitionRecord,
    options: &PlanOptions,
    window_floor: Option<i64>,
    any_accepted: bool,
    watermark: Option<i64>,
) -> Result<PartitionPlan> {
    check_partition_name(&record.partition_name)?;
    let attachment = attachment(connection, &record.partition_name).await?;
    let mut conditions = Vec::new();
    let mut live_rows = None;
    let mut newest_accepted_at = None;
    let mut unknowns = Vec::new();
    if record.state == "attached" && !attachment.attached {
        unknowns.push(format!(
            "the catalog records {} as attached but {PARENT} does not hold it as a partition; run share-archive detach {} to reconcile the catalog with pg_inherits",
            record.partition_name, record.partition_name
        ));
    }
    if record.state == "attached" && !attachment.relation_present {
        unknowns.push(format!(
            "the catalog records {} as attached but no relation of that name exists; restore it from its archive before retention continues",
            record.partition_name
        ));
    }
    if record.state == "detached" && !attachment.relation_present {
        unknowns.push(format!(
            "the catalog records {} as detached but no relation of that name exists; it was dropped outside share-archive, so its archive is now the only copy. Run share-archive drop {} to record that",
            record.partition_name, record.partition_name
        ));
    }
    if attachment.detach_pending {
        unknowns.push(format!(
            "{} carries pg_inherits.inhdetachpending: an earlier DETACH PARTITION ... CONCURRENTLY was interrupted. Run share-archive detach {} to finalize it",
            record.partition_name, record.partition_name
        ));
    }

    if !attachment.attached || !attachment.relation_present {
        for name in CONDITION_NAMES {
            conditions.push(Condition::not_applicable(
                name,
                format!(
                    "{} is recorded {} and is not an attached partition",
                    record.partition_name, record.state
                ),
            ));
        }
    } else {
        let stats = sqlx::query(&format!(
            "SELECT count(*)::bigint AS rows,max(accepted_at) AS newest FROM {}",
            record.partition_name
        ))
        .fetch_one(&mut *connection)
        .await
        .with_context(|| format!("counting the live rows of {}", record.partition_name))?;
        let rows: i64 = stats.try_get("rows")?;
        let newest: Option<DateTime<Utc>> = stats.try_get("newest")?;
        live_rows = Some(rows);
        newest_accepted_at = newest;

        conditions.push(match window_floor {
            _ if !any_accepted => Condition::clear(
                "payout_window",
                "the ledger holds no accepted share, so no payout window reaches this partition",
            ),
            None => Condition::unknown(
                "payout_window",
                "the payout window returned no rows while the ledger holds accepted shares",
            ),
            Some(floor) if floor >= record.upper_seq => Condition::clear(
                "payout_window",
                format!(
                    "the payout window at {}x reaches back only to share_seq {floor}, above this partition's upper bound {}",
                    options.window_multiple, record.upper_seq
                ),
            ),
            Some(floor) => Condition::blocked(
                "payout_window",
                format!(
                    "the payout window at {}x still reaches share_seq {floor}, inside this partition's bounds {}",
                    options.window_multiple,
                    record.bounds()
                ),
            ),
        });

        let age = sqlx::query_scalar::<_, Option<f64>>(&format!(
            "SELECT (extract(epoch FROM clock_timestamp()-max(accepted_at))/86400)::double precision FROM {}",
            record.partition_name
        ))
        .fetch_one(&mut *connection)
        .await?;
        conditions.push(match age {
            None => Condition::clear("retention_age", "the partition holds no row to retain"),
            Some(days) if days >= options.retention_days as f64 => Condition::clear(
                "retention_age",
                format!(
                    "the newest share in the partition is {days:.2} days old, at or past the {} day retention age",
                    options.retention_days
                ),
            ),
            Some(days) => Condition::blocked(
                "retention_age",
                format!(
                    "the newest share in the partition is {days:.2} days old, inside the {} day retention age",
                    options.retention_days
                ),
            ),
        });

        conditions.push(match watermark {
            None => Condition::unknown(
                "rollup_watermark",
                "qbit_hashrate_rollup_progress holds no row: the rollups have never run, so this partition's contribution is not in the permanent rollup tables",
            ),
            Some(last) if last >= record.upper_seq - 1 => Condition::clear(
                "rollup_watermark",
                format!(
                    "the rollup sweep has folded share_seq up to {last}, at or past this partition's last possible row {}",
                    record.upper_seq - 1
                ),
            ),
            Some(last) => Condition::blocked(
                "rollup_watermark",
                format!(
                    "the rollup sweep has folded share_seq up to {last} only; this partition needs {}",
                    record.upper_seq - 1
                ),
            ),
        });

        let (intersecting, unsealed) = intersecting_audits(&mut *connection, &record).await?;
        conditions.push(if unsealed == 0 {
            Condition::clear(
                "audits_sealed",
                format!("all {intersecting} audit row(s) whose snapshot intersects the partition store their canonical bytes"),
            )
        } else {
            Condition::blocked(
                "audits_sealed",
                format!(
                    "{unsealed} of {intersecting} audit row(s) whose snapshot intersects the partition have no canonical_audit_bytes; run share-archive seal {}",
                    record.partition_name
                ),
            )
        });

        let (outbox, deferred) = pending_references(connection, &record).await?;
        conditions.push(if outbox == 0 && deferred == 0 {
            Condition::clear(
                "pending_references",
                "no unfinished block candidates name a share_id in this partition or read their payout window from it, and no deferred shares name a share_id in it",
            )
        } else {
            Condition::blocked(
                "pending_references",
                format!(
                    "{outbox} unfinished block candidate(s) name a share_id in this partition or read their payout window from it, and {deferred} deferred share(s) name a share_id in it; let the outbox finish or reconcile it first"
                ),
            )
        });
    }

    let blockers: Vec<String> = conditions
        .iter()
        .filter(|condition| condition.status != "clear")
        .map(|condition| format!("{}: {}", condition.name, condition.detail))
        .collect();
    let eligible = blockers.is_empty() && unknowns.is_empty();
    Ok(PartitionPlan {
        record,
        attachment,
        live_rows,
        newest_accepted_at,
        conditions,
        blockers,
        unknowns,
        eligible,
    })
}

// ---------------------------------------------------------------------------
// seal
// ---------------------------------------------------------------------------

/// Store the canonical bytes of every audit whose share snapshot intersects
/// the partition and that has none yet, so the block keeps serving its
/// advertised artifact once its shares are gone.
///
/// Each artifact is rebuilt from the still-online shares by
/// [`audit_canonical_bytes`], which refuses anything whose digest is not the
/// `audit_bundle_sha256` the block committed to. The write is guarded on that
/// digest and on the column still being NULL, so a second seal, or one running
/// beside this one, stores nothing and loses nothing. `sealed_at` is recorded
/// only once nothing is left, so an interrupted seal never claims the
/// partition is sealed.
pub async fn seal(ledger: &Ledger, partition_name: &str) -> Result<Value> {
    check_partition_name(partition_name)?;
    let record = catalog_row(&mut *ledger.acquire().await?, partition_name).await?;
    let mut sealed = 0i64;
    let mut sealed_bytes = 0i64;
    let mut cursor = String::new();
    loop {
        // One artifact at a time: a production window is hundreds of
        // megabytes, and nothing here may hold two of them. Each statement
        // takes its own checkout and gives it back, so the rebuild below is
        // never waiting on a connection this loop is sitting on: the operator
        // pool has two.
        let row = sqlx::query(
            "SELECT a.block_hash,a.audit_bundle_sha256 FROM qbit_pool_audit_bundles a \
             JOIN qbit_prism_audit_snapshots s ON s.snapshot_sha256=a.share_snapshot_sha256 \
             WHERE a.canonical_audit_bytes IS NULL AND s.inline_shares IS NULL \
               AND s.first_share_seq<$1 AND s.last_share_seq>=COALESCE($2,s.first_share_seq) \
               AND a.block_hash>$3 ORDER BY a.block_hash LIMIT 1",
        )
        .bind(record.upper_seq)
        .bind(record.lower_seq)
        .bind(&cursor)
        .fetch_optional(&mut *ledger.acquire().await?)
        .await?;
        let Some(row) = row else { break };
        let block_hash: String = row.try_get("block_hash")?;
        let digest: String = row.try_get("audit_bundle_sha256")?;
        cursor = block_hash.clone();
        let bytes = audit_canonical_bytes(&ledger.pool, &block_hash)
            .await
            .with_context(|| {
                format!("rebuilding the canonical audit of block {block_hash} to seal {partition_name}; its shares must still be online to seal it")
            })?
            .with_context(|| {
                format!("block {block_hash} has neither stored canonical bytes nor a share snapshot to rebuild from; import its legacy body first")
            })?;
        let length = i64::try_from(bytes.len())?;
        let updated = sqlx::query(
            "UPDATE qbit_pool_audit_bundles SET canonical_audit_bytes=$3 WHERE block_hash=$1 AND canonical_audit_bytes IS NULL AND audit_bundle_sha256=$2",
        )
        .bind(&block_hash)
        .bind(&digest)
        .bind(bytes)
        .execute(&mut *ledger.acquire().await?)
        .await?
        .rows_affected();
        if updated == 1 {
            sealed += 1;
            sealed_bytes += length;
        }
    }
    let (intersecting, unsealed) =
        intersecting_audits(&mut *ledger.acquire().await?, &record).await?;
    let mut recorded = record.sealed_at;
    if unsealed == 0 {
        let mut tx = ledger.begin().await?;
        sqlx::query("UPDATE qbit_prism_share_partitions SET sealed_at=COALESCE(sealed_at,clock_timestamp()) WHERE partition_name=$1")
            .bind(partition_name)
            .execute(&mut *tx)
            .await?;
        recorded = sqlx::query_scalar(
            "SELECT sealed_at FROM qbit_prism_share_partitions WHERE partition_name=$1",
        )
        .bind(partition_name)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
    }
    Ok(json!({
        "schema": "qbit.prism.share-archive-seal.v1",
        "partition_name": partition_name,
        "intersecting_audits": intersecting,
        "sealed_now": sealed,
        "sealed_bytes": sealed_bytes,
        "unsealed_remaining": unsealed,
        "sealed_at": recorded,
    }))
}

// ---------------------------------------------------------------------------
// archive
// ---------------------------------------------------------------------------

/// Write `<root>/qbit_share_ledger/<partition>/rows.ndjson.gz` and
/// `manifest.json`, and record the location, the digests and the row count.
///
/// Only a partition the share sequence has passed is archived: below that,
/// appends can still land in it and the archive would be incomplete the moment
/// one did. Both files are written to temp names and renamed into place, so a
/// reader never sees a partial archive and a failed run leaves none. An existing
/// archive is overwritten only with `--force`, and only after the replacement
/// manifest exists on disk. The catalog is updated once, after both renames,
/// and the previous verification is cleared with it: a new archive has not
/// been verified. Every later archive chains to the replaced manifest's digest,
/// so their verifications are cleared too and each has to be written again in
/// order; once one of them has left the ledger it cannot be, and the archive
/// is refused instead.
pub async fn archive(
    ledger: &Ledger,
    partition_name: &str,
    root: &Path,
    force: bool,
    created_by: &str,
) -> Result<Value> {
    check_partition_name(partition_name)?;
    let mut connection = ledger.acquire().await?;
    let record = catalog_row(&mut connection, partition_name).await?;
    let attachment = attachment(&mut connection, partition_name).await?;
    ensure!(
        record.state == "attached" && attachment.attached,
        "refusing to archive {partition_name}: it is recorded {} and {} a partition of {PARENT}. Only an attached partition can be archived from its live rows",
        record.state,
        if attachment.attached { "is" } else { "is not" }
    );
    let next_share_seq = next_share_seq(&mut connection).await?;
    check_sequence_passed(&record, next_share_seq, "archive")?;
    drain_appends(ledger, &mut connection).await?;
    ensure!(
        record.archived_at.is_none() || force,
        "refusing to archive {partition_name}: it was already archived at {} into {}. Pass --force to write it again, which also clears the recorded verification, its own and that of every later archive",
        record
            .archived_at
            .map(|at| at.to_rfc3339())
            .unwrap_or_default(),
        record.archive_uri.as_deref().unwrap_or("an unrecorded path")
    );
    let schema_versions: Vec<i32> =
        sqlx::query_scalar("SELECT version FROM qbit_prism_schema_migrations ORDER BY version")
            .fetch_all(&mut *connection)
            .await?;
    // The chain: the archived partition with the next-lower upper_seq. It has
    // to end exactly where this one starts, so partitions are archived in
    // order and the chain never has to be repaired later.
    let previous = archived_predecessor(&mut connection, record.upper_seq).await?;
    ensure!(
        chain_is_adjacent(previous.as_ref().map(|row| row.upper_seq), record.lower_seq),
        "refusing to archive {partition_name}: its chain link would not be adjacent. {} while {partition_name} starts at {}; archive the partitions between them first, in order, so the chain stays contiguous",
        match &previous {
            Some(row) => format!(
                "The nearest archived partition below it, {}, ends at {}",
                row.partition_name, row.upper_seq
            ),
            None => "No partition below it is archived".to_owned(),
        },
        number_or(record.lower_seq, "MINVALUE")
    );
    // Every later archive chains, directly or through the ones between, to
    // this partition's manifest digest, and a new manifest has a new digest.
    // Each of them has to be written again in order and verified again, so
    // their verifications go with this one's; one that has already left the
    // ledger cannot be written again, and then this manifest has to stay.
    let successors = archived_successors(&mut connection, record.upper_seq).await?;
    let departed: Vec<&str> = successors
        .iter()
        .filter(|row| row.state != "attached")
        .map(|row| row.partition_name.as_str())
        .collect();
    ensure!(
        departed.is_empty(),
        "refusing to archive {partition_name} again: every later archive chains to its manifest and would have to be written again in order, but these have left the ledger and cannot be: {}. The recorded archive stays the copy of record; bring its files back from a copy of the archive root instead",
        departed.join(", ")
    );

    let directory = partition_dir(root, partition_name);
    std::fs::create_dir_all(&directory)
        .with_context(|| format!("creating the archive directory {}", directory.display()))?;
    let (rows_temp, file) = TempFile::create(&directory, "rows.ndjson.gz")?;
    let sink = flate2::write::GzEncoder::new(
        HashingWriter {
            inner: std::io::BufWriter::new(file),
            hasher: Sha256::new(),
            bytes: 0,
        },
        flate2::Compression::default(),
    );
    let stream = RowStream::new(
        partition_name,
        record.lower_seq,
        record.upper_seq,
        Some(sink),
    );
    let stream = stream_partition(&mut connection, partition_name, stream).await?;
    let (summary, file) = tokio::task::spawn_blocking(move || stream.finish()).await??;
    let (rows_gz_sha256, rows_gz_bytes) =
        file.context("the archive writer closed without a file digest")?;

    let manifest = ArchiveManifest {
        schema: ARCHIVE_SCHEMA_V1.to_owned(),
        partition_name: partition_name.to_owned(),
        lower_seq: record.lower_seq,
        upper_seq: record.upper_seq,
        row_count: summary.row_count,
        first_share_seq: summary.first_share_seq,
        last_share_seq: summary.last_share_seq,
        first_accepted_at_us: summary.first_accepted_at_us,
        last_accepted_at_us: summary.last_accepted_at_us,
        rows_sha256: summary.rows_sha256.clone(),
        rows_gz_sha256,
        rows_bytes: summary.rows_bytes,
        rows_gz_bytes,
        previous_manifest_sha256: previous
            .as_ref()
            .and_then(|row| row.archive_manifest_sha256.clone()),
        previous_upper_seq: previous.as_ref().map(|row| row.upper_seq),
        schema_versions,
        created_at: Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true),
        created_by: created_by.to_owned(),
    };
    let (manifest_bytes, manifest_sha256) = manifest.canonical()?;
    let (manifest_temp, mut manifest_file) = TempFile::create(&directory, "manifest.json")?;
    manifest_file.write_all(&manifest_bytes)?;
    manifest_file.sync_all()?;
    drop(manifest_file);
    // Both replacements exist before either destination is overwritten.
    let rows_path = directory.join("rows.ndjson.gz");
    let manifest_path = directory.join("manifest.json");
    rows_temp.promote(&rows_path)?;
    manifest_temp.promote(&manifest_path)?;

    let uri = manifest_path.display().to_string();
    // The operator pool holds two connections; give this one back before the
    // catalog transaction asks for one.
    drop(connection);
    let mut tx = ledger.begin().await?;
    let updated = sqlx::query(
        "UPDATE qbit_prism_share_partitions SET archive_uri=$2,archive_manifest_sha256=$3,archive_rows=$4,archive_rows_sha256=$5,archived_at=clock_timestamp(),archive_verified_at=NULL WHERE partition_name=$1 AND state='attached'",
    )
    .bind(partition_name)
    .bind(&uri)
    .bind(&manifest_sha256)
    .bind(summary.row_count)
    .bind(&summary.rows_sha256)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    ensure!(
        updated == 1,
        "{partition_name} left the attached state while it was being archived; the files at {uri} are complete but nothing was recorded. Run share-archive plan and archive it again"
    );
    let verification_cleared: Vec<&str> = successors
        .iter()
        .map(|row| row.partition_name.as_str())
        .collect();
    if !verification_cleared.is_empty() {
        let cleared = sqlx::query(
            "UPDATE qbit_prism_share_partitions SET archive_verified_at=NULL WHERE archive_manifest_sha256 IS NOT NULL AND upper_seq>$1 AND state='attached'",
        )
        .bind(record.upper_seq)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        ensure!(
            cleared == u64::try_from(verification_cleared.len())?,
            "the set of later archived partitions changed while {partition_name} was being archived; the files at {uri} are complete but nothing was recorded. Run share-archive plan and archive it again"
        );
    }
    tx.commit().await?;
    Ok(json!({
        "schema": "qbit.prism.share-archive-archive.v1",
        "partition_name": partition_name,
        "archive_uri": uri,
        "rows_uri": rows_path.display().to_string(),
        "archive_manifest_sha256": manifest_sha256,
        "verification_cleared": verification_cleared,
        "manifest": manifest,
    }))
}

// ---------------------------------------------------------------------------
// verify
// ---------------------------------------------------------------------------

/// Where an archive is: the layout under `--dir`, or, when that is not there,
/// the path the catalog recorded when it was written.
fn manifest_location(root: &Path, record: &PartitionRecord) -> Result<PathBuf> {
    let layout = partition_dir(root, &record.partition_name).join("manifest.json");
    if layout.exists() {
        return Ok(layout);
    }
    let recorded = record.archive_uri.as_deref().with_context(|| {
        format!(
            "{} has no archive: {} does not exist and the catalog records no archive_uri. Run share-archive archive {} --dir <root> first",
            record.partition_name,
            layout.display(),
            record.partition_name
        )
    })?;
    let recorded = PathBuf::from(recorded);
    ensure!(
        recorded.exists(),
        "{} is archived at {} according to the catalog, but neither that path nor {} exists. Restore the archive to one of them",
        record.partition_name,
        recorded.display(),
        layout.display()
    );
    Ok(recorded)
}

/// Re-read the archive, recompute both digests, check the manifest against the
/// catalog row and the chain, including that the chain has no gap, and, while
/// the partition is still attached, stream the live rows again and compare the
/// stream digest and the count. Records `archive_verified_at` only when
/// everything agreed and the live rows were compared, and only once the share
/// sequence has passed the partition; after a detach the verification is
/// reported only.
pub async fn verify(ledger: &Ledger, partition_name: &str, root: &Path) -> Result<Value> {
    check_partition_name(partition_name)?;
    let mut connection = ledger.acquire().await?;
    let record = catalog_row(&mut connection, partition_name).await?;
    let attachment = attachment(&mut connection, partition_name).await?;
    let path = manifest_location(root, &record)?;
    let rows_path = path
        .parent()
        .context("the manifest path has no directory")?
        .join("rows.ndjson.gz");
    let (manifest, manifest_sha256, _) = tokio::task::spawn_blocking({
        let path = path.clone();
        move || read_manifest(&path)
    })
    .await??;
    ensure!(
        manifest.partition_name == partition_name,
        "{} holds the archive of {}, not of {partition_name}",
        path.display(),
        manifest.partition_name
    );
    ensure!(
        manifest.lower_seq == record.lower_seq && manifest.upper_seq == record.upper_seq,
        "{path_display}: the archive records bounds [{}, {}) but the catalog records {}",
        manifest
            .lower_seq
            .map(|lower| lower.to_string())
            .unwrap_or_else(|| "MINVALUE".into()),
        manifest.upper_seq,
        record.bounds(),
        path_display = path.display()
    );
    if let Some(recorded) = &record.archive_manifest_sha256 {
        ensure!(
            recorded == &manifest_sha256,
            "{}: the manifest hashes to {manifest_sha256} but the catalog records {recorded}; this is not the archive that was recorded for {partition_name}",
            path.display()
        );
    }
    if let Some(recorded) = record.archive_rows {
        ensure!(
            recorded == manifest.row_count,
            "{}: the manifest records {} rows but the catalog records {recorded}",
            path.display(),
            manifest.row_count
        );
    }
    if let Some(recorded) = &record.archive_rows_sha256 {
        ensure!(
            recorded == &manifest.rows_sha256,
            "{}: the manifest records rows_sha256 {} but the catalog records {recorded}",
            path.display(),
            manifest.rows_sha256
        );
    }

    // The chain: the archived partition with the next-lower upper_seq.
    let previous = archived_predecessor(&mut connection, record.upper_seq).await?;
    let expected_link = previous
        .as_ref()
        .and_then(|row| row.archive_manifest_sha256.clone());
    let expected_upper = previous.as_ref().map(|row| row.upper_seq);
    ensure!(
        manifest.previous_manifest_sha256 == expected_link
            && manifest.previous_upper_seq == expected_upper,
        "{}: the manifest chains to {:?} at upper_seq {:?} but the catalog's nearest archived predecessor is {:?} at {:?}",
        path.display(),
        manifest.previous_manifest_sha256,
        manifest.previous_upper_seq,
        expected_link,
        expected_upper
    );
    // A link to the nearest archived partition is not enough: it has to end
    // where this one starts, or the chain no longer proves the archived
    // history is contiguous, and that is nothing to certify.
    ensure!(
        chain_is_adjacent(manifest.previous_upper_seq, manifest.lower_seq),
        "{}: the manifest chains to a predecessor ending at {} while the partition starts at {}; a partition between them is missing from the chain",
        path.display(),
        number_or(manifest.previous_upper_seq, "nothing"),
        number_or(manifest.lower_seq, "MINVALUE")
    );

    let rows_path_for_read = rows_path.clone();
    let manifest_for_read = manifest.clone();
    tokio::task::spawn_blocking(move || {
        read_rows_file(&rows_path_for_read, &manifest_for_read, |_| Ok(()))
    })
    .await??;

    let mut live = Value::Null;
    if attachment.attached && attachment.relation_present {
        // The comparison below is a proof only while nothing more can land in
        // the partition: once the sequence has passed its upper bound no
        // append can reach it, and the immutability trigger holds the rest.
        let next_share_seq = next_share_seq(&mut connection).await?;
        check_sequence_passed(&record, next_share_seq, "verify")?;
        drain_appends(ledger, &mut connection).await?;
        let stream = RowStream::new(partition_name, record.lower_seq, record.upper_seq, None);
        let stream = stream_partition(&mut connection, partition_name, stream).await?;
        let (summary, _) = stream.finish()?;
        ensure!(
            summary.rows_sha256 == manifest.rows_sha256,
            "the live rows of {partition_name} stream to rows_sha256 {} but the archive at {} records {}; the archive is not a copy of the partition",
            summary.rows_sha256,
            rows_path.display(),
            manifest.rows_sha256
        );
        ensure!(
            summary.row_count == manifest.row_count,
            "the live rows of {partition_name} number {} but the archive records {}",
            summary.row_count,
            manifest.row_count
        );
        live = json!({"row_count": summary.row_count, "rows_sha256": summary.rows_sha256});
    }

    drop(connection);
    let live_rows_compared = attachment.attached && attachment.relation_present;
    let mut tx = ledger.begin().await?;
    if live_rows_compared {
        // Fence the evidence checked above against archive rewrites. Lock
        // predecessor before successor, in the same order archive updates
        // its own digest and invalidates later verifications.
        let previous: Option<(i64, String)> = sqlx::query_as(
            "SELECT upper_seq,archive_manifest_sha256 FROM qbit_prism_share_partitions WHERE archive_manifest_sha256 IS NOT NULL AND upper_seq<$1 ORDER BY upper_seq DESC LIMIT 1 FOR SHARE",
        )
        .bind(record.upper_seq)
        .fetch_optional(&mut *tx)
        .await?;
        ensure!(
            previous.as_ref().map(|(upper, _)| *upper) == expected_upper
                && previous.as_ref().map(|(_, digest)| digest) == expected_link.as_ref(),
            "{partition_name}'s predecessor archive was rewritten during verification; verify the current archive chain again"
        );
        // The timestamp is the proof detach and drop require: the archive
        // was compared with the live rows. A verify after the detach checks
        // the archive against itself and the catalog only, and reports
        // that without recording it, so it can never stand in for the
        // comparison the detach needed.
        let updated = sqlx::query("UPDATE qbit_prism_share_partitions SET archive_verified_at=clock_timestamp() WHERE partition_name=$1 AND archive_manifest_sha256=$2 AND archived_at IS NOT NULL")
            .bind(partition_name)
            .bind(&manifest_sha256)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        ensure!(
            updated == 1,
            "{partition_name}'s archive was rewritten during verification or has no archived_at in qbit_prism_share_partitions, so the verification cannot be recorded. Run share-archive archive and verify for {partition_name} again"
        );
    }
    let verified_at: Option<DateTime<Utc>> = sqlx::query_scalar(
        "SELECT archive_verified_at FROM qbit_prism_share_partitions WHERE partition_name=$1",
    )
    .bind(partition_name)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(json!({
        "schema": "qbit.prism.share-archive-verify.v1",
        "partition_name": partition_name,
        "archive_uri": path.display().to_string(),
        "archive_manifest_sha256": manifest_sha256,
        "row_count": manifest.row_count,
        "rows_sha256": manifest.rows_sha256,
        "rows_gz_sha256": manifest.rows_gz_sha256,
        "chain_previous_upper_seq": manifest.previous_upper_seq,
        "live_rows_compared": live_rows_compared,
        "live": live,
        "archive_verified_at": verified_at,
    }))
}

// ---------------------------------------------------------------------------
// detach and drop
// ---------------------------------------------------------------------------

/// Take a connection for DDL that must not be cut short.
///
/// `DETACH PARTITION ... CONCURRENTLY` waits for the transactions that can
/// still see the partition, and `DROP TABLE` waits for its ACCESS EXCLUSIVE
/// lock. Both are deliberate operator actions on a partition the plan has
/// already cleared, and neither blocks an append or a read of the parent, so
/// they run without the pool's 15 s statement timeout rather than failing
/// halfway through catalog work.
async fn ddl_connection(ledger: &Ledger) -> Result<sqlx::pool::PoolConnection<Postgres>> {
    let mut connection = ledger.acquire().await?;
    sqlx::query(
        "SELECT set_config('statement_timeout','0',false),set_config('lock_timeout','0',false)",
    )
    .execute(&mut *connection)
    .await?;
    Ok(connection)
}

/// Detach one partition, leaving the table as a standalone relation.
///
/// Requires every condition `plan` evaluates, plus `sealed_at`, `archived_at`
/// and `archive_verified_at`, and a live row count equal to the archive's.
/// There is no override: the conditions are what make the detach reversible
/// from the archive and harmless to every reader.
///
/// EP-STATE and EP-ERRORS: the DDL runs outside any transaction (PostgreSQL
/// refuses `DETACH ... CONCURRENTLY` in a transaction block), so the catalog
/// update cannot be atomic with it. `pg_inherits` is therefore read as the
/// truth on every run: a partition already detached, or one left with
/// `inhdetachpending` by an interrupted attempt, is finalized or simply
/// reconciled into the catalog instead of being detached again.
pub async fn detach(ledger: &Ledger, partition_name: &str, options: &PlanOptions) -> Result<Value> {
    check_partition_name(partition_name)?;
    let report = plan(ledger, options).await?;
    let entry = report
        .partitions
        .iter()
        .find(|entry| entry.record.partition_name == partition_name)
        .with_context(|| format!("{partition_name} has no qbit_prism_share_partitions row"))?;
    let record = &entry.record;
    // A share_id in two attached leaves means a row was written around the
    // append path. It is reported rather than gating by default, because the
    // scan is a pass over every attached partition; when the operator did ask
    // for it, a hit stops the detach rather than being printed and ignored.
    ensure!(
        report
            .duplicate_share_ids
            .as_ref()
            .is_none_or(|duplicates| duplicates.is_empty()),
        "refusing to detach {partition_name}: --check-duplicates found {} share_id value(s) in more than one attached partition. Reconcile them before any partition leaves",
        report
            .duplicate_share_ids
            .as_ref()
            .map(Vec::len)
            .unwrap_or(0)
    );
    let attachment = attachment(&mut *ledger.acquire().await?, partition_name).await?;

    if !attachment.attached {
        // The DDL of an earlier run succeeded and its catalog update did not.
        ensure!(
            attachment.relation_present,
            "{partition_name} is neither a partition of {PARENT} nor a relation; it was dropped. Restore it from its archive with share-archive restore"
        );
        ensure!(
            record.archive_verified_at.is_some(),
            "{partition_name} is no longer a partition of {PARENT} but its archive was never verified, so the catalog cannot record the detach. Verify the archive with share-archive verify {partition_name} --dir <root>"
        );
        let reconciled = record_detached(ledger, partition_name).await?;
        return Ok(json!({
            "schema": "qbit.prism.share-archive-detach.v1",
            "partition_name": partition_name,
            "action": "reconciled",
            "detail": "PostgreSQL no longer holds it as a partition; the catalog was brought in line",
            "detached_at": reconciled,
        }));
    }
    ensure!(
        record.sealed_at.is_some(),
        "refusing to detach {partition_name}: no sealed_at is recorded. Run share-archive seal {partition_name}"
    );
    ensure!(
        record.archived_at.is_some(),
        "refusing to detach {partition_name}: no archived_at is recorded. Run share-archive archive {partition_name} --dir <root>"
    );
    ensure!(
        record.archive_verified_at.is_some(),
        "refusing to detach {partition_name}: no archive_verified_at is recorded. Run share-archive verify {partition_name} --dir <root>"
    );
    // The verification is a proof at the instant it was taken. Ledger rows are
    // immutable, so the only way the partition can have diverged from its
    // archive since is an append that committed after the comparison, and
    // that shows in the count `plan` just took.
    ensure!(
        entry.live_rows.is_some() && entry.live_rows == record.archive_rows,
        "refusing to detach {partition_name}: it holds {} live rows but its verified archive records {}; rows landed in it after the archive was verified. Run share-archive archive {partition_name} --dir <root> --force and verify it again",
        number_or(entry.live_rows, "an unknown number of"),
        number_or(record.archive_rows, "no")
    );
    // An interrupted `DETACH ... CONCURRENTLY` left the partition marked
    // detach-pending: PostgreSQL has already excluded it from the parent for
    // every new snapshot, and only FINALIZE can move it forward. Its
    // eligibility was proved by the run that started the detach, and `plan`
    // reports the pending mark itself as an unknown, so the eligibility gate
    // below would refuse the one statement that resolves it.
    ensure!(
        attachment.detach_pending || entry.eligible,
        "{}",
        entry.refusal("detach")
    );

    let mut ddl = ddl_connection(ledger).await?;
    let statement = if attachment.detach_pending {
        format!("ALTER TABLE {PARENT} DETACH PARTITION {partition_name} FINALIZE")
    } else {
        format!("ALTER TABLE {PARENT} DETACH PARTITION {partition_name} CONCURRENTLY")
    };
    sqlx::raw_sql(&statement)
        .execute(&mut *ddl)
        .await
        .with_context(|| format!("running: {statement}"))?;
    drop(ddl);
    let after = attachment2(ledger, partition_name).await?;
    ensure!(
        !after.attached,
        "{statement} returned without detaching {partition_name}; it is still a partition of {PARENT}. Run share-archive detach {partition_name} again"
    );
    let detached_at = record_detached(ledger, partition_name).await?;
    Ok(json!({
        "schema": "qbit.prism.share-archive-detach.v1",
        "partition_name": partition_name,
        "action": if attachment.detach_pending {"finalized"} else {"detached"},
        "statement": statement,
        "detached_at": detached_at,
    }))
}

/// A fresh attachment read on its own connection, after DDL released its lock.
async fn attachment2(ledger: &Ledger, partition_name: &str) -> Result<AttachmentState> {
    let mut connection = ledger.acquire().await?;
    attachment(&mut connection, partition_name).await
}

/// A number for an operator message, or what its absence means.
fn number_or(number: Option<i64>, absent: &str) -> String {
    number.map_or_else(|| absent.to_owned(), |number| number.to_string())
}

async fn record_detached(ledger: &Ledger, partition_name: &str) -> Result<Option<DateTime<Utc>>> {
    let mut tx = ledger.begin().await?;
    sqlx::query("UPDATE qbit_prism_share_partitions SET state='detached',detached_at=COALESCE(detached_at,clock_timestamp()) WHERE partition_name=$1 AND state='attached'")
        .bind(partition_name)
        .execute(&mut *tx)
        .await?;
    let detached_at = sqlx::query_scalar(
        "SELECT detached_at FROM qbit_prism_share_partitions WHERE partition_name=$1",
    )
    .bind(partition_name)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(detached_at)
}

/// Drop a detached, verified partition whose rows still number what the
/// archive recorded. The archive is the copy of record from here on. The
/// immutability trigger guards UPDATE, DELETE and TRUNCATE, not DROP, so no
/// share row is ever rewritten by this.
pub async fn drop_partition(ledger: &Ledger, partition_name: &str) -> Result<Value> {
    check_partition_name(partition_name)?;
    let (record, attachment) = {
        let mut connection = ledger.acquire().await?;
        let record = catalog_row(&mut connection, partition_name).await?;
        let attachment = attachment(&mut connection, partition_name).await?;
        (record, attachment)
    };
    ensure!(
        record.state == "detached" || record.state == "dropped",
        "refusing to drop {partition_name}: it is recorded {}. Only a detached partition is dropped; run share-archive detach {partition_name} first",
        record.state
    );
    ensure!(
        !attachment.attached,
        "refusing to drop {partition_name}: {PARENT} still holds it as a partition even though the catalog records it detached. Run share-archive detach {partition_name} to reconcile"
    );
    ensure!(
        record.archive_verified_at.is_some(),
        "refusing to drop {partition_name}: no archive_verified_at is recorded, so no copy of its rows is known to exist. Run share-archive verify {partition_name} --dir <root>"
    );
    if attachment.relation_present {
        // The last check before the one irreversible step: the standalone
        // relation holds exactly the rows the verified archive recorded.
        // Ledger rows are immutable, so a count is the whole comparison.
        let held: i64 =
            sqlx::query_scalar(&format!("SELECT count(*)::bigint FROM {partition_name}"))
                .fetch_one(&mut *ledger.acquire().await?)
                .await
                .with_context(|| {
                    format!("counting the rows of {partition_name} before dropping it")
                })?;
        ensure!(
            Some(held) == record.archive_rows,
            "refusing to drop {partition_name}: it holds {held} rows but its verified archive records {}; the archive is not a copy of every row. Read the relation by name and reconcile it with the archive before it leaves",
            number_or(record.archive_rows, "no")
        );
        let mut ddl = ddl_connection(ledger).await?;
        sqlx::raw_sql(&format!("DROP TABLE {partition_name}"))
            .execute(&mut *ddl)
            .await
            .with_context(|| format!("running: DROP TABLE {partition_name}"))?;
    }
    let mut tx = ledger.begin().await?;
    sqlx::query("UPDATE qbit_prism_share_partitions SET state='dropped',dropped_at=COALESCE(dropped_at,clock_timestamp()) WHERE partition_name=$1 AND state='detached'")
        .bind(partition_name)
        .execute(&mut *tx)
        .await?;
    let dropped_at: Option<DateTime<Utc>> = sqlx::query_scalar(
        "SELECT dropped_at FROM qbit_prism_share_partitions WHERE partition_name=$1",
    )
    .bind(partition_name)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(json!({
        "schema": "qbit.prism.share-archive-drop.v1",
        "partition_name": partition_name,
        "relation_dropped": attachment.relation_present,
        "archive_uri": record.archive_uri,
        "dropped_at": dropped_at,
    }))
}

// ---------------------------------------------------------------------------
// restore
// ---------------------------------------------------------------------------

/// Recreate a partition table from its archive, and optionally attach it back.
///
/// The whole restore is one transaction: the table, its constraints, its
/// trigger, every row, the re-streamed digest and, with `--attach`, the
/// attachment and the catalog row. A failure at any point leaves the database
/// exactly as it was, so the command is always safe to run again; that is also
/// why the relation must not exist beforehand, rather than being adopted.
///
/// The rows are inserted with their archived `share_seq`, so the share
/// sequence is never read and never set: a restore of old history must not
/// move the sequence that live appends draw from.
pub async fn restore(
    ledger: &Ledger,
    manifest_path: &Path,
    root: &Path,
    attach: bool,
) -> Result<Value> {
    let path = if manifest_path.is_absolute() || manifest_path.exists() {
        manifest_path.to_path_buf()
    } else {
        root.join(manifest_path)
    };
    let (manifest, manifest_sha256, _) = tokio::task::spawn_blocking({
        let path = path.clone();
        move || read_manifest(&path)
    })
    .await??;
    let partition_name = manifest.partition_name.clone();
    let rows_path = path
        .parent()
        .context("the manifest path has no directory")?
        .join("rows.ndjson.gz");

    let mut connection = ledger.acquire().await?;
    let attachment = attachment(&mut connection, &partition_name).await?;
    ensure!(
        !attachment.relation_present,
        "refusing to restore {partition_name}: a relation already holds that name. Move it aside, or drop it once you are sure it is not the partition itself"
    );
    drop(connection);

    let lower = manifest.lower_seq;
    let upper = manifest.upper_seq;
    let bound = match lower {
        Some(lower) => format!("share_seq >= {lower} AND share_seq < {upper}"),
        None => format!("share_seq < {upper}"),
    };
    let mut tx = ledger.begin().await?;
    sqlx::query(
        "SELECT set_config('statement_timeout','0',true),set_config('lock_timeout','0',true)",
    )
    .execute(&mut *tx)
    .await?;
    // The same shape qbit_prism_share_partition_create builds, minus the
    // ATTACH and the catalog insert: the parent's columns, defaults,
    // constraints and indexes, the per-leaf share_id uniqueness, the bound
    // (validated here on the empty table, so a later ATTACH needs no scan)
    // and the immutability trigger.
    for statement in [
        format!("CREATE TABLE {partition_name} (LIKE {PARENT} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)"),
        format!("ALTER TABLE {partition_name} ADD CONSTRAINT {partition_name}_share_id_key UNIQUE (share_id)"),
        format!("ALTER TABLE {partition_name} ADD CONSTRAINT {partition_name}_bound CHECK ({bound})"),
        format!("CREATE TRIGGER qbit_prism_immutable_share_history BEFORE UPDATE OR DELETE OR TRUNCATE ON {partition_name} FOR EACH STATEMENT EXECUTE FUNCTION qbit_prism_preserve_audit_history()"),
    ] {
        sqlx::raw_sql(&statement)
            .execute(&mut *tx)
            .await
            .with_context(|| format!("running: {statement}"))?;
    }

    // The file is decompressed, digested and parsed on a blocking thread and
    // handed over one batch at a time, so neither the archive nor the restored
    // partition is ever held in memory whole.
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<Vec<ArchiveRow>>(2);
    let reader = tokio::task::spawn_blocking({
        let rows_path = rows_path.clone();
        let manifest = manifest.clone();
        move || -> Result<()> {
            let mut batch = Vec::with_capacity(RESTORE_BATCH_ROWS);
            read_rows_file(&rows_path, &manifest, |row| {
                batch.push(row);
                if batch.len() == RESTORE_BATCH_ROWS {
                    sender
                        .blocking_send(std::mem::take(&mut batch))
                        .map_err(|_| {
                            anyhow::anyhow!("the restore stopped reading {}", rows_path.display())
                        })?;
                    batch = Vec::with_capacity(RESTORE_BATCH_ROWS);
                }
                Ok(())
            })?;
            if !batch.is_empty() {
                sender.blocking_send(batch).map_err(|_| {
                    anyhow::anyhow!("the restore stopped reading {}", rows_path.display())
                })?;
            }
            Ok(())
        }
    });
    let mut inserted = 0i64;
    while let Some(batch) = receiver.recv().await {
        inserted += insert_batch(&mut tx, &partition_name, &batch).await?;
    }
    reader.await??;
    ensure!(
        inserted == manifest.row_count,
        "restored {inserted} rows into {partition_name} but the archive records {}",
        manifest.row_count
    );

    // EP-COMPAT: the restore reproduces the archived bytes, proved by
    // re-streaming the restored table through the same encoder the archive
    // was written with and comparing the digest, not by trusting the loader.
    let stream = RowStream::new(&partition_name, lower, upper, None);
    let stream = stream_partition(&mut tx, &partition_name, stream).await?;
    let (summary, _) = stream.finish()?;
    ensure!(
        summary.rows_sha256 == manifest.rows_sha256 && summary.row_count == manifest.row_count,
        "the restored {partition_name} streams to {} over {} rows but the archive records {} over {} rows",
        summary.rows_sha256,
        summary.row_count,
        manifest.rows_sha256,
        manifest.row_count
    );

    let mut attached = false;
    if attach {
        let from = lower
            .map(|lower| lower.to_string())
            .unwrap_or_else(|| "MINVALUE".into());
        let statement =
            format!("ALTER TABLE {PARENT} ATTACH PARTITION {partition_name} FOR VALUES FROM ({from}) TO ({upper})");
        sqlx::raw_sql(&statement)
            .execute(&mut *tx)
            .await
            .with_context(|| format!("running: {statement}"))?;
        rename_leaf_indexes(&mut tx, &partition_name).await?;
        let existing = sqlx::query("SELECT lower_seq,upper_seq,archive_manifest_sha256 FROM qbit_prism_share_partitions WHERE partition_name=$1 FOR UPDATE")
            .bind(&partition_name)
            .fetch_optional(&mut *tx)
            .await?;
        if let Some(existing) = existing {
            let recorded_lower: Option<i64> = existing.try_get("lower_seq")?;
            let recorded_upper: i64 = existing.try_get("upper_seq")?;
            ensure!(
                recorded_lower == lower && recorded_upper == upper,
                "refusing to re-attach {partition_name}: the catalog records bounds [{recorded_lower:?}, {recorded_upper}) but the archive records [{lower:?}, {upper})"
            );
            if let Some(recorded) =
                existing.try_get::<Option<String>, _>("archive_manifest_sha256")?
            {
                ensure!(
                    recorded == manifest_sha256,
                    "refusing to re-attach {partition_name}: the catalog records another archive as the copy of record ({recorded}); only that archive can return the partition, but the restored manifest hashes to {manifest_sha256}"
                );
            }
            sqlx::query("UPDATE qbit_prism_share_partitions SET state='attached',detached_at=NULL,dropped_at=NULL WHERE partition_name=$1")
                .bind(&partition_name)
                .execute(&mut *tx)
                .await?;
        } else {
            sqlx::query("INSERT INTO qbit_prism_share_partitions(partition_name,lower_seq,upper_seq,state) VALUES($1,$2,$3,'attached')")
                .bind(&partition_name)
                .bind(lower)
                .bind(upper)
                .execute(&mut *tx)
                .await?;
        }
        attached = true;
    }
    tx.commit().await?;
    Ok(json!({
        "schema": "qbit.prism.share-archive-restore.v1",
        "partition_name": partition_name,
        "archive_uri": path.display().to_string(),
        "archive_manifest_sha256": manifest_sha256,
        "row_count": summary.row_count,
        "rows_sha256": summary.rows_sha256,
        "lower_seq": lower,
        "upper_seq": upper,
        "attached": attached,
    }))
}

/// `LIKE ... INCLUDING INDEXES` names the copied indexes after their columns.
/// Once the table is attached, name each one after the parent index it now
/// belongs to, exactly as `qbit_prism_share_partition_create` does, so a
/// restored partition is indistinguishable from one the ledger created.
async fn rename_leaf_indexes(
    tx: &mut Transaction<'_, Postgres>,
    partition_name: &str,
) -> Result<()> {
    let rows = sqlx::query(
        "SELECT child.relname::text AS child_name,parent.relname::text AS parent_name \
         FROM pg_inherits i \
         JOIN pg_class child ON child.oid=i.inhrelid \
         JOIN pg_class parent ON parent.oid=i.inhparent \
         JOIN pg_index x ON x.indexrelid=child.oid \
         WHERE child.relkind='i' AND parent.relkind='I' AND x.indrelid=to_regclass($1)",
    )
    .bind(partition_name)
    .fetch_all(&mut **tx)
    .await?;
    for row in rows {
        let child: String = row.try_get("child_name")?;
        let parent: String = row.try_get("parent_name")?;
        let Some(suffix) = parent.strip_prefix(PARENT) else {
            continue;
        };
        let desired = format!("{partition_name}{suffix}");
        if child != desired {
            check_index_name(&child)?;
            check_index_name(&desired)?;
            sqlx::raw_sql(&format!("ALTER INDEX {child} RENAME TO {desired}"))
                .execute(&mut **tx)
                .await?;
        }
    }
    Ok(())
}

/// Index names come from the catalog, but they still reach SQL by
/// interpolation, so they are held to the same shape as every other
/// identifier this module writes.
fn check_index_name(name: &str) -> Result<()> {
    ensure!(
        !name.is_empty()
            && name.len() <= 63
            && name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
        "refusing to rename index {name}: only lowercase, digits and underscores are expected on a share ledger partition"
    );
    Ok(())
}

/// One multi-row `INSERT` with every value bound, and `share_seq` explicit so
/// the sequence default is never evaluated.
async fn insert_batch(
    tx: &mut Transaction<'_, Postgres>,
    partition_name: &str,
    batch: &[ArchiveRow],
) -> Result<i64> {
    if batch.is_empty() {
        return Ok(0);
    }
    let mut tuples = Vec::with_capacity(batch.len());
    for index in 0..batch.len() {
        let base = index * 17;
        let at = |offset: usize| base + offset + 1;
        tuples.push(format!(
            "(${},${},${},${},decode(${},'hex'),${}::text::numeric,${}::text::numeric,${},${},{},${},{},${},${},${},${},${})",
            at(0),
            at(1),
            at(2),
            at(3),
            at(4),
            at(5),
            at(6),
            at(7),
            at(8),
            RESTORE_TIMESTAMP.replace("$X", &format!("${}", at(9))),
            at(10),
            RESTORE_TIMESTAMP.replace("$X", &format!("${}", at(11))),
            at(12),
            at(13),
            at(14),
            at(15),
            at(16),
        ));
    }
    let sql = format!(
        "INSERT INTO {partition_name}(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,accepted,reject_reason,credit_policy,writer_id,writer_epoch) VALUES {}",
        tuples.join(",")
    );
    let mut query = sqlx::query(&sql);
    for row in batch {
        query = query
            .bind(row.share_seq)
            .bind(&row.share_id)
            .bind(&row.miner_id)
            .bind(&row.payout_order_key)
            .bind(&row.p2mr_program_hex)
            .bind(&row.share_difficulty)
            .bind(&row.network_difficulty)
            .bind(row.template_height)
            .bind(&row.job_id)
            .bind(row.job_issued_at_us)
            .bind(row.ntime)
            .bind(row.accepted_at_us)
            .bind(row.accepted)
            .bind(&row.reject_reason)
            .bind(&row.credit_policy)
            .bind(&row.writer_id)
            .bind(row.writer_epoch);
    }
    Ok(i64::try_from(
        query.execute(&mut **tx).await?.rows_affected(),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row() -> ArchiveRow {
        ArchiveRow {
            share_seq: 1,
            share_id: "alice.rig:0001".into(),
            miner_id: "alice".into(),
            payout_order_key: "alice".into(),
            p2mr_program_hex: "ab".repeat(32),
            share_difficulty: "1".into(),
            network_difficulty: "100".into(),
            template_height: 100,
            job_id: "job".into(),
            job_issued_at_us: 1_000_000,
            accepted_at_us: 1_758_000_000_000_000,
            ntime: 1,
            accepted: true,
            reject_reason: None,
            credit_policy: None,
            writer_id: "server-a".into(),
            writer_epoch: 0,
        }
    }

    fn manifest() -> ArchiveManifest {
        ArchiveManifest {
            schema: ARCHIVE_SCHEMA_V1.into(),
            partition_name: "qbit_share_ledger_p1".into(),
            lower_seq: Some(16_777_216),
            upper_seq: 33_554_432,
            row_count: 2,
            first_share_seq: Some(16_777_216),
            last_share_seq: Some(16_777_217),
            first_accepted_at_us: Some(1),
            last_accepted_at_us: Some(2),
            rows_sha256: "aa".repeat(32),
            rows_gz_sha256: "bb".repeat(32),
            rows_bytes: 400,
            rows_gz_bytes: 120,
            previous_manifest_sha256: Some("cc".repeat(32)),
            previous_upper_seq: Some(16_777_216),
            schema_versions: vec![2, 16],
            created_at: "2026-09-16T00:00:00.000000Z".into(),
            created_by: "server-a".into(),
        }
    }

    /// The design record's key order, with the two nullable columns present as
    /// JSON `null` rather than absent, and one terminator per row.
    #[test]
    fn archive_row_line_has_the_v1_key_order_and_round_trips() {
        let line = row().line().unwrap();
        let text = String::from_utf8(line.clone()).unwrap();
        assert!(text.ends_with('\n'), "{text}");
        let expected = format!(
            "{{\"share_seq\":1,\"share_id\":\"alice.rig:0001\",\"miner_id\":\"alice\",\"payout_order_key\":\"alice\",\"p2mr_program_hex\":\"{}\",\"share_difficulty\":\"1\",\"network_difficulty\":\"100\",\"template_height\":100,\"job_id\":\"job\",\"job_issued_at_us\":1000000,\"accepted_at_us\":1758000000000000,\"ntime\":1,\"accepted\":true,\"reject_reason\":null,\"credit_policy\":null,\"writer_id\":\"server-a\",\"writer_epoch\":0}}",
            "ab".repeat(32)
        );
        assert_eq!(text.trim_end_matches('\n'), expected);
        assert_eq!(ArchiveRow::parse_line(&line).unwrap(), row());
        // The terminator is optional on the way back in; the bytes the digest
        // covers always carry it.
        assert_eq!(
            ArchiveRow::parse_line(&line[..line.len() - 1]).unwrap(),
            row()
        );
    }

    #[test]
    fn archive_row_keeps_every_nullable_column_and_refuses_a_reordered_line() {
        let mut rejected = row();
        rejected.accepted = false;
        rejected.reject_reason = Some("stale-job".into());
        rejected.credit_policy = Some("stale-grace".into());
        let line = rejected.line().unwrap();
        assert!(String::from_utf8_lossy(&line).contains("\"reject_reason\":\"stale-job\""));
        assert_eq!(ArchiveRow::parse_line(&line).unwrap(), rejected);

        let reordered = b"{\"share_id\":\"a\",\"share_seq\":1}\n";
        let error = ArchiveRow::parse_line(reordered).unwrap_err().to_string();
        assert!(error.contains("ndjson row object"), "{error}");
        let scrambled = serde_json::json!({
            "miner_id":"alice","share_seq":1,"share_id":"alice.rig:0001","payout_order_key":"alice",
            "p2mr_program_hex":"ab".repeat(32),"share_difficulty":"1","network_difficulty":"100",
            "template_height":100,"job_id":"job","job_issued_at_us":1000000,
            "accepted_at_us":1758000000000000i64,"ntime":1,"accepted":true,"reject_reason":null,
            "credit_policy":null,"writer_id":"server-a","writer_epoch":0});
        let error = ArchiveRow::parse_line(serde_json::to_string(&scrambled).unwrap().as_bytes())
            .unwrap_err()
            .to_string();
        assert!(error.contains("canonical v1 key order"), "{error}");
    }

    /// The manifest is the record the catalog and the next manifest name by
    /// digest, so its bytes must be a function of its fields alone.
    #[test]
    fn manifest_canonical_bytes_are_fixed_and_carry_no_trailing_newline() {
        let (bytes, digest) = manifest().canonical().unwrap();
        assert!(!bytes.ends_with(b"\n"));
        let text = String::from_utf8(bytes.clone()).unwrap();
        assert!(
            text.starts_with("{\"schema\":\"qbit.prism.share-archive.v1\",\"partition_name\":\"qbit_share_ledger_p1\",\"lower_seq\":16777216,\"upper_seq\":33554432,\"row_count\":2,"),
            "{text}"
        );
        assert!(
            text.ends_with(
                ",\"created_at\":\"2026-09-16T00:00:00.000000Z\",\"created_by\":\"server-a\"}"
            ),
            "{text}"
        );
        assert_eq!(digest, hex::encode(Sha256::digest(&bytes)));
        let (parsed, parsed_digest) = parse_manifest(&bytes, "manifest.json").unwrap();
        assert_eq!(parsed, manifest());
        assert_eq!(parsed_digest, digest);
    }

    /// EP-COMPAT: an archive from a later format is refused by name, and a
    /// manifest that was reformatted is no longer the record it is named by.
    #[test]
    fn manifest_reader_refuses_an_unknown_version_and_a_reformatted_record() {
        let (bytes, _) = manifest().canonical().unwrap();
        let mut future: Value = serde_json::from_slice(&bytes).unwrap();
        future["schema"] = serde_json::json!("qbit.prism.share-archive.v2");
        let error = parse_manifest(&serde_json::to_vec(&future).unwrap(), "manifest.json")
            .unwrap_err()
            .to_string();
        assert!(error.contains("share-archive.v2"), "{error}");
        assert!(error.contains(ARCHIVE_SCHEMA_V1), "{error}");

        let pretty = serde_json::to_vec_pretty(&manifest()).unwrap();
        let error = parse_manifest(&pretty, "manifest.json")
            .unwrap_err()
            .to_string();
        assert!(error.contains("canonical encoding"), "{error}");

        let error = parse_manifest(
            b"{\"schema\":\"qbit.prism.share-archive.v1\"}",
            "manifest.json",
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("not a v1 share archive manifest"), "{error}");
    }

    /// The chain proves the archived history is contiguous: a first partition
    /// has no predecessor, and every other starts where its predecessor ended.
    #[test]
    fn chain_adjacency_accepts_only_a_contiguous_link() {
        assert!(chain_is_adjacent(None, None));
        assert!(chain_is_adjacent(Some(16_777_216), Some(16_777_216)));
        assert!(!chain_is_adjacent(Some(16_777_216), Some(33_554_432)));
        assert!(!chain_is_adjacent(None, Some(16_777_216)));
        assert!(!chain_is_adjacent(Some(16_777_216), None));
    }

    #[test]
    fn partition_names_and_difficulties_are_bounded_before_they_reach_sql() {
        check_partition_name("qbit_share_ledger_p0").unwrap();
        check_partition_name("qbit_share_ledger_p12").unwrap();
        for refused in [
            "qbit_share_ledger",
            "qbit_share_ledger_p",
            "qbit_share_ledger_p0; DROP TABLE qbit_share_ledger",
            "qbit_share_ledger_pa",
            "pg_class",
        ] {
            let error = check_partition_name(refused).unwrap_err().to_string();
            assert!(
                error.contains("is not a share ledger partition name"),
                "{refused}: {error}"
            );
        }
        check_network_difficulty("1").unwrap();
        check_network_difficulty(&"9".repeat(78)).unwrap();
        for refused in ["", "0", "-1", "1.5", "1e9", &"9".repeat(79)] {
            assert!(
                check_network_difficulty(refused).is_err(),
                "accepted {refused}"
            );
        }
    }
}
