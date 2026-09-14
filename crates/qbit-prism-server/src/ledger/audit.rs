use super::*;
use qbit_prism::FoundBlock;

/// Exact content-addressed bytes for the public artifact route. Imported
/// legacy bytes are authoritative; missing legacy sidecars permit the old
/// logical-JSON fallback. Native bodies are reproducible from their immutable
/// share snapshot and never need a second stored copy of the full window.
pub async fn audit_canonical_bytes(pool: &PgPool, block_hash: &str) -> Result<Option<Vec<u8>>> {
    let row = sqlx::query("SELECT audit_bundle,audit_bundle_sha256,share_snapshot_sha256,canonical_audit_bytes FROM qbit_pool_audit_bundles WHERE block_hash=$1")
        .bind(block_hash).fetch_optional(pool).await?;
    let Some(row) = row else { return Ok(None) };
    let expected: String = row.try_get("audit_bundle_sha256")?;
    if let Some(bytes) = row.try_get::<Option<Vec<u8>>, _>("canonical_audit_bytes")? {
        ensure!(
            hex::encode(Sha256::digest(&bytes)) == expected,
            "stored canonical audit bytes have a digest mismatch"
        );
        let value: Value = serde_json::from_slice(&bytes)?;
        ensure!(
            value.is_object(),
            "stored canonical audit bytes must contain a JSON object"
        );
        return Ok(Some(bytes));
    }
    let snapshot: Option<String> = row.try_get("share_snapshot_sha256")?;
    if snapshot.is_none() {
        return Ok(None);
    }
    let body: Option<Value> = row.try_get("audit_bundle")?;
    let mut logical = serde_json::json!({"audit_bundle":body,"audit_bundle_sha256":expected,"share_snapshot_sha256":snapshot});
    materialize_audit_row(pool, &mut logical, None).await?;
    let bundle: AuditBundle = serde_json::from_value(logical["audit_bundle"].take())?;
    let bytes = qbit_prism::canonical_audit_bundle_bytes(&bundle)?;
    ensure!(
        hex::encode(Sha256::digest(&bytes)) == expected,
        "canonical reconstructed audit digest mismatch"
    );
    Ok(Some(bytes))
}

/// Logical audit body of an imported row, decoded from its stored canonical
/// bytes. The bytes must match the row's declared digest and parse with the
/// shared audit parser; the result is the value the import once stored inline.
/// A two-copy body is hundreds of megabytes at production window sizes, so the
/// digest, parse and serialization all stay off the runtime threads. A
/// `permit` is held by the blocking job itself: Tokio keeps running that job
/// after its awaiting caller is dropped, and the permit must bound it anyway.
pub async fn decode_canonical_audit_body(
    bytes: Vec<u8>,
    expected: String,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
) -> Result<Value> {
    tokio::task::spawn_blocking(move || -> Result<Value> {
        let _permit = permit;
        ensure!(
            hex::encode(Sha256::digest(&bytes)) == expected,
            "stored canonical audit bytes have a digest mismatch"
        );
        let value: Value = serde_json::from_slice(&bytes)?;
        // Canonical bytes are always a flat bundle. The shared parser would
        // also resolve an envelope, including files relative to this process.
        ensure!(
            !matches!(
                value["schema"].as_str(),
                Some(qbit_prism::AUDIT_BODY_REF_SCHEMA | qbit_prism::AUDIT_BUNDLE_V2_SCHEMA)
            ),
            "stored canonical audit bytes hold an audit envelope, not a bundle"
        );
        let bundle = qbit_prism::parse_audit_bundle_value(value, None)?;
        Ok(serde_json::to_value(&bundle)?)
    })
    .await?
}

/// Hydrate the `audit_bundle` member of an API/database row. The range and
/// checksum are verified before exposing the logical legacy v1 audit format.
///
/// A native row stores the body without its share window. Rows written
/// before #267 keep `reward_manifest.shares`; rows written since keep only the
/// [`PrismRewardManifestHeader`] and the counted window is rebuilt here from
/// the same immutable shares, through [`restore_reward_manifest`], which
/// refuses a header the rebuild does not reproduce. The canonical digest check
/// at the end then proves the rebuilt body is byte-identical to the one that
/// was signed, whichever shape the row has. Both arms are total: the header
/// type denies a `shares` key, and the full manifest requires one.
///
/// The snapshot digest, the rebuild and the canonical digest are all
/// proportional to the window, hundreds of megabytes at production sizes, so
/// they run on one blocking thread. A `permit` is held by that job itself:
/// Tokio keeps running it after its awaiting caller is dropped, and the permit
/// must bound it anyway. Callers outside the public API pass `None`.
pub async fn materialize_audit_row(
    pool: &PgPool,
    row: &mut Value,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
) -> Result<()> {
    let Some(digest) = row.get("share_snapshot_sha256").and_then(Value::as_str) else {
        return Ok(());
    };
    let digest = digest.to_owned();
    // Snapshots and their share history are immutable, and both hashes below
    // authenticate the reconstruction. Separate checkouts let the public read
    // pool apply the remaining request deadline to each query; retaining one
    // transaction would give a late range scan the first query's full budget.
    let snapshot = sqlx::query("SELECT first_share_seq,last_share_seq,anchor_ms,share_count,inline_shares FROM qbit_prism_audit_snapshots WHERE snapshot_sha256=$1").bind(&digest).fetch_one(pool).await?;
    let inline: Option<Value> = snapshot.try_get("inline_shares")?;
    let shares: Vec<AcceptedShare> = if let Some(inline) = inline {
        serde_json::from_value(inline)?
    } else {
        read_range(
            pool,
            snapshot.try_get("first_share_seq")?,
            snapshot.try_get("last_share_seq")?,
            snapshot.try_get("anchor_ms")?,
        )
        .await?
    };
    ensure!(
        i64::try_from(shares.len())? == snapshot.try_get::<i64, _>("share_count")?,
        "audit share history is incomplete"
    );
    let body = row
        .get_mut("audit_bundle")
        .and_then(Value::as_object_mut)
        .context("audit metadata body is missing")?;
    // The body moves into the blocking job and comes back materialized; the
    // row holds no second copy of it across the await.
    let body = std::mem::take(body);
    let expected = row["audit_bundle_sha256"].as_str().map(str::to_owned);
    let materialized = tokio::task::spawn_blocking(move || -> Result<Value> {
        let _permit = permit;
        ensure!(
            hex::encode(Sha256::digest(serde_json::to_vec(&shares)?)) == digest,
            "audit share snapshot digest mismatch"
        );
        let mut body = body;
        let normalized = body
            .get("reward_manifest")
            .and_then(Value::as_object)
            .is_some_and(|manifest| !manifest.contains_key("shares"));
        if normalized {
            let header: qbit_prism::PrismRewardManifestHeader = serde_json::from_value(
                body.remove("reward_manifest")
                    .context("audit body has no reward manifest")?,
            )?;
            let found_block = FoundBlock::deserialize(
                body.get("found_block")
                    .context("audit body has no found block")?,
            )?;
            let manifest = qbit_prism::restore_reward_manifest(header, &shares, &found_block)?;
            body.insert("reward_manifest".into(), serde_json::to_value(&manifest)?);
        }
        body.insert("shares".into(), serde_json::to_value(shares)?);
        let body = Value::Object(body);
        if let Some(expected) = expected {
            let bundle = AuditBundle::deserialize(&body)?;
            let actual = hex::encode(Sha256::digest(qbit_prism::canonical_audit_bundle_bytes(
                &bundle,
            )?));
            ensure!(
                actual == expected,
                "materialized audit body digest mismatch"
            );
        }
        Ok(body)
    })
    .await??;
    row["audit_bundle"] = materialized;
    Ok(())
}

impl Ledger {
    /// New range-backed bodies, imported canonical bytes, then old inline
    /// bodies. Legacy filesystem bodies are imported by the migration command
    /// or resolved by the public API reader. Imported bytes that fail their
    /// digest or parse are an error, never a fallback to another source.
    pub async fn audit_bundle(&self, block_hash: &str) -> Result<Option<Value>> {
        // Load only the representation that will be served.
        let row = sqlx::query("SELECT audit_bundle_sha256,share_snapshot_sha256,CASE WHEN share_snapshot_sha256 IS NULL THEN canonical_audit_bytes END AS canonical_audit_bytes,CASE WHEN share_snapshot_sha256 IS NOT NULL OR canonical_audit_bytes IS NULL THEN audit_bundle END AS audit_bundle FROM qbit_pool_audit_bundles WHERE block_hash=$1")
            .bind(block_hash)
            .fetch_optional(&self.pool)
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let expected: String = row.try_get("audit_bundle_sha256")?;
        let snapshot: Option<String> = row.try_get("share_snapshot_sha256")?;
        let body: Option<Value> = row.try_get("audit_bundle")?;
        let canonical: Option<Vec<u8>> = row.try_get("canonical_audit_bytes")?;
        // The row still holds its own copy of the bytes; free it before the
        // decode instead of keeping two copies alive across the await.
        drop(row);
        if snapshot.is_some() {
            let mut logical = serde_json::json!({"audit_bundle":body,"audit_bundle_sha256":expected,"share_snapshot_sha256":snapshot});
            materialize_audit_row(&self.pool, &mut logical, None).await?;
            return Ok(Some(logical["audit_bundle"].take()).filter(|body| !body.is_null()));
        }
        if let Some(bytes) = canonical {
            return Ok(Some(
                decode_canonical_audit_body(bytes, expected, None).await?,
            ));
        }
        Ok(body.filter(|body| !body.is_null()))
    }
}

/// The synthetic window an empty ledger pays its solver through. It has no
/// ledger rows, so the durable-range checks exempt it and the snapshot keeps
/// it inline.
fn is_bootstrap_window(shares: &[AcceptedShare]) -> bool {
    shares.len() == 1
        && shares[0].share_id == "bootstrap-share"
        && shares[0].job_id == "bootstrap-job"
}

/// Rows per page of [`verify_durable_range`]. The same page the window reader
/// in `ledger/window.rs` uses; at the ~600 B a decoded production share
/// occupies, one page is about 2.5 MB, so concurrent landings each hold one
/// page of decoded rows, never a second copy of the window.
const VERIFY_PAGE_ROWS: i64 = 4096;

/// Prove the candidate's window is exactly what the immutable ledger holds
/// for its anchored range. This is the read that used to sit inside the
/// settlement transaction; it now runs before the lock is taken, and page by
/// page. The predicate, the `share_seq` ordering and the full equality over
/// every field are unchanged: the pages are compared in order against the
/// same positions of `bundle.shares`, a longer or shorter durable range is a
/// mismatch, and the first mismatched page fails the landing. The bootstrap
/// window has no ledger rows and is exempt, as before. Each page is decoded
/// and compared on a blocking thread.
///
/// The anchored set is frozen once the anchor is issued and ledger rows never
/// change, so nothing this proves can change before the transaction; the
/// in-transaction count guard in [`persist_audit_snapshot`] covers the range
/// again under the lock.
pub(super) async fn verify_durable_range(pool: &PgPool, bundle: &AuditBundle) -> Result<()> {
    let shares = &bundle.shares;
    ensure!(!shares.is_empty(), "audit share snapshot cannot be empty");
    if is_bootstrap_window(shares) {
        return Ok(());
    }
    let first = i64::try_from(shares[0].share_seq)?;
    let last = i64::try_from(shares[shares.len() - 1].share_seq)?;
    let anchor = bundle.found_block.anchor_job_issued_at_ms;
    let mut cursor = first - 1;
    let mut matched = 0usize;
    while cursor < last {
        let rows = sqlx::query(&format!("{SELECT_SHARE} WHERE accepted AND share_seq>$1 AND share_seq<=$2 AND accepted_at<=to_timestamp($3::double precision/1000) AND job_issued_at<=to_timestamp($3::double precision/1000) ORDER BY share_seq LIMIT $4"))
            .bind(cursor).bind(last).bind(anchor).bind(VERIFY_PAGE_ROWS).fetch_all(pool).await?;
        if rows.is_empty() {
            break;
        }
        // The page's counterpart in the candidate window, bounded by the page
        // size. A page that runs past the window is a longer durable range.
        let expected: Vec<AcceptedShare> =
            shares[matched.min(shares.len())..(matched + rows.len()).min(shares.len())].to_vec();
        let page_len = rows.len();
        cursor = tokio::task::spawn_blocking(move || -> Result<i64> {
            let durable = rows
                .iter()
                .map(share_from_row)
                .collect::<Result<Vec<_>>>()?;
            ensure!(
                durable == expected,
                "audit share snapshot differs from canonical database history"
            );
            Ok(i64::try_from(durable[durable.len() - 1].share_seq)?)
        })
        .await??;
        matched += page_len;
    }
    ensure!(
        matched == shares.len(),
        "audit share snapshot differs from canonical database history"
    );
    Ok(())
}

pub(super) async fn persist_audit_snapshot(
    tx: &mut Transaction<'_, Postgres>,
    bundle: &AuditBundle,
) -> Result<String> {
    let shares = &bundle.shares;
    ensure!(!shares.is_empty(), "audit share snapshot cannot be empty");
    ensure!(
        shares.windows(2).all(|s| s[0].share_seq < s[1].share_seq),
        "audit share snapshot must be ordered canonically"
    );
    let digest = hex::encode(Sha256::digest(serde_json::to_vec(shares)?));
    let first = i64::try_from(shares[0].share_seq)?;
    let last = i64::try_from(shares[shares.len() - 1].share_seq)?;
    let anchor = bundle.found_block.anchor_job_issued_at_ms;
    let bootstrap = is_bootstrap_window(shares);
    let inline: Option<Value> = if bootstrap {
        Some(serde_json::to_value(shares)?)
    } else {
        None
    };
    if !bootstrap {
        // The full-equality proof ran before the settlement lock
        // (`verify_durable_range`). Under the lock only the row count of the
        // same anchored predicate is re-read: no share payload, so the lock is
        // not held for a window-sized read.
        let durable: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger WHERE accepted AND share_seq BETWEEN $1 AND $2 AND accepted_at<=to_timestamp($3::double precision/1000) AND job_issued_at<=to_timestamp($3::double precision/1000)")
            .bind(first).bind(last).bind(anchor).fetch_one(&mut **tx).await?;
        ensure!(
            durable == i64::try_from(shares.len())?,
            "audit share snapshot count differs from canonical database history"
        );
    }
    sqlx::query("INSERT INTO qbit_prism_audit_snapshots(snapshot_sha256,first_share_seq,last_share_seq,anchor_ms,share_count,inline_shares) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING")
        .bind(&digest).bind(first).bind(last).bind(anchor).bind(i64::try_from(shares.len())?).bind(inline).execute(&mut **tx).await?;
    Ok(digest)
}

async fn read_range<'e>(
    executor: impl sqlx::Executor<'e, Database = Postgres>,
    first: i64,
    last: i64,
    anchor: i64,
) -> Result<Vec<AcceptedShare>> {
    sqlx::query(&format!("{SELECT_SHARE} WHERE accepted AND share_seq BETWEEN $1 AND $2 AND accepted_at<=to_timestamp($3::double precision/1000) AND job_issued_at<=to_timestamp($3::double precision/1000) ORDER BY share_seq"))
        .bind(first).bind(last).bind(anchor).fetch_all(executor).await?.iter().map(share_from_row).collect()
}
