use super::*;

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
    materialize_audit_row(pool, &mut logical).await?;
    let bundle: AuditBundle = serde_json::from_value(logical["audit_bundle"].take())?;
    let bytes = qbit_prism::canonical_audit_bundle_bytes(&bundle)?;
    ensure!(
        hex::encode(Sha256::digest(&bytes)) == expected,
        "canonical reconstructed audit digest mismatch"
    );
    Ok(Some(bytes))
}

/// Hydrate the `audit_bundle` member of an API/database row. The range and
/// checksum are verified before exposing the logical legacy v1 audit format.
pub async fn materialize_audit_row(pool: &PgPool, row: &mut Value) -> Result<()> {
    let Some(digest) = row.get("share_snapshot_sha256").and_then(Value::as_str) else {
        return Ok(());
    };
    // Snapshots and their share history are immutable, and both hashes below
    // authenticate the reconstruction. Separate checkouts let the public read
    // pool apply the remaining request deadline to each query; retaining one
    // transaction would give a late range scan the first query's full budget.
    let snapshot = sqlx::query("SELECT first_share_seq,last_share_seq,anchor_ms,share_count,inline_shares FROM qbit_prism_audit_snapshots WHERE snapshot_sha256=$1").bind(digest).fetch_one(pool).await?;
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
    ensure!(
        hex::encode(Sha256::digest(serde_json::to_vec(&shares)?)) == digest,
        "audit share snapshot digest mismatch"
    );
    let bundle = row
        .get_mut("audit_bundle")
        .and_then(Value::as_object_mut)
        .context("audit metadata body is missing")?;
    bundle.insert("shares".into(), serde_json::to_value(shares)?);
    if let Some(expected) = row["audit_bundle_sha256"].as_str() {
        let bundle: AuditBundle = serde_json::from_value(row["audit_bundle"].clone())?;
        let actual = hex::encode(Sha256::digest(qbit_prism::canonical_audit_bundle_bytes(
            &bundle,
        )?));
        ensure!(
            actual == expected,
            "materialized audit body digest mismatch"
        );
    }
    Ok(())
}

impl Ledger {
    /// New range-backed and old inline bodies. Legacy filesystem bodies are
    /// imported by the migration command or resolved by the public API reader.
    pub async fn audit_bundle(&self, block_hash: &str) -> Result<Option<Value>> {
        let row: Option<Value> = sqlx::query_scalar(
            "SELECT jsonb_build_object('audit_bundle',audit_bundle,'audit_bundle_sha256',audit_bundle_sha256,'share_snapshot_sha256',share_snapshot_sha256) FROM qbit_pool_audit_bundles WHERE block_hash=$1",
        )
        .bind(block_hash)
        .fetch_optional(&self.pool)
        .await?;
        let Some(mut row) = row else {
            return Ok(None);
        };
        materialize_audit_row(&self.pool, &mut row).await?;
        Ok(row
            .get("audit_bundle")
            .filter(|body| !body.is_null())
            .cloned())
    }
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
    let bootstrap = shares.len() == 1
        && shares[0].share_id == "bootstrap-share"
        && shares[0].job_id == "bootstrap-job";
    let inline: Option<Value> = if bootstrap {
        Some(serde_json::to_value(shares)?)
    } else {
        None
    };
    if !bootstrap {
        let durable = read_range(&mut **tx, first, last, anchor).await?;
        ensure!(
            &durable == shares,
            "audit share snapshot differs from canonical database history"
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
