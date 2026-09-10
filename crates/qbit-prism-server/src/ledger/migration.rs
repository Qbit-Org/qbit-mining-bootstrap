//! Explicit one-time migration of Python filesystem artifacts. Validation is
//! performed before any write; operator files and historical rows are retained.
use super::*;
use std::io::Read;
use std::path::{Path, PathBuf};

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
