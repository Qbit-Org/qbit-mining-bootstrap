//! Explicit one-time migration of Python filesystem artifacts. Validation is
//! performed before any write; operator files and historical rows are retained.
use super::*;
use std::path::{Path, PathBuf};

impl Ledger {
    pub async fn import_legacy_audits(
        &self,
        root_dir: Option<&Path>,
        ledger_key: &str,
    ) -> Result<usize> {
        let rows = sqlx::query("SELECT block_hash,body_uri,audit_bundle_sha256,coinbase_tx_hex FROM qbit_pool_audit_bundles WHERE audit_bundle IS NULL AND body_uri IS NOT NULL ORDER BY created_at,block_hash").fetch_all(&self.pool).await?;
        let mut imported = 0;
        for row in rows {
            let hash: String = row.try_get("block_hash")?;
            let uri: String = row.try_get("body_uri")?;
            let expected_digest: String = row.try_get("audit_bundle_sha256")?;
            let coinbase: String = row.try_get("coinbase_tx_hex")?;
            let path = resolve_import_path(root_dir, &uri)?;
            let key = ledger_key.to_owned();
            let bundle = tokio::task::spawn_blocking(move || -> Result<AuditBundle> {
                let bundle = qbit_prism::load_audit_bundle_from_path(&path)?;
                let report = qbit_prism::verify_audit_bundle_against_coinbase_tx_hex(
                    &bundle, &coinbase, &key,
                )?;
                ensure!(
                    report.audit_bundle_sha256_hex == expected_digest,
                    "legacy audit digest mismatch at {}",
                    path.display()
                );
                Ok(bundle)
            })
            .await??;
            let value = serde_json::to_value(&bundle)?;
            let mut tx = self.pool.begin().await?;
            lock(&mut tx, SETTLEMENT_LOCK).await?;
            writable(&mut tx).await?;
            // Retain legacy inline shape on import, including valid historical
            // snapshots whose ledger history was archived before Rust cutover.
            // Newly mined bodies use the normalized range-backed representation.
            let updated = sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=$2,schema_version=$3,found_block_network_difficulty=$4::text::numeric,found_block_coinbase_value_sats=$5,audit_commitment_leaves_hex=$6,witness_merkle_leaves_hex=$7 WHERE block_hash=$1 AND audit_bundle IS NULL AND body_uri=$8 AND audit_bundle_sha256=$9")
                .bind(&hash).bind(value).bind(&bundle.schema).bind(bundle.found_block.network_difficulty.to_string()).bind(i64::try_from(bundle.found_block.coinbase_value_sats)?)
                .bind(serde_json::to_value(&bundle.audit_commitment_leaves_hex)?).bind(serde_json::to_value(&bundle.witness_merkle_leaves_hex)?).bind(&uri).bind(row.try_get::<String,_>("audit_bundle_sha256")?).execute(&mut *tx).await?.rows_affected();
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
