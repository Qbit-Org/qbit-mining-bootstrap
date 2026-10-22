use super::*;
use qbit_prism_server::ledger::{CompactPrepared, PreparedTemplate};
use serde_json::json;

pub fn same_job(original: &MiningJob<JobContext>, resumed: &MiningJob<JobContext>) -> Result<()> {
    let (a, b) = (&original.wire, &resumed.wire);
    // clean_jobs and created timestamps are delivery-local. All consensus,
    // payout, entropy and assigned-work inputs remain original.
    let wire_identity = |wire: &qbit_prism_server::codec::Job| {
        json!({
            "id": wire.job_id, "previousblockhash": wire.previousblockhash, "prevhash": wire.prevhash,
            "coinb1": &*wire.coinb1, "coinb2": &*wire.coinb2,
            "full_coinbase_prefix": &*wire.full_coinbase_prefix, "full_coinbase_suffix": &*wire.full_coinbase_suffix,
            "merkle_branch": wire.merkle_branch, "transactions": &*wire.transactions,
            "version": wire.version, "mask": wire.version_mask, "bits": wire.nbits,
            "ntime": wire.ntime, "mintime": wire.mintime,
            "network_target": wire.network_target.to_str_radix(16), "target": wire.share_target.to_str_radix(16),
            "difficulty": wire.share_difficulty, "entropy": wire.extranonce1, "entropy_size": wire.extranonce2_size,
            "generation": wire.refresh_generation, "payout_revision": wire.payout_revision,
        })
    };
    ensure!(
        wire_identity(a) == wire_identity(b),
        "resumed wire inputs changed"
    );
    ensure!(
        serde_json::to_value(&original.context.worker)?
            == serde_json::to_value(&resumed.context.worker)?,
        "original worker identity changed"
    );
    ensure!(
        original.context.prepared.window == resumed.context.prepared.window,
        "original window reference changed"
    );
    ensure!(
        original.context.prepared.inputs == resumed.context.prepared.inputs,
        "original builder inputs changed"
    );
    ensure!(
        serde_json::to_value(&original.context.bootstrap_share)?
            == serde_json::to_value(&resumed.context.bootstrap_share)?,
        "bootstrap share changed"
    );
    ensure!(
        serde_json::to_vec(&*original.context.bundle)?
            == serde_json::to_vec(&*resumed.context.bundle)?,
        "submission metadata changed"
    );
    ensure!(
        b.resume_expires_at.is_some(),
        "resume omitted original expiry"
    );
    Ok(())
}

pub async fn compact_storage(f: &Fixture, job: &MiningJob<JobContext>) -> Result<()> {
    let prepared = &job.context.prepared;
    let payload = f.payload(&prepared.storage_key).await?;
    ensure!(
        payload["format_version"] == CompactPrepared::FORMAT_VERSION,
        "runtime refresh still wrote inline prepared storage: no compact format_version"
    );
    for field in [
        "template",
        "snapshot",
        "bundle",
        "accepted_shares",
        "counted_shares",
    ] {
        ensure!(
            payload.get(field).is_none(),
            "compact prepared embedded {field}"
        );
    }
    fn no_share_arrays(value: &Value) -> bool {
        match value {
            Value::Array(items) => items
                .iter()
                .all(|item| item.get("share_seq").is_none() && no_share_arrays(item)),
            Value::Object(fields) => fields.values().all(no_share_arrays),
            _ => true,
        }
    }
    ensure!(
        no_share_arrays(&payload),
        "prepared contains an embedded share array"
    );
    let issued = f.payload(&job.wire.job_id).await?;
    ensure!(
        no_share_arrays(&issued),
        "issued payload contains an embedded share array"
    );
    ensure!(
        issued["prepared_key"] == prepared.storage_key,
        "issued payload lost its original prepared key"
    );
    let stored =
        f.a.ledger
            .compact_prepared(&prepared.storage_key)
            .await?
            .context("runtime compact record did not pass the public canonical reader")?;
    let record = &stored.record;
    ensure!(
        record.window == prepared.window
            && record.share_seq
                == prepared
                    .window
                    .shares
                    .map_or(0, |range| range.last_share_seq)
            && record.payout_revision == prepared.snapshot.payout_revision,
        "durable original window/watermark/revision changed"
    );
    ensure!(
        record.template_sha256 == PreparedTemplate::encode(&prepared.template)?.sha256(),
        "template digest changed"
    );
    ensure!(
        stored.template == prepared.template,
        "retained template changed"
    );
    ensure!(
        qbit_prism::prior_balances_digest(&stored.prior_balances)
            == prepared.window.prior_balances_digest,
        "retained balances changed"
    );
    ensure!(
        record.payout_policy == prepared.inputs.payout_policy
            && record.ctv == prepared.inputs.ctv
            && record.signer_keys == prepared.inputs.signer_keys
            && record.audit_builder_version == prepared.inputs.audit_builder_version,
        "compact original inputs changed"
    );
    if prepared.window.shares.is_none() {
        ensure!(
            record.audit_hashes.is_none() && record.window.shares.is_none(),
            "bootstrap stored shared worker hashes or shares"
        );
    }
    super::audit::prove_original_audit(f, job, stored).await?;
    let row = sqlx::query("SELECT count(*)::bigint AS writes, max(uncompressed_jsonb_bytes) AS maximum FROM runtime_job_writes WHERE job_id=$1").bind(&prepared.storage_key).fetch_one(f.pool()).await?;
    let writes: i64 = row.try_get("writes")?;
    let maximum: Option<i32> = row.try_get("maximum")?;
    ensure!(writes > 0, "no actual prepared SQL write observed");
    let maximum = maximum.context("prepared JSONB measurement is unknown")?;
    ensure!(
        maximum < 1_000_000,
        "prepared JSONB exceeded the compact bound: {maximum}"
    );
    let shares = prepared.window.shares.map_or(0, |range| range.share_count);
    eprintln!("runtime prepared evidence: writes={writes}, max_uncompressed_jsonb_bytes={maximum}, shares={shares}; functional fixture only");
    Ok(())
}
