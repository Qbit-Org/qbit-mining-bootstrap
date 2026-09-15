//! Additive storage for the coordinated compact-prepared cutover (#273).
//!
//! No runtime caller uses these APIs yet. Before enabling them, drain candidates
//! with compatible pre-007 frontends, stop ALL frontends, and verify zero pending
//! candidates. Apply the coordinated migrations, then start compatible binaries.
//! This is not a rolling old/new-writer contract. The generic job APIs keep
//! their existing inline representation until the caller integration lands.
//!
//! Before enabling blob GC, its transaction must acquire SETTLEMENT_LOCK then
//! ORDER_LOCK before inspecting references and deleting blobs. Prepared writers
//! use the former and candidate balance writers the latter. Retain blobs named
//! by live prepared jobs or any candidate still requiring reconstruction; job
//! expiry or candidate lease expiry alone is not a shared-blob deletion rule.
use super::*;
use qbit_prism::{FanoutFeeRatePolicy, PayoutPolicy};

const FORMAT_VERSION: u16 = 1;
const MAX_PREPARED_BYTES: usize = 1_000_000;

/// Original signed-artifact identities for a nonempty prepared window.
/// Empty-window records have none; bootstrap hashes belong to the worker job.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedAuditHashes {
    pub audit_bundle_sha256: String,
    pub coinbase_manifest_sha256: String,
}

/// Immutable reconstruction inputs, with no template, bundle or share array.
///
/// `payout_revision` is the ORIGINAL revision, never the current transaction
/// fence. `window.anchor_ms` is the original anchor and `share_seq` the original
/// snapshot watermark. Builder/key compatibility and publication/lease
/// eligibility are caller decisions, not storage lookup decisions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactPrepared {
    pub format_version: u16,
    pub window: WindowRef,
    pub share_seq: u64,
    pub payout_revision: i64,
    pub template_sha256: String,
    pub parent_hash: String,
    pub parent_of_tip: String,
    /// Original stable-template work fingerprint from the coordinator; this
    /// is distinct from the frontend/cluster configuration fingerprint fence.
    pub fingerprint: String,
    pub generation: u64,
    pub coinbase_suffix_hex: String,
    pub payout_policy: PayoutPolicy,
    /// Explicit null means no CTV; omission is corruption, as in BundleInputs.
    /// The storage reader also requires an exact canonical payload round-trip.
    #[serde(deserialize_with = "Option::deserialize")]
    pub ctv: Option<CandidateCtv>,
    pub fee: Option<FanoutFeeRatePolicy>,
    pub audit_builder_version: u16,
    pub signer_keys: SignerKeys,
    pub audit_hashes: Option<PreparedAuditHashes>,
}

/// Immutable serialized template. Encoding and hashing are synchronous whole-
/// template work: callers must use their admitted blocking build to create it.
#[derive(Clone, Debug)]
pub struct PreparedTemplate {
    pub(super) bytes: Vec<u8>,
    pub(super) digest: String,
    pub(super) parent: String,
}

impl PreparedTemplate {
    /// Hash the exact `serde_json::to_vec(template)` bytes with SHA-256. This
    /// preserves native number encoding, not the original RPC JSON whitespace.
    /// Neither the digest nor the stored bytes pass through JSONB normalization.
    pub fn encode(template: &Value) -> Result<Self> {
        let parent = template["previousblockhash"]
            .as_str()
            .context("prepared template parent missing")?
            .to_owned();
        let bytes = serde_json::to_vec(template)?;
        let digest = hex::encode(Sha256::digest(&bytes));
        Ok(Self {
            bytes,
            digest,
            parent,
        })
    }

    pub fn sha256(&self) -> &str {
        &self.digest
    }
}

/// A storage observation. Callers still revalidate work authority and absolute
/// expiry after reconstruction and before publishing/submitting miner work.
#[derive(Debug)]
pub struct StoredCompactPrepared {
    pub record: CompactPrepared,
    pub template: Value,
    /// Original stored order, authenticated by the semantic balance digest.
    /// Writes use the shared balance helper's canonical byte encoding.
    pub prior_balances: Vec<CarryForwardBalance>,
    /// Immutable deadline supplied to the original storage reservation. A
    /// retry must reuse this value, even if issued work extended retention.
    pub original_expires_at_ms: i64,
    /// Current dependency retention deadline, which may outlive the original
    /// reservation. This is not an issued job's publication/submit deadline.
    pub expires_at_ms: i64,
}

impl CompactPrepared {
    pub const FORMAT_VERSION: u16 = FORMAT_VERSION;

    pub(super) fn validate(&self) -> Result<()> {
        ensure!(
            self.format_version == FORMAT_VERSION,
            "unsupported compact prepared format"
        );
        ensure!(
            self.payout_revision >= 0,
            "negative prepared payout revision"
        );
        i64::try_from(self.share_seq).context("prepared watermark exceeds SQL bigint")?;
        digest_text(&self.template_sha256)?;
        ensure!(!self.parent_hash.is_empty(), "prepared parent missing");
        ensure!(!self.fingerprint.is_empty(), "prepared fingerprint missing");
        hex::decode(&self.coinbase_suffix_hex).context("invalid prepared coinbase suffix")?;
        ensure!(
            !self.coinbase_suffix_hex.is_empty(),
            "prepared coinbase suffix missing"
        );
        if let Some(range) = self.window.shares {
            ensure!(
                range.first_share_seq >= 1
                    && range.last_share_seq >= range.first_share_seq
                    && range.share_count >= 1
                    && range.share_count <= range.last_share_seq - range.first_share_seq + 1
                    && self.share_seq >= range.last_share_seq,
                "invalid prepared window range"
            );
            i64::try_from(range.last_share_seq).context("prepared range exceeds SQL bigint")?;
        }
        ensure!(
            self.audit_hashes.is_some() == self.window.shares.is_some(),
            "prepared audit identity disagrees with bootstrap window"
        );
        if let Some(hashes) = &self.audit_hashes {
            digest_text(&hashes.audit_bundle_sha256)?;
            digest_text(&hashes.coinbase_manifest_sha256)?;
        }
        if let Some(ctv) = &self.ctv {
            ensure!(
                ctv.fanout_fee_policy == self.fee,
                "prepared CTV fee inputs disagree"
            );
        }
        Ok(())
    }
}

impl Ledger {
    /// Atomically reserve a compact prepared record and its immutable blobs.
    ///
    /// This does not issue a miner job or grant a replacement lease. The caller
    /// authorizes the original identity and supplies a separately revalidated
    /// current revision. Retry with the SAME absolute expiry; no retry renews it.
    /// The original expiry is immutable payload identity. A separate issued
    /// writer may extend the row's retention deadline without changing it.
    /// No timeout is introduced here: the caller owns its operation deadline.
    /// The caller supplies a prepared-dependency key, never an issued-job key;
    /// storage does not define or authenticate the coordinator's key namespace.
    #[allow(clippy::too_many_arguments)]
    pub async fn save_compact_prepared(
        &self,
        key: &str,
        record: &CompactPrepared,
        template: &PreparedTemplate,
        balances: &[CarryForwardBalance],
        expected_current_revision: i64,
        expires_at_ms: i64,
    ) -> Result<bool> {
        ensure!(!key.is_empty(), "prepared storage key missing");
        record.validate()?;
        ensure!(
            record.template_sha256 == template.digest && record.parent_hash == template.parent,
            "prepared template identity mismatch"
        );
        let expires = DateTime::<Utc>::from_timestamp_millis(expires_at_ms)
            .context("prepared expiry out of range")?;
        let owned = record.clone();
        let payload =
            tokio::task::spawn_blocking(move || encode_record(&owned, expires_at_ms)).await??;
        let mut tx = self.begin().await?;
        self.lock(&mut tx, SETTLEMENT_LOCK).await?;
        writable(&mut tx).await?;
        require_revision(&mut tx, expected_current_revision).await?;
        // Match the candidate writer's fence: configure/reset uses FOR UPDATE,
        // so this pin remains valid until the dependent record commits.
        let fingerprint: Option<String> = sqlx::query_scalar(
            "SELECT config_fingerprint FROM qbit_prism_cluster WHERE singleton FOR SHARE",
        )
        .fetch_one(&mut *tx)
        .await?;
        if let Some(pinned) = self.config_fingerprint() {
            ensure!(
                fingerprint.as_deref() == Some(pinned),
                "cluster configuration fingerprint differs from this frontend's pin"
            );
        }
        require_live(&mut tx, expires).await?;
        if let Some(range) = record.window.shares {
            // Prefix-only pruning preserves both endpoints of a valid captured
            // range. Also reject an invented last endpoint beyond the ledger;
            // this is still no substitute for read_window's count/digest checks.
            ensure!(
                probe_share_rows(
                    &mut tx,
                    range.first_share_seq as i64,
                    range.last_share_seq as i64
                )
                .await?,
                "prepared share endpoint missing"
            );
        }
        put_template(&mut tx, template).await?;
        let digest = put_balance_snapshot(&mut tx, balances).await?;
        ensure!(
            digest == record.window.prior_balances_digest,
            "prepared balance digest mismatch"
        );
        let range = record.window.shares;
        let inserted = sqlx::query("INSERT INTO qbit_prism_jobs(job_id,instance_id,parent_hash,payout_revision,payload,expires_at,window_anchor_ms,window_prior_balances_sha256,window_first_share_seq,window_last_share_seq,window_share_count,window_snapshot_sha256,template_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) ON CONFLICT DO NOTHING")
            .bind(key).bind(&self.instance_id).bind(&record.parent_hash).bind(record.payout_revision)
            .bind(&payload).bind(expires).bind(record.window.anchor_ms).bind(hex::encode(digest))
            .bind(range.map(|r| r.first_share_seq as i64)).bind(range.map(|r| r.last_share_seq as i64))
            .bind(range.map(|r| r.share_count as i64)).bind(range.map(|r| hex::encode(r.snapshot_sha256)))
            .bind(&record.template_sha256).execute(&mut *tx).await?.rows_affected() == 1;
        if !inserted {
            // Compare every typed column too: equality of the JSON alone cannot
            // authenticate retention metadata or the immutable dependency.
            let row = sqlx::query("SELECT parent_hash,payout_revision,payload,expires_at,window_anchor_ms,window_prior_balances_sha256,window_first_share_seq,window_last_share_seq,window_share_count,window_snapshot_sha256,template_sha256 FROM qbit_prism_jobs WHERE job_id=$1 FOR KEY SHARE")
                .bind(key).fetch_one(&mut *tx).await?;
            ensure!(
                row.try_get::<Value, _>("payload")? == payload
                    && row.try_get::<DateTime<Utc>, _>("expires_at")? >= expires,
                "immutable compact prepared conflict"
            );
            check_columns(&row, record)?;
        }
        require_live(&mut tx, expires).await?;
        tx.commit().await?;
        Ok(inserted)
    }

    /// Read one live prepared dependency and both blobs in one MVCC statement.
    ///
    /// Only an absent/expired row or an explicitly legacy inline prepared row
    /// is a miss. Partial columns, unknown formats, bad digests and missing
    /// blobs are errors. No fallback to current balances or configuration.
    /// Callers must validate and follow the issued row's prepared-dependency
    /// link before this lookup, not route a client-supplied issued-job ID here.
    /// An issued payload at this key is an error, not a legacy prepared miss.
    pub async fn compact_prepared(&self, key: &str) -> Result<Option<StoredCompactPrepared>> {
        let row = sqlx::query("SELECT j.parent_hash,j.payout_revision,j.payload,j.expires_at,j.window_anchor_ms,j.window_prior_balances_sha256,j.window_first_share_seq,j.window_last_share_seq,j.window_share_count,j.window_snapshot_sha256,j.template_sha256,t.template_bytes,b.balances FROM qbit_prism_jobs j LEFT JOIN qbit_prism_templates t ON t.template_sha256=j.template_sha256 LEFT JOIN qbit_prism_balance_snapshots b ON b.prior_balances_digest=j.window_prior_balances_sha256 WHERE j.job_id=$1 AND j.expires_at>clock_timestamp()")
            .bind(key).fetch_optional(&self.pool).await?;
        let Some(row) = row else {
            return Ok(None);
        };
        // Includes large-template and balance decoding/hashing. Cancellation
        // leaves that owned work on its blocking thread; no new deadline.
        tokio::task::spawn_blocking(move || decode_row(row)).await?
    }
}

pub(super) async fn put_template(
    tx: &mut Transaction<'_, Postgres>,
    template: &PreparedTemplate,
) -> Result<()> {
    let inserted = sqlx::query("INSERT INTO qbit_prism_templates(template_sha256,template_bytes) VALUES($1,$2) ON CONFLICT DO NOTHING")
        .bind(&template.digest).bind(&template.bytes).execute(&mut **tx).await?.rows_affected();
    if inserted == 0 {
        let bytes: Vec<u8> = sqlx::query_scalar(
            "SELECT template_bytes FROM qbit_prism_templates WHERE template_sha256=$1",
        )
        .bind(&template.digest)
        .fetch_one(&mut **tx)
        .await?;
        ensure!(
            bytes == template.bytes,
            "immutable prepared template conflict"
        );
    }
    Ok(())
}

async fn require_live(tx: &mut Transaction<'_, Postgres>, expires: DateTime<Utc>) -> Result<()> {
    let live: bool = sqlx::query_scalar("SELECT $1::timestamptz > clock_timestamp()")
        .bind(expires)
        .fetch_one(&mut **tx)
        .await?;
    ensure!(live, "prepared deadline elapsed");
    Ok(())
}

pub(super) fn encode_record(
    record: &CompactPrepared,
    original_expires_at_ms: i64,
) -> Result<Value> {
    let mut payload = serde_json::to_value(record)?;
    payload["original_expires_at_ms"] = Value::from(original_expires_at_ms);
    let bytes = serde_json::to_vec(&payload)?;
    ensure!(
        bytes.len() < MAX_PREPARED_BYTES,
        "compact prepared payload reaches 1 MB"
    );
    Ok(payload)
}

pub(super) fn digest_text(digest: &str) -> Result<()> {
    ensure!(
        digest.len() == 64
            && digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "prepared digest must be 64 lowercase hex characters"
    );
    Ok(())
}

pub(super) fn check_columns(row: &PgRow, record: &CompactPrepared) -> Result<()> {
    let range = record.window.shares;
    ensure!(
        row.try_get::<String, _>("parent_hash")? == record.parent_hash
            && row.try_get::<i64, _>("payout_revision")? == record.payout_revision
            && row
                .try_get::<Option<String>, _>("template_sha256")?
                .as_deref()
                == Some(&record.template_sha256)
            && row.try_get::<Option<i64>, _>("window_anchor_ms")? == Some(record.window.anchor_ms)
            && row.try_get::<Option<String>, _>("window_prior_balances_sha256")?
                == Some(hex::encode(record.window.prior_balances_digest))
            && row.try_get::<Option<i64>, _>("window_first_share_seq")?
                == range.map(|r| r.first_share_seq as i64)
            && row.try_get::<Option<i64>, _>("window_last_share_seq")?
                == range.map(|r| r.last_share_seq as i64)
            && row.try_get::<Option<i64>, _>("window_share_count")?
                == range.map(|r| r.share_count as i64)
            && row.try_get::<Option<String>, _>("window_snapshot_sha256")?
                == range.map(|r| hex::encode(r.snapshot_sha256)),
        "compact prepared payload/column mismatch"
    );
    Ok(())
}

fn decode_row(row: PgRow) -> Result<Option<StoredCompactPrepared>> {
    let payload: Value = row.try_get("payload")?;
    let no_reference = row.try_get::<Option<i64>, _>("window_anchor_ms")?.is_none()
        && row
            .try_get::<Option<String>, _>("window_prior_balances_sha256")?
            .is_none()
        && row
            .try_get::<Option<i64>, _>("window_first_share_seq")?
            .is_none()
        && row
            .try_get::<Option<i64>, _>("window_last_share_seq")?
            .is_none()
        && row
            .try_get::<Option<i64>, _>("window_share_count")?
            .is_none()
        && row
            .try_get::<Option<String>, _>("window_snapshot_sha256")?
            .is_none()
        && row
            .try_get::<Option<String>, _>("template_sha256")?
            .is_none();
    if no_reference
        && payload.get("format_version").is_none()
        && payload.get("window").is_none()
        && payload.get("template_sha256").is_none()
        && payload.get("original_expires_at_ms").is_none()
        && payload.get("snapshot").is_some()
        && payload.get("template").is_some()
    {
        return Ok(None);
    }
    let original_expires_at_ms = payload["original_expires_at_ms"]
        .as_i64()
        .context("invalid compact prepared payload: original expiry missing or invalid")?;
    let original_expires = DateTime::<Utc>::from_timestamp_millis(original_expires_at_ms)
        .context("prepared original expiry out of range")?;
    let retained_until: DateTime<Utc> = row.try_get("expires_at")?;
    ensure!(
        retained_until >= original_expires,
        "prepared retention precedes original expiry"
    );
    let mut inputs = payload.clone();
    inputs
        .as_object_mut()
        .context("invalid compact prepared payload")?
        .remove("original_expires_at_ms");
    let record: CompactPrepared =
        serde_json::from_value(inputs).context("invalid compact prepared payload")?;
    record.validate()?;
    ensure!(
        encode_record(&record, original_expires_at_ms)? == payload,
        "noncanonical compact prepared payload"
    );
    check_columns(&row, &record)?;
    let template_bytes: Option<Vec<u8>> = row.try_get("template_bytes")?;
    let template_bytes = template_bytes.context("prepared template blob missing")?;
    ensure!(
        hex::encode(Sha256::digest(&template_bytes)) == record.template_sha256,
        "prepared template digest mismatch"
    );
    let template: Value =
        serde_json::from_slice(&template_bytes).context("invalid prepared template")?;
    ensure!(
        template["previousblockhash"].as_str() == Some(&record.parent_hash),
        "prepared template parent mismatch"
    );
    let balance_bytes: Option<Vec<u8>> = row.try_get("balances")?;
    let balance_bytes = balance_bytes.context("prepared balance snapshot missing")?;
    let prior_balances: Vec<CarryForwardBalance> =
        serde_json::from_slice(&balance_bytes).context("invalid prepared balance snapshot")?;
    ensure!(
        qbit_prism::prior_balances_digest(&prior_balances) == record.window.prior_balances_digest,
        "prepared balance snapshot digest mismatch"
    );
    Ok(Some(StoredCompactPrepared {
        record,
        template,
        prior_balances,
        original_expires_at_ms,
        expires_at_ms: retained_until.timestamp_millis(),
    }))
}
