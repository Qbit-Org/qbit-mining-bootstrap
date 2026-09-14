//! The block-candidate outbox after migration 007 (#265): a slim document that
//! references the payout window instead of copying it, an insert of prepared
//! bytes under `ORDER_LOCK`, and a claim that authenticates the row before any
//! window is read.
use super::blocks::compact_size;
use super::*;
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{
    AuditBundleBody, FanoutFeeRatePolicy, FoundBlock, PayoutPolicy, SettlementModeConfig,
};
use std::sync::Arc;

/// The public keys the building frontend signed with. The seeds stay local;
/// these are stored so a claim can refuse to rebuild under other keys.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignerKeys {
    pub manifest_key_hex: String,
    pub ledger_key_hex: String,
}

impl SignerKeys {
    /// The public keys of a pair of signing seeds.
    pub fn of(manifest_key: &ManifestSigningKey, ledger_key: &ManifestSigningKey) -> Self {
        Self {
            manifest_key_hex: manifest_key.public_key_hex(),
            ledger_key_hex: ledger_key.public_key_hex(),
        }
    }

    fn matches(&self, other: &SignerKeys) -> bool {
        self.manifest_key_hex
            .eq_ignore_ascii_case(&other.manifest_key_hex)
            && self
                .ledger_key_hex
                .eq_ignore_ascii_case(&other.ledger_key_hex)
    }
}

/// The CTV settlement inputs a job was built with, stored verbatim. Its
/// presence is what selects the CTV builder at a rebuild, in place of the
/// building frontend's `ctv_enabled` flag.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateCtv {
    pub direct_floor_sats: u64,
    pub settlement_config: SettlementModeConfig,
    #[serde(default)]
    pub fanout_fee_policy: Option<FanoutFeeRatePolicy>,
}

/// A found block waiting in `qbit_block_candidate_outbox`.
///
/// The serialized form is the row's `candidate` JSONB document, which
/// `candidate_sha256` covers. It holds the window **reference** and every
/// builder input except the window, all O(1), so the row stays small however
/// large the payout window is. The one input that scales with anything is the
/// block, and it scales with consensus, not the window: it is kept out of the
/// document, in the `block_bytes` column, and authenticated by `block_sha256`.
///
/// A rebuild reads no local configuration for any field that reaches the
/// signed bundle: `found_block`, `payout_policy`, `ctv`, `bootstrap_share`,
/// `coinbase_suffix_hex`, `audit_builder_version` and `signer_keys` all come
/// from here; the shares come from `Ledger::read_window` and the witness
/// leaves from the block itself.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Candidate {
    pub block_hash: String,
    /// SHA-256 of `block_bytes`, lowercase hex. Covered by `candidate_sha256`,
    /// so the `bytea` column is authenticated through the document.
    pub block_sha256: String,
    pub job_id: String,
    /// The revision the job was issued at; the claim's fence, never replaced
    /// by the revision a window read observes.
    pub payout_revision: i64,
    pub window: WindowRef,
    /// The synthetic share a candidate found on an empty window was built
    /// over. Present exactly when `window.shares` is `None`; it is never in
    /// the ledger, so it cannot be referenced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bootstrap_share: Option<AcceptedShare>,
    pub found_block: FoundBlock,
    pub payout_policy: PayoutPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ctv: Option<CandidateCtv>,
    pub audit_builder_version: u16,
    pub signer_keys: SignerKeys,
    /// Set by B's replacement lease (#273) when the lease covered the work the
    /// block was found on. A leased candidate is submitted before any
    /// terminal disposition and rebuilt only from its as-issued balances.
    #[serde(default)]
    pub leased: bool,
    pub coinbase_suffix_hex: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deferred_share: Option<AcceptedShare>,
    /// The assembled block. Never part of the document: it goes to the
    /// `block_bytes` column, and a claim fills it from there.
    #[serde(skip)]
    pub block_bytes: Vec<u8>,
    /// The as-issued prior balances a `leased` enqueue writes back to
    /// `qbit_prism_balance_snapshots`, digest-checked against
    /// `window.prior_balances_digest`. Never part of the document, unused
    /// when `leased` is false, and empty on a claimed candidate.
    #[serde(skip)]
    pub as_issued_balances: Vec<CarryForwardBalance>,
}

impl Candidate {
    /// `block_sha256` for `block_bytes`: the value the document must carry.
    pub fn block_digest_hex(block_bytes: &[u8]) -> String {
        hex::encode(Sha256::digest(block_bytes))
    }
}

/// The rebuilt audit, as parts: the body the borrowing builder produced and
/// the window it was built over. Landing verifies and stores these and never
/// assembles an `AuditBundle`, so the window exists once, inside the `Arc`.
#[derive(Clone, Debug)]
pub struct ClaimParts {
    pub body: Arc<AuditBundleBody>,
    pub shares: Arc<Vec<AcceptedShare>>,
}

#[derive(Clone, Debug)]
pub struct CandidateClaim {
    pub candidate: Candidate,
    pub claim_token: String,
    /// `None` until the claim's rebuild fills it. Landing reads the parts
    /// here; a claim without them cannot land.
    pub parts: Option<ClaimParts>,
}

impl CandidateClaim {
    pub fn with_parts(mut self, parts: ClaimParts) -> Self {
        self.parts = Some(parts);
        self
    }

    /// The claim with the parts of an already-built bundle: the bundle is
    /// split, never copied, and nothing assembles one again. For callers
    /// that hold the bundle the candidate was found on, such as tests that
    /// land through `Ledger::land_candidate` without the coordinator.
    pub fn with_bundle(self, bundle: AuditBundle) -> Self {
        let (body, shares) = bundle.into_parts();
        self.with_parts(ClaimParts {
            body: Arc::new(body),
            shares: Arc::new(shares),
        })
    }
}

/// Rebuild a candidate's audit parts from its stored inputs and the window
/// [`Ledger::read_window`] returned.
///
/// **Synchronous, whole-window work**: the builder walks every share, so a
/// caller runs this inside its own `spawn_blocking`, under the `build_slots`
/// permit it holds. It never calls the coordinator's `build_bundle`, which
/// takes a permit of its own and reads configuration for signed fields, and
/// it reads no configuration itself: the shares are `window`'s (or the stored
/// bootstrap share for an empty window), the witness leaves come from the
/// block, and every other input is the candidate's.
///
/// `manifest_key` and `ledger_key` are the caller's local seeds, the signer;
/// they must be the pair the candidate stores, or the rebuilt bytes could not
/// match the coinbase the block commits to.
pub fn build_claim_parts(
    candidate: &Candidate,
    shares: Vec<AcceptedShare>,
    prior_balances: Vec<CarryForwardBalance>,
    manifest_key: &ManifestSigningKey,
    ledger_key: &ManifestSigningKey,
) -> Result<ClaimParts> {
    let local = SignerKeys::of(manifest_key, ledger_key);
    ensure!(
        candidate.signer_keys.matches(&local),
        "candidate {} was signed with keys {:?}, not this frontend's {:?}; it must not be rebuilt here",
        candidate.block_hash,
        candidate.signer_keys,
        local
    );
    let shares = match (&candidate.window.shares, &candidate.bootstrap_share) {
        (None, Some(bootstrap)) => {
            ensure!(
                shares.is_empty(),
                "an empty-window candidate was given {} ledger shares",
                shares.len()
            );
            vec![bootstrap.clone()]
        }
        (Some(range), None) => {
            ensure!(
                u64::try_from(shares.len())? == range.share_count,
                "candidate window holds {} shares, its reference names {}",
                shares.len(),
                range.share_count
            );
            shares
        }
        _ => bail!("candidate bootstrap share disagrees with its window reference"),
    };
    let witnesses = crate::codec::witness_merkle_leaves_from_block(&candidate.block_bytes)?;
    let body = if let Some(ctv) = &candidate.ctv {
        qbit_prism::build_audit_bundle_body_with_ctv_settlement_options(
            &shares,
            candidate.found_block.clone(),
            prior_balances,
            candidate.payout_policy.clone(),
            ctv.direct_floor_sats,
            ctv.settlement_config,
            ctv.fanout_fee_policy,
            Some(candidate.coinbase_suffix_hex.clone()),
            witnesses,
            manifest_key,
            ledger_key,
        )?
    } else {
        qbit_prism::build_audit_bundle_body_with_coinbase_options(
            &shares,
            candidate.found_block.clone(),
            prior_balances,
            candidate.payout_policy.clone(),
            Some(candidate.coinbase_suffix_hex.clone()),
            witnesses,
            manifest_key,
            ledger_key,
        )?
    };
    Ok(ClaimParts {
        body: Arc::new(body),
        shares: Arc::new(shares),
    })
}

/// A candidate serialized, digested and checked **before** any transaction
/// opens, so the append and enqueue transactions insert prepared bytes only.
pub(super) struct PreparedCandidate<'a> {
    candidate: &'a Candidate,
    document: Value,
    sha256: String,
    deferred: Option<(Value, String)>,
}

/// Serialize, digest and validate a candidate. Runs on the caller's thread
/// before it opens its transaction: the document is O(1) and the only
/// scaling input is the block, whose digest consensus bounds.
pub(super) fn prepare_candidate(candidate: &Candidate) -> Result<PreparedCandidate<'_>> {
    let block = &candidate.block_bytes;
    ensure!(block.len() > 80, "candidate block is truncated");
    let mut hash = Sha256::digest(Sha256::digest(&block[..80])).to_vec();
    hash.reverse();
    ensure!(
        hex::encode(hash) == candidate.block_hash,
        "candidate header hash mismatch"
    );
    ensure!(
        Candidate::block_digest_hex(block) == candidate.block_sha256,
        "candidate block digest mismatch"
    );
    ensure!(
        !candidate.coinbase_suffix_hex.is_empty()
            && hex::decode(&candidate.coinbase_suffix_hex).is_ok(),
        "candidate coinbase suffix must be non-empty hex"
    );
    check_reference_invariants(candidate)?;
    if candidate.leased {
        let mut balances = candidate.as_issued_balances.clone();
        balances.sort_by(|a, b| {
            a.order_key
                .cmp(&b.order_key)
                .then_with(|| a.recipient_id.cmp(&b.recipient_id))
                .then_with(|| a.p2mr_program_hex.cmp(&b.p2mr_program_hex))
        });
        ensure!(
            qbit_prism::prior_balances_digest(&balances) == candidate.window.prior_balances_digest,
            "leased candidate's as-issued balances do not hash to its window reference"
        );
    }
    let document = serde_json::to_value(candidate)?;
    let sha256 = hex::encode(Sha256::digest(serde_json::to_vec(candidate)?));
    let deferred = candidate
        .deferred_share
        .as_ref()
        .map(|share| {
            Ok::<_, anyhow::Error>((
                serde_json::to_value(share)?,
                hex::encode(Sha256::digest(serde_json::to_vec(share)?)),
            ))
        })
        .transpose()?;
    Ok(PreparedCandidate {
        candidate,
        document,
        sha256,
        deferred,
    })
}

/// The two invariants that tie the stored inputs to the reference, checked at
/// enqueue and again at claim decode.
fn check_reference_invariants(candidate: &Candidate) -> Result<()> {
    ensure!(
        candidate.bootstrap_share.is_some() == candidate.window.shares.is_none(),
        "candidate bootstrap share disagrees with its window reference"
    );
    if let Some(share) = &candidate.bootstrap_share {
        ensure!(
            share.share_id == "bootstrap-share" && share.job_id == "bootstrap-job",
            "candidate bootstrap share is not the synthetic bootstrap share"
        );
    }
    ensure!(
        candidate.found_block.anchor_job_issued_at_ms == candidate.window.anchor_ms,
        "candidate found block anchor disagrees with its window reference"
    );
    Ok(())
}

impl Ledger {
    pub async fn enqueue_candidate(&self, candidate: Candidate) -> Result<()> {
        self.enqueue_candidate_once(candidate).await.map(|_| ())
    }

    /// Enqueue a block-only candidate. The row is serialized and digested
    /// before the transaction opens; under `ORDER_LOCK` the transaction only
    /// runs the writer fence, re-establishes what the candidate references
    /// and inserts the prepared bytes.
    pub async fn enqueue_candidate_once(&self, candidate: Candidate) -> Result<bool> {
        let prepared = prepare_candidate(&candidate)?;
        let mut tx = self.pool.begin().await?;
        lock(&mut tx, ORDER_LOCK).await?;
        writable(&mut tx).await?;
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM qbit_block_candidate_outbox WHERE block_hash=$1)",
        )
        .bind(&candidate.block_hash)
        .fetch_one(&mut *tx)
        .await?;
        if exists {
            tx.commit().await?;
            return Ok(false);
        }
        let inserted = self
            .persist_prepared_candidate(&mut tx, &prepared, None)
            .await?;
        tx.commit().await?;
        Ok(inserted)
    }

    /// Insert a prepared candidate inside the caller's `ORDER_LOCK`
    /// transaction. In order: the writer fence, the `leased` balance
    /// snapshot, the window prefix probe, then the insert of the prepared
    /// bytes. Nothing here serializes or digests the candidate.
    pub(super) async fn persist_prepared_candidate(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        prepared: &PreparedCandidate<'_>,
        share_id: Option<&str>,
    ) -> Result<bool> {
        let candidate = prepared.candidate;
        // The writer fence. `FOR SHARE` conflicts with `configure`'s `FOR
        // UPDATE`, so a fingerprint reset and this write cannot interleave:
        // either the write commits first and the reset's refusal sees it, or
        // the reset does and this compares against the new value. A frontend
        // that never pinned a fingerprint (a bare `Ledger` in tests and
        // tooling) has nothing to fence against and still takes the lock.
        let stored: Option<String> = sqlx::query_scalar(
            "SELECT config_fingerprint FROM qbit_prism_cluster WHERE singleton FOR SHARE",
        )
        .fetch_one(&mut **tx)
        .await?;
        if let Some(local) = self.config_fingerprint() {
            if stored.as_deref() != Some(local) {
                let stored = stored.as_deref().unwrap_or("NULL (reset)");
                tracing::error!(
                    block = %candidate.block_hash,
                    stored_fingerprint = stored,
                    local_fingerprint = local,
                    "ALERT: cluster configuration fingerprint is not this frontend's; refusing to enqueue the candidate, stop this frontend"
                );
                bail!(
                    "cluster configuration fingerprint {stored} is not this frontend's pinned {local}; refusing to enqueue candidate {}",
                    candidate.block_hash
                );
            }
        }
        if candidate.leased {
            // A prune may have removed the job row and its balance snapshot
            // between the submission's expiry check and this lock; the
            // as-issued set the job carried is written back, or found already
            // present, so the leased claim can read `AsIssued`.
            let digest = put_balance_snapshot(tx, &candidate.as_issued_balances).await?;
            ensure!(
                digest == candidate.window.prior_balances_digest,
                "leased candidate's as-issued balances do not hash to its window reference"
            );
        }
        if let Some(range) = candidate.window.shares {
            let first = i64::try_from(range.first_share_seq)?;
            // Retention removes only a prefix, so the first row's presence
            // means the whole range is present and the committed row then
            // holds the retention floor. Shares cannot be written back, so a
            // failed probe is not repaired: the block must still reach the
            // node, and the claim then meets `Incomplete`, which #268 recovers.
            if !probe_share_rows(tx, first, first).await? {
                tracing::error!(
                    block = %candidate.block_hash,
                    first_share_seq = first,
                    "ALERT: the candidate's window prefix row is missing; enqueueing anyway so the block reaches the node, its claim will fail to rebuild"
                );
            }
        }
        let (first, last, count, snapshot) = match candidate.window.shares {
            Some(range) => (
                Some(i64::try_from(range.first_share_seq)?),
                Some(i64::try_from(range.last_share_seq)?),
                Some(i64::try_from(range.share_count)?),
                Some(hex::encode(range.snapshot_sha256)),
            ),
            None => (None, None, None, None),
        };
        let inserted = sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,share_id,candidate,candidate_sha256,block_bytes,window_anchor_ms,window_prior_balances_sha256,window_first_share_seq,window_last_share_seq,window_share_count,window_snapshot_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT(block_hash) DO NOTHING")
            .bind(&candidate.block_hash).bind(share_id).bind(&prepared.document).bind(&prepared.sha256).bind(&candidate.block_bytes)
            .bind(candidate.window.anchor_ms).bind(hex::encode(candidate.window.prior_balances_digest)).bind(first).bind(last).bind(count).bind(snapshot)
            .execute(&mut **tx).await?.rows_affected();
        if inserted == 0 {
            let same: bool = sqlx::query_scalar("SELECT candidate_sha256=$2 AND share_id IS NOT DISTINCT FROM $3 FROM qbit_block_candidate_outbox WHERE block_hash=$1").bind(&candidate.block_hash).bind(&prepared.sha256).bind(share_id).fetch_one(&mut **tx).await?;
            ensure!(same, "candidate identity conflict");
        }
        if let Some((payload, digest)) = &prepared.deferred {
            sqlx::query("INSERT INTO qbit_prism_deferred_shares(block_hash,share,share_sha256) VALUES($1,$2,$3) ON CONFLICT DO NOTHING")
                .bind(&candidate.block_hash).bind(payload).bind(digest).execute(&mut **tx).await?;
            let same:bool = sqlx::query_scalar("SELECT share=$2 AND share_sha256=$3 FROM qbit_prism_deferred_shares WHERE block_hash=$1")
                .bind(&candidate.block_hash).bind(payload).bind(digest).fetch_one(&mut **tx).await?;
            ensure!(same, "deferred share identity conflict");
        }
        Ok(inserted == 1)
    }

    pub async fn claim_candidate(&self, lease_seconds: i64) -> Result<Option<CandidateClaim>> {
        ensure!(
            (1..=600).contains(&lease_seconds),
            "invalid candidate lease duration"
        );
        let token = Uuid::new_v4().to_string();
        let mut tx = self.pool.begin().await?;
        writable(&mut tx).await?;
        // Empty polling does not use a scheduling slot. A racing SKIP LOCKED
        // selection can still leave a gap; this weighting is deliberately an
        // approximate service ratio rather than a global serialization point.
        let slot: Option<i64> = sqlx::query_scalar("SELECT nextval('qbit_prism_candidate_dispatch_sequence') WHERE EXISTS(SELECT 1 FROM qbit_block_candidate_outbox WHERE state='pending' AND next_attempt_at<=clock_timestamp() AND (claim_expires_at IS NULL OR claim_expires_at<=clock_timestamp()))")
            .fetch_optional(&mut *tx).await?;
        let mut row = None;
        if let Some(slot) = slot {
            if slot % 8 != 0 {
                row = claim_candidate_lane(&mut tx, true, &token, &self.instance_id, lease_seconds)
                    .await?;
            }
            if row.is_none() {
                row =
                    claim_candidate_lane(&mut tx, false, &token, &self.instance_id, lease_seconds)
                        .await?;
            }
        }
        tx.commit().await?;
        let Some(row) = row else {
            return Ok(None);
        };
        // The document is O(1), but the block digest scales with the block,
        // so the decode stays off the runtime thread that renews leases.
        tokio::task::spawn_blocking(move || decode_claimed_row(&row, token))
            .await?
            .map(Some)
    }

    /// The landed audit row for a block, if an earlier claim already landed
    /// it. Read-only and O(1): it never reads the window or the body, so it
    /// can run on the submit loop. The caller authenticates it against the
    /// candidate's block with [`authenticate_landed_audit`].
    pub async fn landed_audit(&self, block_hash: &str) -> Result<Option<LandedAudit>> {
        let row = sqlx::query("SELECT coinbase_tx_hex,audit_commitment_leaves_hex,share_snapshot_sha256,found_block_bits FROM qbit_pool_audit_bundles WHERE block_hash=$1")
            .bind(block_hash).fetch_optional(&self.pool).await?;
        row.map(|row| {
            let leaves: Option<Value> = row.try_get("audit_commitment_leaves_hex")?;
            Ok(LandedAudit {
                coinbase_tx_hex: row.try_get("coinbase_tx_hex")?,
                audit_commitment_leaves_hex: leaves
                    .map(serde_json::from_value)
                    .transpose()?
                    .unwrap_or_default(),
                share_snapshot_sha256: row.try_get("share_snapshot_sha256")?,
                found_block_bits: row.try_get("found_block_bits")?,
            })
        })
        .transpose()
    }

    /// Record the header's compact bits on a landed audit row that predates
    /// the column, exactly as an idempotent landing does.
    pub async fn record_landed_audit_bits(&self, block_hash: &str, bits: &str) -> Result<()> {
        sqlx::query("UPDATE qbit_pool_audit_bundles SET found_block_bits=$2 WHERE block_hash=$1 AND found_block_bits IS NULL")
            .bind(block_hash).bind(bits).execute(&self.pool).await?;
        Ok(())
    }

    /// Keep a live processing attempt owned while it waits for build capacity
    /// or performs expensive verification. Expired tokens never revive.
    pub async fn renew_candidate_claim(
        &self,
        claim: &CandidateClaim,
        lease_seconds: i64,
    ) -> Result<()> {
        ensure!(
            (1..=600).contains(&lease_seconds),
            "invalid candidate lease duration"
        );
        let mut tx = self.pool.begin().await?;
        writable(&mut tx).await?;
        // Evaluate expiry after obtaining the row lock: a blocked UPDATE can
        // otherwise have matched a live token before waiting past its expiry.
        // NO KEY UPDATE is compatible with the processing transaction's KEY
        // SHARE lock, so audit persistence cannot block its own heartbeat.
        sqlx::query("SELECT block_hash FROM qbit_block_candidate_outbox WHERE block_hash=$1 FOR NO KEY UPDATE")
            .bind(&claim.candidate.block_hash).fetch_optional(&mut *tx).await?;
        let updated = sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_expires_at=clock_timestamp()+$3*interval '1 second',updated_at=clock_timestamp() WHERE block_hash=$1 AND claim_token=$2 AND state='pending' AND claim_expires_at>clock_timestamp()")
            .bind(&claim.candidate.block_hash).bind(&claim.claim_token).bind(lease_seconds).execute(&mut *tx).await?.rows_affected();
        ensure!(updated == 1, "candidate claim was lost or expired");
        tx.commit().await?;
        Ok(())
    }

    pub async fn retry_candidate(&self, claim: &CandidateClaim, error: &str) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        writable(&mut tx).await?;
        lock_candidate_row(&mut tx, claim).await?;
        let result = sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_token=NULL,claim_instance_id=NULL,claim_expires_at=NULL,last_error=$3,next_attempt_at=clock_timestamp()+LEAST(60,attempt_count)*interval '1 second',updated_at=clock_timestamp() WHERE block_hash=$1 AND claim_token=$2 AND state='pending' AND claim_expires_at>clock_timestamp()")
            .bind(&claim.candidate.block_hash).bind(&claim.claim_token).bind(error).execute(&mut *tx).await?;
        ensure!(
            result.rows_affected() == 1,
            "candidate claim was lost or expired"
        );
        tx.commit().await?;
        Ok(())
    }

    pub async fn candidate_revision_valid(&self, candidate: &Candidate) -> Result<bool> {
        Ok(candidate.payout_revision == self.payout_revision().await?)
    }
}

/// The columns of a landed `qbit_pool_audit_bundles` row a recovered claim
/// authenticates against its block, every check O(1).
#[derive(Clone, Debug)]
pub struct LandedAudit {
    pub coinbase_tx_hex: String,
    pub audit_commitment_leaves_hex: Vec<String>,
    pub share_snapshot_sha256: Option<String>,
    pub found_block_bits: Option<String>,
}

/// The compact bits of a block's header, as `qbit_pool_audit_bundles` stores
/// them.
pub fn header_bits_hex(block: &[u8]) -> Result<String> {
    let bits = block.get(72..76).context("truncated block header")?;
    Ok(format!(
        "{:08x}",
        u32::from_le_bytes(bits.try_into().expect("four bytes"))
    ))
}

/// Authenticate a landed audit row against the block the candidate found,
/// instead of a stored digest: the candidate keeps no expected
/// `audit_bundle_sha256`, because computing one at submit would serialize the
/// whole bundle on the share path. The block is the expectation:
///
/// * the landed `coinbase_tx_hex` is exactly the block's first transaction,
///   parsed segwit-aware;
/// * the audit commitment root over the landed leaves equals that coinbase's
///   witness reserved value, which the builder sets to the root;
/// * `share_snapshot_sha256` equals the reference's `snapshot_sha256`, or
///   the digest `persist_audit_snapshot` writes for the stored bootstrap
///   share when the window is empty;
/// * `found_block_bits`, when recorded, matches the header.
///
/// Only then may a claim treat landing as done. It still never finishes only
/// because an audit exists: it continues to observe, renew and submit.
pub fn authenticate_landed_audit(candidate: &Candidate, landed: &LandedAudit) -> Result<()> {
    let block = &candidate.block_bytes;
    ensure!(block.len() > 80, "candidate block is truncated");
    let (tx_count, count_bytes) = compact_size(&block[80..])?;
    ensure!(tx_count > 0, "candidate has no coinbase");
    let coinbase = hex::decode(&landed.coinbase_tx_hex)?;
    let start = 80 + count_bytes;
    ensure!(
        block.get(start..start + coinbase.len()) == Some(coinbase.as_slice()),
        "landed audit coinbase differs from the candidate's block"
    );
    let reserved = coinbase_witness_reserved_value(&coinbase)?;
    let root = qbit_prism::audit_commitment_root_hex(&landed.audit_commitment_leaves_hex)?;
    ensure!(
        root.eq_ignore_ascii_case(&hex::encode(reserved)),
        "landed audit commitment root {root} is not the block's coinbase witness reserved value {}",
        hex::encode(reserved)
    );
    let expected = match (candidate.window.shares, &candidate.bootstrap_share) {
        (Some(range), _) => hex::encode(range.snapshot_sha256),
        (None, Some(share)) => hex::encode(Sha256::digest(serde_json::to_vec(
            std::slice::from_ref(share),
        )?)),
        (None, None) => bail!("candidate bootstrap share disagrees with its window reference"),
    };
    ensure!(
        landed.share_snapshot_sha256.as_deref() == Some(expected.as_str()),
        "landed audit share snapshot {:?} is not the candidate's window {expected}",
        landed.share_snapshot_sha256
    );
    let bits = header_bits_hex(block)?;
    if let Some(stored) = &landed.found_block_bits {
        ensure!(
            stored.eq_ignore_ascii_case(&bits),
            "landed audit bits {stored} differ from the candidate header's {bits}"
        );
    }
    Ok(())
}

/// The witness reserved value of a coinbase: the single item of its only
/// input's witness stack. The whole transaction is walked, segwit-aware, and
/// must end where the bytes end.
pub fn coinbase_witness_reserved_value(tx: &[u8]) -> Result<[u8; 32]> {
    let at = |offset: usize, len: usize| -> Result<&[u8]> {
        tx.get(
            offset
                ..offset
                    .checked_add(len)
                    .context("coinbase length overflow")?,
        )
        .context("truncated coinbase transaction")
    };
    let count = |offset: usize| -> Result<(usize, usize)> {
        let (value, len) =
            compact_size(tx.get(offset..).context("truncated coinbase transaction")?)?;
        Ok((usize::try_from(value)?, len))
    };
    ensure!(
        at(4, 2)? == [0, 1],
        "landed coinbase is not a witness transaction"
    );
    let mut offset = 6;
    let (inputs, len) = count(offset)?;
    offset += len;
    ensure!(inputs == 1, "coinbase must have exactly one input");
    offset += 36;
    let (script, len) = count(offset)?;
    offset += len + script + 4;
    let (outputs, len) = count(offset)?;
    offset += len;
    for _ in 0..outputs {
        offset += 8;
        let (script, len) = count(offset)?;
        offset += len + script;
    }
    let (items, len) = count(offset)?;
    offset += len;
    ensure!(
        items == 1,
        "coinbase witness must hold exactly the reserved value"
    );
    let (size, len) = count(offset)?;
    offset += len;
    ensure!(
        size == 32,
        "coinbase witness reserved value must be 32 bytes"
    );
    let reserved: [u8; 32] = at(offset, 32)?.try_into().expect("32 bytes");
    offset += 32 + 4;
    ensure!(offset == tx.len(), "landed coinbase has trailing bytes");
    Ok(reserved)
}

/// Decode a claimed row into the candidate the row authenticates.
///
/// After 007 there is no compatibility decode: a pending row with a NULL
/// `window_anchor_ms` is a pre-007 row, and the error tells the operator to
/// stop that frontend. Every disagreement between the document and its
/// columns, digest or block is surfaced as corruption.
fn decode_claimed_row(row: &PgRow, token: String) -> Result<CandidateClaim> {
    let block_hash: String = row.try_get("block_hash")?;
    let anchor: Option<i64> = row.try_get("window_anchor_ms")?;
    let Some(anchor) = anchor else {
        bail!(
            "pending candidate {block_hash} carries no window reference: it was written by a pre-007 frontend. \
             Stop every pre-007 frontend and drain the outbox with it before running the post-007 binary"
        );
    };
    let document: Value = row.try_get("candidate")?;
    ensure!(
        document.get("bundle").is_none() && document.get("block_hex").is_none(),
        "pending candidate {block_hash} is an inline pre-007 document on a post-007 schema"
    );
    let mut candidate: Candidate =
        serde_json::from_value(document).context("invalid persisted candidate")?;
    let digest: String = row.try_get("candidate_sha256")?;
    ensure!(
        hex::encode(Sha256::digest(serde_json::to_vec(&candidate)?)) == digest,
        "persisted candidate digest mismatch"
    );
    ensure!(
        candidate.block_hash == block_hash,
        "persisted candidate names block {} in row {block_hash}",
        candidate.block_hash
    );
    // The typed columns are the document's duplicate; any disagreement,
    // including a range in the document with NULL range columns, is corruption.
    ensure!(
        anchor == candidate.window.anchor_ms,
        "candidate window anchor column disagrees with the document"
    );
    let prior: String = row.try_get("window_prior_balances_sha256")?;
    ensure!(
        prior == hex::encode(candidate.window.prior_balances_digest),
        "candidate window balances digest column disagrees with the document"
    );
    let columns: (Option<i64>, Option<i64>, Option<i64>, Option<String>) = (
        row.try_get("window_first_share_seq")?,
        row.try_get("window_last_share_seq")?,
        row.try_get("window_share_count")?,
        row.try_get("window_snapshot_sha256")?,
    );
    match candidate.window.shares {
        Some(range) => ensure!(
            columns
                == (
                    Some(i64::try_from(range.first_share_seq)?),
                    Some(i64::try_from(range.last_share_seq)?),
                    Some(i64::try_from(range.share_count)?),
                    Some(hex::encode(range.snapshot_sha256)),
                ),
            "candidate window range columns disagree with the document"
        ),
        None => ensure!(
            columns == (None, None, None, None),
            "candidate window range columns are set for an empty-window document"
        ),
    }
    let block: Option<Vec<u8>> = row.try_get("block_bytes")?;
    let block = block.context("pending candidate row carries no block bytes")?;
    ensure!(
        Candidate::block_digest_hex(&block) == candidate.block_sha256,
        "candidate block bytes do not hash to the document's block_sha256"
    );
    ensure!(block.len() > 80, "candidate block is truncated");
    let mut hash = Sha256::digest(Sha256::digest(&block[..80])).to_vec();
    hash.reverse();
    ensure!(
        hex::encode(hash) == candidate.block_hash,
        "candidate block header does not hash to block_hash"
    );
    ensure!(
        !candidate.coinbase_suffix_hex.is_empty()
            && hex::decode(&candidate.coinbase_suffix_hex).is_ok(),
        "candidate coinbase suffix must be non-empty hex"
    );
    check_reference_invariants(&candidate)?;
    candidate.block_bytes = block;
    Ok(CandidateClaim {
        candidate,
        claim_token: token,
        parts: None,
    })
}

async fn lock_candidate_row(
    tx: &mut Transaction<'_, Postgres>,
    claim: &CandidateClaim,
) -> Result<()> {
    sqlx::query(
        "SELECT block_hash FROM qbit_block_candidate_outbox WHERE block_hash=$1 FOR UPDATE",
    )
    .bind(&claim.candidate.block_hash)
    .fetch_optional(&mut **tx)
    .await?;
    Ok(())
}

async fn claim_candidate_lane(
    tx: &mut Transaction<'_, Postgres>,
    fresh: bool,
    token: &str,
    instance_id: &str,
    lease_seconds: i64,
) -> Result<Option<PgRow>> {
    let ordering = if fresh {
        "AND attempt_count=0 ORDER BY created_at DESC,block_hash"
    } else {
        // Includes never-attempted rows: continuous new work must not strand
        // an older candidate that has not yet received its first attempt.
        "ORDER BY next_attempt_at,created_at,block_hash"
    };
    let query = format!("WITH next AS (SELECT block_hash FROM qbit_block_candidate_outbox WHERE state='pending' AND next_attempt_at<=clock_timestamp() AND (claim_expires_at IS NULL OR claim_expires_at<=clock_timestamp()) {ordering} FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE qbit_block_candidate_outbox o SET claim_token=$1,claim_instance_id=$2,claim_expires_at=clock_timestamp()+$3*interval '1 second',attempt_count=attempt_count+1,updated_at=clock_timestamp() FROM next WHERE o.block_hash=next.block_hash RETURNING o.block_hash,o.candidate,o.candidate_sha256,o.block_bytes,o.window_anchor_ms,o.window_prior_balances_sha256,o.window_first_share_seq,o.window_last_share_seq,o.window_share_count,o.window_snapshot_sha256");
    Ok(sqlx::query(&query)
        .bind(token)
        .bind(instance_id)
        .bind(lease_seconds)
        .fetch_optional(&mut **tx)
        .await?)
}
