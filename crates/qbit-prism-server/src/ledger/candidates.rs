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
    /// The as-issued prior balances an enqueue writes back to
    /// `qbit_prism_balance_snapshots` when they hash to
    /// `window.prior_balances_digest`, which a `leased` candidate's must.
    /// Never part of the document, consumed by the enqueue's preparation
    /// (sorted, digested and encoded once, off the runtime), and empty on a
    /// claimed candidate.
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

/// Where an outbox row is in the offer-before-landing lifecycle (migration
/// 011). Every variant is unfinished: the row keeps its document, its block
/// bytes and its window reference, and a claim can hold it. The terminal
/// states, `submitted` and `abandoned`, are never claimed and have no
/// variant here.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CandidateState {
    /// Never offered. The only state a claim may offer from, and the only
    /// state that may still be abandoned, on a proven pre-offer supersession.
    #[default]
    Pending,
    /// The durable reservation taken before the one `submitblock` call. A
    /// claim that finds a row here did not take the reservation: the call
    /// may or may not have happened, so it never offers.
    OfferReserved,
    /// The node's answer is recorded; the audit is still to be landed.
    Offered,
    /// Offered, and automation could not finish it: an ambiguous answer, a
    /// lost call, a node rejection, or a landing failure after acceptance.
    /// Retried with read-only chain observations only: never another
    /// `submitblock`, never abandoned.
    Reconciliation,
}

impl CandidateState {
    /// The SQL list of every unfinished state, for `state IN` predicates.
    pub const UNFINISHED_SQL: &'static str =
        "('pending','offer_reserved','offered','reconciliation')";

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::OfferReserved => "offer_reserved",
            Self::Offered => "offered",
            Self::Reconciliation => "reconciliation",
        }
    }

    pub(super) fn parse(state: &str) -> Result<Self> {
        Ok(match state {
            "pending" => Self::Pending,
            "offer_reserved" => Self::OfferReserved,
            "offered" => Self::Offered,
            "reconciliation" => Self::Reconciliation,
            other => {
                bail!("claimed candidate row is in state {other:?}, which no claim lane selects")
            }
        })
    }
}

/// How the one `submitblock` call ended, as the outbox records it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OfferOutcome {
    /// The node answered `null`: it accepted the block, on some chain.
    Accepted,
    /// The node answered a reason string, kept as the offer reply.
    Rejected,
    /// The call's result is not known: a transport failure or timeout after
    /// the request may have been sent, or an outcome lost with the frontend
    /// that took the reservation.
    Unknown,
}

impl OfferOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
            Self::Unknown => "unknown",
        }
    }

    fn parse(outcome: &str) -> Result<Self> {
        Ok(match outcome {
            "accepted" => Self::Accepted,
            "rejected" => Self::Rejected,
            "unknown" => Self::Unknown,
            other => bail!("claimed candidate row records offer outcome {other:?}"),
        })
    }
}

/// What the outbox remembers about a row's one offer.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OfferRecord {
    /// The instance that took the reservation.
    pub reserved_by: Option<String>,
    /// The actual `submitblock` call time on the offering frontend's wall
    /// clock, UNIX milliseconds. `None` when the outcome commit was lost:
    /// the reservation time is never substituted for it.
    pub offered_at_ms: Option<i64>,
    pub outcome: Option<OfferOutcome>,
    /// The node's rejection reason, when it gave one.
    pub reply: Option<String>,
}

/// The lifecycle columns of a claimed row, beside the candidate itself.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClaimLifecycle {
    pub state: CandidateState,
    /// The enqueuing frontend's wall clock (UNIX ms) when the locally
    /// validated proof reached the coordinator; `None` for rows written
    /// before 011. Provenance for the proof-to-first-offer histogram, whose
    /// other boundary is the offering frontend's own wall clock.
    pub proof_observed_at_ms: Option<i64>,
    pub offer: OfferRecord,
}

#[derive(Clone, Debug)]
pub struct CandidateClaim {
    pub candidate: Candidate,
    pub claim_token: String,
    /// `None` until the claim's rebuild fills it. Landing reads the parts
    /// here; a claim without them cannot land.
    pub parts: Option<ClaimParts>,
    /// The row's lifecycle state and offer record as the claim found them.
    pub lifecycle: ClaimLifecycle,
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
/// Owns the candidate: the preparation is whole-set work that runs off the
/// runtime, and what it produced is all the transaction writes.
pub(super) struct PreparedCandidate {
    candidate: Candidate,
    document: Value,
    sha256: String,
    deferred: Option<(Value, String)>,
    /// The canonical stored encoding of the candidate's as-issued balances,
    /// when that set is the one its window reference names: written back to
    /// `qbit_prism_balance_snapshots` under the reference's digest at
    /// enqueue. `None` when the set does not hash to the reference (a caller
    /// that built the candidate by hand), which a leased candidate is
    /// refused for. The set itself was consumed by the preparation: the
    /// transaction sees these bytes and nothing that scales with recipients.
    snapshot: Option<Vec<u8>>,
    /// The proof-observation wall clock the row records, see
    /// [`ClaimLifecycle::proof_observed_at_ms`].
    proof_observed_at_ms: Option<i64>,
}

impl PreparedCandidate {
    pub(super) fn block_hash(&self) -> &str {
        &self.candidate.block_hash
    }
}

/// Serialize, digest and validate a candidate on the blocking pool,
/// recording when its proof was observed. The document is O(1); the block
/// digest scales with the block, which consensus bounds; and the as-issued
/// balance set scales with the window's recipients and is sorted, digested
/// and encoded exactly once, by [`prepare_candidate_blocking`]. None of it
/// runs on a runtime thread, and the caller holds no transaction or lock
/// while it waits: it opens its transaction only with the prepared bytes in
/// hand. A caller cancelled while waiting leaves nothing behind: no
/// transaction has opened, and the task drops the candidate on its own
/// thread.
pub(super) async fn prepare_candidate_observed(
    candidate: Candidate,
    proof_observed_at_ms: Option<i64>,
) -> Result<PreparedCandidate> {
    tokio::task::spawn_blocking(move || prepare_candidate_blocking(candidate, proof_observed_at_ms))
        .await
        .context("candidate preparation task failed")?
}

/// [`prepare_candidate_observed`]'s work: synchronous, whole-set, and never
/// on a runtime thread.
fn prepare_candidate_blocking(
    mut candidate: Candidate,
    proof_observed_at_ms: Option<i64>,
) -> Result<PreparedCandidate> {
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
    check_reference_invariants(&candidate)?;
    // The as-issued set is what the block's coinbase commits to, and after
    // 011 every landing rebuilds from it, so every enqueue whose candidate
    // carries the set the reference names writes it back. The set is taken
    // from the candidate here, sorted into its stored order, digested once
    // and, when the digest is the reference's, encoded once: the digest
    // decides whether this is the referenced set (the vector's order is
    // immaterial), and the encoding is what the transaction inserts. A
    // leased candidate must carry it; any other candidate that does not (a
    // caller that built the candidate by hand) is enqueued without a
    // snapshot and can only land while the current balances still hash to
    // its reference. The vector is dropped here, on this thread, whatever
    // its size.
    let balances = std::mem::take(&mut candidate.as_issued_balances);
    let snapshot = super::window::canonical_as_issued_snapshot(
        balances,
        candidate.window.prior_balances_digest,
    )?;
    ensure!(
        snapshot.is_some() || !candidate.leased,
        "leased candidate's as-issued balances do not hash to its window reference"
    );
    let document = serde_json::to_value(&candidate)?;
    let sha256 = hex::encode(Sha256::digest(serde_json::to_vec(&candidate)?));
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
        snapshot,
        proof_observed_at_ms,
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
    /// before the transaction opens, off the runtime; under `ORDER_LOCK` the
    /// transaction only runs the writer fence, re-establishes what the
    /// candidate references and inserts the prepared bytes.
    pub async fn enqueue_candidate_once(&self, candidate: Candidate) -> Result<bool> {
        self.enqueue_candidate_observed(candidate, None).await
    }

    /// [`Ledger::enqueue_candidate_once`], recording the wall-clock time the
    /// locally validated proof was observed (see
    /// [`ClaimLifecycle::proof_observed_at_ms`]).
    pub async fn enqueue_candidate_observed(
        &self,
        candidate: Candidate,
        proof_observed_at_ms: Option<i64>,
    ) -> Result<bool> {
        let prepared = prepare_candidate_observed(candidate, proof_observed_at_ms).await?;
        let mut tx = self.begin().await?;
        self.lock(&mut tx, ORDER_LOCK).await?;
        writable(&mut tx).await?;
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM qbit_block_candidate_outbox WHERE block_hash=$1)",
        )
        .bind(prepared.block_hash())
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
    /// transaction. In order: the writer fence, the as-issued balance
    /// snapshot, the window prefix probe, then the insert of the prepared
    /// bytes. Nothing here serializes, sorts, digests or encodes anything of
    /// the candidate.
    pub(super) async fn persist_prepared_candidate(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        prepared: &PreparedCandidate,
        share_id: Option<&str>,
    ) -> Result<bool> {
        let candidate = &prepared.candidate;
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
        if let Some(snapshot) = &prepared.snapshot {
            // The as-issued set is written back, or found already present
            // and byte-identical, so the post-offer landing can read
            // `AsIssued` whatever the current balances have become by then.
            // A prune may have removed the job row and its snapshot between
            // the submission's expiry check and this lock, which is why every
            // enqueue re-establishes it rather than only a leased one. The
            // bytes are the canonical encoding the preparation produced off
            // the runtime, of the set it proved hashes to the reference, so
            // the reference's digest keys them.
            super::window::put_canonical_balance_snapshot(
                tx,
                candidate.window.prior_balances_digest,
                snapshot,
            )
            .await?;
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
        let inserted = sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,share_id,candidate,candidate_sha256,block_bytes,window_anchor_ms,window_prior_balances_sha256,window_first_share_seq,window_last_share_seq,window_share_count,window_snapshot_sha256,proof_observed_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT(block_hash) DO NOTHING")
            .bind(&candidate.block_hash).bind(share_id).bind(&prepared.document).bind(&prepared.sha256).bind(&candidate.block_bytes)
            .bind(candidate.window.anchor_ms).bind(hex::encode(candidate.window.prior_balances_digest)).bind(first).bind(last).bind(count).bind(snapshot)
            .bind(prepared.proof_observed_at_ms)
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
        let mut tx = self.begin().await?;
        writable(&mut tx).await?;
        // Empty polling does not use a scheduling slot. A racing SKIP LOCKED
        // selection can still leave a gap; this weighting is deliberately an
        // approximate service ratio rather than a global serialization point.
        let slot: Option<i64> = sqlx::query_scalar(&Self::due_work_probe_sql())
            .fetch_optional(&mut *tx)
            .await?;
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
        // Decode by storage version. A v1 row carries its JSONB candidate; a
        // #258 v2 row carries NULL and a chunk body this server does not
        // import, and a later version is unknown. Those are parked inside the
        // claim transaction so the due lane never offers them again.
        let mut claimed = None;
        if let Some(row) = row {
            let block_hash: String = row.try_get("block_hash")?;
            let state: String = row.try_get("state")?;
            let storage_version: i32 = row.try_get("storage_version")?;
            let candidate: Option<Value> = row.try_get("candidate")?;
            match (storage_version, candidate) {
                // Kept whole rather than reduced to its document: the decode
                // below also authenticates the block bytes against
                // `block_sha256`, and that digest scales with the block.
                (1, Some(_)) => claimed = Some((row, block_hash, state)),
                (version, candidate) => {
                    let reason = if version == 1 {
                        "unfinished storage_version 1 candidate has no JSONB body".to_owned()
                    } else {
                        format!("candidate storage_version {version} is not supported by this server; only version 1 JSONB candidates are (a #258 chunked body must be drained by the 2.x.x release)")
                    };
                    park_candidate(&mut tx, &block_hash, &token, &state, &reason).await?;
                    tracing::warn!(block=%block_hash, storage_version=version, has_body=candidate.is_some(), "parked a candidate this server cannot decode; operator action required");
                }
            }
        }
        tx.commit().await?;
        let Some((row, block_hash, state)) = claimed else {
            return Ok(None);
        };
        // The document is O(1), but the block digest scales with the block,
        // so the decode stays off the runtime thread that renews leases. The
        // row's database hash, the token and the state the claim selected
        // stay here: a validation failure parks exactly the claim this
        // attempt took, never whatever the decoded document names.
        let decode_token = token.clone();
        #[cfg(test)]
        let decode_fault = faults::take(&block_hash, |fault| {
            matches!(
                fault,
                Fault::PanicInDecode
                    | Fault::TransientDecodeDatabaseError
                    | Fault::UnclassifiedDecodeError
            )
        });
        let decoded = tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            faults::before_decode(decode_fault)?;
            decode_claimed_row(&row, decode_token)
        })
        .await?;
        let error = match decoded {
            Ok(claim) => return Ok(Some(claim)),
            Err(error) => error,
        };
        #[cfg(test)]
        faults::after_decode(&self.pool, &block_hash).await?;
        // Only a positively identified malformed row is parked. Any other
        // decode failure, a column extraction or serialization error among
        // them, proves nothing about the stored data: its claim is left to
        // expire and be retried like any failed attempt.
        let Some(&InvalidCandidate(kind)) = error.downcast_ref::<InvalidCandidate>() else {
            return Err(error);
        };
        Err(self
            .park_invalid_candidate(&block_hash, &token, &state, kind, error)
            .await)
    }

    /// Park a claimed row whose persisted input failed validation, in a
    /// writable transaction of its own after the claim committed and the
    /// decode returned. [`park_candidate`] fences it on the claim's database
    /// block hash, token and selected state, so a replacement owner's claim
    /// and a row that has since moved on are never touched.
    ///
    /// Returns the original validation error, its chain intact, with the
    /// parking outcome as context: parked only once exactly one row was
    /// updated and the commit succeeded; not parked when anything before the
    /// commit failed; unknown when the commit itself returned an error, whose
    /// reply may have been lost after the row was durably parked.
    async fn park_invalid_candidate(
        &self,
        block_hash: &str,
        token: &str,
        state: &str,
        kind: ValidationKind,
        error: anyhow::Error,
    ) -> anyhow::Error {
        let reason = parking_reason(block_hash, kind, &error);
        let staged = async {
            let mut tx = self.begin().await?;
            writable(&mut tx).await?;
            park_candidate(&mut tx, block_hash, token, state, &reason).await?;
            #[cfg(test)]
            ensure!(
                faults::take(block_hash, |fault| *fault == Fault::FailBeforeParkCommit).is_none(),
                "injected failure before the parking commit"
            );
            Ok::<_, anyhow::Error>(tx)
        }
        .await;
        let committed = match staged {
            Ok(tx) => {
                let committed = tx.commit().await;
                #[cfg(test)]
                let committed = committed.and_then(|()| faults::commit_reply(block_hash));
                committed.map_err(ParkOutcome::Unknown)
            }
            Err(refused) => Err(ParkOutcome::NotParked(refused)),
        };
        let chain = format!("{error:#}");
        match committed {
            Ok(()) => {
                tracing::warn!(block=%block_hash, validation=kind.as_str(), error=%chain, "parked a candidate that failed validation; operator action required");
                error.context(format!(
                    "candidate {block_hash} failed validation and was parked; operator action required"
                ))
            }
            Err(ParkOutcome::NotParked(failure)) => {
                tracing::error!(block=%block_hash, validation=kind.as_str(), error=%chain, parking_error=%format!("{failure:#}"), "a candidate failed validation and was not parked");
                error.context(format!(
                    "candidate {block_hash} failed validation and was not parked: {failure:#}"
                ))
            }
            Err(ParkOutcome::Unknown(failure)) => {
                tracing::error!(block=%block_hash, validation=kind.as_str(), error=%chain, commit_error=%failure, "a candidate failed validation and whether it was parked is unknown; inspect the row");
                error.context(format!(
                    "candidate {block_hash} failed validation and whether it was parked is unknown: the parking commit failed: {failure}"
                ))
            }
        }
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
        let mut tx = self.begin().await?;
        writable(&mut tx).await?;
        // Evaluate expiry after obtaining the row lock: a blocked UPDATE can
        // otherwise have matched a live token before waiting past its expiry.
        // NO KEY UPDATE is compatible with the processing transaction's KEY
        // SHARE lock, so audit persistence cannot block its own heartbeat.
        sqlx::query("SELECT block_hash FROM qbit_block_candidate_outbox WHERE block_hash=$1 FOR NO KEY UPDATE")
            .bind(&claim.candidate.block_hash).fetch_optional(&mut *tx).await?;
        let updated = sqlx::query(&format!("UPDATE qbit_block_candidate_outbox SET claim_expires_at=clock_timestamp()+$3*interval '1 second',updated_at=clock_timestamp() WHERE block_hash=$1 AND claim_token=$2 AND state IN {} AND claim_expires_at>clock_timestamp()", CandidateState::UNFINISHED_SQL))
            .bind(&claim.candidate.block_hash).bind(&claim.claim_token).bind(lease_seconds).execute(&mut *tx).await?.rows_affected();
        ensure!(updated == 1, "candidate claim was lost or expired");
        tx.commit().await?;
        Ok(())
    }

    /// Release the claim and reschedule the row in whatever unfinished state
    /// it is in. A pending or offered row backs off `min(60, attempt_count)`
    /// seconds; a reconciliation row, which is retried without another
    /// `submitblock` until its block is proven active or an operator
    /// resolves it, backs off `min(3600, 10 * attempt_count)`.
    pub async fn retry_candidate(&self, claim: &CandidateClaim, error: &str) -> Result<()> {
        let mut tx = self.begin().await?;
        writable(&mut tx).await?;
        lock_candidate_row(&mut tx, claim).await?;
        let result = sqlx::query(&format!("UPDATE qbit_block_candidate_outbox SET claim_token=NULL,claim_instance_id=NULL,claim_expires_at=NULL,last_error=$3,next_attempt_at=clock_timestamp()+(CASE WHEN state='reconciliation' THEN LEAST(3600,10*attempt_count) ELSE LEAST(60,attempt_count) END)*interval '1 second',updated_at=clock_timestamp() WHERE block_hash=$1 AND claim_token=$2 AND state IN {} AND claim_expires_at>clock_timestamp()", CandidateState::UNFINISHED_SQL))
            .bind(&claim.candidate.block_hash).bind(&claim.claim_token).bind(error).execute(&mut *tx).await?;
        ensure!(
            result.rows_affected() == 1,
            "candidate claim was lost or expired"
        );
        tx.commit().await?;
        Ok(())
    }

    /// The durable reservation before the one `submitblock` call: the
    /// pending row this live claim holds becomes `offer_reserved`, recording
    /// which instance took it and when (database clock). The row is the
    /// unique reservation per block hash; once it commits, no claim on any
    /// frontend, this one included after a crash, will offer the block
    /// again.
    pub async fn reserve_offer(&self, claim: &CandidateClaim) -> Result<()> {
        let mut tx = self.begin().await?;
        writable(&mut tx).await?;
        sqlx::query("SELECT block_hash FROM qbit_block_candidate_outbox WHERE block_hash=$1 FOR NO KEY UPDATE")
            .bind(&claim.candidate.block_hash).fetch_optional(&mut *tx).await?;
        let reserved = sqlx::query("UPDATE qbit_block_candidate_outbox SET state='offer_reserved',offer_reserved_at=clock_timestamp(),offer_reserved_by=$3,updated_at=clock_timestamp() WHERE block_hash=$1 AND claim_token=$2 AND state='pending' AND claim_expires_at>clock_timestamp()")
            .bind(&claim.candidate.block_hash).bind(&claim.claim_token).bind(&self.instance_id).execute(&mut *tx).await?.rows_affected();
        ensure!(
            reserved == 1,
            "candidate claim was lost or expired before the offer reservation"
        );
        tx.commit().await?;
        Ok(())
    }

    /// Record how the reserved row's one `submitblock` call ended, moving it
    /// to `offered`. `offered_at_ms` is the offering frontend's wall clock
    /// immediately before the call, never the reservation time.
    pub async fn record_offer(
        &self,
        claim: &CandidateClaim,
        offered_at_ms: i64,
        outcome: OfferOutcome,
        reply: Option<&str>,
    ) -> Result<()> {
        let mut tx = self.begin().await?;
        writable(&mut tx).await?;
        sqlx::query("SELECT block_hash FROM qbit_block_candidate_outbox WHERE block_hash=$1 FOR NO KEY UPDATE")
            .bind(&claim.candidate.block_hash).fetch_optional(&mut *tx).await?;
        let recorded = sqlx::query("UPDATE qbit_block_candidate_outbox SET state='offered',offered_at_ms=$3,offer_outcome=$4,offer_reply=$5,updated_at=clock_timestamp() WHERE block_hash=$1 AND claim_token=$2 AND state='offer_reserved' AND claim_expires_at>clock_timestamp()")
            .bind(&claim.candidate.block_hash).bind(&claim.claim_token).bind(offered_at_ms).bind(outcome.as_str()).bind(reply).execute(&mut *tx).await?.rows_affected();
        ensure!(
            recorded == 1,
            "candidate claim was lost or expired while recording the offer outcome"
        );
        tx.commit().await?;
        Ok(())
    }

    /// Adopt a pending row whose block the node already proves active into
    /// the no-resubmission lifecycle, before anything lands: it becomes a
    /// reconciliation row with the node's evidence as its reply, an unknown
    /// outcome (the call that put the block on the chain, if there was one,
    /// is not this attempt's, and its time is unknown, never fabricated),
    /// the reason recorded, and the claim kept so this attempt lands and
    /// confirms it. From here no claim on any frontend offers the block, and
    /// no supersession can abandon it.
    pub async fn adopt_active_candidate(
        &self,
        claim: &CandidateClaim,
        evidence: &str,
        reason: &str,
    ) -> Result<()> {
        ensure!(
            !reason.trim().is_empty() && !evidence.trim().is_empty(),
            "an adoption needs its reason and the node's evidence"
        );
        let mut tx = self.begin().await?;
        writable(&mut tx).await?;
        sqlx::query("SELECT block_hash FROM qbit_block_candidate_outbox WHERE block_hash=$1 FOR NO KEY UPDATE")
            .bind(&claim.candidate.block_hash).fetch_optional(&mut *tx).await?;
        let adopted = sqlx::query("UPDATE qbit_block_candidate_outbox SET state='reconciliation',offer_reserved_at=clock_timestamp(),offer_reserved_by=$3,offer_outcome='unknown',offer_reply=$4,last_error=$5,updated_at=clock_timestamp() WHERE block_hash=$1 AND claim_token=$2 AND state='pending' AND claim_expires_at>clock_timestamp()")
            .bind(&claim.candidate.block_hash).bind(&claim.claim_token).bind(&self.instance_id).bind(evidence).bind(reason).execute(&mut *tx).await?.rows_affected();
        ensure!(
            adopted == 1,
            "candidate claim was lost or expired before the active block was adopted"
        );
        tx.commit().await?;
        Ok(())
    }

    /// Move an offered row this claim holds into `reconciliation`, with the
    /// reason, and release the claim with the reconciliation backoff. A
    /// recovered `offer_reserved` row, whose call may or may not have
    /// happened, records the `unknown` outcome here; an `offered` row keeps
    /// the outcome it recorded. Never abandons and never clears evidence.
    pub async fn reconcile_candidate(&self, claim: &CandidateClaim, reason: &str) -> Result<()> {
        ensure!(
            !reason.trim().is_empty(),
            "a reconciliation row needs a reason"
        );
        let mut tx = self.begin().await?;
        writable(&mut tx).await?;
        lock_candidate_row(&mut tx, claim).await?;
        let moved = sqlx::query("UPDATE qbit_block_candidate_outbox SET state='reconciliation',offer_outcome=COALESCE(offer_outcome,'unknown'),last_error=$3,claim_token=NULL,claim_instance_id=NULL,claim_expires_at=NULL,next_attempt_at=clock_timestamp()+LEAST(3600,10*attempt_count)*interval '1 second',updated_at=clock_timestamp() WHERE block_hash=$1 AND claim_token=$2 AND state IN ('offer_reserved','offered','reconciliation') AND claim_expires_at>clock_timestamp()")
            .bind(&claim.candidate.block_hash).bind(&claim.claim_token).bind(reason).execute(&mut *tx).await?.rows_affected();
        ensure!(
            moved == 1,
            "candidate claim was lost or expired, or the row was never offered"
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

/// The closed set of deterministic reasons a claimed row's persisted input is
/// not a candidate this server may offer. Only [`decode_claimed_row`] names
/// one, at the checks that positively identify malformed persisted data; a
/// database, executor, column extraction or serialization error never does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ValidationKind {
    /// A persisted lifecycle state or offer outcome no variant names.
    Lifecycle,
    /// No window reference: a row a pre-007 frontend wrote.
    WindowReference,
    /// An inline pre-007 document, or JSON that is not a supported `Candidate`.
    Document,
    /// `candidate_sha256` is not the document's canonical digest.
    DocumentDigest,
    /// The document names a block other than the row's `block_hash`.
    DocumentIdentity,
    /// The window columns disagree with the document, or cannot hold its range.
    WindowColumns,
    /// Block bytes missing, truncated, or not hashing to the document's digests.
    Block,
    /// An empty or non-hex coinbase suffix.
    CoinbaseSuffix,
    /// The stored inputs contradict the window reference.
    ReferenceInvariants,
}

impl ValidationKind {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Lifecycle => "lifecycle",
            Self::WindowReference => "window_reference",
            Self::Document => "document",
            Self::DocumentDigest => "document_digest",
            Self::DocumentIdentity => "document_identity",
            Self::WindowColumns => "window_columns",
            Self::Block => "block",
            Self::CoinbaseSuffix => "coinbase_suffix",
            Self::ReferenceInvariants => "reference_invariants",
        }
    }
}

/// The typed context that marks a decode error as a validation failure. It is
/// attached directly over the original diagnosis, which stays in the chain,
/// and a claim finds it with `downcast_ref`, never by matching error text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct InvalidCandidate(pub(super) ValidationKind);

impl std::fmt::Display for InvalidCandidate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "validation {}", self.0.as_str())
    }
}

/// How the follow-up parking transaction ended when it did not commit.
enum ParkOutcome {
    /// Nothing was committed: the transaction could not begin, the writer
    /// fence refused, the claim was no longer held, or it failed before its
    /// commit and rolled back.
    NotParked(anyhow::Error),
    /// The commit returned an error; the row may or may not be parked.
    Unknown(sqlx::Error),
}

/// The bound on the reason a parked row records in `last_error`, in bytes.
pub(super) const PARKING_REASON_MAX_BYTES: usize = 1024;

/// `candidate <database block hash>: validation <kind>: <diagnosis>`, at most
/// [`PARKING_REASON_MAX_BYTES`] bytes of UTF-8. The identity and the kind come
/// first, so only the diagnosis is truncated, on a character boundary and
/// marked with an ellipsis. `error` is the decode's error, whose outermost
/// context is the [`InvalidCandidate`] marker the kind already spells out.
pub(super) fn parking_reason(
    block_hash: &str,
    kind: ValidationKind,
    error: &anyhow::Error,
) -> String {
    let mut reason = format!("candidate {block_hash}: validation {}: ", kind.as_str());
    for (index, cause) in error.chain().skip(1).enumerate() {
        if index > 0 {
            reason.push_str(": ");
        }
        reason.push_str(&cause.to_string());
    }
    if reason.len() > PARKING_REASON_MAX_BYTES {
        let mut end = PARKING_REASON_MAX_BYTES - '…'.len_utf8();
        while !reason.is_char_boundary(end) {
            end -= 1;
        }
        reason.truncate(end);
        reason.push('…');
    }
    reason
}

/// The four range columns of an outbox row, as the claim reads them.
type RangeColumns = (Option<i64>, Option<i64>, Option<i64>, Option<String>);

/// The range columns a document's range must be stored as.
fn range_columns(range: ShareRange) -> Result<RangeColumns> {
    Ok((
        Some(i64::try_from(range.first_share_seq)?),
        Some(i64::try_from(range.last_share_seq)?),
        Some(i64::try_from(range.share_count)?),
        Some(hex::encode(range.snapshot_sha256)),
    ))
}

/// Mark an error as the validation failure `kind`, keeping it as the source.
fn invalid(kind: ValidationKind) -> impl FnOnce(anyhow::Error) -> anyhow::Error {
    move |error| error.context(InvalidCandidate(kind))
}

/// `ensure!` for a persisted-input check: the failure carries its kind.
macro_rules! ensure_valid {
    ($kind:expr, $condition:expr, $($message:tt)+) => {
        if !$condition {
            return Err(anyhow::anyhow!($($message)+).context(InvalidCandidate($kind)));
        }
    };
}

/// Decode a claimed row into the candidate the row authenticates.
///
/// After 007 there is no compatibility decode: a pending row with a NULL
/// `window_anchor_ms` is a pre-007 row, and the error tells the operator to
/// stop that frontend. Every disagreement between the document and its
/// columns, digest or block is surfaced as corruption, marked with its
/// [`ValidationKind`]; a failure to read a column or to serialize the parsed
/// document is left unmarked, because it is not evidence about the row.
fn decode_claimed_row(row: &PgRow, token: String) -> Result<CandidateClaim> {
    let block_hash: String = row.try_get("block_hash")?;
    let state = CandidateState::parse(row.try_get::<String, _>("state")?.as_str())
        .map_err(invalid(ValidationKind::Lifecycle))?;
    let outcome = row
        .try_get::<Option<String>, _>("offer_outcome")?
        .as_deref()
        .map(OfferOutcome::parse)
        .transpose()
        .map_err(invalid(ValidationKind::Lifecycle))?;
    let lifecycle = ClaimLifecycle {
        state,
        proof_observed_at_ms: row.try_get("proof_observed_at_ms")?,
        offer: OfferRecord {
            reserved_by: row.try_get("offer_reserved_by")?,
            offered_at_ms: row.try_get("offered_at_ms")?,
            outcome,
            reply: row.try_get("offer_reply")?,
        },
    };
    let anchor: Option<i64> = row.try_get("window_anchor_ms")?;
    let Some(anchor) = anchor else {
        return Err(anyhow::anyhow!(
            "pending candidate {block_hash} carries no window reference: it was written by a pre-007 frontend. \
             Stop every pre-007 frontend and drain the outbox with it before running the post-007 binary"
        )
        .context(InvalidCandidate(ValidationKind::WindowReference)));
    };
    let document: Value = row.try_get("candidate")?;
    ensure_valid!(
        ValidationKind::Document,
        document.get("bundle").is_none() && document.get("block_hex").is_none(),
        "pending candidate {block_hash} is an inline pre-007 document on a post-007 schema"
    );
    let mut candidate: Candidate = serde_json::from_value(document)
        .context("invalid persisted candidate")
        .map_err(invalid(ValidationKind::Document))?;
    let digest: String = row.try_get("candidate_sha256")?;
    // Serializing the parsed document is not a check of the row: its failure
    // stays unmarked.
    let canonical = serde_json::to_vec(&candidate)?;
    ensure_valid!(
        ValidationKind::DocumentDigest,
        hex::encode(Sha256::digest(canonical)) == digest,
        "persisted candidate digest mismatch"
    );
    ensure_valid!(
        ValidationKind::DocumentIdentity,
        candidate.block_hash == block_hash,
        "persisted candidate names block {} in row {block_hash}",
        candidate.block_hash
    );
    // The typed columns are the document's duplicate; any disagreement,
    // including a range in the document with NULL range columns, is corruption.
    ensure_valid!(
        ValidationKind::WindowColumns,
        anchor == candidate.window.anchor_ms,
        "candidate window anchor column disagrees with the document"
    );
    let prior: String = row.try_get("window_prior_balances_sha256")?;
    ensure_valid!(
        ValidationKind::WindowColumns,
        prior == hex::encode(candidate.window.prior_balances_digest),
        "candidate window balances digest column disagrees with the document"
    );
    let columns: RangeColumns = (
        row.try_get("window_first_share_seq")?,
        row.try_get("window_last_share_seq")?,
        row.try_get("window_share_count")?,
        row.try_get("window_snapshot_sha256")?,
    );
    match candidate.window.shares {
        Some(range) => {
            let expected = range_columns(range)
                .context("candidate window range is not representable in the range columns")
                .map_err(invalid(ValidationKind::WindowColumns))?;
            ensure_valid!(
                ValidationKind::WindowColumns,
                columns == expected,
                "candidate window range columns disagree with the document"
            );
        }
        None => ensure_valid!(
            ValidationKind::WindowColumns,
            columns == (None, None, None, None),
            "candidate window range columns are set for an empty-window document"
        ),
    }
    let block: Option<Vec<u8>> = row.try_get("block_bytes")?;
    let Some(block) = block else {
        return Err(
            anyhow::anyhow!("pending candidate row carries no block bytes")
                .context(InvalidCandidate(ValidationKind::Block)),
        );
    };
    ensure_valid!(
        ValidationKind::Block,
        Candidate::block_digest_hex(&block) == candidate.block_sha256,
        "candidate block bytes do not hash to the document's block_sha256"
    );
    ensure_valid!(
        ValidationKind::Block,
        block.len() > 80,
        "candidate block is truncated"
    );
    let mut hash = Sha256::digest(Sha256::digest(&block[..80])).to_vec();
    hash.reverse();
    ensure_valid!(
        ValidationKind::Block,
        hex::encode(hash) == candidate.block_hash,
        "candidate block header does not hash to block_hash"
    );
    ensure_valid!(
        ValidationKind::CoinbaseSuffix,
        !candidate.coinbase_suffix_hex.is_empty()
            && hex::decode(&candidate.coinbase_suffix_hex).is_ok(),
        "candidate coinbase suffix must be non-empty hex"
    );
    // Shared with the enqueue, whose errors stay unmarked: only this
    // persisted-decode boundary classifies them.
    check_reference_invariants(&candidate).map_err(invalid(ValidationKind::ReferenceInvariants))?;
    candidate.block_bytes = block;
    Ok(CandidateClaim {
        candidate,
        claim_token: token,
        parts: None,
        lifecycle,
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

impl Ledger {
    /// The due-work probe [`Ledger::claim_candidate`] issues first: a
    /// dispatch slot is consumed only while some unfinished row is due and
    /// unclaimed, so empty polling never advances the sequence. Public so a
    /// test can EXPLAIN the statement the server runs rather than a copy.
    pub fn due_work_probe_sql() -> String {
        format!("SELECT nextval('qbit_prism_candidate_dispatch_sequence') WHERE EXISTS(SELECT 1 FROM qbit_block_candidate_outbox WHERE state IN {} AND next_attempt_at<=clock_timestamp() AND (claim_expires_at IS NULL OR claim_expires_at<=clock_timestamp()))", CandidateState::UNFINISHED_SQL)
    }

    /// The row selection of one claim lane, exactly as
    /// [`Ledger::claim_candidate`] issues it inside its claiming statement.
    /// The fresh lane (`fresh`) offers new blocks first: a never-attempted
    /// pending row, newest first. The oldest-due lane serves everything
    /// unfinished, the offered rows waiting for their landing or
    /// reconciliation included, by due time, so no unfinished row is ever
    /// stranded by continuous new work. Public so a test can EXPLAIN the
    /// statement the server runs rather than a copy.
    pub fn claim_lane_sql(fresh: bool) -> String {
        let (states, ordering) = if fresh {
            (
                "('pending')",
                "AND attempt_count=0 ORDER BY created_at DESC,block_hash",
            )
        } else {
            (
                CandidateState::UNFINISHED_SQL,
                "ORDER BY next_attempt_at,created_at,block_hash",
            )
        };
        format!("SELECT block_hash FROM qbit_block_candidate_outbox WHERE state IN {states} AND next_attempt_at<=clock_timestamp() AND (claim_expires_at IS NULL OR claim_expires_at<=clock_timestamp()) {ordering} FOR UPDATE SKIP LOCKED LIMIT 1")
    }
}

async fn claim_candidate_lane(
    tx: &mut Transaction<'_, Postgres>,
    fresh: bool,
    token: &str,
    instance_id: &str,
    lease_seconds: i64,
) -> Result<Option<PgRow>> {
    let query = format!("WITH next AS ({}) UPDATE qbit_block_candidate_outbox o SET claim_token=$1,claim_instance_id=$2,claim_expires_at=clock_timestamp()+$3*interval '1 second',attempt_count=attempt_count+1,updated_at=clock_timestamp() FROM next WHERE o.block_hash=next.block_hash RETURNING o.block_hash,o.storage_version,o.candidate,o.candidate_sha256,o.block_bytes,o.window_anchor_ms,o.window_prior_balances_sha256,o.window_first_share_seq,o.window_last_share_seq,o.window_share_count,o.window_snapshot_sha256,o.state,o.proof_observed_at_ms,o.offer_reserved_by,o.offered_at_ms,o.offer_outcome,o.offer_reply", Ledger::claim_lane_sql(fresh));
    Ok(sqlx::query(&query)
        .bind(token)
        .bind(instance_id)
        .bind(lease_seconds)
        .fetch_optional(&mut **tx)
        .await?)
}

/// Park a claimed row this server cannot decode or that failed validation:
/// release the claim, record why in `last_error`, and move `next_attempt_at`
/// past every lease expiry. The row keeps its state, its body, its block bytes,
/// its window columns and its offer record untouched, so a release that reads
/// it can pick it up by resetting `next_attempt_at`; until then it is operator
/// work, not a retry loop.
///
/// The fence is the claim's database block hash, its token and the state the
/// claim selected, compared in the one `UPDATE`: a delayed caller never parks
/// a replacement owner's claim, nor a row its own token has since advanced to
/// another unfinished state.
async fn park_candidate(
    tx: &mut Transaction<'_, Postgres>,
    block_hash: &str,
    token: &str,
    state: &str,
    reason: &str,
) -> Result<()> {
    let parked = sqlx::query(&format!("UPDATE qbit_block_candidate_outbox SET claim_token=NULL,claim_instance_id=NULL,claim_expires_at=NULL,last_error=$3,next_attempt_at='infinity',updated_at=clock_timestamp() WHERE block_hash=$1 AND claim_token=$2 AND state=$4 AND state IN {}", CandidateState::UNFINISHED_SQL))
        .bind(block_hash).bind(token).bind(reason).bind(state).execute(&mut **tx).await?.rows_affected();
    ensure!(parked == 1, "candidate to park was not held by this claim");
    Ok(())
}

/// Faults a test injects into one claim, keyed by the row's block hash so
/// that tests claiming other rows in the same binary never meet them. Each
/// injected fault fires once.
#[cfg(test)]
pub(super) mod faults {
    use anyhow::Result;
    use sqlx::PgPool;
    use std::sync::Mutex;

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub enum Fault {
        /// The blocking decode panics, so the executor reports a join error.
        PanicInDecode,
        /// The blocking decode fails with a transient database error.
        TransientDecodeDatabaseError,
        /// The blocking decode fails with an unclassified column error.
        UnclassifiedDecodeError,
        /// SQL run after a failed decode and before any parking: another
        /// owner or a state change arriving while the row was being decoded.
        AfterDecodeSql(String),
        /// The parking transaction fails after its `UPDATE`, before commit.
        FailBeforeParkCommit,
        /// The parking commit succeeds but its reply is reported lost.
        LoseParkCommitReply,
    }

    static FAULTS: Mutex<Vec<(String, Fault)>> = Mutex::new(Vec::new());

    pub fn inject(block_hash: &str, fault: Fault) {
        FAULTS.lock().unwrap().push((block_hash.to_owned(), fault));
    }

    pub(in crate::ledger) fn take(
        block_hash: &str,
        wanted: impl Fn(&Fault) -> bool,
    ) -> Option<Fault> {
        let mut faults = FAULTS.lock().unwrap();
        let at = faults
            .iter()
            .position(|(hash, fault)| hash == block_hash && wanted(fault))?;
        Some(faults.remove(at).1)
    }

    pub(in crate::ledger) fn before_decode(fault: Option<Fault>) -> Result<()> {
        match fault {
            Some(Fault::PanicInDecode) => panic!("injected decode panic"),
            Some(Fault::TransientDecodeDatabaseError) => Err(sqlx::Error::PoolTimedOut.into()),
            Some(Fault::UnclassifiedDecodeError) => {
                Err(sqlx::Error::ColumnNotFound("injected unclassified decode error".into()).into())
            }
            _ => Ok(()),
        }
    }

    pub(in crate::ledger) async fn after_decode(pool: &PgPool, block_hash: &str) -> Result<()> {
        if let Some(Fault::AfterDecodeSql(sql)) = take(block_hash, |fault| {
            matches!(fault, Fault::AfterDecodeSql(_))
        }) {
            sqlx::raw_sql(&sql).execute(pool).await?;
        }
        Ok(())
    }

    pub(in crate::ledger) fn commit_reply(block_hash: &str) -> Result<(), sqlx::Error> {
        match take(block_hash, |fault| *fault == Fault::LoseParkCommitReply) {
            Some(_) => Err(sqlx::Error::Protocol(
                "injected lost parking commit reply".into(),
            )),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
use faults::Fault;

#[cfg(test)]
#[path = "candidates/park_tests.rs"]
mod park_tests;

#[cfg(test)]
#[path = "candidates/storm_fault_tests.rs"]
mod storm_fault_tests;
