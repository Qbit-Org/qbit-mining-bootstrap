use super::*;

mod payout_state;
pub use payout_state::PayoutState;
pub(crate) use payout_state::RefreshProbe;
pub(super) mod blocking_drop;
use blocking_drop::{BlockingDrop, ReadAdmission};
mod snapshot_delta;
pub(crate) use snapshot_delta::{
    AcquisitionReport, Advance, LeafWitness, RetainedShares, SnapshotCapture, WindowAcquisition,
};
pub(super) mod cut;
use cut::BindCut;
pub use cut::{OriginIndexMissing, WindowCut};

const ACCEPTED_CUTOFF_SQL: &str =
    "SELECT COALESCE(max(share_seq),0) FROM qbit_share_ledger WHERE accepted";

/// The dual-writer snapshot's [`ACCEPTED_CUTOFF_SQL`]: in the same statement,
/// so in one MVCC snapshot and under the same `ORDER_LOCK`, this node's own
/// cut entry and the peer's share-stream high-water mark (`window/cut.rs`).
/// Every peer row at or below the mark is visible to the cutoff's read, so
/// the cutoff is at least every row the window's cut can admit. `$1` is this
/// node's index, `$2` the anchor, `$3` the retained window's entry for this
/// node or NULL.
static DUAL_CUTOFF_SQL: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    format!(
        "SELECT ({ACCEPTED_CUTOFF_SQL}),({}),({})",
        cut::OWN_CUT_SQL,
        cut::PEER_HIGH_WATER_SQL
    )
});

#[derive(Clone, Debug)]
pub struct AppendResult {
    pub share: AcceptedShare,
    pub inserted: bool,
    /// #657: the fenced append found the payout revision moved past
    /// `expected_revision` and captured its candidate instead. The block is
    /// enqueued as a #478 capture with `share` deferred, credited only if the
    /// block confirms. No share was appended, so `inserted` is false.
    pub captured: bool,
}

/// The share append refused before any statement: this node's database was
/// restored or changed identity under the running frontend (D-8, D-9), see
/// [`Ledger::set_own_log_lost`].
#[derive(Debug)]
pub struct OwnLogLost;

impl std::fmt::Display for OwnLogLost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            "this node's database was restored or changed identity under the running frontend \
             (D-8, D-9): no share is appended until the frontend has stopped; its restart checks \
             the database again first",
        )
    }
}

impl std::error::Error for OwnLogLost {}

/// The pre-commit hook of [`Ledger::append_at_revision_gated`] refused COMMIT.
/// COMMIT was never sent and the transaction was rolled back.
#[derive(Debug)]
pub struct CommitGateClosed;

impl std::fmt::Display for CommitGateClosed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("share commit gate closed before COMMIT")
    }
}

impl std::error::Error for CommitGateClosed {}

/// The share append's payout-revision fence refused a share: the revision
/// moved between the submit check that admitted it at `expected` and the
/// append's read under `ORDER_LOCK`. It is raised before any write, and the
/// transaction is rolled back. A share without a candidate is always refused
/// this way, and so is one that carries a block when capture is off
/// ([`MovedRevision::Refuse`]); with capture on, its block is captured
/// instead (#657, [`AppendResult::captured`]). The miner is answered
/// `stale-job`, as the submit check answers superseded work (#675).
#[derive(Debug)]
pub struct PayoutRevisionChanged {
    pub expected: i64,
    pub observed: i64,
}

impl std::fmt::Display for PayoutRevisionChanged {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "payout revision changed before share commit: admitted at {}, now {}",
            self.expected, self.observed
        )
    }
}

impl std::error::Error for PayoutRevisionChanged {}

/// What a fenced append that carries a block does when the payout revision
/// moved after the share's submit check (#657).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MovedRevision {
    /// Refuse before any write, with [`PayoutRevisionChanged`], as an append
    /// without a block always is.
    Refuse,
    /// Capture the block, its share deferred: the #478 capture the submit
    /// check itself makes when capture is on
    /// (`PRISM_CAPTURE_OVERPAY_CEILING_BPS` > 0).
    Capture,
}

/// A transition was refused before any write, with its original chain epoch
/// unchanged and its predecessor still accepted. Only a new coherent proof
/// may retry it, retaining the ORIGINAL witness epoch.
#[derive(Debug)]
pub(crate) struct ChainObservationRetry;

impl std::fmt::Display for ChainObservationRetry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("chain observation revision changed")
    }
}

impl std::error::Error for ChainObservationRetry {}

/// A strictly lower-work view was refused before any write or COMMIT.
#[derive(Debug)]
pub(crate) struct ChainObservationBehind;

impl std::fmt::Display for ChainObservationBehind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("local node is behind the cluster's cumulative chainwork")
    }
}

impl std::error::Error for ChainObservationBehind {}

/// One coherent cluster snapshot captured before the node proof begins.
/// This token is runtime-only; it does not change issued-work formats.
#[derive(Clone, Debug)]
pub struct ChainObservationState {
    pub payout_revision: i64,
    pub chain_epoch: i64,
    pub best_tip_hash: Option<String>,
}

/// A local transition remains bound to the epoch that first authorized it.
/// A fresh attempt must never replace this epoch with its newer snapshot.
#[derive(Clone, Debug)]
pub struct ChainTransition {
    pub predecessor: String,
    pub origin_chain_epoch: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub anchor_ms: i64,
    pub share_seq: u64,
    pub payout_revision: i64,
    pub shares: Vec<AcceptedShare>,
    pub prior_balances: Vec<CarryForwardBalance>,
    /// The dual-writer window's per-node cut (`window/cut.rs`); `None` in
    /// single-writer mode, where the window is exactly 3.0's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cut: Option<WindowCut>,
}

/// Immutable builder inputs; the issued/current revision belongs to the caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowRef {
    pub anchor_ms: i64,
    #[serde(with = "hex32")]
    pub prior_balances_digest: [u8; 32],
    pub shares: Option<ShareRange>,
    /// The dual-writer window's per-node cut, which every re-read and proof of
    /// the window applies (`window/cut.rs`). Absent from the JSON when `None`,
    /// so a single-writer reference, and every document that holds one,
    /// keeps its 3.0 bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cut: Option<WindowCut>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShareRange {
    pub first_share_seq: u64,
    pub last_share_seq: u64,
    pub share_count: u64,
    /// SHA-256 of native AcceptedShare JSON, not PayoutWindow's sorted-key digest.
    #[serde(with = "hex32")]
    pub snapshot_sha256: [u8; 32],
}

#[derive(Debug)]
pub struct Window {
    pub shares: Vec<AcceptedShare>,
    pub prior_balances: Vec<CarryForwardBalance>,
    /// Current revision in the read transaction; never replace the issued one.
    pub payout_revision: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BalanceSource {
    Current,
    AsIssued,
}

#[derive(Debug, thiserror::Error)]
pub enum WindowError {
    #[error("window range incomplete: expected {expected} shares, read {got}")]
    Incomplete { expected: u64, got: u64 },
    #[error("prior balances changed since the reference was written")]
    PriorBalancesChanged {
        expected: [u8; 32],
        actual: [u8; 32],
    },
    #[error("as-issued balance snapshot missing")]
    BalanceSnapshotMissing { digest: [u8; 32] },
    #[error("window snapshot digest mismatch")]
    SnapshotDigestMismatch {
        expected: [u8; 32],
        actual: [u8; 32],
    },
    #[error("window database error: {0}")]
    Database(#[from] sqlx::Error),
    /// A blocking decode, hash or encode hand-off was cancelled or panicked.
    ///
    /// Callers treat this as a **retryable failure with its own alert, never
    /// as corruption**: no share row, balance row or digest was found wrong,
    /// so it must never take the corruption or abandon path that
    /// [`WindowError::Decode`] and [`WindowError::SnapshotDigestMismatch`]
    /// take. A cancelled read is the ordinary shape of a caller's deadline
    /// expiring or its task being aborted.
    #[error("window blocking task cancelled or failed: {0}")]
    TaskFailed(#[source] tokio::task::JoinError),
    #[error("window decode error: {0}")]
    Decode(#[source] anyhow::Error),
}

impl WindowRef {
    /// The reference for a captured [`Snapshot`].
    ///
    /// **Synchronous, and whole-window work.** Serializing and hashing the
    /// share array is the per-non-cached-refresh cost the design record
    /// budgets at 0.7 to 1.4 s for 400,000 shares, so every caller runs this
    /// inside its own `spawn_blocking`; it is never awaited and never run on
    /// a runtime thread. The digest streams through the module's `DigestWriter`,
    /// so the 233 to 260 MB serialized array is never materialized, and it is
    /// byte-identical to `sha256(serde_json::to_vec(&snapshot.shares))`, the
    /// bytes `qbit_prism_audit_snapshots.snapshot_sha256` already stores for
    /// the same window.
    ///
    /// An empty snapshot gives `shares: None`, the empty-window reference; it
    /// costs only the O(recipients) balances digest, because no share array
    /// is reached. `Snapshot.prior_balances` is hashed in the vector order it
    /// arrives in: [`qbit_prism::prior_balances_digest`] sorts internally, so
    /// the reference does not depend on that order and the read path never
    /// re-sorts the vector it returns.
    pub fn from_snapshot(snapshot: &Snapshot) -> Result<Self> {
        Self::from_snapshot_with(snapshot, || share_array_digest(&snapshot.shares))
    }

    /// [`WindowRef::from_snapshot`] with `snapshot_sha256` already computed
    /// by the caller as `sha256(serde_json::to_vec(&snapshot.shares))` over
    /// this same snapshot: the refresh pipeline's single pass over the share
    /// array feeds this digest and the canonical audit prefix together. The
    /// digest is not checked here; an empty snapshot ignores it.
    pub fn from_snapshot_with_digest(
        snapshot: &Snapshot,
        snapshot_sha256: [u8; 32],
    ) -> Result<Self> {
        Self::from_snapshot_with(snapshot, || Ok(snapshot_sha256))
    }

    fn from_snapshot_with(
        snapshot: &Snapshot,
        snapshot_sha256: impl FnOnce() -> Result<[u8; 32]>,
    ) -> Result<Self> {
        let shares = match (snapshot.shares.first(), snapshot.shares.last()) {
            (Some(first), Some(last)) => Some(ShareRange {
                first_share_seq: first.share_seq,
                last_share_seq: last.share_seq,
                share_count: u64::try_from(snapshot.shares.len())?,
                snapshot_sha256: snapshot_sha256()?,
            }),
            _ => None,
        };
        Ok(Self {
            anchor_ms: snapshot.anchor_ms,
            prior_balances_digest: qbit_prism::prior_balances_digest(&snapshot.prior_balances),
            shares,
            cut: snapshot.cut,
        })
    }
}

/// `sha256(serde_json::to_vec(&shares))` without the serialized copy.
fn share_array_digest(shares: &[AcceptedShare]) -> Result<[u8; 32]> {
    let mut digest = Sha256::new();
    serde_json::to_writer(DigestWriter(&mut digest), shares)?;
    Ok(digest.finalize().into())
}

impl ShareRange {
    fn bounds(self) -> Result<(i64, i64), WindowError> {
        let checked = || -> Result<(i64, i64)> {
            let first = i64::try_from(self.first_share_seq)?;
            let last = i64::try_from(self.last_share_seq)?;
            ensure!(
                first >= 1 && last >= first,
                "invalid window sequence bounds"
            );
            ensure!(
                (1..=u64::try_from(last - first + 1)?).contains(&self.share_count),
                "invalid window share count"
            );
            Ok((first, last))
        };
        checked().map_err(WindowError::Decode)
    }
}

mod hex32 {
    use serde::{de::Error, Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(
        value: &[u8; 32],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&hex::encode(value))
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<[u8; 32], D::Error> {
        let value = String::deserialize(deserializer)?;
        if value.len() != 64
            || !value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(D::Error::custom(
                "digest must be exactly 64 lowercase hex characters",
            ));
        }
        let mut digest = [0; 32];
        hex::decode_to_slice(value, &mut digest).map_err(D::Error::custom)?;
        Ok(digest)
    }
}

impl Ledger {
    /// Reconstruct and authenticate a complete window on the primary, in one
    /// repeatable-read snapshot. This owns its blocking decode/hash hand-offs.
    /// Callers own build/read permits and the single end-to-end deadline; this
    /// API acquires no nested permits and makes no job/candidate eligibility decision.
    pub async fn read_window(
        &self,
        window: &WindowRef,
        balances: BalanceSource,
    ) -> Result<Window, WindowError> {
        self.read_window_owned(window, balances, ReadAdmission::default())
            .await
    }

    async fn read_window_owned(
        &self,
        window: &WindowRef,
        balances: BalanceSource,
        completion: ReadAdmission,
    ) -> Result<Window, WindowError> {
        let bounds = window.shares.map(ShareRange::bounds).transpose()?;
        let mut tx = self.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await?;
        let payout_revision = sqlx::query_scalar(
            "SELECT payout_revision FROM qbit_prism_cluster WHERE singleton AND fatal_error IS NULL AND NOT pg_is_in_recovery()",
        ).fetch_one(&mut *tx).await?;
        let expected_balances = window.prior_balances_digest;
        let prior_balances = match balances {
            BalanceSource::Current => {
                let rows = prior_balance_rows(&mut tx).await?;
                completion
                    .own(rows)
                    .map(move |rows| {
                        let decoded = decode_prior_balances(rows).map_err(WindowError::Decode)?;
                        check_balances(decoded, expected_balances, balances)
                    })
                    .await?
            }
            BalanceSource::AsIssued => {
                let bytes: Vec<u8> = sqlx::query_scalar(
                    "SELECT balances FROM qbit_prism_balance_snapshots WHERE prior_balances_digest=$1",
                ).bind(hex::encode(expected_balances)).fetch_optional(&mut *tx).await?
                    .ok_or(WindowError::BalanceSnapshotMissing { digest: expected_balances })?;
                completion
                    .own(bytes)
                    .map(move |bytes| {
                        let decoded = serde_json::from_slice(&bytes)
                            .map_err(|error| WindowError::Decode(error.into()))?;
                        check_balances(decoded, expected_balances, balances)
                    })
                    .await?
            }
        };
        let shares = if let (Some(range), Some((first, last))) = (window.shares, bounds) {
            if !probe_share_rows(&mut tx, first, last).await? {
                // No page has been read; an endpoint probe is not a window count.
                return Err(WindowError::Incomplete {
                    expected: range.share_count,
                    got: 0,
                });
            }
            let state = read_range_owned(
                &mut tx,
                first,
                last,
                window.anchor_ms,
                window.cut.as_ref(),
                &completion,
                WindowRead::new(),
                move |state: &mut WindowRead, shares| state.page(shares, range.share_count),
            )
            .await?;
            state.map(move |state| state.finish(range)).await?
        } else {
            completion.own(Vec::new())
        };
        tx.commit().await?;
        Ok(Window {
            shares: shares.into_inner(),
            prior_balances: prior_balances.into_inner(),
            payout_revision,
        })
    }

    /// [`Ledger::read_window`], holding the caller's `window_reads` permit for
    /// exactly as long as the read owns database and blocking work.
    ///
    /// The `window_reads` semaphore is the callers' own, created next to
    /// `build_slots` and sized `clamp(database_max_connections - 2, 1,
    /// build_workers)` so at least two pool connections always stay free for
    /// share appends and the candidate-lease heartbeat. A caller takes its
    /// `build_slots` permit first, then this one, and releases this one before
    /// the rebuild; the read still acquires nothing of its own, so there is no
    /// nested acquisition to wait on itself.
    ///
    /// One shared completion owner retains the permit through every blocking
    /// hand-off and payload cleanup. Cancellation drops the transaction and
    /// stops paging immediately, but admission remains held until any running
    /// mapping and every accumulated vector's cleanup actually finish. Separate
    /// cleanup tasks may finish in any order. On success the vectors pass to
    /// the caller and admission is released before its rebuild; on failure it
    /// can remain held while off-runtime cleanup finishes.
    ///
    /// Callers await this on the runtime, exactly as they do
    /// [`Ledger::read_window`]; wrapping either in `spawn_blocking` is
    /// forbidden.
    pub async fn read_window_with_permit(
        &self,
        window: &WindowRef,
        balances: BalanceSource,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<Window, WindowError> {
        self.read_window_owned(window, balances, ReadAdmission::new(permit))
            .await
    }

    pub async fn chain_observation_state(&self) -> Result<ChainObservationState> {
        let (payout_revision, chain_epoch, best_tip_hash) = sqlx::query_as(
            "SELECT payout_revision,chain_epoch,best_tip_hash FROM qbit_prism_cluster WHERE singleton",
        )
        .fetch_one(&mut *self.acquire().await?)
        .await?;
        Ok(ChainObservationState {
            payout_revision,
            chain_epoch,
            best_tip_hash,
        })
    }

    /// Coordinate nodes by cumulative proof of work. Unsequenced observations
    /// cannot replace an accepted tip with an equal-work sibling.
    pub async fn observe_chain_view(
        &self,
        tip: &str,
        height: u64,
        chainwork_hex: &str,
    ) -> Result<i64> {
        self.observe_chain_view_checked(tip, height, chainwork_hex, None)
            .await
    }

    /// Follow an observed local node transition from the accepted predecessor,
    /// with a coherent cluster token read *before* its fresh node proof. A delayed
    /// observation must not reverse a replacement another observer committed.
    /// Callers consume this transition before I/O: unchanged opposing polls,
    /// cancellation, unknown COMMIT outcomes and failed later publication must
    /// not create another transition. Only `ChainObservationRetry` establishes
    /// that no write occurred and no intervening chain epoch invalidated the
    /// original witness. A fresh proof may retry it without rebinding its epoch.
    /// Greater-work observations and the already accepted tip retain their
    /// existing monotonic/no-op semantics, even if the revision has advanced.
    /// Callers must prove that tip, height and work describe the same active tip.
    /// Candidate/settlement observers and cold observers without a coherent
    /// local predecessor use the strict [`Self::observe_chain_view`] path.
    /// A cold conflicting equal-work node waits for convergence or more work;
    /// this is not an authoritative-node election or a failover policy.
    pub async fn observe_chain_transition(
        &self,
        transition: &ChainTransition,
        tip: &str,
        height: u64,
        chainwork_hex: &str,
        observed: &ChainObservationState,
    ) -> Result<i64> {
        let predecessor = &transition.predecessor;
        ensure!(
            predecessor.len() == 64
                && predecessor.bytes().all(|c| c.is_ascii_hexdigit())
                && !predecessor.eq_ignore_ascii_case(tip),
            "invalid chain transition predecessor"
        );
        ensure!(
            transition.origin_chain_epoch >= 0,
            "invalid chain transition epoch"
        );
        self.observe_chain_view_checked(tip, height, chainwork_hex, Some((transition, observed)))
            .await
    }

    async fn observe_chain_view_checked(
        &self,
        tip: &str,
        height: u64,
        chainwork_hex: &str,
        transition: Option<(&ChainTransition, &ChainObservationState)>,
    ) -> Result<i64> {
        ensure!(
            tip.len() == 64 && tip.bytes().all(|c| c.is_ascii_hexdigit()),
            "invalid chain tip hash"
        );
        ensure!(
            !chainwork_hex.is_empty()
                && chainwork_hex.len() <= 64
                && chainwork_hex.bytes().all(|c| c.is_ascii_hexdigit()),
            "invalid cumulative chainwork"
        );
        let work = num_bigint::BigUint::parse_bytes(chainwork_hex.as_bytes(), 16)
            .context("invalid chainwork")?;
        ensure!(
            work > num_bigint::BigUint::from(0u8),
            "chainwork must be positive"
        );
        let work = work.to_str_radix(10);
        let height = i64::try_from(height)?;
        let tip = tip.to_ascii_lowercase();
        let mut tx = self.begin().await?;
        self.lock(&mut tx, SETTLEMENT_LOCK).await?;
        writable(&mut tx).await?;
        let row=sqlx::query("SELECT payout_revision,chain_epoch,best_chainwork=$1::text::numeric AS same_work,best_chainwork<$1::text::numeric AS more_work,best_tip_hash,best_tip_height FROM qbit_prism_cluster WHERE singleton FOR UPDATE")
            .bind(&work).fetch_one(&mut *tx).await?;
        let same: bool = row.try_get("same_work")?;
        let greater: bool = row.try_get("more_work")?;
        if !greater && !same {
            return Err(ChainObservationBehind.into());
        }
        let mut revision: i64 = row.try_get("payout_revision")?;
        let accepted_tip: Option<String> = row.try_get("best_tip_hash")?;
        let same_tip = accepted_tip.as_deref() == Some(&tip);
        if same {
            let same_height = row.try_get::<Option<i64>, _>("best_tip_height")? == Some(height);
            if !same_tip {
                if let Some((witness, observed)) = transition {
                    let from = witness.predecessor.to_ascii_lowercase();
                    ensure!(
                        row.try_get::<i64, _>("chain_epoch")? == witness.origin_chain_epoch
                            && observed.chain_epoch == witness.origin_chain_epoch
                            && observed.best_tip_hash.as_deref() == Some(from.as_str()),
                        "chain observation epoch changed"
                    );
                    if revision != observed.payout_revision {
                        if accepted_tip.as_deref() == Some(from.as_str()) && same_height {
                            // No UPDATE or COMMIT has been attempted. Preserve
                            // this distinction from an indeterminate SQL error.
                            return Err(ChainObservationRetry.into());
                        }
                        bail!("chain observation revision changed");
                    }
                    ensure!(
                        accepted_tip.as_deref() == Some(from.as_str()),
                        "chain transition predecessor changed"
                    );
                }
            }
            ensure!(
                (same_tip || transition.is_some()) && same_height,
                "local node follows a conflicting equal-work chain tip"
            );
        }
        if greater || !same_tip {
            revision=sqlx::query_scalar("UPDATE qbit_prism_cluster SET best_chainwork=$1::text::numeric,best_tip_hash=$2,best_tip_height=$3,payout_revision=payout_revision+1,chain_epoch=chain_epoch+1,updated_at=clock_timestamp() WHERE singleton RETURNING payout_revision")
                .bind(work).bind(tip).bind(height).fetch_one(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(revision)
    }

    pub async fn append(
        &self,
        share: AcceptedShare,
        candidate: Option<Candidate>,
    ) -> Result<AppendResult> {
        self.append_checked(share, candidate, None, None, None, MovedRevision::Refuse)
            .await
    }

    pub async fn append_at_revision(
        &self,
        share: AcceptedShare,
        candidate: Option<Candidate>,
        expected_revision: i64,
    ) -> Result<AppendResult> {
        self.append_checked(
            share,
            candidate,
            None,
            Some(expected_revision),
            None,
            MovedRevision::Refuse,
        )
        .await
    }

    /// [`Ledger::append_at_revision`] with a last-moment veto over COMMIT.
    ///
    /// `pre_commit` is called at most once, only after every statement,
    /// including `persist_candidate`, has succeeded, while `ORDER_LOCK` is
    /// held, and immediately before COMMIT. It must not block or await. If it
    /// returns `false`, COMMIT is never sent: the transaction is rolled back
    /// and the call fails with [`CommitGateClosed`]. Nothing fallible runs
    /// between a `true` return and `tx.commit()`.
    pub async fn append_at_revision_gated(
        &self,
        share: AcceptedShare,
        candidate: Option<Candidate>,
        expected_revision: i64,
        pre_commit: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<AppendResult> {
        self.append_checked(
            share,
            candidate,
            None,
            Some(expected_revision),
            Some(pre_commit),
            MovedRevision::Refuse,
        )
        .await
    }

    /// [`Ledger::append_at_revision_gated`], recording when the candidate's
    /// locally validated proof was observed (a wall clock, UNIX ms); see
    /// `ClaimLifecycle::proof_observed_at_ms`. `moved` decides what a
    /// candidate-bearing append does when the revision moved (#657).
    pub async fn append_at_revision_gated_observed(
        &self,
        share: AcceptedShare,
        candidate: Option<Candidate>,
        proof_observed_at_ms: Option<i64>,
        expected_revision: i64,
        pre_commit: &(dyn Fn() -> bool + Send + Sync),
        moved: MovedRevision,
    ) -> Result<AppendResult> {
        self.append_checked(
            share,
            candidate,
            proof_observed_at_ms,
            Some(expected_revision),
            Some(pre_commit),
            moved,
        )
        .await
    }

    async fn append_checked(
        &self,
        share: AcceptedShare,
        candidate: Option<Candidate>,
        proof_observed_at_ms: Option<i64>,
        expected_revision: Option<i64>,
        pre_commit: Option<&(dyn Fn() -> bool + Send + Sync)>,
        moved: MovedRevision,
    ) -> Result<AppendResult> {
        if self.own_log_lost() {
            return Err(OwnLogLost.into());
        }
        // The ACK path is the incident path. A block-solving share's candidate
        // is serialized, digested and checked here, before the transaction
        // opens and off the runtime, so `ORDER_LOCK` is held only for the
        // share append and the insert of the prepared bytes, whatever the
        // window size, and the runtime thread never prepares the as-issued
        // balances, whatever the recipient count.
        if let Some(candidate) = &candidate {
            ensure!(
                candidate.deferred_share.is_none(),
                "credited candidates cannot also contain a deferred share"
            );
        }
        // A fenced append that may capture prepares its candidate's capture
        // form too (#657): if the payout revision moved after the share's
        // submit check, the append writes the capture instead of losing the
        // block to the fence.
        let prepared = match candidate {
            Some(candidate) if expected_revision.is_some() && moved == MovedRevision::Capture => {
                Some(
                    prepare_fenced_candidate(candidate, proof_observed_at_ms, share.clone())
                        .await?,
                )
            }
            Some(candidate) => {
                Some(prepare_candidate_observed(candidate, proof_observed_at_ms).await?)
            }
            None => None,
        };
        // The copy the retry below would need: `append_in` takes the share by
        // value, and six short string allocations against a round trip to
        // PostgreSQL is the whole price of keeping a refused append
        // recoverable.
        let retry_share = share.clone();
        let attempt = self
            .append_prepared(share, prepared.as_ref(), expected_revision, pre_commit)
            .await;
        // The ledger is partitioned on `share_seq` with no DEFAULT partition
        // (migration 017): once the sequence runs past the last attached
        // bound, the INSERT is refused with SQLSTATE 23514, "no partition of
        // relation ... found for row". That refusal is definite and total. It
        // is raised by the ledger INSERT itself, inside the transaction
        // `append_prepared` opened, and that transaction is rolled back with
        // the share, the clock update, the hash row and any prepared
        // candidate bytes undone together, so nothing of the attempt
        // survives it. [`crate::partitions::run`] should have kept the lead
        // attached; where it has not, a miner's share is not the place to
        // lose the work. Attach the lead on a fresh pool connection and run
        // the whole attempt again, exactly once. Any other error, and a
        // second failure of any kind, is returned unchanged: reconcile,
        // never repeat.
        //
        // `pre_commit` is still called at most once. It runs only after every
        // statement of an attempt has succeeded, which an attempt refused by
        // 23514 never reaches.
        let Err(error) = attempt else { return attempt };
        if !refused_for_want_of_a_partition(&error) {
            return Err(error);
        }
        let created = crate::partitions::ensure_with_metrics(&self.pool, self.metrics.as_deref()).await.context(
            "the share ledger has no partition for the next share_seq and attaching the partition lead failed; run qbit_prism_share_partition_ensure() against the primary",
        )?;
        tracing::warn!(
            created,
            %error,
            "share append found no partition for its sequence; attached the partition lead and retried"
        );
        self.append_prepared(
            retry_share,
            prepared.as_ref(),
            expected_revision,
            pre_commit,
        )
        .await
    }

    /// `BEGIN` for the share append: generic plans for the transaction, so
    /// the share_id probe on the partitioned ledger is planned once per
    /// connection and pruned at run time from its bound share_seq bounds.
    /// With the default `auto` the planner keeps choosing a custom plan for
    /// that probe (it prunes at plan time and looks cheaper than the generic
    /// estimate over every partition) and re-plans it on every share, inside
    /// `ORDER_LOCK`. Sent with `BEGIN` in one round trip; `SET LOCAL` ends
    /// with the transaction, and applies to every statement in it; the others
    /// are key lookups or scan every leaf either way. `AppendConnection::begin`
    /// records the `BEGIN` as complete only after the reply to the whole batch
    /// is read, so a cancel before then still retires the connection (#482).
    ///
    /// Plan cache: SQLx prepares the probe once per connection. Under
    /// `force_generic_plan` its first execution builds the generic plan,
    /// with the bounds as parameters, and every later execution reuses it;
    /// executor startup then prunes the partitions outside the bound values
    /// ("Subplans Removed" in EXPLAIN EXECUTE, pinned by
    /// `the_append_probe_is_pruned_to_the_leaves_between_the_floor_and_the_sequence`).
    /// A partition attach or detach invalidates the plan and the next
    /// execution rebuilds it once.
    const APPEND_TRANSACTION_BEGIN: &str = "BEGIN; SET LOCAL plan_cache_mode = force_generic_plan";

    /// One complete attempt of [`Ledger::append_checked`], from BEGIN to
    /// COMMIT. Runs the share append, the prepared candidate persistence and
    /// the pre-commit gate under one `ORDER_LOCK`; every path out of it has
    /// either committed or rolled the transaction back.
    async fn append_prepared(
        &self,
        share: AcceptedShare,
        prepared: Option<&super::candidates::PreparedCandidate>,
        expected_revision: Option<i64>,
        pre_commit: Option<&(dyn Fn() -> bool + Send + Sync)>,
    ) -> Result<AppendResult> {
        // Everything the first statement needs is ready before the lock.
        let header_hash = share_header_hash(&share.share_id);
        let first_read: &str = if expected_revision.is_some() {
            &APPEND_FENCED_FIRST_READ_SQL
        } else {
            &APPEND_FIRST_READ_SQL
        };
        let admission = super::append_admission::Admission::acquire(&self.pool).await?;
        let mut connection = admission.attach(self.acquire().await?);
        let mut tx = connection.begin(Self::APPEND_TRANSACTION_BEGIN).await?;
        // Dropped on every path out: after the COMMIT or the gate's ROLLBACK
        // below, or, on an error, just before the queued ROLLBACK runs.
        let _order = self
            .lock_order(&mut tx, crate::metrics::OrderLockHolder::Append)
            .await?;
        // Every client round trip from here to COMMIT is time ORDER_LOCK
        // serializes every share behind (#711), so the append reads all it
        // needs before its share_id probe in this one statement: the write
        // guard, the payout revision (read `FOR SHARE` when fenced) and
        // `APPEND_PROBE_SQL`. It must stay a statement of its own after the
        // lock's: under READ COMMITTED a statement's snapshot is taken when
        // it starts, so reads in the lock's own statement would predate the
        // lock and miss the commit of the share appended just before it.
        let first = sqlx::query(first_read)
            .bind(&header_hash)
            .bind(&share.share_id)
            .fetch_one(&mut *tx)
            .await?;
        check_writable(&first)?;
        // `FOR SHARE` waits out a writer holding the cluster row and then
        // returns the row it committed, but the probe's columns keep the
        // statement's snapshot, which predates that commit. They are read
        // again below when the cluster row changed after the snapshot.
        let probe_current: bool = first.try_get("probe_current")?;
        let probe = AppendProbe::from_row(&first)?;
        // The payout-revision fence: `Some((expected, observed))` when a
        // settlement moved the revision between the share's submit check and
        // this read, and the append captures its block (#657). The revision
        // compared is the latest one, as `FOR SHARE` returns it.
        let mut moved = None;
        if let Some(expected) = expected_revision {
            let revision: i64 = first.try_get("payout_revision")?;
            if revision != expected {
                // A plain share, or a block-bearing one when capture is off,
                // is refused here, before any write. A block-bearing share
                // with capture on is not: this transaction enqueues its block
                // as the #478 capture its submit check makes when it sees a
                // superseded revision, the share deferred until the block
                // confirms. Whether the block's parent is still the tip is the
                // offer's to decide, as for any capture: it probes the chain
                // and abandons a superseded parent. The capture-or-credit
                // decision is taken under this ORDER_LOCK and this FOR SHARE
                // read, so the attempt commits the credited share with its
                // candidate or the capture, never both.
                if !prepared.is_some_and(|prepared| prepared.has_capture()) {
                    return Err(PayoutRevisionChanged {
                        expected,
                        observed: revision,
                    }
                    .into());
                }
                moved = Some((expected, revision));
            }
        }
        let result = match (moved, prepared) {
            (Some(_), Some(prepared)) => {
                // A resubmitted proof whose share is already recorded is a
                // duplicate, and enqueues nothing: the probe and answer of a
                // capture at the submit check (`persist_block_only`).
                let recorded: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM qbit_share_ledger WHERE share_id=$1 AND share_seq>=qbit_prism_share_probe_floor())",
                )
                .bind(&share.share_id)
                .fetch_one(&mut *tx)
                .await?;
                if recorded {
                    if let Err(error) = tx.rollback().await {
                        tracing::debug!(%error, "rollback after a recorded share failed");
                    }
                    return Ok(AppendResult {
                        share,
                        inserted: false,
                        captured: false,
                    });
                }
                // An identical capture already enqueued writes nothing and
                // reports `captured: false`: a duplicate, as the block-only
                // path answers an existing outbox row.
                let captured = self.persist_prepared_capture(&mut tx, prepared).await?;
                AppendResult {
                    share,
                    inserted: false,
                    captured,
                }
            }
            _ => {
                let share = validated_share(share)?;
                let probe = if probe_current {
                    probe
                } else {
                    Self::read_append_probe(&mut tx, &share).await?
                };
                let result = self.append_probed(&mut tx, share, probe).await?;
                if let Some(prepared) = prepared {
                    self.persist_prepared_candidate(
                        &mut tx,
                        prepared,
                        Some(&result.share.share_id),
                    )
                    .await?;
                }
                result
            }
        };
        if pre_commit.is_some_and(|allow| !allow()) {
            // Release ORDER_LOCK before the refusal is observed.
            if let Err(error) = tx.rollback().await {
                tracing::debug!(%error, "rollback after a closed commit gate failed");
            }
            // append_probed returns inserted=false only after matching the entire
            // immutable, already-durable row, before any share/clock/hash write.
            // With no candidate write, rollback cannot undo that prior credit.
            if !result.inserted && prepared.is_none() {
                return Ok(result);
            }
            return Err(CommitGateClosed.into());
        }
        tx.commit().await?;
        if let (Some((admitted, observed)), true) = (moved, result.captured) {
            tracing::warn!(
                block = %prepared.map(|prepared| prepared.block_hash()).unwrap_or_default(),
                share_id = %result.share.share_id,
                admitted_revision = admitted,
                payout_revision = observed,
                "payout revision moved before a block-bearing share's commit; captured the block, its share deferred until the block confirms (#657)"
            );
        }
        Ok(result)
    }

    /// The share append's write path inside a transaction that already holds
    /// `ORDER_LOCK`: the share's own checks, [`Self::APPEND_PROBE_SQL`], then
    /// [`Self::append_probed`]. The settlement's deferred-share credit runs it
    /// inside the settlement's transaction; the share append reads the probe
    /// in its first statement instead (`append_prepared`, #711).
    pub(super) async fn append_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        share: AcceptedShare,
    ) -> Result<AppendResult> {
        if self.own_log_lost() {
            return Err(OwnLogLost.into());
        }
        let share = validated_share(share)?;
        let probe = Self::read_append_probe(tx, &share).await?;
        self.append_probed(tx, share, probe).await
    }

    /// [`Self::APPEND_PROBE_SQL`] for `share`, as a statement of its own.
    async fn read_append_probe(
        tx: &mut Transaction<'_, Postgres>,
        share: &AcceptedShare,
    ) -> Result<AppendProbe> {
        let row = sqlx::query(Self::APPEND_PROBE_SQL)
            .bind(share_header_hash(&share.share_id))
            .bind(&share.share_id)
            .fetch_one(&mut **tx)
            .await?;
        AppendProbe::from_row(&row)
    }

    /// What the share append reads before it probes for the share_id, as
    /// named columns. `$1` is the share's header hash and `$2` its share_id.
    ///
    /// The ledger is partitioned by share_seq (migration 017) and its
    /// share_id uniqueness is per leaf, so a share_id probe without a
    /// share_seq bound descends one index per attached partition.
    /// qbit_prism_share_hashes is the authority for accepted headers, but
    /// legacy rejected rows have no header mapping. Before a partition can
    /// leave, verification retains their exact IDs and sequences; imports
    /// register them on attachment. Unregistered rejected rows are in the
    /// release table below conversion_bound; native writers only insert
    /// accepted rows. So this reads the header's credited share_id
    /// (`credited`), the share_id's retained rejected sequence
    /// (`rejected_seq`), `conversion_bound`, and the probe's share_seq bounds
    /// (`probe_floor`, `probe_ceiling`), which [`Self::append_probed`] binds
    /// as parameters.
    ///
    /// The bounds are read in the same statement as the rest and under the
    /// same ORDER_LOCK. As a function call inside the probe's WHERE clause
    /// the floor was applied as a per-leaf filter and the executor descended
    /// every attached partition, the empty lead included; as bound values
    /// the executor prunes at startup to the leaves between them (the share
    /// append's transaction forces generic plans, see
    /// `APPEND_TRANSACTION_BEGIN`, so the probe is planned once per
    /// connection and pruned at run time rather than re-planned per share;
    /// the settlement's deferred-share credit runs this inside the
    /// settlement's transaction under the default mode, once per landed
    /// block, where a custom plan is fine). The ceiling is the next
    /// share_seq: every row that can exist is below it, because rows are
    /// appended under the lock this transaction holds and imported
    /// partitions carry sequences the ledger already handed out (archive
    /// attach refuses a partition holding a row at or above it). The two
    /// subqueries are the bodies of migration 016's
    /// `qbit_prism_share_probe_floor()` and `qbit_prism_share_next_seq()`,
    /// inlined because a SQL-language function is re-planned on every call;
    /// a test holds them equal.
    const APPEND_PROBE_SQL: &str = "SELECT (SELECT share_id FROM qbit_prism_share_hashes WHERE header_hash=$1) AS credited,\
         (SELECT share_seq FROM qbit_prism_rejected_share_ids WHERE share_id=$2) AS rejected_seq,conversion_bound,\
         COALESCE((SELECT COALESCE(lower_seq,0) FROM qbit_prism_share_partitions \
            WHERE state='attached' AND (lower_seq IS NULL OR lower_seq<=next_seq.value) \
            ORDER BY upper_seq DESC OFFSET 2 LIMIT 1),0) AS probe_floor,next_seq.value AS probe_ceiling \
         FROM qbit_prism_share_partitioning, \
         (SELECT CASE WHEN is_called THEN last_value+1 ELSE last_value END AS value \
            FROM qbit_share_ledger_share_seq_seq) AS next_seq WHERE singleton";

    /// The share append's first statement under `ORDER_LOCK` (#711), built
    /// once as [`APPEND_FIRST_READ_SQL`] and [`APPEND_FENCED_FIRST_READ_SQL`]:
    /// the write guard's columns ([`check_writable`] refuses on them), the
    /// cluster row's `payout_revision`, `probe_current`, and
    /// [`Self::APPEND_PROBE_SQL`]'s columns, with its parameters.
    ///
    /// A fenced append reads the cluster row `FOR SHARE`, as the separate
    /// fence statement did: a writer holding the row is waited out, and the
    /// row it committed is the one returned. Every other column keeps the
    /// statement's snapshot, taken before that wait, where the separate
    /// statements after the fence took a fresh one. `probe_current` says
    /// whether the cluster row returned is still the one in that snapshot
    /// (its `xmin` is compared with the snapshot's own read of the row, an
    /// InitPlan that the recheck does not re-run). When it is not, a writer
    /// committed while the read waited, and the caller reads the probe again
    /// in a statement of its own. The probe is a materialized CTE on the
    /// outer side of a `LEFT JOIN`, so `FOR SHARE` and its recheck involve
    /// the cluster row alone, and a missing probe row cannot hide the write
    /// guard's refusal behind an empty result.
    fn append_first_read_sql(fenced: bool) -> String {
        format!(
            "WITH probe AS MATERIALIZED ({}) SELECT {WRITABLE_COLUMNS},c.payout_revision,\
             c.xmin=(SELECT xmin FROM qbit_prism_cluster WHERE singleton) AS probe_current,probe.* \
             FROM qbit_prism_cluster c LEFT JOIN probe ON true WHERE c.singleton{}",
            Self::APPEND_PROBE_SQL,
            if fenced { " FOR SHARE OF c" } else { "" }
        )
    }

    /// The share_id probe between the bounds, then the fallback `condition`
    /// (on `$4`) only when that found nothing, in one statement (#711): the
    /// bounded probe is a materialized CTE, and `NOT EXISTS` over it is a
    /// one-time filter on the fallback, so a share found between the bounds
    /// never runs the fallback, and at most one branch returns a row. Both
    /// branches keep their bounds as parameters, so executor startup prunes
    /// each to its own leaves.
    fn bounded_probe_then_sql(condition: &str) -> String {
        format!(
            "WITH recent AS MATERIALIZED ({SELECT_SHARE} WHERE share_id=$1 AND share_seq>=$2 AND share_seq<$3 LIMIT 1) \
             SELECT * FROM recent UNION ALL \
             ({SELECT_SHARE} WHERE share_id=$1 AND {condition} AND NOT EXISTS(SELECT 1 FROM recent) LIMIT 1)"
        )
    }

    /// The share append's write (#711): the ledger clock, the share row and
    /// its header mapping in one statement. The clock is the same `GREATEST`
    /// as before, and the share's `accepted_at` the same expression of it.
    /// A share whose job was issued after that clock inserts nothing:
    /// `share_seq` comes back NULL and the caller refuses the share, which
    /// rolls the clock back with the transaction, as when the check ran
    /// between two statements. Nothing on these tables observes the
    /// difference: no trigger fires on an INSERT into either table, the
    /// cluster row's only trigger is on `fatal_error`, and
    /// qbit_prism_share_hashes has no foreign key (migration 016 dropped
    /// it). A missing partition still raises SQLSTATE 23514 from the ledger
    /// INSERT, which fails the whole statement, and `append_checked` still
    /// retries it.
    const APPEND_WRITE_SQL: &str = "WITH clock AS (UPDATE qbit_prism_cluster SET ledger_clock_ms=GREATEST(ledger_clock_ms,floor(extract(epoch FROM clock_timestamp())*1000)::bigint) WHERE singleton RETURNING ledger_clock_ms),\
         appended AS (INSERT INTO qbit_share_ledger(share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,credit_policy,accepted,writer_id,writer_epoch) \
            SELECT $1,$2,$3,decode($4,'hex'),$5::text::numeric,$6::text::numeric,$7,$8,to_timestamp($9::double precision/1000),$10,to_timestamp(clock.ledger_clock_ms::double precision/1000),$11,true,$12,0 \
            FROM clock WHERE $9<=clock.ledger_clock_ms RETURNING share_seq),\
         hashed AS (INSERT INTO qbit_prism_share_hashes(header_hash,share_id) SELECT $13,$1 FROM appended) \
         SELECT clock.ledger_clock_ms AS accepted_at_ms,appended.share_seq FROM clock LEFT JOIN appended ON true";

    /// The share append once `probe` has been read under `ORDER_LOCK`: the
    /// share_id probe, then, for a new share, [`Self::APPEND_WRITE_SQL`].
    ///
    /// The probe tries the newest partitions first, between the bounds, then
    /// a registered rejected sequence or the legacy range on an unmapped
    /// miss, so a new share never probes every retained leaf. Either
    /// fallback runs in the bounded probe's statement
    /// ([`Self::bounded_probe_then_sql`], #711). A credited header needs the
    /// full-parent fallback, including legacy worker-scoped duplicates mapped
    /// to an earlier row. Nothing bounds it, so executor startup could not
    /// prune it and would open every retained leaf on every duplicate; it
    /// stays a statement of its own that runs only when the bounded probe
    /// found nothing. A credited row that has left the online ledger cannot
    /// be compared and is refused as the duplicate it is.
    async fn append_probed(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        mut share: AcceptedShare,
        probe: AppendProbe,
    ) -> Result<AppendResult> {
        let AppendProbe {
            credited,
            rejected_seq,
            legacy_bound,
            floor,
            ceiling,
        } = probe;
        let existing = match (rejected_seq, &credited) {
            (Some(seq), _) => {
                sqlx::query(&APPEND_PROBE_THEN_REJECTED_SQL)
                    .bind(&share.share_id)
                    .bind(floor)
                    .bind(ceiling)
                    .bind(seq)
                    .fetch_optional(&mut **tx)
                    .await?
            }
            (None, None) => {
                sqlx::query(&APPEND_PROBE_THEN_LEGACY_SQL)
                    .bind(&share.share_id)
                    .bind(floor)
                    .bind(ceiling)
                    .bind(legacy_bound)
                    .fetch_optional(&mut **tx)
                    .await?
            }
            (None, Some(_)) => {
                let bounded = sqlx::query(&APPEND_BOUNDED_PROBE_SQL)
                    .bind(&share.share_id)
                    .bind(floor)
                    .bind(ceiling)
                    .fetch_optional(&mut **tx)
                    .await?;
                match bounded {
                    Some(row) => Some(row),
                    None => {
                        sqlx::query(&APPEND_FULL_PARENT_PROBE_SQL)
                            .bind(&share.share_id)
                            .fetch_optional(&mut **tx)
                            .await?
                    }
                }
            }
        };
        if let Some(row) = existing {
            let previous = share_from_row(&row)?;
            share.share_seq = previous.share_seq;
            share.accepted_at_ms = previous.accepted_at_ms;
            ensure!(share == previous, "duplicate share_id payload mismatch");
            return Ok(AppendResult {
                share: previous,
                inserted: false,
                captured: false,
            });
        }
        ensure!(
            rejected_seq.is_none(),
            "duplicate-share: rejected share_id is retained globally, and its share is archived"
        );
        if let Some(credited) = credited {
            ensure!(
                credited == share.share_id,
                "duplicate-share: header already credited globally"
            );
            bail!("duplicate-share: header already credited globally, and its share is archived");
        }
        let (accepted_at_ms, seq): (i64, Option<i64>) = sqlx::query_as(Self::APPEND_WRITE_SQL)
            .bind(&share.share_id)
            .bind(&share.miner_id)
            .bind(&share.order_key)
            .bind(&share.p2mr_program_hex)
            .bind(share.share_difficulty.to_string())
            .bind(share.network_difficulty.to_string())
            .bind(i64::try_from(share.template_height)?)
            .bind(&share.job_id)
            .bind(share.job_issued_at_ms)
            .bind(i64::from(share.ntime))
            .bind(&share.credit_policy)
            .bind(&self.instance_id)
            .bind(share_header_hash(&share.share_id))
            .fetch_one(&mut **tx)
            .await?;
        ensure!(
            share.job_issued_at_ms <= accepted_at_ms,
            "share references a job from the future"
        );
        let seq = seq.context("the share ledger INSERT returned no share_seq")?;
        share.share_seq = u64::try_from(seq)?;
        share.accepted_at_ms = accepted_at_ms;
        Ok(AppendResult {
            share,
            inserted: true,
            captured: false,
        })
    }

    /// Captures all three inputs under the same database boundary: ordered
    /// shares, prior balances and their revision. Timestamp barriers preserve
    /// the existing public audit format without relying on host clock sync.
    pub async fn snapshot(&self, network_difficulty: u128) -> Result<Snapshot> {
        Ok(self
            .snapshot_with_admission(network_difficulty, ReadAdmission::default(), None)
            .await?
            .into_inner()
            .snapshot)
    }

    /// Carry runtime build admission through balance/page decoding and cleanup.
    /// The anchor transaction and immutable share query contract are unchanged.
    pub(crate) async fn snapshot_with_admission(
        &self,
        network_difficulty: u128,
        completion: ReadAdmission,
        prior: Option<BlockingDrop<RetainedShares>>,
    ) -> Result<BlockingDrop<SnapshotCapture>> {
        let started = std::time::Instant::now();
        let weight = network_difficulty
            .checked_mul(qbit_prism::PRISM_WINDOW_MULTIPLIER)
            .context("window difficulty overflow")?;
        ensure!(weight > 0, "network difficulty must be positive");
        // A dual-writer frontend's windows carry a cut (`window/cut.rs`);
        // a single writer's are exactly 3.0's.
        let dual_writer_node = self
            .dual_writer_identity()
            .map(|identity| identity.node.index());
        // The retained window's entry for this node bounds the own-cut probe
        // below (`cut::OWN_CUT_SQL`); NULL without one.
        let retained_own = match (dual_writer_node, prior.as_ref().and_then(|prior| prior.cut)) {
            (Some(node), Some(cut)) => cut
                .get(u8::try_from(node)?)?
                .map(i64::try_from)
                .transpose()?,
            _ => None,
        };
        let mut tx = self.begin().await?;
        if dual_writer_node.is_some() {
            // The cut reads probe the (origin_node, share_seq) index, the own
            // one inside ORDER_LOCK; refuse before the locks rather than scan
            // the ledger under them.
            let indexed: bool = sqlx::query_scalar(cut::ORIGIN_INDEX_SQL)
                .fetch_one(&mut *tx)
                .await?;
            if let Some(metrics) = self.metrics.as_deref() {
                metrics.record_origin_index(indexed);
            }
            if !indexed {
                return Err(cut::OriginIndexMissing.into());
            }
        }
        self.lock(&mut tx, SETTLEMENT_LOCK).await?;
        let order = self
            .lock_order(&mut tx, crate::metrics::OrderLockHolder::Prepared)
            .await?;
        writable(&mut tx).await?;
        let row = sqlx::query("UPDATE qbit_prism_cluster SET ledger_clock_ms=GREATEST(ledger_clock_ms,floor(extract(epoch FROM clock_timestamp())*1000)::bigint)+1 WHERE singleton RETURNING ledger_clock_ms-1 AS anchor_ms,payout_revision").fetch_one(&mut *tx).await?;
        let anchor_ms: i64 = row.try_get("anchor_ms")?;
        let payout_revision: i64 = row.try_get("payout_revision")?;
        let (cutoff, own_bound, peer_high_water) = match dual_writer_node {
            None => (
                sqlx::query_scalar(ACCEPTED_CUTOFF_SQL)
                    .fetch_one(&mut *tx)
                    .await?,
                None,
                None,
            ),
            Some(node) => {
                let (cutoff, own, mark): (i64, Option<i64>, Option<i64>) =
                    // Cached, as 3.0's cutoff is, so no plan is made
                    // under ORDER_LOCK: a generic plan prunes the leaves
                    // below the retained entry at executor start, which the
                    // bounded-probe guard checks in both plan modes.
                    sqlx::query_as(&DUAL_CUTOFF_SQL)
                        .bind(node)
                        .bind(anchor_ms)
                        .bind(retained_own)
                        .fetch_one(&mut *tx)
                        .await?;
                (cutoff, own, mark)
            }
        };
        let rows = prior_balance_rows(&mut tx).await?;
        #[cfg(test)]
        let decode_hook = self.snapshot_decode_hook.lock().unwrap().clone();
        // #478: every account's debt, summed on the decoding thread, for the
        // carry-forward debt gauge.
        let debt = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(u64::MAX));
        let debt_sum = debt.clone();
        let prior_balances = completion
            .own(rows)
            .map_anyhow(move |rows| {
                #[cfg(test)]
                if let Some(hook) = decode_hook {
                    hook("balances");
                }
                let balances = decode_prior_balances(rows)?;
                let sum: i128 = balances
                    .iter()
                    .map(|balance| (-balance.balance_sats).max(0))
                    .sum();
                debt_sum.store(
                    u64::try_from(sum).unwrap_or(u64::MAX - 1),
                    std::sync::atomic::Ordering::Relaxed,
                );
                Ok(balances)
            })
            .await?;
        tx.commit().await?;
        drop(order);
        let debt = debt.load(std::sync::atomic::Ordering::Relaxed);
        if let Some(metrics) = self.metrics.as_deref().filter(|_| debt != u64::MAX) {
            metrics.record_carry_forward_debt(debt);
        }
        // Ledger rows are immutable and later commits receive a timestamp
        // strictly greater than this anchor. Release the ordering barrier
        // before scanning a potentially large payout window.
        let mut tx = self.begin().await?;
        // The tag names the writer whose history the rows below are read
        // from, so it is read in this transaction, never the anchor's (#619).
        // A pooled connection can still reach a fenced old primary while the
        // anchor ran on the promoted one, or the reverse; either way the tag
        // matches the rows, and a tag that is not the current writer's makes
        // the work prove its window before it is used.
        let timeline = WriterTimeline::read(&mut tx).await?;
        // A dual-writer window's cut: the own entry read under ORDER_LOCK
        // above, and the peer's, chosen against this anchor (`window/cut.rs`).
        // The peer mark the window's cut used, which the refresh compares.
        let (cut, peer_mark) = match dual_writer_node {
            Some(node) => {
                let own = own_bound.map_or(Ok(None), cut::positive_entry)?;
                let (peer, used) =
                    cut::read_peer_cut(&mut tx, 1 - node, peer_high_water, anchor_ms).await?;
                let cut = if node == 0 {
                    WindowCut::new(own, peer)?
                } else {
                    WindowCut::new(peer, own)?
                };
                (Some(cut), used)
            }
            None => (None, None),
        };
        let cursor = cutoff.checked_add(1).context("share sequence exhausted")?;
        // Without a retired window there is nothing to advance from; the
        // report still says so, because how often that happens is part of
        // what the refresh path costs.
        let mut report = AcquisitionReport::full(WindowAcquisition::NoPrior);
        if let Some(prior) = prior {
            match snapshot_delta::advance(
                &mut tx,
                prior,
                weight,
                anchor_ms,
                cutoff,
                cut,
                &completion,
            )
            .await?
            {
                Advance::Advanced {
                    shares,
                    leaf,
                    mut report,
                } => {
                    let snapshot = shares
                        .map_anyhow(move |shares| {
                            Ok(Snapshot {
                                anchor_ms,
                                share_seq: u64::try_from(cutoff)?,
                                payout_revision,
                                shares,
                                prior_balances: prior_balances.into_inner(),
                                cut,
                            })
                        })
                        .await?;
                    tx.commit().await?;
                    report.elapsed = started.elapsed();
                    if let Some(metrics) = self.metrics.as_deref() {
                        metrics.record_window_acquisition(report.outcome);
                    }
                    return snapshot
                        .map_anyhow(move |snapshot| {
                            Ok(SnapshotCapture {
                                snapshot,
                                leaf: Some(leaf),
                                acquisition: report,
                                timeline,
                                peer_mark,
                            })
                        })
                        .await;
                }
                Advance::Rejected(rejected) => report = rejected,
            }
        }
        // The window's top: the accepted cutoff, or with a cut the higher of
        // its two entries, each a row of its node the cut admits.
        let (top, cursor) = match cut.as_ref().map(WindowCut::top) {
            None => (Some(cutoff), cursor),
            Some(top) => {
                let top = top.map(i64::try_from).transpose()?;
                (top, top.unwrap_or(0) + 1)
            }
        };
        let before = match top {
            Some(top) => {
                snapshot_delta::leaf_witness(&mut tx, top, top, anchor_ms, cut.as_ref(), None)
                    .await?
            }
            None => None,
        };
        let page = format!(
            "{SELECT_SHARE} WHERE {} AND share_seq<$1 ORDER BY share_seq DESC LIMIT 4096",
            cut::window_eligibility_sql(2, cut.map(|_| 3))
        );
        let mut scan = completion.own((Vec::<AcceptedShare>::new(), weight, cursor));
        while scan.1 > 0 {
            let rows = sqlx::query(&page)
                .bind(scan.2)
                .bind(anchor_ms)
                .bind_cut(cut.as_ref())?
                .fetch_all(&mut *tx)
                .await?;
            if rows.is_empty() {
                break;
            }
            report.pages += 1;
            #[cfg(test)]
            let decode_hook = self.snapshot_decode_hook.lock().unwrap().clone();
            scan = completion
                .own(rows)
                .map_anyhow(move |rows| {
                    #[cfg(test)]
                    if let Some(hook) = decode_hook {
                        hook("shares");
                    }
                    let (mut shares, mut remaining, mut cursor) = scan.into_inner();
                    for row in rows {
                        let share = share_from_row(&row)?;
                        cursor = i64::try_from(share.share_seq)?;
                        remaining = remaining.saturating_sub(share.share_difficulty);
                        shares.push(share);
                        if remaining == 0 {
                            break;
                        }
                    }
                    Ok((shares, remaining, cursor))
                })
                .await?;
        }
        let snapshot = scan
            .map_anyhow(move |(mut shares, _, _)| {
                shares.reverse();
                Ok(Snapshot {
                    anchor_ms,
                    share_seq: u64::try_from(cutoff)?,
                    payout_revision,
                    shares,
                    prior_balances: prior_balances.into_inner(),
                    cut,
                })
            })
            .await?;
        let after = if let (Some(first), Some(top)) = (snapshot.shares.first(), top) {
            snapshot_delta::leaf_witness(
                &mut tx,
                i64::try_from(first.share_seq)?,
                top,
                anchor_ms,
                cut.as_ref(),
                Some(i64::try_from(snapshot.shares.len())?),
            )
            .await?
        } else {
            None
        };
        let leaf = after.filter(|witness| before.as_ref() == Some(witness));
        tx.commit().await?;
        report.window_rows = snapshot.shares.len();
        report.elapsed = started.elapsed();
        if let Some(metrics) = self.metrics.as_deref() {
            metrics.record_window_acquisition(report.outcome);
        }
        snapshot
            .map_anyhow(move |snapshot| {
                Ok(SnapshotCapture {
                    snapshot,
                    leaf,
                    acquisition: report,
                    timeline,
                    peer_mark,
                })
            })
            .await
    }
}

pub(super) async fn read_prior_balances(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<Vec<CarryForwardBalance>> {
    let rows = prior_balance_rows(tx).await?;
    tokio::task::spawn_blocking(move || decode_prior_balances(rows)).await?
}

const PRIOR_BALANCE_SQL: &str = "SELECT miner_id,payout_order_key,encode(p2mr_program,'hex') AS program,balance_sats::text AS balance FROM qbit_current_carry_forward_balances()";

async fn prior_balance_rows(tx: &mut Transaction<'_, Postgres>) -> Result<Vec<PgRow>, sqlx::Error> {
    sqlx::query(PRIOR_BALANCE_SQL).fetch_all(&mut **tx).await
}

fn decode_prior_balances(rows: Vec<PgRow>) -> Result<Vec<CarryForwardBalance>> {
    rows.into_iter()
        .map(|row| {
            Ok(CarryForwardBalance {
                recipient_id: row.try_get("miner_id")?,
                order_key: row.try_get("payout_order_key")?,
                p2mr_program_hex: row.try_get("program")?,
                balance_sats: row.try_get::<String, _>("balance")?.parse()?,
            })
        })
        .collect()
}

fn sort_balances(balances: &mut [CarryForwardBalance]) {
    balances.sort_by(|a, b| {
        a.order_key
            .cmp(&b.order_key)
            .then_with(|| a.recipient_id.cmp(&b.recipient_id))
            .then_with(|| a.p2mr_program_hex.cmp(&b.p2mr_program_hex))
    });
}

fn check_balances(
    balances: Vec<CarryForwardBalance>,
    expected: [u8; 32],
    source: BalanceSource,
) -> Result<Vec<CarryForwardBalance>, WindowError> {
    let mut digest_input = balances.clone();
    sort_balances(&mut digest_input);
    let actual = qbit_prism::prior_balances_digest(&digest_input);
    if actual != expected {
        return Err(match source {
            BalanceSource::Current => WindowError::PriorBalancesChanged { expected, actual },
            BalanceSource::AsIssued => WindowError::Decode(anyhow::anyhow!(
                "immutable balance snapshot digest mismatch"
            )),
        });
    }
    Ok(balances)
}

/// The mandatory keyset page bound. One page is about 2 MB of raw rows, and
/// each page runs under the connection's own `statement_timeout`.
const WINDOW_PAGE_ROWS: i64 = 4096;

/// Does every named share row exist? One statement and two primary-key
/// lookups, on the caller's connection or transaction.
///
/// [`Ledger::read_window`] probes both endpoints of a range inside its
/// `REPEATABLE READ READ ONLY` snapshot, so a pruned range becomes a typed
/// [`WindowError::Incomplete`] in one round trip, before any page is read,
/// mapped or hashed. The candidate enqueue and the prepared-record writes use
/// [`probe_window_holding`] instead, which also checks that the last row is
/// the window's own (#619).
///
/// On one writer timeline share rows are immutable, so a row that is absent
/// was pruned and a row that is present can never later stop matching the
/// window predicate. This is deliberately an existence test on `share_seq`
/// only, not the window predicate: it is a retention probe, never a
/// substitute for the per-page count and digest checks. Across an
/// asynchronous promotion a `share_seq` can name another share (#619), which
/// only [`probe_window_holding`] detects.
pub async fn probe_share_rows(
    connection: &mut sqlx::PgConnection,
    first_share_seq: i64,
    last_share_seq: i64,
) -> Result<bool, WindowError> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM qbit_share_ledger WHERE share_seq=$1) AND EXISTS(SELECT 1 FROM qbit_share_ledger WHERE share_seq=$2)",
    ).bind(first_share_seq).bind(last_share_seq).fetch_one(&mut *connection).await?)
}

/// The primary's WAL insertion timeline, the expression #466's leaf witness
/// reads. PostgreSQL increments it at every promotion, so it names the
/// writer a frontend's work was read from. Runtime-only: never serialized or
/// persisted. Under D3's single standby it tells successive writers apart; it
/// is not a global identity for sibling physical copies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriterTimeline(u32);

/// Select-list expression for [`WriterTimeline`]: the timeline field of the
/// WAL file name at the current insert position. It fails during recovery,
/// so every reader keeps it behind a `NOT pg_is_in_recovery()` guard or a
/// writable check.
pub(crate) const WRITER_TIMELINE_SQL: &str = "left(pg_walfile_name(pg_current_wal_lsn()),8)";

impl WriterTimeline {
    /// A fixture timeline for in-memory test ledgers.
    #[cfg(test)]
    pub(crate) const fn new(id: u32) -> Self {
        Self(id)
    }

    /// Parse the eight hex digits [`WRITER_TIMELINE_SQL`] returns.
    pub(crate) fn parse(hex: &str) -> Result<Self> {
        ensure!(
            hex.len() == 8 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "invalid writer timeline {hex:?}"
        );
        Ok(Self(u32::from_str_radix(hex, 16)?))
    }

    /// Read the timeline on the caller's connection or transaction. A failed
    /// statement stays a database error; only an unparsable answer is a
    /// decode error.
    pub(crate) async fn read(connection: &mut sqlx::PgConnection) -> Result<Self, WindowError> {
        let hex: String = sqlx::query_scalar(&format!("SELECT {WRITER_TIMELINE_SQL}"))
            .fetch_one(&mut *connection)
            .await?;
        Self::parse(&hex).map_err(WindowError::Decode)
    }
}

impl std::fmt::Display for WriterTimeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:08X}", self.0)
    }
}

/// What the current primary holds of a window reference's rows (#619).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowHolding {
    /// The window's last row is its own, and its first row is present. An
    /// empty window is always held.
    Held,
    /// The last row is its own, but the first row is gone: retention removed
    /// a prefix of the range.
    PrefixPruned,
    /// The last row is absent, or is another share that does not match the
    /// window's own predicate, or, in a dual-writer window, a node's entry
    /// or the peer's rows under it are missing: this primary does not hold
    /// the history the window was read from.
    NotHeld,
}

/// Does this primary hold `window`'s rows as the rows it was read from? One
/// statement and two primary-key lookups, on the caller's connection or
/// transaction.
///
/// Every native ledger writer appends under `ORDER_LOCK`, so `share_seq`
/// order is commit order, and a physical standby replays a prefix of the
/// WAL. A promoted primary therefore holds every row of a window exactly when
/// it holds the window's **last** row as the same share. Losing the end of
/// that prefix lets the promoted sequence hand the lost numbers out again, to
/// shares credited after the window's anchor. So the last row must exist and
/// satisfy the window's own predicate (`accepted`, accepted and issued by the
/// anchor), the predicate #466's leaf witness applies to an endpoint.
/// Retention only removes a prefix, never the newest row of work young
/// enough to be mined, so a missing first row alone is
/// [`WindowHolding::PrefixPruned`].
///
/// A reissued row can only match the predicate if the promoted host's clock
/// runs behind the old primary's by more than the time from the anchor to
/// its reissue. The landing's count and digest stay the proof either way.
///
/// **A dual-writer window** (one with a cut) holds rows from two logs, and
/// its database has no physical standby (CONTRACT D-19): the rows it can
/// lose are the newest of each log, to a restore. Own-log recovery brings
/// back the own rows the peer had pulled, in `share_seq` order, and the
/// peer sync pulls the peer's again from the restored mark. So the window is
/// held when, besides its last row, this node's entry in the window's range
/// is its row under the predicate (the own rows below it came back with it),
/// and the peer sync's safe mark is at or above the peer's entry
/// ([`cut::entries_held_sql`]). A rewound mark is [`WindowHolding::NotHeld`]
/// whatever retention did; a missing entry row is only when the first row
/// is present, since retention may have removed an entry under it.
pub async fn probe_window_holding(
    connection: &mut sqlx::PgConnection,
    window: &WindowRef,
) -> Result<WindowHolding, WindowError> {
    let Some(range) = window.shares else {
        return Ok(WindowHolding::Held);
    };
    let (first, last) = range.bounds()?;
    let endpoints = format!(
        "SELECT EXISTS(SELECT 1 FROM qbit_share_ledger WHERE share_seq=$1),\
         EXISTS(SELECT 1 FROM qbit_share_ledger WHERE share_seq=$2 AND {})",
        cut::window_eligibility_sql(3, window.cut.map(|_| 4))
    );
    let (first_present, last_held, entries_present, peer_rows_present) = match &window.cut {
        None => {
            let (first_present, last_held): (bool, bool) = sqlx::query_as(&endpoints)
                .bind(first)
                .bind(last)
                .bind(window.anchor_ms)
                .fetch_one(&mut *connection)
                .await?;
            (first_present, last_held, true, true)
        }
        Some(cut) => {
            sqlx::query_as(&format!("{endpoints},{}", cut::entries_held_sql(1, 3, 4)))
                .bind(first)
                .bind(last)
                .bind(window.anchor_ms)
                .bind_cut(Some(cut))?
                .fetch_one(&mut *connection)
                .await?
        }
    };
    Ok(
        match (
            last_held && peer_rows_present,
            first_present,
            entries_present,
        ) {
            (false, _, _) => WindowHolding::NotHeld,
            (true, false, _) => WindowHolding::PrefixPruned,
            (true, true, false) => WindowHolding::NotHeld,
            (true, true, true) => WindowHolding::Held,
        },
    )
}

/// A block candidate refused before its enqueue: the primary that would hold
/// it does not hold its window ([`WindowHolding::NotHeld`]). Its coinbase pays
/// a window no landing here could rebuild or audit, so it is never offered
/// (#619). The transaction is rolled back with the share it carried.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowNotHeld {
    pub block_hash: String,
    pub first_share_seq: u64,
    pub last_share_seq: u64,
    pub anchor_ms: i64,
}

impl std::fmt::Display for WindowNotHeld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "block {} refused before its offer: this primary does not hold its window {}..={} (anchor {}); the last row is absent or is another share, as after an asynchronous promotion lost the end of the history the work was read from (#619)",
            self.block_hash, self.first_share_seq, self.last_share_seq, self.anchor_ms
        )
    }
}

impl std::error::Error for WindowNotHeld {}

/// Read `first..=last` under the payout-window predicate, in ascending keyset
/// pages of at most 4096 rows, on the caller's connection or transaction.
///
/// The runtime thread only issues each statement and receives its wire
/// buffers. Mapping the rows (`share_from_row`) and whatever `consume` does
/// with them run together in one `spawn_blocking` task per page, which takes
/// the state and the rows and hands the state back, so no whole-window serde
/// or hashing ever touches a runtime thread. The same paging implementation
/// serves [`Ledger::read_window`], whose state is the growing share vector
/// and running SHA-256. That caller bounds each page by the reference's
/// `share_count` and checks the final count and digest. This generic API leaves
/// those checks to its consumer, because only it knows the reference.
///
/// `consume` is called once per page, with the page's shares in ascending
/// `share_seq`. The state and the closure both travel to the blocking thread,
/// so both must be `Send + 'static`; a consumer that compares against a vector
/// it does not own takes an `Arc` of it.
///
/// While the read is in flight the state is held in a guard that releases it
/// on a blocking thread if the future is dropped, so a cancellation never frees
/// an accumulated window on a runtime worker. After a successful return the
/// caller owns the state and that off-thread cleanup obligation.
///
/// The reader takes no deadline of its own: every statement runs under the
/// connection's `statement_timeout`, and dropping the future between pages
/// rolls the caller's transaction back and leaves at most one page of blocking
/// work running detached.
///
/// It applies 3.0's predicate, with no dual-writer cut: a dual-writer
/// window's range holds peer rows the window excludes, so it is read through
/// [`Ledger::read_window`], which applies the reference's cut.
pub async fn read_range_paged<S, F>(
    connection: &mut sqlx::PgConnection,
    first: i64,
    last: i64,
    anchor_ms: i64,
    state: S,
    consume: F,
) -> Result<S, WindowError>
where
    S: Send + 'static,
    F: FnMut(&mut S, Vec<AcceptedShare>) -> Result<(), WindowError> + Send + 'static,
{
    read_range_owned(
        connection,
        first,
        last,
        anchor_ms,
        None,
        &ReadAdmission::default(),
        state,
        consume,
    )
    .await
    .map(BlockingDrop::into_inner)
}

/// Keep the completion owner attached to the accumulated state until its
/// caller finishes validation and commits, or hands that state to cleanup.
/// A dual-writer window's `cut` bounds each node's rows as well
/// (`window/cut.rs`); without one the page statement is 3.0's.
// The parameter list is read_range_paged's plus the cut and the completion owner.
#[allow(clippy::too_many_arguments)]
async fn read_range_owned<S, F>(
    connection: &mut sqlx::PgConnection,
    first: i64,
    last: i64,
    anchor_ms: i64,
    cut: Option<&WindowCut>,
    completion: &ReadAdmission,
    state: S,
    consume: F,
) -> Result<BlockingDrop<S>, WindowError>
where
    S: Send + 'static,
    F: FnMut(&mut S, Vec<AcceptedShare>) -> Result<(), WindowError> + Send + 'static,
{
    let mut carried = completion.own((state, consume, first.saturating_sub(1)));
    // `read_range`'s predicate (`ledger/audit.rs`), paged forwards: both
    // bounds are known here, so rows arrive in canonical order and a digest
    // over them can stream. `$1` is the exclusive cursor, which starts one
    // below `first` and advances to each page's last `share_seq`.
    //
    // Each page is planned with its bounds (`persistent(false)`: an unnamed
    // statement, never a cached generic plan). On the partitioned ledger the
    // generic plan for a `share_seq` range with unknown bounds estimates a
    // few hundred rows, so it appends the leaves and sorts them instead of
    // walking the primary key in order; against a 400k-row window that sort
    // reads the whole remaining range on every page, forty times the cost
    // of the ordered scan. Planning a page costs a fraction of a millisecond.
    let page = format!(
        "{SELECT_SHARE} WHERE accepted AND share_seq>$1 AND share_seq<=$2 AND accepted_at<=to_timestamp($3::double precision/1000) AND job_issued_at<=to_timestamp($3::double precision/1000){} ORDER BY share_seq LIMIT {WINDOW_PAGE_ROWS}",
        cut::cut_clause(cut.map(|_| 4), None)
    );
    while carried.get().2 < last {
        let rows = sqlx::query(&page)
            .persistent(false)
            .bind(carried.get().2)
            .bind(last)
            .bind(anchor_ms)
            .bind_cut(cut)?
            .fetch_all(&mut *connection)
            .await?;
        if rows.is_empty() {
            break;
        }
        carried = carried
            .map(move |(mut state, mut consume, mut cursor)| {
                let shares = rows
                    .iter()
                    .map(share_from_row)
                    .collect::<Result<Vec<_>>>()
                    .map_err(WindowError::Decode)?;
                if let Some(last) = shares.last() {
                    cursor = i64::try_from(last.share_seq)
                        .map_err(|error| WindowError::Decode(error.into()))?;
                }
                consume(&mut state, shares)?;
                Ok((state, consume, cursor))
            })
            .await?;
    }
    let (state, consume, _) = carried.into_inner();
    // No await separates the guards. Re-own state before dropping the
    // consumer so even a panicking consumer destructor schedules cleanup.
    let state = completion.own(state);
    drop(consume);
    Ok(state)
}

/// Write the canonical encoding of an as-issued balance set, on the caller's
/// transaction, and return the [`qbit_prism::prior_balances_digest`] that keys
/// it. The writer for `save_job` and its repair, under `SETTLEMENT_LOCK`; the
/// candidate enqueue, which re-establishes what a candidate references under
/// `ORDER_LOCK`, prepares the same encoding before its transaction opens and
/// writes it through [`put_canonical_balance_snapshot`].
///
/// **Stored order.** The row holds compact
/// `serde_json::to_vec(&CarryForwardBalance)` bytes over the set sorted
/// bytewise by `(order_key, recipient_id, p2mr_program_hex)`, the digest's own
/// comparator, exactly as migration 008's column comment specifies. That sort
/// defines the stored encoding of the as-issued set and is what makes the row
/// a function of the set alone: any permutation of the same balances writes
/// the same bytes, so two frontends never disagree about a digest's content.
/// It is unrelated to the read path's order, which is never sorted:
/// `read_prior_balances` and the current-balance read keep their SQL order,
/// and `read_window(…, BalanceSource::AsIssued)` returns these bytes decoded,
/// so it returns the **stored, sorted** order.
///
/// **Immutability.** The insert is `ON CONFLICT DO NOTHING`, so a prune that
/// ran first costs nothing and a re-insert of the same set is free. On a
/// conflict the stored bytes are read back and compared with this encoding
/// byte for byte; a mismatch is [`WindowError::Decode`], because these rows
/// are immutable and can only disagree through corruption or a second,
/// incompatible encoding of the same digest.
///
/// The sort, the digest and the serialization are O(recipients) but are still
/// whole-set work, so they run in `spawn_blocking`; the runtime thread only
/// issues the statements.
pub async fn put_balance_snapshot(
    tx: &mut Transaction<'_, Postgres>,
    balances: &[CarryForwardBalance],
) -> Result<[u8; 32], WindowError> {
    let (digest, bytes) = BlockingDrop::new(balances.to_vec())
        .map(canonical_balance_snapshot)
        .await?
        .into_inner();
    put_canonical_balance_snapshot(tx, digest, &bytes).await?;
    Ok(digest)
}

/// Write a set's canonical encoding under its digest, on the caller's
/// transaction, or verify that the row already there holds exactly these
/// bytes: the statements of [`put_balance_snapshot`], for a caller that
/// prepared the encoding earlier and elsewhere, as the candidate enqueue
/// does before its transaction opens. `bytes` must be the canonical
/// encoding of the set `digest` names; the runtime thread only issues the
/// statements and compares the stored bytes.
pub(super) async fn put_canonical_balance_snapshot(
    tx: &mut Transaction<'_, Postgres>,
    digest: [u8; 32],
    bytes: &[u8],
) -> Result<(), WindowError> {
    let key = hex::encode(digest);
    let written = sqlx::query(
        "INSERT INTO qbit_prism_balance_snapshots(prior_balances_digest,balances) VALUES($1,$2) ON CONFLICT DO NOTHING",
    ).bind(&key).bind(bytes).execute(&mut **tx).await?.rows_affected();
    if written == 0 {
        let stored: Vec<u8> = sqlx::query_scalar(
            "SELECT balances FROM qbit_prism_balance_snapshots WHERE prior_balances_digest=$1",
        )
        .bind(&key)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(WindowError::BalanceSnapshotMissing { digest })?;
        if stored != bytes {
            return Err(WindowError::Decode(anyhow::anyhow!(
                "immutable balance snapshot {key} already holds {} bytes that differ from this window's canonical encoding of {} bytes",
                stored.len(),
                bytes.len()
            )));
        }
    }
    Ok(())
}

/// The canonical stored form of an as-issued balance set: its digest and the
/// bytes `read_window(…, BalanceSource::AsIssued)` decodes.
fn canonical_balance_snapshot(
    mut balances: Vec<CarryForwardBalance>,
) -> Result<([u8; 32], Vec<u8>), WindowError> {
    sort_balances(&mut balances);
    let digest = qbit_prism::prior_balances_digest(&balances);
    let bytes = serde_json::to_vec(&balances).map_err(|error| WindowError::Decode(error.into()))?;
    Ok((digest, bytes))
}

/// The canonical stored encoding of a candidate's as-issued set, if it is
/// the set `expected` names: the set is sorted in place into its stored
/// order and digested once, and only a set whose digest is `expected` is
/// encoded, once. Whole-set work for the preparing thread; the set is
/// dropped here either way.
pub(super) fn canonical_as_issued_snapshot(
    mut balances: Vec<CarryForwardBalance>,
    expected: [u8; 32],
) -> Result<Option<Vec<u8>>, WindowError> {
    sort_balances(&mut balances);
    if qbit_prism::prior_balances_digest(&balances) != expected {
        return Ok(None);
    }
    serde_json::to_vec(&balances)
        .map(Some)
        .map_err(|error| WindowError::Decode(error.into()))
}

struct DigestWriter<'a>(&'a mut Sha256);
impl std::io::Write for DigestWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// [`Ledger::read_window`]'s [`read_range_paged`] state: the window read so
/// far and the running digest of the bytes it has streamed.
struct WindowRead {
    shares: Vec<AcceptedShare>,
    digest: Sha256,
}
impl WindowRead {
    fn new() -> Self {
        Self {
            shares: Vec::new(),
            digest: Sha256::new(),
        }
    }

    fn push(&mut self, share: AcceptedShare) -> Result<(), WindowError> {
        self.digest
            .update(if self.shares.is_empty() { b"[" } else { b"," });
        serde_json::to_writer(DigestWriter(&mut self.digest), &share)
            .map_err(|error| WindowError::Decode(error.into()))?;
        self.shares.push(share);
        Ok(())
    }

    fn page(&mut self, shares: Vec<AcceptedShare>, expected: u64) -> Result<(), WindowError> {
        for share in shares {
            self.push(share)?;
        }
        if self.shares.len() as u64 > expected {
            return Err(WindowError::Incomplete {
                expected,
                got: self.shares.len() as u64,
            });
        }
        Ok(())
    }

    fn finish(mut self, range: ShareRange) -> Result<Vec<AcceptedShare>, WindowError> {
        let got = self.shares.len() as u64;
        if got != range.share_count {
            return Err(WindowError::Incomplete {
                expected: range.share_count,
                got,
            });
        }
        self.digest.update(b"]");
        let actual = self.digest.finalize().into();
        if actual != range.snapshot_sha256 {
            return Err(WindowError::SnapshotDigestMismatch {
                expected: range.snapshot_sha256,
                actual,
            });
        }
        Ok(self.shares)
    }
}

#[cfg(test)]
#[path = "window/reference_tests.rs"]
mod reference_tests;

/// Whether `error` is PostgreSQL refusing a row because the partitioned share
/// ledger has no partition for its `share_seq`.
///
/// SQLSTATE 23514 is `check_violation`, which the ledger also raises for its
/// own CHECK constraints (a bad `credit_policy`, a leaf's bound), so the
/// message is part of the identification: only the routing failure names the
/// missing partition, and only it is repaired by attaching the lead.
fn refused_for_want_of_a_partition(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<sqlx::Error>()
            .and_then(sqlx::Error::as_database_error)
            .is_some_and(|database| {
                database.code().as_deref() == Some("23514")
                    && database.message().contains("no partition of relation")
            })
    })
}

/// The share append's statements whose text is built from shared fragments,
/// built once rather than per share inside `ORDER_LOCK` (#711); see
/// [`Ledger::append_first_read_sql`], [`Ledger::bounded_probe_then_sql`] and
/// [`Ledger::append_probed`].
static APPEND_FIRST_READ_SQL: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| Ledger::append_first_read_sql(false));
static APPEND_FENCED_FIRST_READ_SQL: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| Ledger::append_first_read_sql(true));
static APPEND_PROBE_THEN_REJECTED_SQL: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| Ledger::bounded_probe_then_sql("share_seq=$4"));
static APPEND_PROBE_THEN_LEGACY_SQL: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| Ledger::bounded_probe_then_sql("share_seq<$4"));
static APPEND_BOUNDED_PROBE_SQL: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    format!("{SELECT_SHARE} WHERE share_id=$1 AND share_seq>=$2 AND share_seq<$3")
});
static APPEND_FULL_PARENT_PROBE_SQL: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| format!("{SELECT_SHARE} WHERE share_id=$1"));

/// What the share append reads before its share_id probe, from
/// [`Ledger::APPEND_PROBE_SQL`]'s columns; see there.
struct AppendProbe {
    credited: Option<String>,
    rejected_seq: Option<i64>,
    legacy_bound: i64,
    floor: i64,
    ceiling: i64,
}

impl AppendProbe {
    fn from_row(row: &PgRow) -> Result<Self> {
        Ok(Self {
            credited: row.try_get("credited")?,
            rejected_seq: row.try_get("rejected_seq")?,
            legacy_bound: row.try_get("conversion_bound")?,
            floor: row.try_get("probe_floor")?,
            ceiling: row.try_get("probe_ceiling")?,
        })
    }
}

/// The share append's checks of a share's own fields, with its P2MR program
/// in canonical hex.
fn validated_share(mut share: AcceptedShare) -> Result<AcceptedShare> {
    ensure!(
        share.share_difficulty > 0 && share.network_difficulty > 0,
        "share difficulty must be positive"
    );
    ensure!(
        share
            .credit_policy
            .as_deref()
            .is_none_or(|p| p == "stale-grace"),
        "invalid share credit policy"
    );
    let program = hex::decode(&share.p2mr_program_hex)?;
    ensure!(program.len() == 32, "P2MR program must be 32 bytes");
    share.p2mr_program_hex = hex::encode(program);
    Ok(share)
}

pub(super) fn share_header_hash(share_id: &str) -> String {
    if let Some(suffix) = share_id.get(share_id.len().saturating_sub(64)..) {
        if suffix.len() == 64 && suffix.bytes().all(|b| b.is_ascii_hexdigit()) {
            return suffix.to_ascii_lowercase();
        }
    }
    hex::encode(Sha256::digest(share_id.as_bytes()))
}

pub(super) fn share_from_row(row: &PgRow) -> Result<AcceptedShare> {
    Ok(AcceptedShare {
        share_seq: u64::try_from(row.try_get::<i64, _>("share_seq")?)?,
        share_id: row.try_get("share_id")?,
        miner_id: row.try_get("miner_id")?,
        order_key: row.try_get("payout_order_key")?,
        p2mr_program_hex: row.try_get("program")?,
        share_difficulty: row.try_get::<String, _>("difficulty")?.parse()?,
        network_difficulty: row.try_get::<String, _>("network_difficulty")?.parse()?,
        template_height: u64::try_from(row.try_get::<i64, _>("template_height")?)?,
        job_id: row.try_get("job_id")?,
        job_issued_at_ms: row
            .try_get::<DateTime<Utc>, _>("job_issued_at")?
            .timestamp_millis(),
        accepted_at_ms: row
            .try_get::<DateTime<Utc>, _>("accepted_at")?
            .timestamp_millis(),
        ntime: u32::try_from(row.try_get::<i64, _>("ntime")?)?,
        credit_policy: row.try_get("credit_policy")?,
    })
}
