use super::*;

mod payout_state;
pub use payout_state::PayoutState;
pub(crate) use payout_state::RefreshProbe;
pub(super) mod blocking_drop;
use blocking_drop::{BlockingDrop, ReadAdmission};
mod snapshot_delta;
pub(crate) use snapshot_delta::{LeafWitness, RetainedShares, SnapshotCapture};

const ACCEPTED_CUTOFF_SQL: &str =
    "SELECT COALESCE(max(share_seq),0) FROM qbit_share_ledger WHERE accepted";

#[derive(Clone, Debug)]
pub struct AppendResult {
    pub share: AcceptedShare,
    pub inserted: bool,
}

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
}

/// Immutable builder inputs; the issued/current revision belongs to the caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowRef {
    pub anchor_ms: i64,
    #[serde(with = "hex32")]
    pub prior_balances_digest: [u8; 32],
    pub shares: Option<ShareRange>,
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
        let shares = match (snapshot.shares.first(), snapshot.shares.last()) {
            (Some(first), Some(last)) => Some(ShareRange {
                first_share_seq: first.share_seq,
                last_share_seq: last.share_seq,
                share_count: u64::try_from(snapshot.shares.len())?,
                snapshot_sha256: share_array_digest(&snapshot.shares)?,
            }),
            _ => None,
        };
        Ok(Self {
            anchor_ms: snapshot.anchor_ms,
            prior_balances_digest: qbit_prism::prior_balances_digest(&snapshot.prior_balances),
            shares,
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
        self.append_checked(share, candidate, None, None, None)
            .await
    }

    pub async fn append_at_revision(
        &self,
        share: AcceptedShare,
        candidate: Option<Candidate>,
        expected_revision: i64,
    ) -> Result<AppendResult> {
        self.append_checked(share, candidate, None, Some(expected_revision), None)
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
        )
        .await
    }

    /// [`Ledger::append_at_revision_gated`], recording when the candidate's
    /// locally validated proof was observed (a wall clock, UNIX ms); see
    /// `ClaimLifecycle::proof_observed_at_ms`.
    pub async fn append_at_revision_gated_observed(
        &self,
        share: AcceptedShare,
        candidate: Option<Candidate>,
        proof_observed_at_ms: Option<i64>,
        expected_revision: i64,
        pre_commit: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<AppendResult> {
        self.append_checked(
            share,
            candidate,
            proof_observed_at_ms,
            Some(expected_revision),
            Some(pre_commit),
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
    ) -> Result<AppendResult> {
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
        let prepared = match candidate {
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
        let mut tx = self.begin().await?;
        self.lock(&mut tx, ORDER_LOCK).await?;
        writable(&mut tx).await?;
        if let Some(expected) = expected_revision {
            let revision:i64=sqlx::query_scalar("SELECT payout_revision FROM qbit_prism_cluster WHERE singleton AND fatal_error IS NULL FOR SHARE").fetch_one(&mut *tx).await?;
            ensure!(
                revision == expected,
                "payout revision changed before share commit"
            );
        }
        let result = self.append_in(&mut tx, share).await?;
        if let Some(prepared) = prepared {
            self.persist_prepared_candidate(&mut tx, prepared, Some(&result.share.share_id))
                .await?;
        }
        if pre_commit.is_some_and(|allow| !allow()) {
            // Release ORDER_LOCK before the refusal is observed.
            if let Err(error) = tx.rollback().await {
                tracing::debug!(%error, "rollback after a closed commit gate failed");
            }
            // append_in returns inserted=false only after matching the entire
            // immutable, already-durable row, before any share/clock/hash write.
            // With no candidate write, rollback cannot undo that prior credit.
            if !result.inserted && prepared.is_none() {
                return Ok(result);
            }
            return Err(CommitGateClosed.into());
        }
        tx.commit().await?;
        Ok(result)
    }

    pub(super) async fn append_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        mut share: AcceptedShare,
    ) -> Result<AppendResult> {
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
        // The ledger is partitioned by share_seq (migration 017) and its
        // share_id uniqueness is per leaf, so a share_id probe without a
        // share_seq bound descends one index per attached partition.
        // qbit_prism_share_hashes is the authority for accepted headers, but
        // legacy rejected rows have no header mapping. Before a partition can
        // leave, verification retains their exact IDs and sequences; imports
        // register them on attachment. Unregistered rejected rows are in the
        // release table below conversion_bound; native writers only insert
        // accepted rows. Probe the newest partitions first, then a registered
        // rejected sequence or the legacy range on an unmapped miss, so a new
        // share never probes every retained leaf. A credited header needs the
        // full-parent fallback, including legacy worker-scoped duplicates
        // mapped to an earlier row.
        // A credited row that has left the online ledger cannot be compared
        // and is refused as the duplicate it is.
        let header_hash = share_header_hash(&share.share_id);
        let (credited, rejected_seq, legacy_bound): (Option<String>, Option<i64>, i64) = sqlx::query_as(
            "SELECT (SELECT share_id FROM qbit_prism_share_hashes WHERE header_hash=$1),\
             (SELECT share_seq FROM qbit_prism_rejected_share_ids WHERE share_id=$2),conversion_bound \
             FROM qbit_prism_share_partitioning WHERE singleton",
        )
        .bind(&header_hash)
        .bind(&share.share_id)
        .fetch_one(&mut **tx)
        .await?;
        let mut existing = sqlx::query(&format!(
            "{SELECT_SHARE} WHERE share_id=$1 AND share_seq>=qbit_prism_share_probe_floor()"
        ))
        .bind(&share.share_id)
        .fetch_optional(&mut **tx)
        .await?;
        if existing.is_none() {
            existing = if let Some(seq) = rejected_seq {
                sqlx::query(&format!(
                    "{SELECT_SHARE} WHERE share_id=$1 AND share_seq=$2"
                ))
                .bind(&share.share_id)
                .bind(seq)
                .fetch_optional(&mut **tx)
                .await?
            } else if credited.is_some() {
                sqlx::query(&format!("{SELECT_SHARE} WHERE share_id=$1"))
                    .bind(&share.share_id)
                    .fetch_optional(&mut **tx)
                    .await?
            } else {
                sqlx::query(&format!(
                    "{SELECT_SHARE} WHERE share_id=$1 AND share_seq<$2"
                ))
                .bind(&share.share_id)
                .bind(legacy_bound)
                .fetch_optional(&mut **tx)
                .await?
            };
        }
        if let Some(row) = existing {
            let previous = share_from_row(&row)?;
            share.share_seq = previous.share_seq;
            share.accepted_at_ms = previous.accepted_at_ms;
            ensure!(share == previous, "duplicate share_id payload mismatch");
            return Ok(AppendResult {
                share: previous,
                inserted: false,
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
        let accepted_at_ms: i64 = sqlx::query_scalar("UPDATE qbit_prism_cluster SET ledger_clock_ms=GREATEST(ledger_clock_ms,floor(extract(epoch FROM clock_timestamp())*1000)::bigint) WHERE singleton RETURNING ledger_clock_ms").fetch_one(&mut **tx).await?;
        ensure!(
            share.job_issued_at_ms <= accepted_at_ms,
            "share references a job from the future"
        );
        let seq: i64 = sqlx::query_scalar("INSERT INTO qbit_share_ledger(share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,credit_policy,accepted,writer_id,writer_epoch) VALUES($1,$2,$3,decode($4,'hex'),$5::text::numeric,$6::text::numeric,$7,$8,to_timestamp($9::double precision/1000),$10,to_timestamp($11::double precision/1000),$12,true,$13,0) RETURNING share_seq")
            .bind(&share.share_id).bind(&share.miner_id).bind(&share.order_key).bind(&share.p2mr_program_hex)
            .bind(share.share_difficulty.to_string()).bind(share.network_difficulty.to_string()).bind(i64::try_from(share.template_height)?)
            .bind(&share.job_id).bind(share.job_issued_at_ms).bind(i64::from(share.ntime)).bind(accepted_at_ms).bind(&share.credit_policy).bind(&self.instance_id)
            .fetch_one(&mut **tx).await?;
        sqlx::query("INSERT INTO qbit_prism_share_hashes(header_hash,share_id) VALUES($1,$2)")
            .bind(header_hash)
            .bind(&share.share_id)
            .execute(&mut **tx)
            .await?;
        share.share_seq = u64::try_from(seq)?;
        share.accepted_at_ms = accepted_at_ms;
        Ok(AppendResult {
            share,
            inserted: true,
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
        let weight = network_difficulty
            .checked_mul(qbit_prism::PRISM_WINDOW_MULTIPLIER)
            .context("window difficulty overflow")?;
        ensure!(weight > 0, "network difficulty must be positive");
        let mut tx = self.begin().await?;
        self.lock(&mut tx, SETTLEMENT_LOCK).await?;
        self.lock(&mut tx, ORDER_LOCK).await?;
        writable(&mut tx).await?;
        let row = sqlx::query("UPDATE qbit_prism_cluster SET ledger_clock_ms=GREATEST(ledger_clock_ms,floor(extract(epoch FROM clock_timestamp())*1000)::bigint)+1 WHERE singleton RETURNING ledger_clock_ms-1 AS anchor_ms,payout_revision").fetch_one(&mut *tx).await?;
        let anchor_ms: i64 = row.try_get("anchor_ms")?;
        let payout_revision: i64 = row.try_get("payout_revision")?;
        let cutoff: i64 = sqlx::query_scalar(ACCEPTED_CUTOFF_SQL)
            .fetch_one(&mut *tx)
            .await?;
        let rows = prior_balance_rows(&mut tx).await?;
        #[cfg(test)]
        let decode_hook = self.snapshot_decode_hook.lock().unwrap().clone();
        let prior_balances = completion
            .own(rows)
            .map_anyhow(move |rows| {
                #[cfg(test)]
                if let Some(hook) = decode_hook {
                    hook("balances");
                }
                decode_prior_balances(rows)
            })
            .await?;
        tx.commit().await?;
        // Ledger rows are immutable and later commits receive a timestamp
        // strictly greater than this anchor. Release the ordering barrier
        // before scanning a potentially large payout window.
        let mut tx = self.begin().await?;
        let cursor = cutoff.checked_add(1).context("share sequence exhausted")?;
        if let Some(prior) = prior {
            if let Some((shares, leaf)) = snapshot_delta::advance(
                &mut tx,
                prior,
                network_difficulty,
                weight,
                anchor_ms,
                cutoff,
                &completion,
            )
            .await?
            {
                let snapshot = shares
                    .map_anyhow(move |shares| {
                        Ok(Snapshot {
                            anchor_ms,
                            share_seq: u64::try_from(cutoff)?,
                            payout_revision,
                            shares,
                            prior_balances: prior_balances.into_inner(),
                        })
                    })
                    .await?;
                tx.commit().await?;
                return snapshot
                    .map_anyhow(move |snapshot| {
                        Ok(SnapshotCapture {
                            snapshot,
                            leaf: Some(leaf),
                        })
                    })
                    .await;
            }
        }
        let before = snapshot_delta::leaf_witness(&mut tx, cutoff, cutoff, anchor_ms, None).await?;
        let mut scan = completion.own((Vec::<AcceptedShare>::new(), weight, cursor));
        while scan.1 > 0 {
            let rows = sqlx::query(&format!(
                "{SELECT_SHARE} WHERE {} AND share_seq<$1 ORDER BY share_seq DESC LIMIT 4096",
                super::audit::anchored_eligibility_sql(2)
            ))
            .bind(scan.2)
            .bind(anchor_ms)
            .fetch_all(&mut *tx)
            .await?;
            if rows.is_empty() {
                break;
            }
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
                })
            })
            .await?;
        let after = if let Some(first) = snapshot.shares.first() {
            snapshot_delta::leaf_witness(
                &mut tx,
                i64::try_from(first.share_seq)?,
                cutoff,
                anchor_ms,
                Some(i64::try_from(snapshot.shares.len())?),
            )
            .await?
        } else {
            None
        };
        let leaf = after.filter(|witness| before.as_ref() == Some(witness));
        tx.commit().await?;
        snapshot
            .map_anyhow(move |snapshot| Ok(SnapshotCapture { snapshot, leaf }))
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
/// Two callers share it, and both need it inside a transaction of their own:
///
/// * [`Ledger::read_window`] probes both endpoints of a range inside its
///   `REPEATABLE READ READ ONLY` snapshot, so a pruned range becomes a typed
///   [`WindowError::Incomplete`] in one round trip, before any page is read,
///   mapped or hashed;
/// * the candidate enqueue probes a non-empty window's `first_share_seq`
///   alone, under `ORDER_LOCK` and in the transaction that writes the
///   candidate, by passing that sequence as **both** bounds. Retention only
///   ever removes a prefix, so the presence of the first row means the whole
///   range is present, and the committed row then holds the retention floor.
///   The same probe backs the `save_job` reservation check.
///
/// Share rows are immutable, so a row that is absent was pruned and a row that
/// is present can never later stop matching the window predicate. This is
/// deliberately an existence test on `share_seq` only, not the window
/// predicate: it is a retention probe, never a substitute for the per-page
/// count and digest checks.
pub async fn probe_share_rows(
    connection: &mut sqlx::PgConnection,
    first_share_seq: i64,
    last_share_seq: i64,
) -> Result<bool, WindowError> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM qbit_share_ledger WHERE share_seq=$1) AND EXISTS(SELECT 1 FROM qbit_share_ledger WHERE share_seq=$2)",
    ).bind(first_share_seq).bind(last_share_seq).fetch_one(&mut *connection).await?)
}

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
        &ReadAdmission::default(),
        state,
        consume,
    )
    .await
    .map(BlockingDrop::into_inner)
}

/// Keep the completion owner attached to the accumulated state until its
/// caller finishes validation and commits, or hands that state to cleanup.
async fn read_range_owned<S, F>(
    connection: &mut sqlx::PgConnection,
    first: i64,
    last: i64,
    anchor_ms: i64,
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
        "{SELECT_SHARE} WHERE accepted AND share_seq>$1 AND share_seq<=$2 AND accepted_at<=to_timestamp($3::double precision/1000) AND job_issued_at<=to_timestamp($3::double precision/1000) ORDER BY share_seq LIMIT {WINDOW_PAGE_ROWS}"
    );
    while carried.get().2 < last {
        let rows = sqlx::query(&page)
            .persistent(false)
            .bind(carried.get().2)
            .bind(last)
            .bind(anchor_ms)
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
