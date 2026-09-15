use super::*;

mod payout_state;
pub use payout_state::PayoutState;
mod blocking_drop;
use blocking_drop::BlockingDrop;

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
        let bounds = window.shares.map(ShareRange::bounds).transpose()?;
        let mut tx = self.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await?;
        let payout_revision = sqlx::query_scalar(
            "SELECT payout_revision FROM qbit_prism_cluster WHERE singleton AND fatal_error IS NULL AND NOT pg_is_in_recovery()",
        ).fetch_one(&mut *tx).await?;
        let expected_balances = window.prior_balances_digest;
        let balance_task = match balances {
            BalanceSource::Current => {
                let rows = prior_balance_rows(&mut tx).await?;
                tokio::task::spawn_blocking(move || {
                    let decoded = decode_prior_balances(rows).map_err(WindowError::Decode)?;
                    check_balances(decoded, expected_balances, balances).map(BlockingDrop::new)
                })
            }
            BalanceSource::AsIssued => {
                let bytes: Vec<u8> = sqlx::query_scalar(
                    "SELECT balances FROM qbit_prism_balance_snapshots WHERE prior_balances_digest=$1",
                ).bind(hex::encode(expected_balances)).fetch_optional(&mut *tx).await?
                    .ok_or(WindowError::BalanceSnapshotMissing { digest: expected_balances })?;
                tokio::task::spawn_blocking(move || {
                    let decoded = serde_json::from_slice(&bytes)
                        .map_err(|error| WindowError::Decode(error.into()))?;
                    check_balances(decoded, expected_balances, balances).map(BlockingDrop::new)
                })
            }
        };
        let prior_balances = balance_task.await.map_err(WindowError::TaskFailed)??;
        let shares = if let (Some(range), Some((first, last))) = (window.shares, bounds) {
            if !probe_share_rows(&mut tx, first, last).await? {
                // No page has been read; an endpoint probe is not a window count.
                return Err(WindowError::Incomplete {
                    expected: range.share_count,
                    got: 0,
                });
            }
            let state = read_range_paged(
                &mut tx,
                first,
                last,
                window.anchor_ms,
                WindowRead::new(),
                move |state, shares| state.page(shares, range.share_count),
            )
            .await?;
            let state = BlockingDrop::new(state);
            tokio::task::spawn_blocking(move || {
                state.into_inner().finish(range).map(BlockingDrop::new)
            })
            .await
            .map_err(WindowError::TaskFailed)??
        } else {
            BlockingDrop::new(Vec::new())
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
    /// The permit is not dropped with the future. It is moved into the
    /// module's blocking-drop guard, declared before the read begins, so a
    /// cancellation
    /// releases it on a blocking thread **after** the page state and the
    /// vectors awaiting commit have been released there: the in-progress
    /// read's own guards were created later, so they are dropped first and
    /// their blocking drops are queued first. A successful or failed return
    /// has nothing left to clean up off the runtime, so the permit is released
    /// inline the moment the read returns, before the caller's rebuild.
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
        let permit = BlockingDrop::new(permit);
        let outcome = self.read_window(window, balances).await;
        drop(permit.into_inner());
        outcome
    }

    /// Coordinate nodes by cumulative proof of work. A slower peer or an
    /// equal-work sibling cannot reverse another instance's accepted chain.
    pub async fn observe_chain_view(
        &self,
        tip: &str,
        height: u64,
        chainwork_hex: &str,
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
        let row=sqlx::query("SELECT payout_revision,best_chainwork=$1::text::numeric AS same_work,best_chainwork<$1::text::numeric AS more_work,best_tip_hash,best_tip_height FROM qbit_prism_cluster WHERE singleton FOR UPDATE")
            .bind(&work).fetch_one(&mut *tx).await?;
        let same: bool = row.try_get("same_work")?;
        let greater: bool = row.try_get("more_work")?;
        ensure!(
            greater || same,
            "local node is behind the cluster's cumulative chainwork"
        );
        let mut revision: i64 = row.try_get("payout_revision")?;
        if same {
            ensure!(
                row.try_get::<Option<String>, _>("best_tip_hash")?
                    .as_deref()
                    == Some(&tip)
                    && row.try_get::<Option<i64>, _>("best_tip_height")? == Some(height),
                "local node follows a conflicting equal-work chain tip"
            );
        } else {
            revision=sqlx::query_scalar("UPDATE qbit_prism_cluster SET best_chainwork=$1::text::numeric,best_tip_hash=$2,best_tip_height=$3,payout_revision=payout_revision+1,updated_at=clock_timestamp() WHERE singleton RETURNING payout_revision")
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
        // opens, so `ORDER_LOCK` is held only for the share append and the
        // insert of the prepared bytes, whatever the window size.
        if let Some(candidate) = &candidate {
            ensure!(
                candidate.deferred_share.is_none(),
                "credited candidates cannot also contain a deferred share"
            );
        }
        let prepared = candidate
            .as_ref()
            .map(|candidate| prepare_candidate_observed(candidate, proof_observed_at_ms))
            .transpose()?;
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
        if let Some(prepared) = &prepared {
            self.persist_prepared_candidate(&mut tx, prepared, Some(&result.share.share_id))
                .await?;
        }
        if pre_commit.is_some_and(|allow| !allow()) {
            // Release ORDER_LOCK before the refusal is observed.
            if let Err(error) = tx.rollback().await {
                tracing::debug!(%error, "rollback after a closed commit gate failed");
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
        let existing = sqlx::query(&format!("{SELECT_SHARE} WHERE share_id=$1"))
            .bind(&share.share_id)
            .fetch_optional(&mut **tx)
            .await?;
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
        let header_hash = share_header_hash(&share.share_id);
        let duplicate: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM qbit_prism_share_hashes WHERE header_hash=$1)",
        )
        .bind(&header_hash)
        .fetch_one(&mut **tx)
        .await?;
        ensure!(
            !duplicate,
            "duplicate-share: header already credited globally"
        );
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
        let weight = network_difficulty
            .checked_mul(8)
            .context("window difficulty overflow")?;
        ensure!(weight > 0, "network difficulty must be positive");
        let mut tx = self.begin().await?;
        self.lock(&mut tx, SETTLEMENT_LOCK).await?;
        self.lock(&mut tx, ORDER_LOCK).await?;
        writable(&mut tx).await?;
        let row = sqlx::query("UPDATE qbit_prism_cluster SET ledger_clock_ms=GREATEST(ledger_clock_ms,floor(extract(epoch FROM clock_timestamp())*1000)::bigint)+1 WHERE singleton RETURNING ledger_clock_ms-1 AS anchor_ms,payout_revision").fetch_one(&mut *tx).await?;
        let anchor_ms: i64 = row.try_get("anchor_ms")?;
        let payout_revision: i64 = row.try_get("payout_revision")?;
        let cutoff: i64 = sqlx::query_scalar(
            "SELECT COALESCE(max(share_seq),0) FROM qbit_share_ledger WHERE accepted",
        )
        .fetch_one(&mut *tx)
        .await?;
        let prior_balances = read_prior_balances(&mut tx).await?;
        tx.commit().await?;
        // Ledger rows are immutable and later commits receive a timestamp
        // strictly greater than this anchor. Release the ordering barrier
        // before scanning a potentially large payout window.
        let mut tx = self.begin().await?;
        let mut shares = Vec::new();
        let mut remaining = weight;
        let mut cursor = cutoff.checked_add(1).context("share sequence exhausted")?;
        while remaining > 0 {
            let rows = sqlx::query(&format!("{SELECT_SHARE} WHERE accepted AND share_seq<$1 AND accepted_at<=to_timestamp($2::double precision/1000) AND job_issued_at<=to_timestamp($2::double precision/1000) ORDER BY share_seq DESC LIMIT 4096"))
                .bind(cursor).bind(anchor_ms).fetch_all(&mut *tx).await?;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                let share = share_from_row(&row)?;
                cursor = i64::try_from(share.share_seq)?;
                remaining = remaining.saturating_sub(share.share_difficulty);
                shares.push(share);
                if remaining == 0 {
                    break;
                }
            }
        }
        shares.reverse();
        tx.commit().await?;
        Ok(Snapshot {
            anchor_ms,
            share_seq: u64::try_from(cutoff)?,
            payout_revision,
            shares,
            prior_balances,
        })
    }
}

pub(super) async fn read_prior_balances(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<Vec<CarryForwardBalance>> {
    let rows = prior_balance_rows(tx).await?;
    tokio::task::spawn_blocking(move || decode_prior_balances(rows)).await?
}

async fn prior_balance_rows(tx: &mut Transaction<'_, Postgres>) -> Result<Vec<PgRow>, sqlx::Error> {
    sqlx::query("SELECT miner_id,payout_order_key,encode(p2mr_program,'hex') AS program,balance_sats::text AS balance FROM qbit_current_carry_forward_balances()")
        .fetch_all(&mut **tx).await
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
/// or hashing ever touches a runtime thread. One implementation serves both
/// callers:
///
/// * [`Ledger::read_window`], whose state is the growing `Vec<AcceptedShare>`
///   and the running SHA-256, bounded per page by the reference's
///   `share_count` and checked in full after the last page;
/// * the landing re-read, whose state is the claim's existing share vector and
///   an offset, compared page by page inside the landing transaction. The same
///   final count and digest checks apply there; this reader performs neither,
///   because only the caller knows the reference.
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
    // `read_range`'s predicate (`ledger/audit.rs`), paged forwards: both
    // bounds are known here, so rows arrive in canonical order and a digest
    // over them can stream. `$1` is the exclusive cursor, which starts one
    // below `first` and advances to each page's last `share_seq`.
    let page = format!(
        "{SELECT_SHARE} WHERE accepted AND share_seq>$1 AND share_seq<=$2 AND accepted_at<=to_timestamp($3::double precision/1000) AND job_issued_at<=to_timestamp($3::double precision/1000) ORDER BY share_seq LIMIT {WINDOW_PAGE_ROWS}"
    );
    let mut carried = BlockingDrop::new((state, consume, first.saturating_sub(1)));
    while carried.get().2 < last {
        let rows = sqlx::query(&page)
            .bind(carried.get().2)
            .bind(last)
            .bind(anchor_ms)
            .fetch_all(&mut *connection)
            .await?;
        if rows.is_empty() {
            break;
        }
        carried = tokio::task::spawn_blocking(move || {
            let (mut state, mut consume, mut cursor) = carried.into_inner();
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
            Ok::<_, WindowError>(BlockingDrop::new((state, consume, cursor)))
        })
        .await
        .map_err(WindowError::TaskFailed)??;
    }
    Ok(carried.into_inner().0)
}

/// Write the canonical encoding of an as-issued balance set, on the caller's
/// transaction, and return the [`qbit_prism::prior_balances_digest`] that keys
/// it. Shared by every writer of the row: the candidate enqueue, which
/// re-establishes what a `leased` candidate references under `ORDER_LOCK`, and
/// `save_job` and its repair, under `SETTLEMENT_LOCK`.
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
    let owned = balances.to_vec();
    let (digest, bytes) = tokio::task::spawn_blocking(move || canonical_balance_snapshot(owned))
        .await
        .map_err(WindowError::TaskFailed)??;
    let key = hex::encode(digest);
    let written = sqlx::query(
        "INSERT INTO qbit_prism_balance_snapshots(prior_balances_digest,balances) VALUES($1,$2) ON CONFLICT DO NOTHING",
    ).bind(&key).bind(&bytes).execute(&mut **tx).await?.rows_affected();
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
    Ok(digest)
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

fn share_header_hash(share_id: &str) -> String {
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
