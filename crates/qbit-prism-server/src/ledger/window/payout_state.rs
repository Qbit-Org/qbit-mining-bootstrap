//! Coherent current economics for B's replacement-lease admission integration.
use super::*;

/// A coherent observation, not permission to credit a job by itself.
///
/// The caller must also check the exact publication, lease and absolute expiry,
/// recheck after waits, and retain the transactional revision fence at commit.
/// This current revision must never replace a reconstructed job's issued one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PayoutState {
    pub payout_revision: i64,
    pub prior_balances_digest: [u8; 32],
}

/// Refresh selection metadata and economics from one snapshot. This is not an
/// anchored window or publication authority; admission and publication waits
/// still require their own fresh observations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RefreshProbe {
    pub payout_state: PayoutState,
    pub accepted_share_seq: u64,
    /// The writer timeline of the same snapshot (#619).
    pub timeline: WriterTimeline,
    /// In dual-writer mode, the peer's share-stream high-water mark of the
    /// same snapshot (`window/cut.rs`): when it has moved past the mark a
    /// cached window was taken at, the peer's newer rows, which can all lie
    /// below `accepted_share_seq`, make that window stale. `None` for a
    /// single writer, which reads nothing more than 3.0 did, and before the
    /// first pull.
    pub peer_mark: Option<i64>,
}

const PAYOUT_REVISION_SQL: &str = "SELECT payout_revision FROM qbit_prism_cluster WHERE singleton AND fatal_error IS NULL AND NOT pg_is_in_recovery() AND current_setting('transaction_read_only')='off'";

/// [`PAYOUT_REVISION_SQL`] with the writer timeline in the same statement,
/// behind the same guards, so the refresh probe gains no round trip (#619).
fn refresh_revision_sql() -> String {
    format!("SELECT payout_revision, {WRITER_TIMELINE_SQL} FROM qbit_prism_cluster WHERE singleton AND fatal_error IS NULL AND NOT pg_is_in_recovery() AND current_setting('transaction_read_only')='off'")
}

/// The refresh probe's revision and writer timeline, from one statement.
async fn refresh_revision(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<(i64, WriterTimeline), WindowError> {
    let (payout_revision, timeline): (i64, String) = sqlx::query_as(&refresh_revision_sql())
        .fetch_one(&mut **tx)
        .await?;
    Ok((
        payout_revision,
        WriterTimeline::parse(&timeline).map_err(WindowError::Decode)?,
    ))
}

fn refresh_balances_sql() -> String {
    // Keep the guard in its own first SELECT: merely referencing share history
    // acquires relation locks before PostgreSQL can evaluate a fatal-state guard.
    // The left join emits one marked empty row when there are no balances, so
    // cutoff remains available without inventing a balance or changing its decoder.
    format!(
        "SELECT balances.*, cutoff.accepted_share_seq FROM (SELECT ({ACCEPTED_CUTOFF_SQL}) AS accepted_share_seq) cutoff LEFT JOIN (SELECT true AS has_balance, current_balances.* FROM ({PRIOR_BALANCE_SQL}) current_balances) balances ON true"
    )
}

impl Ledger {
    /// Read the current revision and balance digest from one primary snapshot.
    ///
    /// This is separate from `read_window(AsIssued)`, which must still return
    /// original economics when today's balances differ. No share history or
    /// stored balance blob is read. Decode, sorting, hashing and vector drop
    /// stay inside one blocking closure. The caller owns its original deadline.
    pub async fn payout_state(&self) -> Result<PayoutState, WindowError> {
        let mut tx = self.begin().await?;
        // Do not force READ WRITE: a connection configured read-only must fail
        // the same admission guard as payout_revision, not override that guard.
        // These are only reads, but both must use the same MVCC snapshot.
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *tx)
            .await?;
        let payout_revision = sqlx::query_scalar(PAYOUT_REVISION_SQL)
            .fetch_one(&mut *tx)
            .await?;
        let prior_balances_digest =
            current_balances_digest(&mut tx, &ReadAdmission::default()).await?;
        tx.commit().await?;
        Ok(PayoutState {
            payout_revision,
            prior_balances_digest,
        })
    }

    /// One checkout and MVCC snapshot for revision, accepted cutoff and digest.
    /// No share payload or stored balance blob is read. Unlike `payout_state`,
    /// this refresh-only observation depends on accepted-share metadata.
    /// Retain build admission through actual decode/hash cleanup, including
    /// when the async caller is cancelled while its blocking task is running.
    pub(crate) async fn refresh_probe(
        &self,
        completion: ReadAdmission,
    ) -> Result<RefreshProbe, WindowError> {
        let mut tx = self.begin().await?;
        // Preserve read-only configuration and the primary/fatal-state guard.
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *tx)
            .await?;
        let (payout_revision, timeline) = refresh_revision(&mut tx).await?;
        let (accepted_share_seq, prior_balances_digest) =
            refresh_balances(&mut tx, &completion).await?;
        let peer_mark = match self.dual_writer_identity() {
            Some(_) => {
                sqlx::query_scalar(super::cut::PEER_HIGH_WATER_SQL)
                    .fetch_one(&mut *tx)
                    .await?
            }
            None => None,
        };
        tx.commit().await?;
        Ok(RefreshProbe {
            payout_state: PayoutState {
                payout_revision,
                prior_balances_digest,
            },
            accepted_share_seq,
            timeline,
            peer_mark,
        })
    }
}

async fn current_balances_digest(
    tx: &mut Transaction<'_, Postgres>,
    completion: &ReadAdmission,
) -> Result<[u8; 32], WindowError> {
    let rows = prior_balance_rows(tx).await?;
    Ok(completion
        .own(rows)
        .map(digest_balance_rows)
        .await?
        .into_inner())
}

fn digest_balance_rows(rows: Vec<PgRow>) -> Result<[u8; 32], WindowError> {
    let balances = decode_prior_balances(rows).map_err(WindowError::Decode)?;
    // Decode, semantic sorting, hashing and destruction stay off-runtime.
    Ok(qbit_prism::prior_balances_digest(&balances))
}

async fn refresh_balances(
    tx: &mut Transaction<'_, Postgres>,
    completion: &ReadAdmission,
) -> Result<(u64, [u8; 32]), WindowError> {
    let rows = sqlx::query(&refresh_balances_sql())
        .fetch_all(&mut **tx)
        .await?;
    #[cfg(test)]
    let hash_calls = tests::HASH_CALLS.try_with(std::sync::Arc::clone).ok();
    #[cfg(test)]
    let hash_gate = tests::HASH_GATE.try_with(std::sync::Arc::clone).ok();
    Ok(completion
        .own(rows)
        .map(move |mut rows| {
            #[cfg(test)]
            if let Some(gate) = hash_gate {
                gate();
            }
            let first = rows.first().ok_or(sqlx::Error::RowNotFound)?;
            let accepted_share_seq = u64::try_from(first.try_get::<i64, _>("accepted_share_seq")?)
                .map_err(|error| WindowError::Decode(error.into()))?;
            if first.try_get::<Option<bool>, _>("has_balance")?.is_none() {
                // The outer-join placeholder is not a balance. Drop it on this
                // blocking thread like every real row, including on errors.
                rows.clear();
            }
            let digest = digest_balance_rows(rows)?;
            #[cfg(test)]
            if let Some(calls) = hash_calls {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            Ok((accepted_share_seq, digest))
        })
        .await?
        .into_inner())
}

#[cfg(test)]
#[path = "payout_state_tests.rs"]
mod tests;
