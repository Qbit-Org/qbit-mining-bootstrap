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

impl Ledger {
    /// Read the current revision and balance digest from one primary snapshot.
    ///
    /// This is separate from `read_window(AsIssued)`, which must still return
    /// original economics when today's balances differ. No share history or
    /// stored balance blob is read. Decode, sorting, hashing and vector drop
    /// stay inside one blocking closure. The caller owns its original deadline.
    pub async fn payout_state(&self) -> Result<PayoutState, WindowError> {
        let mut tx = self.pool.begin().await?;
        // Do not force READ WRITE: a connection configured read-only must fail
        // the same admission guard as payout_revision, not override that guard.
        // These are only reads, but both must use the same MVCC snapshot.
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *tx)
            .await?;
        let payout_revision = sqlx::query_scalar(
            "SELECT payout_revision FROM qbit_prism_cluster WHERE singleton AND fatal_error IS NULL AND NOT pg_is_in_recovery() AND current_setting('transaction_read_only')='off'",
        ).fetch_one(&mut *tx).await?;
        let rows = prior_balance_rows(&mut tx).await?;
        let prior_balances_digest = tokio::task::spawn_blocking(move || {
            let balances = decode_prior_balances(rows).map_err(WindowError::Decode)?;
            // The shared digest sorts by its bytewise semantic comparator.
            Ok::<_, WindowError>(qbit_prism::prior_balances_digest(&balances))
        })
        .await
        .map_err(|error| WindowError::Decode(error.into()))??;
        tx.commit().await?;
        Ok(PayoutState {
            payout_revision,
            prior_balances_digest,
        })
    }
}
