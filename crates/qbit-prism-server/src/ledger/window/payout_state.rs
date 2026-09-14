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
        let mut tx = self.begin().await?;
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
        let prior_balances_digest = balance_digest(move || decode_prior_balances(rows)).await?;
        tx.commit().await?;
        Ok(PayoutState {
            payout_revision,
            prior_balances_digest,
        })
    }
}

async fn balance_digest(
    decode: impl FnOnce() -> Result<Vec<CarryForwardBalance>> + Send + 'static,
) -> Result<[u8; 32], WindowError> {
    tokio::task::spawn_blocking(move || {
        let balances = decode().map_err(WindowError::Decode)?;
        // The shared digest sorts by its bytewise semantic comparator.
        Ok(qbit_prism::prior_balances_digest(&balances))
    })
    .await
    .map_err(WindowError::TaskFailed)?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn blocking_panic_is_task_failed_not_corruption() {
        let error = balance_digest(|| panic!("controlled balance decoder panic"))
            .await
            .unwrap_err();
        assert!(matches!(error, WindowError::TaskFailed(join) if join.is_panic()));
    }

    #[tokio::test]
    async fn corrupt_balance_is_decode_not_task_failed() {
        let error = balance_digest(|| {
            // The same numeric parse used by decode_prior_balances.
            let balance_sats: i128 = "corrupt balance".parse()?;
            Ok(vec![CarryForwardBalance {
                recipient_id: "prior".into(),
                order_key: "prior".into(),
                p2mr_program_hex: "22".repeat(32),
                balance_sats,
            }])
        })
        .await
        .unwrap_err();
        assert!(matches!(error, WindowError::Decode(_)));
        assert_eq!(
            balance_digest(|| Ok(Vec::new())).await.unwrap(),
            qbit_prism::prior_balances_digest(&[])
        );
    }
}
