//! The two storage boundaries used by ordinary share admission.
//!
//! Tests replace I/O here, not Coordinator's classification/accounting logic.
//! Production appends retain the transaction-scoped payout revision check.
use super::*;
use futures_util::future::BoxFuture;

pub(super) trait SubmitLedger: Send + Sync {
    fn payout_revision(&self) -> BoxFuture<'_, Result<i64>>;
    fn append_at_revision(
        &self,
        share: AcceptedShare,
        candidate: Option<Candidate>,
        revision: i64,
    ) -> BoxFuture<'_, Result<bool>>;
}

impl SubmitLedger for Ledger {
    fn payout_revision(&self) -> BoxFuture<'_, Result<i64>> {
        Box::pin(Ledger::payout_revision(self))
    }

    fn append_at_revision(
        &self,
        share: AcceptedShare,
        candidate: Option<Candidate>,
        revision: i64,
    ) -> BoxFuture<'_, Result<bool>> {
        Box::pin(async move {
            Ok(Ledger::append_at_revision(self, share, candidate, revision)
                .await?
                .inserted)
        })
    }
}
