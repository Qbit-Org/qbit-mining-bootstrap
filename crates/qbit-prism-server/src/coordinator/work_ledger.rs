//! Work preparation I/O; orchestration and miner decisions stay in Coordinator.
use super::*;
use crate::ledger::{
    BlockingDrop, ChainObservationState, ChainTransition, CompactDependency, CompactPrepared,
    CompactRepair, IssuedJobSave, PayoutState, PoolBlock, PreparedTemplate, ReadAdmission,
    StoredCompactPrepared,
};
use futures_util::future::BoxFuture;

pub(super) trait WorkLedger: Send + Sync {
    // Instrument the real coordinator-owned cleanup, not the fake reader's drop.
    #[cfg(test)]
    fn compact_drop_probe(&self) -> Option<prepared_storage::compact::CompactDropProbe> {
        None
    }
    fn payout_revision(&self) -> BoxFuture<'_, Result<i64>>;
    fn chain_observation_state(&self) -> BoxFuture<'_, Result<ChainObservationState>>;
    fn latest_accepted_share_seq(&self) -> BoxFuture<'_, Result<u64>>;
    // One coherent observation for balance-aware replacement lease admission.
    fn payout_state(&self) -> BoxFuture<'_, Result<PayoutState, WindowError>>;
    fn read_window_with_permit<'a>(
        &'a self,
        window: &'a WindowRef,
        balances: BalanceSource,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> BoxFuture<'a, Result<Window, WindowError>>;
    #[allow(clippy::too_many_arguments)]
    fn save_compact_prepared<'a>(
        &'a self,
        key: &'a str,
        record: &'a CompactPrepared,
        template: &'a PreparedTemplate,
        balances: &'a [qbit_prism::CarryForwardBalance],
        expected_current_revision: i64,
        expires_at_ms: i64,
    ) -> BoxFuture<'a, Result<bool>>;
    #[cfg(test)]
    fn compact_prepared<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<StoredCompactPrepared>>>;
    fn compact_prepared_with_admission<'a>(
        &'a self,
        key: &'a str,
        completion: ReadAdmission,
    ) -> BoxFuture<'a, Result<Option<BlockingDrop<StoredCompactPrepared>>>>;
    fn observe_chain_view<'a>(
        &'a self,
        tip: &'a str,
        height: u64,
        chainwork: &'a str,
    ) -> BoxFuture<'a, Result<i64>>;
    fn observe_chain_transition<'a>(
        &'a self,
        transition: &'a ChainTransition,
        tip: &'a str,
        height: u64,
        chainwork: &'a str,
        observed: &'a ChainObservationState,
    ) -> BoxFuture<'a, Result<i64>>;
    fn snapshot_with_admission(
        &self,
        network: u128,
        completion: ReadAdmission,
    ) -> BoxFuture<'_, Result<BlockingDrop<Snapshot>>>;
    fn pool_blocks(&self) -> BoxFuture<'_, Result<Vec<PoolBlock>>>;
    /// Returns how many blocks the committed reconciliation confirmed
    /// for the first time (`Ledger::reconcile_blocks_at_revision`).
    fn reconcile<'a>(
        &'a self,
        observations: &'a [BlockObservation],
        height: u64,
        revision: i64,
    ) -> BoxFuture<'a, Result<u64>>;
    #[cfg(test)]
    fn save_job<'a>(
        &'a self,
        id: &'a str,
        payload: &'a Value,
        revision: i64,
        parent: &'a str,
        ttl: i64,
    ) -> BoxFuture<'a, Result<()>>;
    #[allow(clippy::too_many_arguments)]
    fn save_issued_job_compact<'a>(
        &'a self,
        id: &'a str,
        payload: &'a Value,
        revision: i64,
        parent: &'a str,
        expires_at_ms: i64,
        dependency: CompactDependency<'a>,
        repair: Option<&'a CompactRepair>,
    ) -> BoxFuture<'a, Result<IssuedJobSave>>;
    fn job<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<Option<Value>>>;
    fn now_ms(&self) -> BoxFuture<'_, Result<i64>>;
}

impl WorkLedger for Ledger {
    fn latest_accepted_share_seq(&self) -> BoxFuture<'_, Result<u64>> {
        Box::pin(Ledger::latest_accepted_share_seq(self))
    }
    fn payout_revision(&self) -> BoxFuture<'_, Result<i64>> {
        Box::pin(Ledger::payout_revision(self))
    }
    fn chain_observation_state(&self) -> BoxFuture<'_, Result<ChainObservationState>> {
        Box::pin(Ledger::chain_observation_state(self))
    }
    fn payout_state(&self) -> BoxFuture<'_, Result<PayoutState, WindowError>> {
        Box::pin(Ledger::payout_state(self))
    }
    fn read_window_with_permit<'a>(
        &'a self,
        window: &'a WindowRef,
        balances: BalanceSource,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> BoxFuture<'a, Result<Window, WindowError>> {
        Box::pin(Ledger::read_window_with_permit(
            self, window, balances, permit,
        ))
    }
    fn save_compact_prepared<'a>(
        &'a self,
        key: &'a str,
        record: &'a CompactPrepared,
        template: &'a PreparedTemplate,
        balances: &'a [qbit_prism::CarryForwardBalance],
        expected_current_revision: i64,
        expires_at_ms: i64,
    ) -> BoxFuture<'a, Result<bool>> {
        Box::pin(Ledger::save_compact_prepared(
            self,
            key,
            record,
            template,
            balances,
            expected_current_revision,
            expires_at_ms,
        ))
    }
    #[cfg(test)]
    fn compact_prepared<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<StoredCompactPrepared>>> {
        Box::pin(Ledger::compact_prepared(self, key))
    }
    fn compact_prepared_with_admission<'a>(
        &'a self,
        key: &'a str,
        completion: ReadAdmission,
    ) -> BoxFuture<'a, Result<Option<BlockingDrop<StoredCompactPrepared>>>> {
        Box::pin(Ledger::compact_prepared_with_admission(
            self, key, completion,
        ))
    }
    fn observe_chain_view<'a>(
        &'a self,
        tip: &'a str,
        height: u64,
        chainwork: &'a str,
    ) -> BoxFuture<'a, Result<i64>> {
        Box::pin(Ledger::observe_chain_view(self, tip, height, chainwork))
    }
    fn observe_chain_transition<'a>(
        &'a self,
        transition: &'a ChainTransition,
        tip: &'a str,
        height: u64,
        chainwork: &'a str,
        observed: &'a ChainObservationState,
    ) -> BoxFuture<'a, Result<i64>> {
        Box::pin(Ledger::observe_chain_transition(
            self, transition, tip, height, chainwork, observed,
        ))
    }
    fn snapshot_with_admission(
        &self,
        network: u128,
        completion: ReadAdmission,
    ) -> BoxFuture<'_, Result<BlockingDrop<Snapshot>>> {
        Box::pin(Ledger::snapshot_with_admission(self, network, completion))
    }
    fn pool_blocks(&self) -> BoxFuture<'_, Result<Vec<PoolBlock>>> {
        Box::pin(Ledger::pool_blocks_for_reconcile(self))
    }
    fn reconcile<'a>(
        &'a self,
        observations: &'a [BlockObservation],
        height: u64,
        revision: i64,
    ) -> BoxFuture<'a, Result<u64>> {
        Box::pin(Ledger::reconcile_blocks_at_revision(
            self,
            observations,
            height,
            revision,
        ))
    }
    #[cfg(test)]
    fn save_job<'a>(
        &'a self,
        id: &'a str,
        payload: &'a Value,
        revision: i64,
        parent: &'a str,
        ttl: i64,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(Ledger::save_job(self, id, payload, revision, parent, ttl))
    }
    fn save_issued_job_compact<'a>(
        &'a self,
        id: &'a str,
        payload: &'a Value,
        revision: i64,
        parent: &'a str,
        expires_at_ms: i64,
        dependency: CompactDependency<'a>,
        repair: Option<&'a CompactRepair>,
    ) -> BoxFuture<'a, Result<IssuedJobSave>> {
        Box::pin(Ledger::save_issued_job_compact(
            self,
            id,
            payload,
            revision,
            parent,
            expires_at_ms,
            dependency,
            repair,
        ))
    }
    fn job<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<Option<Value>>> {
        Box::pin(Ledger::job(self, id))
    }
    fn now_ms(&self) -> BoxFuture<'_, Result<i64>> {
        Box::pin(async move {
            Ok(sqlx::query_scalar(
                "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint",
            )
            .fetch_one(&self.pool)
            .await?)
        })
    }
}
