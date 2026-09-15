//! Work preparation I/O; orchestration and miner decisions stay in Coordinator.
use super::*;
use crate::ledger::{
    CompactDependency, CompactPrepared, CompactRepair, IssuedJobSave, PayoutState, PoolBlock,
    PreparedDependency, PreparedTemplate, StoredCompactPrepared,
};
use futures_util::future::BoxFuture;

pub(super) trait WorkLedger: Send + Sync {
    // Instrument the real coordinator-owned cleanup, not the fake reader's drop.
    #[cfg(test)]
    fn compact_drop_probe(&self) -> Option<prepared_storage::compact::CompactDropProbe> {
        None
    }
    fn payout_revision(&self) -> BoxFuture<'_, Result<i64>>;
    // One coherent observation for balance-aware replacement lease admission.
    fn payout_state(&self) -> BoxFuture<'_, Result<PayoutState, WindowError>>;
    #[allow(dead_code)]
    fn read_window_with_permit<'a>(
        &'a self,
        window: &'a WindowRef,
        balances: BalanceSource,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> BoxFuture<'a, Result<Window, WindowError>>;
    #[allow(dead_code, clippy::too_many_arguments)]
    fn save_compact_prepared<'a>(
        &'a self,
        key: &'a str,
        record: &'a CompactPrepared,
        template: &'a PreparedTemplate,
        balances: &'a [qbit_prism::CarryForwardBalance],
        expected_current_revision: i64,
        expires_at_ms: i64,
    ) -> BoxFuture<'a, Result<bool>>;
    #[allow(dead_code)]
    fn compact_prepared<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<StoredCompactPrepared>>>;
    fn observe_chain_view<'a>(
        &'a self,
        tip: &'a str,
        height: u64,
        chainwork: &'a str,
    ) -> BoxFuture<'a, Result<i64>>;
    fn snapshot(&self, network: u128) -> BoxFuture<'_, Result<Snapshot>>;
    fn pool_blocks(&self) -> BoxFuture<'_, Result<Vec<PoolBlock>>>;
    fn reconcile<'a>(
        &'a self,
        observations: &'a [BlockObservation],
        height: u64,
        revision: i64,
    ) -> BoxFuture<'a, Result<()>>;
    fn save_job<'a>(
        &'a self,
        id: &'a str,
        payload: &'a Value,
        revision: i64,
        parent: &'a str,
        ttl: i64,
    ) -> BoxFuture<'a, Result<()>>;
    #[allow(clippy::too_many_arguments)]
    fn save_issued_job<'a>(
        &'a self,
        id: &'a str,
        payload: &'a Value,
        revision: i64,
        parent: &'a str,
        expires_at_ms: i64,
        dependency: PreparedDependency<'a>,
        repair_payload: Option<&'a Value>,
    ) -> BoxFuture<'a, Result<IssuedJobSave>>;
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
    fn payout_revision(&self) -> BoxFuture<'_, Result<i64>> {
        Box::pin(Ledger::payout_revision(self))
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
    fn compact_prepared<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<StoredCompactPrepared>>> {
        Box::pin(Ledger::compact_prepared(self, key))
    }
    fn observe_chain_view<'a>(
        &'a self,
        tip: &'a str,
        height: u64,
        chainwork: &'a str,
    ) -> BoxFuture<'a, Result<i64>> {
        Box::pin(Ledger::observe_chain_view(self, tip, height, chainwork))
    }
    fn snapshot(&self, network: u128) -> BoxFuture<'_, Result<Snapshot>> {
        Box::pin(Ledger::snapshot(self, network))
    }
    fn pool_blocks(&self) -> BoxFuture<'_, Result<Vec<PoolBlock>>> {
        Box::pin(Ledger::pool_blocks_for_reconcile(self))
    }
    fn reconcile<'a>(
        &'a self,
        observations: &'a [BlockObservation],
        height: u64,
        revision: i64,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(Ledger::reconcile_blocks_at_revision(
            self,
            observations,
            height,
            revision,
        ))
    }
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
    fn save_issued_job<'a>(
        &'a self,
        id: &'a str,
        payload: &'a Value,
        revision: i64,
        parent: &'a str,
        expires_at_ms: i64,
        dependency: PreparedDependency<'a>,
        repair_payload: Option<&'a Value>,
    ) -> BoxFuture<'a, Result<IssuedJobSave>> {
        Box::pin(Ledger::save_issued_job(
            self,
            id,
            payload,
            revision,
            parent,
            expires_at_ms,
            dependency,
            repair_payload,
        ))
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
