//! Work preparation I/O; orchestration and miner decisions stay in Coordinator.
use super::*;
use crate::ledger::{IssuedJobSave, PoolBlock, PreparedDependency};
use futures_util::future::BoxFuture;

pub(super) trait WorkLedger: Send + Sync {
    fn payout_revision(&self) -> BoxFuture<'_, Result<i64>>;
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
    fn job<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<Option<Value>>>;
    fn now_ms(&self) -> BoxFuture<'_, Result<i64>>;
}

impl WorkLedger for Ledger {
    fn payout_revision(&self) -> BoxFuture<'_, Result<i64>> {
        Box::pin(Ledger::payout_revision(self))
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
