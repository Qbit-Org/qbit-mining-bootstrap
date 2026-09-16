//! One anchored window per refresh loop, never retained by issued work.
use super::*;
use prepared_storage::compact::{CanonicalCompactBalances, CompactOwner};

pub(super) type CachedWindow = Arc<RefreshWindow>;

pub(super) struct RefreshWindow {
    pub snapshot: Snapshot,
    pub reference: WindowRef,
    network: u128,
    anchored: Instant,
}

impl RefreshWindow {
    pub fn reusable(
        &self,
        network: u128,
        share_seq: u64,
        state: crate::ledger::PayoutState,
        interval: Duration,
    ) -> bool {
        self.network == network
            && self.snapshot.share_seq == share_seq
            && self.snapshot.payout_revision == state.payout_revision
            && self.reference.prior_balances_digest == state.prior_balances_digest
            && self.anchored.elapsed() < interval
    }
}

impl Coordinator {
    pub(super) async fn capture_refresh_window(
        &self,
        network: u128,
        permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<CompactOwner<CachedWindow>> {
        // The interval starts before the read, not after each template build.
        let anchored = Instant::now();
        let snapshot = self
            .work_ledger
            .snapshot_with_admission(
                network,
                crate::ledger::ReadAdmission::shared(permit.clone()),
            )
            .await?;
        let source = CompactOwner::new((snapshot, permit));
        let result = source
            .spawn_blocking(move |(snapshot, permit)| {
                let admission = permit;
                let mut snapshot = snapshot.into_inner();
                snapshot.prior_balances = CanonicalCompactBalances::prepare(
                    std::mem::take(&mut snapshot.prior_balances),
                    &admission,
                )
                .into_original_build();
                let reference = WindowRef::from_snapshot(&snapshot)?;
                // Only this cache retains the full snapshot between refreshes.
                // No idle build permit, share clone, or new digest format is needed.
                // Full folds remain bounded by the existing builder; an incremental
                // engine needs separate refresh-budget evidence before adoption.
                Ok::<_, anyhow::Error>(CompactOwner::new((
                    Arc::new(RefreshWindow {
                        snapshot,
                        reference,
                        network,
                        anchored,
                    }),
                    admission,
                )))
            })
            .await??;
        let (window, permit) = result.into_inner();
        let window = CompactOwner::new(window);
        drop(permit);
        Ok(window)
    }
}
