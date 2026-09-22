//! One anchored window per refresh loop, never retained by issued work.
use super::*;
use prepared_storage::compact::{
    finish_refresh_body, prepare_refresh_body_unhashed, CanonicalCompactBalances, CompactOwner,
    RefreshBody,
};

mod compute;

pub(super) type CachedWindow = Arc<RefreshWindow>;
type CapturedWindow = CompactOwner<(
    CachedWindow,
    Result<RefreshBody>,
    Arc<tokio::sync::OwnedSemaphorePermit>,
)>;

pub(super) struct RefreshWindow {
    pub snapshot: Snapshot,
    pub reference: WindowRef,
    network: u128,
    leaf: Option<crate::ledger::LeafWitness>,
    anchored: Instant,
}

impl RefreshWindow {
    pub fn into_retained(self) -> crate::ledger::RetainedShares {
        crate::ledger::RetainedShares {
            network: self.network,
            anchor_ms: self.snapshot.anchor_ms,
            cutoff: self.snapshot.share_seq,
            shares: self.snapshot.shares,
            leaf: self.leaf,
        }
    }

    pub fn within_reanchor_interval(&self, interval: Duration) -> bool {
        self.anchored.elapsed() < interval
    }

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
            && self.within_reanchor_interval(interval)
    }
}

impl Coordinator {
    pub(super) async fn capture_refresh_window(
        &self,
        network: u128,
        permit: Arc<tokio::sync::OwnedSemaphorePermit>,
        prior: Option<crate::ledger::BlockingDrop<crate::ledger::RetainedShares>>,
        template: Value,
        suffix: String,
        inputs: BundleInputs,
    ) -> Result<CapturedWindow> {
        // The interval starts before the read, not after each template build.
        let anchored = Instant::now();
        let snapshot = self
            .work_ledger
            .snapshot_with_admission(
                network,
                crate::ledger::ReadAdmission::shared(permit.clone()),
                prior,
            )
            .await?;
        let source = CompactOwner::new((snapshot, permit))
            .spawn_blocking(move |(snapshot, permit)| {
                let admission = permit;
                let mut snapshot = snapshot.into_inner();
                snapshot.snapshot.prior_balances = CanonicalCompactBalances::prepare(
                    std::mem::take(&mut snapshot.snapshot.prior_balances),
                    &admission,
                )
                .into_original_build();
                CompactOwner::new((Arc::new(snapshot), admission))
            })
            .await?;
        let config = self.config.clone();
        let computed = compute::pipeline(
            source,
            |capture| {
                Ok(qbit_prism::CanonicalAuditHashPrefix::new(
                    &capture.snapshot.shares,
                )?)
            },
            |capture| WindowRef::from_snapshot(&capture.snapshot),
            move |capture| {
                prepare_refresh_body_unhashed(&config, &capture.snapshot, &template, suffix, inputs)
            },
            finish_refresh_body,
        )
        .await?;
        computed
            .spawn_blocking(move |computed| {
                let admission = computed.3;
                let (capture, reference, body, _) = computed;
                let crate::ledger::SnapshotCapture { snapshot, leaf } = Arc::try_unwrap(capture)
                    .ok()
                    .expect("both borrowed computations have finished");
                let reference = reference?;
                let window = Arc::new(RefreshWindow {
                    snapshot,
                    reference,
                    network,
                    leaf,
                    anchored,
                });
                // Keep the original cache behavior even when body preparation failed.
                // That error is propagated by capture_refresh before reservation.
                Ok(CompactOwner::new((window, body, admission)))
            })
            .await?
    }
}
