//! One anchored window per refresh loop, never retained by issued work.
use super::*;
use prepared_storage::compact::{
    finish_refresh_body_with, prepare_refresh_body_unhashed_with, CanonicalCompactBalances,
    CompactOwner, RefreshBody,
};

mod compute;

/// The refresh build's parallelism (share-array serialization, counted-share
/// fold, leaf and suffix): the process's builder pool as configured or
/// detected once; tests force many small chunks so the ordered path is
/// exercised on any host.
pub(in crate::coordinator) fn refresh_parallelism() -> qbit_prism::Parallelism {
    if cfg!(test) {
        return qbit_prism::Parallelism::new(3, 5);
    }
    static DETECTED: std::sync::OnceLock<qbit_prism::Parallelism> = std::sync::OnceLock::new();
    *DETECTED.get_or_init(qbit_prism::Parallelism::detect)
}

pub(super) type CachedWindow = Arc<RefreshWindow>;
type CapturedWindow = CompactOwner<(
    CachedWindow,
    Result<RefreshBody>,
    Arc<tokio::sync::OwnedSemaphorePermit>,
)>;

pub(super) struct RefreshWindow {
    pub snapshot: Snapshot,
    pub reference: WindowRef,
    /// How the ledger acquired `snapshot` and what it cost: logged and
    /// counted by the refresh that captured it, never a reuse input.
    pub acquisition: crate::ledger::AcquisitionReport,
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
        // The anchor transaction's revision is the one this work will carry;
        // record it before the build so the landing metric sees it as early
        // as the ledger probe it replaces did.
        self.metrics
            .revision_work_observed(snapshot.snapshot.payout_revision);
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
        let parallelism = refresh_parallelism();
        let computed = compute::pipeline(
            source,
            // One pass over the accepted shares feeds the canonical audit
            // prefix and the WindowRef's share-array digest together.
            move |capture| {
                Ok(qbit_prism::CanonicalAuditHashPrefix::new_with_share_digest(
                    &capture.snapshot.shares,
                    parallelism,
                )?)
            },
            |capture, digest| WindowRef::from_snapshot_with_digest(&capture.snapshot, digest?),
            move |capture| {
                prepare_refresh_body_unhashed_with(
                    &config,
                    &capture.snapshot,
                    &template,
                    suffix,
                    inputs,
                    parallelism,
                )
            },
            move |prefix, body| finish_refresh_body_with(prefix, body, parallelism),
        )
        .await?;
        computed
            .spawn_blocking(move |computed| {
                let admission = computed.3;
                let (capture, reference, body, _) = computed;
                let crate::ledger::SnapshotCapture {
                    snapshot,
                    leaf,
                    acquisition,
                } = Arc::try_unwrap(capture)
                    .ok()
                    .expect("both borrowed computations have finished");
                let reference = reference?;
                let window = Arc::new(RefreshWindow {
                    snapshot,
                    reference,
                    acquisition,
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
