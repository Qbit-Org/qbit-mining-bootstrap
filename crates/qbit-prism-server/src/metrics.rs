//! Bounded native telemetry. Event producers share one registry per instance.
pub mod collectors;
mod events;
mod labels;
mod landing;
mod landing_acks;
mod registry;
pub mod runtime;
mod snapshots;

pub(crate) use events::time_pool_acquire;
pub use labels::*;
pub use registry::{descriptors, Descriptor, Kind, BUCKETS};
use registry::{Family, Registry};
pub(crate) use snapshots::add_known_health_fields;
pub use snapshots::{DatabaseMetrics, DeliveryMetrics, ProcessMetrics};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

pub const COLLECTOR_STALE_AFTER: Duration = Duration::from_secs(30);

#[derive(Default)]
struct CollectionState {
    last_success: Option<Instant>,
    version: u64,
}

/// What one readiness attempt actually learned about the node. An absent field
/// is unknown to this attempt, never a carried-over or invented observation.
#[derive(Clone, Copy, Debug)]
pub struct NodeObservation {
    /// When this attempt began. It orders concurrent attempts, so a slower
    /// older one cannot overwrite a newer observation when it finally lands.
    pub started: Instant,
    /// `getnetworkinfo.connections`, absent unless that call was made and
    /// answered with a peer count. A node answering zero peers is a real zero.
    pub peers: Option<u64>,
    /// `getblockchaininfo.initialblockdownload` and the instant that call
    /// answered, absent unless it answered with a boolean.
    pub chain: Option<(bool, Instant)>,
}

impl NodeObservation {
    /// Every field starts unknown; the attempt fills in what it learns.
    pub fn started() -> Self {
        Self {
            started: Instant::now(),
            peers: None,
            chain: None,
        }
    }
}

#[derive(Default)]
struct NodeState {
    /// Start of the attempt whose values are published.
    observed: Option<Instant>,
    /// When the latest answered getblockchaininfo returned.
    answered: Option<Instant>,
}

pub struct Metrics {
    inner: Mutex<Registry>,
    collections: Mutex<BTreeMap<Collector, CollectionState>>,
    runtime: Arc<runtime::RuntimeMonitor>,
    coverage_gap_since: Mutex<Option<Instant>>,
    landing: Mutex<landing::Landing>,
    node: Mutex<NodeState>,
    /// When this frontend's hashrate rollup last left no unfolded share.
    rollup_caught_up: Mutex<Option<Instant>>,
    /// Share acknowledgements around pool block landings (#602).
    landing_acks: landing_acks::LandingAcks,
}
impl Default for Metrics {
    fn default() -> Self {
        Self::new(Arc::new(runtime::RuntimeMonitor::default()))
    }
}
impl Metrics {
    pub fn new(runtime: Arc<runtime::RuntimeMonitor>) -> Self {
        let mut registry = Registry::default();
        for family in [
            Family::CtvTipRefreshYields,
            Family::CtvChunkRows,
            Family::CtvChunkSeconds,
            Family::Health,
            Family::Workers,
            Family::Connections,
            Family::Authorized,
            Family::Builds,
            Family::RebuildWaiters,
            Family::Covered,
            Family::Missing,
            Family::Accepted,
            Family::Rejected,
            Family::Blocks,
            Family::Delivered,
            Family::DeliveryFailed,
            Family::Stale,
            Family::Duplicate,
            Family::LowDifficulty,
            Family::Grace,
            Family::LateConfirmed,
            Family::CandidatesOrphaned,
            Family::DivergentLandings,
            Family::DivergentOverpay,
            Family::RevisionWorkPending,
            Family::RevisionWorkUnknown,
            Family::AcceptedUnlanded,
            Family::RevisionWorkTimeouts,
            Family::LandingTrimReleased,
        ] {
            registry.register(family, vec![], 0.);
        }
        for family in [
            Family::InitialPending,
            Family::InitialAge,
            Family::CoverageGap,
            Family::Coverage,
            Family::WorkRefreshStalled,
            Family::Candidates,
            Family::CandidateAge,
            Family::CandidateUnacknowledgedAge,
            Family::CandidateLandingFailedAge,
            Family::PartitionLead,
            Family::Rss,
            Family::OpenFds,
            Family::ConnectionLimit,
            Family::CarryForwardDebt,
            Family::NodePeers,
            Family::NodeIbd,
            Family::NodeObservationAge,
            Family::LandingTrimResident,
        ] {
            registry.register(family, vec![], -1.);
        }
        for value in AckResult::ALL {
            registry.register(Family::ShareAck, label("result", value.as_str()), 0.);
        }
        for value in AckResult::ALL {
            registry.register(
                Family::ShareAckLandingWindow,
                label("result", value.as_str()),
                0.,
            );
        }
        for bound in LandingAckBound::ALL {
            registry.register(
                Family::SlowLandingWindows,
                label("p99_above_seconds", bound.as_str()),
                -1.,
            );
        }
        for value in RevisionWorkResult::ALL {
            registry.register(Family::RevisionWork, label("result", value.as_str()), 0.);
        }
        for value in RejectReason::ALL {
            registry.register(Family::Rejections, label("reason_id", value.as_str()), 0.);
        }
        for result in Outcome::ALL {
            registry.register(Family::PoolAcquire, label("result", result.as_str()), 0.);
        }
        // Zero samples make the first refusal visible to `increase()`.
        for reason in ConnectionRefusalReason::ALL {
            registry.register(
                Family::ConnectionRefusals,
                label("reason", reason.as_str()),
                0.,
            );
        }
        for decision in CaptureDecision::ALL {
            registry.register(
                Family::CaptureOfferDecisions,
                label("decision", decision.as_str()),
                0.,
            );
        }
        for path in BlockAckPath::ALL {
            registry.register(Family::BlockAckCapped, label("path", path.as_str()), 0.);
        }
        for outcome in StandbyWaitOutcome::ALL {
            registry.register(
                Family::OfferStandbyWaits,
                label("outcome", outcome.as_str()),
                0.,
            );
        }
        for cause in StaleJobCause::ALL {
            registry.register(
                Family::StaleJobRejections,
                label("cause", cause.as_str()),
                0.,
            );
        }
        for outcome in WindowAcquisition::ALL {
            registry.register(
                Family::WindowAcquisitions,
                label("outcome", outcome.as_str()),
                0.,
            );
        }
        // Owner-dependent hooks are declared without inventing observations.
        // The rollup lag joins them: a disabled rollup publishes no pass, and an
        // absent sample is not the same claim as a lag of -1.
        for family in [
            Family::FirstOffer,
            Family::LockWait,
            Family::OrderLockHold,
            Family::RefreshSeconds,
            Family::RollupLag,
            Family::LandingTrimSeconds,
        ] {
            registry.declare(family);
        }
        for collector in Collector::ALL {
            registry.register(
                Family::CollectorAvailable,
                label("collector", collector.as_str()),
                0.,
            );
            registry.register(
                Family::CollectorSuccess,
                label("collector", collector.as_str()),
                -1.,
            );
            registry.register(
                Family::CollectorAge,
                label("collector", collector.as_str()),
                -1.,
            );
        }
        Self {
            inner: Mutex::new(registry),
            collections: Mutex::new(BTreeMap::new()),
            runtime,
            coverage_gap_since: Mutex::new(None),
            landing: Mutex::new(landing::Landing::default()),
            node: Mutex::new(NodeState::default()),
            rollup_caught_up: Mutex::new(None),
            landing_acks: landing_acks::LandingAcks::default(),
        }
    }
    pub fn runtime(&self) -> Arc<runtime::RuntimeMonitor> {
        self.runtime.clone()
    }
    /// Registry rendering does not query the database, node, or filesystem.
    /// Runtime/freshness are appended at HTTP request time by the snapshot owner.
    pub fn render(&self) -> String {
        self.current_registry().render()
    }
    /// Overlay one coherent registry read without renewing cached-body freshness.
    /// Pool waits remain observable when the health publisher waits on that pool.
    pub(crate) fn overlay_live_observations(&self, body: &mut String) {
        let current = self.current_registry();
        let mut refreshed = String::new();
        for line in body.lines().filter(|line| !registry::is_live_line(line)) {
            refreshed.push_str(line);
            refreshed.push('\n');
        }
        refreshed.push_str(&current.render_filtered(Family::is_live));
        *body = refreshed;
    }
    fn current_registry(&self) -> Registry {
        let landing = self.landing.lock().unwrap_or_else(|e| e.into_inner());
        let stored = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let collections = self.collections.lock().unwrap_or_else(|e| e.into_inner());
        let mut registry = stored.clone();
        registry.set(Family::RevisionWorkPending, Labels::Empty, landing.age());
        registry.set(
            Family::RevisionWorkUnknown,
            Labels::Empty,
            f64::from(landing.unknown()),
        );
        registry.set(
            Family::AcceptedUnlanded,
            Labels::Empty,
            landing.unlanded_age(),
        );
        for collector in Collector::ALL {
            let state = collections.get(collector);
            let age = state
                .and_then(|state| state.last_success)
                .map(|at| at.elapsed());
            registry.set(
                Family::CollectorAge,
                label("collector", collector.as_str()),
                age.map_or(-1., |age| age.as_secs_f64()),
            );
            if age.is_none_or(|age| age > COLLECTOR_STALE_AFTER) {
                snapshots::invalidate(&mut registry, *collector);
            }
        }
        drop(collections);
        let node = self.node.lock().unwrap_or_else(|e| e.into_inner());
        registry.set(
            Family::NodeObservationAge,
            Labels::Empty,
            node.answered.map_or(-1., |at| at.elapsed().as_secs_f64()),
        );
        drop(node);
        let caught_up = *self
            .rollup_caught_up
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        // Only a running rollup loop reserved a sample here; a disabled rollup
        // leaves the family declared and unsampled.
        registry.refresh(
            Family::RollupLag,
            Labels::Empty,
            caught_up.map_or(-1., |at| at.elapsed().as_secs_f64()),
        );
        let streaks = self.landing_acks.streaks(tokio::time::Instant::now());
        for (index, bound) in LandingAckBound::ALL.iter().enumerate() {
            registry.set(
                Family::SlowLandingWindows,
                label("p99_above_seconds", bound.as_str()),
                streaks.map_or(-1., |streaks| f64::from(streaks[index])),
            );
        }
        registry
    }
}

fn label(key: &'static str, value: &str) -> Vec<(&'static str, String)> {
    vec![(key, value.into())]
}
/// B5's freshness owner supplies one monotonic observation for body and headers.
pub(crate) fn render_freshness(age: Option<f64>, stale: bool) -> String {
    let mut registry = Registry::default();
    registry.set(
        Family::SnapshotAvailable,
        vec![],
        u8::from(age.is_some()).into(),
    );
    registry.set(Family::SnapshotStale, vec![], u8::from(stale).into());
    registry.set(Family::SnapshotAge, vec![], age.unwrap_or(-1.));
    registry.render()
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics").finish_non_exhaustive()
    }
}
