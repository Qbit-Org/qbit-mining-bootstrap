//! Bounded native telemetry. Event producers share one registry per instance.
pub mod collectors;
mod events;
mod labels;
mod registry;
pub mod runtime;
mod snapshots;

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

pub struct Metrics {
    inner: Mutex<Registry>,
    collections: Mutex<BTreeMap<Collector, CollectionState>>,
    runtime: Arc<runtime::RuntimeMonitor>,
}
impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
impl Metrics {
    pub fn new() -> Self {
        let mut registry = Registry::default();
        for family in [
            Family::Health,
            Family::Workers,
            Family::Connections,
            Family::Authorized,
            Family::Builds,
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
        ] {
            registry.register(family, vec![], 0.);
        }
        for family in [
            Family::InitialPending,
            Family::InitialAge,
            Family::CoverageGap,
            Family::Coverage,
            Family::Candidates,
            Family::CandidateAge,
            Family::Rss,
        ] {
            registry.register(family, vec![], -1.);
        }
        for value in AckResult::ALL {
            registry.register(Family::ShareAck, label("result", value.as_str()), 0.);
        }
        for value in RejectReason::ALL {
            registry.register(Family::Rejections, label("reason_id", value.as_str()), 0.);
        }
        // Owner-dependent hooks are declared without inventing observations.
        for family in [Family::FirstOffer, Family::PoolAcquire, Family::LockWait] {
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
            runtime: Arc::new(runtime::RuntimeMonitor::default()),
        }
    }
    pub fn runtime(&self) -> Arc<runtime::RuntimeMonitor> {
        self.runtime.clone()
    }
    /// Registry rendering does not query the database, node, or filesystem.
    /// Runtime/freshness are appended at HTTP request time by the snapshot owner.
    pub fn render(&self) -> String {
        let stored = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let collections = self.collections.lock().unwrap_or_else(|e| e.into_inner());
        let mut registry = stored.clone();
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
        drop(stored);
        registry.render()
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
