//! Small Prometheus text registry. Only the typed owner may insert samples.
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;

pub const BUCKETS: &[f64] = &[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1., 2.5, 5., 10., 30.];
const BUCKET_COUNT: usize = BUCKETS.len();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
    Counter,
    Gauge,
    Histogram,
}
impl Kind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Histogram => "histogram",
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub struct Descriptor {
    pub name: &'static str,
    pub kind: Kind,
    pub help: &'static str,
}

macro_rules! families {
    ($($variant:ident: $kind:ident, $name:literal, $help:literal;)+) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
        pub(super) enum Family { $($variant),+ }
        impl Family {
            pub(super) const ALL: &'static [Self] = &[$(Self::$variant),+];
            pub(super) fn descriptor(self) -> Descriptor {
                match self { $(Self::$variant => Descriptor { name: concat!("qbit_prism_", $name), kind: Kind::$kind, help: $help }),+ }
            }
        }
    }
}
families! {
    Health: Gauge, "health_state", "Whether this instance is ready to serve mining work.";
    Workers: Gauge, "runtime_workers", "Configured Tokio runtime worker threads.";
    Connections: Gauge, "connections", "Current local Stratum connections.";
    Authorized: Gauge, "authorized_clients", "Current local authorized Stratum connections.";
    Builds: Gauge, "pending_job_builds", "Current local pending job deliveries.";
    Covered: Gauge, "authorized_with_current_work", "Authorized connections holding the current semantic work generation.";
    Missing: Gauge, "authorized_missing_current_work", "Authorized connections missing the current semantic work generation.";
    Accepted: Counter, "accepted_shares_total", "Shares accepted by this instance since process start.";
    Rejected: Counter, "rejected_shares_total", "Shares rejected by this instance since process start.";
    Blocks: Counter, "blocks_total", "Blocks confirmed by this instance since process start.";
    Delivered: Counter, "job_delivery_successes_total", "Successful local job deliveries.";
    DeliveryFailed: Counter, "job_delivery_failures_total", "Failed local job deliveries.";
    ShareAck: Histogram, "share_ack_seconds", "Complete mining.submit frame arrival to completed response write, by outcome.";
    Rejections: Counter, "rejections_total", "Share rejections by canonical bounded reason ID.";
    Stale: Counter, "stale_shares_total", "Shares rejected as stale or unknown jobs.";
    Duplicate: Counter, "duplicate_shares_total", "Duplicate share rejections.";
    LowDifficulty: Counter, "low_difficulty_shares_total", "Low difficulty share rejections.";
    Grace: Counter, "grace_credited_shares_total", "Durably accepted shares credited by stale grace.";
    InitialPending: Gauge, "stratum_pending_initial_jobs", "Authorized clients awaiting first usable work, or -1 before observation.";
    InitialAge: Gauge, "stratum_oldest_pending_initial_job_seconds", "Oldest first usable work wait, or -1 before observation.";
    CoverageGap: Gauge, "stratum_current_tip_coverage_gap_seconds", "Continuous age of native current-generation coverage below 95 percent, or -1 before observation.";
    Coverage: Gauge, "stratum_semantic_current_work_ratio", "Fraction of authorized connections with current semantic work; one when no clients are authorized.";
    FirstOffer: Histogram, "block_submit_seconds", "Locally validated block proof to first node offer; requires the offer owner's timestamp boundary.";
    Candidates: Gauge, "block_candidates_pending", "Cluster-wide nonterminal candidate count, or -1 when unknown.";
    CandidateAge: Gauge, "block_candidate_oldest_pending_seconds", "Oldest cluster-wide pending candidate age, or -1 when unknown.";
    PoolAcquire: Histogram, "database_pool_acquire_seconds", "Actual database pool acquisition wait by outcome.";
    LockWait: Histogram, "database_advisory_lock_wait_seconds", "Database advisory transaction lock wait by lock and outcome.";
    CollectorAvailable: Gauge, "collector_available", "Whether a collector has a complete successful observation.";
    CollectorSuccess: Gauge, "collector_success", "Whether the latest collector attempt succeeded, or -1 before an attempt.";
    CollectorAge: Gauge, "collector_age_seconds", "Monotonic age of the last successful collector observation, or -1 before success.";
    Rss: Gauge, "process_resident_memory_bytes", "Process resident memory bytes from procfs, or -1 when unknown.";
    RuntimeLag: Gauge, "runtime_lag_seconds", "Latest observed runtime sampler wake lateness, or -1 before the first observation.";
    PollLag: Gauge, "runtime_poll_lag_seconds", "Maximum active poll duration or completed poll duration retained for 60 to 61 seconds, by task.";
    ProgressAge: Gauge, "runtime_progress_age_seconds", "Oldest active operation time since progress; zero when idle.";
    TaskStalled: Gauge, "runtime_task_stalled", "Whether an active poll or operation exceeds its progress budget.";
    SnapshotAvailable: Gauge, "metrics_snapshot_available", "Whether a complete metrics snapshot has been published.";
    SnapshotStale: Gauge, "metrics_snapshot_stale", "Whether the metrics snapshot is missing or exceeds the health freshness budget.";
    SnapshotAge: Gauge, "metrics_snapshot_age_seconds", "Monotonic age of the metrics snapshot, or -1 before the first publication.";
}

impl Family {
    pub(super) fn is_collection(self) -> bool {
        matches!(
            self,
            Self::Candidates
                | Self::CandidateAge
                | Self::Rss
                | Self::CollectorAvailable
                | Self::CollectorSuccess
                | Self::CollectorAge
        )
    }
}
pub(super) fn is_collection_line(line: &str) -> bool {
    let line = line
        .strip_prefix("# HELP ")
        .or_else(|| line.strip_prefix("# TYPE "))
        .unwrap_or(line);
    let name = line.split([' ', '{']).next().unwrap_or_default();
    Family::ALL
        .iter()
        .any(|family| family.is_collection() && family.descriptor().name == name)
}

#[derive(Clone)]
enum Sample {
    Scalar(f64),
    Histogram {
        buckets: [u64; BUCKET_COUNT],
        count: u64,
        sum: f64,
    },
}
type Labels = Vec<(&'static str, String)>;
#[derive(Clone, Default)]
pub(super) struct Registry {
    samples: BTreeMap<(Family, Labels), Sample>,
    declared: BTreeSet<Family>,
}

impl Registry {
    pub(super) fn declare(&mut self, family: Family) {
        self.declared.insert(family);
    }
    pub(super) fn register(&mut self, family: Family, labels: Labels, initial: f64) {
        self.declare(family);
        self.samples.entry((family, labels)).or_insert_with(|| {
            if family.descriptor().kind == Kind::Histogram {
                Sample::Histogram {
                    buckets: [0; BUCKET_COUNT],
                    count: 0,
                    sum: 0.,
                }
            } else {
                Sample::Scalar(initial)
            }
        });
    }
    pub(super) fn set(&mut self, family: Family, labels: Labels, value: f64) {
        assert_ne!(
            family.descriptor().kind,
            Kind::Histogram,
            "cannot set a histogram scalar"
        );
        assert!(value.is_finite());
        self.declare(family);
        self.samples.insert((family, labels), Sample::Scalar(value));
    }
    pub(super) fn increment(&mut self, family: Family, labels: Labels) {
        assert_eq!(
            family.descriptor().kind,
            Kind::Counter,
            "only counters may increment"
        );
        self.register(family, labels.clone(), 0.);
        if let Sample::Scalar(value) = self.samples.get_mut(&(family, labels)).unwrap() {
            *value += 1.;
        }
    }
    pub(super) fn observe(&mut self, family: Family, labels: Labels, seconds: f64) {
        assert_eq!(
            family.descriptor().kind,
            Kind::Histogram,
            "only histograms accept observations"
        );
        self.register(family, labels.clone(), 0.);
        if let Sample::Histogram {
            buckets,
            count,
            sum,
        } = self.samples.get_mut(&(family, labels)).unwrap()
        {
            for (limit, value) in BUCKETS.iter().zip(buckets) {
                if seconds <= *limit {
                    *value += 1;
                }
            }
            *count += 1;
            *sum += seconds;
        }
    }
    pub(super) fn render(&self) -> String {
        self.render_filtered(|_| true)
    }
    pub(super) fn render_filtered(&self, include: impl Fn(Family) -> bool) -> String {
        let mut body = String::new();
        for family in &self.declared {
            if !include(*family) {
                continue;
            }
            let d = family.descriptor();
            writeln!(
                body,
                "# HELP {} {}\n# TYPE {} {}",
                d.name,
                d.help,
                d.name,
                d.kind.as_str()
            )
            .unwrap();
            for ((_, labels), sample) in self.samples.iter().filter(|((f, _), _)| f == family) {
                match sample {
                    Sample::Scalar(value) => line(&mut body, d.name, "", labels, *value),
                    Sample::Histogram {
                        buckets,
                        count,
                        sum,
                    } => {
                        for (limit, value) in BUCKETS.iter().zip(buckets) {
                            let mut labels = labels.clone();
                            labels.push(("le", limit.to_string()));
                            line(&mut body, d.name, "_bucket", &labels, *value as f64);
                        }
                        let mut infinity = labels.clone();
                        infinity.push(("le", "+Inf".into()));
                        line(&mut body, d.name, "_bucket", &infinity, *count as f64);
                        line(&mut body, d.name, "_sum", labels, *sum);
                        line(&mut body, d.name, "_count", labels, *count as f64);
                    }
                }
            }
        }
        body
    }
}
fn line(body: &mut String, name: &str, suffix: &str, labels: &Labels, value: f64) {
    write!(body, "{name}{suffix}").unwrap();
    if !labels.is_empty() {
        body.push('{');
        for (i, (name, value)) in labels.iter().enumerate() {
            if i != 0 {
                body.push(',');
            }
            let escaped = value
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\n");
            write!(body, "{name}=\"{escaped}\"").unwrap();
        }
        body.push('}');
    }
    writeln!(body, " {value}").unwrap();
}

pub fn descriptors() -> impl Iterator<Item = Descriptor> {
    Family::ALL.iter().map(|f| f.descriptor())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrong_family_operations_fail_before_corrupting_exposition() {
        let mut registry = Registry::default();
        registry.register(Family::ShareAck, vec![], 0.);
        for operation in 0..3 {
            assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                match operation {
                    0 => registry.set(Family::ShareAck, vec![], 1.),
                    1 => registry.increment(Family::ShareAck, vec![]),
                    _ => registry.observe(Family::Accepted, vec![], 1.),
                }
            }))
            .is_err());
        }
        let body = registry.render();
        assert!(body.contains("qbit_prism_share_ack_seconds_count 0\n"));
        assert!(!body.contains("qbit_prism_accepted_shares_total"));
    }
}
