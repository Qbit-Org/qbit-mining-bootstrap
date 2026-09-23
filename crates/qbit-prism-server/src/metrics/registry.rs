//! Small Prometheus text registry. Only the typed owner may insert samples.
use super::{Labels, LockKind, Outcome};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;

pub const BUCKETS: &[f64] = &[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1., 2.5, 5., 10., 30.];
const BUCKET_COUNT: usize = SHARE_ACK_BUCKETS.len();

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
    CtvTipRefreshYields: Counter, "ctv_fanout_broadcaster_tip_refresh_yields_total", "CTV passes deferred at a fanout boundary for a newer or unpublished tip.";
    CtvChunkRows: Histogram, "ctv_fanout_broadcaster_chunk_rows", "Fanouts attempted per native broadcaster chunk; each chunk contains one row.";
    CtvChunkSeconds: Histogram, "ctv_fanout_broadcaster_chunk_seconds", "Claimed fanout attempt duration including status persistence, in seconds.";
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
    RevisionWork: Histogram, "accepted_block_to_revision_work_seconds", "Frontend-local definitive acceptance observation to first successful mining.notify write carrying compatible post-landing payout work, in seconds; each frontend samples every block from its own observation, so a sum across instances counts one block once per frontend.";
    RevisionWorkPending: Gauge, "accepted_block_revision_work_pending_seconds", "Monotonic age of the oldest known locally observed acceptance awaiting revision work delivery; -1 when only unknown tracking remains, zero when none; each frontend tracks every block it observes, and a frontend with no connected miners keeps waiting.";
    RevisionWorkUnknown: Gauge, "accepted_block_revision_work_tracking_unknown", "Whether any local landing observation is unknown or incomplete; independent of known pending delivery age.";
    RevisionWorkTimeouts: Counter, "revision_work_build_timeouts_total", "Existing build deadlines actually hit while a known accepted-block revision work wait is open on this frontend; deadlines hit while only unknown tracking remains are not counted.";
    Candidates: Gauge, "block_candidates_pending", "Cluster-wide nonterminal candidate count, or -1 when unknown.";
    CandidateAge: Gauge, "block_candidate_oldest_pending_seconds", "Oldest cluster-wide pending candidate age, or -1 when unknown.";
    PartitionLead: Gauge, "share_ledger_partition_lead_rows", "Rows of attached share ledger partition headroom above the next share_seq, or -1 when unknown.";
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
    LateConfirmed: Counter, "late_confirmed_shares_total", "Shares accepted after the share commit deadline once their in-flight ledger commit was confirmed.";
    ConnectionRefusals: Counter, "stratum_connection_refusals_total", "Stratum connections refused by an admission limit or disconnected by a per-session budget, by closed reason.";
    ConnectionLimit: Gauge, "stratum_connection_limit", "Configured global Stratum connection limit, not currently available permits; -1 before a listener starts.";
    StaleJobRejections: Counter, "stale_job_rejections_total", "Stale-job share rejections by the internal decision that refused them.";
    CandidatesOrphaned: Counter, "block_candidates_orphaned_total", "Offered block candidates this instance settled as proven orphans since process start.";
}

// Keep bucket metadata below the descriptor block to preserve producer links.
// Default BUCKETS cover first-offer, pool-acquisition and advisory-lock timings.
// ACK time includes work outside the default commit deadline/grace interval;
// these additional bounds describe elapsed ACK time, not ledger outcomes.
const SHARE_ACK_BUCKETS: &[f64] = &[
    0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1., 2.5, 5., 10., 15., 20., 30.,
];
const REVISION_WORK_BUCKETS: &[f64] = &[
    0.25, 0.5, 1., 2.5, 5., 10., 30., 60., 120., 300., 307., 600.,
];
// Fixed storage keeps event recording allocation-free. Guard every ladder so
// future changes cannot silently truncate observation or rendering via zip.
const _: () = {
    assert!(BUCKETS.len() <= BUCKET_COUNT);
    assert!(SHARE_ACK_BUCKETS.len() <= BUCKET_COUNT);
    assert!(REVISION_WORK_BUCKETS.len() <= BUCKET_COUNT);
};

impl Family {
    fn buckets(self) -> &'static [f64] {
        match self {
            // Rows, not seconds: a native chunk is one claimed fanout. See
            // docs/prism-metrics-histogram-consumers.md before changing this.
            Self::CtvChunkRows => &[1.],
            Self::ShareAck => SHARE_ACK_BUCKETS,
            Self::RevisionWork => REVISION_WORK_BUCKETS,
            _ => BUCKETS,
        }
    }

    pub(super) fn is_live(self) -> bool {
        self.is_collection()
            || matches!(
                self,
                Self::PoolAcquire
                    | Self::RevisionWork
                    | Self::RevisionWorkPending
                    | Self::RevisionWorkUnknown
                    | Self::RevisionWorkTimeouts
            )
    }

    pub(super) fn is_collection(self) -> bool {
        matches!(
            self,
            Self::Candidates
                | Self::CandidateAge
                | Self::PartitionLead
                | Self::Rss
                | Self::CollectorAvailable
                | Self::CollectorSuccess
                | Self::CollectorAge
        )
    }
}
pub(super) fn is_live_line(line: &str) -> bool {
    let line = line
        .strip_prefix("# HELP ")
        .or_else(|| line.strip_prefix("# TYPE "))
        .unwrap_or(line);
    let name = line.split([' ', '{']).next().unwrap_or_default();
    Family::ALL.iter().any(|family| {
        let descriptor = family.descriptor();
        family.is_live()
            && (descriptor.name == name
                || (descriptor.kind == Kind::Histogram
                    && matches!(
                        name.strip_prefix(descriptor.name),
                        Some("_bucket" | "_count" | "_sum")
                    )))
    })
}

#[derive(Clone)]
enum Sample {
    /// Reserved storage is not an observation and must not render a sample.
    Pending,
    Scalar(f64),
    Histogram {
        buckets: [u64; BUCKET_COUNT],
        count: u64,
        sum: f64,
    },
}
#[derive(Clone, Default)]
pub(super) struct Registry {
    samples: BTreeMap<(Family, Labels), Sample>,
    declared: BTreeSet<Family>,
}

impl Registry {
    pub(super) fn declare(&mut self, family: Family) {
        if !self.declared.insert(family) {
            return;
        }
        // These owner-dependent families have no samples at startup. Reserve
        // their closed keys now so even the first event needs no allocation.
        match family {
            Family::FirstOffer => {
                self.samples
                    .insert((family, Labels::Empty), Sample::Pending);
            }
            Family::LockWait => {
                for lock in LockKind::ALL {
                    for result in Outcome::ALL {
                        self.samples.insert(
                            (
                                family,
                                Labels::Two(("lock", lock.as_str()), ("result", result.as_str())),
                            ),
                            Sample::Pending,
                        );
                    }
                }
            }
            _ => {}
        }
    }
    pub(super) fn register(&mut self, family: Family, labels: impl Into<Labels>, initial: f64) {
        self.sample(family, labels.into(), initial);
    }
    fn sample(&mut self, family: Family, labels: Labels, initial: f64) -> &mut Sample {
        self.declare(family);
        let sample = self
            .samples
            .entry((family, labels))
            .or_insert(Sample::Pending);
        if matches!(sample, Sample::Pending) {
            *sample = if family.descriptor().kind == Kind::Histogram {
                Sample::Histogram {
                    buckets: [0; BUCKET_COUNT],
                    count: 0,
                    sum: 0.,
                }
            } else {
                Sample::Scalar(initial)
            };
        }
        sample
    }
    pub(super) fn set(&mut self, family: Family, labels: impl Into<Labels>, value: f64) {
        assert_ne!(
            family.descriptor().kind,
            Kind::Histogram,
            "cannot set a histogram scalar"
        );
        assert!(value.is_finite());
        self.declare(family);
        self.samples
            .insert((family, labels.into()), Sample::Scalar(value));
    }
    pub(super) fn increment(&mut self, family: Family, labels: impl Into<Labels>) {
        assert_eq!(
            family.descriptor().kind,
            Kind::Counter,
            "only counters may increment"
        );
        if let Sample::Scalar(value) = self.sample(family, labels.into(), 0.) {
            *value += 1.;
        }
    }
    pub(super) fn observe(&mut self, family: Family, labels: impl Into<Labels>, seconds: f64) {
        assert_eq!(
            family.descriptor().kind,
            Kind::Histogram,
            "only histograms accept observations"
        );
        if let Sample::Histogram {
            buckets,
            count,
            sum,
        } = self.sample(family, labels.into(), 0.)
        {
            for (limit, value) in family.buckets().iter().zip(buckets) {
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
                    Sample::Pending => {}
                    Sample::Scalar(value) => line(&mut body, d.name, "", labels.iter(), *value),
                    Sample::Histogram {
                        buckets,
                        count,
                        sum,
                    } => {
                        for (limit, value) in family.buckets().iter().zip(buckets) {
                            let limit = limit.to_string();
                            line(
                                &mut body,
                                d.name,
                                "_bucket",
                                labels.iter().chain([("le", limit.as_str())]),
                                *value as f64,
                            );
                        }
                        line(
                            &mut body,
                            d.name,
                            "_bucket",
                            labels.iter().chain([("le", "+Inf")]),
                            *count as f64,
                        );
                        line(&mut body, d.name, "_sum", labels.iter(), *sum);
                        line(&mut body, d.name, "_count", labels.iter(), *count as f64);
                    }
                }
            }
        }
        body
    }
}
fn line<'a>(
    body: &mut String,
    name: &str,
    suffix: &str,
    labels: impl Iterator<Item = (&'static str, &'a str)>,
    value: f64,
) {
    write!(body, "{name}{suffix}").unwrap();
    let mut labels = labels.peekable();
    if labels.peek().is_some() {
        body.push('{');
        for (i, (name, value)) in labels.enumerate() {
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
    fn owned_and_inline_keys_update_the_same_samples_in_either_insertion_order() {
        for inline_first in [false, true] {
            let mut registry = Registry::default();
            let mut keys = [
                Labels::One(("result", "accepted")),
                Labels::Owned(vec![("result", "accepted".into())]),
            ];
            if !inline_first {
                keys.reverse();
            }
            for key in keys {
                registry.observe(Family::ShareAck, key, 0.125);
            }
            registry.increment(Family::Grace, Labels::Empty);
            registry.increment(Family::Grace, vec![]);
            registry.set(
                Family::CollectorSuccess,
                Labels::One(("collector", "process")),
                -1.,
            );
            registry.set(
                Family::CollectorSuccess,
                vec![("collector", "process".into())],
                0.,
            );
            let body = registry.render();
            assert!(body.contains("qbit_prism_share_ack_seconds_count{result=\"accepted\"} 2\n"));
            assert!(body.contains("qbit_prism_share_ack_seconds_sum{result=\"accepted\"} 0.25\n"));
            assert!(body.contains("qbit_prism_grace_credited_shares_total 2\n"));
            assert!(body.contains("qbit_prism_collector_success{collector=\"process\"} 0\n"));
            assert_eq!(registry.samples.len(), 3);
        }
    }

    #[test]
    fn arbitrary_labels_keep_order_and_escape_backslashes_quotes_and_newlines() {
        let mut registry = Registry::default();
        let value = "\\\"\nλ";
        registry.observe(
            Family::ShareAck,
            vec![
                ("z", value.into()),
                ("a", "".into()),
                ("third", "free-form".into()),
            ],
            0.125,
        );
        let body = registry.render();
        let labels = "z=\"\\\\\\\"\\nλ\",a=\"\",third=\"free-form\"";
        assert!(body.contains(&format!(
            "qbit_prism_share_ack_seconds_bucket{{{labels},le=\"0.25\"}} 1\n"
        )));
        assert!(body.contains(&format!(
            "qbit_prism_share_ack_seconds_sum{{{labels}}} 0.125\n"
        )));
        assert!(body.contains(&format!(
            "qbit_prism_share_ack_seconds_count{{{labels}}} 1\n"
        )));
        // The inline representation uses the same escaping and key equality.
        registry.increment(Family::Rejections, Labels::Two(("z", value), ("a", "")));
        registry.increment(
            Family::Rejections,
            vec![("z", value.into()), ("a", "".into())],
        );
        assert!(registry
            .render()
            .contains("qbit_prism_rejections_total{z=\"\\\\\\\"\\nλ\",a=\"\"} 2\n"));
    }

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
