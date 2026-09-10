//! Publish coupled measurements together; failed reads never become zero.
use super::*;
use crate::stratum::StratumStatsSnapshot;

#[derive(Default)]
pub struct DeliveryMetrics {
    pub pending_initial_jobs: Option<u64>,
    pub oldest_initial_job: Option<Duration>,
    pub coverage_gap: Option<Duration>,
}
pub struct ProcessMetrics {
    pub resident_bytes: u64,
}
#[derive(Default)]
pub struct DatabaseMetrics {
    pub candidates: u64,
    pub candidate_oldest: Duration,
}

impl Metrics {
    /// Adapter for the existing production stats owner, without a second
    /// accounting source or changing its accepted/rejected event semantics.
    pub fn publish_stratum(
        &self,
        snapshot: &StratumStatsSnapshot,
        ready: bool,
        workers: usize,
        blocks: u64,
    ) {
        let mut registry = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        for (family, value) in [
            (Family::Health, u8::from(ready) as f64),
            (Family::Workers, workers as f64),
            (Family::Connections, snapshot.connections as f64),
            (Family::Authorized, snapshot.authorized as f64),
            (Family::Builds, snapshot.pending_builds as f64),
            (
                Family::Covered,
                snapshot.authorized_with_current_work as f64,
            ),
            (
                Family::Missing,
                snapshot.authorized_missing_current_work as f64,
            ),
            (Family::Accepted, snapshot.accepted_submissions as f64),
            (Family::Rejected, snapshot.rejected_submissions as f64),
            (Family::Blocks, blocks as f64),
            (Family::Delivered, snapshot.job_delivery_successes as f64),
            (
                Family::DeliveryFailed,
                snapshot.job_delivery_failures as f64,
            ),
            (
                Family::Coverage,
                if snapshot.authorized == 0 {
                    1.
                } else {
                    snapshot
                        .authorized_with_current_work
                        .min(snapshot.authorized) as f64
                        / snapshot.authorized as f64
                },
            ),
        ] {
            registry.set(family, vec![], value);
        }
    }
    /// Stratum delivery observations cannot overwrite the refresh owner's state.
    pub fn publish_delivery(&self, snapshot: DeliveryMetrics) {
        let mut registry = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        for (family, value) in [
            (
                Family::InitialPending,
                snapshot.pending_initial_jobs.map(|v| v as f64),
            ),
            (
                Family::InitialAge,
                snapshot.oldest_initial_job.map(|v| v.as_secs_f64()),
            ),
            (
                Family::CoverageGap,
                snapshot.coverage_gap.map(|v| v.as_secs_f64()),
            ),
        ] {
            registry.set(family, vec![], value.unwrap_or(-1.));
        }
    }
    pub fn publish_process(&self, snapshot: Option<ProcessMetrics>) {
        self.begin_collection(Collector::Process)
            .publish_process(snapshot);
    }
    pub fn publish_database(&self, snapshot: Option<DatabaseMetrics>) {
        self.begin_collection(Collector::Database)
            .publish_database(snapshot);
    }
    pub fn begin_collection(&self, collector: Collector) -> Collection<'_> {
        let mut collections = self.collections.lock().unwrap_or_else(|e| e.into_inner());
        let state = collections.entry(collector).or_default();
        state.version = state
            .version
            .checked_add(1)
            .expect("collection sequence exhausted");
        Collection {
            metrics: self,
            collector,
            version: state.version,
            finished: false,
        }
    }
}

/// One authorized observation: late results cannot replace a newer attempt.
pub struct Collection<'a> {
    metrics: &'a Metrics,
    collector: Collector,
    version: u64,
    finished: bool,
}
impl Collection<'_> {
    pub fn publish_process(mut self, snapshot: Option<ProcessMetrics>) {
        assert_eq!(self.collector, Collector::Process);
        self.finished = true;
        self.metrics.publish_collection(
            self.collector,
            self.version,
            snapshot.is_some(),
            |registry| {
                if let Some(snapshot) = snapshot {
                    registry.set(Family::Rss, vec![], snapshot.resident_bytes as f64);
                }
            },
        );
    }
    pub fn publish_database(mut self, snapshot: Option<DatabaseMetrics>) {
        assert_eq!(self.collector, Collector::Database);
        self.finished = true;
        self.metrics.publish_collection(
            self.collector,
            self.version,
            snapshot.is_some(),
            |registry| {
                if let Some(snapshot) = snapshot {
                    registry.set(Family::Candidates, vec![], snapshot.candidates as f64);
                    registry.set(
                        Family::CandidateAge,
                        vec![],
                        snapshot.candidate_oldest.as_secs_f64(),
                    );
                }
            },
        );
    }
}
impl Drop for Collection<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.metrics
                .publish_collection(self.collector, self.version, false, |_| {});
        }
    }
}
impl Metrics {
    fn publish_collection(
        &self,
        collector: Collector,
        version: u64,
        success: bool,
        write: impl FnOnce(&mut Registry),
    ) {
        let mut registry = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut collections = self.collections.lock().unwrap_or_else(|e| e.into_inner());
        let state = collections.entry(collector).or_default();
        if state.version != version {
            return;
        }
        if success {
            write(&mut registry);
            state.last_success = Some(Instant::now());
            registry.set(
                Family::CollectorAvailable,
                label("collector", collector.as_str()),
                1.,
            );
        } else {
            invalidate(&mut registry, collector);
        }
        // Failure invalidates values without renewing the last-success timestamp.
        registry.set(
            Family::CollectorSuccess,
            label("collector", collector.as_str()),
            u8::from(success).into(),
        );
    }
}

pub(super) fn invalidate(registry: &mut Registry, collector: Collector) {
    registry.set(
        Family::CollectorAvailable,
        label("collector", collector.as_str()),
        0.,
    );
    let families: &[Family] = match collector {
        Collector::Database => &[Family::Candidates, Family::CandidateAge],
        Collector::Process => &[Family::Rss],
    };
    for family in families {
        registry.set(*family, vec![], -1.);
    }
}

/// Only compatibility aliases whose values have an existing native meaning.
/// ready_miner_count counts accepted-share participants in 2.x, not current
/// connections. That field and max_blocks need an agreed source.
pub fn add_known_health_fields(health: &mut serde_json::Value) {
    if let Some(backend) = health.get("backend").cloned() {
        health["ledger_backend"] = backend;
    }
    if let Some(blocks) = health.get("found_block_count").cloned() {
        if let Some(count) = blocks.as_u64() {
            health["accepted_block"] = serde_json::json!(count > 0);
        }
        health["accepted_block_count"] = blocks;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sample(body: &str, key: &str) -> f64 {
        body.lines()
            .find_map(|line| line.strip_prefix(&format!("{key} ")))
            .unwrap()
            .parse()
            .unwrap()
    }
    #[test]
    fn collector_expiry_and_failure_preserve_last_success_time_without_fabricating_values() {
        let metrics = Metrics::default();
        metrics.publish_process(Some(ProcessMetrics { resident_bytes: 0 }));
        let at = Instant::now() - COLLECTOR_STALE_AFTER - Duration::from_secs(1);
        metrics
            .collections
            .lock()
            .unwrap()
            .get_mut(&Collector::Process)
            .unwrap()
            .last_success = Some(at);
        metrics.publish_process(None);
        let body = metrics.render();
        assert_eq!(
            sample(&body, "qbit_prism_process_resident_memory_bytes"),
            -1.
        );
        assert_eq!(
            sample(&body, "qbit_prism_collector_success{collector=\"process\"}"),
            0.
        );
        assert_eq!(
            metrics.collections.lock().unwrap()[&Collector::Process].last_success,
            Some(at)
        );
        assert_eq!(
            sample(
                &metrics.inner.lock().unwrap().render(),
                "qbit_prism_process_resident_memory_bytes"
            ),
            -1.
        );
    }
    #[test]
    fn out_of_order_collections_and_cancelled_older_attempt_cannot_replace_newer_values() {
        let metrics = Metrics::default();
        let old = metrics.begin_collection(Collector::Database);
        let newer = metrics.begin_collection(Collector::Database);
        newer.publish_database(Some(DatabaseMetrics {
            candidates: 2,
            candidate_oldest: Duration::from_secs(7),
        }));
        old.publish_database(Some(DatabaseMetrics::default()));
        assert_eq!(
            sample(&metrics.render(), "qbit_prism_block_candidates_pending"),
            2.
        );
        let old = metrics.begin_collection(Collector::Database);
        metrics.publish_database(Some(DatabaseMetrics {
            candidates: 3,
            ..DatabaseMetrics::default()
        }));
        drop(old);
        assert_eq!(
            sample(&metrics.render(), "qbit_prism_block_candidates_pending"),
            3.
        );
        drop(metrics.begin_collection(Collector::Database));
        assert_eq!(
            sample(&metrics.render(), "qbit_prism_block_candidates_pending"),
            -1.
        );
        assert_eq!(
            sample(
                &metrics.render(),
                "qbit_prism_collector_success{collector=\"database\"}"
            ),
            0.
        );
    }
    #[test]
    fn known_legacy_health_fields_follow_pinned_types_without_inventing_missing_values() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/health_2x_known_fields.json"
        ))
        .unwrap();
        let mut health = serde_json::json!({"schema":"qbit.prism.audit-health.v1","ok":true,"backend":"postgres","accepted_share_count":7,"found_block_count":3});
        add_known_health_fields(&mut health);
        for (name, kind) in fixture["known_fields"].as_object().unwrap() {
            assert!(
                match kind.as_str().unwrap() {
                    "boolean" => health[name].is_boolean(),
                    "string" => health[name].is_string(),
                    "integer" => health[name].is_u64(),
                    _ => false,
                },
                "legacy field {name}"
            );
        }
        assert_eq!(health["accepted_block"], true);
        assert_eq!(health["accepted_block_count"], 3);
        for name in fixture["unmapped_fields"].as_array().unwrap() {
            assert!(health.get(name.as_str().unwrap()).is_none());
        }
    }
}
