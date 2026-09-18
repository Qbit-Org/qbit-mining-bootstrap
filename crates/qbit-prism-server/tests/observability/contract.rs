//! Independent wire contract: never derive expectations from descriptors, the
//! generated inventory, or the renderer under test.
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, PartialEq)]
pub struct Census {
    pub families: BTreeMap<String, String>,
    pub series: BTreeSet<String>,
}

pub fn samples(body: &str) -> Result<BTreeMap<String, f64>, String> {
    let mut samples = BTreeMap::new();
    for line in body.lines().filter(|line| !line.starts_with('#')) {
        let (key, value) = line.rsplit_once(' ').ok_or_else(|| line.to_owned())?;
        let value: f64 = value.parse().map_err(|_| line.to_owned())?;
        if !value.is_finite() || samples.insert(key.into(), value).is_some() {
            return Err(format!("invalid or duplicate sample: {line}"));
        }
    }
    Ok(samples)
}

pub fn census(body: &str) -> Result<Census, String> {
    let mut families = BTreeMap::new();
    let mut helps = BTreeSet::new();
    for line in body.lines().filter(|line| line.starts_with('#')) {
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            let (name, kind) = rest.split_once(' ').ok_or_else(|| line.to_owned())?;
            if families.insert(name.into(), kind.into()).is_some() {
                return Err(format!("duplicate TYPE: {line}"));
            }
        } else if let Some(rest) = line.strip_prefix("# HELP ") {
            let (name, help) = rest.split_once(' ').ok_or_else(|| line.to_owned())?;
            if help.is_empty() || !helps.insert(name.to_owned()) {
                return Err(format!("empty or duplicate HELP: {line}"));
            }
        } else {
            return Err(format!("unexpected metadata: {line}"));
        }
    }
    if helps != families.keys().cloned().collect() {
        return Err("HELP/TYPE families differ".into());
    }
    Ok(Census {
        families,
        series: samples(body)?.into_keys().collect(),
    })
}

const SECONDS: &[&str] = &[
    "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "30", "+Inf",
];
const ACK_SECONDS: &[&str] = &[
    "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "15", "20", "30", "+Inf",
];

impl Census {
    fn family(&mut self, name: &str, kind: &str, labels: &[String], buckets: &[&str]) {
        let name = format!("qbit_prism_{name}");
        assert!(self.families.insert(name.clone(), kind.into()).is_none());
        let key = |name: &str, labels: &str| {
            if labels.is_empty() {
                name.to_owned()
            } else {
                format!("{name}{{{labels}}}")
            }
        };
        for labels in labels {
            if kind == "histogram" {
                for bound in buckets {
                    let separator = if labels.is_empty() { "" } else { "," };
                    self.series.insert(key(
                        &format!("{name}_bucket"),
                        &format!("{labels}{separator}le=\"{bound}\""),
                    ));
                }
                for suffix in ["sum", "count"] {
                    self.series.insert(key(&format!("{name}_{suffix}"), labels));
                }
            } else {
                self.series.insert(key(&name, labels));
            }
        }
    }
}

fn labels(key: &str, values: &str) -> Vec<String> {
    values
        .split(',')
        .map(|value| format!("{key}=\"{value}\""))
        .collect()
}

pub fn expected(populated: bool) -> Census {
    let mut result = Census {
        families: BTreeMap::new(),
        series: BTreeSet::new(),
    };
    let unlabelled = [String::new()];
    for name in [
        "ctv_fanout_broadcaster_tip_refresh_yields_total",
        "accepted_shares_total",
        "rejected_shares_total",
        "blocks_total",
        "job_delivery_successes_total",
        "job_delivery_failures_total",
        "stale_shares_total",
        "duplicate_shares_total",
        "low_difficulty_shares_total",
        "grace_credited_shares_total",
        "late_confirmed_shares_total",
        "block_candidates_orphaned_total",
        "revision_work_build_timeouts_total",
    ] {
        result.family(name, "counter", &unlabelled, &[]);
    }
    for name in [
        "health_state",
        "runtime_workers",
        "connections",
        "authorized_clients",
        "pending_job_builds",
        "authorized_with_current_work",
        "authorized_missing_current_work",
        "stratum_pending_initial_jobs",
        "stratum_oldest_pending_initial_job_seconds",
        "stratum_current_tip_coverage_gap_seconds",
        "stratum_semantic_current_work_ratio",
        "block_candidates_pending",
        "block_candidate_oldest_pending_seconds",
        "share_ledger_partition_lead_rows",
        "process_resident_memory_bytes",
        "runtime_lag_seconds",
        "metrics_snapshot_available",
        "metrics_snapshot_stale",
        "metrics_snapshot_age_seconds",
        "stratum_connection_limit",
        "accepted_block_revision_work_pending_seconds",
        "accepted_block_revision_work_tracking_unknown",
    ] {
        result.family(name, "gauge", &unlabelled, &[]);
    }
    result.family("rejections_total", "counter", &labels("reason_id", "stale-job,duplicate-share,low-difficulty,malformed-submit,unauthorized-worker,unknown-job,invalid-extranonce,invalid-ntime-or-nonce,backend-rpc-unavailable,internal-error,pool-closed,ledger-confirmation-failed,ledger-outcome-unknown,unrecognised"), &[]);
    result.family(
        "accepted_block_to_revision_work_seconds",
        "histogram",
        &labels("result", "published,degraded,superseded"),
        &[
            "0.25", "0.5", "1", "2.5", "5", "10", "30", "60", "120", "300", "307", "600", "+Inf",
        ],
    );
    result.family(
        "stratum_connection_refusals_total",
        "counter",
        &labels(
            "reason",
            "global_limit,username_limit,ip_limit,malformed_frame_budget,unknown_job_budget,authorize_budget",
        ),
        &[],
    );
    result.family(
        "stale_job_rejections_total",
        "counter",
        &labels(
            "cause",
            "resume_expired,fee_floor,parent_grace,payout_revision",
        ),
        &[],
    );
    for name in [
        "collector_available",
        "collector_success",
        "collector_age_seconds",
    ] {
        result.family(name, "gauge", &labels("collector", "database,process"), &[]);
    }
    for name in [
        "runtime_poll_lag_seconds",
        "runtime_progress_age_seconds",
        "runtime_task_stalled",
    ] {
        result.family(name, "gauge", &labels("task", "refresh,submit,block_wait,broadcast,rollup,health_publisher,stratum_listener,stratum_session,collector,share_partitions"), &[]);
    }
    result.family(
        "ctv_fanout_broadcaster_chunk_rows",
        "histogram",
        &unlabelled,
        &["1", "+Inf"],
    );
    result.family(
        "ctv_fanout_broadcaster_chunk_seconds",
        "histogram",
        &unlabelled,
        SECONDS,
    );
    result.family(
        "share_ack_seconds",
        "histogram",
        &labels("result", "accepted,rejected"),
        ACK_SECONDS,
    );
    result.family(
        "database_pool_acquire_seconds",
        "histogram",
        &labels("result", "success,failure"),
        SECONDS,
    );
    let locks: Vec<_> = ["migration", "order", "settlement"]
        .into_iter()
        .flat_map(|lock| {
            ["success", "failure"].map(|outcome| format!("lock=\"{lock}\",result=\"{outcome}\""))
        })
        .collect();
    // These families declare metadata at startup, but observations remain absent
    // until their owner actually records an event.
    result.family(
        "block_submit_seconds",
        "histogram",
        if populated { &unlabelled } else { &[] },
        SECONDS,
    );
    result.family(
        "database_advisory_lock_wait_seconds",
        "histogram",
        if populated { &locks } else { &[] },
        SECONDS,
    );
    result
}

pub fn validate(body: &str, populated: bool) -> Result<(), String> {
    let actual = census(body)?;
    let expected = expected(populated);
    if actual.families != expected.families {
        return Err(format!(
            "family/type contract changed: {:?}",
            actual.families
        ));
    }
    if actual.series != expected.series {
        return Err(format!(
            "unexpected series: {:?}; missing series: {:?}",
            actual
                .series
                .difference(&expected.series)
                .collect::<Vec<_>>(),
            expected
                .series
                .difference(&actual.series)
                .collect::<Vec<_>>()
        ));
    }
    Ok(())
}

pub fn private_identifiers(
    body: &str,
    identities: &[String],
    heights: &[u64],
) -> Result<(), String> {
    for identity in identities {
        if body.contains(identity) {
            return Err(format!("identifier in exposition: {identity}"));
        }
    }
    // Heights may coincidentally equal a count or elapsed value. Test metadata,
    // names and labels here; paired observations below test numeric attribution.
    for line in body.lines() {
        let identifiers = if line.starts_with('#') {
            line
        } else {
            line.rsplit_once(' ').ok_or_else(|| line.to_owned())?.0
        };
        if heights
            .iter()
            .any(|height| identifiers.contains(&height.to_string()))
        {
            return Err(format!("height in exposition identifiers: {line}"));
        }
    }
    Ok(())
}

pub fn height_independent(
    before: &str,
    before_height: u64,
    after: &str,
    after_height: u64,
) -> Result<(), String> {
    assert_ne!(before_height, after_height);
    let after = samples(after)?;
    for (key, value) in samples(before)? {
        if value == before_height as f64 && after.get(&key) == Some(&(after_height as f64)) {
            return Err(format!("sample tracks input height: {key}"));
        }
    }
    Ok(())
}
