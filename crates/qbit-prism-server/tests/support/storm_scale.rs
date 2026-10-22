//! The candidate-storm suite's cardinality, read once from the environment (#270).
//!
//! `PRISM_TEST_STORM_CANDIDATES` sizes every storm scenario. The default, 100,
//! is the reduced cardinality the `prism-native-postgres` job runs;
//! `test/prism-native-tests.sh` raises it to 3,120, the durable candidate
//! count the 2026-08-20 testnet4 incident left behind one decided height, for
//! a local run. One reader serves every storm test in every target, so the CI
//! path and the local path cannot drift apart.
//!
//! This is not a gate variable. The shared gate
//! (`crates/qbit-prism-test-gate`) decides whether a test runs at all; this
//! decides how large it is once it does. Every storm test therefore executes
//! in both paths and is listed in `test/prism-gated-tests.txt`; none is
//! `#[ignore]`d, because `scripts/run_rust_test_shard.py` runs ignored tests
//! only from its own allowlist and `scripts/check_gate_manifest.py` fails both
//! on a listed test that did not execute and on a test that executed without
//! being listed.
//!
//! Every per-row cost invariant is measured at two cardinalities inside one
//! process, [`BASELINE_CANDIDATES`] and the run's own, and asserted equal.
//! Comparing two points in the same run is what makes the reduced-N CI path
//! prove the same property as a local run at the incident's cardinality,
//! rather than a weaker version of it.

use anyhow::{ensure, Context, Result};

/// The small cardinality every per-row cost is compared against. Large enough
/// that a per-row constant is distinguishable from a fixed setup cost, small
/// enough to add nothing measurable to the job.
pub const BASELINE_CANDIDATES: usize = 24;

/// The reduced cardinality a run uses when the environment says nothing, and
/// the floor any explicit value must meet: a lower setting would let a run
/// record `executed` for the whole suite while proving less than CI does.
pub const DEFAULT_CANDIDATES: usize = 100;

/// The durable block candidates the 2026-08-20 testnet4 incident left behind
/// one decided height. What a local run measures.
pub const OBSERVED_STORM_CANDIDATES: usize = 3_120;

/// An explicit cardinality above this is refused rather than attempted: past
/// it a run is a capacity experiment, not this suite, and #270 fences the
/// set-oriented collapse selector out of scope.
pub const MAX_CANDIDATES: usize = 100_000;

/// The variable that sizes the suite.
pub const CANDIDATES_VAR: &str = "PRISM_TEST_STORM_CANDIDATES";

/// Fixed, greppable prefix of every recorded measurement line. Durations and
/// buffer counts are reported under it and never asserted: a timing assertion
/// on a shared two-vCPU runner is flake, and a flaky gate is worse than none.
pub const REPORT_PREFIX: &str = "[prism-storm]";

/// The storm cardinality for this run.
///
/// Absent means [`DEFAULT_CANDIDATES`]. A value that is present but not a
/// count, or outside `DEFAULT_CANDIDATES..=MAX_CANDIDATES`, is an error naming
/// the variable and what it held: a misconfigured run must fail loudly rather
/// than quietly measure something smaller than CI already proves.
pub fn storm_candidates() -> Result<usize> {
    let count = match std::env::var(CANDIDATES_VAR) {
        Ok(raw) => raw
            .trim()
            .parse::<usize>()
            .with_context(|| format!("{CANDIDATES_VAR}={raw:?} is not a candidate count"))?,
        Err(std::env::VarError::NotPresent) => DEFAULT_CANDIDATES,
        Err(error) => return Err(error).context(format!("{CANDIDATES_VAR} is not readable")),
    };
    ensure!(
        (DEFAULT_CANDIDATES..=MAX_CANDIDATES).contains(&count),
        "{CANDIDATES_VAR}={count} is outside {DEFAULT_CANDIDATES}..={MAX_CANDIDATES}; \
         the suite proves its per-row invariant against {BASELINE_CANDIDATES} baseline rows \
         and a lower setting would prove less than the reduced-N CI path"
    );
    ensure!(
        count > BASELINE_CANDIDATES,
        "{CANDIDATES_VAR}={count} is not above the {BASELINE_CANDIDATES}-row baseline"
    );
    Ok(count)
}

/// Print one recorded measurement: never asserted, always reported, so a PR
/// body quotes the run rather than a memory of it. `name` identifies the
/// scenario, `pairs` are `key=value` facts in the units they were observed in.
pub fn record(name: &str, pairs: &[(&str, String)]) {
    let rendered: Vec<String> = pairs
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect();
    println!("{REPORT_PREFIX} {name} {}", rendered.join(" "));
}
