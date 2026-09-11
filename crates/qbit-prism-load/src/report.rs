//! `load-harness-report.json`: everything the capacity artifact's schema
//! cannot carry.

use anyhow::{Context, Result};
use serde_json::Value;
use std::path::Path;

pub const SCHEMA: &str = "qbit.prism.load-harness.v1";

/// Statements the report must make so no number in the artifact can be read as
/// something it is not (EP-COMPAT).
pub fn honest_value_notes() -> Value {
    serde_json::json!([
        {
            "field": "subject.coordinator_image_digest",
            "note": "This is the SHA-256 of the qbit-prism-server executable's bytes, not an OCI \
                     image digest. There is no image: the binary was built locally. A validator \
                     run with --expect-coordinator-image-digest set to a real image digest will \
                     correctly reject this artifact."
        },
        {
            "field": "configuration.PRISM_SHARE_COMMIT_BATCH_SIZE",
            "note": "Not read by the native runtime (#288). The value 1 describes the behaviour \
                     that actually happens: one share per transaction."
        },
        {
            "field": "configuration.PRISM_SHARE_COMMIT_LINGER_MILLISECONDS",
            "note": "Not read by the native runtime (#288). The value 0 describes the behaviour \
                     that actually happens: no batching delay exists to configure."
        },
        {
            "field": "configuration.PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS",
            "note": "Not read by the native runtime (#288). The value 0 is valid to the \
                     validator and truthfully means there is no such native control; it should \
                     not be read as 'the sweep is disabled'."
        },
        {
            "field": "phases.*.offered_valid_shares",
            "note": "Counts shares the harness believed valid when it offered them: every \
                     acknowledged share, plus every rejection that is not a race the server was \
                     entitled to lose, plus every submit that received no response. Only the \
                     transient rejections (stale-job after a tip change or a payout-revision \
                     bump, an unknown or retired job, a closed pool) are excluded; none of them \
                     persists a share, so none can affect reconciliation. A backend refusal \
                     such as `current chain state is unavailable` or `share was not confirmed \
                     by the database` is a capacity result rather than a harness defect, but it \
                     stays in `offered_valid_shares` and in `rejected_valid_shares` so the \
                     artifact cannot hide it."
        },
        {
            "field": "rejected_valid_shares",
            "note": "Every rejection except the entitled races. Only the harness-bug classes \
                     (low-difficulty, malformed-submit, duplicate-share, invalid-*, \
                     unauthorized-worker) make the run exit non-zero; a backend refusal is \
                     counted, reported and left to the reader as a capacity finding."
        },
        {
            "field": "phases.slow_database.database_delay_milliseconds",
            "note": "The observed one-way per-chunk proxy delay. A database round trip pays it \
                     twice. The configured value and the measured added round-trip time are both \
                     recorded under `delay_proxy`."
        },
        {
            "field": "ack_latency_milliseconds",
            "note": "Client-measured, from the instant the submit line was written to the instant \
                     its response line was read, on the client's monotonic clock. The server's \
                     own qbit_prism_share_ack_seconds histogram measures a different, narrower \
                     boundary and is reported separately as bucket deltas."
        }
    ])
}

/// Write a JSON document with a trailing newline.
pub fn write_json(path: &Path, value: &Value) -> Result<()> {
    let mut text = serde_json::to_string_pretty(value)?;
    text.push('\n');
    std::fs::write(path, text).with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}
