//! The `qbit-prism-capacity-evidence/v2` artifact.
//!
//! Every field is traced to its reader in
//! `crates/qbit-prism-server/src/capacity.rs` (EP-COMPAT). Decimals are written
//! as strings in a form the consumer's exact rational parser reads back
//! unchanged, and phase durations are carried in whole milliseconds so the sum
//! equals `test_duration_seconds` exactly.

use anyhow::{ensure, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use qbit_prism_server::capacity::{
    validate_capacity_evidence, ValidationOptions, CONFIGURATION_KEYS, REQUIRED_PHASES, SCHEMA,
    SUBJECT_KEYS,
};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

pub const TEST_PATH: &str = "stratum-to-postgres";
pub const ARTIFACT_QUALIFICATION: &str = "qualification";
pub const ARTIFACT_EXAMPLE: &str = "example";

/// Milliseconds rendered as a seconds decimal with no precision loss.
pub fn millis_as_seconds(millis: u64) -> String {
    format!("{}.{:03}", millis / 1000, millis % 1000)
}

pub fn millis_as_millis(value: f64) -> String {
    format!("{value:.3}")
}

/// One phase as the artifact records it.
#[derive(Clone, Debug)]
pub struct PhaseEvidence {
    pub name: String,
    pub duration_millis: u64,
    pub offered: u64,
    pub acknowledged: u64,
    pub committed: u64,
    pub rejected_valid: u64,
    pub missing: u64,
    pub unexpected: u64,
    pub acknowledged_digest: String,
    pub committed_digest: String,
    pub ack_p50_millis: f64,
    pub ack_p99_millis: f64,
    /// Completed reconnects; required on the `reconnect` phase.
    pub reconnect_events: Option<u64>,
    /// The observed one-way proxy delay; required on `slow_database`.
    pub database_delay_millis: Option<f64>,
}

/// Everything the artifact needs that is not derived from the phases.
#[derive(Clone, Debug)]
pub struct ArtifactInputs {
    pub artifact_kind: String,
    pub run_id: uuid::Uuid,
    pub generated_at: DateTime<Utc>,
    pub subject: BTreeMap<String, String>,
    pub durability: BTreeMap<String, String>,
    pub configuration: BTreeMap<String, String>,
    pub forecast_peak_shares_per_second: String,
    pub ack_p99_limit_milliseconds: String,
    pub overall_ack_p50_millis: f64,
    pub overall_ack_p99_millis: f64,
    pub overall_acknowledged_digest: String,
    pub overall_committed_digest: String,
    pub phases: Vec<PhaseEvidence>,
}

fn phase_object(phase: &PhaseEvidence) -> Value {
    let mut object = Map::new();
    object.insert("completed".into(), json!(true));
    object.insert(
        "duration_seconds".into(),
        json!(millis_as_seconds(phase.duration_millis)),
    );
    object.insert("offered_valid_shares".into(), json!(phase.offered));
    object.insert("acknowledged_shares".into(), json!(phase.acknowledged));
    object.insert(
        "postgres_unique_committed_shares".into(),
        json!(phase.committed),
    );
    object.insert("rejected_valid_shares".into(), json!(phase.rejected_valid));
    object.insert(
        "missing_acknowledged_share_ids".into(),
        json!(phase.missing),
    );
    object.insert(
        "unexpected_committed_share_ids".into(),
        json!(phase.unexpected),
    );
    object.insert(
        "acknowledged_share_ids_sha256".into(),
        json!(phase.acknowledged_digest),
    );
    object.insert(
        "postgres_share_ids_sha256".into(),
        json!(phase.committed_digest),
    );
    object.insert(
        "ack_latency_milliseconds".into(),
        json!({
            "p50": millis_as_millis(phase.ack_p50_millis),
            "p99": millis_as_millis(phase.ack_p99_millis),
        }),
    );
    if let Some(events) = phase.reconnect_events {
        object.insert("reconnect_events".into(), json!(events));
    }
    if let Some(delay) = phase.database_delay_millis {
        object.insert(
            "database_delay_milliseconds".into(),
            json!(millis_as_millis(delay)),
        );
    }
    Value::Object(object)
}

/// Build the artifact document.
pub fn build(inputs: &ArtifactInputs) -> Result<Value> {
    ensure!(
        inputs.phases.len() == REQUIRED_PHASES.len()
            && REQUIRED_PHASES
                .iter()
                .all(|name| inputs.phases.iter().any(|p| p.name == *name)),
        "the artifact carries exactly the phases {REQUIRED_PHASES:?}"
    );
    for key in SUBJECT_KEYS {
        ensure!(
            inputs.subject.contains_key(*key),
            "subject is missing {key}"
        );
    }
    for key in CONFIGURATION_KEYS {
        ensure!(
            inputs.configuration.contains_key(*key),
            "configuration is missing {key}"
        );
    }
    let mut phases = Map::new();
    for name in REQUIRED_PHASES {
        let phase = inputs
            .phases
            .iter()
            .find(|p| p.name == *name)
            .with_context(|| format!("phase {name} is missing"))?;
        phases.insert((*name).to_owned(), phase_object(phase));
    }
    let total_millis: u64 = inputs.phases.iter().map(|p| p.duration_millis).sum();
    let sum = |pick: fn(&PhaseEvidence) -> u64| -> u64 { inputs.phases.iter().map(pick).sum() };
    let document = json!({
        "schema": SCHEMA,
        "artifact_kind": inputs.artifact_kind,
        "test_path": TEST_PATH,
        "generated_at": inputs.generated_at.to_rfc3339_opts(SecondsFormat::Secs, true),
        "run_id": inputs.run_id.to_string(),
        "subject": inputs.subject,
        "durability": inputs.durability,
        "configuration": inputs.configuration,
        "forecast_peak_shares_per_second": inputs.forecast_peak_shares_per_second,
        "test_duration_seconds": millis_as_seconds(total_millis),
        "offered_valid_shares": sum(|p| p.offered),
        "acknowledged_shares": sum(|p| p.acknowledged),
        "postgres_unique_committed_shares": sum(|p| p.committed),
        "rejected_valid_shares": sum(|p| p.rejected_valid),
        "missing_acknowledged_share_ids": sum(|p| p.missing),
        "unexpected_committed_share_ids": sum(|p| p.unexpected),
        "acknowledged_share_ids_sha256": inputs.overall_acknowledged_digest,
        "postgres_share_ids_sha256": inputs.overall_committed_digest,
        "ack_latency_milliseconds": {
            "p50": millis_as_millis(inputs.overall_ack_p50_millis),
            "p99": millis_as_millis(inputs.overall_ack_p99_millis),
        },
        "ack_p99_limit_milliseconds": inputs.ack_p99_limit_milliseconds,
        "phases": Value::Object(phases),
    });
    Ok(document)
}

/// What became of the capacity-evidence artifact at the end of a run.
#[derive(Clone, Debug)]
pub enum Evidence {
    /// The run completed: the artifact was built, written and validated.
    Written {
        path: std::path::PathBuf,
        document: Value,
        verdict: Verdict,
        command: String,
    },
    /// The run aborted: no artifact is written, and any artifact an earlier
    /// run left at the same path is removed so nothing self-validating
    /// survives an abort.
    Withheld {
        reason: String,
        stale_artifact_removed: bool,
    },
}

/// Build, write and validate the artifact, or withhold it if the run
/// aborted. An aborted run can have fewer than the required phases, or a
/// partial phase that looks complete, and either way its numbers are not
/// capacity evidence; the side report still carries them, marked aborted.
pub fn write_or_withhold(
    inputs: &ArtifactInputs,
    aborted: Option<&str>,
    out: &std::path::Path,
    server_bin: &str,
) -> Result<Evidence> {
    let path = out.join("capacity-evidence.json");
    if let Some(reason) = aborted {
        let stale_artifact_removed = match std::fs::remove_file(&path) {
            Ok(()) => true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => {
                return Err(error).with_context(|| format!("removing stale {}", path.display()))
            }
        };
        return Ok(Evidence::Withheld {
            reason: format!("the run aborted: {reason}"),
            stale_artifact_removed,
        });
    }
    let document = build(inputs)?;
    crate::report::write_json(&path, &document)?;
    let options = validation_options(inputs);
    let verdict = verdict(&document, &options);
    let command = cli_command(inputs, &path.display().to_string(), server_bin);
    Ok(Evidence::Written {
        path,
        document,
        verdict,
        command,
    })
}

/// The options the harness validates its own artifact with: the exact
/// configuration, subject, forecast and limit the run used.
pub fn validation_options(inputs: &ArtifactInputs) -> ValidationOptions {
    ValidationOptions {
        expected_configuration: Some(inputs.configuration.clone()),
        expected_subject: Some(inputs.subject.clone()),
        expected_forecast_peak_shares_per_second: Some(
            inputs.forecast_peak_shares_per_second.clone(),
        ),
        expected_ack_p99_limit_milliseconds: Some(inputs.ack_p99_limit_milliseconds.clone()),
        allow_example: inputs.artifact_kind == ARTIFACT_EXAMPLE,
        ..Default::default()
    }
}

/// The harness's own verdict on the artifact it just wrote.
#[derive(Clone, Debug, serde::Serialize)]
pub struct Verdict {
    pub valid: bool,
    pub summary: Option<String>,
    /// The whole error chain when the artifact is refused.
    pub error_chain: Vec<String>,
}

pub fn verdict(document: &Value, options: &ValidationOptions) -> Verdict {
    match validate_capacity_evidence(document, options) {
        Ok(summary) => Verdict {
            valid: true,
            summary: Some(format!(
                "rate={} shares/s capacity={}x ACK p50={}ms p99={}ms committed={}",
                summary.measured_shares_per_second,
                summary.capacity_multiple,
                summary.ack_p50_milliseconds,
                summary.ack_p99_milliseconds,
                summary.acknowledged_shares
            )),
            error_chain: Vec::new(),
        },
        Err(error) => Verdict {
            valid: false,
            summary: None,
            error_chain: error.chain().map(ToString::to_string).collect(),
        },
    }
}

/// Quote a value so the printed command can be pasted into a shell unchanged.
/// `SHOW server_version` returns things like `16.15 (Ubuntu 16.15-…)`, which a
/// shell would otherwise split and choke on.
pub fn shell_quote(value: &str) -> String {
    let safe = !value.is_empty()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._:=/-+,@".contains(&b));
    if safe {
        value.to_owned()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

/// The exact `qbit-prism-server capacity-evidence` invocation that reproduces
/// the harness's own self-check.
pub fn cli_command(inputs: &ArtifactInputs, evidence_path: &str, server_bin: &str) -> String {
    let mut parts = vec![format!(
        "{} capacity-evidence {}",
        shell_quote(server_bin),
        shell_quote(evidence_path)
    )];
    for key in CONFIGURATION_KEYS {
        let value = inputs.configuration.get(*key).cloned().unwrap_or_default();
        parts.push(format!(
            "  --expect {}",
            shell_quote(&format!("{key}={value}"))
        ));
    }
    for (flag, key) in [
        ("--expect-coordinator-revision", "coordinator_revision"),
        (
            "--expect-coordinator-image-digest",
            "coordinator_image_digest",
        ),
        (
            "--expect-postgres-server-version",
            "postgres_server_version",
        ),
        (
            "--expect-database-profile-sha256",
            "database_profile_sha256",
        ),
    ] {
        let value = inputs.subject.get(key).cloned().unwrap_or_default();
        parts.push(format!("  {flag} {}", shell_quote(&value)));
    }
    parts.push(format!(
        "  --forecast-peak-shares-per-second {}",
        shell_quote(&inputs.forecast_peak_shares_per_second)
    ));
    parts.push(format!(
        "  --ack-p99-limit-milliseconds {}",
        shell_quote(&inputs.ack_p99_limit_milliseconds)
    ));
    if inputs.artifact_kind == ARTIFACT_EXAMPLE {
        parts.push("  --allow-example-evidence-for-tests".into());
    }
    parts.join(" \\\n")
}
