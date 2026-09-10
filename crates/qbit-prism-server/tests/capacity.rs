use qbit_prism_server::capacity::{self, ValidationOptions};
use serde_json::{json, Value};
use std::{collections::BTreeMap, str::FromStr};

fn example() -> Value {
    serde_json::from_str(include_str!(
        "../../../tests/fixtures/prism-capacity-evidence.json"
    ))
    .unwrap()
}
fn qualification() -> Value {
    let mut v = example();
    v["artifact_kind"] = json!("qualification");
    v["run_id"] = json!("91d514da-2c6f-4a8e-8964-b5f64c46ba18");
    v
}
fn options() -> ValidationOptions {
    let fixture = example();
    let map = |value: &Value| {
        value
            .as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    v.as_str()
                        .map(str::to_string)
                        .unwrap_or_else(|| v.to_string()),
                )
            })
            .collect::<BTreeMap<_, _>>()
    };
    ValidationOptions {
        expected_configuration: Some(map(&fixture["configuration"])),
        expected_subject: Some(map(&fixture["subject"])),
        expected_forecast_peak_shares_per_second: Some("100".into()),
        expected_ack_p99_limit_milliseconds: Some("50".into()),
        current_time: Some(
            chrono::DateTime::parse_from_rfc3339("2026-07-13T12:30:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
        ),
        ..Default::default()
    }
}
fn reject(payload: &Value, options: &ValidationOptions, contains: &str) {
    let err = capacity::validate_capacity_evidence(payload, options).unwrap_err();
    assert!(
        format!("{err:#}").contains(contains),
        "expected {contains:?}, got {err:#}"
    );
}
#[test]
fn qualifies_exact_two_times_capacity_and_reconciled_identifiers() {
    let mut o = options();
    o.expected_configuration
        .as_mut()
        .unwrap()
        .insert("PRISM_STRATUM_SHARE_DIFF".into(), "1024.0".into());
    let s = capacity::validate_capacity_evidence(&qualification(), &o).unwrap();
    assert_eq!(s.measured_shares_per_second.to_string(), "200");
    assert_eq!(s.capacity_multiple.to_string(), "2");
    assert_eq!(s.acknowledged_shares.to_string(), "120000");
    assert_eq!(s.ack_p50_milliseconds.to_string(), "6");
    assert_eq!(s.ack_p99_milliseconds.to_string(), "40");
}
#[test]
fn examples_need_explicit_opt_in() {
    reject(&example(), &options(), "example capacity evidence");
    let o = ValidationOptions {
        allow_example: true,
        ..Default::default()
    };
    assert_eq!(
        capacity::validate_capacity_evidence(&example(), &o)
            .unwrap()
            .capacity_multiple
            .to_string(),
        "2"
    );
}
#[test]
fn qualification_requires_all_external_bindings() {
    for field in ["configuration", "subject", "forecast", "limit"] {
        let mut o = options();
        let expected = match field {
            "configuration" => {
                o.expected_configuration = None;
                "expected deployment configuration"
            }
            "subject" => {
                o.expected_subject = None;
                "expected deployment subject"
            }
            "forecast" => {
                o.expected_forecast_peak_shares_per_second = None;
                "externally configured forecast"
            }
            _ => {
                o.expected_ack_p99_limit_milliseconds = None;
                "externally configured ACK p99"
            }
        };
        reject(&qualification(), &o, expected);
    }
}
#[test]
fn validates_time_uuid_and_subject() {
    for (time, expected) in [
        ("2026-07-10T12:00:00Z", "older than"),
        ("2026-07-14T12:00:00Z", "too far in the future"),
        ("2026-07-13 12:00:00Z", "RFC 3339"),
        ("2026-07-13T12:00:00", "RFC 3339"),
    ] {
        let mut v = qualification();
        v["generated_at"] = json!(time);
        reject(&v, &options(), expected);
    }
    let mut v = qualification();
    v["generated_at"] = json!("2026-01-01T00:00:00Z");
    let o = ValidationOptions {
        enforce_freshness: false,
        ..options()
    };
    capacity::validate_capacity_evidence(&v, &o).unwrap();
    v = qualification();
    v["run_id"] = json!("00000000-0000-0000-0000-000000000000");
    reject(&v, &options(), "non-zero run_id");
    for key in capacity::SUBJECT_KEYS {
        let mut o = options();
        o.expected_subject
            .as_mut()
            .unwrap()
            .insert((*key).into(), "different".into());
        reject(&qualification(), &o, &format!("subject.{key}"));
    }
}
#[test]
fn binds_every_configuration_and_checks_vardiff_math() {
    let alternatives = [
        ("PRISM_STRATUM_SHARE_DIFF", "2048"),
        ("PRISM_STRATUM_VARDIFF", "0"),
        ("PRISM_STRATUM_VARDIFF_TARGET_SECONDS", "20"),
        ("PRISM_STRATUM_VARDIFF_MIN_DIFF", "2048"),
        ("PRISM_STRATUM_VARDIFF_START_DIFF", "8192"),
        ("PRISM_STRATUM_VARDIFF_MAX_DIFF", "131072"),
        ("PRISM_STRATUM_VARDIFF_RETARGET_SECONDS", "120"),
        ("PRISM_STRATUM_VARDIFF_MAX_STEP_UP", "2"),
        ("PRISM_STRATUM_VARDIFF_MAX_STEP_DOWN", "2"),
        ("PRISM_STRATUM_VARDIFF_EWMA_ALPHA", "0.5"),
        ("PRISM_STRATUM_VARDIFF_RETARGET_TOLERANCE", "0.3"),
        ("PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS", "20"),
        ("PRISM_SHARE_COMMIT_BATCH_SIZE", "32"),
        ("PRISM_SHARE_COMMIT_LINGER_MILLISECONDS", "7"),
        ("PRISM_SHARE_COMMIT_TIMEOUT_SECONDS", "20"),
        ("PRISM_STRATUM_SEND_TIMEOUT_SECONDS", "25"),
    ];
    assert_eq!(alternatives.len(), capacity::CONFIGURATION_KEYS.len());
    for (key, value) in alternatives {
        let mut o = options();
        o.expected_configuration
            .as_mut()
            .unwrap()
            .insert(key.into(), value.into());
        reject(&qualification(), &o, &format!("configuration.{key}"));
    }
    for (key, value, expected) in [
        (
            "PRISM_STRATUM_VARDIFF_MAX_DIFF",
            "2048",
            "minimum <= start <= maximum",
        ),
        (
            "PRISM_STRATUM_VARDIFF_MAX_STEP_UP",
            "0.5",
            "must be at least 1",
        ),
        (
            "PRISM_STRATUM_VARDIFF_MAX_STEP_DOWN",
            "0.5",
            "must be at least 1",
        ),
        (
            "PRISM_STRATUM_VARDIFF_EWMA_ALPHA",
            "1.1",
            "must not exceed 1",
        ),
        ("PRISM_STRATUM_SHARE_DIFF", "1e-9", "lab-only 1e-9"),
    ] {
        let mut v = qualification();
        v["configuration"][key] = json!(value);
        reject(&v, &options(), expected);
    }
}
#[test]
fn requires_durability_forecast_and_latency_limits() {
    for key in ["fsync", "full_page_writes", "synchronous_commit"] {
        let mut v = qualification();
        v["durability"][key] = json!("off");
        reject(&v, &options(), &format!("durability.{key}"));
    }
    let mut o = options();
    o.expected_forecast_peak_shares_per_second = Some("101".into());
    reject(
        &qualification(),
        &o,
        "forecast_peak_shares_per_second does not match",
    );
    o = options();
    o.expected_ack_p99_limit_milliseconds = Some("51".into());
    reject(
        &qualification(),
        &o,
        "ack_p99_limit_milliseconds does not match",
    );
    let mut v = qualification();
    v["ack_p99_limit_milliseconds"] = json!(16000);
    o = options();
    o.expected_ack_p99_limit_milliseconds = Some("16000".into());
    reject(&v, &o, "cannot exceed PRISM_SHARE_COMMIT_TIMEOUT_SECONDS");
    v = qualification();
    v["phases"]["slow_database"]["ack_latency_milliseconds"]["p99"] = json!(51);
    reject(&v, &options(), "required 50ms");
}
#[test]
fn every_fault_phase_must_be_nontrivial_and_sustain_two_times_forecast() {
    for (phase, key, value, message) in [
        ("steady_state", "duration_seconds", 399, "phase durations"),
        ("reconnect", "duration_seconds", 59, "at least 60"),
        ("reconnect", "reconnect_events", 1, "at least 10"),
        (
            "slow_database",
            "database_delay_milliseconds",
            1,
            "at least 10",
        ),
    ] {
        let mut v = qualification();
        v["phases"][phase][key] = json!(value);
        reject(&v, &options(), message);
    }
    let mut v = qualification();
    for key in [
        "offered_valid_shares",
        "acknowledged_shares",
        "postgres_unique_committed_shares",
    ] {
        v["phases"]["steady_state"][key] = json!(90000);
        v["phases"]["reconnect"][key] = json!(10000);
    }
    reject(&v, &options(), "phases.reconnect sustained rate");
    v = qualification();
    v["phases"]["reconnect"]["completed"] = json!(1);
    reject(&v, &options(), "completed must be true");
}
#[test]
fn detects_share_loss_hash_mismatch_and_phase_totals() {
    for (key, value, message) in [
        (
            "missing_acknowledged_share_ids",
            json!(1),
            "ACK-to-Postgres reconciliation",
        ),
        (
            "postgres_share_ids_sha256",
            json!("f".repeat(64)),
            "identifier digests differ",
        ),
        (
            "rejected_valid_shares",
            json!(1),
            "every offered valid share",
        ),
    ] {
        let mut v = qualification();
        v[key] = value;
        reject(&v, &options(), message);
    }
    let mut v = qualification();
    for key in [
        "offered_valid_shares",
        "acknowledged_shares",
        "postgres_unique_committed_shares",
    ] {
        v["phases"]["steady_state"][key] = json!(80001);
    }
    reject(&v, &options(), "phase offered-share totals");
}
#[test]
fn strict_objects_and_numbers_reject_ambiguous_claims() {
    let mut v = qualification();
    v["unchecked_claim"] = json!(true);
    reject(&v, &options(), "unknown fields");
    for value in [
        json!(true),
        json!("NaN"),
        json!("Infinity"),
        json!("-1"),
        json!("0"),
    ] {
        v = qualification();
        v["forecast_peak_shares_per_second"] = value;
        reject(&v, &options(), "decimal number");
    }
    for value in [
        json!(true),
        json!("1.0"),
        json!("01"),
        json!("+1"),
        json!("1e0"),
    ] {
        v = qualification();
        v["configuration"]["PRISM_STRATUM_VARDIFF"] = value;
        reject(&v, &options(), "must be an integer");
    }
    // Infinitesimally below two times capacity still fails; binary floats round it to 2.
    v = qualification();
    v["test_duration_seconds"] = json!("600.000000000000000000000000000001");
    reject(&v, &options(), "at least 2x forecast");
    assert_eq!(
        capacity::Decimal::from_str("0.1").unwrap(),
        capacity::Decimal::from_str("1e-1").unwrap()
    );
}
#[test]
fn loader_rejects_duplicate_keys_at_any_depth() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("evidence.json");
    for text in [
        r#"{"schema":"first","schema":"second"}"#,
        r#"{"nested":[{"p50":1,"p50":2}]}"#,
    ] {
        std::fs::write(&path, text).unwrap();
        let error =
            capacity::load_capacity_evidence(&path, &ValidationOptions::default()).unwrap_err();
        assert!(format!("{error:#}").contains("duplicate JSON key"));
    }
    std::fs::write(&path, serde_json::to_vec(&example()).unwrap()).unwrap();
    capacity::load_capacity_evidence(
        &path,
        &ValidationOptions {
            allow_example: true,
            ..Default::default()
        },
    )
    .unwrap();
}
