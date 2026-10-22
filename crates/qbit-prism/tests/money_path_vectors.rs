//! Replay the money-path vectors frozen from 2.x.x (issue #269) through the
//! 3.x.x engine.
//!
//! The vectors and their exporter live in `fixtures/vectors/`. Every case
//! must reproduce its frozen value exactly. A case may differ from 2.x.x only
//! through a D2 entry (`expected_2xx`, `expected_3xx` and `d2_entry`) in the
//! bootstrap or below-target credit topics. The set of entries the cases use
//! must equal the set documented in `docs/prism-rust-migration.md`, and every
//! vector file is pinned by its sha256.

use std::collections::{BTreeMap, BTreeSet};

use qbit_prism::{
    apply_payout_policy, apply_proportional_fanout_fee, build_prism_reward_manifest,
    compute_prism_window, select_settlement_mode, select_settlement_mode_with_pinned_direct,
    AcceptedShare, CarryForwardBalance, PrismError, SettlementModeConfig, SettlementRecipient,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const SCHEMA: &str = "qbit.prism.money-path-vectors.v1";
const SOURCE_COMMIT: &str = "504846cc0b72e8f86ed17f896d4ccbbe196a31dc";
/// The sha256 of each vector file as exported from `SOURCE_COMMIT`. The
/// vectors change only through a re-export in a reviewed commit that updates
/// these pins.
const VECTOR_SHA256: [(&str, &str); 7] = [
    (
        "window.json",
        "aef6c5bb2304c345d9cac198f2654c2fd987e09d8677d99b6ae847798020d90d",
    ),
    (
        "carry_only.json",
        "ed10a4361a0362e835f50c392bd5d0cf54f85b35a2c1ae48cc63bebede9c35e7",
    ),
    (
        "pool_fee.json",
        "ee9e64ed43f20dee3da6543fc2f5bde893526a09ad54bffbee5c8611e7d4d146",
    ),
    (
        "fanout_fee_and_ties.json",
        "f9e8c92377ba1d1b98e430d28a0ed6c94e7b60c4b59e9557fe28dce700d5be30",
    ),
    (
        "settlement_chunks.json",
        "4dcddf8d71dd5ff9c51122b5075587f987dba36959830d2a798a8a589675bf0f",
    ),
    (
        "bootstrap_transition.json",
        "6db2c6b8c00a1ce8402c6c97c7dd542b037a005afb538987c0cd75786c1b3fab",
    ),
    (
        "below_target_credit.json",
        "66a419f9a61b9ca4678895f6774ade52837eb6e52f2e530dde66f06154a8754e",
    ),
];
const MIGRATION_DOC: &str = include_str!("../../../docs/prism-rust-migration.md");
const D2_SECTION_HEADING: &str = "## Payout differences from 2.x.x (decision D2)";
const D2_ENTRIES: [&str; 3] = [
    "d2a-bootstrap-pooling",
    "d2b-below-target-credit",
    "d2c-prior-balances-during-bootstrap",
];
const D2_TOPICS: [u64; 2] = [8, 9];
const VECTOR_FILES: [(&str, &str); 7] = [
    (
        "window.json",
        include_str!("../fixtures/vectors/window.json"),
    ),
    (
        "carry_only.json",
        include_str!("../fixtures/vectors/carry_only.json"),
    ),
    (
        "pool_fee.json",
        include_str!("../fixtures/vectors/pool_fee.json"),
    ),
    (
        "fanout_fee_and_ties.json",
        include_str!("../fixtures/vectors/fanout_fee_and_ties.json"),
    ),
    (
        "settlement_chunks.json",
        include_str!("../fixtures/vectors/settlement_chunks.json"),
    ),
    (
        "bootstrap_transition.json",
        include_str!("../fixtures/vectors/bootstrap_transition.json"),
    ),
    (
        "below_target_credit.json",
        include_str!("../fixtures/vectors/below_target_credit.json"),
    ),
];

fn to_value<T: Serialize>(value: &T) -> Value {
    serde_json::to_value(value).expect("engine output serializes")
}

fn field<T: DeserializeOwned>(input: &Value, name: &str) -> Result<T, String> {
    serde_json::from_value(input[name].clone()).map_err(|err| format!("input {name}: {err}"))
}

fn outcome<T: Serialize>(result: Result<T, PrismError>) -> Value {
    match result {
        Ok(value) => json!({ "ok": to_value(&value) }),
        Err(err) => json!({ "error": err.to_string() }),
    }
}

/// Mirrors the exporter's dispatcher over the same public entry points.
fn run(entry_point: &str, input: &Value) -> Result<Value, String> {
    Ok(match entry_point {
        "compute_prism_window" => outcome(compute_prism_window(
            &field::<Vec<AcceptedShare>>(input, "shares")?,
            &field(input, "found_block")?,
        )),
        "apply_payout_policy" => outcome(apply_payout_policy(
            &field(input, "reward_manifest")?,
            &field::<Vec<CarryForwardBalance>>(input, "prior_balances")?,
            &field(input, "policy")?,
        )),
        "apply_proportional_fanout_fee" => outcome(apply_proportional_fanout_fee(
            &field::<Vec<SettlementRecipient>>(input, "recipients")?,
            field(input, "fee_sats")?,
            field(input, "min_output_sats")?,
        )),
        "select_settlement_mode" => {
            let recipients = field::<Vec<SettlementRecipient>>(input, "recipients")?;
            let floor = field(input, "direct_floor_sats")?;
            let config = field::<SettlementModeConfig>(input, "config")?;
            match field::<Option<SettlementRecipient>>(input, "pinned_direct")? {
                Some(pinned) => outcome(select_settlement_mode_with_pinned_direct(
                    &recipients,
                    floor,
                    &config,
                    Some(&pinned),
                )),
                None => outcome(select_settlement_mode(&recipients, floor, &config)),
            }
        }
        "bundle_payout" => bundle_payout(input)?,
        other => return Err(format!("unknown entry point {other}")),
    })
}

/// The payout consequence of one bundle: the reward window and the payout
/// policy manifest, computed exactly as the exporter did.
fn bundle_payout(input: &Value) -> Result<Value, String> {
    let shares = field::<Vec<AcceptedShare>>(input, "shares")?;
    let found_block = field(input, "found_block")?;
    let prior_balances = field::<Vec<CarryForwardBalance>>(input, "prior_balances")?;
    let policy = field(input, "policy")?;
    Ok(outcome(
        build_prism_reward_manifest(&shares, &found_block).and_then(|reward| {
            let payout = apply_payout_policy(&reward, &prior_balances, &policy)?;
            Ok(json!({
                "counted_window_weight": to_value(&reward.counted_window_weight),
                "counted_shares": reward
                    .shares
                    .iter()
                    .map(|share| json!({
                        "share_seq": share.share_seq,
                        "miner_id": share.miner_id,
                        "counted_difficulty": to_value(&share.counted_difficulty),
                    }))
                    .collect::<Vec<_>>(),
                "entitlements": to_value(&reward.entitlements),
                "payout_policy_manifest": to_value(&payout),
            }))
        }),
    ))
}

/// Record every leaf where `actual` departs from `expected`.
fn diff(path: &str, expected: &Value, actual: &Value, mismatches: &mut Vec<String>) {
    match (expected, actual) {
        (Value::Object(want), Value::Object(got)) => {
            for key in want
                .keys()
                .chain(got.keys().filter(|key| !want.contains_key(*key)))
            {
                let child = format!("{path}.{key}");
                match (want.get(key), got.get(key)) {
                    (Some(want), Some(got)) => diff(&child, want, got, mismatches),
                    (Some(want), None) => {
                        mismatches.push(format!("{child}: expected {want}, got nothing"))
                    }
                    (None, Some(got)) => {
                        mismatches.push(format!("{child}: expected nothing, got {got}"))
                    }
                    (None, None) => unreachable!(),
                }
            }
        }
        (Value::Array(want), Value::Array(got)) if want.len() == got.len() => {
            for (index, (want, got)) in want.iter().zip(got).enumerate() {
                diff(&format!("{path}[{index}]"), want, got, mismatches);
            }
        }
        _ if expected == actual => {}
        _ => mismatches.push(format!("{path}: expected {expected}, got {actual}")),
    }
}

fn check(
    path: &str,
    expected: &Value,
    entry_point: &str,
    input: &Value,
    mismatches: &mut Vec<String>,
) {
    match run(entry_point, input) {
        Ok(actual) => diff(path, expected, &actual, mismatches),
        Err(err) => mismatches.push(format!("{path}: {err}")),
    }
}

/// Every anchor in the doc's D2 section, up to the next level-two heading.
fn documented_d2_entries() -> Result<BTreeSet<String>, String> {
    let start = MIGRATION_DOC
        .find(D2_SECTION_HEADING)
        .ok_or_else(|| format!("docs/prism-rust-migration.md has no {D2_SECTION_HEADING:?}"))?;
    let section = &MIGRATION_DOC[start + D2_SECTION_HEADING.len()..];
    let section = &section[..section.find("\n## ").unwrap_or(section.len())];
    Ok(section
        .split("<a id=\"")
        .skip(1)
        .filter_map(|rest| rest.split('"').next())
        .map(str::to_string)
        .collect())
}

#[test]
fn money_path_vectors_match_frozen_2xx_values() {
    let mut mismatches = Vec::new();
    let mut cases_per_topic = BTreeMap::<u64, usize>::new();
    let mut d2_cases = BTreeMap::<String, usize>::new();

    for (file, raw) in VECTOR_FILES {
        let actual_sha256 = hex::encode(Sha256::digest(raw.as_bytes()));
        match VECTOR_SHA256.iter().find(|(name, _)| *name == file) {
            Some((_, pinned)) if *pinned == actual_sha256 => {}
            pinned => mismatches.push(format!(
                "{file} / sha256: pinned {}, got {actual_sha256}; the vectors change only through a \
                 re-export from {SOURCE_COMMIT} in a reviewed commit that updates the pin",
                pinned.map_or("nothing", |(_, pin)| pin)
            )),
        }
        let document: Value =
            serde_json::from_str(raw).unwrap_or_else(|err| panic!("{file} is not JSON: {err}"));
        if document["schema"] != SCHEMA {
            mismatches.push(format!(
                "{file} / schema: expected {SCHEMA}, got {}",
                document["schema"]
            ));
        }
        if document["source_commit"] != SOURCE_COMMIT {
            mismatches.push(format!(
                "{file} / source_commit: expected {SOURCE_COMMIT}, got {}",
                document["source_commit"]
            ));
        }
        if document["produced_by"].as_str().is_none_or(str::is_empty) {
            mismatches.push(format!(
                "{file} / produced_by: missing regeneration command"
            ));
        }
        for case in document["cases"].as_array().into_iter().flatten() {
            let name = case["name"].as_str().unwrap_or("<unnamed>");
            let at = |field: &str| format!("{file} / {name} / {field}");
            let Some(topic) = case["topic"].as_u64() else {
                mismatches.push(at("topic: missing"));
                continue;
            };
            *cases_per_topic.entry(topic).or_default() += 1;
            let entry_point = case["entry_point"].as_str().unwrap_or_default();
            let input = &case["input"];

            if let Some(anchor) = case.get("d2_entry") {
                let anchor = anchor.as_str().unwrap_or_default();
                *d2_cases.entry(anchor.to_string()).or_default() += 1;
                if !D2_TOPICS.contains(&topic) {
                    mismatches.push(at(&format!(
                        "d2_entry: topic {topic} may not differ from 2.x.x"
                    )));
                }
                if !MIGRATION_DOC.contains(&format!("id=\"{anchor}\"")) {
                    mismatches.push(at(&format!(
                        "d2_entry: anchor {anchor} is missing from docs/prism-rust-migration.md"
                    )));
                }
                if case["expected_2xx"] == case["expected_3xx"] {
                    mismatches.push(at("d2_entry: expected_2xx equals expected_3xx"));
                }
                // The 3.x.x rule's payout, and the frozen 2.x.x rule's payout
                // recomputed by this engine: only the rule may differ.
                check(
                    &at("expected_3xx"),
                    &case["expected_3xx"],
                    entry_point,
                    &input["bundle_3xx"],
                    &mut mismatches,
                );
                check(
                    &at("expected_2xx"),
                    &case["expected_2xx"],
                    entry_point,
                    &input["bundle_2xx"],
                    &mut mismatches,
                );
            } else if case.get("expected_2xx").is_some() || case.get("expected_3xx").is_some() {
                mismatches.push(at("expected_2xx/expected_3xx without a d2_entry"));
            } else if entry_point == "bundle_payout" {
                for bundle in ["bundle_3xx", "bundle_2xx"] {
                    check(
                        &at(&format!("expected ({bundle})")),
                        &case["expected"],
                        entry_point,
                        &input[bundle],
                        &mut mismatches,
                    );
                }
            } else {
                check(
                    &at("expected"),
                    &case["expected"],
                    entry_point,
                    input,
                    &mut mismatches,
                );
            }
        }
    }

    // A documented entry with no vector, or a vector with no entry, fails.
    let used: BTreeSet<String> = d2_cases.keys().cloned().collect();
    let listed: BTreeSet<String> = D2_ENTRIES.iter().map(|entry| entry.to_string()).collect();
    match documented_d2_entries() {
        Ok(documented) => {
            if documented != listed {
                mismatches.push(format!(
                    "docs/prism-rust-migration.md D2 entries: expected {listed:?}, got {documented:?}"
                ));
            }
            if used != documented {
                mismatches.push(format!(
                    "D2 entries used by vectors {used:?} differ from the documented entries {documented:?}"
                ));
            }
        }
        Err(err) => mismatches.push(err),
    }

    for topic in 1..=9 {
        let count = cases_per_topic.get(&topic).copied().unwrap_or(0);
        println!("money-path vectors: topic {topic}: {count} case(s)");
        if count < 2 {
            mismatches.push(format!(
                "topic {topic}: expected at least 2 cases, found {count}"
            ));
        }
    }
    println!(
        "money-path vectors: {} D2-tagged case(s): {d2_cases:?}",
        d2_cases.values().sum::<usize>()
    );
    assert!(
        mismatches.is_empty(),
        "{} money-path vector mismatch(es):\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}
