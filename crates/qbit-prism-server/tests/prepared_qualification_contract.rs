//! Preparation-only tests: fixture geometry and qualification guard behavior.
//! These tests do not execute refresh/resume or establish the #273 budgets.

#[path = "support/prepared_work_assertions.rs"]
mod assertions;
// Reuse #264's loader/shape; this target exercises only its pure input API.
#[allow(dead_code)]
#[path = "support/window_fixture.rs"]
mod window_fixture;

use assertions::*;
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::PayoutPolicy;
use qbit_prism_server::ledger::{
    CompactPrepared, PreparedAuditHashes, PreparedTemplate, SignerKeys, Snapshot, WindowRef,
};
use serde_json::{json, Value};
use window_fixture::{WindowPlan, SHARE_BYTES_BAND, WINDOW_WEIGHT};

#[test]
fn production_and_headroom_inputs_fill_exact_windows() {
    for count in [REGRESSION_SHARES, HEADROOM_SHARES] {
        let plan = WindowPlan::new(count).unwrap();
        assert_eq!(plan.share_count(), count);
        assert_eq!(u128::from(count) * plan.share_difficulty(), WINDOW_WEIGHT);
        // Cover every sequence-number width, plus midpoint and final row.
        // No full-window allocation or database load is needed for this check.
        let indices = [
            1,
            9,
            10,
            99,
            100,
            999,
            1_000,
            9_999,
            10_000,
            99_999,
            100_000,
            count / 2,
            count,
        ];
        let mut sample_bytes = 0;
        for index in indices {
            let share = plan.share(index);
            let bytes = serde_json::to_vec(&share).unwrap();
            sample_bytes += bytes.len();
            let decoded: qbit_prism::AcceptedShare = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(decoded, share);
            assert_eq!(share.share_seq, index);
            assert_eq!(share.share_difficulty, plan.share_difficulty());
            assert!(share.accepted_at_ms > share.job_issued_at_ms);
        }
        let average = sample_bytes as f64 / indices.len() as f64;
        assert!((SHARE_BYTES_BAND.0..=SHARE_BYTES_BAND.1).contains(&average));
    }
}

fn compact_payload(nonempty: bool) -> Value {
    let plan = WindowPlan::new(REGRESSION_SHARES).unwrap();
    let snapshot = Snapshot {
        anchor_ms: 1_700_000_000_000,
        share_seq: 5,
        payout_revision: 0,
        shares: if nonempty {
            vec![plan.share(1), plan.share(3)]
        } else {
            vec![]
        },
        prior_balances: vec![],
    };
    let template = PreparedTemplate::encode(&json!({
        "previousblockhash": "ab".repeat(32), "height": 101, "transactions": []
    }))
    .unwrap();
    let manifest = ManifestSigningKey::from_seed_hex(&"41".repeat(32)).unwrap();
    let ledger = ManifestSigningKey::from_seed_hex(&"42".repeat(32)).unwrap();
    let record = CompactPrepared {
        format_version: CompactPrepared::FORMAT_VERSION,
        window: WindowRef::from_snapshot(&snapshot).unwrap(),
        share_seq: snapshot.share_seq,
        payout_revision: snapshot.payout_revision,
        template_sha256: template.sha256().into(),
        parent_hash: "ab".repeat(32),
        parent_of_tip: "ac".repeat(32),
        fingerprint: "qualification-fixture".into(),
        generation: 11,
        coinbase_suffix_hex: "01020300000000".into(),
        payout_policy: PayoutPolicy::day_one_default(),
        ctv: None,
        fee: None,
        audit_builder_version: qbit_prism::AUDIT_BUILDER_VERSION,
        signer_keys: SignerKeys::of(&manifest, &ledger),
        audit_hashes: nonempty.then_some(PreparedAuditHashes {
            audit_bundle_sha256: "cd".repeat(32),
            coinbase_manifest_sha256: "ef".repeat(32),
        }),
    };
    // Match jobs/prepared.rs::encode_record: serialize the real record, then
    // add immutable original expiry. This is serializer coverage, not a DB run.
    let mut payload = serde_json::to_value(&record).unwrap();
    payload["original_expires_at_ms"] = json!(1_700_000_030_000_i64);
    payload
}

#[test]
fn serialized_empty_compact_reference_is_allowed() {
    let payload = compact_payload(false);
    assert_eq!(payload["window"]["shares"], Value::Null);
    assert_no_materialized_shares(&payload).unwrap();
}

#[test]
fn serialized_nonempty_compact_reference_is_allowed() {
    let payload = compact_payload(true);
    let range = &payload["window"]["shares"];
    assert_eq!(range["first_share_seq"], 1);
    assert_eq!(range["last_share_seq"], 3);
    assert_eq!(range["share_count"], 2); // Sequence gaps are valid.
    assert_eq!(range["snapshot_sha256"].as_str().unwrap().len(), 64);
    assert_no_materialized_shares(&payload).unwrap();
}

#[test]
fn stored_shares_are_rejected_at_any_depth() {
    let compact = compact_payload(true);
    let range = &compact["window"]["shares"];
    let row = serde_json::to_value(WindowPlan::new(REGRESSION_SHARES).unwrap().share(1)).unwrap();
    for payload in [
        json!({"shares": []}),
        json!({"snapshot": {"shares": []}}),
        json!({"bundle": {"reward_manifest": {"shares": [1]}}}),
        json!({"nested": [{"shares": null}]}),
        json!({"shares": range}),
        json!({"snapshot": {"shares": [row.clone()]}}),
        json!({"window": {"shares": [row.clone()]}}),
        json!({"nested": [{"window": compact["window"]}]}),
        json!([compact.clone()]),
    ] {
        assert!(assert_no_materialized_shares(&payload).is_err());
    }
    for value in [Value::Null, range.clone(), json!([]), json!([row])] {
        let mut payload = compact.clone();
        payload["hidden"] = json!([{"deeper": {"shares": value}}]);
        assert!(assert_no_materialized_shares(&payload).is_err());
    }
    assert_no_materialized_shares(&json!({"share_count": 400_000, "template_sha256": "digest"}))
        .unwrap();
}

#[test]
fn malformed_or_extra_reference_metadata_is_rejected() {
    let compact = compact_payload(true);
    for value in [json!([]), json!([{}]), json!({}), json!(1), json!("range")] {
        let mut payload = compact.clone();
        payload["window"]["shares"] = value;
        assert!(assert_no_materialized_shares(&payload).is_err());
    }
    for (field, value) in [
        ("first_share_seq", json!(0)),
        ("first_share_seq", json!(4)),
        ("first_share_seq", json!("1")),
        ("last_share_seq", json!(-1)),
        ("last_share_seq", json!(u64::MAX)),
        ("share_count", json!(0)),
        ("share_count", json!(4)),
        ("share_count", json!(2.5)),
        ("snapshot_sha256", Value::Null),
        ("snapshot_sha256", json!("AB".repeat(32))),
        ("snapshot_sha256", json!("ab".repeat(31))),
        ("snapshot_sha256", json!(vec![0; 32])),
        ("extra", json!(1)),
        ("hidden", json!([{"shares": []}])),
    ] {
        let mut payload = compact.clone();
        payload["window"]["shares"][field] = value;
        assert!(assert_no_materialized_shares(&payload).is_err(), "{field}");
    }
    for field in [
        "first_share_seq",
        "last_share_seq",
        "share_count",
        "snapshot_sha256",
    ] {
        let mut payload = compact.clone();
        payload["window"]["shares"]
            .as_object_mut()
            .unwrap()
            .remove(field);
        assert!(
            assert_no_materialized_shares(&payload).is_err(),
            "missing {field}"
        );
    }
    for nonempty in [false, true] {
        for field in ["anchor_ms", "prior_balances_digest", "shares"] {
            let mut payload = compact_payload(nonempty);
            payload["window"].as_object_mut().unwrap().remove(field);
            assert!(
                assert_no_materialized_shares(&payload).is_err(),
                "missing {field}"
            );
        }
        for (field, value) in [
            ("anchor_ms", json!("1700000000000")),
            ("prior_balances_digest", json!("bad-digest")),
            ("extra", json!(1)),
            ("hidden", json!([{"shares": []}])),
        ] {
            let mut payload = compact_payload(nonempty);
            payload["window"][field] = value;
            assert!(assert_no_materialized_shares(&payload).is_err(), "{field}");
        }
    }
}

#[test]
fn unknown_measurements_and_budget_boundaries_never_pass() {
    assert!(assert_refresh_measurements(400_000, 400_000, None, Some(0)).is_err());
    assert!(assert_refresh_measurements(400_000, 400_000, Some(1), None).is_err());
    assert!(assert_refresh_measurements(400_000, 400_000, Some(0), Some(0)).is_err());
    assert!(assert_refresh_measurements(400_000, 399_999, Some(1), Some(1)).is_err());
    assert!(assert_refresh_measurements(0, 0, Some(1), Some(1)).is_err());
    assert!(
        assert_refresh_measurements(400_000, 400_000, Some(JSONB_LIMIT_BYTES), Some(1)).is_err()
    );
    assert!(assert_refresh_measurements(400_000, 400_000, Some(1), Some(WAL_LIMIT_BYTES)).is_err());
    assert_refresh_measurements(
        400_000,
        400_000,
        Some(JSONB_LIMIT_BYTES - 1),
        Some(WAL_LIMIT_BYTES - 1),
    )
    .unwrap();
}

#[test]
fn a_logged_refresh_write_requires_positive_wal() {
    let error = assert_refresh_measurements(400_000, 400_000, Some(1), Some(0))
        .expect_err("a logged prepared write cannot qualify with zero WAL");
    assert!(error.to_string().contains("no WAL"));
    assert_refresh_measurements(400_000, 400_000, Some(1), Some(1)).unwrap();
}

#[test]
fn canonical_comparison_requires_present_identical_bytes() {
    assert!(assert_canonical_equality(b"", b"").is_err());
    assert!(assert_canonical_equality(b"native-audit", b"legacy-audit").is_err());
    assert!(assert_canonical_equality(b"native-audit", b"").is_err());
    assert_canonical_equality(b"native-audit", b"native-audit").unwrap();
}
