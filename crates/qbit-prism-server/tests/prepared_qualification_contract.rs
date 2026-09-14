//! Preparation-only tests: fixture geometry and qualification guard behavior.
//! These tests do not execute refresh/resume or establish the #273 budgets.

#[path = "support/prepared_work_assertions.rs"]
mod assertions;
// Reuse #264's loader/shape; this target exercises only its pure input API.
#[allow(dead_code)]
#[path = "support/window_fixture.rs"]
mod window_fixture;

use assertions::*;
use serde_json::json;
use window_fixture::{WindowPlan, SHARE_BYTES_BAND, WINDOW_WEIGHT};

#[test]
fn production_and_headroom_inputs_fill_exact_windows() {
    for count in [REGRESSION_SHARES, HEADROOM_SHARES] {
        let plan = WindowPlan::new(count).unwrap();
        assert_eq!(plan.share_count(), count);
        assert_eq!(u128::from(count) * plan.share_difficulty(), WINDOW_WEIGHT);
        // Cover every sequence-number width, plus midpoint and final row.
        // No full-window allocation or database load is needed for this check.
        for index in [
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
        ] {
            let share = plan.share(index);
            let bytes = serde_json::to_vec(&share).unwrap();
            assert!((SHARE_BYTES_BAND.0..=SHARE_BYTES_BAND.1).contains(&(bytes.len() as f64)));
            let decoded: qbit_prism::AcceptedShare = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(decoded, share);
            assert_eq!(share.share_seq, index);
            assert_eq!(share.share_difficulty, plan.share_difficulty());
            assert!(share.accepted_at_ms > share.job_issued_at_ms);
        }
    }
}

#[test]
fn stored_shares_are_rejected_at_any_depth() {
    for payload in [
        json!({"shares": []}),
        json!({"snapshot": {"shares": []}}),
        json!({"bundle": {"reward_manifest": {"shares": [1]}}}),
        json!({"nested": [{"shares": null}]}),
    ] {
        assert!(assert_no_shares_key(&payload).is_err());
    }
    assert_no_shares_key(&json!({"share_count": 400_000, "template_sha256": "digest"})).unwrap();
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
    // A successfully observed zero is distinct from a missing measurement.
    assert_refresh_measurements(400_000, 400_000, Some(1), Some(0)).unwrap();
}

#[test]
fn canonical_comparison_requires_present_identical_bytes() {
    assert!(assert_canonical_equality(b"", b"").is_err());
    assert!(assert_canonical_equality(b"native-audit", b"legacy-audit").is_err());
    assert!(assert_canonical_equality(b"native-audit", b"").is_err());
    assert_canonical_equality(b"native-audit", b"native-audit").unwrap();
}
