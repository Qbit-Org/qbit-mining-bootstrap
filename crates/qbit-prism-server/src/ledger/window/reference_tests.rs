use super::*;
use serde_json::json;

fn share(seq: u64) -> AcceptedShare {
    AcceptedShare {
        share_seq: seq,
        share_id: format!("share:{seq}:é\\\""),
        miner_id: "miner".into(),
        order_key: "miner".into(),
        p2mr_program_hex: "11".repeat(32),
        share_difficulty: u64::MAX as u128 + 1,
        network_difficulty: u64::MAX as u128 + 1,
        template_height: 1,
        job_id: "job".into(),
        job_issued_at_ms: 1,
        accepted_at_ms: 2,
        ntime: 1,
        credit_policy: (seq.is_multiple_of(2)).then(|| "stale-grace".into()),
    }
}

#[test]
fn reference_hex_is_strict_and_empty_is_distinct_from_a_range() {
    let empty = WindowRef {
        anchor_ms: 7,
        prior_balances_digest: [0xab; 32],
        shares: None,
    };
    let range = WindowRef {
        shares: Some(ShareRange {
            first_share_seq: 1,
            last_share_seq: 10,
            share_count: 2,
            snapshot_sha256: [0xcd; 32],
        }),
        ..empty
    };
    for reference in [empty, range] {
        let value = serde_json::to_value(reference).unwrap();
        assert_eq!(value["prior_balances_digest"], "ab".repeat(32));
        assert_eq!(
            serde_json::from_value::<WindowRef>(value).unwrap(),
            reference
        );
    }
    for invalid in [
        json!("AB".repeat(32)),
        json!("a".repeat(63)),
        json!("a".repeat(65)),
        json!("g".repeat(64)),
        json!(vec![0; 32]),
        json!(null),
    ] {
        let mut value = serde_json::to_value(range).unwrap();
        value["prior_balances_digest"] = invalid.clone();
        assert!(serde_json::from_value::<WindowRef>(value).is_err());
        let mut value = serde_json::to_value(range).unwrap();
        value["shares"]["snapshot_sha256"] = invalid;
        assert!(serde_json::from_value::<WindowRef>(value).is_err());
    }
}

#[test]
fn ranges_reject_unrepresentable_sequences_and_impossible_counts() {
    for (first, last, count) in [
        (0, 1, 1),
        (2, 1, 1),
        (1, 2, 0),
        (1, 2, 3),
        (1, i64::MAX as u64 + 1, 1),
        (u64::MAX, u64::MAX, 1),
    ] {
        assert!(matches!(
            ShareRange {
                first_share_seq: first,
                last_share_seq: last,
                share_count: count,
                snapshot_sha256: [0; 32]
            }
            .bounds(),
            Err(WindowError::Decode(_))
        ));
    }
    assert_eq!(
        ShareRange {
            first_share_seq: 1,
            last_share_seq: i64::MAX as u64,
            share_count: i64::MAX as u64,
            snapshot_sha256: [0; 32]
        }
        .bounds()
        .unwrap(),
        (1, i64::MAX)
    );
}

#[test]
fn streamed_digest_preserves_native_bytes_and_differs_from_legacy_digest() {
    let shares = vec![share(1), share(3), share(4)];
    let expected: [u8; 32] = Sha256::digest(serde_json::to_vec(&shares).unwrap()).into();
    let mut state = WindowRead::new(0);
    for share in &shares {
        state.push(share.clone()).unwrap();
    }
    let actual = state
        .finish(ShareRange {
            first_share_seq: 1,
            last_share_seq: 4,
            share_count: 3,
            snapshot_sha256: expected,
        })
        .unwrap();
    assert_eq!(actual, shares);
    let legacy =
        qbit_prism::window::PayoutWindow::from_full_snapshot(shares, 2, u128::MAX, 4096).unwrap();
    assert_ne!(hex::encode(expected), legacy.canonical_digest_hex());
}

#[test]
fn balance_order_and_failure_variants_match_the_source() {
    let balance = |id: &str, key: &str| CarryForwardBalance {
        recipient_id: id.into(),
        order_key: key.into(),
        p2mr_program_hex: "11".repeat(32),
        balance_sats: 7,
    };
    let expected = vec![balance("z", "a"), balance("é", "a"), balance("a", "z")];
    let digest = qbit_prism::prior_balances_digest(&expected);
    let mut reversed = expected.clone();
    reversed.reverse();
    for source in [BalanceSource::Current, BalanceSource::AsIssued] {
        assert_eq!(
            check_balances(reversed.clone(), digest, source).unwrap(),
            expected
        );
    }
    assert!(matches!(
        check_balances(reversed.clone(), [0; 32], BalanceSource::Current),
        Err(WindowError::PriorBalancesChanged { .. })
    ));
    assert!(matches!(
        check_balances(reversed, [0; 32], BalanceSource::AsIssued),
        Err(WindowError::Decode(_))
    ));
}
