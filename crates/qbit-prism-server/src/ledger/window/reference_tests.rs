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
    let mut state = WindowRead::new();
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
            reversed
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

fn snapshot_of(shares: Vec<AcceptedShare>) -> Snapshot {
    Snapshot {
        anchor_ms: 1_700_000_000_000,
        share_seq: shares.last().map(|share| share.share_seq).unwrap_or(0),
        payout_revision: 5,
        shares,
        prior_balances: vec![
            CarryForwardBalance {
                recipient_id: "z".into(),
                order_key: "a".into(),
                p2mr_program_hex: "11".repeat(32),
                balance_sats: 7,
            },
            CarryForwardBalance {
                recipient_id: "a".into(),
                order_key: "z".into(),
                p2mr_program_hex: "22".repeat(32),
                balance_sats: 9,
            },
        ],
    }
}

#[test]
fn from_snapshot_streams_the_native_digest_and_keeps_an_empty_window_empty() {
    let shares = vec![share(4), share(9), share(11)];
    let snapshot = snapshot_of(shares.clone());
    let reference = WindowRef::from_snapshot(&snapshot).unwrap();
    assert_eq!(reference.anchor_ms, snapshot.anchor_ms);
    assert_eq!(
        reference.prior_balances_digest,
        qbit_prism::prior_balances_digest(&snapshot.prior_balances)
    );
    let range = reference.shares.expect("a non-empty snapshot has a range");
    assert_eq!(range.first_share_seq, 4);
    assert_eq!(range.last_share_seq, 11);
    assert_eq!(range.share_count, 3);
    // The streamed digest is the bytes `qbit_prism_audit_snapshots` stores.
    let expected: [u8; 32] = Sha256::digest(serde_json::to_vec(&shares).unwrap()).into();
    assert_eq!(range.snapshot_sha256, expected);
    // The same bytes the paged reader accumulates one share at a time.
    let mut state = WindowRead::new();
    for share in &shares {
        state.push(share.clone()).unwrap();
    }
    assert_eq!(state.finish(range).unwrap(), shares);

    let mut permuted = snapshot_of(Vec::new());
    permuted.prior_balances.reverse();
    let empty = WindowRef::from_snapshot(&permuted).unwrap();
    assert!(empty.shares.is_none(), "an empty snapshot has no range");
    // The balances digest sorts internally, so the vector order never moves it.
    assert_eq!(empty.prior_balances_digest, reference.prior_balances_digest);
}

#[test]
fn canonical_balance_snapshot_is_sorted_and_permutation_independent() {
    let balances = snapshot_of(Vec::new()).prior_balances;
    let mut reversed = balances.clone();
    reversed.reverse();
    let (digest, bytes) = canonical_balance_snapshot(balances.clone()).unwrap();
    let (other_digest, other_bytes) = canonical_balance_snapshot(reversed).unwrap();
    assert_eq!((digest, &bytes), (other_digest, &other_bytes));
    let mut sorted = balances;
    sort_balances(&mut sorted);
    assert_eq!(bytes, serde_json::to_vec(&sorted).unwrap());
    assert_eq!(digest, qbit_prism::prior_balances_digest(&sorted));
}

#[tokio::test(flavor = "current_thread")]
async fn a_cancelled_blocking_handoff_is_task_failed_and_never_decode() {
    // A blocking task aborted before it starts is the shape a cancelled
    // hand-off takes. Every hand-off in this module maps it the same way.
    let handle = tokio::task::spawn_blocking(|| unreachable!("cancelled before it ran"));
    handle.abort();
    let error = handle
        .await
        .map(|(): ()| ())
        .map_err(WindowError::TaskFailed)
        .unwrap_err();
    assert!(
        matches!(&error, WindowError::TaskFailed(join) if join.is_cancelled()),
        "{error:?}"
    );
    assert!(!matches!(error, WindowError::Decode(_)));
    assert!(error.to_string().contains("cancelled or failed"), "{error}");
}
