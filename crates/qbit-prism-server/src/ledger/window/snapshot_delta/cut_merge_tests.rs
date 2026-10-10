//! The dual-writer delta's merge, against a model of the full scan.
//!
//! The full scan is the newest-first walk over every eligible row until the
//! saturating weight fold reaches zero, the crossing row kept. The delta
//! path must produce exactly that walk from the retained window, the rows the
//! fresh cut adds at or above its first row, and a margin under it. These
//! cases generate two nodes' interleaved rows, a retained cut and a fresh one
//! that covers it (so the peer's new rows land anywhere: above the retained
//! window, inside it, or below its first row), a target that may move either
//! way, and check the merge, the margin and the assembly against the model.
use super::*;
use proptest::prelude::*;

/// One generated ledger row: its node, `share_seq` and difficulty.
#[derive(Clone, Debug)]
struct Row {
    node: u8,
    seq: u64,
    difficulty: u128,
}

fn share(row: &Row) -> AcceptedShare {
    AcceptedShare {
        share_seq: row.seq,
        share_id: format!("n{}:{}", row.node, row.seq),
        miner_id: format!("miner-{}", row.seq % 3),
        order_key: "00".into(),
        p2mr_program_hex: "11".repeat(32),
        share_difficulty: row.difficulty,
        network_difficulty: 1,
        template_height: 1,
        job_id: "job".into(),
        job_issued_at_ms: 1,
        accepted_at_ms: 2,
        ntime: 1,
        credit_policy: None,
    }
}

/// The full scan over the rows `cut` admits: ascending, from the crossing
/// row up, and the weight left unmet (zero when it crossed).
fn walk(rows: &[Row], cut: &WindowCut, weight: u128) -> (Vec<AcceptedShare>, u128) {
    let mut eligible: Vec<&Row> = rows
        .iter()
        .filter(|row| cut.admits(row.node, row.seq))
        .collect();
    eligible.sort_by_key(|row| std::cmp::Reverse(row.seq));
    let mut remaining = weight;
    let mut window = Vec::new();
    for row in eligible {
        if remaining == 0 {
            break;
        }
        remaining = remaining.saturating_sub(row.difficulty);
        window.push(share(row));
    }
    window.reverse();
    (window, remaining)
}

/// Rows of two writers: node 0 even and node 1 odd values, each ascending
/// with gaps, interleaved however the generator likes, plus difficulties.
fn rows() -> impl Strategy<Value = Vec<Row>> {
    prop::collection::vec((any::<bool>(), 1u64..4, 1u128..2_000), 1..160).prop_map(|steps| {
        let mut next = [2u64, 1];
        steps
            .into_iter()
            .map(|(peer, step, difficulty)| {
                let node = u8::from(peer);
                let seq = next[node as usize];
                next[node as usize] += 2 * step;
                Row {
                    node,
                    seq,
                    difficulty,
                }
            })
            .collect()
    })
}

/// A cut entry for `node` somewhere in its rows' range, or none.
fn entry(rows: &[Row], node: u8, pick: u64) -> Option<u64> {
    let top = rows
        .iter()
        .filter(|row| row.node == node)
        .map(|row| row.seq)
        .max()?;
    match pick % (top + 2) {
        0 => None,
        value => Some(value.min(top + 1)),
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, failure_persistence: None, ..ProptestConfig::default() })]

    #[test]
    fn the_merge_margin_and_assembly_reproduce_the_full_scan(
        rows in rows(),
        picks in (any::<u64>(), any::<u64>(), any::<u64>(), any::<u64>()),
        weights in (1u128..60_000, 1u128..60_000),
    ) {
        // A retained cut and a fresh one that covers it.
        let old = WindowCut::new(entry(&rows, 0, picks.0), entry(&rows, 1, picks.1)).unwrap();
        let grow = |node, pick: u64| {
            let base = old.get(node).unwrap().unwrap_or(0);
            match entry(&rows, node, pick) {
                Some(value) => Some(value.max(base)),
                None => (base > 0).then_some(base),
            }
        };
        let fresh = WindowCut::new(grow(0, picks.2), grow(1, picks.3)).unwrap();
        prop_assert!(fresh.covers(&old));
        let (prior, _) = walk(&rows, &old, weights.0);
        prop_assume!(!prior.is_empty());
        let first = prior[0].share_seq;
        // The delta the path reads: inside the fresh cut, outside the old,
        // at or above the retained first row; node by node, so the merge
        // must sort the two runs.
        let mut delta: Vec<AcceptedShare> = Vec::new();
        for node in 0..WindowCut::NODES {
            delta.extend(
                rows.iter()
                    .filter(|row| {
                        row.node == node
                            && row.seq >= first
                            && fresh.admits(node, row.seq)
                            && !old.admits(node, row.seq)
                    })
                    .map(share),
            );
        }
        let mut merge = merge_cut_delta(prior.clone(), delta, weights.1).unwrap();
        // The margin: every row the fresh cut admits under the retained
        // first row, newest first, until the target is met.
        let mut under: Vec<&Row> = rows
            .iter()
            .filter(|row| row.seq < first && fresh.admits(row.node, row.seq))
            .collect();
        under.sort_by_key(|row| std::cmp::Reverse(row.seq));
        for row in under {
            if merge.remaining == 0 {
                break;
            }
            merge.remaining = merge.remaining.saturating_sub(row.difficulty);
            merge.margin.push(share(row));
        }
        let remaining = merge.remaining;
        let assembled = merge.assemble();
        let (expected, expected_remaining) = walk(&rows, &fresh, weights.1);
        prop_assert_eq!(remaining, expected_remaining);
        prop_assert_eq!(&assembled, &expected);
        if remaining == 0 {
            prop_assert!(window_invariant(&assembled, weights.1));
        }
    }
}

#[test]
fn an_append_only_delta_keeps_the_retained_vector() {
    let rows: Vec<Row> = (1..=6)
        .map(|seq| Row {
            node: 0,
            seq,
            difficulty: 10,
        })
        .collect();
    let prior: Vec<AcceptedShare> = rows[..4].iter().map(share).collect();
    let delta: Vec<AcceptedShare> = rows[4..].iter().map(share).collect();
    let merge = merge_cut_delta(prior, delta, 35).unwrap();
    assert_eq!(merge.remaining, 0);
    assert_eq!(merge.retired, 2);
    assert_eq!(
        merge
            .kept
            .iter()
            .map(|share| share.share_seq)
            .collect::<Vec<_>>(),
        vec![3, 4, 5, 6]
    );
}

#[test]
fn late_peer_rows_inside_the_window_are_merged_in_order() {
    // Node 0 retained 2, 4, 6, 8; node 1's rows 5 and 7 arrive late, inside
    // the retained range, and node 1's 9 above it.
    let retained: Vec<AcceptedShare> = [2, 4, 6, 8]
        .iter()
        .map(|seq| {
            share(&Row {
                node: 0,
                seq: *seq,
                difficulty: 10,
            })
        })
        .collect();
    let late: Vec<AcceptedShare> = [9, 5, 7]
        .iter()
        .map(|seq| {
            share(&Row {
                node: 1,
                seq: *seq,
                difficulty: 10,
            })
        })
        .collect();
    let merge = merge_cut_delta(retained, late, 1_000).unwrap();
    assert_eq!(merge.remaining, 1_000 - 70);
    assert_eq!(merge.retired, 0);
    assert_eq!(
        merge
            .kept
            .iter()
            .map(|share| share.share_seq)
            .collect::<Vec<_>>(),
        vec![2, 4, 5, 6, 7, 8, 9]
    );
}

#[test]
fn a_delta_row_that_repeats_a_retained_row_is_refused() {
    let retained = vec![share(&Row {
        node: 0,
        seq: 4,
        difficulty: 10,
    })];
    let repeat = vec![
        share(&Row {
            node: 1,
            seq: 3,
            difficulty: 10,
        }),
        share(&Row {
            node: 1,
            seq: 4,
            difficulty: 10,
        }),
    ];
    assert!(merge_cut_delta(retained, repeat, 100).is_err());
}

fn retained_with(cut: Option<WindowCut>, seqs: &[u64]) -> RetainedShares {
    RetainedShares {
        network: 1,
        anchor_ms: 100,
        cutoff: *seqs.last().unwrap_or(&0),
        cut,
        shares: seqs
            .iter()
            .map(|seq| {
                share(&Row {
                    node: 0,
                    seq: *seq,
                    difficulty: 1,
                })
            })
            .collect(),
        leaf: Some(LeafWitness {
            tableoid: 1,
            inherits_xmin: "1".into(),
            timeline: "00000001".into(),
        }),
    }
}

#[test]
fn the_cut_refusals_are_made_per_node() {
    let cut = |node_0, node_1| WindowCut::new(node_0, node_1).unwrap();
    let retained = retained_with(Some(cut(Some(8), Some(5))), &[2, 4, 5, 8]);
    // Covering cuts pass; the scalar cutoff is not consulted with a cut.
    assert_eq!(retained.refusal(100, 0, Some(&cut(Some(8), Some(5)))), None);
    assert_eq!(
        retained.refusal(101, 0, Some(&cut(Some(20), Some(9)))),
        None
    );
    // Either node's entry moving back is a regression, and so is a change
    // of mode in either direction.
    for fresh in [
        cut(Some(7), Some(5)),
        cut(Some(8), Some(4)),
        cut(Some(8), None),
    ] {
        assert_eq!(
            retained.refusal(101, 99, Some(&fresh)),
            Some(WindowAcquisition::CutoffRegressed),
            "{fresh:?}"
        );
    }
    assert_eq!(
        retained.refusal(101, 99, None),
        Some(WindowAcquisition::CutoffRegressed)
    );
    assert_eq!(
        retained_with(None, &[1, 2]).refusal(101, 2, Some(&cut(Some(2), None))),
        Some(WindowAcquisition::CutoffRegressed)
    );
    // The anchor still decides first.
    assert_eq!(
        retained.refusal(99, 0, Some(&cut(Some(8), Some(5)))),
        Some(WindowAcquisition::AnchorRegressed)
    );
    // The delta bound sums both nodes' new slots.
    let half = MAX_DELTA_SLOTS / 2;
    assert_eq!(
        retained.refusal(101, 0, Some(&cut(Some(8 + half), Some(5 + half)))),
        None
    );
    assert_eq!(
        retained.refusal(101, 0, Some(&cut(Some(8 + half + 1), Some(5 + half)))),
        Some(WindowAcquisition::DeltaTooLarge)
    );
    // The retained tail must be the cut's top row, as it must be the
    // cutoff without a cut: above it, or below it, is not that window.
    for entries in [(Some(6), Some(5)), (Some(10), Some(5)), (Some(8), Some(9))] {
        assert_eq!(
            retained_with(Some(cut(entries.0, entries.1)), &[2, 4, 8]).refusal(
                101,
                0,
                Some(&cut(Some(20), Some(20)))
            ),
            Some(WindowAcquisition::TailMismatch),
            "{entries:?}"
        );
    }
    assert_eq!(
        retained_with(Some(cut(Some(8), None)), &[2, 4, 8]).refusal(
            101,
            0,
            Some(&cut(Some(10), Some(3)))
        ),
        None
    );
}
