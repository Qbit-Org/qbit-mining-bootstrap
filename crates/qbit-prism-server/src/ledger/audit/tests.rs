use super::*;

fn shares(difficulties: &[u128]) -> Vec<AcceptedShare> {
    difficulties
        .iter()
        .enumerate()
        .map(|(index, &share_difficulty)| AcceptedShare {
            share_seq: index as u64 + 1,
            share_id: String::new(),
            miner_id: String::new(),
            order_key: String::new(),
            p2mr_program_hex: String::new(),
            share_difficulty,
            network_difficulty: 1,
            template_height: 1,
            job_id: String::new(),
            job_issued_at_ms: 0,
            accepted_at_ms: 0,
            ntime: 0,
            credit_policy: None,
        })
        .collect()
}

#[test]
fn oldest_boundary_retains_exact_and_overshooting_crossings() {
    for difficulties in [vec![10], vec![11], vec![7, 3], vec![8, 3]] {
        assert_eq!(
            oldest_boundary(10, &shares(&difficulties)).unwrap(),
            OldestBoundary::Crossing
        );
    }
    assert_eq!(
        oldest_boundary(10, &shares(&[6, 3])).unwrap(),
        OldestBoundary::Partial
    );
    assert_eq!(
        oldest_boundary(10, &shares(&[9])).unwrap(),
        OldestBoundary::Partial
    );
}

#[test]
fn oldest_boundary_refuses_empty_windows_and_excess_prefixes() {
    assert_eq!(
        oldest_boundary(10, &[]).unwrap_err().to_string(),
        "audit share snapshot cannot be empty"
    );
    for difficulties in [vec![1, 10], vec![1, 11], vec![0, 10]] {
        assert_eq!(
            oldest_boundary(10, &shares(&difficulties))
                .unwrap_err()
                .to_string(),
            "audit share snapshot extends past canonical oldest share"
        );
    }
}

#[test]
fn oldest_boundary_saturates_without_overflowing_total_difficulty() {
    assert_eq!(
        oldest_boundary(u128::MAX, &shares(&[u128::MAX, u128::MAX - 1])).unwrap(),
        OldestBoundary::Crossing
    );
    assert_eq!(
        oldest_boundary(u128::MAX, &shares(&[u128::MAX - 2, 1])).unwrap(),
        OldestBoundary::Partial
    );
    assert!(oldest_boundary(u128::MAX, &shares(&[1, u128::MAX, u128::MAX])).is_err());
}

#[test]
fn oldest_boundary_excludes_one_row_across_multiple_database_pages() {
    let mut window = shares(&vec![1; 10_000]);
    assert_eq!(
        oldest_boundary(10_000, &window).unwrap(),
        OldestBoundary::Crossing
    );
    // A page boundary is an ordinary share, never another excluded oldest row.
    window[VERIFY_PAGE_ROWS as usize].share_difficulty = 2;
    assert!(oldest_boundary(10_000, &window).is_err());
    assert_eq!(
        oldest_boundary(10_000, &window[1..]).unwrap(),
        OldestBoundary::Crossing
    );
    assert_eq!(
        oldest_boundary(10_000, &window[2..]).unwrap(),
        OldestBoundary::Partial
    );
}

#[test]
fn oldest_boundary_matches_an_independent_reverse_walk() {
    // Fixed seed, full-width values and zero-weight raw rows exercise the
    // arithmetic domain without a database or a sum that could overflow.
    let mut state = 0x0003_5637_9d73_3ef9_u64;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for trial in 0..20_000 {
        let len = 1 + (next() % 32) as usize;
        let weight = match trial % 4 {
            0 => u128::MAX - u128::from(next() % 32),
            1 => ((u128::from(next()) << 64) | u128::from(next())).max(1),
            _ => 1 + u128::from(next() % 1_000),
        };
        let difficulties: Vec<_> = (0..len)
            .map(|_| match trial % 4 {
                0 => u128::MAX - u128::from(next() % 32),
                1 => (u128::from(next()) << 64) | u128::from(next()),
                _ => u128::from(next() % 100),
            })
            .collect();
        let window = shares(&difficulties);
        // Independent stopping rule: compare before subtracting, walk newest
        // first, and retain the first row that covers the remaining target.
        let mut expected_first = 0;
        let mut remaining = weight;
        for index in (0..len).rev() {
            if difficulties[index] >= remaining {
                expected_first = index;
                break;
            }
            remaining -= difficulties[index];
        }
        for first in 0..len {
            let accepted = match oldest_boundary(weight, &window[first..]) {
                Ok(OldestBoundary::Crossing) => true,
                Ok(OldestBoundary::Partial) => first == 0,
                Err(_) => false,
            };
            assert_eq!(
                accepted,
                first == expected_first,
                "trial {trial}, first {first}, weight {weight}, difficulties {difficulties:?}"
            );
        }
    }
}
