//! Adversarial differential contributed by the independent review of #275: a
//! legacy-style fixture with sequence gaps, mixed difficulties (the schema
//! forbids zero), ineligible rows and a partition boundary; retargets of up to
//! 8x in both directions; rows committed between the anchor and the extension
//! read; and corrupted retained inputs. Every advanced window must equal the
//! full reader's at the same anchor and pass the landing re-derivation; every
//! refusal must be predicted and named.
use super::super::super::super::audit::{verify_durable_range, AuditSnapshotWrite};
use super::*;
use rand::{Rng, SeedableRng};
use std::collections::BTreeMap;

async fn tableoid(ledger: &Ledger, seq: i64) -> Result<Option<i64>> {
    Ok(
        sqlx::query_scalar("SELECT tableoid::bigint FROM qbit_share_ledger WHERE share_seq=$1")
            .bind(seq)
            .fetch_optional(&ledger.pool)
            .await?,
    )
}

/// One fixture row, eligible at any anchor, at an explicit sequence.
async fn insert_row(ledger: &Ledger, seq: i64, difficulty: u128, accepted: bool) -> Result<()> {
    sqlx::query("INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,accepted,reject_reason,writer_id,writer_epoch) VALUES($1,'adv:'||$1::text,'miner','miner',decode(repeat('11',32),'hex'),$2::text::numeric,1,1,'job',to_timestamp(1),1,to_timestamp(2),$3,CASE WHEN $3 THEN NULL ELSE 'submitblock-rejected' END,'fixture',0)")
        .bind(seq).bind(difficulty.to_string()).bind(accepted).execute(&ledger.pool).await?;
    Ok(())
}

/// A row committed after `anchor_ms`, as native appends are: its clock is
/// one past the anchor, so it is ineligible at this anchor and eligible at
/// the next.
async fn insert_late_row(ledger: &Ledger, seq: i64, anchor_ms: i64) -> Result<()> {
    sqlx::query("INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,accepted,writer_id,writer_epoch) VALUES($1,'late:'||$1::text,'miner','miner',decode(repeat('11',32),'hex'),3,1,1,'job',to_timestamp(1),1,to_timestamp(($2::double precision+1)/1000),true,'fixture',0)")
        .bind(seq).bind(anchor_ms).execute(&ledger.pool).await?;
    Ok(())
}

/// The delta path at exactly `full`'s anchor and cutoff, undecided.
async fn advance_at(
    ledger: &Ledger,
    prior: RetainedShares,
    network: u128,
    full: &SnapshotCapture,
) -> Result<Advance> {
    let completion = ReadAdmission::default();
    let mut tx = ledger.begin().await?;
    let result = advance(
        &mut tx,
        completion.own(prior),
        network * 8,
        full.anchor_ms,
        i64::try_from(full.share_seq)?,
        &completion,
    )
    .await?;
    tx.commit().await?;
    Ok(result)
}

fn audit_write(
    shares: Vec<AcceptedShare>,
    anchor_ms: i64,
    network: u128,
) -> Result<AuditSnapshotWrite> {
    Ok(AuditSnapshotWrite {
        digest: hex::encode(Sha256::digest(serde_json::to_vec(&shares)?)),
        first_share_seq: i64::try_from(shares[0].share_seq)?,
        last_share_seq: i64::try_from(shares[shares.len() - 1].share_seq)?,
        anchor_ms,
        network_difficulty: network,
        share_count: i64::try_from(shares.len())?,
        inline: None,
        shares: std::sync::Arc::new(shares),
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_fixture_random_walk_matches_full_reader_or_refuses_for_a_named_reason() -> Result<()>
{
    for seed in [11u64, 2024, 987_654] {
        run(gate::site!(), |ledger| {
            Box::pin(async move {
                // Two leaves with a boundary at 400, which the walk crosses early
                // and then leaves behind.
                sqlx::raw_sql(
                    "ALTER TABLE qbit_share_ledger DETACH PARTITION qbit_share_ledger_p0;
                    SELECT qbit_prism_share_partition_create('qbit_share_ledger_p60',1,400);
                    SELECT qbit_prism_share_partition_create('qbit_share_ledger_p61',400,1000000);",
                )
                .execute(&ledger.pool)
                .await?;
                let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
                let difficulties: [u128; 6] = [1, 2, 3, 5, 8, 13];
                let gaps: [i64; 5] = [1, 1, 1, 2, 5];
                let mut seq: i64 = 0;
                for _ in 0..60 {
                    seq += gaps[rng.gen_range(0..gaps.len())];
                    insert_row(
                        ledger,
                        seq,
                        difficulties[rng.gen_range(0..difficulties.len())],
                        rng.gen_bool(0.9),
                    )
                    .await?;
                }
                let mut network: u128 = 4;
                let mut prior_capture = capture(ledger, network).await?;
                let mut counts: BTreeMap<&'static str, usize> = BTreeMap::new();
                let mut landed = 0usize;
                for step in 0..120 {
                    let appended = if rng.gen_bool(0.25) {
                        0
                    } else {
                        rng.gen_range(1..=40)
                    };
                    for _ in 0..appended {
                        seq += gaps[rng.gen_range(0..gaps.len())];
                        insert_row(
                            ledger,
                            seq,
                            difficulties[rng.gen_range(0..difficulties.len())],
                            rng.gen_bool(0.9),
                        )
                        .await?;
                    }
                    let previous = network;
                    // Every thirtieth step asks for far more weight than the
                    // history holds: the full reader is partial, the delta
                    // path must refuse, and the following step recovers
                    // from a retained partial window.
                    network = if step % 30 == 15 {
                        1 << 20
                    } else {
                        match rng.gen_range(0..9) {
                            0 => network + 1,
                            1 => network.saturating_sub(1).max(1),
                            2 => network * 2,
                            3 => (network / 2).max(1),
                            4 => network * 4,
                            5 => (network / 4).max(1),
                            6 => network * 8,
                            7 => (network / 8).max(1),
                            _ => network,
                        }
                        .min(64)
                    };
                    let full = capture(ledger, network).await?;
                    // Rows committed after the anchor and before the extension
                    // read: above the cutoff and past the anchor, so neither
                    // reader nor the landing proof may see them.
                    for _ in 0..rng.gen_range(0..=3) {
                        seq += 1;
                        insert_late_row(ledger, seq, full.anchor_ms).await?;
                    }
                    let prior = retained(prior_capture.clone(), previous);
                    let prior_first = i64::try_from(prior.shares[0].share_seq)?;
                    let cutoff = i64::try_from(full.share_seq)?;
                    let new_first = i64::try_from(full.shares[0].share_seq)?;
                    let cutoff_leaf = tableoid(ledger, cutoff).await?;
                    let predicted = crosses(&full, network)
                        && prior.leaf.is_some()
                        && cutoff_leaf.is_some()
                        && tableoid(ledger, prior_first).await? == cutoff_leaf
                        && tableoid(ledger, new_first).await? == cutoff_leaf;
                    match advance_at(ledger, prior, network, &full).await? {
                        Advance::Advanced {
                            shares,
                            leaf,
                            report,
                        } => {
                            assert!(
                                predicted,
                                "seed {seed} step {step}: advanced without prediction {report:?}"
                            );
                            let shares = shares.into_inner();
                            assert_eq!(shares, full.shares, "seed {seed} step {step} {report:?}");
                            assert_eq!(Some(&leaf), full.leaf.as_ref());
                            assert_eq!(report.window_rows, shares.len());
                            if report.retired_rows < report.prior_rows {
                                assert_eq!(
                                    report.window_rows,
                                    report.prior_rows - report.retired_rows
                                        + report.margin_rows
                                        + report.delta_rows,
                                    "seed {seed} step {step} {report:?}"
                                );
                            }
                            let candidate = Snapshot {
                                shares: shares.clone(),
                                ..full.snapshot.clone()
                            };
                            assert_eq!(
                                WindowRef::from_snapshot(&candidate)?,
                                WindowRef::from_snapshot(&full)?
                            );
                            // Landing re-derives exactly this window at the
                            // same anchor and target.
                            verify_durable_range(
                                &ledger.pool,
                                &audit_write(shares, full.anchor_ms, network)?,
                                None,
                            )
                            .await?;
                            landed += 1;
                            *counts.entry("advanced").or_default() += 1;
                        }
                        Advance::Rejected(report) => {
                            assert!(
                                !predicted,
                                "seed {seed} step {step}: spurious refusal {report:?}"
                            );
                            // A retained range that starts in the other leaf
                            // is refused before the pages (`leaf_changed`);
                            // a margin that walks back across the boundary
                            // is refused by the merged range's re-witness
                            // (`witness_changed`).
                            assert!(
                                matches!(
                                    report.outcome,
                                    WindowAcquisition::Partial
                                        | WindowAcquisition::LeafChanged
                                        | WindowAcquisition::WitnessChanged
                                        | WindowAcquisition::NoEvidence
                                ),
                                "seed {seed} step {step}: unexpected {report:?}"
                            );
                            *counts.entry(report.outcome.as_str()).or_default() += 1;
                        }
                    }
                    prior_capture = full;
                }
                eprintln!("seed {seed}: {counts:?}, {landed} landed");
                assert!(
                    counts.get("advanced").copied().unwrap_or(0) > 20,
                    "{counts:?}"
                );
                assert!(
                    counts.get("partial").copied().unwrap_or(0) > 0,
                    "{counts:?}"
                );
                Ok(())
            })
        })
        .await?;
    }
    Ok(())
}

/// Corrupted retained inputs are refused for a named reason and never
/// produce a window; an astronomically heavier target is partial.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupted_retained_inputs_are_refused_not_assembled() -> Result<()> {
    run(gate::site!(), |ledger| {
        Box::pin(async move {
            for seq in 1..=40 {
                insert_row(ledger, seq * 2, u128::try_from(1 + seq % 3)?, true).await?;
            }
            let prior = capture(ledger, 4).await?;
            assert!(crosses(&prior, 4));
            let full = capture(ledger, 4).await?;
            let outcome = |result: Advance| match result {
                Advance::Advanced { report, .. } => panic!("assembled: {report:?}"),
                Advance::Rejected(report) => report.outcome,
            };
            // A duplicated last row.
            let mut duplicated = retained(prior.clone(), 4);
            let last = duplicated.shares.last().unwrap().clone();
            duplicated.shares.push(last);
            assert_eq!(
                outcome(advance_at(ledger, duplicated, 4, &full).await?),
                WindowAcquisition::Invariant
            );
            // A fabricated row in a sequence gap.
            let mut fabricated = retained(prior.clone(), 4);
            let mut ghost = fabricated.shares[1].clone();
            ghost.share_seq = fabricated.shares[1].share_seq + 1;
            fabricated.shares.insert(2, ghost);
            assert_eq!(
                outcome(advance_at(ledger, fabricated, 4, &full).await?),
                WindowAcquisition::CountMismatch
            );
            // A missing interior row.
            let mut holed = retained(prior.clone(), 4);
            holed.shares.remove(holed.shares.len() / 2);
            let holed_outcome = outcome(advance_at(ledger, holed, 4, &full).await?);
            assert!(
                matches!(
                    holed_outcome,
                    WindowAcquisition::CountMismatch | WindowAcquisition::Partial
                ),
                "{holed_outcome:?}"
            );
            // Rows out of order.
            let mut shuffled = retained(prior.clone(), 4);
            let len = shuffled.shares.len();
            shuffled.shares.swap(len - 2, len - 3);
            assert_eq!(
                outcome(advance_at(ledger, shuffled, 4, &full).await?),
                WindowAcquisition::Invariant
            );
            // A million times heavier: history runs out.
            let heavy = capture(ledger, 4_000_000).await?;
            assert!(!crosses(&heavy, 4_000_000));
            assert_eq!(
                outcome(advance_at(ledger, retained(prior.clone(), 4), 4_000_000, &heavy).await?),
                WindowAcquisition::Partial
            );
            // The uncorrupted window still advances, and lands.
            let full = capture(ledger, 4).await?;
            match advance_at(ledger, retained(prior, 4), 4, &full).await? {
                Advance::Advanced { shares, .. } => {
                    let shares = shares.into_inner();
                    assert_eq!(shares, full.shares);
                    verify_durable_range(
                        &ledger.pool,
                        &audit_write(shares, full.anchor_ms, 4)?,
                        None,
                    )
                    .await?;
                }
                Advance::Rejected(report) => panic!("refused: {report:?}"),
            }
            Ok(())
        })
    })
    .await
}
