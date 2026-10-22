use super::*;
use futures_util::future::LocalBoxFuture;
use qbit_prism_test_gate as gate;

async fn run(
    site: gate::Site,
    body: impl for<'a> FnOnce(&'a Ledger) -> LocalBoxFuture<'a, Result<()>>,
) -> Result<()> {
    let Some(raw) = gate::database_url(site)? else {
        return Ok(());
    };
    let fixture =
        crate::ledger_test_database::FixtureDatabase::open(&raw, "snapshot_delta_").await?;
    let ledger = match Ledger::connect(&fixture.url, "snapshot-delta".into(), 4, true).await {
        Ok(ledger) => ledger,
        Err(error) => return Err(fixture.abandon(error).await),
    };
    let result = body(&ledger).await;
    ledger.pool.close().await;
    fixture.close(result).await
}

fn share(index: u64, difficulty: u128) -> AcceptedShare {
    AcceptedShare {
        share_seq: 0,
        share_id: format!("delta:{index}"),
        miner_id: "miner:é".into(),
        order_key: "miner".into(),
        p2mr_program_hex: "11".repeat(32),
        share_difficulty: difficulty,
        network_difficulty: 1,
        template_height: 1,
        job_id: "job".into(),
        job_issued_at_ms: 1,
        accepted_at_ms: 2,
        ntime: 1,
        credit_policy: None,
    }
}

async fn capture(ledger: &Ledger, network: u128) -> Result<SnapshotCapture> {
    Ok(ledger
        .snapshot_with_admission(network, ReadAdmission::default(), None)
        .await?
        .into_inner())
}

fn retained(capture: SnapshotCapture, network: u128) -> RetainedShares {
    let SnapshotCapture { snapshot, leaf, .. } = capture;
    RetainedShares {
        network,
        leaf,
        anchor_ms: snapshot.anchor_ms,
        cutoff: snapshot.share_seq,
        shares: snapshot.shares,
    }
}

/// The full reader establishes the fresh anchor/cutoff/revision/balances once.
/// The candidate reads at exactly that same witness, then compares all bytes.
async fn differential(
    ledger: &Ledger,
    prior: RetainedShares,
    network: u128,
    advanced: bool,
) -> Result<SnapshotCapture> {
    let (full, _) = differential_report(ledger, prior, network, advanced).await?;
    Ok(full)
}

/// [`differential`], also returning the delta path's own account of itself.
async fn differential_report(
    ledger: &Ledger,
    prior: RetainedShares,
    network: u128,
    advanced: bool,
) -> Result<(SnapshotCapture, AcquisitionReport)> {
    let full = capture(ledger, network).await?;
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
    let report = match result {
        Advance::Advanced {
            shares,
            leaf,
            report,
        } => {
            assert!(advanced, "unexpected delta acquisition: {report:?}");
            assert_eq!(report.outcome, WindowAcquisition::Advanced);
            assert_eq!(Some(&leaf), full.leaf.as_ref());
            assert!(shares.capacity() <= shares.len().saturating_mul(2).max(4));
            assert_eq!(report.window_rows, shares.len());
            let candidate = Snapshot {
                shares: shares.into_inner(),
                ..full.snapshot.clone()
            };
            assert_eq!(
                serde_json::to_vec(&candidate)?,
                serde_json::to_vec(&full.snapshot)?
            );
            assert_eq!(
                WindowRef::from_snapshot(&candidate)?,
                WindowRef::from_snapshot(&full)?
            );
            report
        }
        Advance::Rejected(report) => {
            assert!(!advanced, "unexpected full-scan fallback: {report:?}");
            assert_ne!(report.outcome, WindowAcquisition::Advanced);
            report
        }
    };
    tx.commit().await?;
    Ok((full, report))
}

/// Bulk rows for size-driven cases: one leaf, fixed difficulty, eligible at
/// any anchor, `share_id` derived from the sequence. Native appends stay the
/// path for the behaviour-driven cases; a case never mixes the two.
async fn bulk_rows(ledger: &Ledger, from: i64, to: i64, difficulty: u128) -> Result<()> {
    sqlx::query("INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,accepted,writer_id,writer_epoch)
        SELECT i,'bulk:'||i::text,'miner','miner',decode(repeat('11',32),'hex'),$3::text::numeric,1,1,'job',to_timestamp(1),1,to_timestamp(2),true,'fixture',0
        FROM generate_series($1::bigint,$2::bigint) i")
        .bind(from).bind(to).bind(difficulty.to_string()).execute(&ledger.pool).await?;
    Ok(())
}

/// Whether the full reader's window at this target crosses (reaches the
/// weight) rather than running out of history: the delta path advances
/// exactly the crossing windows it can prove.
fn crosses(capture: &SnapshotCapture, network: u128) -> bool {
    capture.shares.iter().fold(network * 8, |left, share| {
        left.saturating_sub(share.share_difficulty)
    }) == 0
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_crossing_revision_and_balance_match_full_at_same_anchor() -> Result<()> {
    run(gate::site!(), |ledger| Box::pin(async move {
        for index in 1..=8 { ledger.append(share(index, 3), None).await?; }
        let first = capture(ledger, 1).await?;
        assert_eq!(first.shares.len(), 3);
        ledger.append(share(9, 2), None).await?;
        let next = differential(ledger, retained(first, 1), 1, true).await?;
        assert_eq!(next.shares.iter().map(|s| s.share_seq).collect::<Vec<_>>(), [7, 8, 9]);
        sqlx::query("UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1 WHERE singleton")
            .execute(&ledger.pool).await?;
        let revision = differential(ledger, retained(next, 1), 1, true).await?;
        sqlx::query("INSERT INTO qbit_payout_carry_forward_current(miner_id,payout_order_key,p2mr_program,balance_sats,active_row_count) VALUES('prior','prior',decode(repeat('22',32),'hex'),12345,1)")
            .execute(&ledger.pool).await?;
        let balance = differential(ledger, retained(revision.clone(), 1), 1, true).await?;
        assert_eq!(balance.payout_revision, revision.payout_revision);
        assert_ne!(balance.prior_balances, revision.prior_balances);
        ledger.append(share(10, u128::MAX), None).await?;
        let heavy = differential(ledger, retained(balance, 1), 1, true).await?;
        assert_eq!(heavy.shares.len(), 1);
        assert!(ledger.snapshot(0).await.is_err());
        assert!(ledger.snapshot(u128::MAX).await.is_err());
        assert!(ledger.append(share(11, 0), None).await.is_err());
        Ok(())
    })).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn short_history_retarget_regressions_fall_back_but_rollback_gaps_advance() -> Result<()> {
    run(gate::site!(), |ledger| {
        Box::pin(async move {
            ledger.append(share(1, 1), None).await?;
            let short = capture(ledger, 1).await?;
            differential(ledger, retained(short, 1), 1, false).await?;
            for index in 2..=8 {
                ledger.append(share(index, 1), None).await?;
            }
            let full = capture(ledger, 1).await?;
            differential(ledger, retained(full.clone(), 1), 2, false).await?;
            let mut regressed = retained(full.clone(), 1);
            regressed.cutoff += 1;
            differential(ledger, regressed, 1, false).await?;
            let mut future = retained(full.clone(), 1);
            future.anchor_ms = i64::MAX;
            differential(ledger, future, 1, false).await?;
            let revision = ledger.payout_revision().await?;
            let refused = ledger
                .append_at_revision_gated(share(9, 1), None, revision, &|| false)
                .await;
            assert!(refused
                .unwrap_err()
                .downcast_ref::<CommitGateClosed>()
                .is_some());
            let appended = ledger.append(share(10, 1), None).await?;
            assert_eq!(appended.share.share_seq, full.share_seq + 2);
            let gapped = differential(ledger, retained(full, 1), 1, true).await?;
            // The membership proof tolerates a real sequence rollback gap,
            // including the following zero-delta tip refresh.
            differential(ledger, retained(gapped, 1), 1, true).await?;
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_crossing_interior_detach_and_restore_fall_back() -> Result<()> {
    run(gate::site!(), |ledger| Box::pin(async move {
        // Empty test database only: put the full 16-weight suffix across three
        // leaves, then remove the middle one while both endpoints stay online.
        sqlx::raw_sql("ALTER TABLE qbit_share_ledger DETACH PARTITION qbit_share_ledger_p0;
            SELECT qbit_prism_share_partition_create('qbit_share_ledger_p50',1,7);
            SELECT qbit_prism_share_partition_create('qbit_share_ledger_p51',7,13);
            SELECT qbit_prism_share_partition_create('qbit_share_ledger_p52',13,30);")
            .execute(&ledger.pool).await?;
        for index in 1..=20 { ledger.append(share(index, 1), None).await?; }
        let original = capture(ledger, 2).await?;
        assert_eq!(original.shares.first().unwrap().share_seq, 5);
        differential(ledger, retained(original.clone(), 2), 2, false).await?;
        sqlx::raw_sql("ALTER TABLE qbit_share_ledger DETACH PARTITION qbit_share_ledger_p51")
            .execute(&ledger.pool).await?;
        let missing = differential(ledger, retained(original.clone(), 2), 2, false).await?;
        assert_eq!(missing.shares.len(), 14);
        sqlx::raw_sql("ALTER TABLE qbit_share_ledger ATTACH PARTITION qbit_share_ledger_p51 FOR VALUES FROM (7) TO (13)")
            .execute(&ledger.pool).await?;
        differential(ledger, retained(missing, 2), 2, false).await?;
        differential(ledger, retained(original, 2), 2, false).await?;
        Ok(())
    })).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_fixture_retroactive_insert_and_future_eligibility_are_not_delta_safe() -> Result<()> {
    for future in [false, true] {
        run(gate::site!(), |ledger| Box::pin(async move {
            let future_ms = chrono::Utc::now().timestamp_millis() + 60_000;
            sqlx::query("INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,accepted,writer_id,writer_epoch)
                SELECT i,i::text,'miner','miner',decode(repeat('11',32),'hex'),1,1,1,'job',to_timestamp(1),1,
                CASE WHEN i=5 THEN to_timestamp($1::double precision/1000) ELSE to_timestamp(2) END,true,'fixture',0
                FROM generate_series(1,10) i WHERE i<>5 OR $2")
                .bind(future_ms).bind(future).execute(&ledger.pool).await?;
            let prior = capture(ledger, 1).await?;
            assert_eq!(prior.shares.len(), 8);
            let before = prior.leaf.clone().unwrap();
            if future {
                sqlx::query("UPDATE qbit_prism_cluster SET ledger_clock_ms=$1 WHERE singleton")
                    .bind(future_ms + 1).execute(&ledger.pool).await?;
            } else {
                // Legal under every deployed immutability trigger; neither
                // endpoint nor the leaf incarnation changes.
                sqlx::raw_sql("INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,accepted,writer_id,writer_epoch)
                    VALUES(5,'5','miner','miner',decode(repeat('11',32),'hex'),1,1,1,'job',to_timestamp(1),1,to_timestamp(2),true,'fixture',0)")
                    .execute(&ledger.pool).await?;
            }
            let fresh = differential(ledger, retained(prior.clone(), 1), 1, false).await?;
            assert_ne!(fresh.shares, prior.shares);
            assert!(fresh.shares.iter().any(|share| share.share_seq == 5));
            assert_eq!(fresh.leaf.as_ref(), Some(&before));
            differential(ledger, retained(fresh, 1), 1, true).await?;
            Ok(())
        })).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_leaf_detach_and_reattach_invalidate_acquisition_proof() -> Result<()> {
    run(gate::site!(), |ledger| Box::pin(async move {
        for index in 1..=8 { ledger.append(share(index, 1), None).await?; }
        let prior = capture(ledger, 1).await?;
        let bound: i64 = sqlx::query_scalar("SELECT upper_seq FROM qbit_prism_share_partitions WHERE partition_name='qbit_share_ledger_p0'")
            .fetch_one(&ledger.pool).await?;
        sqlx::raw_sql("ALTER TABLE qbit_share_ledger DETACH PARTITION qbit_share_ledger_p0")
            .execute(&ledger.pool).await?;
        differential(ledger, retained(prior.clone(), 1), 1, false).await?;
        sqlx::raw_sql(&format!("ALTER TABLE qbit_share_ledger ATTACH PARTITION qbit_share_ledger_p0 FOR VALUES FROM (MINVALUE) TO ({bound})"))
            .execute(&ledger.pool).await?;
        let fresh = differential(ledger, retained(prior.clone(), 1), 1, false).await?;
        assert_eq!(fresh.shares, prior.shares);
        assert_ne!(fresh.leaf, prior.leaf);
        let fresh = differential(ledger, retained(fresh, 1), 1, true).await?;
        // Archive-style reconstruction into a new physical leaf: identical
        // immutable rows are not enough to keep the old acquisition token.
        sqlx::raw_sql(&format!("ALTER TABLE qbit_share_ledger DETACH PARTITION qbit_share_ledger_p0;
            ALTER TABLE qbit_share_ledger_p0 RENAME TO delta_restore_source;
            CREATE TABLE qbit_share_ledger_p0 (LIKE delta_restore_source INCLUDING ALL);
            INSERT INTO qbit_share_ledger_p0 SELECT * FROM delta_restore_source;
            CREATE TRIGGER qbit_prism_immutable_share_history BEFORE UPDATE OR DELETE OR TRUNCATE
                ON qbit_share_ledger_p0 FOR EACH STATEMENT EXECUTE FUNCTION qbit_prism_preserve_audit_history();
            ALTER TABLE qbit_share_ledger ATTACH PARTITION qbit_share_ledger_p0 FOR VALUES FROM (MINVALUE) TO ({bound});"))
            .execute(&ledger.pool).await?;
        let restored = differential(ledger, retained(fresh.clone(), 1), 1, false).await?;
        assert_ne!(restored.leaf, fresh.leaf);
        assert_eq!(restored.shares, fresh.shares);
        differential(ledger, retained(restored, 1), 1, true).await?;
        Ok(())
    })).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deferred_confirmation_credit_is_an_idempotent_delta() -> Result<()> {
    run(gate::site!(), |ledger| Box::pin(async move {
        for index in 1..=8 { ledger.append(share(index, 1), None).await?; }
        let prior = capture(ledger, 1).await?;
        let hash = "77".repeat(32);
        sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate_sha256,state,completed_at) VALUES($1,repeat('00',32),'abandoned',clock_timestamp())")
            .bind(&hash).execute(&ledger.pool).await?;
        sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state) VALUES($1,100,repeat('00',32),repeat('11',32),repeat('22',32),'prepared')")
            .bind(&hash).execute(&ledger.pool).await?;
        let deferred = share(9, 1);
        sqlx::query("INSERT INTO qbit_prism_deferred_shares(block_hash,share,share_sha256) VALUES($1,$2,$3)")
            .bind(&hash).bind(serde_json::to_value(&deferred)?)
            .bind(hex::encode(Sha256::digest(serde_json::to_vec(&deferred)?)))
            .execute(&ledger.pool).await?;
        let observations = [BlockObservation { block_hash: hash, active: true }];
        ledger.reconcile_blocks_at_revision(&observations, 101, prior.payout_revision).await?;
        let credited = differential(ledger, retained(prior, 1), 1, true).await?;
        assert_eq!(credited.shares.last().unwrap().share_id, deferred.share_id);
        assert_eq!(credited.share_seq, 9);
        ledger.reconcile_blocks_at_revision(&observations, 101, credited.payout_revision).await?;
        let repeated = differential(ledger, retained(credited, 1), 1, true).await?;
        assert_eq!(repeated.share_seq, 9);
        Ok(())
    })).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_detach_pending_cannot_authorize_retained_rows() -> Result<()> {
    run(gate::site!(), |ledger| Box::pin(async move {
        for index in 1..=8 { ledger.append(share(index, 1), None).await?; }
        let prior = capture(ledger, 1).await?;
        // Keep the old leaf visible to one existing transaction, forcing
        // concurrent DETACH to pause after it marks pg_inherits pending.
        let mut pin = ledger.begin().await?;
        sqlx::query("SELECT share_seq FROM qbit_share_ledger LIMIT 1").fetch_one(&mut *pin).await?;
        let pool = ledger.pool.clone();
        let detach = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
            sqlx::raw_sql("ALTER TABLE qbit_share_ledger DETACH PARTITION qbit_share_ledger_p0 CONCURRENTLY")
                .execute(&pool).await
        }));
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let pending: bool = sqlx::query_scalar("SELECT COALESCE(bool_or(inhdetachpending),false) FROM pg_inherits WHERE inhrelid='qbit_share_ledger_p0'::regclass")
                    .fetch_one(&ledger.pool).await?;
                if pending { return Ok::<_, anyhow::Error>(()); }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }).await??;
        let mut tx = ledger.begin().await?;
        assert!(leaf_witness(&mut tx, 1, 8, prior.anchor_ms, Some(8)).await?.is_none());
        tx.commit().await?;
        pin.rollback().await?;
        tokio::time::timeout(std::time::Duration::from_secs(5), detach).await???;
        differential(ledger, retained(prior, 1), 1, false).await?;
        Ok(())
    })).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn healthy_pool_rotation_and_reconnect_preserve_history_evidence() -> Result<()> {
    run(gate::site!(), |ledger| {
        Box::pin(async move {
            for index in 1..=8 {
                ledger.append(share(index, 1), None).await?;
            }
            let prior = capture(ledger, 1).await?;
            let mut transactions = Vec::new();
            let mut backends = std::collections::BTreeSet::new();
            for _ in 0..4 {
                let mut tx = ledger.begin().await?;
                backends.insert(
                    sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
                        .fetch_one(&mut *tx)
                        .await?,
                );
                // No new privilege is needed for the timeline expression. Exercise
                // the entire witness as a standard non-superuser read-only role.
                sqlx::query("SET LOCAL ROLE pg_read_all_data")
                    .execute(&mut *tx)
                    .await?;
                assert!(
                    !sqlx::query_scalar::<_, bool>(
                        "SELECT rolsuper FROM pg_roles WHERE rolname=current_user"
                    )
                    .fetch_one(&mut *tx)
                    .await?
                );
                assert_eq!(
                    leaf_witness(&mut tx, 1, 8, prior.anchor_ms, Some(8)).await?,
                    prior.leaf
                );
                transactions.push(tx);
            }
            assert_eq!(backends.len(), 4);
            for tx in transactions {
                tx.rollback().await?;
            }
            let rotated = differential(ledger, retained(prior, 1), 1, true).await?;
            let mut connections = Vec::new();
            for _ in 0..4 {
                connections.push(ledger.pool.acquire().await?);
            }
            for connection in connections {
                connection.close().await?;
            }
            let fresh_backend: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&ledger.pool)
                .await?;
            assert!(!backends.contains(&fresh_backend));
            let reconnected = differential(ledger, retained(rotated.clone(), 1), 1, true).await?;
            assert_eq!(reconnected.leaf, rotated.leaf);
            Ok(())
        })
    })
    .await
}

/// Retargets in both directions advance from the retained rows: a lighter
/// target retires rows from the old end, a heavier one reads a bounded margin
/// below the retained first row, and appended shares extend above the
/// cutoff, all within one exact window the full reader agrees with.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retargets_up_and_down_advance_with_exact_windows() -> Result<()> {
    run(gate::site!(), |ledger| {
        Box::pin(async move {
            for index in 1..=40 {
                ledger
                    .append(share(index, u128::from(1 + index % 3)), None)
                    .await?;
            }
            let mut retained_network: u128 = 4;
            let mut prior = capture(ledger, retained_network).await?;
            assert!(crosses(&prior, retained_network));
            let plan: [(u128, u64); 7] = [(5, 0), (3, 0), (6, 2), (2, 1), (7, 0), (7, 5), (4, 0)];
            let mut next_index = 41;
            for (network, appended) in plan {
                for _ in 0..appended {
                    ledger
                        .append(share(next_index, u128::from(1 + next_index % 3)), None)
                        .await?;
                    next_index += 1;
                }
                let (next, report) =
                    differential_report(ledger, retained(prior, retained_network), network, true)
                        .await?;
                assert_eq!(report.delta_rows, usize::try_from(appended)?, "{report:?}");
                if network > retained_network && appended == 0 {
                    assert!(report.margin_rows > 0, "{report:?}");
                    assert_eq!(report.retired_rows, 0, "{report:?}");
                }
                if network < retained_network && appended == 0 {
                    assert!(report.retired_rows > 0, "{report:?}");
                    assert_eq!(report.margin_rows, 0, "{report:?}");
                }
                // Kept retained rows mean no delta row was dropped.
                if report.retired_rows < report.prior_rows {
                    assert_eq!(
                        report.window_rows,
                        report.prior_rows - report.retired_rows
                            + report.margin_rows
                            + report.delta_rows,
                        "{report:?}"
                    );
                }
                prior = next;
                retained_network = network;
            }
            Ok(())
        })
    })
    .await
}

/// Deltas and margins span several pages; the assembled window is byte for
/// byte the full reader's, and the report counts every page.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_page_delta_and_margin_match_full_reader() -> Result<()> {
    run(gate::site!(), |ledger| {
        Box::pin(async move {
            bulk_rows(ledger, 1, 20_000, 1).await?;
            // Weight 8000: the newest 8000 rows.
            let prior = capture(ledger, 1000).await?;
            assert_eq!(prior.shares.len(), 8000);
            assert_eq!(prior.shares[0].share_seq, 12_001);
            // Heavier by 8000 rows: a two-page margin below the retained first row.
            let (heavier, report) =
                differential_report(ledger, retained(prior.clone(), 1000), 2000, true).await?;
            assert_eq!(report.margin_rows, 8000);
            assert_eq!(report.pages, 2);
            assert_eq!(heavier.shares[0].share_seq, 4001);
            // A three-page delta above the cutoff at the lighter target: the
            // retained rows are mostly retired and the delta mostly kept.
            bulk_rows(ledger, 20_001, 29_000, 1).await?;
            let (extended, report) =
                differential_report(ledger, retained(heavier, 2000), 1000, true).await?;
            assert_eq!(report.delta_rows, 9000);
            assert_eq!(report.pages, 3);
            assert_eq!(report.margin_rows, 0);
            assert_eq!(report.retired_rows, 16_000);
            assert_eq!(extended.shares.len(), 8000);
            assert_eq!(extended.shares[0].share_seq, 21_001);
            // A delta that alone outweighs the target retires the whole
            // retained window without reading below it.
            bulk_rows(ledger, 29_001, 38_000, 1).await?;
            let (replaced, report) =
                differential_report(ledger, retained(extended, 1000), 1000, true).await?;
            assert_eq!(report.retired_rows, 8000);
            assert_eq!(replaced.shares[0].share_seq, 30_001);
            Ok(())
        })
    })
    .await
}

/// The size bounds refuse before reading payload: a delta spanning more than
/// `MAX_DELTA_SLOTS` sequence slots, and a heavier target whose margin would
/// need more than `MAX_MARGIN_PAGES` pages, both take the full scan.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_delta_and_margin_take_the_full_scan() -> Result<()> {
    run(gate::site!(), |ledger| {
        Box::pin(async move {
            for index in 1..=8 {
                ledger.append(share(index, 1), None).await?;
            }
            let prior = capture(ledger, 1).await?;
            let mut stale = retained(prior.clone(), 1);
            // The cutoff regression check runs first; move the retained
            // cutoff back so the fresh cutoff is far ahead of it.
            let far = MAX_DELTA_SLOTS + 2;
            bulk_rows(ledger, 9, i64::try_from(far + 8)?, 1).await?;
            stale.cutoff = 8;
            let (_, report) = differential_report(ledger, stale, 1, false).await?;
            assert_eq!(report.outcome, WindowAcquisition::DeltaTooLarge);
            // Now a window whose heavier target needs more margin pages than
            // the bound allows: weight 8 retained, then 8 + the bound + 1.
            let light = capture(ledger, 1).await?;
            assert_eq!(light.shares.len(), 8);
            let too_heavy = u128::try_from(MAX_MARGIN_PAGES * 4096 + 9)?.div_ceil(8);
            let (_, report) =
                differential_report(ledger, retained(light.clone(), 1), too_heavy, false).await?;
            assert_eq!(report.outcome, WindowAcquisition::MarginTooLarge);
            // One page fewer is within the bound and advances.
            let heavy = u128::try_from(MAX_MARGIN_PAGES * 4096)?.div_ceil(8);
            let (_, report) = differential_report(ledger, retained(light, 1), heavy, true).await?;
            assert_eq!(report.margin_rows, MAX_MARGIN_PAGES * 4096 - 8);
            Ok(())
        })
    })
    .await
}

/// Seeded random walks over target, appended shares and difficulties: every
/// crossing window the full reader produces is what the delta path produces,
/// and every window that runs out of history is refused as partial. Exact
/// crossings (the fold reaching zero on the crossing row precisely) are
/// forced by choosing difficulties from a small set.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn random_retargets_and_inserts_match_full_reader() -> Result<()> {
    use rand::{Rng, SeedableRng};
    for seed in [7u64, 1975, 402_000] {
        run(gate::site!(), |ledger| {
            Box::pin(async move {
                let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
                let mut next_index = 1u64;
                for _ in 0..12 {
                    ledger
                        .append(share(next_index, rng.gen_range(1..=4)), None)
                        .await?;
                    next_index += 1;
                }
                let mut network: u128 = 3;
                let mut prior = capture(ledger, network).await?;
                let mut advanced = 0;
                let mut refused = 0;
                for step in 0..40 {
                    let appended = if step % 4 == 3 {
                        0
                    } else {
                        rng.gen_range(0..=6)
                    };
                    for _ in 0..appended {
                        ledger
                            .append(share(next_index, rng.gen_range(1..=4)), None)
                            .await?;
                        next_index += 1;
                    }
                    let previous = network;
                    network = match rng.gen_range(0..5) {
                        0 => network.saturating_sub(1).max(1),
                        1 => network + 1,
                        2 => network * 2,
                        3 => (network / 2).max(1),
                        _ => network,
                    };
                    let expected = capture(ledger, network).await?;
                    let expect_advance = crosses(&expected, network);
                    let (next, report) = differential_report(
                        ledger,
                        retained(prior.clone(), previous),
                        network,
                        expect_advance,
                    )
                    .await?;
                    if expect_advance {
                        advanced += 1;
                    } else {
                        assert_eq!(report.outcome, WindowAcquisition::Partial, "{report:?}");
                        refused += 1;
                    }
                    assert_eq!(next.shares, expected.shares);
                    prior = next;
                }
                assert!(
                    advanced > 0 && refused > 0,
                    "seed {seed}: {advanced} advanced, {refused} refused"
                );
                Ok(())
            })
        })
        .await?;
    }
    Ok(())
}

/// A window the delta path assembled after a retarget lands: the landing
/// re-derivation accepts exactly that range and rejects a superset and an
/// under-covering subset of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_windows_pass_landing_rederivation_and_neighbours_fail() -> Result<()> {
    use super::super::super::audit::{
        persist_audit_snapshot, verify_durable_range, AuditSnapshotWrite,
    };
    run(gate::site!(), |ledger| {
        Box::pin(async move {
            for index in 1..=30 {
                ledger
                    .append(share(index, u128::from(1 + index % 4)), None)
                    .await?;
            }
            let prior = capture(ledger, 3).await?;
            for index in 31..=36 {
                ledger.append(share(index, 2), None).await?;
            }
            // Heavier target with appended shares: margin below, delta above.
            let (window, report) = differential_report(ledger, retained(prior, 3), 5, true).await?;
            assert!(
                report.margin_rows > 0 && report.delta_rows == 6,
                "{report:?}"
            );
            let write = |shares: Vec<AcceptedShare>| -> Result<AuditSnapshotWrite> {
                Ok(AuditSnapshotWrite {
                    digest: hex::encode(Sha256::digest(serde_json::to_vec(&shares)?)),
                    first_share_seq: i64::try_from(shares[0].share_seq)?,
                    last_share_seq: i64::try_from(shares[shares.len() - 1].share_seq)?,
                    anchor_ms: window.anchor_ms,
                    network_difficulty: 5,
                    share_count: i64::try_from(shares.len())?,
                    inline: None,
                    shares: std::sync::Arc::new(shares),
                })
            };
            let exact = write(window.shares.clone())?;
            verify_durable_range(&ledger.pool, &exact, None).await?;
            let mut tx = ledger.begin().await?;
            persist_audit_snapshot(&mut tx, &exact).await?;
            tx.rollback().await?;
            // A superset: one margin row too many leaks past the crossing row.
            let older = capture(ledger, 6).await?;
            assert!(older.shares.len() > window.shares.len());
            let superset =
                write(older.shares[older.shares.len() - window.shares.len() - 1..].to_vec())?;
            assert!(verify_durable_range(&ledger.pool, &superset, None)
                .await
                .unwrap_err()
                .to_string()
                .contains("extends past canonical oldest share"));
            // Under-coverage: the crossing row is missing, so the window is
            // partial while older canonical shares exist.
            let subset = write(window.shares[1..].to_vec())?;
            assert!(verify_durable_range(&ledger.pool, &subset, None)
                .await
                .unwrap_err()
                .to_string()
                .contains("omits oldest canonical shares"));
            Ok(())
        })
    })
    .await
}

mod adversarial;
mod physical_failover;
