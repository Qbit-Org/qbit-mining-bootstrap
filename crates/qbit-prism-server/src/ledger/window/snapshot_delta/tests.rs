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
    let SnapshotCapture { snapshot, leaf } = capture;
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
    let full = capture(ledger, network).await?;
    let completion = ReadAdmission::default();
    let mut tx = ledger.begin().await?;
    let result = advance(
        &mut tx,
        completion.own(prior),
        network,
        network * 8,
        full.anchor_ms,
        i64::try_from(full.share_seq)?,
        &completion,
    )
    .await?;
    assert_eq!(result.is_some(), advanced, "unexpected acquisition path");
    if let Some((shares, _)) = result {
        assert!(shares.capacity() <= shares.len().saturating_mul(2).max(4));
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
    }
    tx.commit().await?;
    Ok(full)
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
