//! Share the compact repair fixture, including its disposable schema and locks.
use super::*;
use qbit_prism_server::ledger::{BlobPruneCursor, Candidate};
use tokio::time::Instant;

const ORDER_LOCK: i64 = 0x505249534d000002;

fn deadline_error(error: &anyhow::Error) -> bool {
    let text = format!("{error:#}");
    text.contains("cleanup deadline elapsed") || text.contains("statement timeout")
}

// Match the runtime's two independent phases; the blob deadline starts only
// after expiry commits under its original database statement budget.
#[derive(Debug, Default, PartialEq, Eq)]
struct JobPruneResult {
    jobs: u64,
    templates: u64,
    balances: u64,
}

async fn sweep_ledger(
    ledger: &Ledger,
    cursor: &mut BlobPruneCursor,
    blob_budget: Duration,
) -> Result<JobPruneResult> {
    let jobs = ledger.prune_expired_jobs().await?;
    let blobs = ledger
        .prune_unreferenced_blobs(cursor, Instant::now() + blob_budget)
        .await?;
    Ok(JobPruneResult {
        jobs,
        templates: blobs.templates,
        balances: blobs.balances,
    })
}

async fn sweep(db: &Database, cursor: &mut BlobPruneCursor) -> Result<JobPruneResult> {
    sweep_ledger(&db.ledger, cursor, Duration::from_secs(5)).await
}

async fn expire(db: &Database) -> Result<()> {
    sqlx::query("UPDATE qbit_prism_jobs SET expires_at=clock_timestamp()-interval '1 second'")
        .execute(&db.ledger.pool)
        .await?;
    Ok(())
}

#[tokio::test]
async fn gc_before_repair_restores_exact_original_blobs_and_identity() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let original = seed(db, true).await?;
            let repair = original.repair()?;
            expire(db).await?;
            let mut cursor = BlobPruneCursor::default();
            ensure!(
                sweep(db, &mut cursor).await?
                    == JobPruneResult {
                        jobs: 1,
                        templates: 1,
                        balances: 1
                    }
            );
            assert_empty(db).await?;
            ensure!(
                save(db, &original, original.expires, None).await?
                    == IssuedJobSave::PreparedMissing
            );
            ensure!(
                save(db, &original, original.expires, Some(&repair)).await? == IssuedJobSave::Saved
            );
            let restored = db.ledger.compact_prepared("prepared").await?.unwrap();
            ensure!(
                restored.record == original.record
                    && restored.original_expires_at_ms == original.expires
            );
            ensure!(restored.template == template());
            let mut balances = original.balances.clone();
            balances.sort_by(|a, b| a.order_key.cmp(&b.order_key));
            ensure!(restored.prior_balances == balances);
            let shares: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger")
                .fetch_one(&db.ledger.pool)
                .await?;
            ensure!(shares == 2, "blob GC touched share history");
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn repair_before_gc_retains_blobs_through_the_child_deadline() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let original = seed(db, true).await?;
            delete_dependencies(db, false).await?;
            let repair = original.repair()?;
            ensure!(
                save(db, &original, original.expires, Some(&repair)).await? == IssuedJobSave::Saved
            );
            let before = snapshot(db).await?;
            let mut cursor = BlobPruneCursor::default();
            for _ in 0..3 {
                ensure!(sweep(db, &mut cursor).await? == JobPruneResult::default());
            }
            ensure!(snapshot(db).await? == before);
            Ok(())
        })
    })
    .await
}

fn candidate(original: &Original) -> Candidate {
    let block = vec![0u8; 81];
    let mut hash = Sha256::digest(Sha256::digest(&block[..80])).to_vec();
    hash.reverse();
    Candidate {
        block_hash: hex::encode(hash),
        block_sha256: Candidate::block_digest_hex(&block),
        job_id: "child".into(),
        payout_revision: 0,
        window: original.record.window,
        bootstrap_share: None,
        found_block: qbit_prism::FoundBlock {
            block_height: 101,
            coinbase_value_sats: 500_000_000,
            network_difficulty: 100,
            anchor_job_issued_at_ms: original.record.window.anchor_ms,
        },
        payout_policy: original.record.payout_policy.clone(),
        ctv: None,
        audit_builder_version: original.record.audit_builder_version,
        signer_keys: original.record.signer_keys.clone(),
        leased: true,
        coinbase_suffix_hex: "00".repeat(12),
        deferred_share: None,
        block_bytes: block,
        as_issued_balances: original.balances.clone(),
    }
}

#[tokio::test]
async fn candidate_claim_expiry_retains_balances_until_terminal_outcome() -> Result<()> {
    for submitted in [false, true] {
        run(move |db| Box::pin(async move {
            let original = seed(db, true).await?;
            let candidate = candidate(&original);
            db.ledger.enqueue_candidate(candidate.clone()).await?;
            let claim = db.ledger.claim_candidate(60).await?.unwrap();
            expire(db).await?;
            sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_expires_at=clock_timestamp()-interval '1 second'")
                .execute(&db.ledger.pool).await?;
            let mut cursor = BlobPruneCursor::default();
            ensure!(sweep(db, &mut cursor).await? == JobPruneResult { jobs: 1, templates: 1, balances: 0 });
            ensure!(db.ledger.finish_candidate(&claim, submitted, None).await.is_err());
            // Reclaim and finish through the real terminal API. A submitted
            // fixture supplies the confirmed block row required by that API.
            let claim = db.ledger.claim_candidate(60).await?.unwrap();
            if submitted {
                sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state) VALUES($1,101,repeat('ab',32),repeat('cd',32),repeat('ef',32),'confirmed')")
                    .bind(&candidate.block_hash).execute(&db.ledger.pool).await?;
            }
            db.ledger.finish_candidate(&claim, submitted, None).await?;
            let mut removed = 0;
            for _ in 0..3 { let result = sweep(db, &mut cursor).await?; ensure!(result.jobs == 0); removed += result.balances; }
            ensure!(removed == 1, "terminal balance orphan was not collected with zero expired jobs");
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_block_candidate_outbox WHERE state=$1 AND window_prior_balances_sha256 IS NULL")
                .bind(if submitted { "submitted" } else { "abandoned" }).fetch_one(&db.ledger.pool).await?;
            ensure!(count == 1, "collector changed terminal history");
            Ok(())
        })).await?;
    }
    Ok(())
}

#[tokio::test]
async fn bounded_pages_pass_live_prefixes_and_wrap_with_zero_expired_jobs() -> Result<()> {
    run(|db| Box::pin(async move {
        sqlx::raw_sql("INSERT INTO qbit_prism_templates SELECT lpad(to_hex(i),64,'0'),'x'::bytea FROM generate_series(1,600) i;
            INSERT INTO qbit_prism_balance_snapshots SELECT lpad(to_hex(i),64,'0'),'[]'::bytea FROM generate_series(1,600) i;
            INSERT INTO qbit_prism_jobs(job_id,instance_id,parent_hash,payout_revision,payload,expires_at,template_sha256,window_anchor_ms,window_prior_balances_sha256)
            SELECT 'live-'||i,'test','parent',0,'{}',clock_timestamp()+interval '1 hour',lpad(to_hex(i),64,'0'),1,lpad(to_hex(i),64,'0') FROM generate_series(1,256) i;
            INSERT INTO qbit_prism_jobs(job_id,instance_id,parent_hash,payout_revision,payload,expires_at)
            SELECT 'expired-'||i,'test','parent',0,'{}',clock_timestamp()-interval '1 hour' FROM generate_series(1,4097) i;")
            .execute(&db.ledger.pool).await?;
        let mut cursor = BlobPruneCursor::default();
        ensure!(sweep(db, &mut cursor).await? == JobPruneResult { jobs: 4096, templates: 0, balances: 0 });
        ensure!(sweep(db, &mut cursor).await? == JobPruneResult { jobs: 1, templates: 256, balances: 256 });
        ensure!(sweep(db, &mut cursor).await? == JobPruneResult { jobs: 0, templates: 88, balances: 88 });
        // Insert behind the cursor; reaching the end must wrap without an
        // unbounded restart scan or a requirement that any job expired.
        sqlx::raw_sql("INSERT INTO qbit_prism_templates VALUES(repeat('0',64),'x'); INSERT INTO qbit_prism_balance_snapshots VALUES(repeat('0',64),'[]');")
            .execute(&db.ledger.pool).await?;
        ensure!(sweep(db, &mut cursor).await? == JobPruneResult::default());
        ensure!(sweep(db, &mut cursor).await? == JobPruneResult { jobs: 0, templates: 1, balances: 1 });
        let remaining: (i64,i64,i64) = sqlx::query_as("SELECT (SELECT count(*) FROM qbit_prism_jobs),(SELECT count(*) FROM qbit_prism_templates),(SELECT count(*) FROM qbit_prism_balance_snapshots)")
            .fetch_one(&db.ledger.pool).await?;
        ensure!(remaining == (256,256,256));
        Ok(())
    })).await
}

#[tokio::test]
async fn collector_waits_for_repair_and_candidate_writers_in_lock_order() -> Result<()> {
    run(|db| Box::pin(async move {
        let original = seed(db, true).await?;
        delete_dependencies(db, true).await?;
        gate_insert(db, true).await?;
        let mut gate = db.ledger.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)").bind(TEST_GATE).execute(&mut *gate).await?;
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut *gate).await?;
        let repair = original.repair()?;
        let expiry = original.expires;
        let ledger = db.ledger.clone();
        let mut writer = Running(tokio::spawn(async move {
            ledger.save_issued_job_compact("child", &child(expiry), 0, &"ab".repeat(32), expiry, repair.dependency("prepared"), Some(&repair)).await
        }));
        let writer_pid = blocked_query(db, pid, "INSERT INTO qbit_prism_jobs").await?;
        let ledger = db.ledger.clone();
        let mut gc = Running(tokio::spawn(async move {
            sweep_ledger(&ledger, &mut BlobPruneCursor::default(), Duration::from_secs(5)).await
        }));
        blocked_query(db, writer_pid, "SELECT pg_advisory_xact_lock").await?;
        gate.rollback().await?;
        ensure!((&mut writer.0).await?? == IssuedJobSave::Saved);
        ensure!((&mut gc.0).await?? == JobPruneResult::default());

        // Candidate writer holds ORDER alone. GC must hold SETTLEMENT while
        // waiting for ORDER, and see a reference committed during that wait.
        let mut tx = db.ledger.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)").bind(ORDER_LOCK).execute(&mut *tx).await?;
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut *tx).await?;
        expire(db).await?;
        let ledger = db.ledger.clone();
        let mut gc = Running(tokio::spawn(async move {
            sweep_ledger(&ledger, &mut BlobPruneCursor::default(), Duration::from_secs(5)).await
        }));
        blocked_query(db, pid, "SELECT pg_advisory_xact_lock").await?;
        let holds_settlement: bool = sqlx::query_scalar("SELECT NOT pg_try_advisory_xact_lock($1)").bind(SETTLEMENT_LOCK).fetch_one(&mut *tx).await?;
        ensure!(holds_settlement, "collector acquired ORDER before SETTLEMENT");
        sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,window_anchor_ms,window_prior_balances_sha256) VALUES('pending','{}',repeat('0',64),1,$1)")
            .bind(hex::encode(original.record.window.prior_balances_digest)).execute(&mut *tx).await?;
        tx.commit().await?;
        ensure!((&mut gc.0).await?? == JobPruneResult { jobs: 2, templates: 1, balances: 0 });
        Ok(())
    })).await
}

#[tokio::test]
async fn blob_failure_preserves_committed_expiry_and_rolls_back_blobs_without_cursor_progress(
) -> Result<()> {
    run(|db| Box::pin(async move {
        seed(db, true).await?;
        expire(db).await?;
        let mut after_expiry = snapshot(db).await?;
        after_expiry[0] = json!([]);
        // Expiry commits independently. Failure in the last blob DELETE must
        // roll back both blob deletes without resurrecting expired jobs.
        sqlx::raw_sql("CREATE FUNCTION reject_balance_gc() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected unknown failure'; END $$;
            CREATE TRIGGER reject_balance_gc BEFORE DELETE ON qbit_prism_balance_snapshots FOR EACH ROW EXECUTE FUNCTION reject_balance_gc();")
            .execute(&db.ledger.pool).await?;
        let mut cursor = BlobPruneCursor::default();
        let error = sweep(db, &mut cursor).await.unwrap_err();
        ensure!(format!("{error:#}").contains("injected unknown failure"));
        rollback_fence(db).await?;
        ensure!(snapshot(db).await? == after_expiry && cursor == BlobPruneCursor::default());
        sqlx::raw_sql(&format!("CREATE OR REPLACE FUNCTION reject_balance_gc() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock({TEST_GATE}); RETURN OLD; END $$;"))
            .execute(&db.ledger.pool).await?;
        let mut gate = db.ledger.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)").bind(TEST_GATE).execute(&mut *gate).await?;
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut *gate).await?;
        let ledger = db.ledger.clone();
        let mut gc = Running(tokio::spawn(async move {
            let mut cursor = BlobPruneCursor::default();
            let result = sweep_ledger(&ledger, &mut cursor, Duration::from_millis(700)).await;
            (result,cursor)
        }));
        blocked_query(db, pid, "DELETE FROM qbit_prism_balance_snapshots").await?;
        let (result,cursor) = (&mut gc.0).await?;
        ensure!(deadline_error(&result.unwrap_err()));
        ensure!(cursor == BlobPruneCursor::default());
        // Keep the blocker held: server-side cancellation must release the
        // collector's locks without help from the blocked query.
        timeout(Duration::from_secs(1), rollback_fence(db)).await??;
        gate.rollback().await?;
        ensure!(snapshot(db).await? == after_expiry);
        sqlx::query("DROP TRIGGER reject_balance_gc ON qbit_prism_balance_snapshots").execute(&db.ledger.pool).await?;
        ensure!(sweep(db, &mut BlobPruneCursor::default()).await? == JobPruneResult { jobs: 0, templates: 1, balances: 1 });
        Ok(())
    })).await
}

#[tokio::test]
async fn expiry_selection_rechecks_renewal_after_its_row_lock_wait() -> Result<()> {
    run(|db| Box::pin(async move {
        let original = seed(db, true).await?;
        expire(db).await?;
        let mut renewal = db.ledger.pool.begin().await?;
        sqlx::query("UPDATE qbit_prism_jobs SET expires_at=to_timestamp($1::double precision/1000) WHERE job_id='prepared'")
            .bind(original.expires).execute(&mut *renewal).await?;
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut *renewal).await?;
        let ledger = db.ledger.clone();
        let mut gc = Running(tokio::spawn(async move {
            sweep_ledger(&ledger, &mut BlobPruneCursor::default(), Duration::from_secs(5)).await
        }));
        // The key was selected from the expired committed version. Once this
        // update commits, the DELETE must recheck the newly live row.
        blocked_query(db, pid, "DELETE FROM qbit_prism_jobs").await?;
        renewal.commit().await?;
        ensure!((&mut gc.0).await?? == JobPruneResult::default());
        let retained = db.ledger.compact_prepared("prepared").await?.unwrap();
        ensure!(retained.record == original.record && retained.expires_at_ms == original.expires);
        Ok(())
    })).await
}

#[tokio::test]
async fn slow_expiry_keeps_advisory_locks_free_for_share_ack_and_candidate_enqueue() -> Result<()> {
    run(|db| Box::pin(async move {
        let original = seed(db, false).await?;
        expire(db).await?;
        // A row-lock wait deterministically models a slow expiry batch without
        // a large TOAST fixture or assumptions about the machine's I/O speed.
        let mut row = db.ledger.pool.begin().await?;
        sqlx::query("SELECT 1 FROM qbit_prism_jobs WHERE job_id='prepared' FOR UPDATE")
            .execute(&mut *row).await?;
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *row).await?;
        let ledger = db.ledger.clone();
        let mut gc = Running(tokio::spawn(async move {
            sweep_ledger(&ledger, &mut BlobPruneCursor::default(), Duration::from_millis(700)).await
        }));
        blocked_query(db, pid, "DELETE FROM qbit_prism_jobs").await?;
        // The future blob budget must not cancel expiry or be consumed by it.
        sleep(Duration::from_millis(800)).await;
        ensure!(!gc.0.is_finished(), "expiry inherited the blob deadline");
        let share = qbit_prism::AcceptedShare {
            share_seq: 0,
            share_id: "append-during-expiry".into(),
            miner_id: "miner".into(),
            order_key: "miner".into(),
            p2mr_program_hex: "11".repeat(32),
            share_difficulty: 1,
            network_difficulty: 100,
            template_height: 100,
            job_id: "child".into(),
            job_issued_at_ms: 1,
            accepted_at_ms: 0,
            ntime: 1_800_000_000,
            credit_policy: None,
        };
        let mut pending = candidate(&original);
        pending.bootstrap_share = Some(qbit_prism::AcceptedShare {
            share_id: "bootstrap-share".into(),
            job_id: "bootstrap-job".into(),
            ..share.clone()
        });
        let appended = timeout(Duration::from_secs(1), db.ledger.append(share, Some(pending)))
            .await.context("slow expiry blocked the share ACK and candidate enqueue")??;
        ensure!(appended.share.share_id == "append-during-expiry");
        // Neither advisory lock may be held while expiry holds/waits for row
        // locks. In particular it cannot acquire SETTLEMENT after a row lock.
        let mut probe = db.ledger.pool.begin().await?;
        for lock in [SETTLEMENT_LOCK, ORDER_LOCK] {
            let free: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
                .bind(lock).fetch_one(&mut *probe).await?;
            ensure!(free, "expiry held advisory lock {lock}");
        }
        probe.rollback().await?;
        // Keep the expiry row blocked through both checks, then let the
        // independently committed candidate retain its balance blob in GC.
        ensure!(!gc.0.is_finished());
        row.rollback().await?;
        ensure!((&mut gc.0).await?? == JobPruneResult { jobs: 1, templates: 1, balances: 0 });
        let counts: (i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM qbit_share_ledger),(SELECT count(*) FROM qbit_block_candidate_outbox WHERE window_prior_balances_sha256 IS NOT NULL)")
            .fetch_one(&db.ledger.pool).await?;
        ensure!(counts == (1, 1));
        Ok(())
    })).await
}

#[tokio::test]
async fn halted_cluster_stops_blob_cleanup_but_expiry_still_commits() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            seed(db, true).await?;
            expire(db).await?;
            let mut after_expiry = snapshot(db).await?;
            after_expiry[0] = json!([]);
            sqlx::query("UPDATE qbit_prism_cluster SET fatal_error='injected halted cluster'")
                .execute(&db.ledger.pool)
                .await?;
            let mut cursor = BlobPruneCursor::default();
            let error = sweep(db, &mut cursor).await.unwrap_err();
            ensure!(format!("{error:#}").contains("cluster halted: injected halted cluster"));
            ensure!(cursor == BlobPruneCursor::default() && snapshot(db).await? == after_expiry);
            // Test-only recovery lets the same cursor retry the unchanged blob page.
            sqlx::query("UPDATE qbit_prism_cluster SET fatal_error=NULL")
                .execute(&db.ledger.pool)
                .await?;
            ensure!(
                sweep(db, &mut cursor).await?
                    == JobPruneResult {
                        jobs: 0,
                        templates: 1,
                        balances: 1
                    }
            );
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn stricter_database_timeout_survives_failure_and_success() -> Result<()> {
    run(|db| Box::pin(async move {
        seed(db, false).await?;
        expire(db).await?;
        let mut after_expiry = snapshot(db).await?;
        after_expiry[0] = json!([]);
        sqlx::raw_sql("CREATE FUNCTION slow_blob_gc() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.3); RETURN OLD; END $$;
            CREATE TRIGGER slow_blob_gc BEFORE DELETE ON qbit_prism_templates FOR EACH ROW EXECUTE FUNCTION slow_blob_gc();")
            .execute(&db.ledger.pool).await?;
        // Keep the other four slots checked out so both cleanup attempts and
        // the post-transaction checks use this configured connection.
        let mut held = Vec::new();
        for _ in 0..4 { held.push(db.ledger.pool.acquire().await?); }
        sqlx::query("SET statement_timeout='50ms'").execute(&db.ledger.pool).await?;
        let mut cursor = BlobPruneCursor::default();
        let error = sweep(db, &mut cursor).await.unwrap_err();
        ensure!(format!("{error:#}").contains("statement timeout"));
        ensure!(cursor == BlobPruneCursor::default() && snapshot(db).await? == after_expiry);
        let setting: String = sqlx::query_scalar("SHOW statement_timeout").fetch_one(&db.ledger.pool).await?;
        ensure!(setting == "50ms", "cleanup changed the configured timeout: {setting}");
        sqlx::query("DROP TRIGGER slow_blob_gc ON qbit_prism_templates").execute(&db.ledger.pool).await?;
        ensure!(sweep(db, &mut cursor).await? == JobPruneResult { jobs: 0, templates: 1, balances: 1 });
        let setting: String = sqlx::query_scalar("SHOW statement_timeout").fetch_one(&db.ledger.pool).await?;
        ensure!(setting == "50ms");
        drop(held);
        Ok(())
    })).await
}

#[tokio::test]
async fn one_deadline_covers_pool_and_both_advisory_waits() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            seed(db, false).await?;
            expire(db).await?;
            let before = snapshot(db).await?;
            let mut held = Vec::new();
            for _ in 0..5 {
                held.push(db.ledger.pool.acquire().await?);
            }
            let mut cursor = BlobPruneCursor::default();
            let error = db
                .ledger
                .prune_unreferenced_blobs(&mut cursor, Instant::now() + Duration::from_millis(100))
                .await
                .unwrap_err();
            ensure!(format!("{error:#}").contains("cleanup deadline elapsed"));
            drop(held);
            let mut settlement = db.ledger.pool.begin().await?;
            let mut ordering = db.ledger.pool.begin().await?;
            sqlx::query("SELECT pg_advisory_xact_lock($1)")
                .bind(SETTLEMENT_LOCK)
                .execute(&mut *settlement)
                .await?;
            sqlx::query("SELECT pg_advisory_xact_lock($1)")
                .bind(ORDER_LOCK)
                .execute(&mut *ordering)
                .await?;
            let settlement_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut *settlement)
                .await?;
            let order_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut *ordering)
                .await?;
            let deadline = Instant::now() + Duration::from_millis(900);
            let ledger = db.ledger.clone();
            let mut gc = Running(tokio::spawn(async move {
                ledger
                    .prune_unreferenced_blobs(&mut BlobPruneCursor::default(), deadline)
                    .await
            }));
            blocked_query(db, settlement_pid, "SELECT pg_advisory_xact_lock").await?;
            tokio::time::sleep_until(deadline - Duration::from_millis(300)).await;
            settlement.rollback().await?;
            blocked_query(db, order_pid, "SELECT pg_advisory_xact_lock").await?;
            let result =
                tokio::time::timeout_at(deadline + Duration::from_millis(200), &mut gc.0).await??;
            ensure!(deadline_error(&result.unwrap_err()));
            timeout(Duration::from_secs(1), rollback_fence(db)).await??;
            ordering.rollback().await?;
            ensure!(snapshot(db).await? == before && cursor == BlobPruneCursor::default());
            Ok(())
        })
    })
    .await
}
