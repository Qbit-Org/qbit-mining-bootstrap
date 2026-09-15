//! Issue #273: public Coordinator activation, with a small real ledger window,
//! PostgreSQL 16 durability enabled, and coordinator-backed Stratum sockets.
//! The inline base intentionally fails compact-representation assertions.
//! Run with PRISM_TEST_REQUIRE_INTEGRATION=1 and PRISM_TEST_DATABASE_URL pointing
//! at a disposable PG16 instance; all fixture tables live in unique schemas.
use anyhow::{ensure, Context, Result};
use qbit_prism_server::{ledger::BlobPruneCursor, stratum::MiningBackend};
use serde_json::json;
use std::time::Duration;
use tokio::time::{sleep, timeout, Instant};

#[path = "support/compact_runtime_e2e/mod.rs"]
mod support;
use support::{
    assertions::{compact_storage, same_job},
    execution::{Fault, FaultPhase},
    run,
    socket::{ordinary_submit, Client, Listener},
    worker, MASK, SETTLEMENT_LOCK,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_issue_resume_preserves_original_work_and_compact_storage() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let worker = f.a.authorize("alice.rig").await?;
            let original = f.issue(&worker, Duration::from_secs(30)).await?;
            let before = f.payload(&original.wire.job_id).await?;
            let own_b =
                f.b.prepared
                    .read()
                    .await
                    .clone()
                    .context("B has no own publication")?;
            ensure!(
                own_b.storage_key != original.context.prepared.storage_key,
                "B did not publish independently"
            );
            let resume_mark = f.proxy.mark();
            let resumed =
                f.b.resume_job(&worker, &original.wire.job_id)
                    .await?
                    .context("same-tip B could not resume A work")?;
            same_job(&original, &resumed)?;
            let returned_rows = f.returned_share_rows(resume_mark)?;
            // A no-change refresh preserves authority to the original identity.
            f.b.refresh_once().await?;
            let again =
                f.b.resume_job(&worker, &original.wire.job_id)
                    .await?
                    .context("unchanged refresh invalidated original work")?;
            same_job(&original, &again)?;
            ensure!(
                f.payload(&original.wire.job_id).await? == before,
                "resume renewed or rewrote original issued inputs"
            );
            ensure!(
                f.b.resume_job(&support::worker("mallory"), &original.wire.job_id)
                    .await?
                    .is_none(),
                "another worker resumed A work"
            );
            compact_storage(f, &original).await?;
            ensure!(
                returned_rows == support::SHARES,
                "resume returned {returned_rows} actual share rows, expected {}",
                support::SHARES
            );
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_old_refresh_cannot_replace_new_tip_publication_or_resume_retired_work(
) -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let worker = f.a.authorize("alice.rig").await?;
            let original = f.issue(&worker, Duration::from_secs(30)).await?;
            let original_key = original.context.prepared.storage_key.clone();
            let mut pause = f.node.pause_next("getblocktemplate")?;
            let a = f.a.clone();
            let pending = tokio::spawn(async move { a.refresh_once().await });
            let entered = timeout(Duration::from_secs(5), pause.entered()).await;
            if !matches!(entered, Ok(Ok(()))) {
                pending.abort();
                let _ = pending.await;
                anyhow::bail!("old template response did not reach the RPC barrier");
            }
            // The handler captured A's OLD template before blocking. B gets
            // the new real node view and publishes it using its own refresh.
            let next_tip = "ef".repeat(32);
            f.node.set_tip(&next_tip, &"ab".repeat(32), 101, "02");
            let advanced = f.b.refresh_once().await;
            pause.release();
            let old_result = timeout(Duration::from_secs(5), pending).await??;
            advanced?;
            ensure!(
                old_result.is_err(),
                "a delayed stale template refresh succeeded"
            );
            ensure!(
                f.a.prepared
                    .read()
                    .await
                    .as_ref()
                    .context("A lost original publication")?
                    .storage_key
                    == original_key,
                "failed old refresh replaced A publication"
            );
            ensure!(
                f.b.prepared
                    .read()
                    .await
                    .as_ref()
                    .context("B has no new publication")?
                    .template["previousblockhash"]
                    == next_tip,
                "B did not publish the advanced tip"
            );
            // Once A publishes its own current-tip replacement, old work is
            // retired. This does not grant B a lease for A's old publication.
            f.a.refresh_once().await?;
            ensure!(
                f.a.resume_job(&worker, &original.wire.job_id)
                    .await?
                    .is_none(),
                "A resumed genuinely superseded work"
            );
            ensure!(
                f.b.resume_job(&worker, &original.wire.job_id)
                    .await?
                    .is_none(),
                "B resumed genuinely superseded work"
            );
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_prepared_dependency_repairs_original_record_without_renewing_identity(
) -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let worker = f.a.authorize("alice.rig").await?;
            let original = f.issue(&worker, Duration::from_secs(30)).await?;
            compact_storage(f, &original).await?;
            let key = &original.context.prepared.storage_key;
            let before = f.payload(key).await?;
            let next =
                f.a.build_job(&worker, "55667788", support::DIFFICULTY, 0.0)
                    .await?;
            ensure!(
                next.context.prepared.storage_key == *key,
                "repair fixture lost original identity"
            );
            ensure!(
                sqlx::query("DELETE FROM qbit_prism_jobs WHERE job_id=$1")
                    .bind(key)
                    .execute(f.pool())
                    .await?
                    .rows_affected()
                    == 1,
                "prepared dependency was not removed"
            );
            // Removing a dependency is a miss. The original in-memory owner,
            // not the receiving frontend, owns the inputs to repair it.
            ensure!(
                f.b.resume_job(&worker, &original.wire.job_id)
                    .await?
                    .is_none(),
                "missing dependency was not a miss"
            );
            f.a.persist_issued_job(&worker, &next, MASK, Duration::from_secs(180))
                .await?;
            ensure!(
                f.payload(key).await? == before,
                "repair changed original record or reservation expiry"
            );
            let resumed =
                f.b.resume_job(&worker, &original.wire.job_id)
                    .await?
                    .context("original issued job did not resume after dependency repair")?;
            same_job(&original, &resumed)?;
            let stored =
                f.a.ledger
                    .compact_prepared(key)
                    .await?
                    .context("repaired dependency missing")?;
            let child = f.payload(&next.wire.job_id).await?;
            ensure!(
                stored.original_expires_at_ms
                    == before["original_expires_at_ms"]
                        .as_i64()
                        .context("original expiry missing")?,
                "repair renewed reservation identity"
            );
            ensure!(
                stored.expires_at_ms
                    >= child["expires_at_ms"]
                        .as_i64()
                        .context("issued expiry missing")?,
                "repaired dependency does not retain the new child"
            );
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bootstrap_resume_keeps_each_workers_original_payout() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(false).await?;
            let alice = worker("alice");
            let bob = worker("bob");
            let first = f.issue(&alice, Duration::from_secs(30)).await?;
            let second = f.issue(&bob, Duration::from_secs(30)).await?;
            ensure!(
                first.context.prepared.storage_key == second.context.prepared.storage_key,
                "workers did not share the empty prepared dependency"
            );
            ensure!(
                first.context.bootstrap_share.is_some() && second.context.bootstrap_share.is_some(),
                "bootstrap synthetic share missing"
            );
            ensure!(
                serde_json::to_vec(&*first.context.bundle)?
                    != serde_json::to_vec(&*second.context.bundle)?,
                "bootstrap accidentally shared a worker payout"
            );
            for (worker, original) in [(&alice, &first), (&bob, &second)] {
                let resumed =
                    f.b.resume_job(worker, &original.wire.job_id)
                        .await?
                        .context("bootstrap resume missed")?;
                same_job(original, &resumed)?;
            }
            compact_storage(f, &first).await
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_socket_reconnect_resumes_original_entropy_mask_and_submits_once() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let mut a = Listener::start(&f.a).await?;
            let mut b = Listener::start(&f.b).await?;
            let mut original_socket = Client::connect(a.address).await?;
            original_socket.configure().await?;
            original_socket.login("alice.rig").await?;
            let worker = f.a.authorize("alice.rig").await?;
            let id = original_socket.notify["params"][0]
                .as_str()
                .context("notify omitted job ID")?
                .to_owned();
            let original =
                f.a.resume_job(&worker, &id)
                    .await?
                    .context("socket issued work was not durable")?;
            ensure!(
                original.wire.extranonce1 == original_socket.extranonce1
                    && original.wire.version_mask == MASK
                    && original.wire.share_difficulty == original_socket.difficulty,
                "socket advertised different issued inputs"
            );
            let before = f.payload(&id).await?;
            let (submit, proof) = ordinary_submit(&original, &worker, 42)?;
            let old_extra = original_socket.extranonce1.clone();
            drop(original_socket);
            let mut reconnected = Client::connect(b.address).await?;
            reconnected.login("alice.rig").await?;
            ensure!(
                reconnected.extranonce1 != old_extra,
                "reconnect did not allocate fresh session entropy"
            );
            // B negotiates no rolling mask. The submitted job retains A's mask.
            reconnected.send(submit.clone()).await?;
            let reply = reconnected.response(42).await?;
            ensure!(
                reply["result"] == true,
                "real resumed share refused: {reply}"
            );
            reconnected.send(submit).await?;
            let duplicate = reconnected.response(42).await?;
            ensure!(
                duplicate["error"][0] == 22,
                "duplicate share was not rejected: {duplicate}"
            );
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM qbit_share_ledger WHERE job_id=$1 AND accepted",
            )
            .bind(&id)
            .fetch_one(f.pool())
            .await?;
            ensure!(
                count == 1,
                "socket submit did not append exactly one accepted share"
            );
            let hash_count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM qbit_prism_share_hashes WHERE header_hash=$1",
            )
            .bind(&proof.block_hash_hex)
            .fetch_one(f.pool())
            .await?;
            ensure!(hash_count == 1, "accepted share header identity changed");
            ensure!(
                f.payload(&id).await? == before,
                "socket reconnect rewrote original expiry or entropy"
            );
            drop(reconnected);
            a.close().await?;
            b.close().await?;
            compact_storage(f, &original).await
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resume_expiry_does_not_slide_and_expired_work_is_a_miss() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let worker = worker("alice");
            let original = f.issue(&worker, Duration::from_secs(2)).await?;
            let before = f.payload(&original.wire.job_id).await?;
            let first =
                f.b.resume_job(&worker, &original.wire.job_id)
                    .await?
                    .context("live resume missed")?;
            sleep(Duration::from_millis(100)).await;
            let second =
                f.b.resume_job(&worker, &original.wire.job_id)
                    .await?
                    .context("second live resume missed")?;
            let first_deadline = first
                .wire
                .resume_expires_at
                .context("first expiry missing")?;
            let second_deadline = second
                .wire
                .resume_expires_at
                .context("second expiry missing")?;
            let expiry = before["expires_at_ms"]
                .as_i64()
                .context("original SQL expiry missing")?;
            sleep(Duration::from_millis(
                (expiry - f.now_ms().await?).max(0) as u64 + 20,
            ))
            .await;
            ensure!(
                first_deadline <= std::time::Instant::now()
                    && second_deadline <= std::time::Instant::now(),
                "resumed wire deadline outlived the original database expiry"
            );
            ensure!(
                f.b.resume_job(&worker, &original.wire.job_id)
                    .await?
                    .is_none(),
                "expired original work resumed"
            );
            ensure!(
                f.payload(&original.wire.job_id).await? == before,
                "expiry changed during resume"
            );
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_issued_inputs_are_errors_and_missing_work_is_a_miss() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let worker = worker("alice");
            let original = f.issue(&worker, Duration::from_secs(30)).await?;
            let before = f.payload(&original.wire.job_id).await?;
            ensure!(
                f.b.resume_job(&worker, "missing-job").await?.is_none(),
                "missing work is not a miss"
            );
            for (key, value) in [
                ("share_target_hex", json!("0")),
                ("share_target_hex", json!("1".repeat(65))),
                ("share_target_hex", json!("zz")),
                ("share_difficulty", json!(0)),
                ("share_difficulty", json!(-1)),
                ("share_difficulty", json!("NaN")),
                ("extranonce1", json!("abcd")),
                ("extranonce1", json!("zzzzzzzz")),
            ] {
                let mut corrupt = before.clone();
                corrupt[key] = value.clone();
                f.replace_payload(&original.wire.job_id, &corrupt).await?;
                let error = match f.b.resume_job(&worker, &original.wire.job_id).await {
                    Err(error) => error,
                    Ok(_) => anyhow::bail!("malformed {key}={value} became work or a cache miss"),
                };
                ensure!(
                    error.reason_id.as_deref() == Some("backend-rpc-unavailable"),
                    "storage corruption lost its truthful backend error: {error:?}"
                );
            }
            f.replace_payload(&original.wire.job_id, &before).await?;
            let resumed =
                f.b.resume_job(&worker, &original.wire.job_id)
                    .await?
                    .context("restored original inputs did not resume")?;
            same_job(&original, &resumed)
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compact_reference_corruption_is_an_error_and_blobs_survive_collection() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let worker = worker("alice");
            let original = f.issue(&worker, Duration::from_secs(30)).await?;
            compact_storage(f, &original).await?;
            let prepared_key = &original.context.prepared.storage_key;
            let before = f.payload(prepared_key).await?;
            let mut incompatible = before.clone();
            incompatible["audit_builder_version"] = json!(
                before["audit_builder_version"].as_u64().context("builder version missing")? + 1
            );
            f.replace_payload(prepared_key, &incompatible).await?;
            ensure!(f.b.resume_job(&worker, &original.wire.job_id).await?.is_none(), "incompatible builder silently rebuilt original work");
            f.replace_payload(prepared_key, &before).await?;
            let collected =
                f.a.ledger
                    .prune_unreferenced_blobs(
                        &mut BlobPruneCursor::default(),
                        Instant::now() + Duration::from_secs(5),
                    )
                    .await?;
            ensure!(
                collected.templates == 0 && collected.balances == 0,
                "collection removed referenced blobs"
            );
            for corrupt in [
                {
                    let mut v = before.clone();
                    v["template_sha256"] = json!("AB".repeat(32));
                    v
                },
                {
                    let mut v = before.clone();
                    v["window"]["shares"]["share_count"] = json!(0);
                    v
                },
                {
                    let mut v = before.clone();
                    v.as_object_mut().context("prepared object")?.remove("ctv");
                    v
                },
                {
                    let mut v = before.clone();
                    v["unknown_input"] = json!(true);
                    v
                },
            ] {
                f.replace_payload(prepared_key, &corrupt).await?;
                let error = match f.b.resume_job(&worker, &original.wire.job_id).await {
                    Err(error) => error,
                    Ok(_) => {
                        anyhow::bail!("noncanonical compact storage became work or a cache miss")
                    }
                };
                ensure!(
                    error.reason_id.as_deref() == Some("backend-rpc-unavailable"),
                    "compact corruption lost backend error"
                );
            }
            f.replace_payload(prepared_key, &before).await?;
            let template_key = before["template_sha256"].as_str().context("template key")?;
            let bytes: Vec<u8> = sqlx::query_scalar(
                "SELECT template_bytes FROM qbit_prism_templates WHERE template_sha256=$1",
            )
            .bind(template_key)
            .fetch_one(f.pool())
            .await?;
            f.replace_template_blob(template_key, b"{}").await?;
            ensure!(
                f.b.resume_job(&worker, &original.wire.job_id)
                    .await
                    .is_err(),
                "corrupt retained template blob was accepted"
            );
            f.replace_template_blob(template_key, &bytes).await?;
            let balance_key = before["window"]["prior_balances_digest"].as_str().context("balance key")?;
            let balances: Vec<u8> = sqlx::query_scalar("SELECT balances FROM qbit_prism_balance_snapshots WHERE prior_balances_digest=$1").bind(balance_key).fetch_one(f.pool()).await?;
            for contents in [b"{}".as_slice(), balances.as_slice()] {
                let mut tx = f.pool().begin().await?;
                sqlx::query("DELETE FROM qbit_prism_balance_snapshots WHERE prior_balances_digest=$1").bind(balance_key).execute(&mut *tx).await?;
                sqlx::query("INSERT INTO qbit_prism_balance_snapshots(prior_balances_digest,balances) VALUES($1,$2)").bind(balance_key).bind(contents).execute(&mut *tx).await?;
                tx.commit().await?;
                if contents == b"{}" {
                    let error = match f.b.resume_job(&worker, &original.wire.job_id).await { Err(error) => error, Ok(_) => anyhow::bail!("corrupt balance blob became work or a miss") };
                    ensure!(error.reason_id.as_deref() == Some("backend-rpc-unavailable"), "balance corruption lost backend error");
                }
            }
            let resumed =
                f.b.resume_job(&worker, &original.wire.job_id)
                    .await?
                    .context("restored compact data did not resume")?;
            same_job(&original, &resumed)
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resumed_compact_work_authenticates_retained_share_rows() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let worker = f.a.authorize("alice.rig").await?;
            let original = f.issue(&worker, Duration::from_secs(30)).await?;
            compact_storage(f, &original).await?;
            let resumed =
                f.b.resume_job(&worker, &original.wire.job_id)
                    .await?
                    .context("initial compact resume missed")?;
            same_job(&original, &resumed)?;
            // Simulate storage corruption in this disposable schema only. The
            // endpoint rows survive; a middle row's authenticated bytes change.
            // Re-enable the write guard in the same transaction before commit.
            let mut tx = f.pool().begin().await?;
            sqlx::raw_sql(
                "ALTER TABLE qbit_share_ledger DISABLE TRIGGER qbit_prism_immutable_share_history",
            )
            .execute(&mut *tx)
            .await?;
            ensure!(
                sqlx::query(
                    "UPDATE qbit_share_ledger SET miner_id=miner_id||'-corrupt' WHERE share_seq=$1"
                )
                .bind((support::SHARES / 2) as i64)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                    == 1,
                "middle-row corruption fixture failed"
            );
            sqlx::raw_sql(
                "ALTER TABLE qbit_share_ledger ENABLE TRIGGER qbit_prism_immutable_share_history",
            )
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            let error = match f.b.resume_job(&worker, &original.wire.job_id).await {
                Err(error) => error,
                Ok(_) => anyhow::bail!("resume trusted cached data after retained row corruption"),
            };
            ensure!(
                error.reason_id.as_deref() == Some("backend-rpc-unavailable"),
                "retained share corruption lost backend error"
            );
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_issued_save_releases_sql_resources_without_publishing() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let worker = worker("alice");
            let job =
                f.a.build_job(&worker, "1a2b3c4d", support::DIFFICULTY, 0.0)
                    .await?;
            let mut lock = f.pool().begin().await?;
            sqlx::query("SELECT pg_advisory_xact_lock($1)")
                .bind(SETTLEMENT_LOCK)
                .execute(&mut *lock)
                .await?;
            let a = f.a.clone();
            let pending_job = job.clone();
            let pending_worker = worker.clone();
            let task = tokio::spawn(async move {
                a.persist_issued_job(&pending_worker, &pending_job, MASK, Duration::from_secs(2))
                    .await
            });
            let observed = f.wait_for_settlement_waiter().await;
            task.abort();
            let cancelled = task.await;
            lock.rollback().await?;
            observed?;
            ensure!(
                cancelled.is_err_and(|error| error.is_cancelled()),
                "save was not cancelled while waiting"
            );
            // A subsequent real save must acquire the same lock and a pool slot.
            let next = timeout(
                Duration::from_secs(5),
                f.issue(&worker, Duration::from_secs(30)),
            )
            .await??;
            ensure!(
                f.a.ledger.job(&job.wire.job_id).await?.is_none(),
                "cancelled wait later published its issued job"
            );
            let resumed =
                f.b.resume_job(&worker, &next.wire.job_id)
                    .await?
                    .context("runtime did not recover after cancellation")?;
            same_job(&next, &resumed)
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_issued_commit_is_observed_and_reconciled_without_reissuing() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let worker = worker("alice");
            let mut job =
                f.a.build_job(&worker, "1a2b3c4d", support::DIFFICULTY, 0.0)
                    .await?;
            job.wire.version_mask = MASK;
            let mark = f.proxy.mark();
            f.proxy.plan(Fault {
                table: "qbit_prism_jobs".into(),
                op: "INSERT".into(),
                phase: FaultPhase::AfterCommit,
            });
            let result =
                f.a.persist_issued_job(&worker, &job, MASK, Duration::from_secs(30))
                    .await;
            ensure!(
                f.proxy.fired().is_some(),
                "commit response fault did not fire"
            );
            let executions = f.proxy.executions_since(mark)?;
            ensure!(
                executions
                    .iter()
                    .any(|execution| execution.is_commit() && !execution.delivered()),
                "unknown commit was not observed on the PostgreSQL wire"
            );
            if let Err(error) = result {
                ensure!(
                    error.reason_id.as_deref() == Some("backend-rpc-unavailable"),
                    "commit transport error was misclassified"
                );
            }
            let before = f.payload(&job.wire.job_id).await?;
            let resumed =
                f.b.resume_job(&worker, &job.wire.job_id)
                    .await?
                    .context("durable unknown commit could not reconcile through resume")?;
            same_job(&job, &resumed)?;
            let writes: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM runtime_job_writes WHERE job_id=$1 AND operation='INSERT'",
            )
            .bind(&job.wire.job_id)
            .fetch_one(f.pool())
            .await?;
            ensure!(
                writes == 1,
                "unknown commit produced duplicate issued writes"
            );
            ensure!(
                f.payload(&job.wire.job_id).await? == before,
                "reconciliation changed the original issued deadline"
            );
            Ok(())
        })
    })
    .await
}
