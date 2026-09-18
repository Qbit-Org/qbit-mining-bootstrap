//! Bounded bulk compact writes against an isolated disposable PostgreSQL schema.
use anyhow::{ensure, Context, Result};
use futures_util::future::LocalBoxFuture;
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::PayoutPolicy;
use qbit_prism_server::ledger::{
    CompactBatchAttempt, CompactDependency, CompactIssuedJob, CompactPrepared, CompactRepair,
    IssuedJobSave, Ledger, PreparedTemplate, SignerKeys, WindowRef,
};
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use sqlx::{Acquire, PgPool};
use std::{sync::Arc, time::Duration};
use tokio::time::{sleep, timeout, Instant};

const INSERT_GATE: i64 = 0x27500003;

#[tokio::test]
async fn original_deadline_includes_pool_wait_and_preserves_stricter_statement_limits() -> Result<()>
{
    run(|db| Box::pin(async move {
        let original = seed(db).await?;
        let entries = jobs(original.expiry + 60_000);
        let before = snapshot(db).await?;
        let mut held = Vec::new();
        for _ in 0..5 { held.push(db.ledger.pool.acquire().await?); }
        let attempt = CompactBatchAttempt::new(Instant::now() + Duration::from_millis(20));
        let result = db.ledger.save_issued_jobs_compact(&entries, 0, &original.record.parent_hash, original.dependency(), &attempt).await;
        ensure!(result.unwrap_err().to_string().contains("deadline elapsed"));
        ensure!(!attempt.commit_started());
        drop(held);
        ensure!(snapshot(db).await? == before);
        for limit in [40, 0] {
            let mut connections = Vec::new();
            for _ in 0..5 {
                let mut connection = db.ledger.pool.acquire().await?;
                sqlx::query("SELECT set_config('statement_timeout',$1,false),set_config('lock_timeout','0',false)")
                    .bind(limit.to_string()).execute(&mut *connection).await?;
                connections.push(connection);
            }
            let mut hold = connections.pop().unwrap();
            let mut hold = hold.begin().await?;
            sqlx::query("SELECT singleton FROM qbit_prism_cluster WHERE singleton FOR UPDATE").execute(&mut *hold).await?;
            drop(connections);
            let attempt = Arc::new(CompactBatchAttempt::new(Instant::now() + Duration::from_millis(150)));
            let started = Instant::now();
            let result = db.ledger.save_issued_jobs_compact(&entries, 0, &original.record.parent_hash, original.dependency(), &attempt).await;
            ensure!(result.is_err() && !attempt.commit_started());
            if limit != 0 { ensure!(format!("{:#}", result.unwrap_err()).contains("statement timeout")); }
            // Keep the authority row held: draining must finish under the SQL timeout.
            timeout(Duration::from_secs(2), attempt.wait_for_cleanup()).await?;
            ensure!(started.elapsed() < Duration::from_secs(2));
            hold.rollback().await?;
            rollback_fence(db).await?;
            ensure!(snapshot(db).await? == before);
            save(db, &original, &entries).await?;
            let configured: String = sqlx::query_scalar("SHOW statement_timeout").fetch_one(&db.ledger.pool).await?;
            ensure!(configured == if limit == 0 { "0" } else { "40ms" }, "{configured}");
            sqlx::query("DELETE FROM qbit_prism_jobs WHERE job_id LIKE 'child-%'").execute(&db.ledger.pool).await?;
            sqlx::query("UPDATE qbit_prism_jobs SET expires_at=to_timestamp($1::double precision/1000) WHERE job_id='prepared'")
                .bind(original.expiry).execute(&db.ledger.pool).await?;
        }
        Ok(())
    })).await
}
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct Database {
    ledger: Ledger,
    admin: PgPool,
    schema: String,
    url: String,
}

async fn run(
    body: impl for<'a> FnOnce(&'a Database) -> LocalBoxFuture<'a, Result<()>>,
) -> Result<()> {
    let _serial = SERIAL.lock().await;
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let admin = PgPool::connect(&raw).await?;
    let schema = format!("prism_issued_batch_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await?;
    let mut url = url::Url::parse(&raw)?;
    url.query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let ledger = Ledger::connect(url.as_str(), "batch-test".into(), 5, true).await?;
    let db = Database {
        ledger,
        admin,
        schema,
        url: url.into(),
    };
    let result = body(&db).await;
    db.ledger.pool.close().await;
    let cleanup = sqlx::query(&format!("DROP SCHEMA {} CASCADE", db.schema))
        .execute(&db.admin)
        .await;
    db.admin.close().await;
    result?;
    cleanup?;
    Ok(())
}

#[derive(Clone)]
struct Original {
    record: CompactPrepared,
    expiry: i64,
}
impl Original {
    fn dependency(&self) -> CompactDependency<'_> {
        CompactDependency {
            key: "prepared",
            original_revision: self.record.payout_revision,
            parent: &self.record.parent_hash,
            original_expires_at_ms: self.expiry,
            template_sha256: &self.record.template_sha256,
            prior_balances_digest: self.record.window.prior_balances_digest,
        }
    }
}

async fn now(db: &Database) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint")
            .fetch_one(&db.ledger.pool)
            .await?,
    )
}

async fn seed(db: &Database) -> Result<Original> {
    let template = PreparedTemplate::encode(
        &json!({"previousblockhash":"ab".repeat(32),"height":101,"transactions":[]}),
    )?;
    let manifest = ManifestSigningKey::from_seed_hex(&"41".repeat(32))?;
    let ledger = ManifestSigningKey::from_seed_hex(&"42".repeat(32))?;
    let record = CompactPrepared {
        format_version: CompactPrepared::FORMAT_VERSION,
        window: WindowRef {
            anchor_ms: 1_700_000_000_000,
            prior_balances_digest: qbit_prism::prior_balances_digest(&[]),
            shares: None,
        },
        share_seq: 0,
        payout_revision: 0,
        template_sha256: template.sha256().into(),
        parent_hash: "ab".repeat(32),
        parent_of_tip: "ac".repeat(32),
        fingerprint: "original".into(),
        generation: 1,
        coinbase_suffix_hex: "01020300000000".into(),
        payout_policy: PayoutPolicy::day_one_default(),
        ctv: None,
        fee: None,
        audit_builder_version: qbit_prism::AUDIT_BUILDER_VERSION,
        signer_keys: SignerKeys::of(&manifest, &ledger),
        audit_hashes: None,
    };
    let expiry = now(db).await? + 60_000;
    db.ledger
        .save_compact_prepared("prepared", &record, &template, &[], 0, expiry)
        .await?;
    Ok(Original { record, expiry })
}

fn jobs(expiry: i64) -> Vec<CompactIssuedJob> {
    (0..8)
        .map(|i| CompactIssuedJob {
            job_id: format!("child-{i}"),
            payload: json!({"prepared_key":"prepared","expires_at_ms":expiry+i,"nonce":i}),
            expires_at_ms: expiry + i,
        })
        .collect()
}

async fn save(
    db: &Database,
    original: &Original,
    jobs: &[CompactIssuedJob],
) -> Result<IssuedJobSave> {
    db.ledger
        .save_issued_jobs_compact(
            jobs,
            0,
            &original.record.parent_hash,
            original.dependency(),
            &CompactBatchAttempt::new(Instant::now() + Duration::from_secs(10)),
        )
        .await
}

async fn snapshot(db: &Database) -> Result<Value> {
    Ok(sqlx::query_scalar("SELECT coalesce(jsonb_agg(to_jsonb(j) ORDER BY job_id),'[]'::jsonb) FROM qbit_prism_jobs j").fetch_one(&db.ledger.pool).await?)
}

async fn blocked(db: &Database, blocker: i32, prefix: &str) -> Result<i32> {
    timeout(Duration::from_secs(5), async {
        loop {
            let found: Option<i32> = sqlx::query_scalar("SELECT pid FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND query LIKE $2 LIMIT 1")
                .bind(blocker).bind(format!("{prefix}%")).fetch_optional(&db.admin).await?;
            if let Some(pid) = found { return Ok::<_, anyhow::Error>(pid); }
            sleep(Duration::from_millis(2)).await;
        }
    }).await.context("expected database wait did not occur")?
}

fn spawn(
    db: &Database,
    original: &Original,
    entries: Vec<CompactIssuedJob>,
    attempt: Arc<CompactBatchAttempt>,
) -> tokio::task::JoinHandle<Result<IssuedJobSave>> {
    let ledger = db.ledger.clone();
    let original = original.clone();
    tokio::spawn(async move {
        ledger
            .save_issued_jobs_compact(
                &entries,
                0,
                &original.record.parent_hash,
                original.dependency(),
                &attempt,
            )
            .await
    })
}

async fn rollback_fence(db: &Database) -> Result<()> {
    timeout(Duration::from_secs(5), async {
        let mut tx = db.ledger.pool.begin().await?;
        sqlx::query("SELECT singleton FROM qbit_prism_cluster WHERE singleton FOR UPDATE")
            .execute(&mut *tx)
            .await?;
        tx.rollback().await?;
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("canceled transaction did not release cluster fence")?
}

#[tokio::test]
async fn one_transaction_preserves_exact_children_max_retention_and_idempotence() -> Result<()> {
    run(|db| Box::pin(async move {
        let original = seed(db).await?;
        let entries = jobs(original.expiry + 60_000);
        let original_payload: Value = sqlx::query_scalar("SELECT payload FROM qbit_prism_jobs WHERE job_id='prepared'").fetch_one(&db.ledger.pool).await?;
        ensure!(save(db, &original, &entries).await? == IssuedJobSave::Saved);
        let (count, transactions): (i64,i64) = sqlx::query_as("SELECT count(*),count(DISTINCT xmin::text) FROM qbit_prism_jobs WHERE job_id LIKE 'child-%'").fetch_one(&db.ledger.pool).await?;
        ensure!((count, transactions) == (8,1), "{count} children in {transactions} transactions");
        for entry in &entries { ensure!(db.ledger.job(&entry.job_id).await? == Some(entry.payload.clone())); }
        let (payload, retention): (Value,i64) = sqlx::query_as("SELECT payload,floor(extract(epoch FROM expires_at)*1000)::bigint FROM qbit_prism_jobs WHERE job_id='prepared'").fetch_one(&db.ledger.pool).await?;
        ensure!(payload == original_payload && retention == entries[7].expires_at_ms + 60_000);
        let before = snapshot(db).await?;
        let mut duplicate = entries.clone(); duplicate.push(entries[0].clone());
        ensure!(save(db, &original, &duplicate).await? == IssuedJobSave::Saved);
        ensure!(snapshot(db).await? == before);
        Ok(())
    })).await
}

#[tokio::test]
async fn conflicting_ids_and_late_insert_failure_roll_back_whole_batch_and_renewal() -> Result<()> {
    run(|db| Box::pin(async move {
        let original = seed(db).await?;
        let entries = jobs(original.expiry + 60_000);
        let before = snapshot(db).await?;
        let mut duplicate = entries.clone(); duplicate.push(entries[0].clone()); duplicate[8].payload["nonce"] = json!(100);
        ensure!(save(db, &original, &duplicate).await.unwrap_err().to_string().contains("immutable job ID conflict"));
        ensure!(snapshot(db).await? == before);
        sqlx::raw_sql("CREATE FUNCTION fail_batch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'late batch failure'; END; $$; CREATE TRIGGER fail_batch AFTER INSERT ON qbit_prism_jobs FOR EACH ROW WHEN (NEW.job_id='child-7') EXECUTE FUNCTION fail_batch();").execute(&db.ledger.pool).await?;
        ensure!(save(db, &original, &entries).await.is_err());
        rollback_fence(db).await?;
        ensure!(snapshot(db).await? == before);
        sqlx::query("DROP TRIGGER fail_batch ON qbit_prism_jobs").execute(&db.ledger.pool).await?;
        save(db, &original, &entries[..1]).await?;
        let before = snapshot(db).await?;
        let mut conflicting = entries.clone(); conflicting[0].payload["nonce"] = json!(9);
        ensure!(save(db, &original, &conflicting).await.unwrap_err().to_string().contains("immutable job ID conflict"));
        rollback_fence(db).await?;
        ensure!(snapshot(db).await? == before);
        Ok(())
    })).await
}

#[tokio::test]
async fn missing_and_corrupt_dependencies_and_batch_boundaries_never_write_children() -> Result<()>
{
    for mutation in [
        "DELETE FROM qbit_prism_jobs WHERE job_id='prepared'",
        "DELETE FROM qbit_prism_templates",
        "DELETE FROM qbit_prism_balance_snapshots",
        "UPDATE qbit_prism_jobs SET payload=jsonb_set(payload,'{generation}','99') || '{\"payout_revision\":9}' WHERE job_id='prepared'",
        "UPDATE qbit_prism_jobs SET template_sha256=repeat('f',64) WHERE job_id='prepared'",
    ] {
        run(move |db| Box::pin(async move {
            let original = seed(db).await?;
            let entries = jobs(original.expiry + 60_000);
            let before = snapshot(db).await?;
            ensure!(save(db, &original, &[]).await.is_err());
            ensure!(save(db, &original, &vec![entries[0].clone();65]).await.is_err());
            ensure!(snapshot(db).await? == before);
            sqlx::query(mutation).execute(&db.ledger.pool).await?;
            let before = snapshot(db).await?;
            let result = save(db, &original, &entries).await;
            if mutation.starts_with("DELETE FROM qbit_prism_jobs") { ensure!(result? == IssuedJobSave::PreparedMissing); }
            else { ensure!(result.is_err()); }
            rollback_fence(db).await?;
            ensure!(snapshot(db).await? == before);
            Ok(())
        })).await?;
    }
    Ok(())
}

#[tokio::test]
async fn revision_configuration_and_fatal_state_revalidate_after_cluster_wait() -> Result<()> {
    for mutation in [
        "payout_revision=1",
        "config_fingerprint=NULL",
        "config_fingerprint='changed'",
        "fatal_error='halt'",
    ] {
        run(move |db| {
            Box::pin(async move {
                let original = seed(db).await?;
                db.ledger
                    .configure("original", &original.record.signer_keys)
                    .await?;
                let before = snapshot(db).await?;
                let mut hold = db.ledger.pool.begin().await?;
                sqlx::query(&format!(
                    "UPDATE qbit_prism_cluster SET {mutation} WHERE singleton"
                ))
                .execute(&mut *hold)
                .await?;
                let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                    .fetch_one(&mut *hold)
                    .await?;
                let saving = spawn(
                    db,
                    &original,
                    jobs(original.expiry + 60_000),
                    Arc::new(CompactBatchAttempt::new(
                        Instant::now() + Duration::from_secs(10),
                    )),
                );
                blocked(db, blocker, "SELECT config_fingerprint").await?;
                hold.commit().await?;
                ensure!(saving.await?.is_err());
                rollback_fence(db).await?;
                ensure!(snapshot(db).await? == before);
                Ok(())
            })
        })
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn ordinary_authority_updates_wait_for_atomic_64_child_commit() -> Result<()> {
    for mutation in [
        "payout_revision=1",
        "config_fingerprint=NULL",
        "config_fingerprint='changed'",
        "fatal_error='halt'",
    ] {
        run(move |db| Box::pin(async move {
            let original = seed(db).await?;
            db.ledger.configure("original", &original.record.signer_keys).await?;
            let entries: Vec<_> = (0..64).map(|i| CompactIssuedJob {
                job_id: format!("child-{i}"),
                payload: json!({"prepared_key":"prepared","expires_at_ms":original.expiry+60_000+i,"nonce":i}),
                expires_at_ms: original.expiry+60_000+i,
            }).collect();
            sqlx::raw_sql(&format!("CREATE FUNCTION gate_batch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock({INSERT_GATE}); RETURN NEW; END; $$; CREATE TRIGGER gate_batch AFTER INSERT ON qbit_prism_jobs FOR EACH ROW WHEN (NEW.job_id='child-63') EXECUTE FUNCTION gate_batch();"))
                .execute(&db.ledger.pool).await?;
            let mut gate = db.ledger.pool.begin().await?;
            sqlx::query("SELECT pg_advisory_xact_lock($1)").bind(INSERT_GATE).execute(&mut *gate).await?;
            let gate_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut *gate).await?;
            let saving = spawn(db, &original, entries.clone(), Arc::new(CompactBatchAttempt::new(Instant::now()+Duration::from_secs(10))));
            let writer_pid = blocked(db, gate_pid, "INSERT INTO qbit_prism_jobs").await?;
            let pool = db.ledger.pool.clone();
            let updating = tokio::spawn(async move {
                sqlx::query(&format!("UPDATE qbit_prism_cluster SET {mutation} WHERE singleton")).execute(&pool).await
            });
            blocked(db, writer_pid, "UPDATE qbit_prism_cluster").await?;
            let children: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_prism_jobs WHERE job_id LIKE 'child-%'").fetch_one(&db.ledger.pool).await?;
            ensure!(children == 0, "partial cohort visible before commit");
            gate.rollback().await?;
            ensure!(saving.await?? == IssuedJobSave::Saved);
            updating.await??;
            let (count, transactions): (i64, i64) = sqlx::query_as("SELECT count(*),count(DISTINCT xmin::text) FROM qbit_prism_jobs WHERE job_id LIKE 'child-%'").fetch_one(&db.ledger.pool).await?;
            ensure!((count, transactions) == (64, 1));
            for entry in &entries { ensure!(db.ledger.job(&entry.job_id).await? == Some(entry.payload.clone())); }
            let before = snapshot(db).await?;
            ensure!(save(db, &original, &entries).await.is_err(), "revoked authority allowed retry");
            rollback_fence(db).await?;
            ensure!(snapshot(db).await? == before);
            Ok(())
        })).await?;
    }
    Ok(())
}

#[tokio::test]
async fn two_frontends_overlap_exact_children_without_renewal_loss_or_partial_conflicts(
) -> Result<()> {
    for cold in [false, true] {
        run(move |db| Box::pin(async move {
            let original = seed(db).await?;
            let peer = Ledger::connect(&db.url, "batch-peer".into(), 3, true).await?;
            let result: Result<()> = async {
                let entries = jobs(original.expiry + 60_000);
                let stored = db.ledger.compact_prepared("prepared").await?.unwrap();
                let repair = CompactRepair::encode(&stored.record, &PreparedTemplate::encode(&stored.template)?,
                    &stored.prior_balances, stored.original_expires_at_ms)?;
                if cold {
                    sqlx::query("DELETE FROM qbit_prism_jobs WHERE job_id='prepared'").execute(&db.ledger.pool).await?;
                }
                sqlx::raw_sql(&format!("CREATE FUNCTION gate_overlap() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock({INSERT_GATE}); RETURN NEW; END; $$; CREATE TRIGGER gate_overlap AFTER INSERT ON qbit_prism_jobs FOR EACH ROW WHEN (NEW.job_id='child-0') EXECUTE FUNCTION gate_overlap();"))
                    .execute(&db.ledger.pool).await?;
                let mut hold = db.ledger.pool.begin().await?;
                sqlx::query("SELECT pg_advisory_xact_lock($1)").bind(INSERT_GATE).execute(&mut *hold).await?;
                let hold_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut *hold).await?;
                let ledger = db.ledger.clone();
                let first_entries = entries.clone();
                let first_original = original.clone();
                let first = tokio::spawn(async move {
                    if cold {
                        let child = &first_entries[0];
                        ledger.save_issued_job_compact(&child.job_id, &child.payload, 0,
                            &first_original.record.parent_hash, child.expires_at_ms,
                            repair.dependency("prepared"), Some(&repair)).await
                    } else {
                        ledger.save_issued_jobs_compact(&first_entries, 0, &first_original.record.parent_hash,
                            first_original.dependency(), &CompactBatchAttempt::new(Instant::now()+Duration::from_secs(10))).await
                    }
                });
                let first_pid = blocked(db, hold_pid, "INSERT INTO qbit_prism_jobs").await?;
                let mut peer_entries = entries.clone();
                // Same overlapping identities plus one longer-lived new child.
                peer_entries.push(CompactIssuedJob { job_id:"child-peer".into(),
                    expires_at_ms:original.expiry+180_000,
                    payload:json!({"prepared_key":"prepared","expires_at_ms":original.expiry+180_000}) });
                let peer_original = original.clone();
                let peer_ledger = peer.clone();
                let expected = peer_entries.clone();
                let second = tokio::spawn(async move {
                    peer_ledger.save_issued_jobs_compact(&peer_entries, 0, &peer_original.record.parent_hash,
                        peer_original.dependency(), &CompactBatchAttempt::new(Instant::now()+Duration::from_secs(10))).await
                });
                if cold {
                    // An uncommitted dependency is physically absent to this
                    // reader. Runtime repair followers retry the same identity.
                    ensure!(second.await?? == IssuedJobSave::PreparedMissing);
                } else {
                    blocked(db, first_pid, "UPDATE qbit_prism_jobs").await?;
                    hold.rollback().await?;
                    ensure!(first.await?? == IssuedJobSave::Saved);
                    ensure!(second.await?? == IssuedJobSave::Saved);
                    return finish_overlap(db, &peer, &original, &expected).await;
                }
                hold.rollback().await?;
                ensure!(first.await?? == IssuedJobSave::Saved);
                ensure!(peer.save_issued_jobs_compact(&expected, 0, &original.record.parent_hash, original.dependency(),
                    &CompactBatchAttempt::new(Instant::now()+Duration::from_secs(10))).await? == IssuedJobSave::Saved);
                finish_overlap(db, &peer, &original, &expected).await
            }.await;
            peer.pool.close().await;
            result
        })).await?;
    }
    Ok(())
}

async fn finish_overlap(
    db: &Database,
    peer: &Ledger,
    original: &Original,
    entries: &[CompactIssuedJob],
) -> Result<()> {
    for entry in entries {
        ensure!(db.ledger.job(&entry.job_id).await? == Some(entry.payload.clone()));
    }
    let stored = db.ledger.compact_prepared("prepared").await?.unwrap();
    ensure!(
        stored.record == original.record
            && stored.original_expires_at_ms == original.expiry
            && stored.expires_at_ms == entries.last().unwrap().expires_at_ms + 60_000
    );
    let before = snapshot(db).await?;
    let mut conflict = entries.to_vec();
    conflict[0].payload["nonce"] = json!(999);
    conflict.push(CompactIssuedJob {
        job_id: "child-conflict".into(),
        expires_at_ms: original.expiry + 360_000,
        payload: json!({"prepared_key":"prepared","expires_at_ms":original.expiry+360_000}),
    });
    ensure!(peer
        .save_issued_jobs_compact(
            &conflict,
            0,
            &original.record.parent_hash,
            original.dependency(),
            &CompactBatchAttempt::new(Instant::now() + Duration::from_secs(10))
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("immutable job ID conflict"));
    rollback_fence(db).await?;
    ensure!(
        snapshot(db).await? == before,
        "conflicting cohort leaked child or renewal"
    );
    ensure!(save(db, original, entries).await? == IssuedJobSave::Saved);
    ensure!(snapshot(db).await? == before);
    Ok(())
}

#[tokio::test]
async fn atomic_64_cohort_is_durable_while_three_second_settlement_stub_remains_held() -> Result<()>
{
    run(|db| Box::pin(async move {
        let original = seed(db).await?;
        let entries: Vec<_> = (0..64).map(|i| CompactIssuedJob {
            job_id:format!("child-{i}"), expires_at_ms:original.expiry+i,
            payload:json!({"prepared_key":"prepared","expires_at_ms":original.expiry+i,"nonce":i}),
        }).collect();
        let mut stub = db.ledger.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)").bind(0x505249534d000003_i64).execute(&mut *stub).await?;
        let release_at = Instant::now()+Duration::from_secs(3);
        tokio::time::timeout_at(release_at, async {
            ensure!(save(db, &original, &entries).await? == IssuedJobSave::Saved);
            for entry in &entries { ensure!(db.ledger.job(&entry.job_id).await? == Some(entry.payload.clone())); }
            let (count, transactions): (i64,i64) = sqlx::query_as("SELECT count(*),count(DISTINCT xmin::text) FROM qbit_prism_jobs WHERE job_id LIKE 'child-%'").fetch_one(&db.ledger.pool).await?;
            ensure!((count,transactions) == (64,1));
            Ok::<_,anyhow::Error>(())
        }).await.context("64-child commit did not precede stub release")??;
        tokio::time::sleep_until(release_at).await;
        stub.rollback().await?;
        Ok(())
    })).await
}

#[tokio::test]
async fn chain_transition_waits_for_issuance_then_fences_stale_revision_and_aba_epoch() -> Result<()>
{
    use qbit_prism_server::ledger::ChainTransition;
    run(|db| Box::pin(async move {
        let original = seed(db).await?;
        let parent = original.record.parent_hash.clone();
        let sibling = "cd".repeat(32);
        let revision = db.ledger.observe_chain_view(&parent, 101, "01").await?;
        let observed = db.ledger.chain_observation_state().await?;
        let transition = ChainTransition { predecessor:parent.clone(), origin_chain_epoch:observed.chain_epoch };
        sqlx::raw_sql(&format!("CREATE FUNCTION gate_epoch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock({INSERT_GATE}); RETURN NEW; END; $$; CREATE TRIGGER gate_epoch AFTER INSERT ON qbit_prism_jobs FOR EACH ROW WHEN (NEW.job_id='child-7') EXECUTE FUNCTION gate_epoch();"))
            .execute(&db.ledger.pool).await?;
        let mut hold = db.ledger.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)").bind(INSERT_GATE).execute(&mut *hold).await?;
        let hold_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut *hold).await?;
        let ledger = db.ledger.clone();
        let first = original.clone();
        let entries = jobs(original.expiry);
        let saving = tokio::spawn(async move {
            ledger.save_issued_jobs_compact(&entries, revision, &first.record.parent_hash, first.dependency(),
                &CompactBatchAttempt::new(Instant::now()+Duration::from_secs(10))).await
        });
        let saving_pid = blocked(db, hold_pid, "INSERT INTO qbit_prism_jobs").await?;
        let ledger = db.ledger.clone();
        let next = transition.clone();
        let state = observed.clone();
        let tip = sibling.clone();
        let changing = tokio::spawn(async move { ledger.observe_chain_transition(&next, &tip, 101, "01", &state).await });
        blocked(db, saving_pid, "SELECT payout_revision,chain_epoch").await?;
        hold.rollback().await?;
        ensure!(saving.await?? == IssuedJobSave::Saved);
        ensure!(changing.await?? == revision+1);
        let state = db.ledger.chain_observation_state().await?;
        ensure!(state.chain_epoch == observed.chain_epoch+1);
        ensure!(db.ledger.save_issued_jobs_compact(&jobs(original.expiry), revision, &parent, original.dependency(),
            &CompactBatchAttempt::new(Instant::now()+Duration::from_secs(10))).await.is_err());
        db.ledger.observe_chain_transition(&ChainTransition {predecessor:sibling.clone(), origin_chain_epoch:state.chain_epoch},
            &parent, 101, "01", &state).await?;
        let aba = db.ledger.chain_observation_state().await?;
        ensure!(aba.best_tip_hash.as_deref() == Some(parent.as_str()) && aba.chain_epoch == observed.chain_epoch+2);
        ensure!(db.ledger.observe_chain_transition(&transition, &sibling, 101, "01", &aba).await.is_err(), "stale ABA epoch gained authority");
        let durable: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_prism_jobs WHERE job_id LIKE 'child-%' AND payout_revision=$1")
            .bind(revision).fetch_one(&db.ledger.pool).await?;
        ensure!(durable == 8, "later authority rewrote original issued rows");
        Ok(())
    })).await
}

#[tokio::test]
async fn earliest_child_expiry_survives_cluster_dependency_blob_and_insert_waits() -> Result<()> {
    for phase in 0..4 {
        run(move |db| Box::pin(async move {
            let original = seed(db).await?;
            let before = snapshot(db).await?;
            let mut hold = db.ledger.pool.begin().await?;
            let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut *hold).await?;
            let query = match phase {
                0 => "SELECT singleton FROM qbit_prism_cluster WHERE singleton FOR UPDATE".into(),
                1 => "SELECT true FROM qbit_prism_jobs WHERE job_id='prepared' FOR UPDATE".into(),
                2 => "SELECT true FROM qbit_prism_templates FOR UPDATE".into(),
                _ => {
                    sqlx::raw_sql(&format!("CREATE FUNCTION gate_batch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock({INSERT_GATE}); RETURN NEW; END; $$; CREATE TRIGGER gate_batch AFTER INSERT ON qbit_prism_jobs FOR EACH ROW WHEN (NEW.job_id='child-7') EXECUTE FUNCTION gate_batch();")).execute(&db.ledger.pool).await?;
                    format!("SELECT pg_advisory_xact_lock({INSERT_GATE})")
                }
            };
            sqlx::query(&query).execute(&mut *hold).await?;
            let mut entries = jobs(original.expiry + 60_000);
            entries[0].expires_at_ms = now(db).await? + 150;
            entries[0].payload["expires_at_ms"] = json!(entries[0].expires_at_ms);
            let saving = spawn(db, &original, entries, Arc::new(CompactBatchAttempt::new(Instant::now() + Duration::from_secs(10))));
            let prefix = match phase { 0 => "SELECT config_fingerprint", 1 => "SELECT parent_hash", 2 => "SELECT true FROM qbit_prism_templates", _ => "INSERT INTO qbit_prism_jobs" };
            blocked(db, blocker, prefix).await?;
            sleep(Duration::from_millis(180)).await;
            hold.rollback().await?;
            let error = saving.await?.unwrap_err();
            ensure!(error.to_string().contains("deadline elapsed"), "{error:#}");
            rollback_fence(db).await?;
            ensure!(snapshot(db).await? == before);
            Ok(())
        })).await?;
    }
    Ok(())
}

#[tokio::test]
async fn canceled_insert_releases_pool_and_lost_commit_ack_reconciles_exact_identity() -> Result<()>
{
    for committing in [false, true] {
        run(move |db| Box::pin(async move {
            let original = seed(db).await?;
            let before = snapshot(db).await?;
            let trigger = if committing { "CONSTRAINT TRIGGER gate_batch AFTER INSERT ON qbit_prism_jobs DEFERRABLE INITIALLY DEFERRED" }
                else { "TRIGGER gate_batch AFTER INSERT ON qbit_prism_jobs" };
            sqlx::raw_sql(&format!("CREATE FUNCTION gate_batch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock({INSERT_GATE}); RETURN NEW; END; $$; CREATE {trigger} FOR EACH ROW WHEN (NEW.job_id='child-7') EXECUTE FUNCTION gate_batch();")).execute(&db.ledger.pool).await?;
            let mut hold = db.ledger.pool.begin().await?;
            sqlx::query("SELECT pg_advisory_xact_lock($1)").bind(INSERT_GATE).execute(&mut *hold).await?;
            let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut *hold).await?;
            let entries = jobs(original.expiry + 60_000);
            let attempt = Arc::new(CompactBatchAttempt::new(Instant::now() + Duration::from_secs(10)));
            let saving = spawn(db, &original, entries.clone(), attempt.clone());
            blocked(db, blocker, if committing { "COMMIT" } else { "INSERT INTO qbit_prism_jobs" }).await?;
            ensure!(attempt.commit_started() == committing);
            saving.abort(); ensure!(saving.await.unwrap_err().is_cancelled());
            hold.rollback().await?;
            rollback_fence(db).await?;
            if !committing { ensure!(snapshot(db).await? == before); }
            // Reconcile only these exact IDs/payloads/deadlines; no fresh replay.
            save(db, &original, &entries).await?;
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_prism_jobs WHERE job_id LIKE 'child-%'").fetch_one(&db.ledger.pool).await?;
            ensure!(count == 8);
            timeout(Duration::from_secs(5), async {
                loop {
                    if db.ledger.pool.num_idle() == db.ledger.pool.size() as usize { break; }
                    sleep(Duration::from_millis(2)).await;
                }
            }).await.context("batch connection was not released")?;
            Ok(())
        })).await?;
    }
    Ok(())
}
