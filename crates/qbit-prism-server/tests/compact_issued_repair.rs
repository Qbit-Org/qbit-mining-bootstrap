//! Atomic compact issued dependency repair; no runtime writer is activated.
//! Uses only the disposable database supplied by test/prism-native-tests.sh.
use anyhow::{ensure, Context, Result};
use futures_util::future::LocalBoxFuture;
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{CarryForwardBalance, PayoutPolicy};
use qbit_prism_server::ledger::{
    CompactDependency, CompactPrepared, CompactRepair, IssuedJobSave, Ledger, PreparedAuditHashes,
    PreparedDependency, PreparedTemplate, ShareRange, SignerKeys, WindowRef,
};
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::time::Duration;
use tokio::time::{sleep, timeout};

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const SETTLEMENT_LOCK: i64 = 0x505249534d000003;
const TEST_GATE: i64 = 0x27300002;

#[path = "support/blob_cleanup.rs"]
mod blob_cleanup;

struct Database {
    admin: PgPool,
    ledger: Ledger,
    schema: String,
}

impl Database {
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let admin = PgPool::connect(&raw).await?;
        let schema = format!("prism_compact_issued_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut url = url::Url::parse(&raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        let ledger = Ledger::connect(url.as_str(), "compact-issued-test".into(), 5, true).await?;
        Ok(Some(Self {
            admin,
            ledger,
            schema,
        }))
    }

    async fn now_ms(&self) -> Result<i64> {
        Ok(
            sqlx::query_scalar(
                "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
            )
            .fetch_one(&self.ledger.pool)
            .await?,
        )
    }

    async fn expires(&self) -> Result<i64> {
        Ok(self.now_ms().await? + 60_000)
    }

    async fn close(self) -> Result<()> {
        self.ledger.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await?;
        self.admin.close().await;
        Ok(())
    }
}

async fn run(
    body: impl for<'a> FnOnce(&'a Database) -> LocalBoxFuture<'a, Result<()>>,
) -> Result<()> {
    let _serial = SERIAL.lock().await;
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = body(&db).await;
    let cleanup = db.close().await;
    match (result, cleanup) {
        (Ok(()), cleanup) => cleanup,
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(cleanup)) => {
            Err(error.context(format!("schema cleanup also failed: {cleanup}")))
        }
    }
}

fn balances() -> Vec<CarryForwardBalance> {
    vec![
        CarryForwardBalance {
            recipient_id: "b".into(),
            order_key: "z".into(),
            p2mr_program_hex: "22".repeat(32),
            balance_sats: 200,
        },
        CarryForwardBalance {
            recipient_id: "a".into(),
            order_key: "a".into(),
            p2mr_program_hex: "11".repeat(32),
            balance_sats: -100,
        },
    ]
}

fn template() -> Value {
    json!({"previousblockhash": "ab".repeat(32), "height": 101, "transactions": []})
}

fn record(
    template: &PreparedTemplate,
    balances: &[CarryForwardBalance],
    nonempty: bool,
) -> CompactPrepared {
    let manifest = ManifestSigningKey::from_seed_hex(&"41".repeat(32)).unwrap();
    let ledger = ManifestSigningKey::from_seed_hex(&"42".repeat(32)).unwrap();
    CompactPrepared {
        format_version: CompactPrepared::FORMAT_VERSION,
        window: WindowRef {
            anchor_ms: 1_700_000_000_000,
            prior_balances_digest: qbit_prism::prior_balances_digest(balances),
            shares: nonempty.then_some(ShareRange {
                first_share_seq: 1,
                last_share_seq: 3,
                share_count: 2,
                snapshot_sha256: [0x34; 32],
            }),
        },
        share_seq: 5,
        payout_revision: 0,
        template_sha256: template.sha256().into(),
        parent_hash: "ab".repeat(32),
        parent_of_tip: "ac".repeat(32),
        fingerprint: "original-fingerprint".into(),
        generation: 11,
        coinbase_suffix_hex: "01020300000000".into(),
        payout_policy: PayoutPolicy::day_one_default(),
        ctv: None,
        fee: None,
        audit_builder_version: qbit_prism::AUDIT_BUILDER_VERSION,
        signer_keys: SignerKeys::of(&manifest, &ledger),
        audit_hashes: nonempty.then_some(PreparedAuditHashes {
            audit_bundle_sha256: "cd".repeat(32),
            coinbase_manifest_sha256: "ef".repeat(32),
        }),
    }
}

async fn seed_share(db: &Database, sequence: i64) -> Result<()> {
    sqlx::query("INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,accepted,writer_id,writer_epoch) VALUES($1,$2,'a','a',decode(repeat('11',32),'hex'),1,100,100,'test-job',to_timestamp(1),1,to_timestamp(1),true,'storage-test',0)")
        .bind(sequence).bind(format!("test-share-{sequence}")).execute(&db.ledger.pool).await?;
    Ok(())
}

async fn seed_endpoints(db: &Database) -> Result<()> {
    seed_share(db, 1).await?;
    seed_share(db, 3).await?;
    Ok(())
}

async fn assert_empty(db: &Database) -> Result<()> {
    let counts: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM qbit_prism_jobs),(SELECT count(*) FROM qbit_prism_templates),(SELECT count(*) FROM qbit_prism_balance_snapshots)")
        .fetch_one(&db.ledger.pool).await?;
    ensure!(counts == (0, 0, 0), "partial prepared write: {counts:?}");
    Ok(())
}

struct Original {
    record: CompactPrepared,
    template: PreparedTemplate,
    balances: Vec<CarryForwardBalance>,
    expires: i64,
}

impl Original {
    fn dependency(&self) -> CompactDependency<'_> {
        CompactDependency {
            key: "prepared",
            original_revision: self.record.payout_revision,
            parent: &self.record.parent_hash,
            original_expires_at_ms: self.expires,
            template_sha256: &self.record.template_sha256,
            prior_balances_digest: self.record.window.prior_balances_digest,
        }
    }

    fn repair(&self) -> Result<CompactRepair> {
        CompactRepair::encode(&self.record, &self.template, &self.balances, self.expires)
    }
}

async fn save(
    db: &Database,
    original: &Original,
    expiry: i64,
    repair: Option<&CompactRepair>,
) -> Result<IssuedJobSave> {
    db.ledger
        .save_issued_job_compact(
            "child",
            &child(expiry),
            0,
            &original.record.parent_hash,
            expiry,
            original.dependency(),
            repair,
        )
        .await
}

async fn seed(db: &Database, nonempty: bool) -> Result<Original> {
    if nonempty {
        seed_endpoints(db).await?;
    }
    let template = PreparedTemplate::encode(&template())?;
    let balances = if nonempty { balances() } else { vec![] };
    let record = record(&template, &balances, nonempty);
    let expires = db.expires().await?;
    db.ledger
        .save_compact_prepared("prepared", &record, &template, &balances, 0, expires)
        .await?;
    Ok(Original {
        record,
        template,
        balances,
        expires,
    })
}

fn child(expires: i64) -> Value {
    json!({"prepared_key": "prepared", "expires_at_ms": expires, "extranonce1": "00000001"})
}

async fn snapshot(db: &Database) -> Result<Value> {
    Ok(sqlx::query_scalar("SELECT jsonb_build_array((SELECT coalesce(jsonb_agg(to_jsonb(j) ORDER BY job_id),'[]'::jsonb) FROM qbit_prism_jobs j),(SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY template_sha256),'[]'::jsonb) FROM qbit_prism_templates t),(SELECT coalesce(jsonb_agg(to_jsonb(b) ORDER BY prior_balances_digest),'[]'::jsonb) FROM qbit_prism_balance_snapshots b))")
        .fetch_one(&db.ledger.pool).await?)
}

async fn delete_dependencies(db: &Database, blobs: bool) -> Result<()> {
    sqlx::query("DELETE FROM qbit_prism_jobs WHERE job_id='prepared'")
        .execute(&db.ledger.pool)
        .await?;
    if blobs {
        sqlx::query("DELETE FROM qbit_prism_templates")
            .execute(&db.ledger.pool)
            .await?;
        sqlx::query("DELETE FROM qbit_prism_balance_snapshots")
            .execute(&db.ledger.pool)
            .await?;
    }
    Ok(())
}

struct Running<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for Running<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn blocked_query(db: &Database, blocker: i32, prefix: &str) -> Result<i32> {
    timeout(Duration::from_secs(5), async {
        loop {
            let pid: Option<i32> = sqlx::query_scalar("SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid)) AND query LIKE $2 ORDER BY pid LIMIT 1")
                .bind(blocker).bind(format!("{prefix}%")).fetch_optional(&db.admin).await?;
            if let Some(pid) = pid { return Ok::<_, anyhow::Error>(pid); }
            sleep(Duration::from_millis(5)).await;
        }
    }).await.context("expected two-connection PostgreSQL wait did not occur")?
}

async fn gate_insert(db: &Database, child: bool) -> Result<()> {
    let key = if child { "child" } else { "prepared" };
    sqlx::raw_sql(&format!("CREATE OR REPLACE FUNCTION gate_compact_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock({TEST_GATE}); RETURN NEW; END; $$; DROP TRIGGER IF EXISTS gate_compact_insert ON qbit_prism_jobs; CREATE TRIGGER gate_compact_insert AFTER INSERT ON qbit_prism_jobs FOR EACH ROW WHEN (NEW.job_id='{key}') EXECUTE FUNCTION gate_compact_insert();"))
        .execute(&db.ledger.pool).await?;
    Ok(())
}

async fn rollback_fence(db: &Database) -> Result<()> {
    let mut tx = db.ledger.pool.begin().await?;
    // This fixture also exercises GC: preserve its advisory-release proof,
    // then fence cancellation of the now advisory-independent compact writers.
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(SETTLEMENT_LOCK)
        .execute(&mut *tx)
        .await?;
    sqlx::query("SELECT singleton FROM qbit_prism_cluster WHERE singleton FOR UPDATE")
        .execute(&mut *tx)
        .await?;
    tx.rollback().await?;
    Ok(())
}

#[tokio::test]
async fn hot_save_and_exact_retry_preserve_originals_and_child_deadlines() -> Result<()> {
    for nonempty in [false, true] {
        run(move |db| {
            Box::pin(async move {
                let original = seed(db, nonempty).await?;
                let payload = db.ledger.job("prepared").await?.unwrap();
                let expiry = original.expires + 5_000;
                ensure!(save(db, &original, expiry, None).await? == IssuedJobSave::Saved);
                let retained = db.ledger.compact_prepared("prepared").await?.unwrap();
                ensure!(
                    retained.record == original.record
                        && retained.original_expires_at_ms == original.expires
                );
                ensure!(retained.expires_at_ms == expiry + 60_000);
                ensure!(db.ledger.job("prepared").await? == Some(payload));
                ensure!(db.ledger.job("child").await? == Some(child(expiry)));
                let before = snapshot(db).await?;
                ensure!(save(db, &original, expiry, None).await? == IssuedJobSave::Saved);
                let repair = original.repair()?;
                ensure!(save(db, &original, expiry, Some(&repair)).await? == IssuedJobSave::Saved);
                ensure!(
                    snapshot(db).await? == before,
                    "exact retry renewed or rewrote data"
                );
                for wrong in [expiry - 1, expiry + 120_000] {
                    ensure!(save(db, &original, wrong, Some(&repair))
                        .await
                        .unwrap_err()
                        .to_string()
                        .contains("immutable job ID conflict"));
                    ensure!(
                        snapshot(db).await? == before,
                        "child conflict leaked retention change"
                    );
                }
                sqlx::query("UPDATE qbit_prism_jobs SET template_sha256=$1 WHERE job_id='child'")
                    .bind(&original.record.template_sha256)
                    .execute(&db.ledger.pool)
                    .await?;
                let before = snapshot(db).await?;
                ensure!(save(db, &original, expiry, None)
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("immutable job ID conflict"));
                ensure!(
                    snapshot(db).await? == before,
                    "child typed-column corruption was accepted"
                );
                Ok(())
            })
        })
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn missing_record_restores_exact_inputs_with_surviving_or_missing_blobs() -> Result<()> {
    for (nonempty, remove_blobs) in [(false, false), (false, true), (true, false), (true, true)] {
        run(move |db| Box::pin(async move {
            let original = seed(db, nonempty).await?;
            let payload = db.ledger.job("prepared").await?.unwrap();
            let bytes: (Vec<u8>, Vec<u8>) = sqlx::query_as("SELECT t.template_bytes,b.balances FROM qbit_prism_templates t CROSS JOIN qbit_prism_balance_snapshots b")
                .fetch_one(&db.ledger.pool).await?;
            delete_dependencies(db, remove_blobs).await?;
            let before = snapshot(db).await?;
            let expiry = original.expires + 1_000;
            ensure!(save(db, &original, expiry, None).await? == IssuedJobSave::PreparedMissing);
            ensure!(snapshot(db).await? == before, "miss wrote partial state");
            let repair = original.repair()?;
            ensure!(save(db, &original, expiry, Some(&repair)).await? == IssuedJobSave::Saved);
            ensure!(db.ledger.job("prepared").await? == Some(payload));
            let stored = db.ledger.compact_prepared("prepared").await?.unwrap();
            ensure!(stored.record == original.record && stored.original_expires_at_ms == original.expires
                && stored.expires_at_ms == expiry + 60_000);
            let after: (Vec<u8>, Vec<u8>) = sqlx::query_as("SELECT t.template_bytes,b.balances FROM qbit_prism_templates t CROSS JOIN qbit_prism_balance_snapshots b")
                .fetch_one(&db.ledger.pool).await?;
            ensure!(after == bytes, "repair changed exact existing helper encoding");
            let before = snapshot(db).await?;
            ensure!(save(db, &original, expiry, Some(&repair)).await? == IssuedJobSave::Saved);
            ensure!(snapshot(db).await? == before);
            Ok(())
        })).await?;
    }
    Ok(())
}

async fn corrupt_blob(db: &Database, original: &Original, template: bool) -> Result<()> {
    if template {
        sqlx::query("DELETE FROM qbit_prism_templates")
            .execute(&db.ledger.pool)
            .await?;
        sqlx::query(
            "INSERT INTO qbit_prism_templates(template_sha256,template_bytes) VALUES($1,$2)",
        )
        .bind(&original.record.template_sha256)
        .bind(b"corrupt-template".as_slice())
        .execute(&db.ledger.pool)
        .await?;
    } else {
        sqlx::query("DELETE FROM qbit_prism_balance_snapshots")
            .execute(&db.ledger.pool)
            .await?;
        sqlx::query("INSERT INTO qbit_prism_balance_snapshots(prior_balances_digest,balances) VALUES($1,$2)")
            .bind(hex::encode(original.record.window.prior_balances_digest)).bind(b"corrupt-balances".as_slice())
            .execute(&db.ledger.pool).await?;
    }
    Ok(())
}

#[tokio::test]
async fn surviving_record_never_repairs_missing_blobs_and_rejects_known_corruption() -> Result<()> {
    for (template, corrupt) in [(true, false), (false, false), (true, true), (false, true)] {
        run(move |db| {
            Box::pin(async move {
                let original = seed(db, true).await?;
                let repair = original.repair()?;
                if corrupt {
                    corrupt_blob(db, &original, template).await?;
                } else {
                    sqlx::query(if template {
                        "DELETE FROM qbit_prism_templates"
                    } else {
                        "DELETE FROM qbit_prism_balance_snapshots"
                    })
                    .execute(&db.ledger.pool)
                    .await?;
                }
                let before = snapshot(db).await?;
                // Metadata-only hot checks can detect absence, not unseen bad bytes.
                if !corrupt {
                    let error = save(db, &original, original.expires, None)
                        .await
                        .unwrap_err();
                    ensure!(error.to_string().contains("missing"));
                    ensure!(snapshot(db).await? == before);
                }
                ensure!(save(db, &original, original.expires, Some(&repair))
                    .await
                    .is_err());
                ensure!(
                    snapshot(db).await? == before,
                    "surviving record was silently repaired"
                );
                ensure!(db.ledger.compact_prepared("prepared").await.is_err());
                Ok(())
            })
        })
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn conflicting_survivors_leave_no_partial_repair() -> Result<()> {
    for template in [false, true] {
        run(move |db| {
            Box::pin(async move {
                let original = seed(db, true).await?;
                let repair = original.repair()?;
                delete_dependencies(db, true).await?;
                corrupt_blob(db, &original, template).await?;
                let before = snapshot(db).await?;
                ensure!(save(db, &original, original.expires, Some(&repair))
                    .await
                    .is_err());
                ensure!(
                    snapshot(db).await? == before,
                    "survivor conflict leaked another blob"
                );
                Ok(())
            })
        })
        .await?;
    }
    run(|db| Box::pin(async move {
        let original = seed(db, false).await?;
        let repair = original.repair()?;
        sqlx::query("UPDATE qbit_prism_jobs SET payload=jsonb_set(payload,'{fingerprint}','\"different-original\"') WHERE job_id='prepared'")
            .execute(&db.ledger.pool).await?;
        let before = snapshot(db).await?;
        ensure!(save(db, &original, original.expires, Some(&repair)).await.unwrap_err().to_string().contains("immutable compact prepared conflict"));
        ensure!(snapshot(db).await? == before);
        Ok(())
    })).await
}

#[tokio::test]
async fn hot_metadata_rejects_payload_column_and_original_identity_mismatches() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let original = seed(db, true).await?;
            let repair = original.repair()?;
            for mutation in [
                "parent_hash='other-parent'",
                "payout_revision=1",
                "template_sha256=repeat('11',32)",
                "window_anchor_ms=window_anchor_ms+1",
                "window_prior_balances_sha256=repeat('11',32)",
                "window_first_share_seq=2",
                "window_last_share_seq=4",
                "window_share_count=1",
                "window_snapshot_sha256=repeat('11',32)",
                "expires_at=to_timestamp((payload->>'original_expires_at_ms')::numeric/1000)-interval '1 millisecond'",
                "payload=payload-'original_expires_at_ms'",
                "payload=jsonb_set(payload,'{original_expires_at_ms}','null')",
                "payload=jsonb_set(payload,'{original_expires_at_ms}','1.5')",
                "payload=jsonb_set(payload,'{format_version}','\"1\"')",
                "payload=jsonb_set(payload,'{format_version}','1.0')",
                "payload=jsonb_set(payload,'{format_version}','99')",
                "payload=jsonb_set(payload,'{window,anchor_ms}','1')",
            ] {
                sqlx::query(&format!(
                    "UPDATE qbit_prism_jobs SET {mutation} WHERE job_id='prepared'"
                ))
                .execute(&db.ledger.pool)
                .await?;
                let before = snapshot(db).await?;
                ensure!(
                    save(db, &original, original.expires, None).await.is_err(),
                    "accepted hot mutation {mutation}"
                );
                ensure!(
                    save(db, &original, original.expires, Some(&repair))
                        .await
                        .is_err(),
                    "accepted repair mutation {mutation}"
                );
                ensure!(snapshot(db).await? == before);
                delete_dependencies(db, false).await?;
                db.ledger
                    .save_compact_prepared(
                        "prepared",
                        &original.record,
                        &original.template,
                        &original.balances,
                        0,
                        original.expires,
                    )
                    .await?;
            }
            for wrong in [original.expires - 1, original.expires + 1] {
                let before = snapshot(db).await?;
                let dependency = CompactDependency {
                    original_expires_at_ms: wrong,
                    ..original.dependency()
                };
                ensure!(db
                    .ledger
                    .save_issued_job_compact(
                        "child",
                        &child(original.expires),
                        0,
                        &original.record.parent_hash,
                        original.expires,
                        dependency,
                        None
                    )
                    .await
                    .is_err());
                ensure!(snapshot(db).await? == before);
            }
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn invalid_repair_inputs_and_child_boundaries_write_nothing() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let original = seed(db, false).await?;
            let repair = original.repair()?;
            delete_dependencies(db, true).await?;
            for dependency in [
                CompactDependency {
                    key: "",
                    ..original.dependency()
                },
                CompactDependency {
                    original_revision: 1,
                    ..original.dependency()
                },
                CompactDependency {
                    original_revision: -1,
                    ..original.dependency()
                },
                CompactDependency {
                    original_expires_at_ms: original.expires + 1,
                    ..original.dependency()
                },
                CompactDependency {
                    original_expires_at_ms: i64::MAX,
                    ..original.dependency()
                },
                CompactDependency {
                    parent: "wrong-parent",
                    ..original.dependency()
                },
                CompactDependency {
                    template_sha256: "invalid",
                    ..original.dependency()
                },
                CompactDependency {
                    prior_balances_digest: [0; 32],
                    ..original.dependency()
                },
            ] {
                ensure!(db
                    .ledger
                    .save_issued_job_compact(
                        "child",
                        &child(original.expires),
                        0,
                        &original.record.parent_hash,
                        original.expires,
                        dependency,
                        Some(&repair)
                    )
                    .await
                    .is_err());
                assert_empty(db).await?;
            }
            for id in ["", "prepared"] {
                ensure!(db
                    .ledger
                    .save_issued_job_compact(
                        id,
                        &child(original.expires),
                        0,
                        &original.record.parent_hash,
                        original.expires,
                        original.dependency(),
                        Some(&repair)
                    )
                    .await
                    .is_err());
                assert_empty(db).await?;
            }
            for value in [
                json!({}),
                json!({"prepared_key":"wrong","expires_at_ms":original.expires}),
                child(original.expires + 1),
            ] {
                ensure!(db
                    .ledger
                    .save_issued_job_compact(
                        "child",
                        &value,
                        0,
                        &original.record.parent_hash,
                        original.expires,
                        original.dependency(),
                        Some(&repair)
                    )
                    .await
                    .is_err());
                assert_empty(db).await?;
            }
            for expiry in [0, -1, i64::MAX] {
                ensure!(save(db, &original, expiry, Some(&repair)).await.is_err());
                assert_empty(db).await?;
            }
            let mut bad = original.record.clone();
            bad.generation += 1;
            bad.template_sha256 = "00".repeat(32);
            ensure!(
                CompactRepair::encode(&bad, &original.template, &[], original.expires).is_err()
            );
            ensure!(CompactRepair::encode(
                &original.record,
                &original.template,
                &balances(),
                original.expires
            )
            .is_err());
            ensure!(
                CompactRepair::encode(&original.record, &original.template, &[], i64::MAX).is_err()
            );
            assert_empty(db).await?;
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn elapsed_original_is_identity_only_while_the_child_deadline_stays_live() -> Result<()> {
    run(|db| Box::pin(async move {
        let mut original = seed(db, false).await?;
        original.expires = db.now_ms().await? - 10_000;
        sqlx::query("UPDATE qbit_prism_jobs SET payload=jsonb_set(payload,'{original_expires_at_ms}',to_jsonb($1::bigint)) WHERE job_id='prepared'")
            .bind(original.expires).execute(&db.ledger.pool).await?;
        let payload = db.ledger.job("prepared").await?.unwrap();
        let expiry = db.expires().await?;
        let repair = original.repair()?;
        delete_dependencies(db, true).await?;
        ensure!(save(db, &original, expiry, Some(&repair)).await? == IssuedJobSave::Saved);
        let stored = db.ledger.compact_prepared("prepared").await?.unwrap();
        ensure!(stored.original_expires_at_ms == original.expires && stored.expires_at_ms == expiry+60_000);
        ensure!(db.ledger.job("prepared").await? == Some(payload));
        ensure!(db.ledger.job("child").await? == Some(child(expiry)));
        ensure!(db.ledger.save_compact_prepared("prepared", &original.record, &original.template,
            &[], 0, original.expires).await.unwrap_err().to_string().contains("prepared deadline elapsed"));
        let before = snapshot(db).await?;
        ensure!(save(db, &original, expiry, Some(&repair)).await? == IssuedJobSave::Saved);
        ensure!(snapshot(db).await? == before);
        Ok(())
    })).await
}

#[tokio::test]
async fn conflicting_child_survivor_rolls_back_complete_dependency_restoration() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let original = seed(db, true).await?;
            let expiry = original.expires;
            ensure!(save(db, &original, expiry, None).await? == IssuedJobSave::Saved);
            let repair = original.repair()?;
            delete_dependencies(db, true).await?;
            let before = snapshot(db).await?;
            ensure!(save(db, &original, expiry + 1_000, Some(&repair))
                .await
                .unwrap_err()
                .to_string()
                .contains("immutable job ID conflict"));
            ensure!(
                snapshot(db).await? == before,
                "child conflict leaked restored dependencies"
            );
            ensure!(save(db, &original, expiry, Some(&repair)).await? == IssuedJobSave::Saved);
            ensure!(db.ledger.job("child").await? == Some(child(expiry)));
            Ok(())
        })
    })
    .await
}

fn spawn_save(
    ledger: Ledger,
    repair: CompactRepair,
    expiry: i64,
) -> Running<Result<IssuedJobSave>> {
    Running(tokio::spawn(async move {
        let parent = repair.dependency("prepared").parent.to_owned();
        ledger
            .save_issued_job_compact(
                "child",
                &child(expiry),
                0,
                &parent,
                expiry,
                repair.dependency("prepared"),
                Some(&repair),
            )
            .await
    }))
}

#[tokio::test]
async fn current_revision_configuration_and_writable_fences_recheck_after_row_waits() -> Result<()>
{
    for (mutation, expected) in [
        ("payout_revision=1", "payout revision changed"),
        (
            "config_fingerprint=NULL",
            "cluster configuration fingerprint differs",
        ),
        (
            "config_fingerprint='rotated'",
            "cluster configuration fingerprint differs",
        ),
        ("fatal_error='test halt'", "cluster halted"),
    ] {
        run(move |db| {
            Box::pin(async move {
                let original = seed(db, false).await?;
                db.ledger
                    .configure("original", &original.record.signer_keys)
                    .await?;
                let repair = original.repair()?;
                delete_dependencies(db, true).await?;
                let mut hold = db.ledger.pool.begin().await?;
                // An authority writer as the server makes one: the row FOR UPDATE, then
                // the UPDATE (a bare non-key UPDATE is forbidden and passes a KEY SHARE
                // job fence by design, #479).
                sqlx::query("SELECT singleton FROM qbit_prism_cluster WHERE singleton FOR UPDATE")
                    .execute(&mut *hold)
                    .await?;
                sqlx::query(&format!(
                    "UPDATE qbit_prism_cluster SET {mutation} WHERE singleton"
                ))
                .execute(&mut *hold)
                .await?;
                let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                    .fetch_one(&mut *hold)
                    .await?;
                let mut saving = spawn_save(db.ledger.clone(), repair, original.expires);
                blocked_query(db, blocker, "SELECT config_fingerprint").await?;
                hold.commit().await?;
                let error = timeout(Duration::from_secs(5), &mut saving.0)
                    .await??
                    .unwrap_err();
                ensure!(error.to_string().contains(expected), "{error:#}");
                assert_empty(db).await?;
                Ok(())
            })
        })
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn child_deadline_covers_cluster_dependency_and_post_insert_waits() -> Result<()> {
    for phase in 0..4 {
        run(move |db| {
            Box::pin(async move {
                let original = seed(db, false).await?;
                let repair = original.repair()?;
                if phase != 1 {
                    delete_dependencies(db, true).await?;
                }
                if phase >= 2 {
                    gate_insert(db, phase == 3).await?;
                }
                let mut hold = db.ledger.pool.begin().await?;
                let prefix = if phase == 1 {
                    sqlx::query(
                        "SELECT job_id FROM qbit_prism_jobs WHERE job_id='prepared' FOR UPDATE",
                    )
                    .fetch_one(&mut *hold)
                    .await?;
                    "SELECT parent_hash"
                } else if phase == 0 {
                    sqlx::query(
                        "SELECT singleton FROM qbit_prism_cluster WHERE singleton FOR UPDATE",
                    )
                    .execute(&mut *hold)
                    .await?;
                    "SELECT config_fingerprint"
                } else {
                    sqlx::query("SELECT pg_advisory_xact_lock($1)")
                        .bind(TEST_GATE)
                        .execute(&mut *hold)
                        .await?;
                    "INSERT INTO qbit_prism_jobs"
                };
                let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                    .fetch_one(&mut *hold)
                    .await?;
                let before = snapshot(db).await?;
                let expiry = db.now_ms().await? + 1_000;
                let mut saving = spawn_save(db.ledger.clone(), repair, expiry);
                blocked_query(db, blocker, prefix).await?;
                ensure!(snapshot(db).await? == before, "uncommitted repair leaked");
                while db.now_ms().await? <= expiry {
                    sleep(Duration::from_millis(5)).await;
                }
                hold.rollback().await?;
                let error = timeout(Duration::from_secs(5), &mut saving.0)
                    .await??
                    .unwrap_err();
                ensure!(
                    error.to_string().contains("issued job deadline elapsed"),
                    "phase {phase}: {error:#}"
                );
                ensure!(
                    snapshot(db).await? == before,
                    "deadline changed durable state"
                );
                Ok(())
            })
        })
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_or_failed_inserts_roll_back_record_blobs_and_child() -> Result<()> {
    for child_gate in [false, true] {
        run(move |db| {
            Box::pin(async move {
                let original = seed(db, false).await?;
                let repair = original.repair()?;
                delete_dependencies(db, true).await?;
                gate_insert(db, child_gate).await?;
                let mut hold = db.ledger.pool.begin().await?;
                sqlx::query("SELECT pg_advisory_xact_lock($1)")
                    .bind(TEST_GATE)
                    .execute(&mut *hold)
                    .await?;
                let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                    .fetch_one(&mut *hold)
                    .await?;
                let mut saving = spawn_save(db.ledger.clone(), repair, original.expires);
                blocked_query(db, blocker, "INSERT INTO qbit_prism_jobs").await?;
                assert_empty(db).await?;
                saving.0.abort();
                ensure!((&mut saving.0).await.unwrap_err().is_cancelled());
                hold.rollback().await?;
                rollback_fence(db).await?;
                assert_empty(db).await?;
                Ok(())
            })
        })
        .await?;
    }
    run(|db| Box::pin(async move {
        let original = seed(db, false).await?;
        let repair = original.repair()?;
        delete_dependencies(db, true).await?;
        sqlx::raw_sql("CREATE FUNCTION fail_child() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected child storage failure'; END; $$; CREATE TRIGGER fail_child AFTER INSERT ON qbit_prism_jobs FOR EACH ROW WHEN (NEW.job_id='child') EXECUTE FUNCTION fail_child();")
            .execute(&db.ledger.pool).await?;
        let error = save(db, &original, original.expires, Some(&repair)).await.unwrap_err();
        ensure!(format!("{error:#}").contains("injected child storage failure"));
        rollback_fence(db).await?;
        assert_empty(db).await?;
        Ok(())
    })).await
}

#[tokio::test]
async fn concurrent_repairs_publish_one_dependency_identity_and_fixed_children() -> Result<()> {
    run(|db| Box::pin(async move {
        let original = seed(db, true).await?;
        let repair = original.repair()?;
        delete_dependencies(db, true).await?;
        let expiry = original.expires+1_000;
        let calls = (0..8).map(|index| {
            let repair = &repair;
            let parent = &original.record.parent_hash;
            async move {
                let key = format!("child-{index}");
                let result = db.ledger.save_issued_job_compact(&key, &child(expiry), 0, parent,
                    expiry, repair.dependency("prepared"), Some(repair)).await?;
                ensure!(result == IssuedJobSave::Saved);
                ensure!(db.ledger.job(&key).await? == Some(child(expiry)));
                Ok::<_,anyhow::Error>(())
            }
        });
        for result in futures_util::future::join_all(calls).await { result?; }
        let counts: (i64,i64,i64) = sqlx::query_as("SELECT (SELECT count(*) FROM qbit_prism_jobs),(SELECT count(*) FROM qbit_prism_templates),(SELECT count(*) FROM qbit_prism_balance_snapshots)")
            .fetch_one(&db.ledger.pool).await?;
        ensure!(counts == (9,1,1), "{counts:?}");
        let stored = db.ledger.compact_prepared("prepared").await?.unwrap();
        ensure!(stored.record == original.record && stored.original_expires_at_ms == original.expires
            && stored.expires_at_ms == expiry+60_000);
        Ok(())
    })).await
}

#[tokio::test]
async fn direct_prepared_and_single_persistence_finish_before_settlement_stub_release() -> Result<()>
{
    for mode in ["prepared", "hot", "repair-survivors", "repair-missing"] {
        run(move |db| {
            Box::pin(async move {
                let original = seed(db, true).await?;
                let repair = original.repair()?;
                if mode != "hot" {
                    delete_dependencies(db, mode == "repair-missing").await?;
                }
                let mut stub = db.ledger.pool.begin().await?;
                sqlx::query("SELECT pg_advisory_xact_lock($1)")
                    .bind(SETTLEMENT_LOCK)
                    .execute(&mut *stub)
                    .await?;
                let release_at = tokio::time::Instant::now() + Duration::from_secs(3);
                tokio::time::timeout_at(release_at, async {
                    if mode == "prepared" {
                        ensure!(
                            db.ledger
                                .save_compact_prepared(
                                    "prepared",
                                    &original.record,
                                    &original.template,
                                    &original.balances,
                                    0,
                                    original.expires
                                )
                                .await?
                        );
                    } else {
                        ensure!(
                            save(
                                db,
                                &original,
                                original.expires,
                                if mode == "hot" { None } else { Some(&repair) }
                            )
                            .await?
                                == IssuedJobSave::Saved
                        );
                        ensure!(db.ledger.job("child").await? == Some(child(original.expires)));
                    }
                    let stored = db
                        .ledger
                        .compact_prepared("prepared")
                        .await?
                        .context("missing durable prepared row or blobs")?;
                    ensure!(
                        stored.record == original.record
                            && stored.original_expires_at_ms == original.expires
                            && stored.template == template()
                    );
                    let mut probe = db.ledger.pool.begin().await?;
                    let held: bool = sqlx::query_scalar("SELECT NOT pg_try_advisory_xact_lock($1)")
                        .bind(SETTLEMENT_LOCK)
                        .fetch_one(&mut *probe)
                        .await?;
                    ensure!(held, "stub released before exact durable verification");
                    probe.rollback().await?;
                    Ok::<_, anyhow::Error>(())
                })
                .await
                .context("direct persistence waited for settlement stub")??;
                tokio::time::sleep_until(release_at).await;
                stub.rollback().await?;
                Ok(())
            })
        })
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn ordinary_authority_updates_wait_for_cold_repair_commit() -> Result<()> {
    for mutation in [
        "payout_revision=1",
        "fatal_error='halt'",
        "config_fingerprint=NULL",
        "config_fingerprint='rotated'",
    ] {
        run(move |db| {
            Box::pin(async move {
                let original = seed(db, true).await?;
                db.ledger
                    .configure("original", &original.record.signer_keys)
                    .await?;
                delete_dependencies(db, false).await?;
                gate_insert(db, true).await?;
                let mut gate = db.ledger.pool.begin().await?;
                sqlx::query("SELECT pg_advisory_xact_lock($1)")
                    .bind(TEST_GATE)
                    .execute(&mut *gate)
                    .await?;
                let gate_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                    .fetch_one(&mut *gate)
                    .await?;
                let mut saving =
                    spawn_save(db.ledger.clone(), original.repair()?, original.expires);
                let writer_pid = blocked_query(db, gate_pid, "INSERT INTO qbit_prism_jobs").await?;
                let pool = db.ledger.pool.clone();
                // An authority writer as the server makes one: the row FOR UPDATE, then
                // the UPDATE (a bare non-key UPDATE is forbidden and passes a KEY SHARE
                // job fence by design, #479).
                let mut updating = Running(tokio::spawn(async move {
                    let mut tx = pool.begin().await?;
                    sqlx::query(
                        "SELECT singleton FROM qbit_prism_cluster WHERE singleton FOR UPDATE",
                    )
                    .execute(&mut *tx)
                    .await?;
                    sqlx::query(&format!(
                        "UPDATE qbit_prism_cluster SET {mutation} WHERE singleton"
                    ))
                    .execute(&mut *tx)
                    .await?;
                    tx.commit().await
                }));
                blocked_query(db, writer_pid, "SELECT singleton FROM qbit_prism_cluster").await?;
                ensure!(db.ledger.job("child").await?.is_none());
                gate.rollback().await?;
                ensure!((&mut saving.0).await?? == IssuedJobSave::Saved);
                (&mut updating.0).await??;
                ensure!(db.ledger.job("child").await? == Some(child(original.expires)));
                let before = snapshot(db).await?;
                ensure!(
                    save(db, &original, original.expires, Some(&original.repair()?))
                        .await
                        .is_err()
                );
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
async fn large_template_and_canonical_balances_match_the_existing_encoding_exactly() -> Result<()> {
    run(|db| Box::pin(async move {
        let number = "18446744073709551616001";
        let exact = format!(r#"{{"height":101,"number":{number},"previousblockhash":"{}","rate":0.000000000000000000001,"transactions":[{{"data":"{}"}}]}}"#,
            "ab".repeat(32), "ab".repeat(1_100_000));
        let value: Value = serde_json::from_str(&exact)?;
        let template = PreparedTemplate::encode(&value)?;
        ensure!(template.sha256() == hex::encode(Sha256::digest(exact.as_bytes())));
        let mut balances = balances();
        balances.push(CarryForwardBalance { recipient_id:"a".into(), order_key:"a".into(),
            p2mr_program_hex:"00".repeat(32), balance_sats:9 });
        let record = record(&template, &balances, false);
        let expires = db.expires().await?;
        db.ledger.save_compact_prepared("prepared", &record, &template, &balances, 0, expires).await?;
        let original = Original { record, template, balances, expires };
        let original_payload = db.ledger.job("prepared").await?.unwrap();
        let canonical: Vec<u8> = sqlx::query_scalar("SELECT balances FROM qbit_prism_balance_snapshots")
            .fetch_one(&db.ledger.pool).await?;
        let repair = original.repair()?;
        delete_dependencies(db, true).await?;
        ensure!(save(db, &original, expires, Some(&repair)).await? == IssuedJobSave::Saved);
        let (template_bytes,balance_bytes): (Vec<u8>,Vec<u8>) = sqlx::query_as("SELECT t.template_bytes,b.balances FROM qbit_prism_templates t CROSS JOIN qbit_prism_balance_snapshots b")
            .fetch_one(&db.ledger.pool).await?;
        ensure!(template_bytes == exact.as_bytes() && balance_bytes == canonical);
        ensure!(template_bytes.len() > 2_000_000 && serde_json::to_vec(&original_payload)?.len() < 10_000);
        ensure!(db.ledger.job("prepared").await? == Some(original_payload));
        // A permuted set must retain the same canonical bytes on exact retry.
        let mut reversed = original.balances.clone(); reversed.reverse();
        let repair = CompactRepair::encode(&original.record, &original.template, &reversed, expires)?;
        let before = snapshot(db).await?;
        ensure!(save(db, &original, expires, Some(&repair)).await? == IssuedJobSave::Saved);
        ensure!(snapshot(db).await? == before);
        Ok(())
    })).await
}

#[tokio::test]
async fn legacy_inline_calls_and_cross_kind_errors_keep_their_existing_contract() -> Result<()> {
    run(|db| Box::pin(async move {
        let original = seed(db, false).await?;
        let inline = json!({"snapshot":{"payout_revision":0},"template":{"previousblockhash":original.record.parent_hash},"coinbase_suffix":"original-inline"});
        db.ledger.save_job("legacy", &inline, 0, &original.record.parent_hash, 60).await?;
        let expiry = original.expires;
        let payload = json!({"prepared_key":"legacy","expires_at_ms":expiry});
        ensure!(db.ledger.save_issued_job("legacy-child", &payload, 0, &original.record.parent_hash,
            expiry, PreparedDependency { key:"legacy",original_revision:0,parent:&original.record.parent_hash },
            Some(&inline)).await? == IssuedJobSave::Saved);
        ensure!(db.ledger.job("legacy").await? == Some(inline.clone()));
        for key in ["legacy", "legacy-child"] {
            let before = snapshot(db).await?;
            let payload = json!({"prepared_key":key,"expires_at_ms":expiry});
            let dependency = CompactDependency { key, ..original.dependency() };
            let error = db.ledger.save_issued_job_compact("new-child", &payload, 0,
                &original.record.parent_hash, expiry, dependency, None).await.unwrap_err();
            ensure!(error.to_string().contains("immutable compact prepared dependency conflict"));
            ensure!(snapshot(db).await? == before);
        }
        let before = snapshot(db).await?;
        ensure!(db.ledger.save_issued_job("wrong-inline-repair", &child(expiry), 0,
            &original.record.parent_hash, expiry,
            PreparedDependency { key:"prepared",original_revision:0,parent:&original.record.parent_hash },
            Some(&inline)).await.is_err());
        ensure!(snapshot(db).await? == before);
        Ok(())
    })).await
}
