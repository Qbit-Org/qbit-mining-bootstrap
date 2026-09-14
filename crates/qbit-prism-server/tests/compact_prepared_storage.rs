//! Additive storage qualification, with no compact runtime writer enabled.
//! Uses only the disposable database supplied by test/prism-native-tests.sh.
use anyhow::{ensure, Context, Result};
use futures_util::future::LocalBoxFuture;
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{CarryForwardBalance, FanoutFeeRatePolicy, PayoutPolicy, SettlementModeConfig};
use qbit_prism_server::ledger::{
    CandidateCtv, CompactPrepared, IssuedJobSave, Ledger, PreparedAuditHashes, PreparedDependency,
    PreparedTemplate, ShareRange, SignerKeys, WindowError, WindowRef,
};
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use std::time::Duration;
use tokio::time::{sleep, timeout};

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const SETTLEMENT_LOCK: i64 = 0x505249534d000003;
const TEST_GATE: i64 = 0x27300001;

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
        let schema = format!("prism_compact_storage_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut url = url::Url::parse(&raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        let ledger = Ledger::connect(url.as_str(), "compact-storage-test".into(), 5, true).await?;
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

#[tokio::test]
async fn empty_and_nonempty_prepared_records_round_trip_original_inputs_and_exact_blobs(
) -> Result<()> {
    run(|db| Box::pin(async move {
        seed_endpoints(db).await?;
        // JSON numbers must survive without f64 rounding or JSONB normalization.
        let exact = format!(r#"{{"height":101,"number":18446744073709551616001,"previousblockhash":"{}","rate":0.000000000000000000001,"transactions":[]}}"#, "ab".repeat(32));
        let value: Value = serde_json::from_str(&exact)?;
        let template = PreparedTemplate::encode(&value)?;
        ensure!(template.sha256() == hex::encode(Sha256::digest(exact.as_bytes())));
        for nonempty in [false, true] {
            let balances = if nonempty { balances() } else { vec![] };
            let mut record = record(&template, &balances, nonempty);
            if nonempty {
                let fee = FanoutFeeRatePolicy::new(321, 12_000);
                record.fee = Some(fee);
                record.ctv = Some(CandidateCtv { direct_floor_sats: 777, settlement_config: SettlementModeConfig::default(), fanout_fee_policy: Some(fee) });
            }
            let key = format!("prepared:{nonempty}");
            let expires = db.expires().await?;
            ensure!(db.ledger.save_compact_prepared(&key, &record, &template, &balances, 0, expires).await?);
            let stored = db.ledger.compact_prepared(&key).await?.context("not found")?;
            ensure!(stored.record == record && stored.template == value && stored.expires_at_ms == expires && stored.original_expires_at_ms == expires);
            let mut sorted = balances.clone(); sorted.reverse();
            ensure!(stored.prior_balances == sorted);
            let row = sqlx::query("SELECT j.payload,j.window_first_share_seq,j.window_last_share_seq,j.window_share_count,t.template_bytes,b.balances FROM qbit_prism_jobs j JOIN qbit_prism_templates t USING(template_sha256) JOIN qbit_prism_balance_snapshots b ON b.prior_balances_digest=j.window_prior_balances_sha256 WHERE j.job_id=$1")
                .bind(&key).fetch_one(&db.ledger.pool).await?;
            ensure!(row.try_get::<Vec<u8>,_>("template_bytes")? == exact.as_bytes());
            ensure!(row.try_get::<Vec<u8>,_>("balances")? == serde_json::to_vec(&sorted)?);
            let payload: Value = row.try_get("payload")?;
            let mut expected_payload = serde_json::to_value(&record)?;
            expected_payload["original_expires_at_ms"] = json!(expires);
            ensure!(payload == expected_payload);
            for field in ["template", "snapshot", "bundle", "prior_balances"] { ensure!(payload.get(field).is_none()); }
            ensure!(!payload["window"]["shares"].is_array());
            ensure!(row.try_get::<Option<i64>,_>("window_first_share_seq")? == nonempty.then_some(1));
            ensure!(row.try_get::<Option<i64>,_>("window_last_share_seq")? == nonempty.then_some(3));
            ensure!(row.try_get::<Option<i64>,_>("window_share_count")? == nonempty.then_some(2));
        }
        Ok(())
    })).await
}

#[tokio::test]
async fn large_template_stays_out_of_compact_payload() -> Result<()> {
    run(|db| Box::pin(async move {
        let mut value = template();
        value["transactions"] = json!([{"data": "ab".repeat(1_100_000)}]);
        let value_for_encoding = value.clone();
        let template = tokio::task::spawn_blocking(move || PreparedTemplate::encode(&value_for_encoding)).await??;
        let record = record(&template, &[], false);
        db.ledger.save_compact_prepared("large", &record, &template, &[], 0, db.expires().await?).await?;
        let (payload_bytes, template_bytes): (i32, i32) = sqlx::query_as("SELECT octet_length(j.payload::text),octet_length(t.template_bytes) FROM qbit_prism_jobs j JOIN qbit_prism_templates t USING(template_sha256) WHERE j.job_id='large'")
            .fetch_one(&db.ledger.pool).await?;
        ensure!(payload_bytes < 10_000 && template_bytes > 2_000_000, "{payload_bytes} payload bytes / {template_bytes} template bytes");
        ensure!(db.ledger.compact_prepared("large").await?.unwrap().template == value);
        Ok(())
    })).await
}

#[tokio::test]
async fn retries_preserve_original_identity_expiry_and_transaction_fence() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let template = PreparedTemplate::encode(&template())?;
            let record = record(&template, &[], false);
            let expires = db.expires().await?;
            // Model a lost commit acknowledgement by discarding the first result.
            db.ledger
                .save_compact_prepared("retry", &record, &template, &[], 0, expires)
                .await?;
            ensure!(
                !db.ledger
                    .save_compact_prepared("retry", &record, &template, &[], 0, expires)
                    .await?
            );
            let original_payload = db.ledger.job("retry").await?.unwrap();
            let child_expires = expires + 60_000;
            let child = json!({"prepared_key": "retry", "expires_at_ms": child_expires});
            ensure!(
                db.ledger
                    .save_issued_job(
                        "renewing-child",
                        &child,
                        0,
                        &record.parent_hash,
                        child_expires,
                        PreparedDependency {
                            key: "retry",
                            original_revision: 0,
                            parent: &record.parent_hash
                        },
                        None,
                    )
                    .await?
                    == IssuedJobSave::Saved
            );
            let retained = db.ledger.compact_prepared("retry").await?.unwrap();
            ensure!(
                retained.original_expires_at_ms == expires
                    && retained.expires_at_ms >= child_expires
            );
            // A lost-ack retry reconciles even after a real child extended
            // retention, without changing either deadline or the payload.
            ensure!(
                !db.ledger
                    .save_compact_prepared("retry", &record, &template, &[], 0, expires)
                    .await?
            );
            let retried = db.ledger.compact_prepared("retry").await?.unwrap();
            ensure!(
                retried.original_expires_at_ms == expires
                    && retried.expires_at_ms == retained.expires_at_ms
            );
            ensure!(db.ledger.job("retry").await? == Some(original_payload));
            ensure!(db.ledger.job("renewing-child").await? == Some(child));
            let child_column: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
                "SELECT expires_at FROM qbit_prism_jobs WHERE job_id='renewing-child'",
            )
            .fetch_one(&db.ledger.pool)
            .await?;
            ensure!(child_column.timestamp_millis() == child_expires);
            for changed in [expires - 1, expires + 1] {
                let error = db
                    .ledger
                    .save_compact_prepared("retry", &record, &template, &[], 0, changed)
                    .await
                    .unwrap_err();
                ensure!(error
                    .to_string()
                    .contains("immutable compact prepared conflict"));
            }
            // The immutable baseline also authenticates retention metadata.
            // Keep the damaged column live so lookup cannot classify it a miss.
            sqlx::query("UPDATE qbit_prism_jobs SET expires_at=$1 WHERE job_id='retry'")
                .bind(chrono::DateTime::<chrono::Utc>::from_timestamp_millis(expires - 1).unwrap())
                .execute(&db.ledger.pool)
                .await?;
            ensure!(db
                .ledger
                .compact_prepared("retry")
                .await
                .unwrap_err()
                .to_string()
                .contains("retention precedes original expiry"));
            ensure!(db
                .ledger
                .save_compact_prepared("retry", &record, &template, &[], 0, expires)
                .await
                .is_err());
            sqlx::query("UPDATE qbit_prism_jobs SET expires_at=$1 WHERE job_id='retry'")
                .bind(
                    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(retained.expires_at_ms)
                        .unwrap(),
                )
                .execute(&db.ledger.pool)
                .await?;
            let mut other = record.clone();
            other.generation += 1;
            ensure!(db
                .ledger
                .save_compact_prepared("retry", &other, &template, &[], 0, expires)
                .await
                .is_err());
            sqlx::query("UPDATE qbit_prism_cluster SET payout_revision=1 WHERE singleton")
                .execute(&db.ledger.pool)
                .await?;
            ensure!(db
                .ledger
                .save_compact_prepared("stale", &record, &template, &[], 0, expires)
                .await
                .is_err());
            // A current transaction fence must not overwrite original economics.
            ensure!(
                db.ledger
                    .save_compact_prepared("original", &record, &template, &[], 1, expires)
                    .await?
            );
            let stored = db.ledger.compact_prepared("original").await?.unwrap();
            ensure!(
                stored.record.payout_revision == 0
                    && stored.expires_at_ms == expires
                    && stored.original_expires_at_ms == expires
            );
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_prism_jobs")
                .fetch_one(&db.ledger.pool)
                .await?;
            ensure!(count == 3);
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn immutable_blob_conflicts_and_wrong_balance_digest_roll_back_atomically() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let template = PreparedTemplate::encode(&template())?;
            let record = record(&template, &[], false);
            let expires = db.expires().await?;
            sqlx::query("INSERT INTO qbit_prism_templates VALUES($1,$2)")
                .bind(template.sha256())
                .bind(b"different".as_slice())
                .execute(&db.ledger.pool)
                .await?;
            let error = db
                .ledger
                .save_compact_prepared("conflict", &record, &template, &[], 0, expires)
                .await
                .unwrap_err();
            ensure!(error
                .to_string()
                .contains("immutable prepared template conflict"));
            sqlx::query("DELETE FROM qbit_prism_templates")
                .execute(&db.ledger.pool)
                .await?;
            assert_empty(db).await?;
            // Same semantic digest, different bytes: ON CONFLICT alone is insufficient.
            sqlx::query("INSERT INTO qbit_prism_balance_snapshots VALUES($1,$2)")
                .bind(hex::encode(record.window.prior_balances_digest))
                .bind(b"[ ]".as_slice())
                .execute(&db.ledger.pool)
                .await?;
            let error = db
                .ledger
                .save_compact_prepared("conflict", &record, &template, &[], 0, expires)
                .await
                .unwrap_err();
            ensure!(matches!(
                error.downcast_ref::<WindowError>(),
                Some(WindowError::Decode(_))
            ));
            sqlx::query("DELETE FROM qbit_prism_balance_snapshots")
                .execute(&db.ledger.pool)
                .await?;
            assert_empty(db).await?;
            let error = db
                .ledger
                .save_compact_prepared(
                    "wrong-balances",
                    &record,
                    &template,
                    &balances(),
                    0,
                    expires,
                )
                .await
                .unwrap_err();
            ensure!(error
                .to_string()
                .contains("prepared balance digest mismatch"));
            assert_empty(db).await?;
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn compact_reader_and_retry_reject_payload_column_disagreement() -> Result<()> {
    run(|db| Box::pin(async move {
        seed_endpoints(db).await?;
        let template = PreparedTemplate::encode(&template())?;
        let record = record(&template, &[], true);
        let expires = db.expires().await?;
        // Each mutation still satisfies the SQL three-state check.
        for assignment in [
            "parent_hash=repeat('aa',32)", "payout_revision=1", "window_anchor_ms=2",
            "window_prior_balances_sha256=repeat('00',32)", "window_first_share_seq=2",
            "window_last_share_seq=4", "window_share_count=1", "window_snapshot_sha256=repeat('00',32)",
            "template_sha256=repeat('00',32)", "template_sha256=NULL",
            "window_anchor_ms=NULL,window_prior_balances_sha256=NULL,window_first_share_seq=NULL,window_last_share_seq=NULL,window_share_count=NULL,window_snapshot_sha256=NULL,template_sha256=NULL",
        ] {
            db.ledger.save_compact_prepared("columns", &record, &template, &[], 0, expires).await?;
            sqlx::query(&format!("UPDATE qbit_prism_jobs SET {assignment} WHERE job_id='columns'")).execute(&db.ledger.pool).await?;
            let error = db.ledger.compact_prepared("columns").await.unwrap_err();
            ensure!(error.to_string().contains("payload/column mismatch"), "{assignment}: {error:#}");
            ensure!(db.ledger.save_compact_prepared("columns", &record, &template, &[], 0, expires).await.is_err(), "retry accepted {assignment}");
            sqlx::query("DELETE FROM qbit_prism_jobs").execute(&db.ledger.pool).await?;
        }
        Ok(())
    })).await
}

#[tokio::test]
async fn missing_or_corrupt_blobs_are_errors_not_misses() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let value = template();
            let template = PreparedTemplate::encode(&value)?;
            let balances = balances();
            let record = record(&template, &balances, false);
            let expires = db.expires().await?;
            db.ledger
                .save_compact_prepared("blobs", &record, &template, &balances, 0, expires)
                .await?;
            sqlx::query("DELETE FROM qbit_prism_templates")
                .execute(&db.ledger.pool)
                .await?;
            ensure!(db
                .ledger
                .compact_prepared("blobs")
                .await
                .unwrap_err()
                .to_string()
                .contains("template blob missing"));
            sqlx::query("INSERT INTO qbit_prism_templates VALUES($1,$2)")
                .bind(template.sha256())
                .bind(b"{}".as_slice())
                .execute(&db.ledger.pool)
                .await?;
            ensure!(db
                .ledger
                .compact_prepared("blobs")
                .await
                .unwrap_err()
                .to_string()
                .contains("template digest mismatch"));
            sqlx::query("DELETE FROM qbit_prism_templates")
                .execute(&db.ledger.pool)
                .await?;
            sqlx::query("INSERT INTO qbit_prism_templates VALUES($1,$2)")
                .bind(template.sha256())
                .bind(serde_json::to_vec(&value)?)
                .execute(&db.ledger.pool)
                .await?;
            sqlx::query("DELETE FROM qbit_prism_balance_snapshots")
                .execute(&db.ledger.pool)
                .await?;
            ensure!(db
                .ledger
                .compact_prepared("blobs")
                .await
                .unwrap_err()
                .to_string()
                .contains("balance snapshot missing"));
            sqlx::query("INSERT INTO qbit_prism_balance_snapshots VALUES($1,$2)")
                .bind(hex::encode(record.window.prior_balances_digest))
                .bind(b"[]".as_slice())
                .execute(&db.ledger.pool)
                .await?;
            ensure!(db
                .ledger
                .compact_prepared("blobs")
                .await
                .unwrap_err()
                .to_string()
                .contains("balance snapshot digest mismatch"));
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn old_inline_callers_remain_unchanged_and_only_explicit_legacy_rows_miss() -> Result<()> {
    run(|db| Box::pin(async move {
        let parent = "ab".repeat(32);
        let inline = json!({"template": template(), "snapshot": {"payout_revision": 0}, "bundle": null});
        db.ledger.save_job("legacy", &inline, 0, &parent, 60).await?;
        ensure!(db.ledger.job("legacy").await? == Some(inline.clone()));
        ensure!(db.ledger.compact_prepared("legacy").await?.is_none());
        let expires = db.expires().await?;
        let child = json!({"prepared_key": "legacy", "expires_at_ms": expires, "extranonce1": "00000001"});
        ensure!(db.ledger.save_issued_job("child", &child, 0, &parent, expires, PreparedDependency { key: "legacy", original_revision: 0, parent: &parent }, Some(&inline)).await? == IssuedJobSave::Saved);
        ensure!(db.ledger.job("child").await? == Some(child));
        // The caller must follow the dependency link. Misrouting an issued ID
        // into the typed prepared lookup must remain an error, never a miss.
        ensure!(db.ledger.compact_prepared("child").await.unwrap_err().to_string().contains("invalid compact prepared payload"));
        ensure!(db.ledger.compact_prepared("missing").await?.is_none());
        let template = PreparedTemplate::encode(&template())?;
        let record = record(&template, &[], false);
        db.ledger.save_compact_prepared("modern", &record, &template, &[], 0, expires).await?;
        let original_payload = db.ledger.job("modern").await?.unwrap();
        for mutation in [
            "jsonb_set(payload,'{format_version}','99')", "payload - 'format_version'",
            "payload || '{\"bundle\":null}'::jsonb", "payload || '{\"snapshot\":{}}'::jsonb",
            "jsonb_set(payload,'{window}','null')", "jsonb_set(payload,'{payout_policy,unknown}','1')",
            "payload - 'original_expires_at_ms'", "jsonb_set(payload,'{original_expires_at_ms}','null')",
            "jsonb_set(payload,'{original_expires_at_ms}','1.5')",
            "jsonb_set(payload,'{original_expires_at_ms}','9223372036854775807')",
        ] {
            sqlx::query(&format!("UPDATE qbit_prism_jobs SET payload={mutation} WHERE job_id='modern'")).execute(&db.ledger.pool).await?;
            ensure!(db.ledger.compact_prepared("modern").await.is_err(), "accepted {mutation}");
            sqlx::query("UPDATE qbit_prism_jobs SET payload=$1 WHERE job_id='modern'").bind(&original_payload).execute(&db.ledger.pool).await?;
        }
        // A historically expired reservation may still be retained for a live
        // child. Hydration is an observation, not authority to renew or publish.
        let past_original = db.now_ms().await? - 1;
        sqlx::query("UPDATE qbit_prism_jobs SET payload=jsonb_set(payload,'{original_expires_at_ms}',to_jsonb($1::bigint)) WHERE job_id='modern'")
            .bind(past_original).execute(&db.ledger.pool).await?;
        let retained = db.ledger.compact_prepared("modern").await?.unwrap();
        ensure!(retained.original_expires_at_ms == past_original && retained.expires_at_ms == expires);
        ensure!(db.ledger.save_compact_prepared("modern", &record, &template, &[], 0, past_original).await.unwrap_err().to_string().contains("prepared deadline elapsed"));
        sqlx::query("UPDATE qbit_prism_jobs SET expires_at=clock_timestamp()-interval '1 second' WHERE job_id='modern'").execute(&db.ledger.pool).await?;
        ensure!(db.ledger.compact_prepared("modern").await?.is_none());
        Ok(())
    })).await
}

#[tokio::test]
async fn invalid_references_and_oversize_metadata_publish_nothing() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let template = PreparedTemplate::encode(&template())?;
            let original = record(&template, &[], false);
            let expires = db.expires().await?;
            for case in 0..13 {
                let mut record = original.clone();
                match case {
                    0 => record.format_version += 1,
                    1 => record.template_sha256 = "AB".repeat(32),
                    2 => record.parent_hash = "aa".repeat(32),
                    3 => record.coinbase_suffix_hex = "xyz".into(),
                    4 => record.share_seq = u64::MAX,
                    5 => record.payout_revision = -1,
                    6 => record.fingerprint = "x".repeat(1_000_000),
                    7 => {
                        record.window.shares = Some(ShareRange {
                            first_share_seq: 0,
                            last_share_seq: 1,
                            share_count: 1,
                            snapshot_sha256: [0; 32],
                        });
                    }
                    8 => {
                        record.window.shares = Some(ShareRange {
                            first_share_seq: 1,
                            last_share_seq: 2,
                            share_count: 3,
                            snapshot_sha256: [0; 32],
                        });
                    }
                    9 => {
                        record.audit_hashes = Some(PreparedAuditHashes {
                            audit_bundle_sha256: "aa".repeat(32),
                            coinbase_manifest_sha256: "bb".repeat(32),
                        })
                    }
                    10 => {
                        record = self::record(&template, &[], true);
                        record.audit_hashes.as_mut().unwrap().audit_bundle_sha256 = "BAD".into();
                    }
                    11 => {
                        record.ctv = Some(CandidateCtv {
                            direct_floor_sats: 1,
                            settlement_config: SettlementModeConfig::default(),
                            fanout_fee_policy: Some(FanoutFeeRatePolicy::new(1, 10_000)),
                        })
                    }
                    12 => {
                        record = self::record(&template, &[], true);
                        record.share_seq = 2;
                    }
                    _ => unreachable!(),
                }
                ensure!(
                    db.ledger
                        .save_compact_prepared("invalid", &record, &template, &[], 0, expires)
                        .await
                        .is_err(),
                    "accepted {case}"
                );
                assert_empty(db).await?;
            }
            let nonempty = record(&template, &[], true);
            ensure!(db
                .ledger
                .save_compact_prepared("pruned", &nonempty, &template, &[], 0, expires)
                .await
                .unwrap_err()
                .to_string()
                .contains("prepared share endpoint missing"));
            assert_empty(db).await?;
            // A retained first endpoint does not prove an arbitrary last
            // endpoint was ever captured. Reject before publishing any blob.
            seed_share(db, 1).await?;
            ensure!(db
                .ledger
                .save_compact_prepared("beyond-ledger", &nonempty, &template, &[], 0, expires)
                .await
                .unwrap_err()
                .to_string()
                .contains("prepared share endpoint missing"));
            assert_empty(db).await?;
            seed_share(db, 3).await?;
            ensure!(
                db.ledger
                    .save_compact_prepared("retained", &nonempty, &template, &[], 0, expires)
                    .await?
            );
            ensure!(
                db.ledger
                    .compact_prepared("retained")
                    .await?
                    .unwrap()
                    .record
                    == nonempty
            );
            Ok(())
        })
    })
    .await
}

async fn wait_for_gate(db: &Database, key: i64) -> Result<()> {
    timeout(Duration::from_secs(5), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND objid::bigint=$1)")
                .bind(key & 0xffff_ffff).fetch_one(&db.admin).await?;
            if waiting { return Ok::<_,anyhow::Error>(()); }
            sleep(Duration::from_millis(5)).await;
        }
    }).await.context("writer never reached gate")?
}

async fn gate_insert(db: &Database) -> Result<()> {
    sqlx::raw_sql(&format!("CREATE FUNCTION gate_prepared_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock({TEST_GATE}); RETURN NEW; END; $$; CREATE TRIGGER gate_prepared_insert AFTER INSERT ON qbit_prism_jobs FOR EACH ROW EXECUTE FUNCTION gate_prepared_insert();"))
        .execute(&db.ledger.pool).await?;
    Ok(())
}

#[tokio::test]
async fn cancellation_after_blob_inserts_rolls_back_all_effects() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            gate_insert(db).await?;
            let mut gate = db.admin.begin().await?;
            sqlx::query("SELECT pg_advisory_xact_lock($1)")
                .bind(TEST_GATE)
                .execute(&mut *gate)
                .await?;
            let ledger = db.ledger.clone();
            let expires = db.expires().await?;
            let writer = tokio::spawn(async move {
                let template = PreparedTemplate::encode(&template()).unwrap();
                let record = record(&template, &balances(), false);
                ledger
                    .save_compact_prepared("cancel", &record, &template, &balances(), 0, expires)
                    .await
            });
            wait_for_gate(db, TEST_GATE).await?;
            writer.abort();
            ensure!(writer.await.unwrap_err().is_cancelled());
            gate.rollback().await?;
            // Taking the same lock waits for the cancelled transaction's rollback.
            let mut fence = db.ledger.pool.begin().await?;
            sqlx::query("SELECT pg_advisory_xact_lock($1)")
                .bind(SETTLEMENT_LOCK)
                .execute(&mut *fence)
                .await?;
            fence.rollback().await?;
            assert_empty(db).await?;
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn fixed_expiry_is_checked_after_lock_and_post_insert_waits() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            for after_blobs in [false, true] {
                if after_blobs {
                    gate_insert(db).await?;
                }
                let key = if after_blobs {
                    TEST_GATE
                } else {
                    SETTLEMENT_LOCK
                };
                let mut gate = db.admin.begin().await?;
                sqlx::query("SELECT pg_advisory_xact_lock($1)")
                    .bind(key)
                    .execute(&mut *gate)
                    .await?;
                let expires = db.now_ms().await? + 300;
                let ledger = db.ledger.clone();
                let writer = tokio::spawn(async move {
                    let template = PreparedTemplate::encode(&template()).unwrap();
                    let record = record(&template, &[], false);
                    ledger
                        .save_compact_prepared("deadline", &record, &template, &[], 0, expires)
                        .await
                });
                wait_for_gate(db, key).await?;
                while db.now_ms().await? <= expires {
                    sleep(Duration::from_millis(5)).await;
                }
                gate.rollback().await?;
                ensure!(writer
                    .await?
                    .unwrap_err()
                    .to_string()
                    .contains("prepared deadline elapsed"));
                assert_empty(db).await?;
            }
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn concurrent_retries_create_one_immutable_dependency() -> Result<()> {
    run(|db| Box::pin(async move {
        let template = PreparedTemplate::encode(&template())?;
        let balances = balances();
        let record = record(&template, &balances, false);
        let expires = db.expires().await?;
        let saves = (0..8).map(|_| db.ledger.save_compact_prepared("concurrent", &record, &template, &balances, 0, expires));
        let results = futures_util::future::join_all(saves).await;
        let mut inserted = 0;
        for result in results { inserted += usize::from(result?); }
        ensure!(inserted == 1);
        let counts: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM qbit_prism_jobs),(SELECT count(*) FROM qbit_prism_templates),(SELECT count(*) FROM qbit_prism_balance_snapshots)")
            .fetch_one(&db.ledger.pool).await?;
        ensure!(counts == (1, 1, 1));
        let stored = db.ledger.compact_prepared("concurrent").await?.unwrap();
        ensure!(stored.expires_at_ms == expires && stored.record == record);
        Ok(())
    })).await
}

#[tokio::test]
async fn configured_writer_refuses_a_reset_or_rotated_cluster_pin() -> Result<()> {
    run(|db| Box::pin(async move {
        let template = PreparedTemplate::encode(&template())?;
        let record = record(&template, &[], false);
        let expires = db.expires().await?;
        db.ledger.configure("original-config", &record.signer_keys).await?;
        db.ledger.save_compact_prepared("original", &record, &template, &[], 0, expires).await?;
        for changed in [None, Some("replacement-config")] {
            sqlx::query("UPDATE qbit_prism_cluster SET config_fingerprint=$1 WHERE singleton")
                .bind(changed).execute(&db.ledger.pool).await?;
            let error = db.ledger.save_compact_prepared("stale-frontend", &record, &template, &[], 0, expires).await.unwrap_err();
            ensure!(error.to_string().contains("cluster configuration fingerprint differs"));
            ensure!(db.ledger.compact_prepared("stale-frontend").await?.is_none());
        }
        let counts: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM qbit_prism_jobs),(SELECT count(*) FROM qbit_prism_templates),(SELECT count(*) FROM qbit_prism_balance_snapshots)")
            .fetch_one(&db.ledger.pool).await?;
        ensure!(counts == (1, 1, 1));
        Ok(())
    })).await
}
