//! Isolated B273 schema/reader tests; these do not enable compact runtime jobs.
//! Run through test/prism-native-tests.sh cargo-args --locked -p qbit-prism-server
//! --test window_reference. The PR319 SQL coexistence test is explicitly ignored.
use anyhow::{ensure, Context, Result};
use futures_util::future::LocalBoxFuture;
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{AcceptedShare, CarryForwardBalance, FoundBlock, PayoutPolicy};
use qbit_prism_server::ledger::{BalanceSource, Ledger, ShareRange, WindowError, WindowRef};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::{sync::Mutex, time::Duration};
use tokio::time::{sleep, timeout};

#[allow(dead_code)]
#[path = "support/window_fixture.rs"]
mod window_fixture;

#[path = "window_reference/payout_state.rs"]
mod payout_state;

const ANCHOR: i64 = 1_700_000_000_000;
const MIGRATION_008: &str = include_str!("../migrations/008_prepared_window_reference.sql");

struct Database {
    admin: PgPool,
    pool: PgPool,
    url: String,
    schema: String,
    ledgers: Mutex<Vec<PgPool>>,
}

impl Database {
    async fn open() -> Result<Option<Self>> {
        let raw = match std::env::var("PRISM_TEST_DATABASE_URL") {
            Ok(raw) if !raw.trim().is_empty() => raw,
            Ok(_) | Err(std::env::VarError::NotPresent) => {
                ensure!(std::env::var("PRISM_TEST_REQUIRE_INTEGRATION").as_deref() != Ok("1")
                    && std::env::var("GITHUB_JOB").as_deref() != Ok("prism-native-postgres"),
                    "window_reference requires PRISM_TEST_DATABASE_URL in the PostgreSQL integration job");
                eprintln!("SKIPPED window_reference: set disposable PRISM_TEST_DATABASE_URL");
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        let admin = PgPool::connect(&raw).await?;
        let schema = format!("prism_window_ref_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut url = url::Url::parse(&raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(url.as_str())
            .await?;
        // Build the actual pre-008 schema, without temporarily installing 008
        // and then trying to reverse-engineer its predecessor by dropping fields.
        sqlx::raw_sql(include_str!("../../qbit-prism/sql/001_share_ledger.sql"))
            .execute(&pool)
            .await?;
        let mut tx = pool.begin().await?;
        for migration in [
            include_str!("../migrations/002_multi_instance.sql"),
            include_str!("../migrations/003_2x_compatibility.sql"),
            include_str!("../migrations/004_cpfp_retired_funding.sql"),
            include_str!("../migrations/005_candidate_dispatch.sql"),
        ] {
            sqlx::raw_sql(migration).execute(&mut *tx).await?;
        }
        sqlx::raw_sql("CREATE TABLE qbit_prism_schema_migrations(version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT clock_timestamp()); INSERT INTO qbit_prism_schema_migrations(version) VALUES(2),(3),(4),(5)")
            .execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(Some(Self {
            admin,
            pool,
            url: url.to_string(),
            schema,
            ledgers: Mutex::new(Vec::new()),
        }))
    }

    async fn ledger(&self) -> Result<Ledger> {
        let ledger = Ledger::connect(&self.url, "window-reference".into(), 4, true).await?;
        self.ledgers.lock().unwrap().push(ledger.pool.clone());
        Ok(ledger)
    }

    async fn close(self) -> Result<()> {
        let ledgers = self.ledgers.into_inner().unwrap();
        for pool in ledgers {
            pool.close().await;
        }
        self.pool.close().await;
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

fn share(seq: u64, filtered: bool) -> AcceptedShare {
    AcceptedShare {
        share_seq: seq,
        share_id: format!("share-{seq}"),
        miner_id: "miner".into(),
        order_key: "miner".into(),
        p2mr_program_hex: "11".repeat(32),
        share_difficulty: 1,
        network_difficulty: 1000,
        template_height: 100,
        job_id: "job".into(),
        job_issued_at_ms: ANCHOR + if filtered && seq % 13 == 0 { 1 } else { -1 },
        accepted_at_ms: ANCHOR + i64::from(filtered && seq % 11 == 0),
        ntime: 100,
        credit_policy: (seq % 2 == 0).then(|| "stale-grace".into()),
    }
}

async fn seed_shares(pool: &PgPool, first: i64, last: i64, filtered: bool) -> Result<()> {
    sqlx::query("INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,credit_policy,accepted,reject_reason,writer_id,writer_epoch)
        SELECT i,'share-'||i,'miner','miner',decode(repeat('11',32),'hex'),1,1000,100,'job',
            to_timestamp(($4+CASE WHEN $3 AND i%13=0 THEN 1 ELSE -1 END)::double precision/1000),100,
            to_timestamp(($4+CASE WHEN $3 AND i%11=0 THEN 1 ELSE 0 END)::double precision/1000),
            CASE WHEN i%2=0 THEN 'stale-grace' END,NOT $3 OR i%7<>0,
            CASE WHEN $3 AND i%7=0 THEN 'stale-job' END,'window-ref-test',0
        FROM generate_series($1::bigint,$2::bigint) AS g(i)")
        .bind(first).bind(last).bind(filtered).bind(ANCHOR).execute(pool).await?;
    Ok(())
}

struct HashWriter(Sha256);
impl std::io::Write for HashWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn reference(shares: &[AcceptedShare], balances: &[CarryForwardBalance]) -> WindowRef {
    let mut hash = HashWriter(Sha256::new());
    serde_json::to_writer(&mut hash, shares).unwrap();
    WindowRef {
        anchor_ms: ANCHOR,
        prior_balances_digest: qbit_prism::prior_balances_digest(balances),
        shares: shares.first().map(|first| ShareRange {
            first_share_seq: first.share_seq,
            last_share_seq: shares.last().unwrap().share_seq,
            share_count: shares.len() as u64,
            snapshot_sha256: hash.0.finalize().into(),
        }),
    }
}

async fn stored_balances(pool: &PgPool, balances: &[CarryForwardBalance]) -> Result<()> {
    sqlx::query(
        "INSERT INTO qbit_prism_balance_snapshots(prior_balances_digest,balances) VALUES($1,$2)",
    )
    .bind(hex::encode(qbit_prism::prior_balances_digest(balances)))
    .bind(serde_json::to_vec(balances)?)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test]
async fn ascending_pages_match_native_rows_and_filter_gaps() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let ledger = db.ledger().await?;
            seed_shares(&db.pool, 1, 6000, true).await?;
            let expected: Vec<_> = (1..=6000)
                .filter(|i| i % 7 != 0 && i % 11 != 0 && i % 13 != 0)
                .map(|i| share(i, true))
                .collect();
            // Also cover >4096 accepted rows, an exact page boundary, and a singleton.
            for shares in [&expected[..], &expected[..4096], &expected[..1]] {
                let result = ledger
                    .read_window(&reference(shares, &[]), BalanceSource::Current)
                    .await?;
                ensure!(result.shares == shares, "native window rows differ");
            }
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn as_issued_balances_and_signed_outputs_survive_current_revision_change() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let ledger = db.ledger().await?;
            seed_shares(&db.pool, 1, 3, false).await?;
            let shares: Vec<_> = (1..=3).map(|i| share(i, false)).collect();
            let original = vec![CarryForwardBalance {
                recipient_id: "prior".into(),
                order_key: "prior".into(),
                p2mr_program_hex: "22".repeat(32),
                balance_sats: 12345,
            }];
            stored_balances(&db.pool, &original).await?;
            let window = reference(&shares, &original);
            sqlx::query("INSERT INTO qbit_payout_carry_forward_current(miner_id,payout_order_key,p2mr_program,balance_sats,active_row_count) VALUES('prior','prior',decode(repeat('22',32),'hex'),12345,1)")
                .execute(&db.pool).await?;
            ensure!(ledger.read_window(&window, BalanceSource::Current).await?.prior_balances == original,
                "original balances did not match the current view before replacement");
            let manifest = ManifestSigningKey::from_seed_hex(&"01".repeat(32))?;
            let ledger_key = ManifestSigningKey::from_seed_hex(&"02".repeat(32))?;
            let found = FoundBlock {
                block_height: 101,
                coinbase_value_sats: 5_000_000_000,
                network_difficulty: 1000,
                anchor_job_issued_at_ms: ANCHOR,
            };
            let before = qbit_prism::build_audit_bundle_body(
                &shares,
                found.clone(),
                original.clone(),
                PayoutPolicy::day_one_default(),
                &manifest,
                &ledger_key,
            )?;
            let mut replacement = db.pool.begin().await?;
            sqlx::raw_sql("UPDATE qbit_prism_cluster SET payout_revision=7; UPDATE qbit_payout_carry_forward_current SET balance_sats=67890")
                .execute(&mut *replacement).await?;
            replacement.commit().await?;
            let after = ledger.read_window(&window, BalanceSource::AsIssued).await?;
            ensure!(
                after.payout_revision == 7 && after.prior_balances == original,
                "issued/current fences mixed"
            );
            let body = qbit_prism::build_audit_bundle_body(
                &after.shares,
                found,
                after.prior_balances,
                PayoutPolicy::day_one_default(),
                &manifest,
                &ledger_key,
            )?;
            ensure!(
                qbit_prism::canonical_audit_bundle_bytes_from_parts(&before, &shares)?
                    == qbit_prism::canonical_audit_bundle_bytes_from_parts(&body, &after.shares)?,
                "original signed audit bytes changed"
            );
            ensure!(
                matches!(
                    ledger.read_window(&window, BalanceSource::Current).await,
                    Err(WindowError::PriorBalancesChanged { .. })
                ),
                "current source hid changed balances"
            );
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn empty_window_never_queries_share_history_and_errors_keep_their_types() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let ledger = db.ledger().await?;
            sqlx::query("ALTER TABLE qbit_share_ledger RENAME TO hidden_history")
                .execute(&db.pool)
                .await?;
            let empty = reference(&[], &[]);
            ensure!(
                ledger
                    .read_window(&empty, BalanceSource::Current)
                    .await?
                    .shares
                    .is_empty(),
                "empty range read shares"
            );
            ensure!(
                matches!(
                    ledger.read_window(&empty, BalanceSource::AsIssued).await,
                    Err(WindowError::BalanceSnapshotMissing { .. })
                ),
                "missing balance blob was not an error"
            );
            stored_balances(&db.pool, &[]).await?;
            ensure!(
                ledger
                    .read_window(&empty, BalanceSource::AsIssued)
                    .await?
                    .shares
                    .is_empty(),
                "as-issued empty read"
            );
            let mut missing = reference(&[share(1, false)], &[]);
            ensure!(
                matches!(
                    ledger.read_window(&missing, BalanceSource::Current).await,
                    Err(WindowError::Database(_))
                ),
                "missing table lost database error"
            );
            missing.prior_balances_digest = [1; 32];
            ensure!(
                matches!(
                    ledger.read_window(&missing, BalanceSource::Current).await,
                    Err(WindowError::PriorBalancesChanged { .. })
                ),
                "range queried before balances"
            );
            for (key, bytes) in [
                ("01".repeat(32), b"not-json".to_vec()),
                ("02".repeat(32), b"[]".to_vec()),
            ] {
                sqlx::query("INSERT INTO qbit_prism_balance_snapshots VALUES($1,$2)")
                    .bind(&key)
                    .bind(bytes)
                    .execute(&db.pool)
                    .await?;
                let bad = WindowRef {
                    prior_balances_digest: hex::decode(key)?.try_into().unwrap(),
                    ..missing
                };
                ensure!(
                    matches!(
                        ledger.read_window(&bad, BalanceSource::AsIssued).await,
                        Err(WindowError::Decode(_))
                    ),
                    "corruption did not precede range read"
                );
            }
            sqlx::query("INSERT INTO qbit_payout_carry_forward_current(miner_id,payout_order_key,p2mr_program,balance_sats,active_row_count) VALUES('overflow','overflow',decode(repeat('22',32),'hex'),power(10::numeric,50),1)")
                .execute(&db.pool).await?;
            ensure!(matches!(ledger.read_window(&empty, BalanceSource::Current).await,
                Err(WindowError::Decode(_))), "unrepresentable balance lost decode error");
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn missing_range_count_and_digest_fail_without_partial_windows() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let ledger = db.ledger().await?;
            seed_shares(&db.pool, 1, 1, false).await?;
            seed_shares(&db.pool, 3, 5, false).await?;
            let expected: Vec<_> = (1..=5).map(|i| share(i, false)).collect();
            let window = reference(&expected, &[]);
            ensure!(
                matches!(
                    ledger.read_window(&window, BalanceSource::Current).await,
                    Err(WindowError::Incomplete {
                        expected: 5,
                        got: 4
                    })
                ),
                "interior gap not rejected"
            );
            let mut missing = window;
            missing.shares.as_mut().unwrap().last_share_seq = 6;
            ensure!(
                matches!(
                    ledger.read_window(&missing, BalanceSource::Current).await,
                    Err(WindowError::Incomplete { got: 0, .. })
                ),
                "endpoint probe did not fail first"
            );
            let mut too_many = window;
            too_many.shares.as_mut().unwrap().share_count = 3;
            ensure!(
                matches!(
                    ledger.read_window(&too_many, BalanceSource::Current).await,
                    Err(WindowError::Incomplete {
                        expected: 3,
                        got: 4
                    })
                ),
                "extra rows not rejected"
            );
            let mut mismatch = window;
            mismatch.shares.as_mut().unwrap().share_count = 4;
            ensure!(
                matches!(
                    ledger.read_window(&mismatch, BalanceSource::Current).await,
                    Err(WindowError::SnapshotDigestMismatch { .. })
                ),
                "wrong native digest not rejected"
            );
            sqlx::query("INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,credit_policy,writer_id,writer_epoch) SELECT 6,'overflow',miner_id,payout_order_key,p2mr_program,power(10::numeric,50),network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,credit_policy,writer_id,writer_epoch FROM qbit_share_ledger WHERE share_seq=1")
                .execute(&db.pool).await?;
            ensure!(matches!(ledger.read_window(&reference(&[share(6, false)], &[]), BalanceSource::Current).await,
                Err(WindowError::Decode(_))), "unrepresentable share lost decode error");
            Ok(())
        })
    })
    .await
}

async fn wait_for_endpoint(pool: &PgPool, schema: &str) -> Result<()> {
    timeout(Duration::from_secs(5), async {
        loop {
            let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'SELECT EXISTS(SELECT 1 FROM qbit_share_ledger%' AND pid IN (SELECT pid FROM pg_locks WHERE relation=$1::regclass))")
                .bind(format!("{schema}.qbit_share_ledger")).fetch_one(pool).await?;
            if blocked { return Ok::<_, anyhow::Error>(()); }
            sleep(Duration::from_millis(10)).await;
        }
    }).await.context("window reader never reached endpoint probe")?
}

#[tokio::test]
async fn revision_balances_and_pages_use_one_repeatable_read_snapshot() -> Result<()> {
    run(|db| Box::pin(async move {
        let ledger = db.ledger().await?;
        seed_shares(&db.pool, 1, 1, false).await?;
        seed_shares(&db.pool, 3, 5000, false).await?;
        let expected: Vec<_> = (1..=5000).filter(|i| *i != 2).map(|i| share(i, false)).collect();
        let window = reference(&expected, &[]);
        let original_revision = ledger.payout_revision().await?;
        let mut lock = db.pool.begin().await?;
        sqlx::query("LOCK TABLE qbit_share_ledger IN ACCESS EXCLUSIVE MODE").execute(&mut *lock).await?;
        let reader = tokio::spawn(async move { ledger.read_window(&window, BalanceSource::Current).await });
        wait_for_endpoint(&db.admin, &db.schema).await?;
        // The locking transaction inserts a previously absent interior row and
        // changes the fence/economics before letting the paged reader proceed.
        let row = share(2, false);
        sqlx::query("INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,credit_policy,writer_id,writer_epoch) VALUES(2,'share-2','miner','miner',decode(repeat('11',32),'hex'),1,1000,100,'job',to_timestamp($1::double precision/1000),100,to_timestamp($2::double precision/1000),'stale-grace','window-ref-test',0)")
            .bind(row.job_issued_at_ms).bind(row.accepted_at_ms).execute(&mut *lock).await?;
        sqlx::raw_sql("UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1; INSERT INTO qbit_payout_carry_forward_current(miner_id,payout_order_key,p2mr_program,balance_sats,active_row_count) VALUES('prior','prior',decode(repeat('22',32),'hex'),12345,1)")
            .execute(&mut *lock).await?;
        lock.commit().await?;
        let result = timeout(Duration::from_secs(5), reader).await???;
        ensure!(result.payout_revision == original_revision && result.prior_balances.is_empty()
            && result.shares == expected, "reader mixed database snapshots");
        Ok(())
    })).await
}

#[tokio::test]
async fn database_timeout_and_cancellation_return_the_single_connection() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let mut ledger = db.ledger().await?;
            // Keep normal construction, but constrain the reader to this test's
            // single-connection pool to make a leaked transaction observable.
            ledger.pool = db.pool.clone();
            seed_shares(&db.pool, 1, 1, false).await?;
            let window = reference(&[share(1, false)], &[]);
            for cancel in [false, true] {
                sqlx::query(if cancel {
                    "SET statement_timeout='5s'"
                } else {
                    "SET statement_timeout='80ms'"
                })
                .execute(&db.pool)
                .await?;
                let mut lock = db.admin.begin().await?;
                sqlx::query(&format!(
                    "LOCK TABLE {}.qbit_share_ledger IN ACCESS EXCLUSIVE MODE",
                    db.schema
                ))
                .execute(&mut *lock)
                .await?;
                if cancel {
                    let source = ledger.clone();
                    let read = tokio::spawn(async move {
                        source.read_window(&window, BalanceSource::Current).await
                    });
                    wait_for_endpoint(&db.admin, &db.schema).await?;
                    read.abort();
                    ensure!(
                        read.await.unwrap_err().is_cancelled(),
                        "reader was not cancelled"
                    );
                } else {
                    let error = ledger
                        .read_window(&window, BalanceSource::Current)
                        .await
                        .unwrap_err();
                    ensure!(
                        matches!(error, WindowError::Database(sqlx::Error::Database(ref error))
                    if error.code().as_deref() == Some("57014")),
                        "statement timeout lost its SQLSTATE: {error}"
                    );
                }
                lock.rollback().await?;
                let mut connection = timeout(Duration::from_secs(2), db.pool.acquire()).await??;
                let read_only: String = sqlx::query_scalar("SHOW transaction_read_only")
                    .fetch_one(&mut *connection)
                    .await?;
                ensure!(
                    read_only == "off",
                    "cancelled read-only transaction leaked into pool"
                );
                drop(connection);
                ensure!(
                    ledger
                        .read_window(&window, BalanceSource::Current)
                        .await?
                        .shares
                        .len()
                        == 1,
                    "reader did not recover"
                );
            }
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn migration_preserves_legacy_payloads_and_applies_missing_008_below_009() -> Result<()> {
    run(|db| Box::pin(async move {
        let original = json!({"extranonce1":"ABCDEF01", "snapshot":{"shares":[{"legacy":true}]}});
        sqlx::query("INSERT INTO qbit_prism_jobs(job_id,instance_id,parent_hash,payout_revision,payload,expires_at) VALUES('legacy','test','parent',0,$1,clock_timestamp()+interval '1 hour')")
            .bind(&original).execute(&db.pool).await?;
        // A higher number is intentionally present before the actual runner.
        sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(9)").execute(&db.pool).await?;
        let _ledger = db.ledger().await?;
        let row: Value = sqlx::query_scalar("SELECT to_jsonb(j) FROM qbit_prism_jobs j WHERE job_id='legacy'").fetch_one(&db.pool).await?;
        ensure!(row["payload"] == original && row["window_anchor_ms"].is_null()
            && row["window_first_share_seq"].is_null() && row["template_sha256"].is_null(), "legacy row was rewritten");
        let _restarted = db.ledger().await?;
        sqlx::raw_sql(MIGRATION_008).execute(&db.pool).await?;
        let versions: Vec<i32> = sqlx::query_scalar("SELECT version FROM qbit_prism_schema_migrations ORDER BY version")
            .fetch_all(&db.pool).await?;
        ensure!(versions == [2,3,4,5,8,9], "migration membership/order wrong: {versions:?}");
        Ok(())
    })).await
}

#[tokio::test]
async fn window_sql_check_accepts_exactly_the_three_null_states() -> Result<()> {
    run(|db| Box::pin(async move {
        let _ledger = db.ledger().await?;
        // Every partial-null combination matters: PostgreSQL CHECK accepts NULL.
        for mask in 0..64 {
            let set = |bit| mask & (1 << bit) != 0;
            let result = sqlx::query("INSERT INTO qbit_prism_jobs(job_id,instance_id,parent_hash,payout_revision,payload,expires_at,window_anchor_ms,window_prior_balances_sha256,window_first_share_seq,window_last_share_seq,window_share_count,window_snapshot_sha256) VALUES($1,'test','parent',0,'{}',clock_timestamp(),$2,$3,$4,$5,$6,$7)")
                .bind(format!("null-state-{mask}")).bind(set(0).then_some(ANCHOR)).bind(set(1).then(|| "00".repeat(32)))
                .bind(set(2).then_some(1i64)).bind(set(3).then_some(3i64)).bind(set(4).then_some(2i64))
                .bind(set(5).then(|| "00".repeat(32))).execute(&db.pool).await;
            ensure!(result.is_ok() == matches!(mask, 0 | 3 | 63), "incorrect null-state {mask}: {result:?}");
        }
        for (column, value) in [("window_first_share_seq","0"), ("window_last_share_seq","0"),
            ("window_share_count","0"), ("window_share_count","4"),
            ("window_snapshot_sha256","repeat('A',64)"), ("window_prior_balances_sha256","'00'"),
            ("template_sha256","repeat('g',64)")] {
            ensure!(sqlx::query(&format!("UPDATE qbit_prism_jobs SET {column}={value} WHERE job_id='null-state-63'"))
                .execute(&db.pool).await.is_err(), "invalid {column} accepted");
        }
        Ok(())
    })).await
}

#[tokio::test]
async fn external_blobs_are_immutable_and_remain_deletable_for_gc() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let _ledger = db.ledger().await?;
            for (table, key, payload) in [
                ("qbit_prism_templates", "template_sha256", "template_bytes"),
                (
                    "qbit_prism_balance_snapshots",
                    "prior_balances_digest",
                    "balances",
                ),
            ] {
                let insert = format!(
                    "INSERT INTO {table}({key},{payload}) VALUES($1,$2) ON CONFLICT DO NOTHING"
                );
                sqlx::query(&insert)
                    .bind("ab".repeat(32))
                    .bind(b"original".to_vec())
                    .execute(&db.pool)
                    .await?;
                let retry = sqlx::query(&insert)
                    .bind("ab".repeat(32))
                    .bind(b"conflict".to_vec())
                    .execute(&db.pool)
                    .await?;
                ensure!(
                    retry.rows_affected() == 0,
                    "immutable retry overwrote original"
                );
                for assignment in [
                    format!("{key}=repeat('cd',32)"),
                    format!("{payload}=decode('00','hex')"),
                ] {
                    let error = sqlx::query(&format!("UPDATE {table} SET {assignment}"))
                        .execute(&db.pool)
                        .await
                        .unwrap_err();
                    ensure!(
                        error.as_database_error().and_then(|e| e.code()).as_deref()
                            == Some("55000"),
                        "immutable update accepted"
                    );
                }
                let bytes: Vec<u8> = sqlx::query_scalar(&format!("SELECT {payload} FROM {table}"))
                    .fetch_one(&db.pool)
                    .await?;
                ensure!(bytes == b"original", "blob content changed");
                ensure!(
                    sqlx::query(&format!("DELETE FROM {table}"))
                        .execute(&db.pool)
                        .await?
                        .rows_affected()
                        == 1,
                    "GC cannot remove blobs"
                );
            }
            Ok(())
        })
    })
    .await
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL and PRISM_TEST_MIGRATION_009 pointing to reviewed PR319 SQL"]
async fn reviewed_009_sql_coexists_with_008_in_both_orders() -> Result<()> {
    require_database()?;
    let path = std::env::var("PRISM_TEST_MIGRATION_009")
        .context("supply the reviewed PR319 migration path")?;
    let migration = std::fs::read_to_string(path)?;
    for nine_first in [true, false] {
        let migration = migration.clone();
        run(move |db| Box::pin(async move {
            if !nine_first { db.ledger().await?; }
            let mut tx = db.pool.begin().await?;
            sqlx::query("SELECT pg_advisory_xact_lock($1)").bind(0x505249534d000001i64).execute(&mut *tx).await?;
            sqlx::raw_sql(&migration).execute(&mut *tx).await?;
            sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(9)").execute(&mut *tx).await?;
            tx.commit().await?;
            db.ledger().await?;
            db.ledger().await?;
            let index: String = sqlx::query_scalar("SELECT indexdef FROM pg_indexes WHERE schemaname=current_schema() AND indexname='qbit_prism_jobs_extranonce1_expiry_idx'")
                .fetch_one(&db.pool).await?;
            ensure!(index.contains("lower(") && index.contains("extranonce1") && index.contains("expires_at"), "009 index changed");
            let versions: Vec<i32> = sqlx::query_scalar("SELECT version FROM qbit_prism_schema_migrations ORDER BY version").fetch_all(&db.pool).await?;
            ensure!(versions == [2,3,4,5,8,9], "008/009 order failed");
            Ok(())
        })).await?;
    }
    Ok(())
}

fn require_database() -> Result<()> {
    ensure!(
        std::env::var("PRISM_TEST_DATABASE_URL").is_ok_and(|raw| !raw.trim().is_empty()),
        "explicit qualification requires disposable PRISM_TEST_DATABASE_URL"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "reader-only 400k/500k PostgreSQL qualification; not a refresh/resume or WAL gate"]
async fn production_shaped_reader_at_400k_and_500k() -> Result<()> {
    require_database()?;
    for count in [400_000, 500_000] {
        run(move |db| {
            Box::pin(async move {
                let ledger = db.ledger().await?;
                let plan = window_fixture::WindowPlan::new(count)?;
                plan.load(&db.pool, "window-reference-scale").await?;
                let expected: Vec<_> = (1..=count).map(|i| plan.share(i)).collect();
                let mut window = reference(&expected, &[]);
                window.anchor_ms = ANCHOR + count as i64 + 1;
                let started = std::time::Instant::now();
                let rebuilt = ledger.read_window(&window, BalanceSource::Current).await?;
                let elapsed = started.elapsed();
                ensure!(
                    rebuilt.shares == expected,
                    "large reader changed native rows"
                );
                println!(
                    "reader_only shares={count} returned_rows={} elapsed_ms={} native_digest={}",
                    rebuilt.shares.len(),
                    elapsed.as_millis(),
                    hex::encode(window.shares.unwrap().snapshot_sha256)
                );
                Ok(())
            })
        })
        .await?;
    }
    Ok(())
}
