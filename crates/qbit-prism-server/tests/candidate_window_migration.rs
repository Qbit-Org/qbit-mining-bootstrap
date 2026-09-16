//! A265's migration 007 and the shared window primitives it adds, against a
//! real PostgreSQL. Nothing here enables the candidate switch: 007 is additive
//! and no runtime path writes its columns yet.
//!
//! Run through test/prism-native-tests.sh cargo-args --locked -p
//! qbit-prism-server --test candidate_window_migration.
use anyhow::{ensure, Context, Result};
use futures_util::future::LocalBoxFuture;
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{
    build_audit_bundle, verify_audit_bundle_with_ledger_public_key, AcceptedShare,
    CarryForwardBalance, FoundBlock, PayoutPolicy,
};
use qbit_prism_server::ledger::{
    probe_share_rows, put_balance_snapshot, read_range_paged, BalanceSource, Candidate, Ledger,
    ShareRange, SignerKeys, Snapshot, WindowError, WindowRef, REQUIRED_SCHEMA_VERSIONS,
};
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

const ANCHOR: i64 = 1_700_000_000_000;
const BASE_SCHEMA: &str = include_str!("../../qbit-prism/sql/001_share_ledger.sql");
/// Every version the membership runner installs except 007, so a database can
/// be built in exactly the pre-007 state the runner then completes.
/// Every native migration except 007, 011 and 012, so a connect applies
/// those three and nothing else. #289's 014, #153's 013, #360's 010 and
/// #321's 006 belong here for the
/// same reason 008 and 009 do: leaving one out makes the connect apply a
/// further migration, and the timestamp assertions then fail for a reason
/// unrelated to 007. 006 matters most, because its own drain check refuses
/// the very rows these tests hand to 007's, so without it the refusal under
/// test never runs. #266's 011 cannot be pre-applied: its lifecycle CHECK
/// names the columns 007 adds, so the runner always applies it after 007,
/// followed by 012's startup fence, and these tests accept all three.
const PRE_007: [(i32, &str); 10] = [
    (2, include_str!("../migrations/002_multi_instance.sql")),
    (3, include_str!("../migrations/003_2x_compatibility.sql")),
    (
        4,
        include_str!("../migrations/004_cpfp_retired_funding.sql"),
    ),
    (5, include_str!("../migrations/005_candidate_dispatch.sql")),
    (6, include_str!("../migrations/006_source_schema.sql")),
    (
        8,
        include_str!("../migrations/008_prepared_window_reference.sql"),
    ),
    (9, include_str!("../migrations/009_wrap_safe_sessions.sql")),
    (
        10,
        include_str!("../migrations/010_fatal_state_recovery.sql"),
    ),
    (
        13,
        include_str!("../migrations/013_share_ledger_index_trim.sql"),
    ),
    (14, include_str!("../migrations/014_policy_transition.sql")),
];
/// The columns 011 adds to the outbox, which the connect that applies 007
/// adds as well.
const OFFER_COLUMNS: [&str; 6] = [
    "proof_observed_at_ms",
    "offer_reserved_at",
    "offer_reserved_by",
    "offered_at_ms",
    "offer_outcome",
    "offer_reply",
];
const WINDOW_COLUMNS: [&str; 7] = [
    "window_anchor_ms",
    "window_prior_balances_sha256",
    "window_first_share_seq",
    "window_last_share_seq",
    "window_share_count",
    "window_snapshot_sha256",
    "block_bytes",
];

struct Database {
    admin: PgPool,
    pool: PgPool,
    url: String,
    schema: String,
    ledgers: Mutex<Vec<PgPool>>,
}

impl Database {
    /// An empty schema. Each test installs exactly the starting state it needs,
    /// because the whole point here is which migrations have already run.
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let admin = PgPool::connect(&raw).await?;
        let schema = format!("prism_candidate_window_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut url = url::Url::parse(&raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        // One connection, and none of `Ledger::connect`'s statement/lock
        // timeouts, so a test can park a reader on a gate for as long as it
        // needs to.
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(url.as_str())
            .await?;
        Ok(Some(Self {
            admin,
            pool,
            url: url.to_string(),
            schema,
            ledgers: Mutex::new(Vec::new()),
        }))
    }

    /// The real schema a post-006 3.x.x database is in before 007 exists:
    /// every other migration applied and recorded, and no version 7 row.
    async fn apply_pre_007(&self) -> Result<()> {
        sqlx::raw_sql(BASE_SCHEMA).execute(&self.pool).await?;
        let mut tx = self.pool.begin().await?;
        sqlx::raw_sql("CREATE TABLE qbit_prism_schema_migrations(version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT clock_timestamp())")
            .execute(&mut *tx).await?;
        for (version, migration) in PRE_007 {
            sqlx::raw_sql(migration).execute(&mut *tx).await?;
            sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES($1)")
                .bind(version)
                .execute(&mut *tx)
                .await?;
        }
        // 006's source row is written by the migration runner, not by its SQL,
        // and every later start refuses a database at 6 without it. Seeding the
        // file alone would therefore fail before 007 is reached.
        sqlx::query("INSERT INTO qbit_prism_migration_source(source_state,prior_schema_version,migrated_by) VALUES('native',5,'candidate-window-migration-test') ON CONFLICT (singleton) DO NOTHING")
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn ledger(&self, id: &str) -> Result<Ledger> {
        let ledger = Ledger::connect(&self.url, id.to_owned(), 4, true).await?;
        self.ledgers.lock().unwrap().push(ledger.pool.clone());
        Ok(ledger)
    }

    async fn versions(&self) -> Result<Vec<i32>> {
        Ok(
            sqlx::query_scalar("SELECT version FROM qbit_prism_schema_migrations ORDER BY version")
                .fetch_all(&self.pool)
                .await?,
        )
    }

    async fn has_column(&self, table: &str, column: &str) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=$1 AND column_name=$2)")
            .bind(table).bind(column).fetch_one(&self.pool).await?)
    }

    async fn close(self) -> Result<()> {
        for pool in self.ledgers.into_inner().unwrap() {
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

async fn outbox_columns(pool: &PgPool) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar("SELECT column_name::text FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='qbit_block_candidate_outbox' ORDER BY column_name")
        .fetch_all(pool).await?)
}

fn keys() -> (ManifestSigningKey, ManifestSigningKey) {
    (
        ManifestSigningKey::from_seed_hex(&"42".repeat(32)).unwrap(),
        ManifestSigningKey::from_seed_hex(&"43".repeat(32)).unwrap(),
    )
}

fn signer_keys() -> SignerKeys {
    let (manifest_key, ledger_key) = keys();
    SignerKeys::of(&manifest_key, &ledger_key)
}

fn appended_share(id: u64) -> AcceptedShare {
    AcceptedShare {
        share_seq: 0,
        share_id: format!("worker:{id:064x}"),
        miner_id: "miner".into(),
        order_key: "miner".into(),
        p2mr_program_hex: "11".repeat(32),
        share_difficulty: 1,
        network_difficulty: 100,
        template_height: 100,
        job_id: "job".into(),
        job_issued_at_ms: 1,
        accepted_at_ms: 0,
        ntime: 1_800_000_000,
        credit_policy: None,
    }
}

/// The slim candidate for `snapshot`'s window beside the bundle it was found
/// on, whose parts a direct `land_candidate` takes.
fn candidate_for(snapshot: &Snapshot, nonce: u32) -> Result<(Candidate, qbit_prism::AuditBundle)> {
    let (coinbase_key, ledger_key) = keys();
    let bundle = build_audit_bundle(
        snapshot.shares.clone(),
        FoundBlock {
            block_height: 101,
            coinbase_value_sats: 500_000_000,
            network_difficulty: 100,
            anchor_job_issued_at_ms: snapshot.anchor_ms,
        },
        snapshot.prior_balances.clone(),
        PayoutPolicy::day_one_default(),
        &coinbase_key,
        &ledger_key,
    )?;
    let report = verify_audit_bundle_with_ledger_public_key(&bundle, &ledger_key.public_key_hex())?;
    let mut block = vec![0u8; 80];
    block[..4].copy_from_slice(&0x20000000u32.to_le_bytes());
    block[4..36].fill(0x22);
    let mut txid = hex::decode(&report.coinbase_txid)?;
    txid.reverse();
    block[36..68].copy_from_slice(&txid);
    block[68..72].copy_from_slice(&1_800_000_000u32.to_le_bytes());
    block[72..76].copy_from_slice(&0x207fffffu32.to_le_bytes());
    block[76..80].copy_from_slice(&nonce.to_le_bytes());
    let mut hash = Sha256::digest(Sha256::digest(&block)).to_vec();
    hash.reverse();
    block.push(1);
    block.extend(hex::decode(&report.coinbase_tx_hex)?);
    let candidate = Candidate {
        block_hash: hex::encode(hash),
        block_sha256: Candidate::block_digest_hex(&block),
        job_id: "job".into(),
        payout_revision: snapshot.payout_revision,
        window: WindowRef::from_snapshot(snapshot)?,
        bootstrap_share: None,
        found_block: bundle.found_block.clone(),
        payout_policy: PayoutPolicy::day_one_default(),
        ctv: None,
        audit_builder_version: qbit_prism::AUDIT_BUILDER_VERSION,
        signer_keys: signer_keys(),
        leased: false,
        coinbase_suffix_hex: "00".repeat(12),
        deferred_share: None,
        block_bytes: block,
        as_issued_balances: Vec::new(),
    };
    Ok((candidate, bundle))
}

/// The share `seed_shares` writes for `share_seq`, built independently of the
/// reader so a digest over these is not a tautology.
fn seeded_share(share_seq: u64) -> AcceptedShare {
    AcceptedShare {
        share_seq,
        share_id: format!("share-{share_seq}"),
        miner_id: "miner".into(),
        order_key: "miner".into(),
        p2mr_program_hex: "11".repeat(32),
        share_difficulty: 1,
        network_difficulty: 1000,
        template_height: 100,
        job_id: "job".into(),
        job_issued_at_ms: ANCHOR - 1,
        accepted_at_ms: ANCHOR,
        ntime: 100,
        credit_policy: None,
    }
}

/// Write `first..=last` directly, with `rejected` the one sequence the window
/// predicate must skip. Share rows are immutable, so a gap inside a range can
/// only be produced this way, never by deleting a row afterwards.
async fn seed_shares(pool: &PgPool, first: i64, last: i64, rejected: i64) -> Result<()> {
    sqlx::query("INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,accepted_at,credit_policy,accepted,reject_reason,writer_id,writer_epoch)
        SELECT i,'share-'||i,'miner','miner',decode(repeat('11',32),'hex'),1,1000,100,'job',
            to_timestamp(($4-1)::double precision/1000),100,to_timestamp($4::double precision/1000),
            NULL,i<>$3,CASE WHEN i=$3 THEN 'stale-job' END,'candidate-window-test',0
        FROM generate_series($1::bigint,$2::bigint) AS g(i)")
        .bind(first).bind(last).bind(rejected).bind(ANCHOR).execute(pool).await?;
    Ok(())
}

fn balance(recipient_id: &str, order_key: &str, balance_sats: i128) -> CarryForwardBalance {
    CarryForwardBalance {
        recipient_id: recipient_id.into(),
        order_key: order_key.into(),
        p2mr_program_hex: "11".repeat(32),
        balance_sats,
    }
}

fn canonical(balances: &[CarryForwardBalance]) -> Vec<CarryForwardBalance> {
    let mut sorted = balances.to_vec();
    sorted.sort_by(|a, b| {
        a.order_key
            .cmp(&b.order_key)
            .then_with(|| a.recipient_id.cmp(&b.recipient_id))
            .then_with(|| a.p2mr_program_hex.cmp(&b.p2mr_program_hex))
    });
    sorted
}

/// The 2.x.x carry seed `tests/support/ledger_2x.rs` uses, so the acceptance
/// case starts from the real frozen schema and not a native one.
async fn seed_legacy_carry(pool: &PgPool) -> Result<()> {
    sqlx::raw_sql("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state) VALUES(repeat('aa',32),100,repeat('00',32),repeat('ab',32),repeat('ac',32),'confirmed'),(repeat('ee',32),101,repeat('aa',32),repeat('ef',32),repeat('e0',32),'prepared'); INSERT INTO qbit_payout_carry_forward(block_height,block_hash,miner_id,payout_order_key,p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,onchain_amount_sats,carry_forward_balance_sats,action) VALUES(100,repeat('aa',32),'miner-a','a',decode(repeat('11',32),'hex'),1000,0,1000,0,1000,'accrued'),(101,repeat('ee',32),'miner-b','b',decode(repeat('22',32),'hex'),500,0,500,0,500,'accrued'); DELETE FROM qbit_payout_carry_forward_current; UPDATE qbit_pool_blocks SET chain_state='confirmed' WHERE block_hash=repeat('ee',32);")
        .execute(pool).await?;
    Ok(())
}

#[tokio::test]
async fn legacy_2x_schema_with_terminal_outbox_rows_gains_007_and_keeps_every_row() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            // The standalone 2.x.x schema, with its own BEGIN/COMMIT and no
            // native tables, exactly as the upgrade meets it.
            sqlx::raw_sql(BASE_SCHEMA).execute(&db.pool).await?;
            seed_legacy_carry(&db.pool).await?;
            // Terminal rows only: a drained 2.x.x outbox has candidate NULL.
            for (index, tag) in ["a1", "b2", "c3"].iter().enumerate() {
                sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,state,attempt_count,last_error,completed_at) VALUES($1,NULL,$2,$3,$4,$5,clock_timestamp()-interval '1 hour')")
                    .bind(tag.repeat(32)).bind(format!("{tag}{}", "0".repeat(62)))
                    .bind(if index == 2 { "abandoned" } else { "submitted" })
                    .bind(index as i32).bind((index == 2).then_some("gave up"))
                    .execute(&db.pool).await?;
            }
            let before: Vec<Value> = sqlx::query_scalar(
                "SELECT to_jsonb(o) FROM qbit_block_candidate_outbox o ORDER BY block_hash",
            )
            .fetch_all(&db.pool)
            .await?;
            ensure!(before.len() == 3, "fixture rows missing");

            let _ledger = db.ledger("legacy-upgrade").await?;
            ensure!(
                db.versions().await? == REQUIRED_SCHEMA_VERSIONS,
                "007 did not join the applied set"
            );
            let after: Vec<Value> = sqlx::query_scalar(
                "SELECT to_jsonb(o) FROM qbit_block_candidate_outbox o ORDER BY block_hash",
            )
            .fetch_all(&db.pool)
            .await?;
            ensure!(after.len() == before.len(), "007 changed the row count");
            for (before, after) in before.iter().zip(&after) {
                let before = before.as_object().context("outbox row must be an object")?;
                let after = after.as_object().context("outbox row must be an object")?;
                // Every field the 2.x.x row already carried, unchanged. The
                // native migrations 003 to 005 add columns of their own here,
                // so this compares the original fields, not the whole row.
                for (column, value) in before {
                    ensure!(
                        after.get(column) == Some(value),
                        "007 rewrote {column}: {value:?} became {:?}",
                        after.get(column)
                    );
                }
                // And the reference columns are the "no window" state.
                for column in WINDOW_COLUMNS {
                    ensure!(
                        !before.contains_key(column),
                        "{column} already existed before 007"
                    );
                    ensure!(
                        after.get(column) == Some(&Value::Null),
                        "007 left {column} set on a terminal row"
                    );
                }
            }
            let identities: Vec<String> = sqlx::query_scalar(
                "SELECT block_hash FROM qbit_block_candidate_outbox ORDER BY block_hash",
            )
            .fetch_all(&db.pool)
            .await?;
            ensure!(
                identities == ["a1", "b2", "c3"].map(|tag| tag.repeat(32)).to_vec(),
                "007 changed the terminal row identities: {identities:?}"
            );
            // The carry-forward repair the same connect performs is unaffected.
            ensure!(
                sqlx::query_scalar::<_, i64>(
                    "SELECT count(*) FROM qbit_carry_forward_current_drift()"
                )
                .fetch_one(&db.pool)
                .await?
                    == 0,
                "carry-forward drift after 007"
            );
            Ok(())
        })
    })
    .await
}

/// The three pre-007 pending shapes 007 refuses, each on its own database.
#[derive(Clone, Copy, Debug)]
enum PreSevenShape {
    /// A native pre-007 inline candidate.
    InlineBundle,
    /// #258's chunked version-2 row, which relaxed 001's pending CHECK.
    NullCandidate,
    /// The same shape named directly, on a 2.x.x schema that has the column.
    StorageVersion,
}

#[tokio::test]
async fn migration_007_refuses_every_pre007_pending_shape_and_applies_nothing() -> Result<()> {
    for shape in [
        PreSevenShape::InlineBundle,
        PreSevenShape::NullCandidate,
        PreSevenShape::StorageVersion,
    ] {
        run(move |db| Box::pin(async move {
            db.apply_pre_007().await?;
            let hash = "7a".repeat(32);
            match shape {
                PreSevenShape::InlineBundle => {
                    sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256) VALUES($1,$2,$3)")
                        .bind(&hash).bind(json!({"block_hash": &hash, "bundle": {"schema": "qbit.prism.audit-bundle.v1"}}))
                        .bind("11".repeat(32)).execute(&db.pool).await?;
                }
                PreSevenShape::NullCandidate => {
                    // #258's migration replaces 001's pending CHECK so a
                    // version-2 row can keep its body outside `candidate`.
                    sqlx::raw_sql("DO $$ DECLARE name text; BEGIN SELECT conname INTO name FROM pg_constraint WHERE conrelid='qbit_block_candidate_outbox'::regclass AND contype='c' AND pg_get_constraintdef(oid) LIKE '%completed_at%'; EXECUTE format('ALTER TABLE qbit_block_candidate_outbox DROP CONSTRAINT %I', name); END $$;")
                        .execute(&db.pool).await?;
                    sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256) VALUES($1,NULL,$2)")
                        .bind(&hash).bind("11".repeat(32)).execute(&db.pool).await?;
                }
                PreSevenShape::StorageVersion => {
                    // #321's 006 already added the column, so only the row is
                    // set up here.
                    sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,storage_version) VALUES($1,'{}'::jsonb,$2,2)")
                        .bind(&hash).bind("11".repeat(32)).execute(&db.pool).await?;
                }
            }
            // Since #321, 006 always adds `storage_version` and runs before
            // 007, so every native schema reaching 007 has the column and the
            // refusal's no-column branch is unreachable from here. The three
            // shapes still differ as rows, which is what 007 refuses on.
            ensure!(
                db.has_column("qbit_block_candidate_outbox", "storage_version").await?,
                "{shape:?} did not set up the schema it means to test"
            );

            let error = db.ledger("refused").await.err()
                .with_context(|| format!("007 accepted a {shape:?} pending row"))?;
            let text = format!("{error:#}");
            ensure!(text.contains(&hash), "the refusal did not name the row: {text}");
            ensure!(
                text.contains("Drain the outbox") && text.contains("stop every"),
                "the refusal did not name the drain procedure: {text}"
            );
            // The whole migration transaction rolled back: no version 7 row,
            // no column and no index from 007.
            ensure!(!db.versions().await?.contains(&7), "version 7 was recorded");
            for column in WINDOW_COLUMNS {
                ensure!(
                    !db.has_column("qbit_block_candidate_outbox", column).await?,
                    "{column} survived the refusal"
                );
            }
            ensure!(
                !sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE schemaname=current_schema() AND indexname='qbit_block_candidate_outbox_window_balances_idx')")
                    .fetch_one(&db.pool).await?,
                "007's index survived the refusal"
            );

            // Once the operator has drained the row, the same binary starts.
            sqlx::query("UPDATE qbit_block_candidate_outbox SET state='submitted',candidate=NULL,completed_at=clock_timestamp()")
                .execute(&db.pool).await?;
            let _ledger = db.ledger("drained").await?;
            ensure!(db.versions().await? == REQUIRED_SCHEMA_VERSIONS, "007 did not apply after the drain");
            Ok(())
        })).await?;
    }
    Ok(())
}

#[tokio::test]
async fn migration_007_alone_is_applied_on_a_database_at_2_3_4_5_6_8_9_10() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            db.apply_pre_007().await?;
            let before: Vec<(i32, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
                "SELECT version,applied_at FROM qbit_prism_schema_migrations ORDER BY version",
            )
            .fetch_all(&db.pool)
            .await?;
            let _ledger = db.ledger("membership").await?;
            let after: Vec<(i32, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
                "SELECT version,applied_at FROM qbit_prism_schema_migrations ORDER BY version",
            )
            .fetch_all(&db.pool)
            .await?;
            ensure!(
                after
                    .iter()
                    .map(|(version, _)| *version)
                    .collect::<Vec<_>>()
                    == REQUIRED_SCHEMA_VERSIONS
            );
            // Every migration installed before this run keeps its original
            // timestamp; only missing migrations are applied.
            ensure!(
                after
                    .iter()
                    .filter(|(version, _)| before.iter().any(|(installed, _)| installed == version))
                    .copied()
                    .collect::<Vec<_>>()
                    == before,
                "the runner reapplied a migration that was already installed"
            );
            for column in WINDOW_COLUMNS {
                ensure!(
                    db.has_column("qbit_block_candidate_outbox", column).await?,
                    "007 did not add {column}"
                );
            }
            // A restart applies nothing further.
            let _restarted = db.ledger("membership-restart").await?;
            ensure!(db.versions().await? == REQUIRED_SCHEMA_VERSIONS);
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn outbox_window_check_accepts_exactly_the_three_states_and_indexes_the_digest() -> Result<()>
{
    run(|db| Box::pin(async move {
        db.apply_pre_007().await?;
        let before = outbox_columns(&db.pool).await?;
        let _ledger = db.ledger("check").await?;
        // 007 adds exactly its own columns to the table, and 011, which the
        // same connect applies after it, exactly its own.
        let added: Vec<String> = outbox_columns(&db.pool).await?
            .into_iter().filter(|column| !before.contains(column)).collect();
        let mut expected = WINDOW_COLUMNS.map(str::to_owned).to_vec();
        expected.extend(OFFER_COLUMNS.map(str::to_owned));
        expected.sort();
        ensure!(added == expected, "007 and 011 changed the outbox column set: {added:?}");
        // Every partial-null combination matters: a CHECK that evaluates to
        // NULL passes, so only num_nulls/num_nonnulls rejects a partial group.
        for mask in 0..64u32 {
            let set = |bit: u32| mask & (1u32 << bit) != 0;
            let result = sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,window_anchor_ms,window_prior_balances_sha256,window_first_share_seq,window_last_share_seq,window_share_count,window_snapshot_sha256) VALUES($1,'{}'::jsonb,$2,$3,$4,$5,$6,$7,$8)")
                .bind(format!("{mask:064x}")).bind("11".repeat(32))
                .bind(set(0).then_some(ANCHOR)).bind(set(1).then(|| "00".repeat(32)))
                .bind(set(2).then_some(1i64)).bind(set(3).then_some(3i64)).bind(set(4).then_some(2i64))
                .bind(set(5).then(|| "00".repeat(32))).execute(&db.pool).await;
            ensure!(
                result.is_ok() == matches!(mask, 0 | 3 | 63),
                "incorrect null-state {mask}: {result:?}"
            );
        }
        let range_row = format!("{:064x}", 63);
        for (column, value) in [
            ("window_first_share_seq", "0"),
            ("window_last_share_seq", "0"),
            ("window_share_count", "0"),
            ("window_share_count", "4"),
            ("window_snapshot_sha256", "repeat('A',64)"),
            ("window_snapshot_sha256", "repeat('a',63)"),
            ("window_prior_balances_sha256", "'00'"),
            ("window_prior_balances_sha256", "repeat('A',64)"),
        ] {
            ensure!(
                sqlx::query(&format!("UPDATE qbit_block_candidate_outbox SET {column}={value} WHERE block_hash=$1"))
                    .bind(&range_row).execute(&db.pool).await.is_err(),
                "invalid {column}={value} accepted"
            );
        }
        // `block_bytes` is nullable and unconstrained here: the terminal
        // UPDATE that NULLs it with the window columns is the switch's work.
        sqlx::query("UPDATE qbit_block_candidate_outbox SET block_bytes=decode('00ff','hex') WHERE block_hash=$1")
            .bind(&range_row).execute(&db.pool).await?;
        let index: String = sqlx::query_scalar("SELECT indexdef FROM pg_indexes WHERE schemaname=current_schema() AND indexname='qbit_block_candidate_outbox_window_balances_idx'")
            .fetch_one(&db.pool).await?;
        ensure!(
            index.contains("window_prior_balances_sha256") && index.contains("IS NOT NULL"),
            "retention index is not the partial digest index: {index}"
        );
        Ok(())
    })).await
}

#[tokio::test]
async fn configure_retains_the_pinned_cluster_fingerprint() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let first = db.ledger("fingerprint-first").await?;
            ensure!(
                first.config_fingerprint().is_none(),
                "a ledger that has not configured must retain nothing"
            );
            first.configure("fingerprint-one", &signer_keys()).await?;
            ensure!(first.config_fingerprint() == Some("fingerprint-one"));
            // Every clone of one frontend's ledger sees the same pinned value.
            ensure!(first.clone().config_fingerprint() == Some("fingerprint-one"));

            // A second frontend verifies the pinned row and retains it too.
            let second = db.ledger("fingerprint-second").await?;
            second.configure("fingerprint-one", &signer_keys()).await?;
            ensure!(second.config_fingerprint() == Some("fingerprint-one"));

            // A mismatch is still refused, and nothing is retained from it.
            let third = db.ledger("fingerprint-third").await?;
            ensure!(third
                .configure("fingerprint-two", &signer_keys())
                .await
                .is_err());
            ensure!(
                third.config_fingerprint().is_none(),
                "a refused configure retained a fingerprint"
            );
            let stored: Option<String> = sqlx::query_scalar(
                "SELECT config_fingerprint FROM qbit_prism_cluster WHERE singleton",
            )
            .fetch_one(&db.pool)
            .await?;
            ensure!(stored.as_deref() == Some("fingerprint-one"));
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn window_reference_from_snapshot_matches_the_landed_audit_snapshot_digest() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let ledger = db.ledger("from-snapshot").await?;
            // An empty snapshot is the empty-window reference, not an error.
            let empty = ledger.snapshot(100).await?;
            ensure!(empty.shares.is_empty(), "fixture is not an empty window");
            let empty_reference = WindowRef::from_snapshot(&empty)?;
            ensure!(empty_reference.shares.is_none());
            ensure!(empty_reference.anchor_ms == empty.anchor_ms);
            ensure!(ledger
                .read_window(&empty_reference, BalanceSource::Current)
                .await?
                .shares
                .is_empty());

            for id in 1..=4u64 {
                ledger.append(appended_share(id), None).await?;
            }
            let snapshot = ledger.snapshot(100).await?;
            ensure!(snapshot.shares.len() == 4, "fixture window is wrong");
            let reference = WindowRef::from_snapshot(&snapshot)?;
            let range = reference
                .shares
                .context("a four-share window has a range")?;
            let expected: [u8; 32] = Sha256::digest(serde_json::to_vec(&snapshot.shares)?).into();
            ensure!(
                range.snapshot_sha256 == expected,
                "streamed digest is not sha256(serde_json::to_vec(&shares))"
            );
            ensure!(range.first_share_seq == snapshot.shares[0].share_seq);
            ensure!(range.last_share_seq == snapshot.shares[3].share_seq);
            ensure!(range.share_count == 4);

            // Landing writes the same digest for the same window.
            let (candidate, bundle) = candidate_for(&snapshot, 11)?;
            ledger.enqueue_candidate(candidate).await?;
            let claim = ledger
                .claim_candidate(60)
                .await?
                .context("the enqueued candidate was not claimable")?
                .with_bundle(bundle);
            ledger
                .land_candidate(&claim, &keys().1.public_key_hex())
                .await?;
            let stored: Vec<String> =
                sqlx::query_scalar("SELECT snapshot_sha256 FROM qbit_prism_audit_snapshots")
                    .fetch_all(&db.pool)
                    .await?;
            ensure!(
                stored == vec![hex::encode(range.snapshot_sha256)],
                "landing wrote a different snapshot digest: {stored:?}"
            );
            // And the reference reads back exactly the shares it was built from.
            ensure!(
                ledger
                    .read_window(&reference, BalanceSource::Current)
                    .await?
                    .shares
                    == snapshot.shares
            );
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn balance_snapshot_writer_is_canonical_idempotent_and_conflict_checked() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let ledger = db.ledger("balances").await?;
            let balances = vec![
                balance("z", "a", 7),
                balance("a", "z", 9),
                balance("m", "a", 11),
            ];
            let sorted = canonical(&balances);
            ensure!(sorted != balances, "fixture is already canonical");

            let mut tx = ledger.pool.begin().await?;
            let digest = put_balance_snapshot(&mut tx, &balances).await?;
            tx.commit().await?;
            ensure!(digest == qbit_prism::prior_balances_digest(&balances));
            let key = hex::encode(digest);
            let stored: Vec<u8> = sqlx::query_scalar(
                "SELECT balances FROM qbit_prism_balance_snapshots WHERE prior_balances_digest=$1",
            )
            .bind(&key)
            .fetch_one(&db.pool)
            .await?;
            ensure!(
                stored == serde_json::to_vec(&sorted)?,
                "the writer did not store the canonical sort"
            );

            // A second write of any permutation is idempotent, not an error.
            let mut reversed = balances.clone();
            reversed.reverse();
            let mut tx = ledger.pool.begin().await?;
            ensure!(put_balance_snapshot(&mut tx, &reversed).await? == digest);
            tx.commit().await?;
            let after: Vec<u8> = sqlx::query_scalar(
                "SELECT balances FROM qbit_prism_balance_snapshots WHERE prior_balances_digest=$1",
            )
            .bind(&key)
            .fetch_one(&db.pool)
            .await?;
            ensure!(after == stored, "an idempotent rewrite changed the row");

            // read_window(AsIssued) returns the stored order, which is the sort
            // the writer applied, never the caller's original vector order.
            let reference = WindowRef {
                anchor_ms: ANCHOR,
                prior_balances_digest: digest,
                shares: None,
            };
            let window = ledger
                .read_window(&reference, BalanceSource::AsIssued)
                .await?;
            ensure!(
                window.prior_balances == sorted,
                "AsIssued did not return the stored canonical order"
            );
            ensure!(window.shares.is_empty());

            // A different encoding under the same key is an error, not a
            // silent overwrite and not a silent acceptance.
            sqlx::query("DELETE FROM qbit_prism_balance_snapshots")
                .execute(&db.pool)
                .await?;
            sqlx::query("INSERT INTO qbit_prism_balance_snapshots(prior_balances_digest,balances) VALUES($1,$2)")
                .bind(&key).bind(serde_json::to_vec(&balances)?).execute(&db.pool).await?;
            let mut tx = ledger.pool.begin().await?;
            let error = put_balance_snapshot(&mut tx, &balances)
                .await
                .err()
                .context("a conflicting stored encoding was accepted")?;
            ensure!(matches!(error, WindowError::Decode(_)), "{error:?}");
            tx.rollback().await?;

            // An empty set is a legitimate as-issued reference.
            let mut tx = ledger.pool.begin().await?;
            let empty = put_balance_snapshot(&mut tx, &[]).await?;
            tx.commit().await?;
            ensure!(empty == qbit_prism::prior_balances_digest(&[]));
            ensure!(ledger
                .read_window(
                    &WindowRef {
                        anchor_ms: ANCHOR,
                        prior_balances_digest: empty,
                        shares: None
                    },
                    BalanceSource::AsIssued
                )
                .await?
                .prior_balances
                .is_empty());
            Ok(())
        })
    })
    .await
}

const SEEDED_LAST: u64 = 5_000;
const SEEDED_REJECTED: u64 = 2_500;

#[tokio::test]
async fn share_probe_and_paged_reader_serve_a_callers_transaction() -> Result<()> {
    run(|db| Box::pin(async move {
        let ledger = db.ledger("primitives").await?;
        seed_shares(&db.pool, 1, SEEDED_LAST as i64, SEEDED_REJECTED as i64).await?;
        let expected: Vec<AcceptedShare> = (1..=SEEDED_LAST)
            .filter(|seq| *seq != SEEDED_REJECTED)
            .map(seeded_share)
            .collect();

        let mut tx = ledger.pool.begin().await?;
        // The probe is an existence test on both endpoints, in one round trip.
        ensure!(probe_share_rows(&mut tx, 1, SEEDED_LAST as i64).await?);
        ensure!(!probe_share_rows(&mut tx, 1, SEEDED_LAST as i64 + 1).await?, "probe hit a missing endpoint");
        ensure!(!probe_share_rows(&mut tx, 0, SEEDED_LAST as i64).await?, "probe hit a missing endpoint");
        // The enqueue probes one row by passing it as both bounds.
        ensure!(probe_share_rows(&mut tx, 1, 1).await?);
        ensure!(!probe_share_rows(&mut tx, SEEDED_LAST as i64 + 7, SEEDED_LAST as i64 + 7).await?);

        // The landing re-read's shape: compare each page with its slice of a
        // vector the caller already holds, inside the caller's transaction.
        let held = Arc::new(expected.clone());
        let source = held.clone();
        let (matched, pages) = read_range_paged(
            &mut tx,
            1,
            SEEDED_LAST as i64,
            ANCHOR,
            (0usize, 0usize),
            move |state, page| {
                let end = state.0 + page.len();
                let slice = source
                    .get(state.0..end)
                    .ok_or_else(|| WindowError::Decode(anyhow::anyhow!("page past the held window")))?;
                if slice != page {
                    return Err(WindowError::Decode(anyhow::anyhow!("page differs from the held window")));
                }
                state.0 = end;
                state.1 += 1;
                Ok(())
            },
        )
        .await?;
        ensure!(matched == expected.len(), "paged reader compared {matched} of {} shares", expected.len());
        ensure!(pages == 2, "4,999 rows must arrive in one full 4096-row page and one short page, not {pages}");
        tx.commit().await?;

        let range = |share_count| WindowRef {
            anchor_ms: ANCHOR,
            prior_balances_digest: qbit_prism::prior_balances_digest(&[]),
            shares: Some(ShareRange {
                first_share_seq: 1,
                last_share_seq: SEEDED_LAST,
                share_count,
                snapshot_sha256: Sha256::digest(serde_json::to_vec(&expected).unwrap()).into(),
            }),
        };
        // read_window itself is unchanged by the refactor.
        let window = ledger.read_window(&range(expected.len() as u64), BalanceSource::Current).await?;
        ensure!(window.shares == expected, "read_window returned a different window");
        // A row the predicate skips inside the range is Incomplete, with no
        // partial window for the caller to mistake for a rebuildable one.
        let error = ledger.read_window(&range(SEEDED_LAST), BalanceSource::Current).await
            .err().context("a short window was accepted")?;
        ensure!(
            matches!(error, WindowError::Incomplete { expected: e, got } if e == SEEDED_LAST && got == SEEDED_LAST - 1),
            "{error:?}"
        );
        Ok(())
    })).await
}

#[tokio::test]
async fn paged_reader_reports_a_failed_blocking_handoff_as_task_failed() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let ledger = db.ledger("task-failed").await?;
            seed_shares(&db.pool, 1, 10, 0).await?;
            let mut tx = ledger.pool.begin().await?;
            // The consumer runs on the blocking thread, so its failure reaches
            // the reader as a JoinError. The panic message this prints is the
            // expected output of the case, not a test failure.
            let error = read_range_paged(
                &mut tx,
                1,
                10,
                ANCHOR,
                (),
                |_state: &mut (), _page: Vec<AcceptedShare>| -> Result<(), WindowError> {
                    panic!("deliberate page-consumer failure")
                },
            )
            .await
            .err()
            .context("a failed blocking hand-off was reported as success")?;
            ensure!(
                matches!(&error, WindowError::TaskFailed(join) if join.is_panic()),
                "{error:?}"
            );
            ensure!(
                !matches!(error, WindowError::Decode(_)),
                "a task failure was reported as corruption"
            );
            tx.rollback().await?;
            Ok(())
        })
    })
    .await
}

/// The permit is the caller's `window_reads` slot. It must outlive every
/// blocking hand-off the read owns, including the cleanup a cancellation
/// leaves behind, and it must never be released on a runtime thread.
///
/// This builds its own runtime with a single blocking thread, occupies that
/// thread, and cancels a reader parked mid-page on a database gate. Nothing
/// queued behind the occupied thread can run, so the semaphore proves the
/// order: no permit while the cleanup is pending, a permit once it has run.
#[test]
fn read_window_permit_outlives_a_cancelled_page_and_is_released_off_the_runtime() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        // `block_on` polls this on the test's own thread, which the gate needs.
        let Some(db) = Database::open().await? else {
            return Ok(());
        };
        let result = permit_release_body(&db).await;
        let cleanup = db.close().await;
        match (result, cleanup) {
            (Ok(()), cleanup) => cleanup,
            (Err(error), Ok(())) => Err(error),
            (Err(error), Err(cleanup)) => {
                Err(error.context(format!("schema cleanup also failed: {cleanup}")))
            }
        }
    })
}

async fn permit_release_body(db: &Database) -> Result<()> {
    const LAST: i64 = 6_000;
    const ACCUMULATED: i64 = 4_096;
    let mut ledger = db.ledger("permit").await?;
    // The reader must use the untimed single-connection pool: the gate below
    // holds its page far longer than `Ledger::connect`'s lock timeout allows.
    ledger.pool = db.pool.clone();
    seed_shares(&db.pool, 1, LAST, 0).await?;

    let gate_key: i64 = sqlx::query_scalar("SELECT oid::bigint FROM pg_namespace WHERE nspname=$1")
        .bind(&db.schema)
        .fetch_one(&db.pool)
        .await?;
    sqlx::raw_sql(&format!(
        "ALTER TABLE qbit_share_ledger RENAME TO window_permit_source;
         CREATE FUNCTION gate_after_page(seq bigint, value text) RETURNS text
         LANGUAGE plpgsql VOLATILE AS 'BEGIN IF seq > {ACCUMULATED} THEN
             PERFORM pg_advisory_xact_lock({gate_key}); END IF; RETURN value; END;'"
    ))
    .execute(&db.pool)
    .await?;
    let columns: String = sqlx::query_scalar("SELECT string_agg(CASE WHEN attname='share_id' THEN 'gate_after_page(share_seq,share_id) AS share_id' ELSE quote_ident(attname) END, ',' ORDER BY attnum) FROM pg_attribute WHERE attrelid='window_permit_source'::regclass AND attnum>0 AND NOT attisdropped")
        .fetch_one(&db.pool).await?;
    // Ordered, and with sorting and non-index scans off on this test-only
    // connection, so the first page streams instead of evaluating the gate for
    // the whole range before any row is returned.
    sqlx::raw_sql(&format!("CREATE VIEW qbit_share_ledger AS SELECT {columns} FROM window_permit_source ORDER BY share_seq; ANALYZE window_permit_source;"))
        .execute(&db.pool).await?;
    sqlx::raw_sql("SET enable_bitmapscan=off; SET enable_seqscan=off; SET enable_sort=off")
        .execute(&db.pool)
        .await?;

    let mut gate = db.admin.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(gate_key)
        .execute(&mut *gate)
        .await?;

    let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
    let permit = semaphore.clone().acquire_owned().await?;
    let window = WindowRef {
        anchor_ms: ANCHOR,
        prior_balances_digest: qbit_prism::prior_balances_digest(&[]),
        shares: Some(ShareRange {
            first_share_seq: 1,
            last_share_seq: LAST as u64,
            share_count: LAST as u64,
            // Unreachable: the final page never returns while the gate holds.
            snapshot_sha256: [0; 32],
        }),
    };
    let reader = tokio::spawn(async move {
        ledger
            .read_window_with_permit(&window, BalanceSource::Current, permit)
            .await
    });
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND classid=0 AND objid::bigint=$1)")
                .bind(gate_key).fetch_one(&db.admin).await?;
            if blocked {
                return Ok::<_, anyhow::Error>(());
            }
            ensure!(!reader.is_finished(), "the reader finished before its gated page");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("the reader never reached its gated page")??;
    ensure!(
        semaphore.try_acquire().is_err(),
        "the permit was released while the read was still running"
    );

    // Occupy the only blocking thread. Every cleanup the cancellation hands
    // off, and the permit's own release, now has to queue behind it.
    let (entered, occupied) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel::<()>();
    let occupier = tokio::task::spawn_blocking(move || {
        entered.send(()).unwrap();
        wait.recv().unwrap();
    });
    occupied.await?;

    reader.abort();
    ensure!(
        reader.await.unwrap_err().is_cancelled(),
        "the reader was not cancelled"
    );
    // The read's future is gone, but its window and its permit are not: both
    // are waiting on the blocking thread the test is holding.
    for _ in 0..20 {
        ensure!(
            semaphore.try_acquire().is_err(),
            "the permit was released on the runtime instead of behind the blocking cleanup"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    release.send(())?;
    occupier.await?;
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if semaphore.try_acquire().is_ok() {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("the permit was never released after the blocking cleanup ran")??;

    gate.rollback().await?;
    Ok(())
}
