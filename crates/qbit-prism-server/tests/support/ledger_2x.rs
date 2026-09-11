//! The frozen 2.x.x source schema, and every upgrade test that starts from it.
//!
//! The fixtures under `tests/fixtures/schema_2x` are byte-exact copies of the
//! 2.x.x release SQL (see the README there). Upgrade tests build their source
//! from those copies, never from the live in-tree files, so a DDL edit to the
//! live `001_share_ledger.sql` cannot be absorbed silently: the digest tests
//! below pin the fixtures, pin the live file, and compare the two.
use super::*;
use qbit_prism_server::ledger::{
    audit_canonical_bytes, MigrationSource, SourceState, REQUIRED_SCHEMA_VERSION, SOURCE_STATES,
};
use sqlx::Row;
use std::io::Write;

/// `crates/qbit-prism/sql/001_share_ledger.sql` at 2.x.x v2.0.2 (`504846c`);
/// byte-identical at v2.0.1 (`95ffe06`) and v2.0.0 (`f6854a0`).
pub const FROZEN_2X_001: &str = include_str!("../fixtures/schema_2x/001_share_ledger.sql");
/// `crates/qbit-prism/sql/002_candidate_bodies.sql` at 2.x.x v2.0.2 (`504846c`, #258).
pub const FROZEN_2X_002: &str = include_str!("../fixtures/schema_2x/002_candidate_bodies.sql");
pub const FROZEN_2X_001_SHA256: &str =
    "9dfdad0651cb92d8a007fd62f50184d9bfdb9d41fda22f8be657ed6c2da92aca";
pub const FROZEN_2X_002_SHA256: &str =
    "e36b2056a993543bb360c2a81ef961c96a6277a2c2b3c31a2723acafaab34b19";
const RELEASE_COMMIT_2_0_2: &str = "504846cc0b72e8f86ed17f896d4ccbbe196a31dc";

/// The live in-tree files the migrator applies (001) and carries (002).
const LIVE_001: &str = include_str!("../../../qbit-prism/sql/001_share_ledger.sql");
const LIVE_002: &str = include_str!("../../../qbit-prism/sql/002_candidate_bodies.sql");
/// The live 001 differs from the release only in comments (#244). Its digest
/// is pinned so that any edit is a reviewed act; the statement comparison in
/// `live_sql_stays_pinned_to_the_frozen_2x_release` proves the DDL is still
/// the release DDL.
pub const LIVE_001_SHA256: &str =
    "a5ce618f0010f34336ba6c77fb5c3f873425b136f1957a73264c7ddaed2c0848";

fn sha256_hex(text: &str) -> String {
    hex::encode(Sha256::digest(text.as_bytes()))
}

/// The lines of a SQL file with `--` comments removed, both full-line and
/// trailing ones, leaving `--` inside single-quoted strings alone. Blank
/// lines are dropped, so two files with the same statements compare equal
/// whatever their comments say.
fn sql_statements(sql: &str) -> Vec<String> {
    sql.lines()
        .filter_map(|line| {
            let mut kept = String::new();
            let mut quoted = false;
            let mut chars = line.chars().peekable();
            while let Some(character) = chars.next() {
                if character == '\'' {
                    quoted = !quoted;
                } else if !quoted && character == '-' && chars.peek() == Some(&'-') {
                    break;
                }
                kept.push(character);
            }
            let kept = kept.trim_end();
            (!kept.is_empty()).then(|| kept.to_owned())
        })
        .collect()
}

#[test]
fn frozen_2x_fixtures_match_their_pinned_release_digests() {
    for (name, text, pinned) in [
        ("001_share_ledger.sql", FROZEN_2X_001, FROZEN_2X_001_SHA256),
        (
            "002_candidate_bodies.sql",
            FROZEN_2X_002,
            FROZEN_2X_002_SHA256,
        ),
    ] {
        assert_eq!(
            sha256_hex(text),
            pinned,
            "tests/fixtures/schema_2x/{name} is a byte-exact copy of the 2.x.x v2.0.2 release file \
             and must never be edited. Restore it with: git show {RELEASE_COMMIT_2_0_2}:crates/qbit-prism/sql/{name} \
             > crates/qbit-prism-server/tests/fixtures/schema_2x/{name}"
        );
    }
}

#[test]
fn live_sql_stays_pinned_to_the_frozen_2x_release() {
    assert_eq!(
        sql_statements("SELECT '--' -- trailing\n  -- whole line\n\nSELECT 1;\n"),
        vec!["SELECT '--'", "SELECT 1;"]
    );
    let (live, frozen) = (sql_statements(LIVE_001), sql_statements(FROZEN_2X_001));
    let first_difference = live
        .iter()
        .zip(&frozen)
        .position(|(live, frozen)| live != frozen)
        .or_else(|| (live.len() != frozen.len()).then_some(live.len().min(frozen.len())));
    if let Some(index) = first_difference {
        panic!(
            "crates/qbit-prism/sql/001_share_ledger.sql no longer matches the 2.x.x v2.0.2 release \
             statement for statement; the first difference is at statement line {} (live: {:?}, \
             release: {:?}). The migrator applies this file to every 2.x.x database, so its DDL \
             must stay identical to tests/fixtures/schema_2x/001_share_ledger.sql. Revert the DDL \
             change here and put it in a new numbered migration under \
             crates/qbit-prism-server/migrations/ (007 is next), then bump REQUIRED_SCHEMA_VERSION \
             in src/ledger/migration.rs",
            index + 1,
            live.get(index).map(String::as_str).unwrap_or("<end of file>"),
            frozen.get(index).map(String::as_str).unwrap_or("<end of file>")
        );
    }
    let live_001 = sha256_hex(LIVE_001);
    assert_eq!(
        live_001, LIVE_001_SHA256,
        "crates/qbit-prism/sql/001_share_ledger.sql changed. Its statements still match the frozen \
         v2.0.2 release, so only comments changed: if that is intended, set LIVE_001_SHA256 in \
         tests/support/ledger_2x.rs to {live_001}. A DDL change belongs in a new numbered migration \
         under crates/qbit-prism-server/migrations/, never in this file"
    );
    assert_eq!(
        sha256_hex(LIVE_002),
        FROZEN_2X_002_SHA256,
        "crates/qbit-prism/sql/002_candidate_bodies.sql is carried as the frozen 2.x.x v2.0.2 \
         artifact; 3.x.x never applies it and the migrator pins the objects it created. Restore it \
         with: git show {RELEASE_COMMIT_2_0_2}:crates/qbit-prism/sql/002_candidate_bodies.sql \
         > crates/qbit-prism/sql/002_candidate_bodies.sql. A native schema change belongs in a new \
         numbered migration under crates/qbit-prism-server/migrations/"
    );
}

#[test]
fn source_state_table_is_the_pinned_data() {
    let rows: Vec<(&str, &str)> = SOURCE_STATES
        .iter()
        .map(|rule| (rule.name, rule.verdict))
        .collect();
    assert_eq!(
        rows,
        vec![
            ("fresh", "accept"),
            ("pre-#258", "accept after the drain check"),
            ("#258 applied", "accept after the drain check"),
            ("partial 002", "refuse, naming the missing object"),
            ("newer", "refuse before any DDL"),
            ("drifted 001", "refuse transactionally, naming the object"),
        ]
    );
    assert_eq!(SourceState::Pre258.release().map(|r| r.0), Some("2.0.1"));
    assert_eq!(
        SourceState::Applied258.release().map(|r| r.1),
        Some(RELEASE_COMMIT_2_0_2)
    );
    assert_eq!(REQUIRED_SCHEMA_VERSION, 6);
}

/// Build a 2.x.x source from the frozen release files. Each file is the
/// standalone 2.x.x SQL: 001 carries its own BEGIN/COMMIT, 002 is applied by
/// the 2.x.x ledger in a separate script call after it.
pub async fn apply_frozen_2x_schema(pool: &PgPool, state: SourceState) -> Result<()> {
    match state {
        SourceState::Fresh => {}
        SourceState::Pre258 => {
            sqlx::raw_sql(FROZEN_2X_001).execute(pool).await?;
        }
        SourceState::Applied258 => {
            sqlx::raw_sql(FROZEN_2X_001).execute(pool).await?;
            sqlx::raw_sql(FROZEN_2X_002).execute(pool).await?;
        }
    }
    Ok(())
}

pub fn legacy_hash(byte: u8) -> String {
    format!("{byte:02x}").repeat(32)
}

/// A pending v1 row as the 2.x.x writer leaves it: a JSONB body without the
/// native `payout_revision`/`bundle`/`block_hash` fields.
pub async fn insert_v1_pending(pool: &PgPool, hash: &str) -> Result<()> {
    sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256) VALUES($1,$2,$3)")
        .bind(hash).bind(json!({"block_hex":"legacy-python-payload"})).bind("88".repeat(32)).execute(pool).await?;
    Ok(())
}

pub async fn insert_v1_terminal(pool: &PgPool, hash: &str, state: &str) -> Result<()> {
    sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,state,completed_at) VALUES($1,NULL,$2,$3,clock_timestamp())")
        .bind(hash).bind("88".repeat(32)).bind(state).execute(pool).await?;
    Ok(())
}

/// A pending v2 row as #258's writer leaves it: `candidate` NULL, a sealed
/// chunk-body manifest, and `body_id` pointing at it. An empty body seals
/// under 002's completeness trigger, so no chunk rows are needed.
pub async fn insert_v2_pending(pool: &PgPool, hash: &str) -> Result<String> {
    let body_id = Uuid::new_v4().simple().to_string();
    let digest = "99".repeat(32);
    sqlx::query("INSERT INTO qbit_block_candidate_body(body_id,storage_version,block_hash,candidate_sha256,byte_count,chunk_count,chunk_bytes,share_count,shares_offset,shares_end,staging_writer_id,staging_writer_epoch,staging_session_token) VALUES($1,2,$2,$3,0,0,1,0,0,0,'python',1,'session')")
        .bind(&body_id).bind(hash).bind(&digest).execute(pool).await?;
    sqlx::query("UPDATE qbit_block_candidate_body SET state='sealed',sealed_at=clock_timestamp() WHERE body_id=$1")
        .bind(&body_id).execute(pool).await?;
    sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,storage_version,body_id) VALUES($1,NULL,$2,2,$3)")
        .bind(hash).bind(&digest).bind(&body_id).execute(pool).await?;
    Ok(body_id)
}

/// A terminal v2 row: the body was detached and retired at terminalization.
pub async fn insert_v2_terminal(pool: &PgPool, hash: &str, state: &str) -> Result<()> {
    sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,storage_version,body_id,retired_body_id,state,completed_at) VALUES($1,NULL,$2,2,NULL,$3,$4,clock_timestamp())")
        .bind(hash).bind("99".repeat(32)).bind(Uuid::new_v4().simple().to_string()).bind(state).execute(pool).await?;
    Ok(())
}

/// Terminalize a pending row the way the 2.x.x submitter does when it
/// finishes the candidate: clear the JSONB body, detach and retire any
/// chunk body. This stands in for the drain the guide requires.
pub async fn drain_2x_row(pool: &PgPool, hash: &str, v2: bool) -> Result<()> {
    if v2 {
        let body_id: Option<String> = sqlx::query_scalar("UPDATE qbit_block_candidate_outbox SET state='submitted',candidate=NULL,body_id=NULL,retired_body_id=body_id,completed_at=clock_timestamp(),updated_at=clock_timestamp() WHERE block_hash=$1 RETURNING retired_body_id")
            .bind(hash).fetch_one(pool).await?;
        sqlx::query("UPDATE qbit_block_candidate_body SET state='retired',retired_at=clock_timestamp() WHERE body_id=$1")
            .bind(body_id).execute(pool).await?;
    } else {
        sqlx::query("UPDATE qbit_block_candidate_outbox SET state='submitted',candidate=NULL,completed_at=clock_timestamp(),updated_at=clock_timestamp() WHERE block_hash=$1")
            .bind(hash).execute(pool).await?;
    }
    Ok(())
}

async fn native_tables_absent(pool: &PgPool) -> Result<bool> {
    Ok(sqlx::query_scalar("SELECT to_regclass('qbit_prism_schema_migrations') IS NULL AND to_regclass('qbit_prism_cluster') IS NULL AND to_regclass('qbit_prism_migration_source') IS NULL")
        .fetch_one(pool).await?)
}

async fn pending_rows(pool: &PgPool) -> Result<i64> {
    Ok(
        sqlx::query_scalar(
            "SELECT count(*) FROM qbit_block_candidate_outbox WHERE state='pending'",
        )
        .fetch_one(pool)
        .await?,
    )
}

async fn capability(pool: &PgPool) -> Result<Option<i32>> {
    Ok(sqlx::query_scalar("SELECT capability_value FROM qbit_prism_schema_capabilities WHERE capability='candidate_storage_version'")
        .fetch_optional(pool).await?)
}

async fn schema_version(pool: &PgPool) -> Result<i32> {
    Ok(
        sqlx::query_scalar("SELECT max(version) FROM qbit_prism_schema_migrations")
            .fetch_one(pool)
            .await?,
    )
}

/// The writers still work over the migrated schema: a native candidate is
/// persisted, claimed, landed and finished as a version 1 row.
async fn exercise_native_writers(ledger: &Ledger, share_id: u64, nonce: u32) -> Result<String> {
    ledger.append(share(share_id), None).await?;
    let block = candidate(&ledger.snapshot(100).await?, nonce)?;
    ledger.enqueue_candidate(block.clone()).await?;
    let claim = ledger
        .claim_candidate(60)
        .await?
        .context("native candidate not claimed")?;
    assert_eq!(claim.candidate.block_hash, block.block_hash);
    ledger
        .land_candidate(&claim, &keys().1.public_key_hex())
        .await?;
    ledger.finish_candidate(&claim, true, None).await?;
    let row = sqlx::query("SELECT state,storage_version,candidate IS NULL AS detached FROM qbit_block_candidate_outbox WHERE block_hash=$1")
        .bind(&block.block_hash).fetch_one(&ledger.pool).await?;
    assert_eq!(row.try_get::<String, _>("state")?, "submitted");
    assert_eq!(row.try_get::<i32, _>("storage_version")?, 1);
    assert!(row.try_get::<bool, _>("detached")?);
    Ok(block.block_hash)
}

async fn parked_row(
    pool: &PgPool,
    hash: &str,
) -> Result<(Option<String>, Option<String>, bool, i32, String)> {
    let row = sqlx::query("SELECT claim_token,last_error,next_attempt_at='infinity' AS parked,attempt_count,state FROM qbit_block_candidate_outbox WHERE block_hash=$1")
        .bind(hash).fetch_one(pool).await?;
    Ok((
        row.try_get("claim_token")?,
        row.try_get("last_error")?,
        row.try_get("parked")?,
        row.try_get("attempt_count")?,
        row.try_get("state")?,
    ))
}

#[tokio::test]
async fn frozen_258_source_refuses_pending_v1_and_v2_rows_and_migrates_terminal_rows() -> Result<()>
{
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    apply_frozen_2x_schema(&pool, SourceState::Applied258).await?;
    let (v1_pending, v2_pending) = (legacy_hash(0x11), legacy_hash(0x22));
    let (v1_terminal, v2_terminal) = (legacy_hash(0x33), legacy_hash(0x44));
    insert_v1_pending(&pool, &v1_pending).await?;
    let v2_body = insert_v2_pending(&pool, &v2_pending).await?;
    insert_v1_terminal(&pool, &v1_terminal, "submitted").await?;
    insert_v2_terminal(&pool, &v2_terminal, "abandoned").await?;
    assert_eq!(capability(&pool).await?, Some(2));

    // Both pending rows block, and the refusal names them and the drain.
    let error = db
        .ledger("a")
        .await
        .err()
        .context("migration accepted an undrained #258 outbox")?
        .to_string();
    assert!(error.contains("outbox is not drained"), "{error}");
    assert!(
        error.contains("2 pending 2.x.x candidate row(s)"),
        "{error}"
    );
    assert!(
        error.contains(&format!("block_hash={v1_pending} storage_version=1")),
        "{error}"
    );
    assert!(
        error.contains(&format!("block_hash={v2_pending} storage_version=2")),
        "{error}"
    );
    assert!(
        error.contains("lab.prism.recover_pending_blocks"),
        "{error}"
    );
    assert!(error.contains("v2.0.2"), "{error}");
    assert!(native_tables_absent(&pool).await?, "refusal ran DDL");
    assert_eq!(pending_rows(&pool).await?, 2, "refusal changed 2.x.x rows");

    // The v2 row alone still blocks: the capability row says 002 ran, the
    // rows say whether v2 work is pending.
    drain_2x_row(&pool, &v1_pending, false).await?;
    let error = db
        .ledger("a")
        .await
        .err()
        .context("migration accepted an undrained v2 row")?
        .to_string();
    assert!(
        error.contains("1 pending 2.x.x candidate row(s)"),
        "{error}"
    );
    assert!(
        error.contains(&v2_pending) && !error.contains(&v1_pending),
        "{error}"
    );
    drain_2x_row(&pool, &v2_pending, true).await?;

    let ledger = db.ledger("a").await?;
    assert_eq!(schema_version(&pool).await?, REQUIRED_SCHEMA_VERSION);
    assert_eq!(capability(&pool).await?, Some(2), "capability row lost");
    let source = ledger
        .migration_source()
        .await?
        .context("migration source not recorded")?;
    assert_eq!(source.source_state, "258_applied");
    assert_eq!(source.source_release.as_deref(), Some("2.0.2"));
    assert_eq!(source.source_commit.as_deref(), Some(RELEASE_COMMIT_2_0_2));
    assert_eq!(source.candidate_storage_version, Some(2));
    assert_eq!(source.prior_schema_version, 0);
    assert_eq!(source.migrated_by, "a");
    let terminal: Vec<(String, String, i32, Option<String>)> = sqlx::query_as("SELECT block_hash,state,storage_version,retired_body_id FROM qbit_block_candidate_outbox ORDER BY block_hash")
        .fetch_all(&pool).await?;
    assert_eq!(
        terminal
            .iter()
            .map(|(hash, state, version, _)| (hash.as_str(), state.as_str(), *version))
            .collect::<Vec<_>>(),
        vec![
            (v1_pending.as_str(), "submitted", 1),
            (v2_pending.as_str(), "submitted", 2),
            (v1_terminal.as_str(), "submitted", 1),
            (v2_terminal.as_str(), "abandoned", 2),
        ]
    );
    assert_eq!(terminal[1].3.as_deref(), Some(v2_body.as_str()));
    assert_eq!(pending_rows(&pool).await?, 0);

    // The writers work over 001 + 002, and a second instance starts without
    // initialize because the schema is at the required version.
    exercise_native_writers(&ledger, 1, 5001).await?;
    let follower = Ledger::connect(&db.url, "b".into(), 8, false).await?;
    assert_eq!(
        follower.migration_source().await?,
        Some(source.clone()),
        "later starts see what was migrated"
    );
    let again = db.ledger("c").await?;
    assert_eq!(
        again.migration_source().await?,
        Some(source),
        "a repeated migrate rewrote the source record"
    );

    // A v2 row that appears after cutover is parked, not looped through the
    // claim lane, and does not block ordinary v1 work.
    let late_v2 = legacy_hash(0x55);
    insert_v2_pending(&pool, &late_v2).await?;
    assert!(ledger.claim_candidate(60).await?.is_none());
    let (token, last_error, parked, attempts, state) = parked_row(&pool, &late_v2).await?;
    assert!(token.is_none());
    assert!(
        last_error
            .as_deref()
            .is_some_and(|e| e.contains("storage_version 2")),
        "{last_error:?}"
    );
    assert!(parked && attempts == 1 && state == "pending");
    assert!(follower.claim_candidate(60).await?.is_none());
    let native = exercise_native_writers(&ledger, 2, 5002).await?;
    assert_ne!(native, late_v2);
    pool.close().await;
    db.close(vec![ledger, follower, again]).await
}

#[tokio::test]
async fn frozen_pre_258_source_refuses_pending_v1_cleanly_and_migrates_terminal_rows() -> Result<()>
{
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    let (pending, terminal) = (legacy_hash(0x11), legacy_hash(0x33));
    insert_v1_pending(&pool, &pending).await?;
    insert_v1_terminal(&pool, &terminal, "abandoned").await?;
    let error = db
        .ledger("a")
        .await
        .err()
        .context("migration accepted an undrained pre-#258 outbox")?
        .to_string();
    assert!(error.contains("outbox is not drained"), "{error}");
    assert!(
        error.contains(&format!("block_hash={pending} storage_version=1")),
        "{error}"
    );
    assert!(
        !error.contains("does not exist") && !error.contains("column"),
        "pre-#258 refusal was a SQL error: {error}"
    );
    assert!(native_tables_absent(&pool).await?, "refusal ran DDL");
    assert_eq!(pending_rows(&pool).await?, 1);
    drain_2x_row(&pool, &pending, false).await?;

    let ledger = db.ledger("a").await?;
    assert_eq!(schema_version(&pool).await?, REQUIRED_SCHEMA_VERSION);
    let source = ledger
        .migration_source()
        .await?
        .context("migration source not recorded")?;
    assert_eq!(
        source,
        MigrationSource {
            source_state: "pre_258".into(),
            source_release: Some("2.0.1".into()),
            source_commit: Some("95ffe063846d51f83999a66cc654da5f7476fdef".into()),
            candidate_storage_version: None,
            prior_schema_version: 0,
            migrated_by: "a".into(),
            migrated_at: source.migrated_at,
        }
    );
    // 006 declares the version 1 capability and the storage_version column
    // for the claim lane; the chunk tables are not carried.
    assert_eq!(capability(&pool).await?, Some(1));
    assert!(sqlx::query_scalar::<_,bool>("SELECT to_regclass('qbit_block_candidate_body') IS NULL AND EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass('qbit_block_candidate_outbox') AND attname='storage_version' AND NOT attisdropped)").fetch_one(&pool).await?);
    let rows: Vec<(String, String, i32)> = sqlx::query_as(
        "SELECT block_hash,state,storage_version FROM qbit_block_candidate_outbox ORDER BY block_hash",
    )
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        rows,
        vec![
            (pending.clone(), "submitted".into(), 1),
            (terminal.clone(), "abandoned".into(), 1)
        ]
    );
    exercise_native_writers(&ledger, 1, 5101).await?;
    let follower = Ledger::connect(&db.url, "b".into(), 8, false).await?;
    pool.close().await;
    db.close(vec![ledger, follower]).await
}

#[tokio::test]
async fn partial_002_source_is_refused_naming_the_missing_object() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    apply_frozen_2x_schema(&pool, SourceState::Applied258).await?;
    // Body tables and the capability row, but one 002 object missing.
    sqlx::raw_sql(
        "DROP TRIGGER qbit_block_candidate_publication_guard ON qbit_block_candidate_outbox",
    )
    .execute(&pool)
    .await?;
    let error = db
        .ledger("a")
        .await
        .err()
        .context("migration accepted a partial 002 source")?
        .to_string();
    assert!(error.contains("partial 002"), "{error}");
    assert!(
        error.contains(
            "missing trigger qbit_block_candidate_publication_guard on qbit_block_candidate_outbox"
        ),
        "{error}"
    );
    assert!(error.contains("v2.0.2"), "{error}");
    assert!(native_tables_absent(&pool).await?, "refusal ran DDL");
    // Body tables without the capability row.
    sqlx::raw_sql(FROZEN_2X_002).execute(&pool).await?;
    sqlx::raw_sql("DELETE FROM qbit_prism_schema_capabilities")
        .execute(&pool)
        .await?;
    let error = db.ledger("a").await.err().context("accepted")?.to_string();
    assert!(
        error.contains(
            "missing row candidate_storage_version = 2 in qbit_prism_schema_capabilities"
        ),
        "{error}"
    );
    // Finishing 002 with the 2.x.x release makes the source acceptable.
    sqlx::raw_sql(FROZEN_2X_002).execute(&pool).await?;
    let ledger = db.ledger("a").await?;
    assert_eq!(
        ledger.migration_source().await?.map(|s| s.source_state),
        Some("258_applied".into())
    );
    pool.close().await;
    db.close(vec![ledger]).await?;

    // The reverse: the capability row without the body tables.
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    sqlx::raw_sql("CREATE TABLE qbit_prism_schema_capabilities(capability text PRIMARY KEY,capability_value integer NOT NULL,updated_at timestamptz NOT NULL DEFAULT clock_timestamp()); INSERT INTO qbit_prism_schema_capabilities(capability,capability_value) VALUES('candidate_storage_version',2)")
        .execute(&pool).await?;
    let error = db.ledger("a").await.err().context("accepted")?.to_string();
    assert!(error.contains("partial 002"), "{error}");
    assert!(
        error.contains("missing table qbit_block_candidate_body,"),
        "{error}"
    );
    assert!(
        error.contains("column qbit_block_candidate_outbox.storage_version"),
        "{error}"
    );
    assert!(native_tables_absent(&pool).await?, "refusal ran DDL");
    pool.close().await;
    db.close(vec![]).await
}

#[tokio::test]
async fn drifted_pre_258_source_is_refused_naming_each_object_and_rolls_back() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    let terminal = legacy_hash(0x33);
    insert_v1_terminal(&pool, &terminal, "submitted").await?;
    // Six edits 001's IF NOT EXISTS cannot repair: a foreign key 001 only
    // creates inside CREATE TABLE, a NOT NULL and a column type it never
    // re-asserts, an index that keeps its name but not its definition, and
    // the structure of two sequences behind bigserial columns, which
    // CREATE TABLE IF NOT EXISTS leaves as they are. (001 re-creates every
    // function and trigger itself, so those are only checked as found on
    // 002 objects.)
    sqlx::raw_sql("ALTER TABLE qbit_block_candidate_outbox DROP CONSTRAINT qbit_block_candidate_outbox_share_id_fkey; ALTER TABLE qbit_pool_blocks ALTER COLUMN parent_hash DROP NOT NULL; ALTER TABLE qbit_share_ledger ALTER COLUMN ntime TYPE integer; DROP INDEX qbit_share_ledger_template_height_idx; CREATE INDEX qbit_share_ledger_template_height_idx ON qbit_share_ledger (template_height); ALTER SEQUENCE qbit_share_ledger_share_seq_seq MAXVALUE 1000000; ALTER SEQUENCE qbit_pool_payout_entries_payout_entry_seq_seq INCREMENT BY 2")
        .execute(&pool).await?;
    let error = db
        .ledger("a")
        .await
        .err()
        .context("migration accepted a drifted 001 source")?
        .to_string();
    assert!(
        error.contains("refusing to migrate a drifted 001 source"),
        "{error}"
    );
    assert!(
        error.contains("does not match the v2.0.x release schema (v2.0.1, 001_share_ledger.sql), 6 object(s) differ"),
        "{error}"
    );
    assert!(
        error.contains("missing constraint qbit_block_candidate_outbox_share_id_fkey on qbit_block_candidate_outbox: FOREIGN KEY (share_id) REFERENCES qbit_share_ledger(share_id)"),
        "{error}"
    );
    assert!(
        error.contains(
            "column qbit_pool_blocks.parent_hash differs: expected NOT NULL, found nullable"
        ),
        "{error}"
    );
    assert!(
        error.contains(
            "column qbit_share_ledger.ntime differs: type expected bigint, found integer"
        ),
        "{error}"
    );
    assert!(
        error.contains("index qbit_share_ledger_template_height_idx differs: expected CREATE INDEX qbit_share_ledger_template_height_idx ON qbit_share_ledger USING btree (template_height, share_seq)")
            && error.contains(", found CREATE INDEX qbit_share_ledger_template_height_idx ON qbit_share_ledger USING btree (template_height)"),
        "{error}"
    );
    assert!(
        error.contains("sequence qbit_share_ledger_share_seq_seq differs: maximum expected 9223372036854775807, found 1000000"),
        "{error}"
    );
    assert!(
        error.contains("sequence qbit_pool_payout_entries_payout_entry_seq_seq differs: increment expected 1, found 2"),
        "{error}"
    );
    assert!(
        error.contains("Nothing was changed")
            && error.contains("Restore the pre-migration backup, or bring the database to the release schema with the 2.x.x release"),
        "{error}"
    );
    // The whole transaction rolled back: no native table, no scratch schema,
    // the 2.x.x rows and the drift exactly as they were.
    assert!(native_tables_absent(&pool).await?, "refusal ran DDL");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM pg_namespace WHERE nspname LIKE 'qbit_prism_scratch_%'"
        )
        .fetch_one(&pool)
        .await?,
        0,
        "scratch schema survived the refusal"
    );
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT block_hash,state FROM qbit_block_candidate_outbox ORDER BY block_hash",
    )
    .fetch_all(&pool)
    .await?;
    assert_eq!(rows, vec![(terminal.clone(), "submitted".into())]);
    assert!(sqlx::query_scalar::<_,bool>("SELECT NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=to_regclass('qbit_block_candidate_outbox') AND conname='qbit_block_candidate_outbox_share_id_fkey') AND EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass('qbit_share_ledger') AND attname='ntime' AND atttypid='integer'::regtype) AND (SELECT seqmax FROM pg_sequence WHERE seqrelid=to_regclass('qbit_share_ledger_share_seq_seq'))=1000000 AND (SELECT seqincrement FROM pg_sequence WHERE seqrelid=to_regclass('qbit_pool_payout_entries_payout_entry_seq_seq'))=2").fetch_one(&pool).await?, "refusal changed the source schema");
    // Back at the release schema (the index by re-running the 2.x.x file),
    // the source migrates and is still recorded as the v2.0.1 schema.
    sqlx::raw_sql("ALTER TABLE qbit_block_candidate_outbox ADD CONSTRAINT qbit_block_candidate_outbox_share_id_fkey FOREIGN KEY (share_id) REFERENCES qbit_share_ledger(share_id); ALTER TABLE qbit_pool_blocks ALTER COLUMN parent_hash SET NOT NULL; ALTER TABLE qbit_share_ledger ALTER COLUMN ntime TYPE bigint; DROP INDEX qbit_share_ledger_template_height_idx; ALTER SEQUENCE qbit_share_ledger_share_seq_seq NO MAXVALUE; ALTER SEQUENCE qbit_pool_payout_entries_payout_entry_seq_seq INCREMENT BY 1")
        .execute(&pool).await?;
    sqlx::raw_sql(FROZEN_2X_001).execute(&pool).await?;
    let ledger = db.ledger("a").await?;
    assert_eq!(
        ledger.migration_source().await?.map(|s| s.source_state),
        Some("pre_258".into())
    );
    exercise_native_writers(&ledger, 1, 5201).await?;
    pool.close().await;
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn drifted_002_object_is_refused_on_a_258_source() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    apply_frozen_2x_schema(&pool, SourceState::Applied258).await?;
    insert_v2_terminal(&pool, &legacy_hash(0x44), "abandoned").await?;
    // 3.x.x never re-applies 002, so its definitions are checked as found:
    // a replaced function body and a disabled trigger.
    sqlx::raw_sql("CREATE OR REPLACE FUNCTION qbit_prism_fact_oversized(value jsonb, expected text, byte_limit integer) RETURNS boolean AS $$ SELECT false $$ LANGUAGE sql IMMUTABLE; ALTER TABLE qbit_block_candidate_body_chunk DISABLE TRIGGER qbit_block_candidate_body_chunk_guard")
        .execute(&pool).await?;
    let error = db
        .ledger("a")
        .await
        .err()
        .context("migration accepted a drifted 002 object")?
        .to_string();
    assert!(
        error.contains("refusing to migrate a drifted 001 source"),
        "{error}"
    );
    assert!(
        error.contains(
            "(v2.0.2, 001_share_ledger.sql and 002_candidate_bodies.sql), 2 object(s) differ"
        ),
        "{error}"
    );
    assert!(
        error.contains("function qbit_prism_fact_oversized(value jsonb, expected text, byte_limit integer) differs: body"),
        "{error}"
    );
    assert!(
        error.contains("trigger qbit_block_candidate_body_chunk_guard on qbit_block_candidate_body_chunk differs: expected enabled, found disabled"),
        "{error}"
    );
    assert!(native_tables_absent(&pool).await?, "refusal ran DDL");
    assert_eq!(capability(&pool).await?, Some(2));
    // The v2.0.2 release re-creates its functions and triggers.
    sqlx::raw_sql(FROZEN_2X_002).execute(&pool).await?;
    let ledger = db.ledger("a").await?;
    assert_eq!(
        ledger.migration_source().await?.map(|s| s.source_state),
        Some("258_applied".into())
    );
    pool.close().await;
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn tolerated_source_differences_still_migrate() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    sqlx::raw_sql("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256) VALUES(repeat('aa',32),100,repeat('00',32),repeat('ab',32),repeat('ac',32))")
        .execute(&pool).await?;
    // Extra objects an operator may have added (the operator table brings
    // its own sequence); a column 001 creates in CREATE TABLE moved to a
    // different physical position with its data; the NOT VALID mark 001
    // itself leaves on an upgraded table; and a share sequence a 2.x.x
    // deployment has advanced, which is data, not structure.
    sqlx::raw_sql("CREATE TABLE operator_notes(note_id bigserial PRIMARY KEY, note text NOT NULL); ALTER TABLE qbit_share_ledger ADD COLUMN operator_note text; CREATE INDEX qbit_share_ledger_operator_idx ON qbit_share_ledger (miner_id); ALTER TABLE qbit_pool_blocks ADD COLUMN parent_hash_moved text; UPDATE qbit_pool_blocks SET parent_hash_moved=parent_hash; ALTER TABLE qbit_pool_blocks DROP COLUMN parent_hash; ALTER TABLE qbit_pool_blocks RENAME COLUMN parent_hash_moved TO parent_hash; ALTER TABLE qbit_pool_blocks ALTER COLUMN parent_hash SET NOT NULL; ALTER TABLE qbit_share_ledger DROP CONSTRAINT qbit_share_ledger_credit_policy_check; ALTER TABLE qbit_share_ledger ADD CONSTRAINT qbit_share_ledger_credit_policy_check CHECK (credit_policy IS NULL OR credit_policy IN ('stale-grace')) NOT VALID; SELECT setval('qbit_share_ledger_share_seq_seq', 5000000)")
        .execute(&pool).await?;
    let order: Vec<String> = sqlx::query_scalar("SELECT attname::text FROM pg_attribute WHERE attrelid=to_regclass('qbit_pool_blocks') AND attnum>0 AND NOT attisdropped ORDER BY attnum")
        .fetch_all(&pool).await?;
    assert_eq!(order.last().map(String::as_str), Some("parent_hash"));
    let ledger = db.ledger("a").await?;
    assert_eq!(schema_version(&pool).await?, REQUIRED_SCHEMA_VERSION);
    assert_eq!(
        ledger.migration_source().await?.map(|s| s.source_state),
        Some("pre_258".into())
    );
    // The extras and the moved column survive, with the data.
    assert!(sqlx::query_scalar::<_,bool>("SELECT to_regclass('operator_notes') IS NOT NULL AND to_regclass('qbit_share_ledger_operator_idx') IS NOT NULL AND EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass('qbit_share_ledger') AND attname='operator_note' AND NOT attisdropped)").fetch_one(&pool).await?);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT parent_hash FROM qbit_pool_blocks WHERE block_hash=repeat('aa',32)"
        )
        .fetch_one(&pool)
        .await?,
        "00".repeat(32)
    );
    exercise_native_writers(&ledger, 1, 5301).await?;
    // The advanced sequence kept its position: the first native share took
    // the next value after it.
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT min(share_seq) FROM qbit_share_ledger")
            .fetch_one(&pool)
            .await?,
        5_000_001
    );
    pool.close().await;
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn migrate_role_without_create_on_the_database_is_told_the_grant_it_needs() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    let database: String = sqlx::query_scalar("SELECT current_database()::text")
        .fetch_one(&pool)
        .await?;
    // A role that may create in the test schema but not in the database, and
    // that owns the 2.x.x objects because it applied them.
    let role = format!("prism_limited_{}", Uuid::new_v4().simple());
    sqlx::raw_sql(&format!(
        "CREATE ROLE {role} LOGIN PASSWORD 'limited'; GRANT USAGE, CREATE ON SCHEMA {} TO {role}",
        db.schema
    ))
    .execute(&pool)
    .await?;
    let mut limited = url::Url::parse(&db.url)?;
    limited
        .set_username(&role)
        .ok()
        .context("limited role username")?;
    limited
        .set_password(Some("limited"))
        .ok()
        .context("limited role password")?;
    let limited_pool = PgPool::connect(limited.as_str()).await?;
    apply_frozen_2x_schema(&limited_pool, SourceState::Pre258).await?;
    limited_pool.close().await;
    let error = Ledger::connect(limited.as_str(), "limited".into(), 8, true)
        .await
        .err()
        .context("migration ran the release-schema check without CREATE on the database")?
        .to_string();
    assert!(
        error.contains(&format!(
            "role {role} lacks the CREATE privilege on database {database}"
        )),
        "{error}"
    );
    assert!(
        error.contains(&format!("GRANT CREATE ON DATABASE {database} TO {role}"))
            && error.contains("nothing was changed"),
        "{error}"
    );
    assert!(native_tables_absent(&pool).await?, "refusal ran DDL");
    // With the grant the same role migrates.
    sqlx::raw_sql(&format!("GRANT CREATE ON DATABASE {database} TO {role}"))
        .execute(&pool)
        .await?;
    let ledger = Ledger::connect(limited.as_str(), "limited".into(), 8, true).await?;
    assert_eq!(
        ledger.migration_source().await?.map(|s| s.source_state),
        Some("pre_258".into())
    );
    ledger.pool.close().await;
    sqlx::raw_sql(&format!("DROP OWNED BY {role}; DROP ROLE {role}"))
        .execute(&pool)
        .await?;
    pool.close().await;
    db.close(vec![]).await
}

#[tokio::test]
async fn newer_storage_version_or_capability_is_refused_at_migrate_and_at_connect() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    apply_frozen_2x_schema(&pool, SourceState::Applied258).await?;
    sqlx::raw_sql("UPDATE qbit_prism_schema_capabilities SET capability_value=3 WHERE capability='candidate_storage_version'")
        .execute(&pool).await?;
    let error = db
        .ledger("a")
        .await
        .err()
        .context("migration accepted candidate_storage_version = 3")?
        .to_string();
    assert!(error.contains("newer source before any DDL"), "{error}");
    assert!(
        error.contains("candidate_storage_version = 3, but this server understands candidate_storage_version 1 to 2"),
        "{error}"
    );
    assert!(native_tables_absent(&pool).await?, "refusal ran DDL");
    sqlx::raw_sql("UPDATE qbit_prism_schema_capabilities SET capability_value=2; INSERT INTO qbit_prism_schema_capabilities(capability,capability_value) VALUES('sealed_share_pages',1)")
        .execute(&pool).await?;
    let error = db.ledger("a").await.err().context("accepted")?.to_string();
    assert!(
        error.contains("capability sealed_share_pages = 1, which this server does not understand"),
        "{error}"
    );
    assert!(native_tables_absent(&pool).await?, "refusal ran DDL");
    sqlx::raw_sql(
        "DELETE FROM qbit_prism_schema_capabilities WHERE capability='sealed_share_pages'",
    )
    .execute(&pool)
    .await?;
    let ledger = db.ledger("a").await?;

    // The same refusal at connect, with and without initialize, on the
    // migrated database.
    sqlx::raw_sql("UPDATE qbit_prism_schema_capabilities SET capability_value=3 WHERE capability='candidate_storage_version'")
        .execute(&pool).await?;
    for initialize in [false, true] {
        let error = Ledger::connect(&db.url, "b".into(), 8, initialize)
            .await
            .err()
            .with_context(|| {
                format!("connect(initialize={initialize}) accepted candidate_storage_version = 3")
            })?
            .to_string();
        assert!(error.contains("candidate_storage_version = 3"), "{error}");
    }
    sqlx::raw_sql("UPDATE qbit_prism_schema_capabilities SET capability_value=2; INSERT INTO qbit_prism_schema_capabilities(capability,capability_value) VALUES('sealed_share_pages',1)")
        .execute(&pool).await?;
    let error = Ledger::connect(&db.url, "b".into(), 8, false)
        .await
        .err()
        .context("connect accepted an unknown capability")?
        .to_string();
    assert!(error.contains("sealed_share_pages"), "{error}");
    sqlx::raw_sql(
        "DELETE FROM qbit_prism_schema_capabilities WHERE capability='sealed_share_pages'",
    )
    .execute(&pool)
    .await?;
    let follower = Ledger::connect(&db.url, "b".into(), 8, false).await?;
    pool.close().await;
    db.close(vec![ledger, follower]).await
}

#[tokio::test]
async fn startup_without_initialize_requires_the_current_schema_version() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    // A 2.x.x database the native migrate has not seen.
    let error = Ledger::connect(&db.url, "cold".into(), 8, false)
        .await
        .err()
        .context("a non-initializing start accepted a 2.x.x database")?
        .to_string();
    assert!(
        error.contains(&format!(
            "requires schema version {REQUIRED_SCHEMA_VERSION}"
        )),
        "{error}"
    );
    assert!(error.contains("qbit-prism-server migrate"), "{error}");
    assert!(
        native_tables_absent(&pool).await?,
        "a non-initializing start ran DDL"
    );

    // An older native schema: everything but the newest migration.
    let ledger = db.ledger("init").await?;
    sqlx::query("DELETE FROM qbit_prism_schema_migrations WHERE version=$1")
        .bind(REQUIRED_SCHEMA_VERSION)
        .execute(&pool)
        .await?;
    let error = Ledger::connect(&db.url, "cold".into(), 8, false)
        .await
        .err()
        .context("a non-initializing start accepted an older schema")?
        .to_string();
    assert!(
        error.contains(&format!(
            "schema version {} is below the version {REQUIRED_SCHEMA_VERSION} this server requires",
            REQUIRED_SCHEMA_VERSION - 1
        )),
        "{error}"
    );
    assert!(error.contains("qbit-prism-server migrate"), "{error}");
    // Initializing brings it forward again, without rewriting the source record.
    let repaired = db.ledger("init-again").await?;
    assert_eq!(schema_version(&pool).await?, REQUIRED_SCHEMA_VERSION);
    assert_eq!(
        repaired.migration_source().await?.map(|s| s.migrated_by),
        Some("init".into())
    );

    // A newer schema: a later release's additive migration ran first, and a
    // frontend still on this release keeps starting during the rollout. A
    // format it must not touch is declared as a capability, which
    // newer_storage_version_or_capability_is_refused_at_migrate_and_at_connect
    // covers.
    sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES($1)")
        .bind(REQUIRED_SCHEMA_VERSION + 1)
        .execute(&pool)
        .await?;
    let follower = Ledger::connect(&db.url, "cold".into(), 8, false)
        .await
        .context("a start refused an additive newer schema")?;
    assert_eq!(
        schema_version(&pool).await?,
        REQUIRED_SCHEMA_VERSION + 1,
        "a non-initializing start rewrote the newer schema"
    );
    // Initializing on it is a no-op too: no migration is reapplied and the
    // source record stands.
    let initializer = db.ledger("init-on-newer").await?;
    assert_eq!(schema_version(&pool).await?, REQUIRED_SCHEMA_VERSION + 1);
    assert_eq!(
        initializer.migration_source().await?.map(|s| s.migrated_by),
        Some("init".into())
    );
    pool.close().await;
    db.close(vec![ledger, repaired, follower, initializer])
        .await
}

#[tokio::test]
async fn unknown_storage_version_row_is_parked_and_not_reclaimed_at_lease_expiry() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    a.append(share(1), None).await?;
    let unknown = candidate(&a.snapshot(100).await?, 6001)?;
    a.enqueue_candidate(unknown.clone()).await?;
    // A later release's storage version on a row this binary cannot decode.
    sqlx::query("UPDATE qbit_block_candidate_outbox SET storage_version=3 WHERE block_hash=$1")
        .bind(&unknown.block_hash)
        .execute(&a.pool)
        .await?;
    assert!(
        a.claim_candidate(60).await?.is_none(),
        "claimed an unknown version"
    );
    let (token, last_error, parked, attempts, state) =
        parked_row(&a.pool, &unknown.block_hash).await?;
    assert!(token.is_none(), "parked row kept its claim");
    assert!(
        last_error
            .as_deref()
            .is_some_and(|e| e.contains("storage_version 3")),
        "{last_error:?}"
    );
    assert!(parked, "next_attempt_at was not moved past every lease");
    assert_eq!((attempts, state.as_str()), (1, "pending"));
    // The next lease expiry, on either instance, does not offer it again.
    sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_expires_at=clock_timestamp()-interval '1 second' WHERE block_hash=$1")
        .bind(&unknown.block_hash).execute(&a.pool).await?;
    assert!(b.claim_candidate(60).await?.is_none());
    assert!(a.claim_candidate(60).await?.is_none());
    let (_, _, _, attempts, _) = parked_row(&a.pool, &unknown.block_hash).await?;
    assert_eq!(attempts, 1, "parked row was re-claimed");
    // Ordinary work is unaffected.
    let good = candidate(&a.snapshot(100).await?, 6002)?;
    a.enqueue_candidate(good.clone()).await?;
    let claim = b.claim_candidate(60).await?.context("good candidate")?;
    assert_eq!(claim.candidate.block_hash, good.block_hash);
    b.finish_candidate(&claim, false, Some("test")).await?;
    // Parking is operator-reversible: a release that reads the row, or an
    // operator who converted it, resets next_attempt_at.
    sqlx::query("UPDATE qbit_block_candidate_outbox SET storage_version=1,next_attempt_at=clock_timestamp() WHERE block_hash=$1")
        .bind(&unknown.block_hash).execute(&a.pool).await?;
    let claim = a.claim_candidate(60).await?.context("unparked candidate")?;
    assert_eq!(claim.candidate.block_hash, unknown.block_hash);
    db.close(vec![a, b]).await
}

async fn seed_legacy_carry(pool: &PgPool) -> Result<()> {
    sqlx::raw_sql("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state) VALUES(repeat('aa',32),100,repeat('00',32),repeat('ab',32),repeat('ac',32),'confirmed'),(repeat('ee',32),101,repeat('aa',32),repeat('ef',32),repeat('e0',32),'prepared'); INSERT INTO qbit_payout_carry_forward(block_height,block_hash,miner_id,payout_order_key,p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,onchain_amount_sats,carry_forward_balance_sats,action) VALUES(100,repeat('aa',32),'miner-a','a',decode(repeat('11',32),'hex'),1000,0,1000,0,1000,'accrued'),(101,repeat('ee',32),'miner-b','b',decode(repeat('22',32),'hex'),500,0,500,0,500,'accrued'); DELETE FROM qbit_payout_carry_forward_current; UPDATE qbit_pool_blocks SET chain_state='confirmed' WHERE block_hash=repeat('ee',32);")
        .execute(pool).await?;
    Ok(())
}

async fn carry_summary(pool: &PgPool) -> Result<serde_json::Value> {
    Ok(sqlx::query_scalar("SELECT coalesce(jsonb_agg(jsonb_build_object('miner',miner_id,'balance',balance_sats::text,'count',active_row_count) ORDER BY miner_id),'[]'::jsonb) FROM qbit_payout_carry_forward_current")
        .fetch_one(pool).await?)
}

#[tokio::test]
async fn legacy_2x_upgrade_repairs_partial_carry_seed_and_preserves_shared_state() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    // This is the standalone 2.x schema, with its own BEGIN/COMMIT and no
    // native tables. Reproduce the interrupted legacy apply's partial seed.
    apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    seed_legacy_carry(&pool).await?;
    assert_eq!(
        carry_summary(&pool).await?,
        json!([{"miner":"miner-b","balance":"500","count":1}])
    );
    sqlx::raw_sql("INSERT INTO qbit_worker_difficulty(listener,worker_username,difficulty,evidence_at) VALUES('primary','miner.worker',42,clock_timestamp()); INSERT INTO qbit_hashrate_rollup_progress(singleton,last_share_seq) VALUES(true,123);")
        .execute(&pool).await?;
    let ordinals: Vec<i64> = sqlx::query_scalar(
        "SELECT audit_publication_sequence FROM qbit_pool_blocks ORDER BY block_hash",
    )
    .fetch_all(&pool)
    .await?;
    let ledger = db.ledger("native-upgrade").await?;
    assert_eq!(
        carry_summary(&pool).await?,
        json!([{"miner":"miner-a","balance":"1000","count":1},{"miner":"miner-b","balance":"500","count":1}])
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM qbit_carry_forward_current_drift()")
            .fetch_one(&pool)
            .await?,
        0
    );
    assert_eq!(schema_version(&pool).await?, REQUIRED_SCHEMA_VERSION);
    assert_eq!(
        ledger.migration_source().await?.map(|s| s.source_state),
        Some("pre_258".into())
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT last_share_seq FROM qbit_hashrate_rollup_progress")
            .fetch_one(&pool)
            .await?,
        123
    );
    assert_eq!(
        ledger
            .worker_difficulty("primary", "miner.worker", 60)
            .await?
            .unwrap()
            .difficulty,
        42.0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT audit_publication_sequence FROM qbit_pool_blocks ORDER BY block_hash"
        )
        .fetch_all(&pool)
        .await?,
        ordinals
    );
    assert!(sqlx::query("INSERT INTO qbit_ledger_writer_lease(singleton,writer_id,writer_epoch,writer_session_token,lease_expires_at) VALUES(true,'python',1,'token',clock_timestamp()+interval '1 hour')").execute(&pool).await.is_err());
    pool.close().await;
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn legacy_2x_failed_native_migration_rolls_back_seed_repair_and_all_ddl() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    seed_legacy_carry(&pool).await?;
    // Balance-only damage exercises the guard's drift comparison even when
    // active row counts match. Failure is deliberately after the base schema.
    sqlx::raw_sql("SELECT qbit_rebuild_carry_forward_current_balances(); UPDATE qbit_payout_carry_forward_current SET balance_sats=balance_sats+1 WHERE miner_id='miner-a'; CREATE TABLE qbit_prism_share_hashes(unexpected text);").execute(&pool).await?;
    let poisoned = carry_summary(&pool).await?;
    assert!(db.ledger("failed-upgrade").await.is_err());
    assert_eq!(
        carry_summary(&pool).await?,
        poisoned,
        "base schema committed before the native migration failed"
    );
    assert!(!sqlx::query_scalar::<_,bool>("SELECT to_regclass('qbit_prism_cluster') IS NOT NULL OR to_regclass('qbit_prism_schema_migrations') IS NOT NULL").fetch_one(&pool).await?);
    sqlx::query("DROP TABLE qbit_prism_share_hashes")
        .execute(&pool)
        .await?;
    let ledger = db.ledger("retry-upgrade").await?;
    assert_eq!(
        carry_summary(&pool).await?,
        json!([{"miner":"miner-a","balance":"1000","count":1},{"miner":"miner-b","balance":"500","count":1}])
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM qbit_carry_forward_current_drift()")
            .fetch_one(&pool)
            .await?,
        0
    );
    pool.close().await;
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn native_publication_ordinal_survives_reactivation_and_excludes_rejected_candidates(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("a").await?;
    ledger.append(share(1), None).await?;
    let block = candidate(&ledger.snapshot(100).await?, 3001)?;
    ledger.enqueue_candidate(block.clone()).await?;
    let claim = ledger.claim_candidate(60).await?.unwrap();
    ledger
        .land_candidate(&claim, &keys().1.public_key_hex())
        .await?;
    assert_eq!(
        sqlx::query_scalar::<_, Option<i64>>(
            "SELECT audit_publication_sequence FROM qbit_pool_blocks WHERE block_hash=$1"
        )
        .bind(&block.block_hash)
        .fetch_one(&ledger.pool)
        .await?,
        None
    );
    ledger.finish_candidate(&claim, true, None).await?;
    let ordinal: i64 = sqlx::query_scalar(
        "SELECT audit_publication_sequence FROM qbit_pool_blocks WHERE block_hash=$1",
    )
    .bind(&block.block_hash)
    .fetch_one(&ledger.pool)
    .await?;
    ledger
        .reconcile_blocks(
            &[BlockObservation {
                block_hash: block.block_hash.clone(),
                active: false,
            }],
            101,
        )
        .await?;
    let inactive=sqlx::query("SELECT audit_publication_sequence,inactive_since IS NOT NULL AS disconnected,chain_state,maturity_state FROM qbit_pool_blocks WHERE block_hash=$1").bind(&block.block_hash).fetch_one(&ledger.pool).await?;
    assert_eq!(
        inactive.try_get::<i64, _>("audit_publication_sequence")?,
        ordinal
    );
    assert!(inactive.try_get::<bool, _>("disconnected")?);
    assert_eq!(inactive.try_get::<String, _>("chain_state")?, "inactive");
    assert_eq!(inactive.try_get::<String, _>("maturity_state")?, "immature");
    ledger
        .reconcile_blocks(
            &[BlockObservation {
                block_hash: block.block_hash.clone(),
                active: true,
            }],
            101,
        )
        .await?;
    assert!(sqlx::query_scalar::<_,bool>("SELECT inactive_since IS NULL AND audit_publication_sequence=$2 FROM qbit_pool_blocks WHERE block_hash=$1").bind(&block.block_hash).bind(ordinal).fetch_one(&ledger.pool).await?);
    let rejected = candidate(&ledger.snapshot(100).await?, 3002)?;
    ledger.enqueue_candidate(rejected.clone()).await?;
    let claim = ledger.claim_candidate(60).await?.unwrap();
    ledger
        .land_candidate(&claim, &keys().1.public_key_hex())
        .await?;
    ledger
        .finish_candidate(&claim, false, Some("node rejected"))
        .await?;
    assert!(sqlx::query_scalar::<_,bool>("SELECT audit_publication_sequence IS NULL AND inactive_since IS NULL FROM qbit_pool_blocks WHERE block_hash=$1").bind(&rejected.block_hash).fetch_one(&ledger.pool).await?);
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn canonical_2x_sidecar_import_preserves_exact_bytes_and_fails_closed() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("a").await?;
    ledger.append(share(1), None).await?;
    let block = candidate(&ledger.snapshot(100).await?, 4001)?;
    ledger.enqueue_candidate(block.clone()).await?;
    let claim = ledger.claim_candidate(60).await?.unwrap();
    let report = ledger
        .land_candidate(&claim, &keys().1.public_key_hex())
        .await?;
    let expected = qbit_prism::canonical_audit_bundle_bytes(&block.bundle)?;
    assert_eq!(
        audit_canonical_bytes(&ledger.pool, &block.block_hash).await?,
        Some(expected.clone())
    );
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT canonical_audit_bytes IS NULL FROM qbit_pool_audit_bundles WHERE block_hash=$1"
        )
        .bind(&block.block_hash)
        .fetch_one(&ledger.pool)
        .await?,
        "native range snapshots must not duplicate their full share window"
    );
    let dir = tempfile::tempdir()?;
    let body_path = dir.path().join("legacy-audit.json");
    std::fs::write(&body_path, serde_json::to_vec(&block.bundle)?)?;
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=$2,share_snapshot_sha256=NULL,body_uri=$3 WHERE block_hash=$1").bind(&block.block_hash).bind(serde_json::to_value(&block.bundle)?).bind(body_path.to_str().unwrap()).execute(&ledger.pool).await?;
    assert_eq!(
        audit_canonical_bytes(&ledger.pool, &block.block_hash).await?,
        None
    );
    let path = dir.path().join(format!(
        "prism-audit-bundle-canonical-{}-{}.json.gz",
        block.block_hash, report.audit_bundle_sha256_hex
    ));
    std::fs::write(&path, b"corrupt gzip")?;
    assert!(
        ledger
            .import_legacy_audits(Some(dir.path()), &keys().1.public_key_hex())
            .await
            .is_err(),
        "corrupt present sidecar fell back to a valid body"
    );
    let write_gzip = |bytes: &[u8]| -> Result<()> {
        let mut encoder = flate2::GzBuilder::new()
            .mtime(0)
            .write(std::fs::File::create(&path)?, flate2::Compression::best());
        encoder.write_all(bytes)?;
        encoder.finish()?;
        Ok(())
    };
    write_gzip(b"{}")?;
    assert!(ledger
        .import_legacy_audits(Some(dir.path()), &keys().1.public_key_hex())
        .await
        .is_err());
    write_gzip(&expected)?;
    assert!(ledger
        .import_legacy_audits(Some(dir.path()), &keys().0.public_key_hex())
        .await
        .is_err());
    assert_eq!(
        ledger
            .import_legacy_audits(Some(dir.path()), &keys().1.public_key_hex())
            .await?,
        1
    );
    assert_eq!(
        ledger
            .import_legacy_audits(Some(dir.path()), &keys().1.public_key_hex())
            .await?,
        0
    );
    assert_eq!(
        audit_canonical_bytes(&ledger.pool, &block.block_hash).await?,
        Some(expected)
    );
    assert!(path.exists() && body_path.exists());
    sqlx::query("UPDATE qbit_pool_audit_bundles SET canonical_audit_bytes=$2 WHERE block_hash=$1")
        .bind(&block.block_hash)
        .bind(b"{}".to_vec())
        .execute(&ledger.pool)
        .await?;
    assert!(audit_canonical_bytes(&ledger.pool, &block.block_hash)
        .await
        .is_err());
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn durable_worker_hints_order_by_shared_evidence_and_keep_original_ttl() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("a").await?;
    let b = db.ledger("b").await?;
    let accepted = share(8001);
    a.append(accepted.clone(), None).await?;
    let evidence = a.share_accepted_at_ms(&accepted.share_id).await?.unwrap();
    assert!(
        a.record_worker_difficulty("primary", "alice.worker", 32.0, evidence)
            .await?
    );
    assert!(
        !b.record_worker_difficulty("primary", "alice.worker", 64.0, evidence - 1)
            .await?
    );
    assert!(
        b.record_worker_difficulty("primary", "alice.worker", 48.0, evidence)
            .await?
    );
    assert!(
        a.lower_worker_difficulty("primary", "alice.worker", 8.0)
            .await?
    );
    assert!(
        !a.lower_worker_difficulty("primary", "alice.worker", 16.0)
            .await?
    );
    let hint = b
        .worker_difficulty("primary", "alice.worker", 60)
        .await?
        .unwrap();
    assert_eq!(hint.difficulty, 8.0);
    assert_eq!(hint.evidence_at_ms, evidence);
    assert!(hint.age_ms < 60_000);
    assert!(b
        .worker_difficulty("highdiff", "alice.worker", 60)
        .await?
        .is_none());
    assert!(b
        .worker_difficulty("primary", "Alice.worker", 60)
        .await?
        .is_none());
    for invalid in [f64::NAN, f64::INFINITY, 0.0, -1.0] {
        assert!(a
            .record_worker_difficulty("primary", "invalid", invalid, evidence)
            .await
            .is_err());
    }
    assert!(
        a.record_worker_difficulty("primary", "expired-a", 64.0, evidence - 120_000)
            .await?
    );
    assert!(
        a.record_worker_difficulty("primary", "expired-b", 64.0, evidence - 120_000)
            .await?
    );
    assert!(
        a.lower_worker_difficulty("primary", "expired-a", 1.0)
            .await?
    );
    assert!(a
        .worker_difficulty("primary", "expired-a", 60)
        .await?
        .is_none());
    assert_eq!(a.prune_worker_difficulties(60, 1).await?, 1);
    assert_eq!(b.prune_worker_difficulties(60, 1).await?, 1);
    assert_eq!(a.prune_worker_difficulties(60, 1).await?, 0);
    db.close(vec![a, b]).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn audit_range_query_uses_deadline_remaining_after_delayed_snapshot() -> Result<()> {
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use qbit_prism_server::api::{self, ApiConfig, ApiState};
    use std::time::{Duration, Instant};
    use tower::ServiceExt;
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("audit-deadline").await?;
    ledger.append(share(9001), None).await?;
    let block = candidate(&ledger.snapshot(100).await?, 9001)?;
    ledger.enqueue_candidate(block.clone()).await?;
    let claim = ledger.claim_candidate(60).await?.unwrap();
    let report = ledger
        .land_candidate(&claim, &keys().1.public_key_hex())
        .await?;
    let app = api::router(ApiState::new(
        ledger.pool.clone(),
        ApiConfig {
            cache_enabled: false,
            read_timeout: Duration::from_millis(800),
            ..Default::default()
        },
        std::sync::Arc::new(qbit_prism_server::metrics::Metrics::default()),
    ));
    let path = format!("/public/v1/artifacts/{}", report.audit_bundle_sha256_hex);
    let mut snapshot_lock = ledger.pool.begin().await?;
    let snapshot_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *snapshot_lock)
        .await?;
    sqlx::query("LOCK TABLE qbit_prism_audit_snapshots IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *snapshot_lock)
        .await?;
    let mut share_lock = ledger.pool.begin().await?;
    let share_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *share_lock)
        .await?;
    sqlx::query("LOCK TABLE qbit_share_ledger IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *share_lock)
        .await?;
    let request = Request::builder().uri(&path).body(Body::empty())?;
    let started = Instant::now();
    let response = tokio::spawn(app.clone().oneshot(request));
    // Wait until the real public query spends its first part of the budget
    // behind the snapshot lock, then move it onto the independently held
    // share table lock. A reused transaction would still have ~800ms there.
    tokio::time::timeout(Duration::from_millis(500),async {
        loop {
            let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='prism-public-read' AND state='active' AND wait_event_type='Lock' AND $1=ANY(pg_blocking_pids(pid)))").bind(snapshot_pid).fetch_one(&db.admin).await?;
            if waiting {return Ok::<_,anyhow::Error>(())}
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await??;
    tokio::time::sleep_until(tokio::time::Instant::from_std(
        started + Duration::from_millis(500),
    ))
    .await;
    snapshot_lock.rollback().await?;
    tokio::time::timeout(Duration::from_millis(250),async {
        loop {
            let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='prism-public-read' AND state='active' AND wait_event_type='Lock' AND $1=ANY(pg_blocking_pids(pid)))").bind(share_pid).fetch_one(&db.admin).await?;
            if waiting {return Ok::<_,anyhow::Error>(())}
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await??;
    let response = response.await??;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let error: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await?)?;
    assert_eq!(error["error"]["code"], "read_timeout");
    assert!(started.elapsed() < Duration::from_millis(1100));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let waiting:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE application_name='prism-public-read' AND state='active' AND wait_event_type='Lock' AND $1=ANY(pg_blocking_pids(pid))").bind(share_pid).fetch_one(&db.admin).await?;
    assert_eq!(
        waiting, 0,
        "late audit range SQL outlived the HTTP deadline and occupied the public pool"
    );
    share_lock.rollback().await?;
    let response = app
        .oneshot(Request::builder().uri(&path).body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), usize::MAX).await?;
    assert_eq!(
        bytes.as_ref(),
        qbit_prism::canonical_audit_bundle_bytes(&block.bundle)?
    );
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn compact_bits_metadata_comes_from_durable_header_and_recovers_without_audit_changes(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let a = db.ledger("bits-a").await?;
    let b = db.ledger("bits-b").await?;
    a.append(share(9101), None).await?;
    let mut block = candidate(&a.snapshot(100).await?, 9101)?;
    let canonical = qbit_prism::canonical_audit_bundle_bytes(&block.bundle)?;
    // Deliberately asymmetric compact bytes prove display endianness. Their
    // value is independent of the audit's scaled network-difficulty integer.
    let mut bytes = hex::decode(&block.block_hex)?;
    bytes[72..76].copy_from_slice(&0x1d00ffffu32.to_le_bytes());
    let mut hash = Sha256::digest(Sha256::digest(&bytes[..80])).to_vec();
    hash.reverse();
    block.block_hash = hex::encode(hash);
    block.block_hex = hex::encode(bytes);
    a.enqueue_candidate(block.clone()).await?;
    let claim = a.claim_candidate(60).await?.unwrap();
    let report = a.land_candidate(&claim, &keys().1.public_key_hex()).await?;
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT found_block_bits FROM qbit_pool_audit_bundles WHERE block_hash=$1"
        )
        .bind(&block.block_hash)
        .fetch_one(&a.pool)
        .await?,
        "1d00ffff"
    );
    assert_eq!(
        report.audit_bundle_sha256_hex,
        hex::encode(Sha256::digest(&canonical))
    );
    // Simulate a pre-fix prepared row and an owner crash. The next physical
    // instance recovers the same old candidate format with its original header.
    sqlx::query("UPDATE qbit_pool_audit_bundles SET found_block_bits=NULL WHERE block_hash=$1")
        .bind(&block.block_hash)
        .execute(&a.pool)
        .await?;
    sqlx::query("UPDATE qbit_block_candidate_outbox SET claim_expires_at=clock_timestamp()-interval '1 second' WHERE block_hash=$1").bind(&block.block_hash).execute(&a.pool).await?;
    let recovered = b.claim_candidate(60).await?.unwrap();
    assert_eq!(recovered.candidate.block_hex, block.block_hex);
    let recovered_report = b
        .land_candidate(&recovered, &keys().1.public_key_hex())
        .await?;
    assert_eq!(
        recovered_report.audit_bundle_sha256_hex,
        report.audit_bundle_sha256_hex
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT found_block_bits FROM qbit_pool_audit_bundles WHERE block_hash=$1"
        )
        .bind(&block.block_hash)
        .fetch_one(&b.pool)
        .await?,
        "1d00ffff"
    );
    assert_eq!(
        audit_canonical_bytes(&b.pool, &block.block_hash).await?,
        Some(canonical)
    );
    sqlx::query(
        "UPDATE qbit_pool_audit_bundles SET found_block_bits='207fffff' WHERE block_hash=$1",
    )
    .bind(&block.block_hash)
    .execute(&b.pool)
    .await?;
    assert!(
        b.land_candidate(&recovered, &keys().1.public_key_hex())
            .await
            .is_err(),
        "idempotent landing accepted contradictory header metadata"
    );
    db.close(vec![a, b]).await
}
