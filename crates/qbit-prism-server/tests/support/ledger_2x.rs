//! The frozen 2.x.x source schema, and every upgrade test that starts from it.
//!
//! The fixtures under `tests/fixtures/schema_2x` are byte-exact copies of the
//! 2.x.x release SQL (see the README there). Upgrade tests build their source
//! from those copies, never from the live in-tree files, so a DDL edit to the
//! live `001_share_ledger.sql` cannot be absorbed silently: the digest tests
//! below pin the fixtures, pin the live file, and compare the two.
use super::*;
use qbit_prism_server::ledger::{
    audit_canonical_bytes, MigrationSource, SourceState, NOT_VALID_EXEMPT,
    REQUIRED_SCHEMA_VERSIONS, SOURCE_STATES,
};
use serde_json::Value;
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
             crates/qbit-prism-server/migrations/ (007 is next; 008 and 009 are taken), then add it to \
             REQUIRED_SCHEMA_VERSIONS in src/ledger/migration.rs",
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

/// The constraints a SQL file adds with `NOT VALID` and never validates
/// afterwards, as (table, constraint) pairs. Comments are removed and the
/// text is split on `;`; quotes and parentheses are dropped so a statement
/// built with `format('ALTER TABLE %I.t ' 'ADD CONSTRAINT ' ...)` reads like
/// a plain one, and a `%I.` schema placeholder is removed from the table
/// name.
fn constraints_left_not_valid(sql: &str) -> Vec<(String, String)> {
    let text = sql_statements(sql)
        .join(" ")
        .replace('\'', "")
        .replace(['(', ')'], " ");
    let mut added = Vec::new();
    let mut validated = Vec::new();
    for statement in text.split(';') {
        let words: Vec<&str> = statement
            .split_whitespace()
            .map(|word| word.trim_end_matches(','))
            .collect();
        let after = |keyword: [&str; 2]| {
            words
                .windows(2)
                .position(|pair| pair == keyword)
                .map(|index| (index, words.get(index + 2)))
                .and_then(|(index, word)| {
                    word.map(|word| (index, word.trim_start_matches("%I.").to_owned()))
                })
        };
        let Some((_, table)) = after(["ALTER", "TABLE"]) else {
            continue;
        };
        if let Some((index, name)) = after(["ADD", "CONSTRAINT"]) {
            if words[index..]
                .windows(2)
                .any(|pair| pair == ["NOT", "VALID"])
            {
                added.push((table.clone(), name));
            }
        }
        if let Some((_, name)) = after(["VALIDATE", "CONSTRAINT"]) {
            validated.push((table, name));
        }
    }
    added.retain(|pair| !validated.contains(pair));
    added.sort();
    added.dedup();
    added
}

#[test]
fn frozen_release_not_valid_constraints_are_exactly_the_pinned_exemptions() {
    // The parser: a plain ADD ... NOT VALID, one built with format(), a
    // validated add, and one validated later in the same file.
    assert_eq!(
        constraints_left_not_valid(
            "ALTER TABLE t ADD CONSTRAINT t_a_check CHECK (a > 0) NOT VALID; -- kept\n\
             EXECUTE format('ALTER TABLE %I.u ' 'ADD CONSTRAINT ' 'u_b_check ' 'CHECK (b > 0) NOT VALID', ns);\n\
             ALTER TABLE v ADD CONSTRAINT v_c_check CHECK (c > 0);\n\
             ALTER TABLE w ADD CONSTRAINT w_d_check CHECK (d > 0) NOT VALID;\n\
             EXECUTE format('ALTER TABLE %I.w ' 'VALIDATE CONSTRAINT ' 'w_d_check', ns);\n"
        ),
        vec![
            ("t".to_owned(), "t_a_check".to_owned()),
            ("u".to_owned(), "u_b_check".to_owned())
        ]
    );
    let mut left = constraints_left_not_valid(FROZEN_2X_001);
    left.extend(constraints_left_not_valid(FROZEN_2X_002));
    left.sort();
    let pinned: Vec<(String, String)> = NOT_VALID_EXEMPT
        .iter()
        .map(|(table, name)| ((*table).to_owned(), (*name).to_owned()))
        .collect();
    assert_eq!(
        left, pinned,
        "NOT_VALID_EXEMPT in src/ledger/migration.rs must be exactly the constraints the frozen \
         2.x.x release SQL (tests/fixtures/schema_2x) adds NOT VALID and never validates"
    );
    assert_eq!(
        pinned,
        vec![(
            "qbit_share_ledger".to_owned(),
            "qbit_share_ledger_credit_policy_check".to_owned()
        )]
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
            (
                "partial 001",
                "refuse before any DDL, naming the objects present"
            ),
            ("pre-#258", "accept after the drain check"),
            ("#258 applied", "accept after the drain check"),
            ("partial 002", "refuse, naming the missing object"),
            ("newer", "refuse before any DDL"),
            (
                "native collision",
                "refuse before any DDL, naming the objects"
            ),
            ("drifted 001", "refuse transactionally, naming the object"),
        ]
    );
    assert_eq!(SourceState::Pre258.release().map(|r| r.0), Some("2.0.1"));
    assert_eq!(
        SourceState::Applied258.release().map(|r| r.1),
        Some(RELEASE_COMMIT_2_0_2)
    );
    assert_eq!(REQUIRED_SCHEMA_VERSIONS, [2, 3, 4, 5, 6, 8, 9]);
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

/// Leave only the named tables, sequences and functions of a frozen 001
/// apply in place, as a selective restore does: every other function, then
/// every other table (with what depends on it) and standalone sequence, is
/// dropped. A kept table keeps its indexes; its triggers go with the
/// functions they call.
async fn leave_only(pool: &PgPool, keep: &[&str]) -> Result<()> {
    let keep = keep
        .iter()
        .map(|name| format!("'{name}'"))
        .collect::<Vec<_>>()
        .join(",");
    sqlx::raw_sql(&format!(
        "DO $$ DECLARE item record; BEGIN
           FOR item IN SELECT p.oid::regprocedure::text AS signature FROM pg_proc p WHERE p.pronamespace=current_schema()::regnamespace AND p.proname<>ALL(ARRAY[{keep}]::text[]) LOOP
             EXECUTE format('DROP FUNCTION %s CASCADE', item.signature);
           END LOOP;
           FOR item IN SELECT c.relname, c.relkind FROM pg_class c WHERE c.relnamespace=current_schema()::regnamespace AND c.relkind IN ('r','S') AND c.relname<>ALL(ARRAY[{keep}]::text[]) LOOP
             IF item.relkind='r' THEN EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', item.relname);
             ELSE EXECUTE format('DROP SEQUENCE IF EXISTS %I CASCADE', item.relname); END IF;
           END LOOP;
         END $$"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

/// Every relation and function of the test schema, so a refusal can be
/// shown to have changed nothing.
async fn schema_objects(pool: &PgPool) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar("SELECT relkind::text||' '||relname::text FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relkind IN ('r','S','i') UNION ALL SELECT 'f '||oid::regprocedure::text FROM pg_proc WHERE pronamespace=current_schema()::regnamespace ORDER BY 1")
        .fetch_all(pool).await?)
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

/// Every applied migration, in order: the gate checks each one, so no test
/// reads a high-water mark.
async fn schema_versions(pool: &PgPool) -> Result<Vec<i32>> {
    Ok(
        sqlx::query_scalar("SELECT version FROM qbit_prism_schema_migrations ORDER BY version")
            .fetch_all(pool)
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
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
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
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
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
async fn partial_001_source_is_refused_before_any_ddl_naming_the_objects_present() -> Result<()> {
    // Three selective restores of a v2.0.1 database, each without
    // qbit_share_ledger: one table with its indexes, one standalone
    // sequence, one function. Each object is the release definition,
    // because it came from applying the frozen 001 and dropping the rest.
    for (keep, named, count) in [
        (
            "qbit_pool_blocks",
            vec![
                "table qbit_pool_blocks",
                "index qbit_pool_blocks_audit_publication_sequence_idx on qbit_pool_blocks",
                "index qbit_pool_blocks_maturity_idx on qbit_pool_blocks",
                "index qbit_pool_blocks_public_recent_idx on qbit_pool_blocks",
            ],
            4,
        ),
        (
            "qbit_audit_publication_sequence_seq",
            vec!["sequence qbit_audit_publication_sequence_seq"],
            1,
        ),
        (
            "qbit_prism_window",
            vec!["function qbit_prism_window(anchor_job_issued_at timestamp with time zone, window_weight numeric)"],
            1,
        ),
    ] {
        let Some(db) = Database::open().await? else {
            return Ok(());
        };
        let pool = PgPool::connect(&db.url).await?;
        apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
        leave_only(&pool, &[keep]).await?;
        let before = schema_objects(&pool).await?;
        assert!(
            !before.iter().any(|object| object.ends_with(" qbit_share_ledger")),
            "{before:?}"
        );
        let error = db
            .ledger("a")
            .await
            .err()
            .with_context(|| format!("migration accepted a partial 001 source (only {keep})"))?
            .to_string();
        assert!(
            error.contains("refusing to migrate a partial 001 source before any DDL: the database has no qbit_share_ledger but holds"),
            "{error}"
        );
        assert!(
            error.contains(&format!(
                "holds {count} object(s) that the 2.x.x release's 001_share_ledger.sql creates ({})",
                named.join("; ")
            )),
            "{error}"
        );
        assert!(
            error.contains("so it is neither an empty database nor a 2.x.x ledger")
                && error.contains("Nothing was changed. Restore the full pre-migration backup, or migrate into an empty database"),
            "{error}"
        );
        assert!(native_tables_absent(&pool).await?, "refusal ran DDL");
        assert_eq!(schema_objects(&pool).await?, before, "refusal changed the schema");
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM pg_namespace WHERE nspname LIKE 'qbit_prism_scratch_%'"
            )
            .fetch_one(&pool)
            .await?,
            0,
            "scratch schema survived the refusal"
        );
        pool.close().await;
        db.close(vec![]).await?;
    }
    Ok(())
}

#[tokio::test]
async fn empty_database_migrates_as_fresh_with_or_without_an_operator_table() -> Result<()> {
    // Truly empty.
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    assert!(schema_objects(&pool).await?.is_empty());
    let ledger = db.ledger("a").await?;
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
    assert_eq!(
        ledger.migration_source().await?.map(|s| s.source_state),
        Some("fresh".into())
    );
    exercise_native_writers(&ledger, 1, 5401).await?;
    pool.close().await;
    db.close(vec![ledger]).await?;

    // Empty apart from an operator's own table, which brings a sequence of
    // its own: nothing the release creates, so still fresh, and kept.
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    sqlx::raw_sql("CREATE TABLE operator_notes(note_id bigserial PRIMARY KEY, note text NOT NULL); INSERT INTO operator_notes(note) VALUES('restored by hand')")
        .execute(&pool).await?;
    let ledger = db.ledger("a").await?;
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
    assert_eq!(
        ledger.migration_source().await?.map(|s| s.source_state),
        Some("fresh".into())
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT note FROM operator_notes WHERE note_id=1")
            .fetch_one(&pool)
            .await?,
        "restored by hand"
    );
    exercise_native_writers(&ledger, 1, 5402).await?;
    pool.close().await;
    db.close(vec![ledger]).await
}

/// `qbit_prism_schema_migrations` is created by the migrator first, inside
/// the migration transaction, so a refusal before any DDL leaves none.
async fn migrator_table_absent(pool: &PgPool) -> Result<bool> {
    Ok(
        sqlx::query_scalar("SELECT to_regclass('qbit_prism_schema_migrations') IS NULL")
            .fetch_one(pool)
            .await?,
    )
}

/// The object Codex's report names: a cluster table whose shape no native
/// writer can use, which 002's `CREATE TABLE IF NOT EXISTS` would keep.
const STRAY_CLUSTER_TABLE: &str = "CREATE TABLE qbit_prism_cluster(singleton boolean PRIMARY KEY)";

#[tokio::test]
async fn empty_database_with_a_stray_native_table_is_refused_naming_it_before_any_ddl() -> Result<()>
{
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    sqlx::raw_sql(STRAY_CLUSTER_TABLE).execute(&pool).await?;
    let before = schema_objects(&pool).await?;
    let error = db
        .ledger("init")
        .await
        .err()
        .context("migrate accepted an empty database with a stray native table")?
        .to_string();
    assert!(
        error.contains("refusing to migrate a native collision source before any DDL: the empty database already holds 1 object(s) that the native migrations create and the 2.x.x release does not (table qbit_prism_cluster)"),
        "{error}"
    );
    assert!(
        error.contains("Nothing was changed")
            && error.contains("Restore the full pre-migration backup")
            && error.contains("remove them yourself"),
        "{error}"
    );
    assert!(
        !error.contains("DROP"),
        "the refusal must not suggest a drop: {error}"
    );
    // Unchanged: no version recorded, no migrator table, the stray table as it was.
    assert!(migrator_table_absent(&pool).await?);
    assert_eq!(schema_objects(&pool).await?, before);
    assert!(sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass('qbit_prism_cluster') AND attname='singleton') AND NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass('qbit_prism_cluster') AND attname='fatal_error')")
        .fetch_one(&pool).await?, "the stray table was altered");
    // Removed by the operator, the same database migrates as fresh.
    sqlx::raw_sql("DROP TABLE qbit_prism_cluster")
        .execute(&pool)
        .await?;
    let ledger = db.ledger("init").await?;
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
    assert_eq!(
        ledger.migration_source().await?.map(|s| s.source_state),
        Some("fresh".into())
    );
    exercise_native_writers(&ledger, 1, 5901).await?;
    pool.close().await;
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn two_x_source_with_stray_native_objects_is_refused_naming_each_before_any_ddl() -> Result<()>
{
    for state in [SourceState::Pre258, SourceState::Applied258] {
        let Some(db) = Database::open().await? else {
            return Ok(());
        };
        let pool = PgPool::connect(&db.url).await?;
        apply_frozen_2x_schema(&pool, state).await?;
        insert_v1_terminal(&pool, &legacy_hash(0x55), "submitted").await?;
        // A native table (002), a native function (002, a trigger function
        // 002 would replace) and a native sequence (005, whose IF NOT
        // EXISTS would keep this narrower one).
        sqlx::raw_sql(&format!("{STRAY_CLUSTER_TABLE}; CREATE FUNCTION qbit_prism_reject_legacy_writer() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$; CREATE SEQUENCE qbit_prism_candidate_dispatch_sequence AS integer"))
            .execute(&pool).await?;
        let before = schema_objects(&pool).await?;
        let error = db
            .ledger("this-build")
            .await
            .err()
            .with_context(|| {
                format!("migrate accepted a {state:?} source with stray native objects")
            })?
            .to_string();
        assert!(
            error.contains("refusing to migrate a native collision source before any DDL: the 2.x.x database already holds 3 object(s) that the native migrations create and the 2.x.x release does not (table qbit_prism_cluster; sequence qbit_prism_candidate_dispatch_sequence; function qbit_prism_reject_legacy_writer())"),
            "{error}"
        );
        assert!(
            error.contains("Nothing was changed") && error.contains("remove them yourself"),
            "{error}"
        );
        // Unchanged: no migrator table, every object and the row as they were.
        assert!(migrator_table_absent(&pool).await?);
        assert_eq!(schema_objects(&pool).await?, before);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM qbit_block_candidate_outbox")
                .fetch_one(&pool)
                .await?,
            1
        );
        // The release's own objects were never the collision: on a #258
        // source the capability table is in both sets and its row stands.
        if state == SourceState::Applied258 {
            assert_eq!(capability(&pool).await?, Some(2));
        }
        // Removed by the operator, the same database migrates.
        sqlx::raw_sql("DROP TABLE qbit_prism_cluster; DROP FUNCTION qbit_prism_reject_legacy_writer(); DROP SEQUENCE qbit_prism_candidate_dispatch_sequence")
            .execute(&pool).await?;
        let ledger = db.ledger("this-build").await?;
        assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
        assert_eq!(
            ledger.migration_source().await?.map(|s| s.source_state),
            Some(state.as_str().to_owned())
        );
        exercise_native_writers(&ledger, 1, 5902).await?;
        pool.close().await;
        db.close(vec![ledger]).await?;
    }
    Ok(())
}

/// A reserved name held by a relation of another kind: a view under the
/// name of a native table, and an operator table whose unique constraint is
/// named `qbit_prism_candidate_claim_idx`, so its index owns that name.
/// `IF NOT EXISTS` looks at the name alone, so 002 would skip the outbox
/// index it meant to create, record the migration anyway, and leave
/// candidate polling to scan the outbox. Refused before any DDL on an empty
/// database and on a 2.x.x source alike, naming each by what holds the
/// name; renamed, the same database migrates and 002's index is on the
/// outbox.
#[tokio::test]
async fn reserved_name_held_by_a_relation_of_another_kind_is_refused_before_any_ddl() -> Result<()>
{
    for (state, nonce) in [(SourceState::Fresh, 6501), (SourceState::Pre258, 6502)] {
        let Some(db) = Database::open().await? else {
            return Ok(());
        };
        let pool = PgPool::connect(&db.url).await?;
        apply_frozen_2x_schema(&pool, state).await?;
        sqlx::raw_sql("CREATE TABLE operator_notes(note_id bigint NOT NULL, note text, CONSTRAINT qbit_prism_candidate_claim_idx UNIQUE (note_id)); CREATE VIEW qbit_prism_jobs AS SELECT 1 AS job_id")
            .execute(&pool).await?;
        let before = schema_objects(&pool).await?;
        let error = db
            .ledger("this-build")
            .await
            .err()
            .with_context(|| {
                format!("migrate accepted a {state:?} source with reserved names held by a view and a constraint-backed index")
            })?
            .to_string();
        let holder = match state {
            SourceState::Fresh => "empty database",
            _ => "2.x.x database",
        };
        assert!(
            error.contains(&format!("refusing to migrate a native collision source before any DDL: the {holder} already holds 2 object(s) that the native migrations create and the 2.x.x release does not (view qbit_prism_jobs; index qbit_prism_candidate_claim_idx backing constraint qbit_prism_candidate_claim_idx on operator_notes)")),
            "{error}"
        );
        assert!(
            error.contains("skip its own object where a relation of another kind holds the name")
                && error.contains("Nothing was changed"),
            "{error}"
        );
        // Unchanged: no migrator table, every object as it was, the view
        // still there.
        assert!(migrator_table_absent(&pool).await?);
        assert_eq!(schema_objects(&pool).await?, before);
        assert!(
            sqlx::query_scalar::<_, bool>("SELECT to_regclass('qbit_prism_jobs') IS NOT NULL AND to_regclass('qbit_prism_candidate_claim_idx') IS NOT NULL")
                .fetch_one(&pool).await?
        );
        // Renamed, the same database migrates; the operator's objects are
        // kept and 002's index is on the outbox.
        sqlx::raw_sql("ALTER TABLE operator_notes RENAME CONSTRAINT qbit_prism_candidate_claim_idx TO operator_notes_note_id_key; ALTER VIEW qbit_prism_jobs RENAME TO operator_jobs")
            .execute(&pool).await?;
        let ledger = db.ledger("this-build").await?;
        assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT tablename::text FROM pg_indexes WHERE schemaname=current_schema() AND indexname='qbit_prism_candidate_claim_idx'")
                .fetch_one(&pool).await?,
            "qbit_block_candidate_outbox"
        );
        assert!(
            sqlx::query_scalar::<_, bool>("SELECT to_regclass('operator_jobs') IS NOT NULL AND to_regclass('operator_notes_note_id_key') IS NOT NULL")
                .fetch_one(&pool).await?
        );
        exercise_native_writers(&ledger, 1, nonce).await?;
        pool.close().await;
        db.close(vec![ledger]).await?;
    }
    Ok(())
}

/// The type of a column of the test schema as the server renders it, or
/// `None` when the table has no such column.
async fn column_type(pool: &PgPool, table: &str, column: &str) -> Result<Option<String>> {
    Ok(sqlx::query_scalar("SELECT format_type(atttypid,atttypmod) FROM pg_attribute WHERE attrelid=to_regclass($1) AND attname=$2 AND attnum>0 AND NOT attisdropped")
        .bind(table).bind(column).fetch_optional(pool).await?)
}

#[tokio::test]
async fn two_x_source_with_a_stray_native_column_is_refused_naming_it_before_any_ddl() -> Result<()>
{
    // claim_token is a column 002 adds to the outbox with ADD COLUMN IF NOT
    // EXISTS on either state; the release never creates it. (The outbox's
    // storage_version is the release's own on a #258 source, and a stray
    // one on a pre-#258 source is a 002 object, refused as a partial 002
    // before this check.)
    for state in [SourceState::Pre258, SourceState::Applied258] {
        let Some(db) = Database::open().await? else {
            return Ok(());
        };
        let pool = PgPool::connect(&db.url).await?;
        apply_frozen_2x_schema(&pool, state).await?;
        insert_v1_terminal(&pool, &legacy_hash(0x56), "submitted").await?;
        // A claim token of a type the native claim lane cannot use, which
        // 002's ADD COLUMN IF NOT EXISTS would keep.
        sqlx::raw_sql("ALTER TABLE qbit_block_candidate_outbox ADD COLUMN claim_token integer")
            .execute(&pool)
            .await?;
        let before = schema_objects(&pool).await?;
        let error = db
            .ledger("this-build")
            .await
            .err()
            .with_context(|| {
                format!("migrate accepted a {state:?} source with a stray native column")
            })?
            .to_string();
        assert!(
            error.contains("refusing to migrate a native collision source before any DDL: the 2.x.x database already holds 1 object(s) that the native migrations create and the 2.x.x release does not (column qbit_block_candidate_outbox.claim_token)"),
            "{error}"
        );
        assert!(
            error.contains("Nothing was changed") && error.contains("remove them yourself"),
            "{error}"
        );
        assert!(
            !error.contains("DROP"),
            "the refusal must not suggest a drop: {error}"
        );
        // Unchanged: no migrator table, every object and the column as they
        // were, the row still there.
        assert!(migrator_table_absent(&pool).await?);
        assert_eq!(schema_objects(&pool).await?, before);
        assert_eq!(
            column_type(&pool, "qbit_block_candidate_outbox", "claim_token")
                .await?
                .as_deref(),
            Some("integer")
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM qbit_block_candidate_outbox")
                .fetch_one(&pool)
                .await?,
            1
        );
        // Removed by the operator, the same database migrates, and 002 adds
        // the column it wanted.
        sqlx::raw_sql("ALTER TABLE qbit_block_candidate_outbox DROP COLUMN claim_token")
            .execute(&pool)
            .await?;
        let ledger = db.ledger("this-build").await?;
        assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
        assert_eq!(
            ledger.migration_source().await?.map(|s| s.source_state),
            Some(state.as_str().to_owned())
        );
        assert_eq!(
            column_type(&pool, "qbit_block_candidate_outbox", "claim_token")
                .await?
                .as_deref(),
            Some("text")
        );
        exercise_native_writers(&ledger, 1, 5903).await?;
        pool.close().await;
        db.close(vec![ledger]).await?;
    }
    Ok(())
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
async fn unlogged_release_table_or_sequence_is_refused_naming_it_and_rolls_back() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    insert_v1_terminal(&pool, &legacy_hash(0x33), "submitted").await?;
    // qbit_payout_carry_forward holds the carried balances and has no
    // foreign key in either direction, so SET UNLOGGED is accepted as it
    // is, and the sequence behind its bigserial column follows the table.
    // Columns, constraints and indexes are all still the release's, so only
    // persistence can tell these apart from the scratch apply. (The explicit
    // qbit_audit_publication_sequence_seq is not used here: 001's own guard
    // already refuses that sequence when it is not logged.)
    sqlx::raw_sql("ALTER TABLE qbit_payout_carry_forward SET UNLOGGED")
        .execute(&pool)
        .await?;
    let persistence = || async {
        sqlx::query_scalar::<_, String>("SELECT string_agg(relname::text||'='||relpersistence::text,',' ORDER BY relname) FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relname IN ('qbit_payout_carry_forward','qbit_payout_carry_forward_carry_forward_seq_seq')")
            .fetch_one(&pool).await
    };
    assert_eq!(
        persistence().await?,
        "qbit_payout_carry_forward=u,qbit_payout_carry_forward_carry_forward_seq_seq=u"
    );
    let error = db
        .ledger("a")
        .await
        .err()
        .context("migration accepted an UNLOGGED release table")?
        .to_string();
    assert!(
        error.contains("refusing to migrate a drifted 001 source"),
        "{error}"
    );
    assert!(error.contains("2 object(s) differ"), "{error}");
    assert!(
        error.contains("table qbit_payout_carry_forward differs: expected logged, found UNLOGGED"),
        "{error}"
    );
    assert!(
        error.contains("sequence qbit_payout_carry_forward_carry_forward_seq_seq differs: expected logged, found UNLOGGED"),
        "{error}"
    );
    assert!(error.contains("Nothing was changed"), "{error}");
    // Rolled back whole: no native table, the relations as unlogged as
    // they were, the row untouched.
    assert!(native_tables_absent(&pool).await?, "refusal ran DDL");
    assert_eq!(
        persistence().await?,
        "qbit_payout_carry_forward=u,qbit_payout_carry_forward_carry_forward_seq_seq=u"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM qbit_block_candidate_outbox")
            .fetch_one(&pool)
            .await?,
        1
    );
    // Logged again, the same source migrates and is recorded as v2.0.1.
    sqlx::raw_sql("ALTER TABLE qbit_payout_carry_forward SET LOGGED")
        .execute(&pool)
        .await?;
    let ledger = db.ledger("a").await?;
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
    assert_eq!(
        ledger.migration_source().await?.map(|s| s.source_state),
        Some("pre_258".into())
    );
    exercise_native_writers(&ledger, 1, 5801).await?;
    pool.close().await;
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn release_constraint_left_not_valid_is_refused_naming_it_and_rolls_back() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    insert_v1_terminal(&pool, &legacy_hash(0x33), "submitted").await?;
    // A foreign key and a CHECK the release validates, dropped and re-added
    // NOT VALID: the same definitions, so they used to compare equal to the
    // validated release constraints whatever rows they were never checked
    // against. 001 creates both only inside CREATE TABLE, so its re-apply
    // leaves them as they are.
    sqlx::raw_sql("ALTER TABLE qbit_block_candidate_outbox DROP CONSTRAINT qbit_block_candidate_outbox_share_id_fkey; ALTER TABLE qbit_block_candidate_outbox ADD CONSTRAINT qbit_block_candidate_outbox_share_id_fkey FOREIGN KEY (share_id) REFERENCES qbit_share_ledger(share_id) NOT VALID; ALTER TABLE qbit_payout_carry_forward DROP CONSTRAINT qbit_payout_carry_forward_block_height_check; ALTER TABLE qbit_payout_carry_forward ADD CONSTRAINT qbit_payout_carry_forward_block_height_check CHECK (block_height >= 0) NOT VALID")
        .execute(&pool).await?;
    let not_valid = || async {
        sqlx::query_scalar::<_, String>("SELECT string_agg(conrelid::regclass::text||'.'||conname::text,',' ORDER BY conname) FROM pg_constraint WHERE connamespace=current_schema()::regnamespace AND NOT convalidated")
            .fetch_one(&pool).await
    };
    let left = "qbit_block_candidate_outbox.qbit_block_candidate_outbox_share_id_fkey,qbit_payout_carry_forward.qbit_payout_carry_forward_block_height_check";
    assert_eq!(not_valid().await?, left);
    let error = db
        .ledger("a")
        .await
        .err()
        .context("migration accepted a release constraint left NOT VALID")?
        .to_string();
    assert!(
        error.contains("refusing to migrate a drifted 001 source"),
        "{error}"
    );
    assert!(error.contains("2 object(s) differ"), "{error}");
    assert!(
        error.contains("constraint qbit_block_candidate_outbox_share_id_fkey on qbit_block_candidate_outbox is NOT VALID; the release validates it"),
        "{error}"
    );
    assert!(
        error.contains("constraint qbit_payout_carry_forward_block_height_check on qbit_payout_carry_forward is NOT VALID; the release validates it"),
        "{error}"
    );
    assert!(error.contains("Nothing was changed"), "{error}");
    // Rolled back whole: no native table, and the refusal validated nothing.
    assert!(native_tables_absent(&pool).await?, "refusal ran DDL");
    assert_eq!(not_valid().await?, left);
    // Validated, once the rows are known to satisfy them, the same source
    // migrates and is recorded as v2.0.1.
    sqlx::raw_sql("ALTER TABLE qbit_block_candidate_outbox VALIDATE CONSTRAINT qbit_block_candidate_outbox_share_id_fkey; ALTER TABLE qbit_payout_carry_forward VALIDATE CONSTRAINT qbit_payout_carry_forward_block_height_check")
        .execute(&pool).await?;
    let ledger = db.ledger("a").await?;
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
    assert_eq!(
        ledger.migration_source().await?.map(|s| s.source_state),
        Some("pre_258".into())
    );
    exercise_native_writers(&ledger, 1, 5901).await?;
    pool.close().await;
    db.close(vec![ledger]).await
}

/// A table created with `INHERITS (qbit_share_ledger)`: PostgreSQL includes
/// its rows in every query of the ledger, the unqualified share reads
/// included, so rows the release constraints never checked would enter the
/// accounting. The child is an extra table; the release table it changes is
/// drift, named. The release table made to inherit from another table is
/// drift the same way. Refused and rolled back, nothing dropped; detached,
/// the same source migrates and keeps the extra tables.
#[tokio::test]
async fn inheritance_involving_a_release_table_is_refused_naming_it_and_rolls_back() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    sqlx::raw_sql("CREATE TABLE qbit_share_ledger_2025 () INHERITS (qbit_share_ledger)")
        .execute(&pool)
        .await?;
    let error = db
        .ledger("a")
        .await
        .err()
        .context("migration accepted a child table of the share ledger")?
        .to_string();
    assert!(
        error.contains("refusing to migrate a drifted 001 source"),
        "{error}"
    );
    assert!(error.contains("1 object(s) differ"), "{error}");
    assert!(
        error.contains("table qbit_share_ledger differs: expected no child table, found child table(s) qbit_share_ledger_2025"),
        "{error}"
    );
    assert!(error.contains("Nothing was changed"), "{error}");
    assert!(native_tables_absent(&pool).await?, "refusal ran DDL");
    // The release table made a child of an operator table instead.
    sqlx::raw_sql("ALTER TABLE qbit_share_ledger_2025 NO INHERIT qbit_share_ledger; CREATE TABLE ledger_archive (LIKE qbit_share_ledger); ALTER TABLE qbit_share_ledger INHERIT ledger_archive")
        .execute(&pool).await?;
    let error = db
        .ledger("a")
        .await
        .err()
        .context("migration accepted the share ledger inheriting from another table")?
        .to_string();
    assert!(error.contains("1 object(s) differ"), "{error}");
    assert!(
        error.contains("table qbit_share_ledger differs: expected no parent table, found parent table(s) ledger_archive"),
        "{error}"
    );
    assert!(native_tables_absent(&pool).await?, "refusal ran DDL");
    // Detached, the same source migrates; both extra tables are kept.
    sqlx::raw_sql("ALTER TABLE qbit_share_ledger NO INHERIT ledger_archive")
        .execute(&pool)
        .await?;
    let ledger = db.ledger("a").await?;
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
    assert!(sqlx::query_scalar::<_, bool>("SELECT to_regclass('qbit_share_ledger_2025') IS NOT NULL AND to_regclass('ledger_archive') IS NOT NULL")
        .fetch_one(&pool).await?);
    exercise_native_writers(&ledger, 1, 6401).await?;
    pool.close().await;
    db.close(vec![ledger]).await
}

/// Enable or disable the internal triggers that enforce one constraint, on
/// every table they are on, as a superuser's `ALTER TABLE ... DISABLE
/// TRIGGER` does during a bulk load.
async fn set_enforcement(pool: &PgPool, constraint: &str, enable: bool) -> Result<()> {
    sqlx::raw_sql(&format!(
        "DO $$ DECLARE item record; BEGIN
           FOR item IN SELECT t.tgname, t.tgrelid::regclass AS rel FROM pg_trigger t JOIN pg_constraint k ON k.oid=t.tgconstraint WHERE t.tgisinternal AND k.conname='{constraint}' AND k.connamespace=current_schema()::regnamespace LOOP
             EXECUTE format('ALTER TABLE %s {} TRIGGER %I', item.rel, item.tgname);
           END LOOP;
         END $$",
        if enable { "ENABLE" } else { "DISABLE" }
    ))
    .execute(pool)
    .await?;
    Ok(())
}

/// The `tgenabled` state of each internal trigger enforcing a constraint,
/// concatenated in order.
async fn enforcement_states(pool: &PgPool, constraint: &str) -> Result<String> {
    Ok(sqlx::query_scalar("SELECT string_agg(t.tgenabled::text,'' ORDER BY t.tgenabled) FROM pg_trigger t JOIN pg_constraint k ON k.oid=t.tgconstraint WHERE t.tgisinternal AND k.conname=$1 AND k.connamespace=current_schema()::regnamespace")
        .bind(constraint).fetch_one(pool).await?)
}

/// A release foreign key whose enforcement triggers a superuser disabled:
/// its definition and validation state are as the release made them, but
/// no new row is checked against it, and 001 never re-enables a trigger it
/// did not create. Refused naming the constraint, rolled back whole;
/// enabled again, the same source migrates.
#[tokio::test]
async fn release_foreign_key_with_disabled_enforcement_triggers_is_refused_naming_it_and_rolls_back(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    insert_v1_terminal(&pool, &legacy_hash(0x36), "submitted").await?;
    let fkey = "qbit_block_candidate_outbox_share_id_fkey";
    assert_eq!(enforcement_states(&pool, fkey).await?, "OOOO");
    // Only that key's four referential triggers, on the outbox and on the
    // ledger it references; every user trigger stays enabled.
    set_enforcement(&pool, fkey, false).await?;
    assert_eq!(enforcement_states(&pool, fkey).await?, "DDDD");
    let error = db
        .ledger("a")
        .await
        .err()
        .context("migration accepted a release foreign key with its enforcement triggers disabled")?
        .to_string();
    assert!(
        error.contains("refusing to migrate a drifted 001 source"),
        "{error}"
    );
    assert!(error.contains("1 object(s) differ"), "{error}");
    assert!(
        error.contains("constraint qbit_block_candidate_outbox_share_id_fkey on qbit_block_candidate_outbox differs: enforcement triggers expected 4 enabled, found 4 disabled"),
        "{error}"
    );
    assert!(error.contains("Nothing was changed"), "{error}");
    // Rolled back whole: no native table, and the triggers as they were.
    assert!(native_tables_absent(&pool).await?, "refusal ran DDL");
    assert_eq!(enforcement_states(&pool, fkey).await?, "DDDD");
    // Enabled again, the same source migrates and is recorded as v2.0.1.
    set_enforcement(&pool, fkey, true).await?;
    let ledger = db.ledger("a").await?;
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
    assert_eq!(
        ledger.migration_source().await?.map(|s| s.source_state),
        Some("pre_258".into())
    );
    exercise_native_writers(&ledger, 1, 6301).await?;
    pool.close().await;
    db.close(vec![ledger]).await
}

/// A share row as a 2.x.x writer holding lease epoch `epoch` inserts it.
async fn insert_share_as_writer(pool: &PgPool, share_id: &str, epoch: i64) -> Result<()> {
    sqlx::query("INSERT INTO qbit_share_ledger(share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,writer_id,writer_epoch) VALUES($1,'miner','miner',decode(repeat('11',32),'hex'),1,100,100,'job',clock_timestamp(),1800000000,'python',$2)")
        .bind(share_id).bind(epoch).execute(pool).await?;
    Ok(())
}

/// Every share with the epoch its writer stamped, in commit order.
async fn ledger_epochs(pool: &PgPool) -> Result<Vec<(String, i64)>> {
    Ok(
        sqlx::query_as("SELECT share_id,writer_epoch FROM qbit_share_ledger ORDER BY share_seq")
            .fetch_all(pool)
            .await?,
    )
}

/// An extra CHECK can accept every legacy row while refusing native writes.
/// Refusal must preserve both frozen sources; only the operator removes it.
#[tokio::test]
async fn extra_constraint_on_a_release_table_is_refused_naming_it_and_rolls_back() -> Result<()> {
    for (state, nonce) in [(SourceState::Pre258, 6501), (SourceState::Applied258, 6502)] {
        let Some(db) = Database::open().await? else {
            return Ok(());
        };
        let pool = PgPool::connect(&db.url).await?;
        apply_frozen_2x_schema(&pool, state).await?;
        insert_share_as_writer(&pool, "legacy:1", 1).await?;
        sqlx::raw_sql("ALTER TABLE qbit_share_ledger ADD CONSTRAINT operator_epoch_check CHECK (writer_epoch > 0); CREATE TABLE operator_notes(note text CONSTRAINT operator_note_check CHECK (note <> ''))")
            .execute(&pool).await?;
        let refused = insert_share_as_writer(&pool, "native:0", 0)
            .await
            .err()
            .context("the CHECK accepted writer_epoch 0")?
            .to_string();
        assert!(refused.contains("operator_epoch_check"), "{refused}");
        let objects = schema_objects(&pool).await?;
        let legacy = ledger_epochs(&pool).await?;
        let error = db
            .ledger("a")
            .await
            .err()
            .context("migration accepted a behavior-changing extra constraint")?
            .to_string();
        assert!(
            error.contains("refusing to migrate a drifted 001 source"),
            "{error}"
        );
        assert!(error.contains("1 object(s) differ"), "{error}");
        assert!(error.contains("constraint operator_epoch_check on qbit_share_ledger: CHECK ((writer_epoch > 0)); the release does not create it"), "{error}");
        assert!(!error.contains("operator_note_check"), "{error}");
        assert!(error.contains("Nothing was changed"), "{error}");
        assert!(native_tables_absent(&pool).await?, "refusal ran native DDL");
        assert_eq!(schema_objects(&pool).await?, objects);
        assert_eq!(ledger_epochs(&pool).await?, legacy);
        assert!(sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='qbit_share_ledger'::regclass AND conname='operator_epoch_check' AND convalidated)")
            .fetch_one(&pool).await?);

        sqlx::raw_sql("ALTER TABLE qbit_share_ledger DROP CONSTRAINT operator_epoch_check")
            .execute(&pool)
            .await?;
        let ledger = db.ledger("a").await?;
        assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
        assert_eq!(
            ledger.migration_source().await?.map(|s| s.source_state),
            Some(state.as_str().to_owned())
        );
        assert!(sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='operator_notes'::regclass AND conname='operator_note_check')")
            .fetch_one(&pool).await?);
        exercise_native_writers(&ledger, 1, nonce).await?;
        assert_eq!(
            ledger_epochs(&pool).await?,
            [legacy, vec![(share(1).share_id, 0)]].concat()
        );
        pool.close().await;
        db.close(vec![ledger]).await?;
    }
    Ok(())
}

/// Extra unique keys, expressions and predicates can accept legacy rows
/// while rejecting native epoch-zero writes. Refusal preserves the source;
/// plain column indexes and indexes on the operator's own tables survive.
#[tokio::test]
async fn extra_write_affecting_index_is_refused_naming_it_and_rolls_back() -> Result<()> {
    for state in [SourceState::Pre258, SourceState::Applied258] {
        for (ddl, reason, unique) in [
            (
                "CREATE UNIQUE INDEX operator_native_epoch ON qbit_share_ledger ((1)) WHERE writer_epoch = 0",
                "unique, expression, partial",
                true,
            ),
            (
                "CREATE UNIQUE INDEX operator_native_epoch ON qbit_share_ledger (writer_epoch)",
                "unique",
                true,
            ),
            (
                "CREATE INDEX operator_native_epoch ON qbit_share_ledger ((1 / writer_epoch))",
                "expression",
                false,
            ),
            (
                "CREATE INDEX operator_native_epoch ON qbit_share_ledger (miner_id) WHERE 1 / writer_epoch > 0",
                "partial",
                false,
            ),
        ] {
            let Some(db) = Database::open().await? else {
                return Ok(());
            };
            let pool = PgPool::connect(&db.url).await?;
            apply_frozen_2x_schema(&pool, state).await?;
            insert_share_as_writer(&pool, "legacy:1", 1).await?;
            sqlx::raw_sql(ddl).execute(&pool).await?;
            sqlx::raw_sql("CREATE INDEX operator_miner_idx ON qbit_share_ledger (miner_id); CREATE TABLE operator_notes(note text NOT NULL); CREATE UNIQUE INDEX operator_notes_idx ON operator_notes ((lower(note))) WHERE note <> ''")
                .execute(&pool).await?;
            if unique {
                insert_share_as_writer(&pool, "native:probe", 0).await?;
            }
            let refused = insert_share_as_writer(&pool, "native:refused", 0)
                .await
                .err()
                .context("the extra index accepted the incompatible native write")?
                .to_string();
            assert!(
                refused.contains(if unique { "duplicate key" } else { "division by zero" }),
                "{refused}"
            );
            let objects = schema_objects(&pool).await?;
            let rows: Vec<Value> = sqlx::query_scalar(
                "SELECT to_jsonb(s) FROM qbit_share_ledger s ORDER BY share_seq",
            )
            .fetch_all(&pool)
            .await?;
            let definition: String = sqlx::query_scalar(
                "SELECT pg_get_indexdef('operator_native_epoch'::regclass)",
            )
            .fetch_one(&pool)
            .await?;
            let error = db
                .ledger("a")
                .await
                .err()
                .context("migration accepted an extra index that rejects native writes")?
                .to_string();
            assert!(error.contains("refusing to migrate a drifted 001 source"), "{error}");
            assert!(error.contains("1 object(s) differ"), "{error}");
            assert!(error.contains(&format!(
                "index operator_native_epoch on qbit_share_ledger is an extra {reason} index; it can constrain or evaluate native writes"
            )), "{error}");
            assert!(error.contains("Nothing was changed"), "{error}");
            assert!(!error.contains("operator_notes"), "{error}");
            assert!(!error.contains("operator_miner_idx"), "{error}");
            assert!(native_tables_absent(&pool).await?, "refusal ran native DDL");
            assert_eq!(schema_objects(&pool).await?, objects);
            assert_eq!(
                sqlx::query_scalar::<_, Value>(
                    "SELECT to_jsonb(s) FROM qbit_share_ledger s ORDER BY share_seq"
                ).fetch_all(&pool).await?,
                rows
            );
            assert_eq!(
                sqlx::query_scalar::<_, String>(
                    "SELECT pg_get_indexdef('operator_native_epoch'::regclass)"
                ).fetch_one(&pool).await?,
                definition
            );

            // Only the operator removes the index; the migrator never does.
            sqlx::raw_sql("DROP INDEX operator_native_epoch")
                .execute(&pool).await?;
            let ledger = db.ledger("a").await?;
            assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
            assert_eq!(
                ledger.migration_source().await?.map(|s| s.source_state),
                Some(state.as_str().to_owned())
            );
            exercise_native_writers(&ledger, 1, 6701).await?;
            ledger.append(share(2), None).await?;
            assert_eq!(
                sqlx::query_scalar::<_, i64>("SELECT count(*) FROM qbit_share_ledger WHERE writer_epoch = 0")
                    .fetch_one(&pool).await?,
                if unique { 3 } else { 2 }
            );
            assert!(sqlx::query_scalar::<_, bool>("SELECT to_regclass('operator_miner_idx') IS NOT NULL AND to_regclass('operator_notes_idx') IS NOT NULL")
                .fetch_one(&pool).await?);
            pool.close().await;
            db.close(vec![ledger]).await?;
        }
    }
    Ok(())
}

/// A backfilled required column has no value for an omitted native insert.
/// Accept it only after the operator makes omission possible again.
#[tokio::test]
async fn required_extra_column_is_refused_naming_it_and_rolls_back() -> Result<()> {
    for (state, nonce) in [(SourceState::Pre258, 6601), (SourceState::Applied258, 6602)] {
        let Some(db) = Database::open().await? else {
            return Ok(());
        };
        let pool = PgPool::connect(&db.url).await?;
        apply_frozen_2x_schema(&pool, state).await?;
        insert_share_as_writer(&pool, "legacy:1", 1).await?;
        sqlx::raw_sql("ALTER TABLE qbit_share_ledger ADD COLUMN operator_note text DEFAULT 'backfilled'; ALTER TABLE qbit_share_ledger ALTER COLUMN operator_note SET NOT NULL; ALTER TABLE qbit_share_ledger ALTER COLUMN operator_note DROP DEFAULT")
            .execute(&pool).await?;
        let refused = insert_share_as_writer(&pool, "native:0", 0)
            .await
            .err()
            .context("the required column accepted an omitted value")?
            .to_string();
        assert!(refused.contains("operator_note"), "{refused}");
        let objects = schema_objects(&pool).await?;
        let rows: Vec<Value> = sqlx::query_scalar("SELECT to_jsonb(s) FROM qbit_share_ledger s")
            .fetch_all(&pool)
            .await?;
        let error = db
            .ledger("a")
            .await
            .err()
            .context("migration accepted an unsatisfied required extra column")?
            .to_string();
        assert!(
            error.contains("refusing to migrate a drifted 001 source"),
            "{error}"
        );
        assert!(error.contains("1 object(s) differ"), "{error}");
        assert!(error.contains("column qbit_share_ledger.operator_note is an extra NOT NULL column without a default, identity or generated expression; native inserts omit it"), "{error}");
        assert!(error.contains("Nothing was changed"), "{error}");
        assert!(native_tables_absent(&pool).await?, "refusal ran native DDL");
        assert_eq!(schema_objects(&pool).await?, objects);
        assert_eq!(
            sqlx::query_scalar::<_, Value>("SELECT to_jsonb(s) FROM qbit_share_ledger s")
                .fetch_all(&pool)
                .await?,
            rows
        );
        assert!(sqlx::query_scalar::<_, bool>("SELECT attnotnull AND NOT atthasdef FROM pg_attribute WHERE attrelid='qbit_share_ledger'::regclass AND attname='operator_note'")
            .fetch_one(&pool).await?);

        // Plain nullable columns need no expression evaluation for omission.
        sqlx::raw_sql("ALTER TABLE qbit_share_ledger ALTER COLUMN operator_note DROP NOT NULL; ALTER TABLE qbit_share_ledger ADD COLUMN operator_nullable text")
            .execute(&pool).await?;
        let ledger = db.ledger("a").await?;
        assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
        assert_eq!(
            ledger.migration_source().await?.map(|s| s.source_state),
            Some(state.as_str().to_owned())
        );
        exercise_native_writers(&ledger, 1, nonce).await?;
        let (note, supplied): (Option<String>, bool) = sqlx::query_as("SELECT operator_note, operator_nullable IS NULL FROM qbit_share_ledger WHERE share_id=$1")
            .bind(share(1).share_id).fetch_one(&pool).await?;
        assert_eq!(note, None);
        assert!(supplied);
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT operator_note FROM qbit_share_ledger WHERE share_id='legacy:1'"
            )
            .fetch_one(&pool)
            .await?,
            "backfilled"
        );
        pool.close().await;
        db.close(vec![ledger]).await?;
    }
    Ok(())
}

/// Omitted columns can still execute defaults or generated expressions.
/// Refusal preserves source data; plain nullable columns work after repair.
#[tokio::test]
async fn executable_extra_column_is_refused_naming_it_and_rolls_back() -> Result<()> {
    for state in [SourceState::Pre258, SourceState::Applied258] {
        for (ddl, repair) in [
            ("ADD COLUMN operator_value bigint GENERATED ALWAYS AS (10 / writer_epoch) STORED", "DROP EXPRESSION"),
            ("ADD COLUMN operator_value bigint; ALTER TABLE qbit_share_ledger ALTER COLUMN operator_value SET DEFAULT (10 / 0)", "DROP DEFAULT"),
        ] {
            let Some(db) = Database::open().await? else { return Ok(()); };
            let pool = PgPool::connect(&db.url).await?;
            apply_frozen_2x_schema(&pool, state).await?;
            insert_share_as_writer(&pool, "legacy:1", 1).await?;
            sqlx::raw_sql(&format!("ALTER TABLE qbit_share_ledger {ddl}"))
                .execute(&pool).await?;
            let refused = insert_share_as_writer(&pool, "native:0", 0).await
                .err().context("the expression accepted the native insert")?.to_string();
            assert!(refused.contains("division by zero"), "{refused}");
            let objects = schema_objects(&pool).await?;
            let rows: Vec<Value> = sqlx::query_scalar("SELECT to_jsonb(s) FROM qbit_share_ledger s")
                .fetch_all(&pool).await?;
            let error = db.ledger("a").await.err()
                .context("migration accepted an executable extra column")?.to_string();
            assert!(error.contains("refusing to migrate a drifted 001 source"), "{error}");
            assert!(error.contains("column qbit_share_ledger.operator_value has an extra default, identity or generated expression; native writes can evaluate it"), "{error}");
            assert!(error.contains("Nothing was changed"), "{error}");
            assert!(native_tables_absent(&pool).await?);
            assert_eq!(schema_objects(&pool).await?, objects);
            assert_eq!(sqlx::query_scalar::<_, Value>("SELECT to_jsonb(s) FROM qbit_share_ledger s")
                .fetch_all(&pool).await?, rows);
            sqlx::raw_sql(&format!("ALTER TABLE qbit_share_ledger ALTER COLUMN operator_value {repair}"))
                .execute(&pool).await?;
            let ledger = db.ledger("a").await?;
            assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
            exercise_native_writers(&ledger, 1, 6701).await?;
            pool.close().await;
            db.close(vec![ledger]).await?;
        }
    }
    Ok(())
}

/// Discovery in a later schema must never create a parallel empty ledger.
#[tokio::test]
async fn migration_refuses_source_objects_resolved_from_a_later_schema() -> Result<()> {
    for state in [SourceState::Pre258, SourceState::Applied258] {
        for native in [false, true] {
            let Some(db) = Database::open().await? else {
                return Ok(());
            };
            let pool = PgPool::connect(&db.url).await?;
            apply_frozen_2x_schema(&pool, state).await?;
            insert_share_as_writer(&pool, "legacy:1", 1).await?;
            let earlier = if native {
                Some(db.ledger("earlier-build").await?)
            } else {
                None
            };
            let empty = format!("{}_empty", db.schema);
            sqlx::raw_sql(&format!("CREATE SCHEMA {empty}"))
                .execute(&pool)
                .await?;
            let objects = schema_objects(&pool).await?;
            let rows: Vec<Value> =
                sqlx::query_scalar("SELECT to_jsonb(s) FROM qbit_share_ledger s")
                    .fetch_all(&pool)
                    .await?;
            let mut url = url::Url::parse(&db.url)?;
            let base_query: Vec<(String, String)> = url
                .query_pairs()
                .filter(|(key, _)| key != "options")
                .map(|(key, value)| (key.into_owned(), value.into_owned()))
                .collect();
            url.query_pairs_mut()
                .clear()
                .extend_pairs(base_query.clone())
                .append_pair("options", &format!("-csearch_path={empty},{}", db.schema));
            let error = Ledger::connect(url.as_str(), "wrong-schema".into(), 8, true)
                .await
                .err()
                .context("migration shadowed the legacy ledger with a parallel schema")?
                .to_string();
            assert!(
                error.contains("refusing to migrate before any DDL"),
                "{error}"
            );
            assert!(
                error.contains(&format!("current schema {empty}")),
                "{error}"
            );
            assert!(error.contains(&db.schema), "{error}");
            assert!(error.contains("search_path"), "{error}");
            assert!(error.contains("Nothing was changed"), "{error}");
            let empty_objects: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_class WHERE relnamespace=$1::regnamespace",
            )
            .bind(&empty)
            .fetch_one(&pool)
            .await?;
            assert_eq!(empty_objects, 0, "migration wrote to the empty schema");
            assert_eq!(schema_objects(&pool).await?, objects);
            assert_eq!(
                sqlx::query_scalar::<_, Value>("SELECT to_jsonb(s) FROM qbit_share_ledger s")
                    .fetch_all(&pool)
                    .await?,
                rows
            );
            // A multi-schema path is supported when the source is first.
            url.query_pairs_mut()
                .clear()
                .extend_pairs(base_query)
                .append_pair("options", &format!("-csearch_path={},{}", db.schema, empty));
            let migrated = Ledger::connect(url.as_str(), "right-schema".into(), 8, true).await?;
            assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
            assert_eq!(
                sqlx::query_scalar::<_, i64>(
                    "SELECT count(*) FROM qbit_share_ledger WHERE share_id='legacy:1'"
                )
                .fetch_one(&migrated.pool)
                .await?,
                1
            );
            exercise_native_writers(&migrated, 1, 7201).await?;
            sqlx::raw_sql(&format!("DROP SCHEMA {empty}"))
                .execute(&pool)
                .await?;
            pool.close().await;
            let mut ledgers = vec![migrated];
            ledgers.extend(earlier);
            db.close(ledgers).await?;
        }
    }
    Ok(())
}

/// Domain checks and nullability are not represented by attnotnull.
#[tokio::test]
async fn domain_extra_column_is_refused_naming_it_and_rolls_back() -> Result<()> {
    for state in [SourceState::Pre258, SourceState::Applied258] {
        for constraint in ["CHECK (VALUE IS NOT NULL)", "NOT NULL"] {
            let Some(db) = Database::open().await? else {
                return Ok(());
            };
            let pool = PgPool::connect(&db.url).await?;
            apply_frozen_2x_schema(&pool, state).await?;
            sqlx::raw_sql(&format!("CREATE DOMAIN operator_required AS text {constraint}; ALTER TABLE qbit_share_ledger ADD COLUMN operator_note operator_required; CREATE TABLE operator_notes(note operator_required); INSERT INTO operator_notes VALUES('kept')"))
                .execute(&pool).await?;
            let refused = insert_share_as_writer(&pool, "native:0", 0)
                .await
                .err()
                .context("domain accepted an omitted native value")?
                .to_string();
            assert!(refused.contains("operator_required"), "{refused}");
            let objects = schema_objects(&pool).await?;
            let error = db
                .ledger("a")
                .await
                .err()
                .context("migration accepted a domain-typed extra column")?
                .to_string();
            assert!(
                error.contains("refusing to migrate a drifted 001 source"),
                "{error}"
            );
            assert!(
                error.contains("column qbit_share_ledger.operator_note has an extra domain type"),
                "{error}"
            );
            assert!(!error.contains("operator_notes"), "{error}");
            assert!(error.contains("Nothing was changed"), "{error}");
            assert!(native_tables_absent(&pool).await?);
            assert_eq!(schema_objects(&pool).await?, objects);
            assert!(sqlx::query_scalar::<_, bool>("SELECT a.atttypid='operator_required'::regtype FROM pg_attribute a WHERE a.attrelid='qbit_share_ledger'::regclass AND a.attname='operator_note'").fetch_one(&pool).await?);
            sqlx::raw_sql("ALTER TABLE qbit_share_ledger ALTER COLUMN operator_note TYPE text")
                .execute(&pool)
                .await?;
            let ledger = db.ledger("a").await?;
            exercise_native_writers(&ledger, 1, 7101).await?;
            assert_eq!(
                sqlx::query_scalar::<_, String>("SELECT note::text FROM operator_notes")
                    .fetch_one(&pool)
                    .await?,
                "kept"
            );
            assert!(sqlx::query("INSERT INTO operator_notes VALUES(NULL)")
                .execute(&pool)
                .await
                .is_err());
            pool.close().await;
            db.close(vec![ledger]).await?;
        }
    }
    Ok(())
}

/// Rewrite rules run before triggers and can suppress native INSERT RETURNING.
#[tokio::test]
async fn extra_rewrite_rule_is_refused_naming_it_and_rolls_back() -> Result<()> {
    for state in [SourceState::Pre258, SourceState::Applied258] {
        let Some(db) = Database::open().await? else {
            return Ok(());
        };
        let pool = PgPool::connect(&db.url).await?;
        apply_frozen_2x_schema(&pool, state).await?;
        insert_share_as_writer(&pool, "legacy:1", 1).await?;
        sqlx::raw_sql("CREATE RULE operator_suppress_share AS ON INSERT TO qbit_share_ledger DO INSTEAD NOTHING; CREATE TABLE operator_notes(note text); CREATE RULE operator_suppress_note AS ON INSERT TO operator_notes DO INSTEAD NOTHING")
            .execute(&pool).await?;
        let refused = sqlx::query("INSERT INTO qbit_share_ledger SELECT * FROM qbit_share_ledger WHERE false RETURNING share_seq")
            .execute(&pool).await.err().context("rewrite rule accepted INSERT RETURNING")?.to_string();
        assert!(refused.contains("RETURNING"), "{refused}");
        let objects = schema_objects(&pool).await?;
        let rows: Vec<Value> = sqlx::query_scalar("SELECT to_jsonb(s) FROM qbit_share_ledger s")
            .fetch_all(&pool)
            .await?;
        let rules_sql = "SELECT pg_get_ruledef(r.oid) || ':' || r.ev_enabled::text FROM pg_rewrite r JOIN pg_class c ON c.oid=r.ev_class WHERE c.relnamespace=current_schema()::regnamespace ORDER BY 1";
        let rules: Vec<String> = sqlx::query_scalar(rules_sql).fetch_all(&pool).await?;
        let error = db
            .ledger("a")
            .await
            .err()
            .context("migration accepted an extra rewrite rule")?
            .to_string();
        assert!(
            error.contains("refusing to migrate a drifted 001 source"),
            "{error}"
        );
        assert!(
            error.contains("rule operator_suppress_share on qbit_share_ledger"),
            "{error}"
        );
        assert!(!error.contains("operator_suppress_note"), "{error}");
        assert!(error.contains("Nothing was changed"), "{error}");
        assert!(native_tables_absent(&pool).await?);
        assert_eq!(schema_objects(&pool).await?, objects);
        assert_eq!(
            sqlx::query_scalar::<_, String>(rules_sql)
                .fetch_all(&pool)
                .await?,
            rules
        );
        assert_eq!(
            sqlx::query_scalar::<_, Value>("SELECT to_jsonb(s) FROM qbit_share_ledger s")
                .fetch_all(&pool)
                .await?,
            rows
        );
        sqlx::raw_sql("DROP RULE operator_suppress_share ON qbit_share_ledger")
            .execute(&pool)
            .await?;
        let ledger = db.ledger("a").await?;
        assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
        exercise_native_writers(&ledger, 1, 6801).await?;
        assert_eq!(
            sqlx::query("INSERT INTO operator_notes VALUES('suppressed')")
                .execute(&pool)
                .await?
                .rows_affected(),
            0
        );
        pool.close().await;
        db.close(vec![ledger]).await?;
    }
    Ok(())
}

/// Foreign keys owned elsewhere still constrain deletes on release tables.
#[tokio::test]
async fn incoming_foreign_key_is_refused_naming_it_and_rolls_back() -> Result<()> {
    for state in [SourceState::Pre258, SourceState::Applied258] {
        for external in [false, true] {
            let Some(db) = Database::open().await? else {
                return Ok(());
            };
            let pool = PgPool::connect(&db.url).await?;
            apply_frozen_2x_schema(&pool, state).await?;
            insert_share_as_writer(&pool, "legacy:1", 1).await?;
            let operator_schema = format!("{}_operator", db.schema);
            let owner = if external {
                sqlx::raw_sql(&format!("CREATE SCHEMA {operator_schema}"))
                    .execute(&pool)
                    .await?;
                // The same relation name in another schema is still an extra.
                format!("{operator_schema}.qbit_share_ledger")
            } else {
                "operator_refs".to_owned()
            };
            sqlx::raw_sql(&format!("CREATE TABLE {owner}(share_seq bigint CONSTRAINT operator_share_fk REFERENCES qbit_share_ledger(share_seq), attempt_seq bigint CONSTRAINT operator_attempt_fk REFERENCES qbit_ctv_fanout_broadcast_attempts(attempt_seq)); INSERT INTO {owner}(share_seq) SELECT share_seq FROM qbit_share_ledger; CREATE TABLE operator_notes(note_id bigint PRIMARY KEY); CREATE TABLE operator_own_refs(note_id bigint CONSTRAINT operator_own_fk REFERENCES operator_notes(note_id))"))
                .execute(&pool).await?;
            let refused = sqlx::query("DELETE FROM qbit_share_ledger WHERE share_id='legacy:1'")
                .execute(&pool)
                .await
                .err()
                .context("incoming foreign key allowed the referenced delete")?
                .to_string();
            assert!(refused.contains("operator_share_fk"), "{refused}");
            let objects = schema_objects(&pool).await?;
            let rows: Vec<Value> =
                sqlx::query_scalar("SELECT to_jsonb(s) FROM qbit_share_ledger s")
                    .fetch_all(&pool)
                    .await?;
            let constraints_sql = "SELECT k.oid::bigint,pg_get_constraintdef(k.oid) FROM pg_constraint k JOIN pg_class target ON target.oid=k.confrelid WHERE target.relnamespace=current_schema()::regnamespace ORDER BY 1";
            let constraints: Vec<(i64, String)> =
                sqlx::query_as(constraints_sql).fetch_all(&pool).await?;
            let error = db
                .ledger("a")
                .await
                .err()
                .context("migration accepted an incoming foreign key")?
                .to_string();
            assert!(
                error.contains("refusing to migrate a drifted 001 source"),
                "{error}"
            );
            assert!(error.contains(&format!("foreign key operator_share_fk on {owner} references release table qbit_share_ledger")), "{error}");
            assert!(error.contains(&format!("foreign key operator_attempt_fk on {owner} references release table qbit_ctv_fanout_broadcast_attempts")), "{error}");
            assert!(!error.contains("operator_own_fk"), "{error}");
            assert!(error.contains("Nothing was changed"), "{error}");
            assert!(native_tables_absent(&pool).await?);
            assert_eq!(schema_objects(&pool).await?, objects);
            assert_eq!(
                sqlx::query_as::<_, (i64, String)>(constraints_sql)
                    .fetch_all(&pool)
                    .await?,
                constraints
            );
            assert_eq!(
                sqlx::query_scalar::<_, Value>("SELECT to_jsonb(s) FROM qbit_share_ledger s")
                    .fetch_all(&pool)
                    .await?,
                rows
            );
            sqlx::raw_sql(&format!("ALTER TABLE {owner} DROP CONSTRAINT operator_share_fk, DROP CONSTRAINT operator_attempt_fk"))
                .execute(&pool).await?;
            let ledger = db.ledger("a").await?;
            assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
            exercise_native_writers(&ledger, 1, 6901).await?;
            assert!(sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='operator_own_refs'::regclass AND conname='operator_own_fk')").fetch_one(&pool).await?);
            if external {
                sqlx::raw_sql(&format!("DROP SCHEMA {operator_schema} CASCADE"))
                    .execute(&pool)
                    .await?;
            }
            pool.close().await;
            db.close(vec![ledger]).await?;
        }
    }
    Ok(())
}

/// The operator's own triggers, as `table.trigger=state`.
async fn operator_triggers(pool: &PgPool) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar("SELECT c.relname::text||'.'||t.tgname::text||'='||t.tgenabled::text FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid WHERE c.relnamespace=current_schema()::regnamespace AND NOT t.tgisinternal AND t.tgname LIKE 'operator%' ORDER BY 1")
        .fetch_all(pool).await?)
}

/// A trigger the release does not create on a release table: an operator's
/// guard that refuses `writer_epoch = 0`, which the 2.x.x writer's leased
/// epochs never trip and every native share insert, which writes epoch 0,
/// would. Refused naming the trigger and its table, rolled back whole with
/// the trigger and the legacy rows as they were; without the guard the
/// same source migrates and the native writers work. A trigger on the
/// operator's own table is theirs and survives. Both frozen sources.
#[tokio::test]
async fn extra_trigger_on_a_release_table_is_refused_naming_it_and_rolls_back() -> Result<()> {
    for (state, nonce) in [(SourceState::Pre258, 6401), (SourceState::Applied258, 6402)] {
        let Some(db) = Database::open().await? else {
            return Ok(());
        };
        let pool = PgPool::connect(&db.url).await?;
        apply_frozen_2x_schema(&pool, state).await?;
        let terminal = legacy_hash(0x37);
        insert_v1_terminal(&pool, &terminal, "submitted").await?;
        sqlx::raw_sql("CREATE FUNCTION operator_reject_epoch_zero() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.writer_epoch = 0 THEN RAISE EXCEPTION 'operator guard: writer_epoch 0 is not a leased epoch'; END IF; RETURN NEW; END $$; CREATE TRIGGER operator_epoch_guard BEFORE INSERT ON qbit_share_ledger FOR EACH ROW EXECUTE FUNCTION operator_reject_epoch_zero(); CREATE TABLE operator_notes(note_id bigserial PRIMARY KEY, note text NOT NULL, noted_at timestamptz); CREATE FUNCTION operator_notes_stamp() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.noted_at := clock_timestamp(); RETURN NEW; END $$; CREATE TRIGGER operator_notes_stamp BEFORE INSERT ON operator_notes FOR EACH ROW EXECUTE FUNCTION operator_notes_stamp()")
            .execute(&pool).await?;
        // The guard lets the legacy writer's share through and refuses the
        // native writer's.
        insert_share_as_writer(&pool, "legacy:1", 1).await?;
        let refused = insert_share_as_writer(&pool, "native:0", 0)
            .await
            .err()
            .context("the guard accepted writer_epoch 0")?
            .to_string();
        assert!(
            refused.contains("operator guard: writer_epoch 0 is not a leased epoch"),
            "{refused}"
        );
        let guarded = vec![
            "operator_notes.operator_notes_stamp=O".to_owned(),
            "qbit_share_ledger.operator_epoch_guard=O".to_owned(),
        ];
        assert_eq!(operator_triggers(&pool).await?, guarded);
        let legacy = vec![("legacy:1".to_owned(), 1)];
        assert_eq!(ledger_epochs(&pool).await?, legacy);
        let objects = schema_objects(&pool).await?;
        let (release, files) = match state {
            SourceState::Applied258 => (
                "v2.0.2",
                "001_share_ledger.sql and 002_candidate_bodies.sql",
            ),
            _ => ("v2.0.1", "001_share_ledger.sql"),
        };
        let error = db
            .ledger("a")
            .await
            .err()
            .context(
                "migration accepted a release table with a trigger the release does not create",
            )?
            .to_string();
        assert!(
            error.contains("refusing to migrate a drifted 001 source"),
            "{error}"
        );
        assert!(
            error.contains(&format!(
                "does not match the v2.0.x release schema ({release}, {files}), 1 object(s) differ"
            )),
            "{error}"
        );
        assert!(
            error.contains("trigger operator_epoch_guard on qbit_share_ledger: CREATE TRIGGER operator_epoch_guard BEFORE INSERT ON qbit_share_ledger FOR EACH ROW EXECUTE FUNCTION operator_reject_epoch_zero(); the release has no trigger on this table"),
            "{error}"
        );
        assert!(!error.contains("operator_notes"), "{error}");
        assert!(error.contains("Nothing was changed"), "{error}");
        // Rolled back whole: no native table or version, the same objects,
        // both triggers and the legacy rows as they were.
        assert!(native_tables_absent(&pool).await?, "refusal ran DDL");
        assert_eq!(schema_objects(&pool).await?, objects);
        assert_eq!(operator_triggers(&pool).await?, guarded);
        assert_eq!(ledger_epochs(&pool).await?, legacy);
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT block_hash,state FROM qbit_block_candidate_outbox ORDER BY block_hash",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(rows, vec![(terminal.clone(), "submitted".into())]);
        // Without the guard (the operator's decision, not the migrator's)
        // the same source migrates and is recorded as what it was; the
        // operator's table keeps a trigger that still fires; the native
        // writers put their epoch-0 shares after the legacy one.
        sqlx::raw_sql("DROP TRIGGER operator_epoch_guard ON qbit_share_ledger")
            .execute(&pool)
            .await?;
        let ledger = db.ledger("a").await?;
        assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
        assert_eq!(
            ledger.migration_source().await?.map(|s| s.source_state),
            Some(state.as_str().to_owned())
        );
        assert_eq!(
            operator_triggers(&pool).await?,
            vec!["operator_notes.operator_notes_stamp=O".to_owned()]
        );
        assert!(
            sqlx::query_scalar::<_, bool>(
                "INSERT INTO operator_notes(note) VALUES('kept') RETURNING noted_at IS NOT NULL"
            )
            .fetch_one(&pool)
            .await?
        );
        exercise_native_writers(&ledger, 1, nonce).await?;
        assert_eq!(
            ledger_epochs(&pool).await?,
            [legacy, vec![(share(1).share_id, 0)]].concat()
        );
        pool.close().await;
        db.close(vec![ledger]).await?;
    }
    Ok(())
}

#[tokio::test]
async fn row_level_security_on_a_release_table_is_refused_naming_it_and_rolls_back() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    let database: String = sqlx::query_scalar("SELECT current_database()::text")
        .fetch_one(&pool)
        .await?;
    // The test's own connection is a superuser, which bypasses row-level
    // security, so the source is built and migrated by an ordinary role
    // that owns the 2.x.x objects and may create in the database: the
    // migrate role a deployment uses, and the one the security applies to.
    let role = format!("prism_rls_{}", Uuid::new_v4().simple());
    sqlx::raw_sql(&format!(
        "CREATE ROLE {role} LOGIN PASSWORD 'rls'; GRANT USAGE, CREATE ON SCHEMA {} TO {role}; GRANT CREATE ON DATABASE {database} TO {role}",
        db.schema
    ))
    .execute(&pool)
    .await?;
    let mut limited = url::Url::parse(&db.url)?;
    limited.set_username(&role).ok().context("role username")?;
    limited
        .set_password(Some("rls"))
        .ok()
        .context("role password")?;
    let limited_pool = PgPool::connect(limited.as_str()).await?;
    apply_frozen_2x_schema(&limited_pool, SourceState::Pre258).await?;
    let hash = legacy_hash(0x44);
    insert_v1_pending(&limited_pool, &hash).await?;
    // Forced row-level security with a policy that hides pending rows. To
    // the owner the outbox now looks drained, so the drain check passes and
    // the native claim lane would never see the row; only the catalog says
    // otherwise.
    sqlx::raw_sql("ALTER TABLE qbit_block_candidate_outbox ENABLE ROW LEVEL SECURITY; ALTER TABLE qbit_block_candidate_outbox FORCE ROW LEVEL SECURITY; CREATE POLICY hide_pending ON qbit_block_candidate_outbox USING (state <> 'pending')")
        .execute(&limited_pool).await?;
    let security = || async {
        sqlx::query_scalar::<_, String>("SELECT relrowsecurity::text||','||relforcerowsecurity::text||coalesce((SELECT ','||string_agg(polname::text||'='||polcmd::text||':'||coalesce(pg_get_expr(polqual,polrelid),''),';' ORDER BY polname) FROM pg_policy WHERE polrelid=c.oid),'') FROM pg_class c WHERE c.oid=to_regclass('qbit_block_candidate_outbox')")
            .fetch_one(&pool).await
    };
    let hidden = "true,true,hide_pending=*:(state <> 'pending'::text)";
    assert_eq!(security().await?, hidden);
    assert_eq!(pending_rows(&pool).await?, 1);
    assert_eq!(pending_rows(&limited_pool).await?, 0);
    let objects = schema_objects(&pool).await?;
    let error = Ledger::connect(limited.as_str(), "rls".into(), 8, true)
        .await
        .err()
        .context(
            "migration accepted a release table with forced row-level security and a hiding policy",
        )?
        .to_string();
    assert!(
        error.contains("refusing to migrate a drifted 001 source"),
        "{error}"
    );
    assert!(error.contains("2 object(s) differ"), "{error}");
    assert!(
        error.contains("table qbit_block_candidate_outbox differs: expected row-level security disabled, found enabled and forced"),
        "{error}"
    );
    assert!(
        error.contains("policy hide_pending on qbit_block_candidate_outbox: FOR ALL USING ((state <> 'pending'::text)); the release has no row-level security policy on this table"),
        "{error}"
    );
    assert!(error.contains("Nothing was changed"), "{error}");
    // Rolled back whole: no native table, the same objects, the security as
    // it was, the row still pending and still hidden from the role.
    assert!(native_tables_absent(&pool).await?, "refusal ran DDL");
    assert_eq!(schema_objects(&pool).await?, objects);
    assert_eq!(security().await?, hidden);
    assert_eq!(pending_rows(&pool).await?, 1);
    assert_eq!(pending_rows(&limited_pool).await?, 0);
    // Without the policy, forced security hides every row from the owner
    // just the same: still refused, naming the flags.
    sqlx::raw_sql("DROP POLICY hide_pending ON qbit_block_candidate_outbox")
        .execute(&limited_pool)
        .await?;
    assert_eq!(security().await?, "true,true");
    assert_eq!(pending_rows(&limited_pool).await?, 0);
    let error = Ledger::connect(limited.as_str(), "rls".into(), 8, true)
        .await
        .err()
        .context("migration accepted a release table with forced row-level security")?
        .to_string();
    assert!(
        error.contains("refusing to migrate a drifted 001 source"),
        "{error}"
    );
    assert!(error.contains("1 object(s) differ"), "{error}");
    assert!(
        error.contains("table qbit_block_candidate_outbox differs: expected row-level security disabled, found enabled and forced"),
        "{error}"
    );
    assert!(!error.contains("policy"), "{error}");
    assert!(native_tables_absent(&pool).await?, "refusal ran DDL");
    assert_eq!(schema_objects(&pool).await?, objects);
    assert_eq!(security().await?, "true,true");
    // Security off, the role sees the row again, and the drain check
    // refuses it as it must; drained, the same source migrates as the same
    // role and is recorded as v2.0.1.
    sqlx::raw_sql("ALTER TABLE qbit_block_candidate_outbox NO FORCE ROW LEVEL SECURITY; ALTER TABLE qbit_block_candidate_outbox DISABLE ROW LEVEL SECURITY")
        .execute(&limited_pool).await?;
    assert_eq!(security().await?, "false,false");
    assert_eq!(pending_rows(&limited_pool).await?, 1);
    let error = Ledger::connect(limited.as_str(), "rls".into(), 8, true)
        .await
        .err()
        .context("migration accepted an undrained outbox once its row was visible")?
        .to_string();
    assert!(
        error.contains(
            "legacy Python block outbox is not drained: 1 pending 2.x.x candidate row(s)"
        ),
        "{error}"
    );
    assert!(native_tables_absent(&pool).await?, "refusal ran DDL");
    drain_2x_row(&limited_pool, &hash, false).await?;
    let ledger = Ledger::connect(limited.as_str(), "rls".into(), 8, true).await?;
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
    assert_eq!(
        ledger.migration_source().await?.map(|s| s.source_state),
        Some("pre_258".into())
    );
    exercise_native_writers(&ledger, 1, 6001).await?;
    ledger.pool.close().await;
    limited_pool.close().await;
    sqlx::raw_sql(&format!("DROP OWNED BY {role}; DROP ROLE {role}"))
        .execute(&pool)
        .await?;
    pool.close().await;
    db.close(vec![]).await
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
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
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

/// A database at 6 declares `candidate_storage_version`: 006 created the
/// table and the row, and nothing native removes them. Without the row, or
/// the table, the database can no longer say which release wrote it, so a
/// start refuses it instead of reading the absence as a legacy state, and
/// migrate refuses it before any DDL rather than applying a missing 009
/// above it. Declared again as 006 declares it, the database starts and
/// migrates.
#[tokio::test]
async fn migrated_database_without_its_capability_declaration_is_refused_at_connect_and_at_migrate(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    let ledger = db.ledger("init").await?;
    let row_gone = "database is at schema migration 6 but qbit_prism_schema_capabilities has no candidate_storage_version row";
    let table_gone = "database is at schema migration 6 but has no qbit_prism_schema_capabilities";
    // On the migrated database: the row deleted, then the table dropped,
    // refused with and without initialize, and nothing changes.
    for (statement, message, remedy) in [
        (
            "DELETE FROM qbit_prism_schema_capabilities WHERE capability='candidate_storage_version'",
            row_gone,
            "VALUES('candidate_storage_version',1)",
        ),
        (
            "DROP TABLE qbit_prism_schema_capabilities",
            table_gone,
            "006_source_schema.sql",
        ),
    ] {
        sqlx::raw_sql(statement).execute(&pool).await?;
        let before = schema_objects(&pool).await?;
        for initialize in [false, true] {
            let error = Ledger::connect(&db.url, "cold".into(), 8, initialize)
                .await
                .err()
                .with_context(|| {
                    format!("connect(initialize={initialize}) accepted a database at 6 without its capability declaration ({statement})")
                })?
                .to_string();
            assert!(error.contains(message), "{error}");
            assert!(error.contains(remedy), "{error}");
            if initialize {
                assert!(
                    error.contains("refusing to migrate a native database at schema migrations 2, 3, 4, 5, 6, 8, 9 before any DDL"),
                    "{error}"
                );
            }
            assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
            assert_eq!(schema_objects(&pool).await?, before);
        }
    }
    // Declared again as 006 declares it, the database starts.
    sqlx::raw_sql(include_str!("../../migrations/006_source_schema.sql"))
        .execute(&pool)
        .await?;
    assert_eq!(capability(&pool).await?, Some(1));
    let follower = Ledger::connect(&db.url, "cold".into(), 8, false).await?;
    // With 009 missing as well, migrate refuses the missing row before any
    // DDL: 009 is not applied above it. Declared again, it is.
    undo_009(&pool).await?;
    sqlx::raw_sql(
        "DELETE FROM qbit_prism_schema_capabilities WHERE capability='candidate_storage_version'",
    )
    .execute(&pool)
    .await?;
    let before = schema_objects(&pool).await?;
    let error = db
        .ledger("this-build")
        .await
        .err()
        .context("migrate applied 009 above a missing capability declaration")?
        .to_string();
    assert!(
        error.contains("refusing to migrate a native database at schema migrations 2, 3, 4, 5, 6, 8 before any DDL"),
        "{error}"
    );
    assert!(error.contains(row_gone), "{error}");
    assert_eq!(schema_versions(&pool).await?, [2, 3, 4, 5, 6, 8]);
    assert_eq!(schema_objects(&pool).await?, before);
    sqlx::raw_sql("INSERT INTO qbit_prism_schema_capabilities(capability,capability_value) VALUES('candidate_storage_version',1)")
        .execute(&pool).await?;
    let migrated = db.ledger("this-build").await?;
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
    assert_eq!(capability(&pool).await?, Some(1));
    exercise_native_writers(&migrated, 1, 6201).await?;
    pool.close().await;
    db.close(vec![ledger, follower, migrated]).await
}

/// Simulate the database a build without migration 009 left: the current
/// migration, then 009 undone. Its table, its job index and the sequence
/// cycle go with the version row, so 009 can run again.
async fn undo_009(pool: &PgPool) -> Result<()> {
    assert_eq!(schema_versions(pool).await?, REQUIRED_SCHEMA_VERSIONS);
    sqlx::raw_sql("DELETE FROM qbit_prism_schema_migrations WHERE version=9; DROP TABLE qbit_prism_session_reservations; DROP INDEX qbit_prism_jobs_extranonce1_expiry_idx; ALTER SEQUENCE qbit_prism_session_sequence NO CYCLE")
        .execute(pool).await?;
    assert_eq!(schema_versions(pool).await?, [2, 3, 4, 5, 6, 8]);
    Ok(())
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
        error.contains("requires schema migrations 2, 3, 4, 5, 6, 8, 9"),
        "{error}"
    );
    assert!(error.contains("qbit-prism-server migrate"), "{error}");
    assert!(
        native_tables_absent(&pool).await?,
        "a non-initializing start ran DDL"
    );

    // An older native schema: everything but the newest migration. Each
    // required migration is checked on its own, so the last one missing is
    // refused like any other gap.
    let ledger = db.ledger("init").await?;
    undo_009(&pool).await?;
    let error = Ledger::connect(&db.url, "cold".into(), 8, false)
        .await
        .err()
        .context("a non-initializing start accepted an older schema")?
        .to_string();
    assert!(
        error.contains(
            "missing migration(s) 9; this server requires 2, 3, 4, 5, 6, 8, 9 and found 2, 3, 4, 5, 6, 8"
        ),
        "{error}"
    );
    assert!(error.contains("qbit-prism-server migrate"), "{error}");
    assert_eq!(
        schema_versions(&pool).await?,
        [2, 3, 4, 5, 6, 8],
        "a non-initializing start ran a migration"
    );
    // Initializing brings it forward again, without rewriting the source record.
    let repaired = db.ledger("init-again").await?;
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
    assert_eq!(
        repaired.migration_source().await?.map(|s| s.migrated_by),
        Some("init".into())
    );

    // A migration this release does not know: a later release's additive
    // migration ran first (007 is reserved), and a frontend still on this
    // release keeps starting during the rollout. A format it must not touch
    // is declared as a capability, which
    // newer_storage_version_or_capability_is_refused_at_migrate_and_at_connect
    // covers.
    sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(7)")
        .execute(&pool)
        .await?;
    let follower = Ledger::connect(&db.url, "cold".into(), 8, false)
        .await
        .context("a start refused an additive unknown migration")?;
    assert_eq!(
        schema_versions(&pool).await?,
        [2, 3, 4, 5, 6, 7, 8, 9],
        "a non-initializing start rewrote the newer schema"
    );
    // Initializing on it is a no-op too: no migration is reapplied and the
    // source record stands.
    let initializer = db.ledger("init-on-newer").await?;
    assert_eq!(schema_versions(&pool).await?, [2, 3, 4, 5, 6, 7, 8, 9]);
    assert_eq!(
        initializer.migration_source().await?.map(|s| s.migrated_by),
        Some("init".into())
    );
    pool.close().await;
    db.close(vec![ledger, repaired, follower, initializer])
        .await
}

/// When a migration was recorded, as text, to prove a step was not re-run.
async fn applied_at(pool: &PgPool, version: i32) -> Result<String> {
    Ok(sqlx::query_scalar(
        "SELECT applied_at::text FROM qbit_prism_schema_migrations WHERE version=$1",
    )
    .bind(version)
    .fetch_one(pool)
    .await?)
}

/// A database a build with 009 but without 006 left at migrations 2, 3, 4,
/// 5, 8 and 9: the next migrate applies 006 alone, in its place, and leaves
/// 009 as it found it.
#[tokio::test]
async fn pre_006_native_schema_with_009_applies_006_on_the_next_migrate() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    let earlier = db.ledger("earlier-build").await?;
    undo_006(&pool, SourceState::Pre258).await?;
    let applied_009 = applied_at(&pool, 9).await?;
    let migrated = db.ledger("this-build").await?;
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
    assert!(
        !objects_006_absent(&pool, SourceState::Pre258).await?,
        "006 did not run"
    );
    assert_eq!(capability(&pool).await?, Some(1));
    // 009 was neither reapplied nor re-recorded: its record, its table and
    // the sequence cycle are as the earlier build left them.
    assert_eq!(applied_at(&pool, 9).await?, applied_009);
    let cycled: bool = sqlx::query_scalar("SELECT seqcycle FROM pg_sequence WHERE seqrelid='qbit_prism_session_sequence'::regclass AND to_regclass('qbit_prism_session_reservations') IS NOT NULL")
        .fetch_one(&pool).await?;
    assert!(cycled);
    let source = migrated
        .migration_source()
        .await?
        .context("migration source not recorded")?;
    assert_eq!(source.source_state, "native");
    assert_eq!(source.prior_schema_version, 9);
    assert_eq!(source.migrated_by, "this-build");
    exercise_native_writers(&migrated, 1, 5801).await?;
    pool.close().await;
    db.close(vec![earlier, migrated]).await
}

/// The same database started without `PRISM_POSTGRES_INIT_SCHEMA`: 009 being
/// present does not stand in for the missing 006.
#[tokio::test]
async fn startup_without_initialize_refuses_a_pre_006_native_schema_with_009() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    let earlier = db.ledger("earlier-build").await?;
    undo_006(&pool, SourceState::Pre258).await?;
    let error = Ledger::connect(&db.url, "cold".into(), 8, false)
        .await
        .err()
        .context("a non-initializing start accepted a database without 006")?
        .to_string();
    assert!(
        error.contains(
            "missing migration(s) 6; this server requires 2, 3, 4, 5, 6, 8, 9 and found 2, 3, 4, 5, 8, 9"
        ),
        "{error}"
    );
    assert!(error.contains("qbit-prism-server migrate"), "{error}");
    // Nothing ran: the same rows, and none of 006's objects.
    assert_eq!(schema_versions(&pool).await?, [2, 3, 4, 5, 8, 9]);
    assert!(objects_006_absent(&pool, SourceState::Pre258).await?);
    pool.close().await;
    db.close(vec![earlier]).await
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

/// Simulate the database an earlier 3.x.x build left at migrations 2, 3, 4,
/// 5 and 9, before migration 006 existed, on a 2.x.x source of the given
/// state. Built faithfully: the frozen release files, then the current
/// migration, then 006 undone. The version 6 row is deleted and `qbit_prism_migration_source`
/// is dropped. On a #258 source 006's other statements were no-ops, because
/// 002 had already added `storage_version` and the capability row and 006's
/// `ADD COLUMN IF NOT EXISTS` and `ON CONFLICT DO NOTHING` left them alone,
/// so the 002 objects and the capability row stay exactly as 002 made them.
/// On a pre-#258 source 006 created the column and the capability table
/// itself, so both are dropped again: that build saw an outbox without
/// `storage_version`.
async fn undo_006(pool: &PgPool, state: SourceState) -> Result<()> {
    assert_eq!(schema_versions(pool).await?, REQUIRED_SCHEMA_VERSIONS);
    sqlx::raw_sql("DELETE FROM qbit_prism_schema_migrations WHERE version=6; DROP TABLE qbit_prism_migration_source")
        .execute(pool).await?;
    if state == SourceState::Pre258 {
        sqlx::raw_sql("ALTER TABLE qbit_block_candidate_outbox DROP COLUMN storage_version; DROP TABLE qbit_prism_schema_capabilities")
            .execute(pool).await?;
    }
    assert_eq!(schema_versions(pool).await?, [2, 3, 4, 5, 8, 9]);
    assert!(objects_006_absent(pool, state).await?);
    Ok(())
}

/// Nothing 006 creates is present: its record table, and on a pre-#258
/// source the capability table and the `storage_version` column too (002
/// owns those on a #258 source).
async fn objects_006_absent(pool: &PgPool, state: SourceState) -> Result<bool> {
    let record_absent: bool =
        sqlx::query_scalar("SELECT to_regclass('qbit_prism_migration_source') IS NULL")
            .fetch_one(pool)
            .await?;
    if state == SourceState::Applied258 {
        return Ok(record_absent);
    }
    let rest_absent: bool = sqlx::query_scalar("SELECT to_regclass('qbit_prism_schema_capabilities') IS NULL AND NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass('qbit_block_candidate_outbox') AND attname='storage_version' AND NOT attisdropped)")
        .fetch_one(pool).await?;
    Ok(record_absent && rest_absent)
}

#[tokio::test]
async fn pre_006_native_schema_on_a_258_source_refuses_a_pending_v2_row_before_any_ddl(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    apply_frozen_2x_schema(&pool, SourceState::Applied258).await?;
    insert_v2_terminal(&pool, &legacy_hash(0x44), "abandoned").await?;
    let earlier = db.ledger("earlier-build").await?;
    undo_006(&pool, SourceState::Applied258).await?;
    // The pending v2 row that build's v1-only predicate never counted.
    let pending = legacy_hash(0x22);
    let body = insert_v2_pending(&pool, &pending).await?;
    let before = schema_objects(&pool).await?;
    let error = db
        .ledger("this-build")
        .await
        .err()
        .context("migration 006 accepted a pre-006 native database with a pending v2 row")?
        .to_string();
    assert!(
        error.contains("refusing to apply migration 006 to a native database at schema migrations 2, 3, 4, 5, 8, 9: an earlier 3.x.x build migrated it before the drain rule covered these rows, and the legacy Python block outbox is not drained: 1 pending 2.x.x candidate row(s) cannot be replayed natively"),
        "{error}"
    );
    assert!(
        error.contains(&format!("block_hash={pending} storage_version=2")),
        "{error}"
    );
    assert!(
        error.contains("Nothing was changed. Restore the pre-migration 2.x.x backup")
            && error.contains("lab.prism.recover_pending_blocks")
            && error.contains("not supported against a native schema")
            && error.contains("Do not delete pending rows"),
        "{error}"
    );
    // Unchanged: migrations 2, 3, 4, 5, 8, 9, no 006 object, the schema and the rows as they were.
    assert_eq!(schema_versions(&pool).await?, [2, 3, 4, 5, 8, 9]);
    assert!(objects_006_absent(&pool, SourceState::Applied258).await?);
    assert_eq!(schema_objects(&pool).await?, before);
    assert_eq!(pending_rows(&pool).await?, 1);
    assert_eq!(capability(&pool).await?, Some(2));
    assert!(sqlx::query_scalar::<_,bool>("SELECT state='pending' AND body_id=$2 AND claim_token IS NULL FROM qbit_block_candidate_outbox WHERE block_hash=$1")
        .bind(&pending).bind(&body).fetch_one(&pool).await?, "refusal touched the row");
    // Drained with the 2.x.x release, the same database migrates to 6 and is
    // recorded as a native source that declared version 2.
    drain_2x_row(&pool, &pending, true).await?;
    let migrated = db.ledger("this-build").await?;
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
    let source = migrated
        .migration_source()
        .await?
        .context("migration source not recorded")?;
    assert_eq!(source.source_state, "native");
    // The highest migration the earlier build had recorded: 009.
    assert_eq!(source.prior_schema_version, 9);
    assert_eq!(source.candidate_storage_version, Some(2));
    assert_eq!(capability(&pool).await?, Some(2));
    exercise_native_writers(&migrated, 1, 5501).await?;
    pool.close().await;
    db.close(vec![earlier, migrated]).await
}

/// A database an earlier 3.x.x build migrated, which a newer release then
/// wrote: its capability rows are refused before 004, 005 and 006 run, so
/// this build never alters it and never records version 6 for it. On a
/// #258 source the row is 002's and survives `undo_006`.
#[tokio::test]
async fn pre_006_native_schema_declaring_a_newer_capability_is_refused_before_any_ddl() -> Result<()>
{
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    apply_frozen_2x_schema(&pool, SourceState::Applied258).await?;
    let earlier = db.ledger("earlier-build").await?;
    undo_006(&pool, SourceState::Applied258).await?;
    // The row 002 made, raised as a newer release would raise it.
    sqlx::raw_sql("UPDATE qbit_prism_schema_capabilities SET capability_value=3 WHERE capability='candidate_storage_version'")
        .execute(&pool).await?;
    let before = schema_objects(&pool).await?;
    let error = db
        .ledger("this-build")
        .await
        .err()
        .context(
            "migrate accepted a pre-006 native database declaring candidate_storage_version = 3",
        )?
        .to_string();
    assert!(
        error.contains("refusing to migrate a native database at schema migrations 2, 3, 4, 5, 8, 9 before any DDL"),
        "{error}"
    );
    assert!(
        error.contains("candidate_storage_version = 3, but this server understands candidate_storage_version 1 to 2"),
        "{error}"
    );
    assert!(error.contains("upgrade the server"), "{error}");
    // Unchanged: migrations 2, 3, 4, 5, 8, 9, no 006 object, the schema and
    // the row as they were.
    assert_eq!(schema_versions(&pool).await?, [2, 3, 4, 5, 8, 9]);
    assert!(objects_006_absent(&pool, SourceState::Applied258).await?);
    assert_eq!(schema_objects(&pool).await?, before);
    assert_eq!(capability(&pool).await?, Some(3));
    // A start without initialize is refused as well, by whichever gate
    // reads the database first: the missing 006 or the row.
    let error = Ledger::connect(&db.url, "cold".into(), 8, false)
        .await
        .err()
        .context("a non-initializing start accepted a pre-006 native database declaring candidate_storage_version = 3")?
        .to_string();
    assert!(
        error.contains("missing migration(s) 6") || error.contains("candidate_storage_version = 3"),
        "{error}"
    );
    assert_eq!(schema_versions(&pool).await?, [2, 3, 4, 5, 8, 9]);
    assert_eq!(schema_objects(&pool).await?, before);
    // Back at 2, a capability this release does not know is refused the
    // same way, naming it.
    sqlx::raw_sql("UPDATE qbit_prism_schema_capabilities SET capability_value=2; INSERT INTO qbit_prism_schema_capabilities(capability,capability_value) VALUES('sealed_share_pages',1)")
        .execute(&pool).await?;
    let error = db
        .ledger("this-build")
        .await
        .err()
        .context("migrate accepted a pre-006 native database declaring an unknown capability")?
        .to_string();
    assert!(
        error.contains("refusing to migrate a native database at schema migrations 2, 3, 4, 5, 8, 9 before any DDL"),
        "{error}"
    );
    assert!(
        error.contains("capability sealed_share_pages = 1, which this server does not understand"),
        "{error}"
    );
    assert_eq!(schema_versions(&pool).await?, [2, 3, 4, 5, 8, 9]);
    assert!(objects_006_absent(&pool, SourceState::Applied258).await?);
    assert_eq!(schema_objects(&pool).await?, before);
    assert_eq!(capability(&pool).await?, Some(2));
    // Without that row the same database migrates to 6 and is recorded as
    // a native source that declared version 2.
    sqlx::raw_sql(
        "DELETE FROM qbit_prism_schema_capabilities WHERE capability='sealed_share_pages'",
    )
    .execute(&pool)
    .await?;
    let migrated = db.ledger("this-build").await?;
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
    let source = migrated
        .migration_source()
        .await?
        .context("migration source not recorded")?;
    assert_eq!(source.source_state, "native");
    assert_eq!(source.prior_schema_version, 9);
    assert_eq!(source.candidate_storage_version, Some(2));
    assert_eq!(capability(&pool).await?, Some(2));
    exercise_native_writers(&migrated, 1, 6001).await?;
    pool.close().await;
    db.close(vec![earlier, migrated]).await
}

/// A migration record with 3 and not 2: every native build records both in
/// one transaction, so the record was edited or restored selectively.
/// Migrate refuses it before any DDL, so the missing 006 stays missing
/// rather than being applied above a broken record, and it neither re-runs
/// 002 nor records it unseen; a start without initialize refuses the gap.
/// Recorded again by the operator, the same database migrates.
#[tokio::test]
async fn native_record_with_3_and_not_2_is_refused_before_any_ddl_and_not_repaired() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    let earlier = db.ledger("earlier-build").await?;
    undo_006(&pool, SourceState::Pre258).await?;
    sqlx::query("DELETE FROM qbit_prism_schema_migrations WHERE version=2")
        .execute(&pool)
        .await?;
    assert_eq!(schema_versions(&pool).await?, [3, 4, 5, 8, 9]);
    let before = schema_objects(&pool).await?;
    let error = db
        .ledger("this-build")
        .await
        .err()
        .context("migrate accepted a record with 3 and not 2")?
        .to_string();
    assert!(
        error.contains("refusing to migrate a native database at schema migrations 3, 4, 5, 8, 9 before any DDL: migration 3 is recorded and 2 is not"),
        "{error}"
    );
    assert!(
        error.contains("Nothing was changed")
            && error.contains("INSERT INTO qbit_prism_schema_migrations(version) VALUES(2)"),
        "{error}"
    );
    // Unchanged: the record as it was, no 006 object, the schema as it was.
    assert_eq!(schema_versions(&pool).await?, [3, 4, 5, 8, 9]);
    assert!(objects_006_absent(&pool, SourceState::Pre258).await?);
    assert_eq!(schema_objects(&pool).await?, before);
    // A start without initialize refuses the gap and changes nothing either.
    let error = Ledger::connect(&db.url, "cold".into(), 8, false)
        .await
        .err()
        .context("a non-initializing start accepted a record with 3 and not 2")?
        .to_string();
    assert!(
        error.contains(
            "missing migration(s) 2, 6; this server requires 2, 3, 4, 5, 6, 8, 9 and found 3, 4, 5, 8, 9"
        ),
        "{error}"
    );
    assert_eq!(schema_versions(&pool).await?, [3, 4, 5, 8, 9]);
    assert_eq!(schema_objects(&pool).await?, before);
    // Recorded again once 002's objects are verified present, the same
    // database migrates: 006 runs, nothing else is re-run.
    let applied_003 = applied_at(&pool, 3).await?;
    sqlx::query("INSERT INTO qbit_prism_schema_migrations(version) VALUES(2)")
        .execute(&pool)
        .await?;
    let migrated = db.ledger("this-build").await?;
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
    assert_eq!(applied_at(&pool, 3).await?, applied_003);
    assert!(
        !objects_006_absent(&pool, SourceState::Pre258).await?,
        "006 did not run"
    );
    exercise_native_writers(&migrated, 1, 6101).await?;
    pool.close().await;
    db.close(vec![earlier, migrated]).await
}

/// 006 must not preserve a malformed column that breaks future native candidates.
#[tokio::test]
async fn pre_006_native_schema_refuses_a_malformed_storage_version_column() -> Result<()> {
    for state in [SourceState::Pre258, SourceState::Applied258] {
        for alteration in [
            "ALTER COLUMN storage_version DROP NOT NULL, ALTER COLUMN storage_version DROP DEFAULT",
            "ALTER COLUMN storage_version DROP NOT NULL",
            "ALTER COLUMN storage_version DROP DEFAULT",
            "ALTER COLUMN storage_version SET DEFAULT 2",
            "ALTER COLUMN storage_version TYPE bigint",
        ] {
            let Some(db) = Database::open().await? else {
                return Ok(());
            };
            let pool = PgPool::connect(&db.url).await?;
            apply_frozen_2x_schema(&pool, state).await?;
            let earlier = db.ledger("earlier-build").await?;
            earlier.append(share(1), None).await?;
            let block = candidate(&earlier.snapshot(100).await?, 7001)?;
            earlier.enqueue_candidate(block.clone()).await?;
            undo_006(&pool, state).await?;
            if state == SourceState::Pre258 {
                sqlx::raw_sql("ALTER TABLE qbit_block_candidate_outbox ADD COLUMN storage_version integer NOT NULL DEFAULT 1").execute(&pool).await?;
            }
            sqlx::raw_sql(&format!(
                "ALTER TABLE qbit_block_candidate_outbox {alteration}"
            ))
            .execute(&pool)
            .await?;
            assert!(
                sqlx::query_scalar::<_, bool>(
                    "SELECT bool_and(storage_version=1) FROM qbit_block_candidate_outbox"
                )
                .fetch_one(&pool)
                .await?
            );
            let versions = schema_versions(&pool).await?;
            let objects = schema_objects(&pool).await?;
            let columns_sql = "SELECT format_type(a.atttypid,a.atttypmod),a.attnotnull,pg_get_expr(d.adbin,d.adrelid) FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attrelid='qbit_block_candidate_outbox'::regclass AND a.attname='storage_version' AND NOT a.attisdropped";
            let column: (String, bool, Option<String>) =
                sqlx::query_as(columns_sql).fetch_one(&pool).await?;
            let rows: Vec<Value> =
                sqlx::query_scalar("SELECT to_jsonb(o) FROM qbit_block_candidate_outbox o")
                    .fetch_all(&pool)
                    .await?;
            let error = db
                .ledger("this-build")
                .await
                .err()
                .context("006 accepted a malformed storage_version column")?
                .to_string();
            assert!(
                error.contains("refusing to migrate a native database"),
                "{error}"
            );
            assert!(
                error.contains(
                    "before any DDL: column qbit_block_candidate_outbox.storage_version differs"
                ),
                "{error}"
            );
            assert!(error.contains("Nothing was changed"), "{error}");
            assert_eq!(schema_versions(&pool).await?, versions);
            assert_eq!(schema_objects(&pool).await?, objects);
            assert_eq!(
                sqlx::query_as::<_, (String, bool, Option<String>)>(columns_sql)
                    .fetch_one(&pool)
                    .await?,
                column
            );
            assert_eq!(
                sqlx::query_scalar::<_, Value>(
                    "SELECT to_jsonb(o) FROM qbit_block_candidate_outbox o"
                )
                .fetch_all(&pool)
                .await?,
                rows
            );
            assert!(
                sqlx::query_scalar::<_, bool>(
                    "SELECT to_regclass('qbit_prism_migration_source') IS NULL"
                )
                .fetch_one(&pool)
                .await?
            );
            sqlx::raw_sql("ALTER TABLE qbit_block_candidate_outbox ALTER COLUMN storage_version TYPE integer, ALTER COLUMN storage_version SET NOT NULL, ALTER COLUMN storage_version SET DEFAULT 1").execute(&pool).await?;
            let migrated = db.ledger("this-build").await?;
            assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
            let claim = migrated
                .claim_candidate(60)
                .await?
                .context("existing native candidate was stranded")?;
            assert_eq!(claim.candidate.block_hash, block.block_hash);
            migrated
                .land_candidate(&claim, &keys().1.public_key_hex())
                .await?;
            migrated.finish_candidate(&claim, true, None).await?;
            exercise_native_writers(&migrated, 2, 7002).await?;
            pool.close().await;
            db.close(vec![earlier, migrated]).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn pre_006_native_schema_with_only_native_pending_candidates_migrates_and_keeps_them_claimable(
) -> Result<()> {
    for state in [SourceState::Applied258, SourceState::Pre258] {
        let Some(db) = Database::open().await? else {
            return Ok(());
        };
        let pool = PgPool::connect(&db.url).await?;
        apply_frozen_2x_schema(&pool, state).await?;
        insert_v1_terminal(&pool, &legacy_hash(0x33), "submitted").await?;
        let earlier = db.ledger("earlier-build").await?;
        // A block that build found and had not submitted yet: a native v1
        // row, whose body carries payout_revision, bundle and block_hash.
        earlier.append(share(1), None).await?;
        let block = candidate(&earlier.snapshot(100).await?, 5601)?;
        earlier.enqueue_candidate(block.clone()).await?;
        undo_006(&pool, state).await?;
        assert_eq!(pending_rows(&pool).await?, 1);
        let migrated = db.ledger("this-build").await.with_context(|| {
            format!("006 refused a native pending candidate on a {state:?} source")
        })?;
        assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
        let source = migrated
            .migration_source()
            .await?
            .context("migration source not recorded")?;
        assert_eq!(source.source_state, "native");
        assert_eq!(source.prior_schema_version, 9);
        let declared = match state {
            SourceState::Applied258 => Some(2),
            _ => None,
        };
        assert_eq!(source.candidate_storage_version, declared);
        assert_eq!(capability(&pool).await?, Some(declared.unwrap_or(1)));
        let claim = migrated
            .claim_candidate(60)
            .await?
            .context("the native candidate is no longer claimable after 006")?;
        assert_eq!(claim.candidate.block_hash, block.block_hash);
        migrated
            .land_candidate(&claim, &keys().1.public_key_hex())
            .await?;
        migrated.finish_candidate(&claim, true, None).await?;
        assert_eq!(pending_rows(&pool).await?, 0);
        pool.close().await;
        db.close(vec![earlier, migrated]).await?;
    }
    Ok(())
}

#[tokio::test]
async fn pre_006_native_schema_on_a_pre_258_source_refuses_an_undrained_v1_row_with_the_v1_only_predicate(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let pool = PgPool::connect(&db.url).await?;
    apply_frozen_2x_schema(&pool, SourceState::Pre258).await?;
    let earlier = db.ledger("earlier-build").await?;
    undo_006(&pool, SourceState::Pre258).await?;
    // A 2.x.x v1 row the native lane cannot replay, on an outbox without
    // storage_version or body_id: the predicate must take its v1-only form.
    let pending = legacy_hash(0x11);
    insert_v1_pending(&pool, &pending).await?;
    let error = db
        .ledger("this-build")
        .await
        .err()
        .context("migration 006 accepted a pre-006 native database with an undrained v1 row")?
        .to_string();
    assert!(
        error.contains("refusing to apply migration 006 to a native database at schema migrations 2, 3, 4, 5, 8, 9"),
        "{error}"
    );
    assert!(
        error.contains(&format!("block_hash={pending} storage_version=1")),
        "{error}"
    );
    assert!(
        !error.contains("does not exist"),
        "pre-#258 native refusal was a SQL error: {error}"
    );
    assert_eq!(schema_versions(&pool).await?, [2, 3, 4, 5, 8, 9]);
    assert!(objects_006_absent(&pool, SourceState::Pre258).await?);
    assert_eq!(pending_rows(&pool).await?, 1);
    drain_2x_row(&pool, &pending, false).await?;
    let migrated = db.ledger("this-build").await?;
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
    assert_eq!(capability(&pool).await?, Some(1));
    assert_eq!(
        migrated
            .migration_source()
            .await?
            .map(|s| (s.source_state, s.prior_schema_version)),
        Some(("native".into(), 9))
    );
    exercise_native_writers(&migrated, 1, 5701).await?;
    pool.close().await;
    db.close(vec![earlier, migrated]).await
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
    assert_eq!(schema_versions(&pool).await?, REQUIRED_SCHEMA_VERSIONS);
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

// ---------------------------------------------------------------------------
// Imported legacy audits are served from their canonical bytes (#265).
// ---------------------------------------------------------------------------

fn imported_audit_router(pool: &PgPool) -> axum::Router {
    use qbit_prism_server::api::{self, ApiConfig, ApiState};
    api::router(ApiState::new(
        pool.clone(),
        ApiConfig {
            cache_enabled: false,
            ..Default::default()
        },
        std::sync::Arc::new(qbit_prism_server::metrics::Metrics::default()),
    ))
}

async fn api_get(app: &axum::Router, path: &str) -> Result<(axum::http::StatusCode, Value)> {
    use tower::ServiceExt;
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri(path)
                .body(axum::body::Body::empty())?,
        )
        .await?;
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    Ok((status, serde_json::from_slice(&bytes)?))
}

async fn land_confirmed(ledger: &Ledger, block: &Candidate) -> Result<()> {
    ledger.enqueue_candidate(block.clone()).await?;
    let claim = ledger.claim_candidate(60).await?.unwrap();
    ledger
        .land_candidate(&claim, &keys().1.public_key_hex())
        .await?;
    ledger.finish_candidate(&claim, true, None).await
}

/// Reshape a landed native row as a 2.x externalized row: its body lives only
/// in the `body_uri` file, as the legacy import finds it.
async fn externalize(
    pool: &PgPool,
    block: &Candidate,
    dir: &std::path::Path,
) -> Result<std::path::PathBuf> {
    let path = dir.join(format!("legacy-audit-{}.json", block.block_hash));
    std::fs::write(&path, serde_json::to_vec(&block.bundle)?)?;
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=NULL,share_snapshot_sha256=NULL,body_uri=$2 WHERE block_hash=$1")
        .bind(&block.block_hash).bind(path.to_str().unwrap()).execute(pool).await?;
    Ok(path)
}

/// The metadata columns the import writes, plus the stored bits it must not.
async fn audit_metadata(pool: &PgPool, hash: &str) -> Result<Value> {
    Ok(sqlx::query_scalar("SELECT jsonb_build_object('schema_version',schema_version,'difficulty',found_block_network_difficulty::text,'value',found_block_coinbase_value_sats,'commitments',audit_commitment_leaves_hex,'witness',witness_merkle_leaves_hex,'bits',found_block_bits) FROM qbit_pool_audit_bundles WHERE block_hash=$1")
        .bind(hash).fetch_one(pool).await?)
}

async fn canonical_state(pool: &PgPool, hash: &str) -> Result<(bool, bool, bool)> {
    let row = sqlx::query("SELECT audit_bundle IS NULL AS body_null,canonical_audit_bytes IS NOT NULL AS canonical,encode(sha256(canonical_audit_bytes),'hex') IS NOT DISTINCT FROM audit_bundle_sha256 AS digest FROM qbit_pool_audit_bundles WHERE block_hash=$1")
        .bind(hash).fetch_one(pool).await?;
    Ok((
        row.try_get("body_null")?,
        row.try_get("canonical")?,
        row.try_get("digest")?,
    ))
}

async fn dashboard_row(app: &axum::Router, hash: &str) -> Result<Value> {
    let (status, blocks) = api_get(app, "/public/v1/blocks?chain_state=all&limit=100").await?;
    ensure_ok(status, &blocks)?;
    blocks["rows"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|row| row["hash"] == hash)
        .cloned()
        .with_context(|| format!("dashboard is missing {hash}: {blocks}"))
}

fn ensure_ok(status: axum::http::StatusCode, body: &Value) -> Result<()> {
    anyhow::ensure!(status == axum::http::StatusCode::OK, "{status}: {body}");
    Ok(())
}

/// The dashboard as the pre-#265 import left it: the same metadata columns
/// plus the inline logical body. Restores the row's current body afterwards.
async fn dashboard_row_with_inline_import(
    app: &axum::Router,
    pool: &PgPool,
    block: &Candidate,
) -> Result<Value> {
    let current: Option<Value> =
        sqlx::query_scalar("SELECT audit_bundle FROM qbit_pool_audit_bundles WHERE block_hash=$1")
            .bind(&block.block_hash)
            .fetch_one(pool)
            .await?;
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=$2 WHERE block_hash=$1")
        .bind(&block.block_hash)
        .bind(serde_json::to_value(&block.bundle)?)
        .execute(pool)
        .await?;
    let row = dashboard_row(app, &block.block_hash).await;
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=$2 WHERE block_hash=$1")
        .bind(&block.block_hash)
        .bind(current)
        .execute(pool)
        .await?;
    row
}

/// A bundle that records its settlement decision, as the coordinator builds it.
fn settled_bundle(
    snapshot: &Snapshot,
    direct_floor_sats: u64,
    config: qbit_prism::SettlementModeConfig,
) -> Result<qbit_prism::AuditBundle> {
    let (coinbase_key, ledger_key) = keys();
    Ok(qbit_prism::build_audit_bundle_with_ctv_settlement_options(
        snapshot.shares.clone(),
        FoundBlock {
            block_height: 101,
            coinbase_value_sats: 500_000_000,
            network_difficulty: 100,
            anchor_job_issued_at_ms: snapshot.anchor_ms,
        },
        snapshot.prior_balances.clone(),
        PayoutPolicy::day_one_default(),
        direct_floor_sats,
        config,
        Some(qbit_prism::FanoutFeeRatePolicy::new(1000, 12000)),
        None,
        vec![],
        &coinbase_key,
        &ledger_key,
    )?)
}

#[tokio::test]
async fn imported_external_audit_is_served_from_canonical_bytes_without_the_legacy_file(
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("imported-external").await?;
    ledger.append(share(5001), None).await?;
    let snapshot = ledger.snapshot(100).await?;
    let block = candidate_with_bundle(
        settled_bundle(&snapshot, 0, Default::default())?,
        snapshot.payout_revision,
        5001,
    )?;
    let hash = block.block_hash.clone();
    let logical = serde_json::to_value(&block.bundle)?;
    let commitment = block.bundle.audit_commitment_leaves_hex[0].clone();
    land_confirmed(&ledger, &block).await?;
    let metadata = audit_metadata(&ledger.pool, &hash).await?;
    assert_eq!(metadata["bits"], "207fffff");
    let dir = tempfile::tempdir()?;
    let file = externalize(&ledger.pool, &block, dir.path()).await?;
    let app = imported_audit_router(&ledger.pool);
    let block_path = format!("/audit/blocks/{hash}/bundle");
    let commitment_path = format!("/audit/commitments/{commitment}/bundle");
    let settlement_path = format!("/public/v1/blocks/{hash}/settlement-artifacts");
    // Before the import, the legacy file serves the same logical body the
    // pre-#265 import stored inline.
    let (status, before_block) = api_get(&app, &block_path).await?;
    ensure_ok(status, &before_block)?;
    assert_eq!(before_block["audit_bundle"], logical);
    let (status, before_commitment) = api_get(&app, &commitment_path).await?;
    ensure_ok(status, &before_commitment)?;
    let (status, mut before_settlement) = api_get(&app, &settlement_path).await?;
    ensure_ok(status, &before_settlement)?;
    let before_dashboard = dashboard_row(&app, &hash).await?;
    // 2.x wrote no metadata for some rows. The import restores all of it
    // except the bits, which no audit body carries and which stay as stored.
    sqlx::query("UPDATE qbit_pool_audit_bundles SET schema_version=NULL,found_block_network_difficulty=NULL,found_block_coinbase_value_sats=NULL,audit_commitment_leaves_hex=NULL,witness_merkle_leaves_hex=NULL WHERE block_hash=$1")
        .bind(&hash).execute(&ledger.pool).await?;
    assert_eq!(
        ledger
            .import_legacy_audits(Some(dir.path()), &keys().1.public_key_hex())
            .await?,
        1
    );
    assert_eq!(
        canonical_state(&ledger.pool, &hash).await?,
        (true, true, true),
        "import must store digest-checked canonical bytes and no inline body"
    );
    assert_eq!(audit_metadata(&ledger.pool, &hash).await?, metadata);
    std::fs::remove_file(&file)?;
    assert_eq!(ledger.audit_bundle(&hash).await?, Some(logical.clone()));
    let (status, after_block) = api_get(&app, &block_path).await?;
    ensure_ok(status, &after_block)?;
    assert_eq!(after_block, before_block);
    assert!(
        after_block.get("body_uri").is_none() && after_block.get("share_snapshot_sha256").is_none()
    );
    let (status, after_commitment) = api_get(&app, &commitment_path).await?;
    ensure_ok(status, &after_commitment)?;
    assert_eq!(after_commitment, before_commitment);
    // Direct-coinbase blocks take their settlement payload from the body.
    let (status, mut after_settlement) = api_get(&app, &settlement_path).await?;
    ensure_ok(status, &after_settlement)?;
    for payload in [&mut before_settlement, &mut after_settlement] {
        payload.as_object_mut().unwrap().remove("generated_at");
    }
    assert_eq!(after_settlement, before_settlement);
    assert_eq!(after_settlement["settlement_mode"], "direct_coinbase");
    assert!(
        after_settlement["artifact_links"]
            .as_array()
            .unwrap()
            .iter()
            .any(|link| link["kind"] == "audit_bundle"),
        "{after_settlement}"
    );
    let dashboard = dashboard_row(&app, &hash).await?;
    assert_eq!(dashboard, before_dashboard);
    assert_eq!(
        dashboard,
        dashboard_row_with_inline_import(&app, &ledger.pool, &block).await?
    );
    assert_eq!(dashboard["bits"], "207fffff");
    let (status, latest) = api_get(&app, "/audit/latest").await?;
    ensure_ok(status, &latest)?;
    assert_eq!(latest["job_share_count"], 1);
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn corrupt_imported_canonical_bytes_are_refused_not_served_from_the_legacy_file() -> Result<()>
{
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("imported-corrupt").await?;
    ledger.append(share(5101), None).await?;
    let block = candidate(&ledger.snapshot(100).await?, 5101)?;
    let hash = block.block_hash.clone();
    let commitment = block.bundle.audit_commitment_leaves_hex[0].clone();
    land_confirmed(&ledger, &block).await?;
    let dir = tempfile::tempdir()?;
    let file = externalize(&ledger.pool, &block, dir.path()).await?;
    ledger
        .import_legacy_audits(Some(dir.path()), &keys().1.public_key_hex())
        .await?;
    let (bytes, digest): (Vec<u8>, String) = sqlx::query_as(
        "SELECT canonical_audit_bytes,audit_bundle_sha256 FROM qbit_pool_audit_bundles WHERE block_hash=$1",
    )
    .bind(&hash)
    .fetch_one(&ledger.pool)
    .await?;
    let app = imported_audit_router(&ledger.pool);
    let paths = [
        format!("/audit/blocks/{hash}/bundle"),
        format!("/audit/commitments/{commitment}/bundle"),
    ];
    let mut flipped = bytes.clone();
    flipped[bytes.len() / 2] ^= 0x01;
    let digest_of = |bytes: &[u8]| hex::encode(Sha256::digest(bytes));
    // A well-formed inline body-ref envelope the shared parser would resolve.
    let envelope = {
        let mut body = serde_json::to_value(&block.bundle)?;
        let shares = body.as_object_mut().unwrap().remove("shares").unwrap();
        let seq = block.bundle.shares[0].share_seq;
        serde_json::to_vec(
            &json!({"schema":qbit_prism::AUDIT_BODY_REF_SCHEMA,"audit_bundle_sha256":digest,"share_count":1,"bundle_without_shares":body,"share_parts":[{"kind":"inline","first_share_seq":seq,"last_share_seq":seq,"share_count":1,"shares":shares}]}),
        )?
    };
    for (case, stored, declared) in [
        ("one flipped byte", flipped.clone(), digest.clone()),
        ("declared digest mismatch", bytes.clone(), "00".repeat(32)),
        (
            "digest-valid empty object",
            b"{}".to_vec(),
            digest_of(b"{}"),
        ),
        ("digest-valid non-object", b"[]".to_vec(), digest_of(b"[]")),
        (
            "digest-valid non-JSON",
            b"not json".to_vec(),
            digest_of(b"not json"),
        ),
        (
            "digest-valid body-ref envelope",
            envelope.clone(),
            digest_of(&envelope),
        ),
    ] {
        sqlx::query("UPDATE qbit_pool_audit_bundles SET canonical_audit_bytes=$2,audit_bundle_sha256=$3 WHERE block_hash=$1")
            .bind(&hash).bind(&stored).bind(&declared).execute(&ledger.pool).await?;
        assert!(file.exists());
        assert!(
            ledger.audit_bundle(&hash).await.is_err(),
            "{case}: Ledger::audit_bundle served a corrupt imported body"
        );
        for path in &paths {
            let (status, body) = api_get(&app, path).await?;
            assert_eq!(
                status,
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "{case}: {path} served {body}"
            );
            assert!(body.get("audit_bundle").is_none(), "{case}: {path}: {body}");
        }
    }
    sqlx::query("UPDATE qbit_pool_audit_bundles SET canonical_audit_bytes=$2,audit_bundle_sha256=$3 WHERE block_hash=$1")
        .bind(&hash).bind(&bytes).bind(&digest).execute(&ledger.pool).await?;
    assert_eq!(
        ledger.audit_bundle(&hash).await?,
        Some(serde_json::to_value(&block.bundle)?)
    );
    for path in &paths {
        let (status, body) = api_get(&app, path).await?;
        ensure_ok(status, &body)?;
    }
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn imported_ctv_audit_backfills_and_links_without_the_legacy_file() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("imported-ctv").await?;
    ledger.append(share(5201), None).await?;
    let mut second = share(5202);
    second.miner_id = "other".into();
    second.order_key = "other".into();
    second.p2mr_program_hex = "22".repeat(32);
    ledger.append(second, None).await?;
    let snapshot = ledger.snapshot(100).await?;
    let ledger_key = keys().1;
    let bundle = settled_bundle(
        &snapshot,
        u64::MAX,
        qbit_prism::SettlementModeConfig {
            max_fanout_recipients_per_transaction: 1,
            ..Default::default()
        },
    )?;
    let block = candidate_with_bundle(bundle, snapshot.payout_revision, 5201)?;
    let hash = block.block_hash.clone();
    land_confirmed(&ledger, &block).await?;
    let dir = tempfile::tempdir()?;
    let file = externalize(&ledger.pool, &block, dir.path()).await?;
    assert_eq!(
        ledger
            .import_legacy_audits(Some(dir.path()), &ledger_key.public_key_hex())
            .await?,
        1
    );
    std::fs::remove_file(&file)?;
    assert_eq!(ledger.backfill_ctv(&ledger_key.public_key_hex()).await?, 0);
    sqlx::query("DELETE FROM qbit_ctv_fanout_artifacts WHERE block_hash=$1 AND chunk_index=1")
        .bind(&hash)
        .execute(&ledger.pool)
        .await?;
    assert_eq!(ledger.backfill_ctv(&ledger_key.public_key_hex()).await?, 1);
    assert_eq!(ledger.backfill_ctv(&ledger_key.public_key_hex()).await?, 0);
    let app = imported_audit_router(&ledger.pool);
    // CTV payloads come from the fanout tables; the audit link comes from the
    // metadata shortcut rather than a whole-body decode.
    let (status, settlement) = api_get(
        &app,
        &format!("/public/v1/blocks/{hash}/settlement-artifacts"),
    )
    .await?;
    ensure_ok(status, &settlement)?;
    assert_eq!(settlement["settlement_mode"], "ctv_fanout");
    let audit_sha = settlement["audit_bundle_sha256"].as_str().unwrap();
    assert!(
        settlement["artifact_links"]
            .as_array()
            .unwrap()
            .iter()
            .any(|link| link["kind"] == "audit_bundle" && link["sha256"] == audit_sha),
        "{settlement}"
    );
    assert_eq!(settlement["fanouts"].as_array().unwrap().len(), 2);
    db.close(vec![ledger]).await
}

#[tokio::test]
async fn inline_only_legacy_audit_import_keeps_its_body_and_serves_canonical_bytes() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("imported-inline").await?;
    ledger.append(share(5301), None).await?;
    let block = candidate(&ledger.snapshot(100).await?, 5301)?;
    let hash = block.block_hash.clone();
    let logical = serde_json::to_value(&block.bundle)?;
    land_confirmed(&ledger, &block).await?;
    let digest: String = sqlx::query_scalar(
        "SELECT audit_bundle_sha256 FROM qbit_pool_audit_bundles WHERE block_hash=$1",
    )
    .bind(&hash)
    .fetch_one(&ledger.pool)
    .await?;
    // A 2.x row from before externalization: an inline body-ref envelope, no
    // body_uri, and none of the metadata columns, including the bits.
    let mut body = logical.clone();
    let shares = body.as_object_mut().unwrap().remove("shares").unwrap();
    let seq = block.bundle.shares[0].share_seq;
    let envelope = json!({"schema":"qbit.prism.audit-body-ref.v1","audit_bundle_sha256":digest,"share_count":1,"bundle_without_shares":body,"share_parts":[{"kind":"inline","first_share_seq":seq,"last_share_seq":seq,"share_count":1,"shares":shares}]});
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=$2,share_snapshot_sha256=NULL,body_uri=NULL,schema_version=NULL,found_block_network_difficulty=NULL,found_block_coinbase_value_sats=NULL,audit_commitment_leaves_hex=NULL,witness_merkle_leaves_hex=NULL,found_block_bits=NULL WHERE block_hash=$1")
        .bind(&hash).bind(&envelope).execute(&ledger.pool).await?;
    let app = imported_audit_router(&ledger.pool);
    let block_path = format!("/audit/blocks/{hash}/bundle");
    let (status, before) = api_get(&app, &block_path).await?;
    ensure_ok(status, &before)?;
    assert_eq!(before["audit_bundle"], envelope);
    assert_eq!(
        ledger
            .import_legacy_audits(None, &keys().1.public_key_hex())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, Value>(
            "SELECT audit_bundle FROM qbit_pool_audit_bundles WHERE block_hash=$1"
        )
        .bind(&hash)
        .fetch_one(&ledger.pool)
        .await?,
        envelope,
        "option A keeps an inline-only row's stored body unchanged"
    );
    assert_eq!(
        canonical_state(&ledger.pool, &hash).await?,
        (false, true, true)
    );
    let metadata = audit_metadata(&ledger.pool, &hash).await?;
    assert_eq!(metadata["difficulty"], "100");
    assert_eq!(metadata["value"], 500_000_000);
    assert_eq!(
        metadata["commitments"],
        json!(block.bundle.audit_commitment_leaves_hex)
    );
    assert!(metadata["bits"].is_null(), "the import invented bits");
    // Both readers serve the canonical bytes' logical body, not the envelope.
    assert_eq!(ledger.audit_bundle(&hash).await?, Some(logical.clone()));
    let (status, after) = api_get(&app, &block_path).await?;
    ensure_ok(status, &after)?;
    assert_eq!(after["audit_bundle"], logical);
    let commitment = &block.bundle.audit_commitment_leaves_hex[0];
    let (status, by_commitment) =
        api_get(&app, &format!("/audit/commitments/{commitment}/bundle")).await?;
    ensure_ok(status, &by_commitment)?;
    assert_eq!(by_commitment["audit_bundle"], logical);
    let dashboard = dashboard_row(&app, &hash).await?;
    assert_eq!(
        dashboard,
        dashboard_row_with_inline_import(&app, &ledger.pool, &block).await?
    );
    assert_eq!(dashboard["bits"], "00000000");
    // A second run finds nothing left to import and leaves the row alone.
    assert_eq!(
        ledger
            .import_legacy_audits(None, &keys().1.public_key_hex())
            .await?,
        0
    );
    db.close(vec![ledger]).await
}

/// The `audit_bundle` member each public bundle query loads for a row.
async fn bundle_query_bodies(pool: &PgPool, hash: &str, commitment: &str) -> Result<[Value; 2]> {
    let mut bodies = [Value::Null, Value::Null];
    for (body, (sql, id)) in bodies.iter_mut().zip([
        (include_str!("../../src/api/queries/audit_bundle.sql"), hash),
        (
            include_str!("../../src/api/queries/audit_bundle_by_commitment.sql"),
            commitment,
        ),
    ]) {
        let mut row: Value = sqlx::query_scalar(sql).bind(id).fetch_one(pool).await?;
        anyhow::ensure!(
            row["block_hash"] == hash && row.get("audit_bundle").is_some(),
            "{row}"
        );
        *body = row["audit_bundle"].take();
    }
    Ok(bodies)
}

#[tokio::test]
async fn superseded_inline_audit_body_is_not_loaded_for_imported_rows() -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("imported-superseded").await?;
    ledger.append(share(5401), None).await?;
    let block = candidate(&ledger.snapshot(100).await?, 5401)?;
    let hash = block.block_hash.clone();
    let logical = serde_json::to_value(&block.bundle)?;
    let commitment = block.bundle.audit_commitment_leaves_hex[0].clone();
    land_confirmed(&ledger, &block).await?;
    let dir = tempfile::tempdir()?;
    let file = externalize(&ledger.pool, &block, dir.path()).await?;
    let app = imported_audit_router(&ledger.pool);
    let paths = [
        format!("/audit/blocks/{hash}/bundle"),
        format!("/audit/commitments/{commitment}/bundle"),
    ];
    let mut before = Vec::new();
    for path in &paths {
        let (status, body) = api_get(&app, path).await?;
        ensure_ok(status, &body)?;
        assert_eq!(body["audit_bundle"], logical);
        before.push(body);
    }
    assert_eq!(
        ledger
            .import_legacy_audits(Some(dir.path()), &keys().1.public_key_hex())
            .await?,
        1
    );
    std::fs::remove_file(&file)?;
    // The row as the pre-change import left it: the full inline logical body
    // beside the canonical bytes that supersede it, its body_uri, and no
    // snapshot.
    sqlx::query("UPDATE qbit_pool_audit_bundles SET audit_bundle=$2 WHERE block_hash=$1")
        .bind(&hash)
        .bind(&logical)
        .execute(&ledger.pool)
        .await?;
    assert_eq!(
        canonical_state(&ledger.pool, &hash).await?,
        (false, true, true)
    );
    assert!(sqlx::query_scalar::<_, bool>("SELECT body_uri IS NOT NULL AND share_snapshot_sha256 IS NULL FROM qbit_pool_audit_bundles WHERE block_hash=$1")
        .bind(&hash).fetch_one(&ledger.pool).await?);
    assert_eq!(
        bundle_query_bodies(&ledger.pool, &hash, &commitment).await?,
        [Value::Null, Value::Null],
        "a bundle query loaded the superseded inline body"
    );
    for (path, before) in paths.iter().zip(&before) {
        let (status, body) = api_get(&app, path).await?;
        ensure_ok(status, &body)?;
        assert_eq!(&body, before, "{path}");
    }
    assert_eq!(ledger.audit_bundle(&hash).await?, Some(logical.clone()));
    // Without canonical bytes, the inline body is the served representation.
    sqlx::query(
        "UPDATE qbit_pool_audit_bundles SET canonical_audit_bytes=NULL WHERE block_hash=$1",
    )
    .bind(&hash)
    .execute(&ledger.pool)
    .await?;
    assert_eq!(
        bundle_query_bodies(&ledger.pool, &hash, &commitment).await?,
        [logical.clone(), logical.clone()]
    );
    for (path, before) in paths.iter().zip(&before) {
        let (status, body) = api_get(&app, path).await?;
        ensure_ok(status, &body)?;
        assert_eq!(&body, before, "{path}");
    }
    db.close(vec![ledger]).await
}

fn ensure_read_timeout((status, body): (axum::http::StatusCode, Value)) -> Result<()> {
    anyhow::ensure!(
        status == axum::http::StatusCode::SERVICE_UNAVAILABLE
            && body["error"]["code"] == "read_timeout",
        "{status}: {body}"
    );
    Ok(())
}

/// Imported audit decodes run after their read connection is released. The
/// runtime has one blocking thread and the test occupies it, so a decode the
/// API starts stays queued, holding its permit, until the test lets it run.
#[test]
fn imported_audit_decode_limit_outlives_a_dropped_request() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()?
        .block_on(decode_limit_outlives_a_dropped_request())
}

async fn decode_limit_outlives_a_dropped_request() -> Result<()> {
    use qbit_prism_server::api::{self, ApiConfig, ApiState};
    use std::time::Duration;
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let ledger = db.ledger("imported-decode-limit").await?;
    ledger.append(share(5501), None).await?;
    let snapshot = ledger.snapshot(100).await?;
    let block = candidate_with_bundle(
        settled_bundle(&snapshot, 0, Default::default())?,
        snapshot.payout_revision,
        5501,
    )?;
    let hash = block.block_hash.clone();
    land_confirmed(&ledger, &block).await?;
    let dir = tempfile::tempdir()?;
    let file = externalize(&ledger.pool, &block, dir.path()).await?;
    assert_eq!(
        ledger
            .import_legacy_audits(Some(dir.path()), &keys().1.public_key_hex())
            .await?,
        1
    );
    std::fs::remove_file(&file)?;
    let state = ApiState::new(
        ledger.pool.clone(),
        ApiConfig {
            cache_enabled: false,
            read_timeout: Duration::from_secs(1),
            ..Default::default()
        },
        std::sync::Arc::new(qbit_prism_server::metrics::Metrics::default()),
    )
    .with_read_concurrency(1);
    let limit = state.audit_decode_limit();
    let app = api::router(state);
    // A direct-coinbase settlement is read from one decode of the body.
    let path = format!("/public/v1/blocks/{hash}/settlement-artifacts");
    let settlement = |app: axum::Router| {
        let path = path.clone();
        async move {
            let (status, mut body) = api_get(&app, &path).await?;
            ensure_ok(status, &body)?;
            body.as_object_mut().unwrap().remove("generated_at");
            Ok::<_, anyhow::Error>(body)
        }
    };
    let baseline = settlement(app.clone()).await?;
    assert_eq!(baseline["settlement_mode"], "direct_coinbase");
    assert_eq!(limit.available_permits(), 1);
    // While another holder has the only permit, no decode starts: the
    // request spends its own deadline waiting instead.
    let held = limit.clone().acquire_owned().await?;
    ensure_read_timeout(api_get(&app, &path).await?)?;
    drop(held);
    assert_eq!(settlement(app.clone()).await?, baseline);

    let (release, blocked) = std::sync::mpsc::channel::<()>();
    let (started, running) = tokio::sync::oneshot::channel();
    let blocker = tokio::task::spawn_blocking(move || {
        let _ = started.send(());
        let _ = blocked.recv();
    });
    running.await?;
    // The request takes the permit and queues its decode; the deadline then
    // drops the request while it awaits that decode.
    ensure_read_timeout(api_get(&app, &path).await?)?;
    assert_eq!(
        limit.available_permits(),
        0,
        "a dropped request released its permit before its decode finished"
    );
    // A second request cannot start another decode meanwhile.
    ensure_read_timeout(api_get(&app, &path).await?)?;
    assert_eq!(limit.available_permits(), 0);
    release.send(())?;
    blocker.await?;
    tokio::time::timeout(Duration::from_secs(10), async {
        while limit.available_permits() != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("the finished decode kept its permit")?;
    assert_eq!(settlement(app).await?, baseline);
    db.close(vec![ledger]).await
}
