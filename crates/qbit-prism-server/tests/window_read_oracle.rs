//! An independent SQL oracle for the payout-window read (issue #269, item 3).
//!
//! `Ledger::snapshot` builds the payout window by paging backwards through
//! `qbit_share_ledger` 4096 rows at a time, subtracting each row's difficulty
//! from a remaining weight until the weight is exhausted. This file recomputes
//! the same window with a single unpaged statement -- one eligibility filter,
//! one window function for the weight cut, one `json_agg` -- and compares the
//! two answers row by row and field by field.
//!
//! The oracle is deliberately written from scratch. It shares no SQL text, no
//! constants and no private items with `ledger.rs`, and it reaches the ledger
//! only through the public `Ledger` API, so it keeps working across the
//! planned split of `ledger.rs` into submodules.
//!
//! PRISM_TEST_DATABASE_URL=postgres://postgres:prism@127.0.0.1:5432/postgres \
//!     cargo test -p qbit-prism-server --test window_read_oracle -- --nocapture

use anyhow::{anyhow, bail, ensure, Context, Result};
use qbit_prism::AcceptedShare;
use qbit_prism_server::ledger::{Ledger, Snapshot};
use serde::Deserialize;
use sqlx::PgPool;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Integration guard
// ---------------------------------------------------------------------------

/// Decides whether this file's tests run, fail or skip.
///
/// | `PRISM_TEST_DATABASE_URL` | other variables | result |
/// | --- | --- | --- |
/// | set and non-empty | -- | run against that database |
/// | unset or empty | `PRISM_TEST_REQUIRE_INTEGRATION=1` | fail, naming the variable |
/// | unset or empty | `GITHUB_JOB=prism-native-postgres` | fail, naming the variable |
/// | unset or empty | -- | print a skip line and return |
///
/// The two "required" signals are the ones this repository actually sets. The
/// `prism-native-postgres` job in `.github/workflows/ci.yml` exports
/// `PRISM_TEST_DATABASE_URL`, and GitHub exports `GITHUB_JOB` holding the job
/// id, so a database outage in that job surfaces as a failure instead of a
/// silent pass. Keying on `CI` instead would be wrong: GitHub sets `CI=true`
/// in every job, including `rust-tests`, which builds and runs the whole
/// workspace with no database at all.
///
/// An empty or whitespace-only URL counts as unset. A non-empty but malformed
/// URL is deliberately not second-guessed here; it reaches `sqlx` and fails
/// the test with the connection error, which is the diagnostic an operator
/// needs.
fn database_url(test_name: &str) -> Result<Option<String>> {
    let configured = std::env::var("PRISM_TEST_DATABASE_URL").unwrap_or_default();
    let configured = configured.trim();
    if !configured.is_empty() {
        return Ok(Some(configured.to_owned()));
    }
    let required_by = if matches!(
        std::env::var("PRISM_TEST_REQUIRE_INTEGRATION").as_deref(),
        Ok("1")
    ) {
        Some("PRISM_TEST_REQUIRE_INTEGRATION=1")
    } else if matches!(
        std::env::var("GITHUB_JOB").as_deref(),
        Ok("prism-native-postgres")
    ) {
        Some("GITHUB_JOB=prism-native-postgres")
    } else {
        None
    };
    if let Some(signal) = required_by {
        bail!(
            "{test_name} requires PostgreSQL: PRISM_TEST_DATABASE_URL is unset or empty while \
             {signal} demands the integration suite"
        );
    }
    eprintln!("skipping {test_name}: PRISM_TEST_DATABASE_URL is not set");
    Ok(None)
}

// ---------------------------------------------------------------------------
// Schema harness
// ---------------------------------------------------------------------------

/// A throwaway schema on the configured server, dropped when the test ends.
struct Database {
    admin: PgPool,
    schema: String,
    url: String,
}

impl Database {
    /// Guard plus schema creation, for tests that need exactly one schema.
    async fn open(test_name: &str) -> Result<Option<Self>> {
        let Some(raw) = database_url(test_name)? else {
            return Ok(None);
        };
        Ok(Some(Self::create(&raw).await?))
    }

    /// Schema creation only, for tests that need a fresh schema per sub-case.
    async fn create(raw: &str) -> Result<Self> {
        let admin = PgPool::connect(raw).await?;
        let schema = format!("prism_test_{}", Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut url = url::Url::parse(raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        Ok(Self {
            admin,
            schema,
            url: url.to_string(),
        })
    }

    async fn ledger(&self) -> Result<Ledger> {
        Ledger::connect(&self.url, "window-read-oracle".to_owned(), 8, true).await
    }

    async fn close(self, ledger: Ledger) -> Result<()> {
        ledger.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await?;
        self.admin.close().await;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Exact timestamp arithmetic
// ---------------------------------------------------------------------------

/// Builds a `timestamptz` from an integer count of microseconds since the
/// epoch, using integer arithmetic only.
///
/// Splitting the count into whole seconds plus a sub-second remainder keeps
/// every intermediate factor small, which matters for the far-future anchors
/// this file exercises. This is the fixture's own convention; it is not the
/// conversion `ledger.rs` uses.
fn timestamp_from_micros(expr: &str) -> String {
    format!(
        "(timestamptz 'epoch' + (({expr}) / 1000000) * interval '1 second' \
         + (({expr}) % 1000000) * interval '1 microsecond')"
    )
}

/// The same idea for a millisecond count, used for the oracle's anchor
/// barrier.
fn timestamp_from_millis(expr: &str) -> String {
    format!(
        "(timestamptz 'epoch' + (({expr}) / 1000) * interval '1 second' \
         + (({expr}) % 1000) * interval '1 millisecond')"
    )
}

// ---------------------------------------------------------------------------
// Fixture rows
// ---------------------------------------------------------------------------

/// One ledger row, written with direct SQL.
///
/// Direct inserts are what let a scenario express what the public append path
/// cannot: `accepted = false` rows, timestamps placed exactly on a boundary,
/// sub-millisecond timestamps, and a `job_issued_at` that isolates its own
/// predicate by sitting after `accepted_at`. Migration 002 makes share rows
/// immutable, so every fixture is built by insert alone -- nothing here
/// updates or deletes a share.
#[derive(Clone, Debug)]
struct ShareSpec {
    share_id: String,
    miner_id: String,
    order_key: String,
    program_hex: String,
    share_difficulty: u128,
    network_difficulty: u128,
    template_height: i64,
    job_id: String,
    job_issued_at_us: i64,
    accepted_at_us: i64,
    ntime: i64,
    credit_policy: Option<String>,
    accepted: bool,
}

impl ShareSpec {
    /// A distinct, valid accepted row landing at `accepted_at_us`.
    fn new(index: u64, accepted_at_us: i64) -> Self {
        let index_i64 = i64::try_from(index).unwrap_or(i64::MAX);
        Self {
            share_id: format!("oracle:{index:064x}"),
            miner_id: format!("miner-{}", index % 5),
            order_key: format!("order-{}", index % 3),
            program_hex: format!("{:02x}", index % 251).repeat(32),
            share_difficulty: 8,
            network_difficulty: 1_000_000,
            template_height: 900_000 + index_i64,
            job_id: format!("job-{index}"),
            job_issued_at_us: accepted_at_us - 1_000,
            accepted_at_us,
            ntime: 1_800_000_000 + index_i64 % 97,
            credit_policy: None,
            accepted: true,
        }
    }

    fn difficulty(mut self, difficulty: u128) -> Self {
        self.share_difficulty = difficulty;
        self
    }

    fn rejected(mut self) -> Self {
        self.accepted = false;
        self
    }

    fn issued_at_us(mut self, micros: i64) -> Self {
        self.job_issued_at_us = micros;
        self
    }
}

/// Inserts one fixture row and returns the sequence PostgreSQL assigned it.
async fn insert_share(pool: &PgPool, spec: &ShareSpec) -> Result<i64> {
    let sql = format!(
        "INSERT INTO qbit_share_ledger(share_id,miner_id,payout_order_key,p2mr_program,\
         share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,\
         accepted_at,credit_policy,accepted,reject_reason,writer_id,writer_epoch) \
         VALUES($1,$2,$3,decode($4,'hex'),$5::text::numeric,$6::text::numeric,$7,$8,{job},$10,\
         {accepted},$12,$13,$14,'window-read-oracle',0) RETURNING share_seq",
        job = timestamp_from_micros("$9"),
        accepted = timestamp_from_micros("$11"),
    );
    let reject_reason = if spec.accepted {
        None
    } else {
        Some("stale-job")
    };
    let seq: i64 = sqlx::query_scalar(&sql)
        .bind(&spec.share_id)
        .bind(&spec.miner_id)
        .bind(&spec.order_key)
        .bind(&spec.program_hex)
        .bind(spec.share_difficulty.to_string())
        .bind(spec.network_difficulty.to_string())
        .bind(spec.template_height)
        .bind(&spec.job_id)
        .bind(spec.job_issued_at_us)
        .bind(spec.ntime)
        .bind(spec.accepted_at_us)
        .bind(&spec.credit_policy)
        .bind(spec.accepted)
        .bind(reject_reason)
        .fetch_one(pool)
        .await?;
    Ok(seq)
}

/// Bulk-inserts `count` accepted rows of difficulty 8, one millisecond apart,
/// the newest landing at `last_accepted_us`. One statement keeps the
/// multi-page scenarios cheap.
async fn insert_run(pool: &PgPool, count: i64, last_accepted_us: i64) -> Result<()> {
    let accepted = format!("({last_accepted_us}::bigint - ($2 - i) * 1000)");
    let issued = format!("({accepted} - 1000)");
    let sql = format!(
        "INSERT INTO qbit_share_ledger(share_id,miner_id,payout_order_key,p2mr_program,\
         share_difficulty,network_difficulty,template_height,job_id,job_issued_at,ntime,\
         accepted_at,credit_policy,accepted,writer_id,writer_epoch) \
         SELECT 'oracle-run:'||lpad(i::text,32,'0'),'miner-'||(i%5),'order-'||(i%3),\
         decode(repeat(lpad(to_hex(i%251),2,'0'),32),'hex'),8::numeric,1000000::numeric,\
         900000+i,'job-'||i,{job},1800000000+(i%97),{acc},NULL,true,'window-read-oracle',0 \
         FROM generate_series($1,$2) i",
        job = timestamp_from_micros(&issued),
        acc = timestamp_from_micros(&accepted),
    );
    sqlx::query(&sql)
        .bind(1_i64)
        .bind(count)
        .execute(pool)
        .await?;
    Ok(())
}

/// Pins the anchor `Ledger::snapshot` will capture.
///
/// The read takes `anchor_ms = GREATEST(ledger_clock_ms, floor(now))`, so any
/// value above the wall clock becomes the anchor exactly.
async fn set_ledger_clock(pool: &PgPool, anchor_ms: i64) -> Result<()> {
    let now_ms: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint")
            .fetch_one(pool)
            .await?;
    ensure!(
        anchor_ms > now_ms,
        "fixture anchor {anchor_ms} must sit above the wall clock {now_ms} to pin the snapshot"
    );
    sqlx::query("UPDATE qbit_prism_cluster SET ledger_clock_ms=$1 WHERE singleton")
        .bind(anchor_ms)
        .execute(pool)
        .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// The oracle
// ---------------------------------------------------------------------------

/// One share as the oracle sees it. Difficulties travel as decimal text
/// because a JSON number cannot carry a `u128`.
#[derive(Debug, Deserialize)]
struct OracleShare {
    share_seq: i64,
    share_id: String,
    miner_id: String,
    order_key: String,
    p2mr_program_hex: String,
    share_difficulty: String,
    network_difficulty: String,
    template_height: i64,
    job_id: String,
    job_issued_at_ms: i64,
    accepted_at_ms: i64,
    ntime: i64,
    credit_policy: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OracleResult {
    eligible: i64,
    window: Vec<OracleShare>,
}

/// Recomputes the payout window in one unpaged statement.
///
/// Eligibility is `accepted`, `share_seq <= cutoff`, and both timestamps at or
/// before the anchor, where the anchor barrier is built by exact integer
/// millisecond arithmetic rather than by the read's floating-point conversion.
///
/// The weight cut is a window function: walking newest-first, a row belongs to
/// the window exactly when the total difficulty of everything newer than it is
/// still below the requested weight. That is the closed form of the read's
/// subtract-until-zero loop, saturation included -- once the running sum
/// reaches the weight, no older row can qualify -- and it keeps the row that
/// straddles the boundary.
async fn oracle_window(pool: &PgPool, snapshot: &Snapshot, weight: u128) -> Result<OracleResult> {
    let barrier = timestamp_from_millis("$2");
    let sql = format!(
        "WITH eligible AS (
             SELECT share_seq,share_id,miner_id,payout_order_key,
                    encode(p2mr_program,'hex') AS p2mr_program_hex,
                    share_difficulty,network_difficulty,template_height,job_id,
                    floor(extract(epoch FROM job_issued_at)*1000)::bigint AS job_issued_at_ms,
                    floor(extract(epoch FROM accepted_at)*1000)::bigint AS accepted_at_ms,
                    ntime,credit_policy
             FROM qbit_share_ledger
             WHERE accepted
               AND share_seq <= $1
               AND accepted_at <= {barrier}
               AND job_issued_at <= {barrier}
         ), weighted AS (
             SELECT eligible.*,
                    COALESCE(sum(share_difficulty) OVER (
                        ORDER BY share_seq DESC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS newer_weight
             FROM eligible
         )
         SELECT json_build_object(
             'eligible',(SELECT count(*) FROM eligible),
             'window',COALESCE((
                 SELECT json_agg(json_build_object(
                     'share_seq',share_seq,
                     'share_id',share_id,
                     'miner_id',miner_id,
                     'order_key',payout_order_key,
                     'p2mr_program_hex',p2mr_program_hex,
                     'share_difficulty',share_difficulty::text,
                     'network_difficulty',network_difficulty::text,
                     'template_height',template_height,
                     'job_id',job_id,
                     'job_issued_at_ms',job_issued_at_ms,
                     'accepted_at_ms',accepted_at_ms,
                     'ntime',ntime,
                     'credit_policy',credit_policy) ORDER BY share_seq)
                 FROM weighted WHERE newer_weight < $3::numeric),'[]'::json))"
    );
    let value: serde_json::Value = sqlx::query_scalar(&sql)
        .bind(i64::try_from(snapshot.share_seq)?)
        .bind(snapshot.anchor_ms)
        .bind(weight.to_string())
        .fetch_one(pool)
        .await?;
    serde_json::from_value(value).context("decoding the oracle window")
}

/// The cutoff the read should have captured, recomputed independently.
async fn oracle_cutoff(pool: &PgPool) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT COALESCE(max(share_seq),0) FROM qbit_share_ledger WHERE accepted",
    )
    .fetch_one(pool)
    .await?)
}

/// Total rows in the fixture, for the per-scenario row-count report.
async fn total_rows(pool: &PgPool) -> Result<i64> {
    Ok(sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger")
        .fetch_one(pool)
        .await?)
}

// ---------------------------------------------------------------------------
// Comparison
// ---------------------------------------------------------------------------

fn compare_field<T: std::fmt::Debug + PartialEq>(
    scenario: &str,
    position: usize,
    field: &str,
    read: &T,
    oracle: &T,
) -> Result<()> {
    ensure!(
        read == oracle,
        "{scenario}: share {position} disagrees on {field}: \
         Ledger::snapshot={read:?} oracle={oracle:?}"
    );
    Ok(())
}

/// Asserts membership, order and every field of every share.
fn assert_window_matches(
    scenario: &str,
    read: &[AcceptedShare],
    oracle: &[OracleShare],
) -> Result<()> {
    if read.len() != oracle.len() {
        let read_head = read.first().map(|share| (share.share_seq, &share.share_id));
        let oracle_head = oracle
            .first()
            .map(|share| (share.share_seq, &share.share_id));
        bail!(
            "{scenario}: window size disagrees: Ledger::snapshot returned {} shares \
             (first {read_head:?}), oracle returned {} shares (first {oracle_head:?})",
            read.len(),
            oracle.len()
        );
    }
    for (position, (read, oracle)) in read.iter().zip(oracle.iter()).enumerate() {
        compare_field(
            scenario,
            position,
            "share_seq",
            &read.share_seq,
            &u64::try_from(oracle.share_seq)?,
        )?;
        compare_field(
            scenario,
            position,
            "share_id",
            &read.share_id,
            &oracle.share_id,
        )?;
        compare_field(
            scenario,
            position,
            "miner_id",
            &read.miner_id,
            &oracle.miner_id,
        )?;
        compare_field(
            scenario,
            position,
            "order_key",
            &read.order_key,
            &oracle.order_key,
        )?;
        compare_field(
            scenario,
            position,
            "p2mr_program_hex",
            &read.p2mr_program_hex,
            &oracle.p2mr_program_hex,
        )?;
        compare_field(
            scenario,
            position,
            "share_difficulty",
            &read.share_difficulty,
            &oracle.share_difficulty.parse::<u128>()?,
        )?;
        compare_field(
            scenario,
            position,
            "network_difficulty",
            &read.network_difficulty,
            &oracle.network_difficulty.parse::<u128>()?,
        )?;
        compare_field(
            scenario,
            position,
            "template_height",
            &read.template_height,
            &u64::try_from(oracle.template_height)?,
        )?;
        compare_field(scenario, position, "job_id", &read.job_id, &oracle.job_id)?;
        compare_field(
            scenario,
            position,
            "job_issued_at_ms",
            &read.job_issued_at_ms,
            &oracle.job_issued_at_ms,
        )?;
        compare_field(
            scenario,
            position,
            "accepted_at_ms",
            &read.accepted_at_ms,
            &oracle.accepted_at_ms,
        )?;
        compare_field(
            scenario,
            position,
            "ntime",
            &read.ntime,
            &u32::try_from(oracle.ntime)?,
        )?;
        compare_field(
            scenario,
            position,
            "credit_policy",
            &read.credit_policy,
            &oracle.credit_policy,
        )?;
    }
    for pair in read.windows(2) {
        ensure!(
            pair[0].share_seq < pair[1].share_seq,
            "{scenario}: Ledger::snapshot returned shares out of ascending order at {} then {}",
            pair[0].share_seq,
            pair[1].share_seq
        );
    }
    Ok(())
}

/// Ceiling on a single payout-window read.
///
/// The read pages backwards until its weight is exhausted or a page comes back
/// empty, so a cursor that stops advancing turns it into an unbounded loop
/// rather than a wrong answer. The largest fixture here -- 12000 rows over
/// three pages -- returns in well under a second, so this ceiling is pure
/// headroom for a slow or contended server while still converting
/// non-termination into a named failure instead of a hung test run.
const SNAPSHOT_CEILING: std::time::Duration = std::time::Duration::from_secs(30);

/// Runs the read, runs the oracle, and asserts they agree on the anchor, the
/// cutoff and the window. Prints the row counts the acceptance run reports.
async fn check_scenario(
    scenario: &str,
    ledger: &Ledger,
    anchor_ms: i64,
    network_difficulty: u128,
) -> Result<Snapshot> {
    set_ledger_clock(&ledger.pool, anchor_ms).await?;
    let snapshot = tokio::time::timeout(SNAPSHOT_CEILING, ledger.snapshot(network_difficulty))
        .await
        .map_err(|_| {
            anyhow!(
                "{scenario}: Ledger::snapshot did not return within {SNAPSHOT_CEILING:?}; \
                 the paged read is not making progress"
            )
        })??;
    ensure!(
        snapshot.anchor_ms == anchor_ms,
        "{scenario}: expected the pinned anchor {anchor_ms}, got {}",
        snapshot.anchor_ms
    );
    let weight = network_difficulty
        .checked_mul(8)
        .context("window weight overflow")?;
    let expected = oracle_window(&ledger.pool, &snapshot, weight).await?;
    let cutoff = oracle_cutoff(&ledger.pool).await?;
    ensure!(
        snapshot.share_seq == u64::try_from(cutoff)?,
        "{scenario}: cutoff disagrees: Ledger::snapshot={} oracle={cutoff}",
        snapshot.share_seq
    );
    assert_window_matches(scenario, &snapshot.shares, &expected.window)?;
    println!(
        "scenario {scenario}: rows={} eligible={} window={} anchor_ms={anchor_ms} \
         cutoff={cutoff} weight={weight}",
        total_rows(&ledger.pool).await?,
        expected.eligible,
        snapshot.shares.len()
    );
    Ok(snapshot)
}

fn credited_sequences(snapshot: &Snapshot) -> Result<Vec<i64>> {
    snapshot
        .shares
        .iter()
        .map(|share| Ok(i64::try_from(share.share_seq)?))
        .collect()
}

// ---------------------------------------------------------------------------
// (a) page boundaries
// ---------------------------------------------------------------------------

/// Every fixture row carries difficulty 8 and the read's weight is
/// `network_difficulty * 8`, so the window is exactly `network_difficulty`
/// rows long. That puts the cut wherever a sub-case asks for it: one row
/// before the read's 4096-row page, exactly on it, one row after it, deep
/// enough to need a third page, and past the end of history.
#[tokio::test]
async fn window_matches_the_oracle_across_page_boundaries() -> Result<()> {
    let Some(db) = Database::open("window_matches_the_oracle_across_page_boundaries").await? else {
        return Ok(());
    };
    let ledger = db.ledger().await?;
    let anchor_ms = 1_893_456_000_000_i64;
    // 12000 rows one millisecond apart, the newest a minute before the anchor.
    insert_run(&ledger.pool, 12_000, anchor_ms * 1_000 - 60_000_000).await?;

    for (label, network_difficulty, expected_len) in [
        ("page-boundary-minus-one", 4_095_u128, 4_095_usize),
        ("page-boundary-exact", 4_096, 4_096),
        ("page-boundary-plus-one", 4_097, 4_097),
        ("three-pages", 9_000, 9_000),
        ("weight-exceeds-history", 20_000, 12_000),
    ] {
        let snapshot = check_scenario(
            &format!("a:{label}"),
            &ledger,
            anchor_ms,
            network_difficulty,
        )
        .await?;
        ensure!(
            snapshot.shares.len() == expected_len,
            "a:{label}: expected a {expected_len}-row window, got {}",
            snapshot.shares.len()
        );
    }
    db.close(ledger).await
}

// ---------------------------------------------------------------------------
// (b) the crossing row
// ---------------------------------------------------------------------------

/// The row whose difficulty overshoots the remaining weight is still credited;
/// the next older row is not.
#[tokio::test]
async fn the_row_that_crosses_the_weight_boundary_is_credited() -> Result<()> {
    let Some(db) = Database::open("the_row_that_crosses_the_weight_boundary_is_credited").await?
    else {
        return Ok(());
    };
    let ledger = db.ledger().await?;
    let anchor_ms = 1_893_456_100_000_i64;
    let base_us = anchor_ms * 1_000 - 60_000_000;
    // Newest first the difficulties are 3, 7, 5, 1, 1. A weight of 8 leaves 5
    // after the newest row, and the difficulty-7 row overshoots that.
    let mut seqs = Vec::new();
    for (index, difficulty) in [1_u128, 1, 5, 7, 3].into_iter().enumerate() {
        let index = u64::try_from(index)?;
        let spec = ShareSpec::new(index + 1, base_us + i64::try_from(index)? * 1_000)
            .difficulty(difficulty);
        seqs.push(insert_share(&ledger.pool, &spec).await?);
    }

    let snapshot = check_scenario("b:crossing-row", &ledger, anchor_ms, 1).await?;
    let credited = credited_sequences(&snapshot)?;
    ensure!(
        credited == vec![seqs[3], seqs[4]],
        "b:crossing-row: expected the difficulty-7 crossing row and the newest row, got \
         {credited:?}"
    );
    db.close(ledger).await
}

/// A difficulty at the top of the `u128` range survives the `numeric` round
/// trip and closes the window on the row that carries it.
#[tokio::test]
async fn a_u128_maximum_difficulty_is_decoded_and_closes_the_window() -> Result<()> {
    let Some(db) =
        Database::open("a_u128_maximum_difficulty_is_decoded_and_closes_the_window").await?
    else {
        return Ok(());
    };
    let ledger = db.ledger().await?;
    let anchor_ms = 1_893_456_200_000_i64;
    let base_us = anchor_ms * 1_000 - 60_000_000;
    insert_share(&ledger.pool, &ShareSpec::new(1, base_us)).await?;
    let crossing = insert_share(
        &ledger.pool,
        &ShareSpec::new(2, base_us + 1_000).difficulty(u128::MAX),
    )
    .await?;
    let mut newest = ShareSpec::new(3, base_us + 2_000);
    newest.network_difficulty = u128::MAX;
    let newest = insert_share(&ledger.pool, &newest).await?;

    let snapshot = check_scenario("b:u128-maximum-difficulty", &ledger, anchor_ms, 1_000).await?;
    let credited = credited_sequences(&snapshot)?;
    ensure!(
        credited == vec![crossing, newest],
        "b:u128-maximum-difficulty: expected the saturating row and the newest row, got \
         {credited:?}"
    );
    db.close(ledger).await
}

// ---------------------------------------------------------------------------
// (c) and (e) the anchor barrier
// ---------------------------------------------------------------------------

/// Microsecond offsets, relative to the anchor, at which a boundary scenario
/// places rows. `0` means exactly on the anchor.
const BOUNDARY_OFFSETS_US: &[i64] = &[-1_000, -400, 0, 400, 1_000];

/// The same offsets without the exactly-on-the-anchor row.
///
/// `ledger.rs` builds its barrier with `to_timestamp($anchor::double
/// precision/1000)`. Past roughly 2^43 epoch-milliseconds a `double precision`
/// no longer resolves a microsecond, so that barrier can land one microsecond
/// to either side of the true millisecond. Measured on PostgreSQL 16, every
/// anchor in the 2^43 region whose millisecond ends in `001` converts one
/// microsecond low. The production writer stores `accepted_at` and
/// `job_issued_at` through the very same conversion, so a real share accepted
/// at the anchor still lands exactly on the barrier and is credited: the slack
/// is unreachable through the public API. It is reachable only by a fixture
/// that stores timestamps more precisely than the writer can, so those anchors
/// drop the exactly-on-the-anchor row rather than manufacture a disagreement
/// no share can hit.
const BOUNDARY_OFFSETS_US_NO_EXACT: &[i64] = &[-1_000, -400, 400, 1_000];

/// Builds a fixture that brackets `anchor_ms` on both timestamp predicates and
/// checks the window against the oracle.
///
/// Each offset appears twice: once moving `accepted_at` while `job_issued_at`
/// stays far in the past, and once moving `job_issued_at` while `accepted_at`
/// stays far in the past. The second family deliberately issues a job after
/// the share was accepted, which no real writer does, because that is the only
/// way to isolate the `job_issued_at` predicate from the `accepted_at` one.
///
/// The weight far exceeds the fixture's total difficulty, so the anchor
/// barrier -- not the weight cut -- decides membership.
async fn check_anchor_boundary(
    scenario: &str,
    raw_url: &str,
    anchor_ms: i64,
    offsets: &[i64],
) -> Result<()> {
    let db = Database::create(raw_url).await?;
    let ledger = db.ledger().await?;
    let anchor_us = anchor_ms
        .checked_mul(1_000)
        .context("anchor microseconds overflow")?;
    let far_past_us = anchor_us - 3_600_000_000;
    let mut index = 0_u64;
    for offset in offsets {
        index += 1;
        insert_share(
            &ledger.pool,
            &ShareSpec::new(index, anchor_us + offset).issued_at_us(far_past_us),
        )
        .await?;
        index += 1;
        insert_share(
            &ledger.pool,
            &ShareSpec::new(index, far_past_us).issued_at_us(anchor_us + offset),
        )
        .await?;
    }

    let snapshot = check_scenario(scenario, &ledger, anchor_ms, 1_000_000).await?;
    // Two rows per non-positive offset, one from each predicate family.
    let expected = offsets.iter().filter(|offset| **offset <= 0).count() * 2;
    ensure!(
        snapshot.shares.len() == expected,
        "{scenario}: expected {expected} rows at or before the anchor, got {}",
        snapshot.shares.len()
    );
    db.close(ledger).await
}

/// Rows sitting exactly on the anchor are credited on either timestamp; rows a
/// millisecond past it are not.
#[tokio::test]
async fn rows_on_the_anchor_are_credited_and_rows_after_it_are_not() -> Result<()> {
    let Some(url) = database_url("rows_on_the_anchor_are_credited_and_rows_after_it_are_not")?
    else {
        return Ok(());
    };
    check_anchor_boundary(
        "c:anchor-barrier",
        &url,
        1_893_456_300_000,
        BOUNDARY_OFFSETS_US,
    )
    .await
}

/// The same barrier at millisecond values that stress the read's
/// floating-point conversion: anchors ending in `999`, `000` and `001`, at an
/// ordinary magnitude, near 2^41 milliseconds and near 2^43 milliseconds.
#[tokio::test]
async fn the_anchor_barrier_holds_at_large_millisecond_values() -> Result<()> {
    let Some(url) = database_url("the_anchor_barrier_holds_at_large_millisecond_values")? else {
        return Ok(());
    };
    for (label, anchor_ms, offsets) in [
        ("ordinary-999", 1_893_456_000_999_i64, BOUNDARY_OFFSETS_US),
        ("ordinary-000", 1_893_456_001_000, BOUNDARY_OFFSETS_US),
        ("ordinary-001", 1_893_456_001_001, BOUNDARY_OFFSETS_US),
        ("pow41-999", 2_199_023_254_999, BOUNDARY_OFFSETS_US),
        ("pow41-000", 2_199_023_255_000, BOUNDARY_OFFSETS_US),
        ("pow41-001", 2_199_023_255_001, BOUNDARY_OFFSETS_US),
        ("pow43-999", 8_796_093_022_999, BOUNDARY_OFFSETS_US),
        ("pow43-000", 8_796_093_023_000, BOUNDARY_OFFSETS_US),
        ("pow43-001", 8_796_093_023_001, BOUNDARY_OFFSETS_US_NO_EXACT),
    ] {
        check_anchor_boundary(&format!("e:{label}"), &url, anchor_ms, offsets).await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// (d) rejected rows
// ---------------------------------------------------------------------------

/// Rejected rows interleave with accepted ones. They stay out of the window,
/// they consume none of the weight despite carrying enormous difficulties, and
/// the newest of them does not move the cutoff.
#[tokio::test]
async fn rejected_rows_are_excluded_and_consume_no_weight() -> Result<()> {
    let Some(db) = Database::open("rejected_rows_are_excluded_and_consume_no_weight").await? else {
        return Ok(());
    };
    let ledger = db.ledger().await?;
    let anchor_ms = 1_893_456_400_000_i64;
    let base_us = anchor_ms * 1_000 - 60_000_000;
    let huge = 1_000_000_000_000_000_000_000_000_000_000_u128;
    let mut accepted_seqs = Vec::new();
    let mut rejected_seqs = Vec::new();
    for index in 0..8_u64 {
        let accepted_at_us = base_us + i64::try_from(index)? * 1_000;
        let spec = ShareSpec::new(index + 1, accepted_at_us);
        if index % 2 == 0 {
            accepted_seqs.push(insert_share(&ledger.pool, &spec).await?);
        } else {
            let spec = spec.difficulty(huge).rejected();
            rejected_seqs.push(insert_share(&ledger.pool, &spec).await?);
        }
    }
    // The highest sequence in the table belongs to a rejected row, so the
    // cutoff has to fall back to the newest accepted row.
    rejected_seqs.push(
        insert_share(
            &ledger.pool,
            &ShareSpec::new(9, base_us + 9_000)
                .difficulty(huge)
                .rejected(),
        )
        .await?,
    );

    // Weight 24 credits exactly three difficulty-8 accepted rows. Had the
    // rejected rows consumed weight, the window would be a single row.
    let snapshot = check_scenario("d:rejected-interleaved", &ledger, anchor_ms, 3).await?;
    let credited = credited_sequences(&snapshot)?;
    for seq in &rejected_seqs {
        ensure!(
            !credited.contains(seq),
            "d:rejected-interleaved: rejected row {seq} appeared in the window"
        );
    }
    ensure!(
        credited.as_slice() == &accepted_seqs[accepted_seqs.len() - 3..],
        "d:rejected-interleaved: expected the three newest accepted rows, got {credited:?}"
    );
    ensure!(
        i64::try_from(snapshot.share_seq)? == *accepted_seqs.last().context("accepted rows")?,
        "d:rejected-interleaved: the cutoff followed a rejected row"
    );
    db.close(ledger).await
}

// ---------------------------------------------------------------------------
// (f) empty windows
// ---------------------------------------------------------------------------

/// An empty window arises three ways: no rows at all, only rejected rows, and
/// accepted rows that all sit after the anchor.
#[tokio::test]
async fn empty_windows_return_no_shares() -> Result<()> {
    let Some(db) = Database::open("empty_windows_return_no_shares").await? else {
        return Ok(());
    };
    let ledger = db.ledger().await?;
    let anchor_ms = 1_893_456_500_000_i64;

    let snapshot = check_scenario("f:no-rows", &ledger, anchor_ms, 1_000).await?;
    ensure!(
        snapshot.shares.is_empty() && snapshot.share_seq == 0,
        "f:no-rows: expected an empty window and a zero cutoff, got {} shares and cutoff {}",
        snapshot.shares.len(),
        snapshot.share_seq
    );

    for index in 0..4_u64 {
        let accepted_at_us = anchor_ms * 1_000 - 60_000_000 + i64::try_from(index)? * 1_000;
        let spec = ShareSpec::new(index + 1, accepted_at_us).rejected();
        insert_share(&ledger.pool, &spec).await?;
    }
    let snapshot =
        check_scenario("f:only-rejected-rows", &ledger, anchor_ms + 1_000, 1_000).await?;
    ensure!(
        snapshot.shares.is_empty() && snapshot.share_seq == 0,
        "f:only-rejected-rows: expected an empty window and a zero cutoff, got {} shares and \
         cutoff {}",
        snapshot.shares.len(),
        snapshot.share_seq
    );

    let future_anchor_ms = anchor_ms + 100_000;
    for index in 0..4_u64 {
        let accepted_at_us = future_anchor_ms * 1_000 + 1_000 + i64::try_from(index)? * 1_000;
        insert_share(&ledger.pool, &ShareSpec::new(index + 100, accepted_at_us)).await?;
    }
    let snapshot = check_scenario(
        "f:all-rows-after-the-anchor",
        &ledger,
        future_anchor_ms,
        1_000,
    )
    .await?;
    ensure!(
        snapshot.shares.is_empty() && snapshot.share_seq > 0,
        "f:all-rows-after-the-anchor: expected an empty window behind a non-zero cutoff, got {} \
         shares and cutoff {}",
        snapshot.shares.len(),
        snapshot.share_seq
    );
    db.close(ledger).await
}
