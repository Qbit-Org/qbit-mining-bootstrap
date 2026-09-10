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

/// A share for the public append path. `accepted_at_ms` is left at zero
/// because `Ledger::append` stamps it from the ledger clock.
fn writer_share(index: u64, job_issued_at_ms: i64) -> AcceptedShare {
    AcceptedShare {
        share_seq: 0,
        share_id: format!("writer:{index:064x}"),
        miner_id: format!("miner-{}", index % 5),
        order_key: format!("order-{}", index % 3),
        p2mr_program_hex: format!("{:02x}", index % 251).repeat(32),
        share_difficulty: 8,
        network_difficulty: 1_000_000,
        template_height: 900_000 + index,
        job_id: format!("job-{index}"),
        job_issued_at_ms,
        accepted_at_ms: 0,
        ntime: 1_800_000_000 + u32::try_from(index % 97).unwrap_or(0),
        credit_policy: None,
    }
}

/// Places two shares on the anchor through the public append path, and
/// returns their sequences as `(on the anchor, one millisecond after it)`.
///
/// `Ledger::append` stamps `accepted_at` with
/// `GREATEST(ledger_clock_ms, floor(now))` -- no `+1`, unlike the read's
/// anchor bump -- and stores both timestamps through the writer's own
/// conversion, requiring only that `job_issued_at_ms <= accepted_at_ms`. So
/// pinning the ledger clock chooses the acceptance millisecond exactly, and
/// the public API alone can put a real share on either side of an anchor: one
/// accepted at the anchor on both timestamps, and one accepted a millisecond
/// past it. The credited window must contain the first and not the second.
async fn append_anchor_pair(ledger: &Ledger, anchor_ms: i64, index: u64) -> Result<(u64, u64)> {
    set_ledger_clock(&ledger.pool, anchor_ms + 1).await?;
    let after = ledger
        .append(writer_share(index, anchor_ms), None)
        .await
        .context("appending the share accepted after the anchor")?
        .share;
    set_ledger_clock(&ledger.pool, anchor_ms).await?;
    let on = ledger
        .append(writer_share(index + 1, anchor_ms), None)
        .await
        .context("appending the share accepted on the anchor")?
        .share;
    ensure!(
        on.accepted_at_ms == anchor_ms && on.job_issued_at_ms == anchor_ms,
        "the append path was expected to accept a share exactly on anchor {anchor_ms}, got \
         accepted_at_ms={} job_issued_at_ms={}",
        on.accepted_at_ms,
        on.job_issued_at_ms
    );
    ensure!(
        after.accepted_at_ms == anchor_ms + 1,
        "the append path was expected to accept a share one millisecond after anchor \
         {anchor_ms}, got accepted_at_ms={}",
        after.accepted_at_ms
    );
    Ok((on.share_seq, after.share_seq))
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

/// Pins the anchor `Ledger::snapshot` will capture, and the millisecond
/// `Ledger::append` will stamp on the next share it accepts.
///
/// Both take `GREATEST(ledger_clock_ms, floor(now))`, so any value above the
/// wall clock becomes that millisecond exactly.
async fn set_ledger_clock(pool: &PgPool, anchor_ms: i64) -> Result<()> {
    let now_ms = wall_clock_ms(pool).await?;
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

async fn wall_clock_ms(pool: &PgPool) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint")
            .fetch_one(pool)
            .await?,
    )
}

/// Margin between the wall clock and a derived anchor. A day is far more than
/// a test run needs, and it keeps a derived anchor legible in a failure
/// message as "tomorrow" rather than "a few seconds from now".
const ANCHOR_MARGIN_MS: i64 = 86_400_000;

/// Derived anchors land on a round millisecond boundary so a scenario can add
/// its own suffix -- `999`, `1_000`, `1_001` -- without disturbing the
/// magnitude.
const ANCHOR_GRANULARITY_MS: i64 = 1_000_000;

/// An anchor at an ordinary magnitude, derived from the database clock.
///
/// A pinned anchor has to sit above the wall clock, so a hard-coded ordinary
/// anchor is a dated fuse: the suite would start failing on the day the clock
/// passed it, for no reason in the code. Deriving it at run time removes the
/// date from the fixture while keeping the millisecond suffix, which is the
/// part the boundary scenarios actually exercise.
async fn derived_anchor(pool: &PgPool, suffix_ms: i64) -> Result<i64> {
    let now_ms = wall_clock_ms(pool).await?;
    let floor = now_ms
        .checked_add(ANCHOR_MARGIN_MS)
        .context("derived anchor margin overflow")?;
    // Round strictly up, so the anchor clears the margin even when the wall
    // clock already sits on a granularity boundary.
    let base = floor
        .div_euclid(ANCHOR_GRANULARITY_MS)
        .checked_add(1)
        .and_then(|units| units.checked_mul(ANCHOR_GRANULARITY_MS))
        .context("derived anchor rounding overflow")?;
    base.checked_add(suffix_ms)
        .context("derived anchor suffix overflow")
}

/// How a scenario chooses its anchor.
///
/// No fixed anchor may ever fall below the wall clock, so fixed anchors are
/// used only for the far-future magnitude cases; everything at an ordinary
/// magnitude is derived from the database clock at run time.
#[derive(Clone, Copy, Debug)]
enum Anchor {
    Derived(i64),
    Fixed(i64),
}

impl Anchor {
    async fn resolve(self, pool: &PgPool) -> Result<i64> {
        match self {
            Anchor::Derived(suffix_ms) => derived_anchor(pool, suffix_ms).await,
            Anchor::Fixed(anchor_ms) => Ok(anchor_ms),
        }
    }
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

/// Pins the anchor, runs the read under the ceiling, and confirms the read
/// captured the anchor that was pinned.
async fn pinned_snapshot(
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
    Ok(snapshot)
}

/// Runs the read, runs the oracle, and asserts they agree on the anchor, the
/// cutoff and the window. Prints the row counts the acceptance run reports.
async fn check_scenario(
    scenario: &str,
    ledger: &Ledger,
    anchor_ms: i64,
    network_difficulty: u128,
) -> Result<Snapshot> {
    let snapshot = pinned_snapshot(scenario, ledger, anchor_ms, network_difficulty).await?;
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
    let anchor_ms = derived_anchor(&ledger.pool, 0).await?;
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
    let anchor_ms = derived_anchor(&ledger.pool, 0).await?;
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
    let anchor_ms = derived_anchor(&ledger.pool, 0).await?;
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

/// Asserts the anchor's millisecond survives the read's own
/// `to_timestamp(double precision/1000)` conversion unchanged.
///
/// This is a precondition on the fixture, not part of the oracle. The oracle
/// compares stored timestamps against an exact integer-millisecond barrier
/// while the read compares them against the converted one. Where the
/// conversion is exact those are the same instant and every row kind agrees.
/// Where it is not, the two barriers sit a microsecond apart, and a row placed
/// on the anchor falls on one side or the other purely according to how it was
/// written: a fixture storing the exact millisecond and a share written through
/// `Ledger::append` land on opposite sides. Past roughly 2^43 epoch-
/// milliseconds -- the year 2248 -- a `double precision` no longer resolves a
/// microsecond, and that is where the inexact anchors live. Measured on
/// PostgreSQL 16 in that region, every millisecond ending in `999` converts one
/// microsecond high, every one ending in `001` converts one microsecond low,
/// and `000` is exact; at 2^42 and at every realistic epoch millisecond all
/// three suffixes are exact. These scenarios therefore stay below 2^43, and
/// this guard stops the fixture silently picking an anchor that does not.
/// Millisecond behaviour past 2^43 is asserted separately, in
/// `writer_rows_follow_integer_milliseconds_past_2_43`, in the integer
/// milliseconds `AcceptedShare` actually carries.
async fn ensure_anchor_converts_exactly(
    pool: &PgPool,
    scenario: &str,
    anchor_ms: i64,
) -> Result<()> {
    ensure!(
        converts_exactly(pool, anchor_ms).await?,
        "{scenario}: anchor {anchor_ms} does not survive the read's double-precision \
         conversion, so the exact-time oracle and the read would disagree on any row placed \
         exactly on it; pick an anchor below 2^43 epoch-milliseconds"
    );
    Ok(())
}

/// Whether `ms` is representable exactly by the writer's and the read's shared
/// `to_timestamp(double precision/1000)` conversion.
///
/// Classification only: it decides which assertion a millisecond is held to,
/// never what the payout window should contain.
async fn converts_exactly(pool: &PgPool, ms: i64) -> Result<bool> {
    compare_conversion(pool, ms, "=").await
}

/// Whether the conversion of `ms` lands *below* the exact millisecond.
///
/// This is the direction that matters for the read-back. `share_from_row`
/// floors, so a conversion one microsecond high still floors to the same
/// millisecond and round-trips cleanly; only a conversion below the
/// millisecond loses one.
async fn converts_below(pool: &PgPool, ms: i64) -> Result<bool> {
    compare_conversion(pool, ms, "<").await
}

async fn compare_conversion(pool: &PgPool, ms: i64, operator: &str) -> Result<bool> {
    let sql = format!(
        "SELECT to_timestamp($1::double precision/1000) {operator} {}",
        timestamp_from_millis("$1")
    );
    Ok(sqlx::query_scalar(&sql).bind(ms).fetch_one(pool).await?)
}

/// Builds a fixture that brackets `anchor_ms` on both timestamp predicates and
/// checks the window against the oracle.
///
/// Each offset appears twice: once moving `accepted_at` while `job_issued_at`
/// stays far in the past, and once moving `job_issued_at` while `accepted_at`
/// stays far in the past. The second family deliberately issues a job after
/// the share was accepted, which no real writer does, because that is the only
/// way to isolate the `job_issued_at` predicate from the `accepted_at` one.
///
/// On top of those, one pair goes in through the public append path, so the
/// barrier is asserted against a share the writer could really have produced
/// and not only against fixtures.
///
/// The weight far exceeds the fixture's total difficulty, so the anchor
/// barrier -- not the weight cut -- decides membership.
async fn check_anchor_boundary(
    scenario: &str,
    raw_url: &str,
    anchor: Anchor,
    offsets: &[i64],
) -> Result<()> {
    let db = Database::create(raw_url).await?;
    let ledger = db.ledger().await?;
    let anchor_ms = anchor.resolve(&ledger.pool).await?;
    ensure_anchor_converts_exactly(&ledger.pool, scenario, anchor_ms).await?;
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
    let (writer_on_anchor, writer_after_anchor) =
        append_anchor_pair(&ledger, anchor_ms, WRITER_INDEX_BASE).await?;

    let snapshot = check_scenario(scenario, &ledger, anchor_ms, 1_000_000).await?;
    // Two rows per non-positive offset, one from each predicate family, plus
    // the appended share accepted on the anchor.
    let expected = offsets.iter().filter(|offset| **offset <= 0).count() * 2 + 1;
    ensure!(
        snapshot.shares.len() == expected,
        "{scenario}: expected {expected} rows at or before the anchor, got {}",
        snapshot.shares.len()
    );
    let credited = credited_sequences(&snapshot)?;
    ensure!(
        credited.contains(&i64::try_from(writer_on_anchor)?),
        "{scenario}: writer-on-anchor share {writer_on_anchor}, accepted by the public append \
         path at exactly the anchor, was not credited; window {credited:?}"
    );
    ensure!(
        !credited.contains(&i64::try_from(writer_after_anchor)?),
        "{scenario}: writer-after-anchor share {writer_after_anchor}, accepted one millisecond \
         past the anchor, was credited; window {credited:?}"
    );
    db.close(ledger).await
}

/// Where the appended share indices start, clear of the direct-insert rows.
const WRITER_INDEX_BASE: u64 = 1_000;

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
        Anchor::Derived(0),
        BOUNDARY_OFFSETS_US,
    )
    .await
}

/// The same barrier at millisecond values that stress the read's
/// floating-point conversion: anchors ending in `999`, `000` and `001`, at an
/// ordinary magnitude and near 2^42 milliseconds (the year 2109).
///
/// The large magnitude is fixed because that is the whole point of the case,
/// and it sits centuries ahead of any wall clock this suite will meet. The
/// ordinary magnitude is derived, so it can never become a dated fuse. Every
/// anchor here converts exactly, which `ensure_anchor_converts_exactly`
/// enforces, so all three row kinds -- direct inserts on the anchor and 400
/// microseconds either side of it, and the appended pair -- must agree with the
/// oracle. Past 2^43 the conversion stops being exact, and that region is
/// covered by `writer_rows_follow_integer_milliseconds_past_2_43` instead.
#[tokio::test]
async fn the_anchor_barrier_holds_at_large_millisecond_values() -> Result<()> {
    let Some(url) = database_url("the_anchor_barrier_holds_at_large_millisecond_values")? else {
        return Ok(());
    };
    for (label, anchor) in [
        ("ordinary-999", Anchor::Derived(999)),
        ("ordinary-000", Anchor::Derived(1_000)),
        ("ordinary-001", Anchor::Derived(1_001)),
        ("pow42-999", Anchor::Fixed(4_398_046_510_999)),
        ("pow42-000", Anchor::Fixed(4_398_046_511_000)),
        ("pow42-001", Anchor::Fixed(4_398_046_511_001)),
    ] {
        check_anchor_boundary(&format!("e:{label}"), &url, anchor, BOUNDARY_OFFSETS_US).await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// (g) writer-only milliseconds past 2^43
// ---------------------------------------------------------------------------

/// Past roughly 2^43 epoch-milliseconds the read's
/// `to_timestamp(double precision/1000)` no longer resolves a microsecond, so
/// a stored timestamp can sit a microsecond either side of its millisecond and
/// the exact-time oracle stops being the right yardstick. The contract
/// `AcceptedShare` carries is integer milliseconds, so that is what this test
/// asserts, and it does so without the oracle and without a single direct
/// insert: every row goes in through `Ledger::append`, exactly as the
/// production writer writes it.
///
/// A row is credited if and only if the `accepted_at_ms` that `append`
/// returned is at or before the anchor. That holds at all three anchors: the
/// barrier and the stored value are converted the same way, so they shift
/// together and the credited set stays right.
///
/// The read-back of that millisecond does not hold. `share_from_row` converts
/// `accepted_at` with chrono's `timestamp_millis()`, which floors, so a value
/// stored a microsecond low reads back a whole millisecond early, and the
/// millisecond the audit bundle carries is off by one. This is a real defect,
/// reported separately; it is confined to dates past about the year 2248 and
/// is unreachable at a realistic epoch millisecond.
///
/// The defect follows the individual millisecond, not the anchor's suffix: the
/// rows a millisecond either side of an anchor have their own suffixes and
/// their own conversions. Measured on PostgreSQL 16 over two seconds of
/// milliseconds around 2^43, 480000 of 2000001 -- 24% -- read back one
/// millisecond early and the rest are exact; over a comparable span at 2^42,
/// none are. Of the nine milliseconds this test writes, 8796093022998 and
/// 8796093023001 read back early and the other seven are exact.
///
/// Only a conversion that lands *below* its millisecond loses one: flooring a
/// value a microsecond high returns the same millisecond, so those round-trip
/// cleanly. So rather than skip the read-back where it is known to be wrong,
/// this test pins it from both sides: a millisecond whose conversion is at or
/// above it must round-trip unchanged, and one whose conversion is below it
/// must read back early by exactly one millisecond and no more. Nothing
/// known-wrong is asserted as right, nothing is hidden, and a regression in
/// either direction fails.
#[tokio::test]
async fn writer_rows_follow_integer_milliseconds_past_2_43() -> Result<()> {
    let Some(url) = database_url("writer_rows_follow_integer_milliseconds_past_2_43")? else {
        return Ok(());
    };
    for (label, anchor_ms) in [
        ("pow43-999", 8_796_093_022_999_i64),
        ("pow43-000", 8_796_093_023_000),
        ("pow43-001", 8_796_093_023_001),
    ] {
        let scenario = format!("g:{label}");
        let db = Database::create(&url).await?;
        let ledger = db.ledger().await?;

        // One share a millisecond before the anchor, one on it, one after.
        let mut appended = Vec::new();
        for (position, offset) in [("before", -1_i64), ("on", 0), ("after", 1)] {
            let accepted_ms = anchor_ms
                .checked_add(offset)
                .context("writer anchor offset overflow")?;
            set_ledger_clock(&ledger.pool, accepted_ms).await?;
            let index = WRITER_INDEX_BASE + u64::try_from(appended.len())?;
            let share = ledger
                .append(writer_share(index, accepted_ms), None)
                .await
                .with_context(|| format!("{scenario}: appending the {position} share"))?
                .share;
            ensure!(
                share.accepted_at_ms == accepted_ms && share.job_issued_at_ms == accepted_ms,
                "{scenario}: expected the {position} share at {accepted_ms}, got \
                 accepted_at_ms={} job_issued_at_ms={}",
                share.accepted_at_ms,
                share.job_issued_at_ms
            );
            appended.push((position, share));
        }

        let snapshot = pinned_snapshot(&scenario, &ledger, anchor_ms, 1_000_000).await?;
        let mut read_back_early = Vec::new();
        for (position, share) in &appended {
            // The contract: credited exactly when the accepted millisecond is
            // at or before the anchor millisecond.
            let expected = share.accepted_at_ms <= anchor_ms;
            let credited = snapshot
                .shares
                .iter()
                .find(|candidate| candidate.share_id == share.share_id);
            ensure!(
                credited.is_some() == expected,
                "{scenario}: the {position} share, accepted at {} against anchor {anchor_ms}, \
                 should {} been credited",
                share.accepted_at_ms,
                if expected { "have" } else { "not have" }
            );
            let Some(credited) = credited else {
                continue;
            };
            let drift = credited.accepted_at_ms - share.accepted_at_ms;
            ensure!(
                credited.job_issued_at_ms - share.job_issued_at_ms == drift,
                "{scenario}: the {position} share's two timestamps drifted apart on read-back: \
                 appended accepted_at_ms={} job_issued_at_ms={}, read back accepted_at_ms={} \
                 job_issued_at_ms={}",
                share.accepted_at_ms,
                share.job_issued_at_ms,
                credited.accepted_at_ms,
                credited.job_issued_at_ms
            );
            if converts_below(&ledger.pool, share.accepted_at_ms).await? {
                ensure!(
                    drift == -1,
                    "{scenario}: millisecond {} converts below its own millisecond, so the \
                     {position} share was expected to read back exactly one millisecond early; \
                     it read back {} ({drift} ms)",
                    share.accepted_at_ms,
                    credited.accepted_at_ms
                );
                read_back_early.push(share.accepted_at_ms);
            } else {
                ensure!(
                    drift == 0,
                    "{scenario}: millisecond {} converts at or above its own millisecond, so \
                     the {position} share had to round-trip unchanged, but it read back {} \
                     ({drift} ms)",
                    share.accepted_at_ms,
                    credited.accepted_at_ms
                );
            }
        }
        println!(
            "scenario {scenario}: rows={} window={} anchor_ms={anchor_ms} cutoff={} \
             read_back_early={read_back_early:?}",
            total_rows(&ledger.pool).await?,
            snapshot.shares.len(),
            snapshot.share_seq
        );
        db.close(ledger).await?;
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
    let anchor_ms = derived_anchor(&ledger.pool, 0).await?;
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
    let anchor_ms = derived_anchor(&ledger.pool, 0).await?;

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
