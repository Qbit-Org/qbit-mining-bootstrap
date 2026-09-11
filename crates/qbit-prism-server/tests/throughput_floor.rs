//! End-to-end share-append throughput floor for the native ledger (issue #271).
//!
//! What this measures, and what it deliberately does not:
//!
//! * It measures `Ledger::append`, the real production share path. Every
//!   append is one transaction that takes the cluster-wide `ORDER_LOCK`
//!   advisory lock, checks writability, bumps the single-row ledger clock and
//!   writes two rows (`src/ledger/window.rs`). Share appends are therefore
//!   serialized cluster-wide, and the number this file produces is the rate at
//!   which that serialized transaction retires.
//! * It is **not** comparable with the 2.x.x `qbit.prism.postgres-throughput.v1`
//!   report, which timed one bulk `INSERT ... SELECT ... FROM generate_series`:
//!   one statement, one commit, no advisory lock and no concurrency. That is
//!   why this report carries a new schema name, `…v2`.
//! * It is **not** a Stratum-level number either. Nothing here speaks the
//!   Stratum protocol; the frontend's parsing, hashing and RPC work is absent,
//!   so the rate here is an upper bound on what a full pool can commit.
//!
//! The run seeds a payout window with the #264 fixture generator, then appends
//! shares at several concurrency levels (1, 2 and 4 appenders by default), each
//! appender holding its own `Ledger` with its own pool and its own
//! `instance_id`, which is how separate production frontends behave. While a
//! level runs, a connection outside every appender pool samples `pg_locks` for
//! backends waiting on `ORDER_LOCK`, so the report can say how much of the wall
//! clock was spent queueing on that one lock.
//!
//! Every figure is host-specific. The report records the CPU model, core count,
//! memory, OS, PostgreSQL version and build profile alongside the rates, and no
//! rate from one machine should be carried to another.
//!
//! ```text
//! PRISM_TEST_DATABASE_URL=postgres://prism_test:prism_test@127.0.0.1:5432/prism_test \
//!     cargo test -p qbit-prism-server --test throughput_floor -- --nocapture
//! ```
//!
//! # Run this against a database nothing else is appending to
//!
//! The per-run schema isolates the *tables*, but `ORDER_LOCK` is a PostgreSQL
//! advisory lock, and an advisory lock is scoped to the **database**, not to a
//! schema. Any other backend appending shares into the same database — another
//! copy of this test, a developer's PRISM, a staging frontend — serializes
//! against this run's appenders and roughly halves the rate this file reports.
//! So: point `PRISM_TEST_DATABASE_URL` at a database that is doing nothing
//! else, ideally a disposable one.
//!
//! Because "ideally" is not "always", the run does not rely on the operator
//! getting that right. Every connection it opens carries a per-run
//! `application_name` (`throughput-<uuid>`), and the lock sampler splits what
//! it sees into this run's waiters and everyone else's. A level during which
//! any foreign backend held or waited on `ORDER_LOCK` is marked
//! `contaminated: true`, in the level, at the top of the report and in the
//! failure message. Such a level is still measured and still gated against the
//! floor — the rate it recorded is the rate the appends actually achieved — but
//! the number describes two workloads sharing one lock rather than this one,
//! and nothing downstream should treat it as a property of the append path.
//!
//! `contaminated` is `null`, not `false`, on a level whose sampler could not
//! look (an error, or a level that ended before the first poll returned).
//! "Nobody checked" is not "nothing was there".
//!
//! # A cancelled run leaves its schema behind
//!
//! Cleanup is an ordinary `await` on every exit path, so a passing run, a
//! failing run and a panicking run all drop their schema. A **cancelled** run
//! does not: `SIGINT` (Ctrl-C) kills the process before the drop runs, and the
//! `prism_throughput_<uuid>` schema survives with its seeded window in it.
//! Installing a signal handler would mean a new dependency, which this test is
//! not allowed to add, so the leak is documented rather than fixed. It is the
//! same behaviour as the sibling database tests in this crate.
//!
//! Find and drop the leftovers by hand:
//!
//! ```text
//! SELECT nspname FROM pg_namespace WHERE nspname LIKE 'prism\_throughput\_%';
//! DROP SCHEMA prism_throughput_0ee059ccc0314cbb8a814c0319f846d5 CASCADE;
//! ```
//!
//! Only do that when no run is in flight: a live run's schema matches the same
//! pattern.

use anyhow::{bail, ensure, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use qbit_prism_server::ledger::Ledger;
use serde_json::{Map, Value};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{Barrier, Notify};
use uuid::Uuid;

/// The #264 window generator, shared read-only with the JSONB ceiling gate.
/// `dead_code` is allowed because each test binary that includes the module
/// gets its own copy and uses a different part of it; this file seeds and
/// builds shares but never calls the round-trip verifier, and warning about
/// that would only push the fixture towards whichever consumer compiled last.
#[path = "support/window_fixture.rs"]
#[allow(dead_code)]
mod window_fixture;
use window_fixture::{WindowPlan, WINDOW_WEIGHT};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Report schema. Deliberately not `…v1`: see the module docs for why the two
/// generations of this report cannot be compared.
const REPORT_SCHEMA: &str = "qbit.prism.postgres-throughput.v2";

/// `ORDER_LOCK` is `0x505249534d000002` (`src/ledger.rs`). PostgreSQL splits a
/// 64-bit advisory key across `pg_locks.classid` (the high half) and
/// `pg_locks.objid` (the low half), with `objsubid = 1` marking the two-int
/// form. These are those two halves, spelled out so a change to the key in
/// production shows up here as a sampler that suddenly sees no waiters rather
/// than as a silently wrong number.
const ORDER_LOCK_CLASSID: i64 = 0x5052_4953;
const ORDER_LOCK_OBJID: i64 = 0x4d00_0002;
const ORDER_LOCK_OBJSUBID: i32 = 1;

/// PROVISIONAL: not yet calibrated on the CI runner.
///
/// Derived as 25% of the slowest level rate measured while writing this file:
/// 285.6 shares/s at one appender, in a **debug** build, on an 8-vCPU "Intel
/// Core Processor (Haswell, no TSX)" VM with 22 GiB of RAM, against PostgreSQL
/// 16.15 in Docker with `fsync=on`, `full_page_writes=on` and
/// `synchronous_commit=on`. 25% of that is 71.4, rounded down to 71.
///
/// The debug figure is the right base precisely because CI's
/// `prism-native-postgres` job builds and runs this test in debug. The 4x
/// margin absorbs the GitHub 2-vCPU runner being slower than this host and
/// run-to-run variance on shared hardware; it is wide enough that this constant
/// catches a collapse of the append path but not a modest regression. The
/// coordinator replaces it with a value measured on the CI runner itself, at
/// which point the margin can shrink and the floor starts earning its name.
const CI_MIN_SHARES_PER_SEC: f64 = 71.0;

/// Each appender appends sequentially, so one connection would do; `connect`
/// floors the pool at 2 anyway (`src/ledger/connect.rs`). Keeping it at the
/// floor means the pool can never be what limits a level.
const APPENDER_POOL_CONNECTIONS: u32 = 2;

/// Pool for the seeding ledger. The fixture loads in batched statements over a
/// single connection; the floor of 2 is plenty.
const SEED_POOL_CONNECTIONS: u32 = 2;

const WRITER_SEED: &str = "throughput-seed";

const MIN_SHARES_VAR: &str = "QBIT_PRISM_MIN_SHARES_PER_SEC";
const APPENDERS_VAR: &str = "QBIT_PRISM_THROUGHPUT_APPENDERS";
const SHARES_VAR: &str = "QBIT_PRISM_THROUGHPUT_SHARES";
const WINDOW_SHARES_VAR: &str = "QBIT_PRISM_THROUGHPUT_WINDOW_SHARES";
const LOCK_SAMPLE_VAR: &str = "QBIT_PRISM_THROUGHPUT_LOCK_SAMPLE_MS";
const REPORT_VAR: &str = "QBIT_PRISM_THROUGHPUT_REPORT";

const DEFAULT_APPENDERS: &str = "1,2,4";
const DEFAULT_LOCK_SAMPLE_MS: u64 = 10;
/// The sampler's interval is bounded at both ends, at entry.
///
/// Below 1 ms there is no interval at all, and the "sample" would be a busy
/// loop competing with the appenders for the server. Above 1000 ms the poll can
/// no longer resolve what it exists to measure: an `ORDER_LOCK` wait lasts
/// milliseconds, so a sampler that looks once a second mostly looks between the
/// waits and reports a handful of points as if they were a series. A coarse
/// interval also used to be dead time at the end of every level; the sampler
/// now wakes on demand, but the ceiling stays because the *measurement* is what
/// a second-scale interval ruins.
const MIN_LOCK_SAMPLE_MS: u64 = 1;
const MAX_LOCK_SAMPLE_MS: u64 = 1_000;
/// An appender count above this is refused: the levels are meant to model a
/// handful of production frontends, and a four-figure count would exhaust the
/// server's connection slots long before it said anything about the lock.
const MAX_APPENDERS: u64 = 64;

const NOTES: &str = "v2 measures per-share transactional Ledger::append: one BEGIN, one \
                     ORDER_LOCK advisory lock, one writability check, one ledger-clock update, \
                     two INSERTs and one COMMIT per share. It is NOT comparable with v1, which \
                     timed a single bulk INSERT ... SELECT ... FROM generate_series with no \
                     advisory lock, no concurrency and one commit for the whole batch. Rates are \
                     host-specific; read them together with the machine, postgres_version, \
                     durability and build_profile fields.";

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
/// This is `window_read_oracle.rs`'s guard, copied verbatim so the two files
/// cannot drift into different answers for the same environment. The reasoning
/// is recorded there: CI's database-free `rust-tests` job builds and runs every
/// workspace target with no server, so skipping has to stay possible, while
/// `prism-native-postgres` runs `--all-targets` against a PostgreSQL 16 service
/// and a database outage there must be a failure rather than a silent pass.
/// That second job is also how this floor reaches CI with no workflow change.
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
// Environment parsing (EP-VALIDATION)
// ---------------------------------------------------------------------------

/// A variable that is set but unreadable is an error, never an absent one:
/// treating a non-Unicode `QBIT_PRISM_MIN_SHARES_PER_SEC` as unset would
/// quietly substitute the compiled-in floor for a deliberately configured one.
fn env_raw(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => bail!("{name} is set but not readable: {error}"),
    }
}

/// The floor, in shares per second. Must be a finite decimal at or above zero.
///
/// `f64::from_str` accepts `nan`, `inf` and `-inf`, and every one of them would
/// make the comparison against the measured rate meaningless (`NaN` compares
/// false against everything, so a `nan` floor would pass every run). They are
/// rejected here, at entry, before any database work.
fn parse_minimum(raw: Option<&str>, fallback: f64) -> Result<f64> {
    let Some(raw) = raw else {
        return Ok(fallback);
    };
    let trimmed = raw.trim();
    ensure!(
        !trimmed.is_empty(),
        "{MIN_SHARES_VAR} is set to an empty value; it must be a finite decimal >= 0, \
         or unset to use the compiled-in floor"
    );
    let value: f64 = trimmed.parse().with_context(|| {
        format!("{MIN_SHARES_VAR}={trimmed:?} is not a decimal number of shares per second")
    })?;
    ensure!(
        value.is_finite(),
        "{MIN_SHARES_VAR}={trimmed:?} is not finite; a non-finite floor can never be compared \
         against a measured rate"
    );
    ensure!(
        value >= 0.0,
        "{MIN_SHARES_VAR}={trimmed:?} is negative; the floor must be >= 0"
    );
    Ok(value)
}

/// A positive count. Zero and negative values are refused rather than clamped,
/// because "append zero shares" would report an infinite rate.
fn parse_positive_u64(name: &str, raw: Option<&str>, fallback: u64) -> Result<u64> {
    let Some(raw) = raw else {
        return Ok(fallback);
    };
    let trimmed = raw.trim();
    ensure!(
        !trimmed.is_empty(),
        "{name} is set to an empty value; it must be a positive integer"
    );
    let value: u64 = trimmed
        .parse()
        .with_context(|| format!("{name}={trimmed:?} is not a positive integer"))?;
    ensure!(value > 0, "{name}={trimmed:?} must be greater than zero");
    Ok(value)
}

/// The lock sampling interval, in milliseconds, bounded to
/// `MIN_LOCK_SAMPLE_MS..=MAX_LOCK_SAMPLE_MS`.
///
/// Checked here, at entry, alongside every other variable and before any
/// connection is opened (EP-VALIDATION), so a run configured to produce
/// meaningless wait figures costs nothing rather than costing a full seeding
/// pass first.
fn parse_lock_sample_ms(raw: Option<&str>, fallback: u64) -> Result<u64> {
    let value = parse_positive_u64(LOCK_SAMPLE_VAR, raw, fallback)?;
    ensure!(
        (MIN_LOCK_SAMPLE_MS..=MAX_LOCK_SAMPLE_MS).contains(&value),
        "{LOCK_SAMPLE_VAR}={value} is outside {MIN_LOCK_SAMPLE_MS}..={MAX_LOCK_SAMPLE_MS} \
         milliseconds; an interval coarser than a second cannot resolve millisecond lock \
         waits, and would report a few stray points as if they were a series"
    );
    Ok(value)
}

/// The window size must divide the fixture's window weight exactly, otherwise
/// the payout window the seeded shares form would not be the requested number
/// of shares wide (`tests/support/window_fixture.rs`). `WindowPlan::new` says
/// the same thing, but it says it after the schema exists; saying it here keeps
/// the promise that a misconfigured run costs nothing.
fn parse_window_shares(raw: Option<&str>, fallback: u64) -> Result<u64> {
    let value = parse_positive_u64(WINDOW_SHARES_VAR, raw, fallback)?;
    let weight = u128::from(value);
    ensure!(
        weight <= WINDOW_WEIGHT,
        "{WINDOW_SHARES_VAR}={value} exceeds the fixture window weight {WINDOW_WEIGHT}; \
         each seeded share needs a difficulty of at least 1"
    );
    ensure!(
        WINDOW_WEIGHT.is_multiple_of(weight),
        "{WINDOW_SHARES_VAR}={value} must divide the fixture window weight {WINDOW_WEIGHT} \
         exactly, otherwise the seeded payout window would not be {value} shares wide"
    );
    Ok(value)
}

/// The concurrency levels, as a comma-separated list of appender counts.
///
/// Duplicates are refused: two levels with the same count would hand their
/// appenders the same `writer_id`s, and the per-appender committed counts in
/// the report would then mix two levels together.
fn parse_appenders(raw: Option<&str>) -> Result<Vec<u32>> {
    let raw = raw.unwrap_or(DEFAULT_APPENDERS);
    let trimmed = raw.trim();
    ensure!(
        !trimmed.is_empty(),
        "{APPENDERS_VAR} is set to an empty value; it must be a comma-separated list of \
         appender counts, for example {DEFAULT_APPENDERS:?}"
    );
    let mut levels = Vec::new();
    for field in trimmed.split(',') {
        let field = field.trim();
        ensure!(
            !field.is_empty(),
            "{APPENDERS_VAR}={trimmed:?} has an empty entry; every entry must be an appender \
             count between 1 and {MAX_APPENDERS}"
        );
        let count: u64 = field.parse().with_context(|| {
            format!("{APPENDERS_VAR} entry {field:?} is not a positive integer")
        })?;
        ensure!(
            (1..=MAX_APPENDERS).contains(&count),
            "{APPENDERS_VAR} entry {field:?} must be between 1 and {MAX_APPENDERS}"
        );
        let count = u32::try_from(count)?;
        ensure!(
            !levels.contains(&count),
            "{APPENDERS_VAR}={trimmed:?} repeats the appender count {count}; every level needs \
             a distinct count so its appenders get distinct writer IDs"
        );
        levels.push(count);
    }
    Ok(levels)
}

/// Splits a level's share budget across its appenders.
///
/// When the budget does not divide evenly the remainder goes to the lowest
/// appender indexes, one share each, so the split is a pure function of
/// `(total, appenders)` and two runs of the same configuration append exactly
/// the same shares. The level's aggregate rate is over the whole budget either
/// way, so an uneven split does not bias it.
fn distribute(total: u64, appenders: u32) -> Vec<u64> {
    let appenders = u64::from(appenders);
    let base = total / appenders;
    let remainder = total % appenders;
    (0..appenders)
        .map(|index| base + u64::from(index < remainder))
        .collect()
}

/// `none` when nothing is streaming, `sync` when a commit can be made to wait
/// for a standby, `async` when standbys exist but none of them holds a commit.
///
/// Both inputs matter. `pg_stat_replication.sync_state` names what each
/// connected standby is right now, and `synchronous_standby_names` names what
/// the server is configured to wait for. A non-empty `synchronous_standby_names`
/// with no connected standby is still `sync`: commits will block as soon as one
/// is expected, which is exactly the durability fact a throughput number has to
/// be read against.
fn derive_replication_mode(sync_states: &[String], standby_names: &str) -> &'static str {
    if sync_states
        .iter()
        .any(|state| matches!(state.trim(), "sync" | "quorum"))
    {
        return "sync";
    }
    if !standby_names.trim().is_empty() {
        return "sync";
    }
    if sync_states.is_empty() {
        "none"
    } else {
        "async"
    }
}

/// Everything the run needs, validated before it touches a database.
#[derive(Clone, Debug)]
struct Config {
    test_name: String,
    /// The `application_name` every connection this run opens carries, so the
    /// lock sampler can tell this run's backends from everyone else's
    /// (EP-STATE). Unique per run, never per test name: two copies of the same
    /// test against one database must not look like one run.
    application_name: String,
    window_shares: u64,
    shares_per_level: u64,
    appenders: Vec<u32>,
    lock_sample: Duration,
    minimum: f64,
    minimum_source: String,
    report_path: PathBuf,
}

/// Per-test defaults, overridden by the `QBIT_PRISM_THROUGHPUT_*` variables.
#[derive(Clone, Copy, Debug)]
struct Defaults {
    window_shares: u64,
    shares_per_level: u64,
}

impl Config {
    fn from_env(test_name: &str, defaults: Defaults) -> Result<Self> {
        let window_shares = parse_window_shares(
            env_raw(WINDOW_SHARES_VAR)?.as_deref(),
            defaults.window_shares,
        )?;
        let shares_per_level = parse_positive_u64(
            SHARES_VAR,
            env_raw(SHARES_VAR)?.as_deref(),
            defaults.shares_per_level,
        )?;
        let appenders = parse_appenders(env_raw(APPENDERS_VAR)?.as_deref())?;
        let widest = appenders.iter().copied().max().unwrap_or(1);
        ensure!(
            shares_per_level >= u64::from(widest),
            "{SHARES_VAR}={shares_per_level} is below the widest level's appender count \
             {widest}; every appender must get at least one share"
        );
        let lock_sample_ms =
            parse_lock_sample_ms(env_raw(LOCK_SAMPLE_VAR)?.as_deref(), DEFAULT_LOCK_SAMPLE_MS)?;
        let raw_minimum = env_raw(MIN_SHARES_VAR)?;
        let minimum = parse_minimum(raw_minimum.as_deref(), CI_MIN_SHARES_PER_SEC)?;
        let minimum_source = match raw_minimum {
            Some(_) => format!("{MIN_SHARES_VAR} environment variable"),
            None => {
                "compiled-in CI_MIN_SHARES_PER_SEC (provisional, not yet CI-calibrated)".to_owned()
            }
        };
        let report_path = match env_raw(REPORT_VAR)? {
            Some(raw) => {
                let trimmed = raw.trim();
                ensure!(
                    !trimmed.is_empty(),
                    "{REPORT_VAR} is set to an empty value; it must be a writable file path"
                );
                PathBuf::from(trimmed)
            }
            None => default_report_path(test_name),
        };
        Ok(Self {
            test_name: test_name.to_owned(),
            application_name: format!("throughput-{}", Uuid::new_v4().simple()),
            window_shares,
            shares_per_level,
            appenders,
            lock_sample: Duration::from_millis(lock_sample_ms),
            minimum,
            minimum_source,
            report_path,
        })
    }
}

/// `<workspace root>/target/prism-postgres-throughput-<test name>.json`.
///
/// The path carries the test name because the two floor tests can run in
/// parallel threads of one binary (`--include-ignored`), and a single default
/// path meant whichever finished last silently overwrote the other. With a path
/// per test neither can lose, and the `test` field in the report says which one
/// wrote it even when an operator has redirected both with
/// `QBIT_PRISM_THROUGHPUT_REPORT`, which remains the explicit override.
fn default_report_path(test_name: &str) -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = manifest
        .parent()
        .and_then(Path::parent)
        .unwrap_or(manifest)
        .to_path_buf();
    root.join("target")
        .join(format!("prism-postgres-throughput-{test_name}.json"))
}

// ---------------------------------------------------------------------------
// Per-run schema
// ---------------------------------------------------------------------------

/// A throwaway schema on the configured server, dropped on every exit path.
///
/// The run creates tables, loads six-figure row counts and takes a cluster-wide
/// advisory lock; none of that may land in `public` or in another test's
/// schema. `search_path` is pushed into the URL rather than set per session, so
/// every pool `Ledger::connect` opens from that URL lands in the same place.
///
/// The same trick carries the run's `application_name`: it goes into the URL,
/// which is the only thing `Ledger::connect` is handed, so the seeding ledger,
/// every appender ledger and the sampler all announce themselves with it
/// without any production code having to know this test exists.
struct Database {
    admin: PgPool,
    schema: String,
    url: String,
}

impl Database {
    async fn open(raw: &str, application_name: &str) -> Result<Self> {
        let mut url = url::Url::parse(raw)?;
        url.query_pairs_mut()
            .append_pair("application_name", application_name);
        let admin = PgPool::connect(url.as_str()).await?;
        // EP-STATE: everything downstream — which waiter is ours, which level
        // is contaminated, whether the floor describes one workload — rests on
        // the server having actually recorded this name. sqlx 0.8 parses
        // `application_name` out of the URL and sends it in the startup packet,
        // but "the library is supposed to" is not evidence, and a silently
        // ignored parameter would make every foreign backend look like ours and
        // every contaminated run look clean. So the run asks the server, once,
        // and refuses to measure anything if the answer is wrong.
        let announced: String = sqlx::query_scalar("SELECT current_setting('application_name')")
            .fetch_one(&admin)
            .await
            .context("reading back the run's application_name")?;
        if announced != application_name {
            admin.close().await;
            bail!(
                "the server recorded application_name {announced:?} for a connection opened with \
                 application_name={application_name:?}; without it the lock sampler cannot tell \
                 this run's backends from any other, so no measurement here would be trustworthy"
            );
        }
        let schema = format!("prism_throughput_{}", Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        Ok(Self {
            admin,
            schema,
            url: url.to_string(),
        })
    }

    /// EP-ERRORS: the schema goes away whether the run passed or failed, so a
    /// red run leaves no `prism_throughput_%` schema on a server that outlives
    /// it.
    async fn close(self) -> Result<()> {
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await?;
        self.admin.close().await;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ORDER_LOCK wait sampling
// ---------------------------------------------------------------------------

/// The sampler's stop signal: a flag it can test, plus a wake-up it can wait
/// on.
///
/// The flag alone was not enough. The sampler used to test it only at the top
/// of the loop and then block in `sleep(interval)`, so every level paid up to
/// one whole interval of dead time after its appends had finished, and
/// `run_level` sat in `sampler.await` for all of it. Pairing the flag with a
/// `Notify` makes the sleep interruptible: `stop()` sets the flag *and* hands
/// the sampler a permit, `notify_one` stores that permit even if the sampler is
/// not waiting yet, so there is no window in which the signal can be lost
/// (EP-ERRORS: shutdown is prompt and guaranteed, not eventual).
#[derive(Default)]
struct SamplerStop {
    flag: AtomicBool,
    wake: Notify,
}

impl SamplerStop {
    fn stop(&self) {
        self.flag.store(true, Ordering::Relaxed);
        self.wake.notify_one();
    }

    fn stopped(&self) -> bool {
        self.flag.load(Ordering::Relaxed)
    }

    /// Sleeps for `interval`, or returns as soon as `stop` is called.
    async fn sleep_until_stopped(&self, interval: Duration) {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = self.wake.notified() => {}
        }
    }
}

/// What a level's sampler saw. Every figure is an estimate from a 100 Hz poll,
/// never an exact accounting, and the report labels it as such.
#[derive(Debug, Default)]
struct LockWaitSummary {
    samples: u64,
    total_waiters: u64,
    max_waiters: u64,
    /// Distinct `(pid, waitstart)` pairs. A wait shorter than the sampling
    /// interval can be missed entirely, so this is a lower bound and is
    /// reported as one.
    episodes: BTreeSet<(i32, String)>,
    /// Sum over samples of `waiters x (time since the previous sample)`, a
    /// Riemann estimate of waiter-seconds.
    waiter_seconds: f64,
    /// The largest number of backends *not* from this run seen holding or
    /// waiting on `ORDER_LOCK` in a single sample.
    foreign_waiters_max: u64,
    /// Distinct pids of backends not from this run seen holding or waiting on
    /// `ORDER_LOCK` at any point in the level.
    foreign_backends: BTreeSet<i32>,
    /// Set when the sampler could not do its job. Then every figure above is
    /// reported as `null`: an unsampled level has an unknown wait, not a zero
    /// one (EP-OBSERVABILITY).
    error: Option<String>,
}

impl LockWaitSummary {
    /// The error to report, which is the sampler's own error if it had one and
    /// otherwise the fact that it never completed a poll.
    ///
    /// A level that ends before the first query returns used to emit
    /// `mean_waiters: 0.0`, `max_waiters: 0` and `estimated_waiter_seconds:
    /// 0.0` beside `samples: 0`. That reads as "nobody ever waited on the lock"
    /// when the truth is "nobody looked", which is precisely the zero-for-
    /// unknown this file forbids elsewhere (EP-OBSERVABILITY).
    fn effective_error(&self) -> Option<String> {
        self.error.clone().or_else(|| {
            (self.samples == 0)
                .then(|| "the level ended before the sampler completed a poll".to_owned())
        })
    }

    /// `Some(true)` when a foreign backend was seen, `Some(false)` when the
    /// sampler looked and saw none, `None` when it could not look. `None` is
    /// deliberate: "the sampler never ran" is not evidence that the database
    /// was quiet, and rendering it as `false` would be the same lie as
    /// rendering an unmeasured wait as zero.
    fn contaminated(&self) -> Option<bool> {
        match self.effective_error() {
            Some(_) => None,
            None => Some(!self.foreign_backends.is_empty()),
        }
    }

    fn to_json(&self, interval: Duration) -> Value {
        let mut object = Map::new();
        object.insert(
            "sample_interval_milliseconds".to_owned(),
            Value::from(interval.as_millis() as u64),
        );
        object.insert(
            "method".to_owned(),
            Value::from(
                "pg_locks left-joined to pg_stat_activity, filtered to the ORDER_LOCK advisory \
                 lock in this database, polled on a connection outside every appender pool; a \
                 row is this run's when pg_stat_activity.application_name matches the run's \
                 application_name, and foreign otherwise",
            ),
        );
        object.insert(
            "estimate".to_owned(),
            Value::from(
                "sampled: waiter counts are point estimates, episodes are a lower bound because \
                 a wait shorter than the interval can be missed, and waiter_seconds is a Riemann \
                 sum over the samples",
            ),
        );
        // The foreign figures are about a *different* population from the four
        // below them: this run's waiters are ungranted holders-in-waiting of
        // ORDER_LOCK, while a foreign backend counts whether it is waiting on
        // the lock or holding it, because either way it is serialising against
        // this run's appends.
        object.insert(
            "foreign_scope".to_owned(),
            Value::from(
                "a backend is foreign when it is not part of this run; it counts whether it was \
                 waiting on ORDER_LOCK or holding it, because either serialises against this \
                 run's appends. A backend whose pg_stat_activity row this role may not read \
                 counts as foreign, which is the safe direction",
            ),
        );
        let error = self.effective_error();
        match &error {
            Some(error) => {
                object.insert("error".to_owned(), Value::from(error.clone()));
                for key in [
                    "mean_waiters",
                    "max_waiters",
                    "episodes_lower_bound",
                    "estimated_waiter_seconds",
                    "foreign_waiters_max",
                    "foreign_backends_seen",
                ] {
                    object.insert(key.to_owned(), Value::Null);
                }
                // A real sampler error leaves the sample count unknown too; a
                // level that simply ended too early knows exactly how many
                // polls completed, and it was none.
                object.insert(
                    "samples".to_owned(),
                    match self.error {
                        Some(_) => Value::Null,
                        None => Value::from(self.samples),
                    },
                );
            }
            None => {
                object.insert("error".to_owned(), Value::Null);
                object.insert("samples".to_owned(), Value::from(self.samples));
                let mean = self.total_waiters as f64 / self.samples as f64;
                object.insert("mean_waiters".to_owned(), json_f64(mean));
                object.insert("max_waiters".to_owned(), Value::from(self.max_waiters));
                object.insert(
                    "episodes_lower_bound".to_owned(),
                    Value::from(self.episodes.len() as u64),
                );
                object.insert(
                    "estimated_waiter_seconds".to_owned(),
                    json_f64(self.waiter_seconds),
                );
                object.insert(
                    "foreign_waiters_max".to_owned(),
                    Value::from(self.foreign_waiters_max),
                );
                object.insert(
                    "foreign_backends_seen".to_owned(),
                    Value::from(self.foreign_backends.len() as u64),
                );
            }
        }
        object.insert(
            "contaminated".to_owned(),
            self.contaminated().map_or(Value::Null, Value::from),
        );
        Value::Object(object)
    }
}

/// Polls for backends queued behind `ORDER_LOCK` until `stop` is signalled.
///
/// The connection is the caller's, opened outside every appender pool, so the
/// sampler can never be the reason an appender waits for a connection. The
/// first query failure ends the sampling and is recorded; carrying on would
/// produce a series with an unknown hole in it.
///
/// The query deliberately does not filter on `granted`. This run's *waiters*
/// are the ungranted rows, as before, but a foreign backend counts either way:
/// one that holds `ORDER_LOCK` is making this run's appenders wait just as
/// surely as one that is queued behind them.
async fn sample_order_lock(
    pool: PgPool,
    interval: Duration,
    application_name: String,
    stop: Arc<SamplerStop>,
) -> LockWaitSummary {
    const SQL: &str = "SELECT l.pid, l.granted, l.waitstart, \
                       (a.application_name IS NOT DISTINCT FROM $4) AS own \
                       FROM pg_locks l \
                       LEFT JOIN pg_stat_activity a ON a.pid = l.pid \
                       WHERE l.locktype = 'advisory' \
                       AND l.classid = $1::bigint::oid \
                       AND l.objid = $2::bigint::oid \
                       AND l.objsubid = $3 \
                       AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())";
    let mut summary = LockWaitSummary::default();
    let mut previous = Instant::now();
    while !stop.stopped() {
        let rows = sqlx::query(SQL)
            .bind(ORDER_LOCK_CLASSID)
            .bind(ORDER_LOCK_OBJID)
            .bind(ORDER_LOCK_OBJSUBID)
            .bind(&application_name)
            .fetch_all(&pool)
            .await;
        let now = Instant::now();
        let elapsed = now.duration_since(previous).as_secs_f64();
        previous = now;
        match rows {
            Ok(rows) => {
                let mut waiters = 0u64;
                let mut foreign = 0u64;
                for row in rows {
                    let pid: i32 = match row.try_get("pid") {
                        Ok(pid) => pid,
                        Err(error) => {
                            summary.error = Some(format!("pg_locks.pid is unreadable: {error}"));
                            return summary;
                        }
                    };
                    let granted: bool = match row.try_get("granted") {
                        Ok(granted) => granted,
                        Err(error) => {
                            summary.error =
                                Some(format!("pg_locks.granted is unreadable: {error}"));
                            return summary;
                        }
                    };
                    let own: bool = match row.try_get("own") {
                        Ok(own) => own,
                        Err(error) => {
                            summary.error = Some(format!(
                                "pg_stat_activity.application_name is unreadable: {error}"
                            ));
                            return summary;
                        }
                    };
                    if !own {
                        foreign += 1;
                        summary.foreign_backends.insert(pid);
                        continue;
                    }
                    if granted {
                        continue;
                    }
                    waiters += 1;
                    // `waitstart` arrived in PostgreSQL 14 and can still be
                    // NULL for a lock whose wait had not been recorded when the
                    // snapshot was taken.
                    let waitstart = match row.try_get::<Option<DateTime<Utc>>, _>("waitstart") {
                        Ok(Some(at)) => at.to_rfc3339_opts(SecondsFormat::Micros, true),
                        Ok(None) => "unknown".to_owned(),
                        Err(error) => {
                            summary.error =
                                Some(format!("pg_locks.waitstart is unreadable: {error}"));
                            return summary;
                        }
                    };
                    summary.episodes.insert((pid, waitstart));
                }
                summary.samples += 1;
                summary.total_waiters += waiters;
                summary.max_waiters = summary.max_waiters.max(waiters);
                summary.waiter_seconds += waiters as f64 * elapsed;
                summary.foreign_waiters_max = summary.foreign_waiters_max.max(foreign);
            }
            Err(error) => {
                summary.error = Some(format!("ORDER_LOCK sampling failed: {error}"));
                return summary;
            }
        }
        stop.sleep_until_stopped(interval).await;
    }
    summary
}

/// `pg_stat_statements` aggregates for the advisory-lock statement, when the
/// extension is installed. It is a low-overhead cross-check on the sampler:
/// `SELECT pg_advisory_xact_lock($1)` is almost entirely lock wait.
///
/// The caveat, recorded in the report: `MIGRATION_LOCK` and `SETTLEMENT_LOCK`
/// share that statement text, so the aggregate covers all three keys. This run
/// takes the other two only while creating the schema, before any level starts
/// and before the per-level reset.
async fn read_lock_statements(pool: &PgPool) -> Value {
    const QUERY_MATCH: &str = "%pg_advisory_xact_lock%";
    let mut object = Map::new();
    object.insert(
        "statement".to_owned(),
        Value::from("SELECT pg_advisory_xact_lock($1)"),
    );
    object.insert(
        "caveat".to_owned(),
        Value::from(
            "pg_stat_statements normalizes MIGRATION_LOCK, ORDER_LOCK and SETTLEMENT_LOCK to one \
             statement; the counters are reset immediately before each level, after the schema \
             has been created, so they are dominated by ORDER_LOCK",
        ),
    );
    let unavailable = |object: &mut Map<String, Value>, reason: String| {
        object.insert("status".to_owned(), Value::from("unavailable"));
        object.insert("reason".to_owned(), Value::from(reason));
        object.insert("calls".to_owned(), Value::Null);
        object.insert("total_exec_time_milliseconds".to_owned(), Value::Null);
    };
    let namespace = match lock_statements_namespace(pool).await {
        Ok(Some(namespace)) => namespace,
        Ok(None) => {
            unavailable(
                &mut object,
                "the pg_stat_statements extension is not installed on this server".to_owned(),
            );
            return Value::Object(object);
        }
        Err(error) => {
            unavailable(
                &mut object,
                format!("could not look up the pg_stat_statements extension: {error}"),
            );
            return Value::Object(object);
        }
    };
    let row = sqlx::query(&format!(
        "SELECT coalesce(sum(calls),0)::bigint AS calls, \
         coalesce(sum(total_exec_time),0)::double precision AS total_exec_time \
         FROM {namespace}.pg_stat_statements WHERE query LIKE $1"
    ))
    .bind(QUERY_MATCH)
    .fetch_one(pool)
    .await;
    match row {
        Ok(row) => {
            let calls: Result<i64, _> = row.try_get("calls");
            let total: Result<f64, _> = row.try_get("total_exec_time");
            match (calls, total) {
                (Ok(calls), Ok(total)) => {
                    object.insert("status".to_owned(), Value::from("available"));
                    object.insert("reason".to_owned(), Value::Null);
                    object.insert("calls".to_owned(), Value::from(calls));
                    object.insert("total_exec_time_milliseconds".to_owned(), json_f64(total));
                }
                (Err(error), _) | (_, Err(error)) => unavailable(
                    &mut object,
                    format!("pg_stat_statements columns are unreadable: {error}"),
                ),
            }
        }
        Err(error) => unavailable(
            &mut object,
            format!("pg_stat_statements could not be read: {error}"),
        ),
    }
    Value::Object(object)
}

/// The schema `pg_stat_statements` was installed into, already quoted by the
/// server, or `None` when the extension is absent.
///
/// The name has to be resolved rather than written unqualified. Every
/// connection in this run carries `options=-csearch_path=<run schema>`, which
/// *replaces* the search path, so `public` is not on it; an unqualified
/// `pg_stat_statements` would then fail to resolve on a server that has the
/// extension installed, and the report would say "unavailable" about a view
/// that was sitting right there.
async fn lock_statements_namespace(pool: &PgPool) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT quote_ident(n.nspname) FROM pg_extension e \
         JOIN pg_namespace n ON n.oid = e.extnamespace \
         WHERE e.extname = 'pg_stat_statements'",
    )
    .fetch_optional(pool)
    .await
}

/// Best-effort reset before a level. A failure is not fatal: the extension may
/// be absent, or the role may lack the privilege, and either way the level's
/// `pg_stat_statements` block reports what it could read.
async fn reset_lock_statements(pool: &PgPool) {
    if let Ok(Some(namespace)) = lock_statements_namespace(pool).await {
        let _ = sqlx::query(&format!("SELECT {namespace}.pg_stat_statements_reset()"))
            .execute(pool)
            .await;
    }
}

// ---------------------------------------------------------------------------
// Measurement
// ---------------------------------------------------------------------------

struct LevelResult {
    appenders: u32,
    share_count: i64,
    append_seconds: f64,
    shares_per_second: f64,
    passed_minimum: bool,
    /// `Some(true)` when a backend outside this run held or waited on
    /// `ORDER_LOCK` while this level was being timed, `Some(false)` when the
    /// sampler looked and found none, `None` when it could not look.
    contaminated: Option<bool>,
    per_appender: Vec<(String, i64)>,
    order_lock: Value,
}

struct Measurement {
    postgres_version: String,
    postgres_server_version: String,
    replication_mode: String,
    durability: Durability,
    levels: Vec<LevelResult>,
    share_count: i64,
    slowest_shares_per_second: f64,
    passed_minimum: bool,
    /// The run's verdict, folded from the levels: `Some(true)` if any level saw
    /// a foreign backend, `Some(false)` only if every level looked and none
    /// did, `None` when no level saw contamination but at least one could not
    /// tell.
    contaminated: Option<bool>,
}

/// Folds the levels' verdicts into the run's. `true` wins over everything, and
/// an unknown wins over `false`: one level that could not be checked is enough
/// to stop the run claiming it had the database to itself.
fn fold_contamination(levels: impl IntoIterator<Item = Option<bool>>) -> Option<bool> {
    let mut verdict = Some(false);
    for level in levels {
        match level {
            Some(true) => return Some(true),
            Some(false) => {}
            None => verdict = None,
        }
    }
    verdict
}

/// The sentence the report and the failure message both carry when a level ran
/// against a database that something else was using.
const CONTAMINATION_NOTE: &str =
    "the database was shared: a backend outside this run held or waited on ORDER_LOCK while a \
     level was being timed. ORDER_LOCK is database-wide, not schema-wide, so those appends \
     serialised against this run's. The rates below are what this run's appends actually \
     achieved and they are still gated against the floor, but they describe two workloads \
     sharing one lock rather than the append path on its own, and must not be carried forward \
     as a property of the append path. Re-run against a database nothing else is appending to.";

/// The same warning for a run in which some level could not be checked at all.
const CONTAMINATION_UNKNOWN_NOTE: &str =
    "whether the database was shared is unknown: at least one level's ORDER_LOCK sampler could \
     not complete a poll, so nothing observed that level's foreign backends. Read the per-level \
     order_lock.error before treating these rates as describing one workload.";

struct Durability {
    fsync: String,
    full_page_writes: String,
    synchronous_commit: String,
}

/// Read through an appender's own pool, so the recorded `synchronous_commit` is
/// the session value PRISM actually commits under: `Ledger::connect`'s
/// `after_connect` forces it to `on` (or leaves `remote_apply` alone) on every
/// connection it opens, and a server-level default read on some other
/// connection could disagree.
async fn read_durability(pool: &PgPool) -> Result<Durability> {
    let row = sqlx::query(
        "SELECT current_setting('fsync') AS fsync, \
         current_setting('full_page_writes') AS full_page_writes, \
         current_setting('synchronous_commit') AS synchronous_commit",
    )
    .fetch_one(pool)
    .await?;
    Ok(Durability {
        fsync: row.try_get("fsync")?,
        full_page_writes: row.try_get("full_page_writes")?,
        synchronous_commit: row.try_get("synchronous_commit")?,
    })
}

/// Replication is read, never assumed. A server that refuses the catalogue
/// (an insufficiently privileged role, for instance) leaves the field
/// `unknown` rather than the more flattering `none`.
async fn read_replication_mode(pool: &PgPool) -> String {
    let states: Result<Vec<String>, _> =
        sqlx::query_scalar("SELECT coalesce(sync_state,'') FROM pg_stat_replication")
            .fetch_all(pool)
            .await;
    let names: Result<String, _> = sqlx::query_scalar("SHOW synchronous_standby_names")
        .fetch_one(pool)
        .await;
    match (states, names) {
        (Ok(states), Ok(names)) => derive_replication_mode(&states, &names).to_owned(),
        _ => "unknown".to_owned(),
    }
}

async fn committed_count(pool: &PgPool, writer_id: &str) -> Result<i64> {
    sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger WHERE accepted AND writer_id = $1")
        .bind(writer_id)
        .fetch_one(pool)
        .await
        .with_context(|| format!("counting committed shares for writer {writer_id}"))
}

async fn measure(db: &Database, config: &Config) -> Result<Measurement> {
    // The seeding ledger is the only one that initialises: it creates the
    // schema, the migrations and the `qbit_prism_cluster` singleton the
    // appenders' `writable()` check reads.
    let seed = Ledger::connect(&db.url, WRITER_SEED.to_owned(), SEED_POOL_CONNECTIONS, true)
        .await
        .context("connecting the seeding ledger")?;
    let outcome = seed_and_run(db, config, &seed).await;
    seed.pool.close().await;
    outcome
}

async fn seed_and_run(db: &Database, config: &Config, seed: &Ledger) -> Result<Measurement> {
    let plan = WindowPlan::new(config.window_shares)?;
    let load = plan
        .load(&seed.pool, WRITER_SEED)
        .await
        .context("seeding the payout window")?;
    println!(
        "throughput_floor: seeded {} shares in {:.3}s ({:.0} rows/s, ~{} serialized bytes)",
        load.rows, load.seconds, load.rows_per_second, load.serialized_bytes
    );

    let postgres_version: String = sqlx::query_scalar("SELECT version()")
        .fetch_one(&seed.pool)
        .await?;
    let postgres_server_version: String = sqlx::query_scalar("SHOW server_version")
        .fetch_one(&seed.pool)
        .await?;
    let replication_mode = read_replication_mode(&seed.pool).await;

    // Share indexes never repeat, across levels or against the seeded window,
    // so every `share_id` and every derived header hash is globally unique and
    // no append can be turned into an idempotent no-op by a collision.
    let mut next_index = config.window_shares + 1;
    let mut durability = None;
    let mut levels = Vec::new();
    let mut total_shares = 0i64;
    let mut slowest = f64::INFINITY;
    let mut passed_all = true;

    for &appenders in &config.appenders {
        let mut ledgers = Vec::new();
        for index in 0..appenders {
            let ledger = Ledger::connect(
                &db.url,
                format!("throughput-appender-{appenders}-{index}"),
                APPENDER_POOL_CONNECTIONS,
                false,
            )
            .await
            .with_context(|| format!("connecting appender {index} of level {appenders}"))?;
            if durability.is_none() {
                durability = Some(read_durability(&ledger.pool).await?);
            }
            ledgers.push(ledger);
        }
        let result = run_level(db, config, &plan, &ledgers, appenders, &mut next_index).await;
        for ledger in &ledgers {
            ledger.pool.close().await;
        }
        let result = result?;
        total_shares += result.share_count;
        slowest = slowest.min(result.shares_per_second);
        passed_all &= result.passed_minimum;
        println!(
            "throughput_floor: {} appender(s): {} shares in {:.3}s = {:.1} shares/s (floor {:.1}, {})",
            result.appenders,
            result.share_count,
            result.append_seconds,
            result.shares_per_second,
            config.minimum,
            if result.passed_minimum { "pass" } else { "FAIL" },
        );
        match result.contaminated {
            Some(true) => println!(
                "throughput_floor: WARNING, level of {} appender(s) is CONTAMINATED: {}",
                result.appenders, CONTAMINATION_NOTE
            ),
            None => println!(
                "throughput_floor: WARNING, level of {} appender(s): {}",
                result.appenders, CONTAMINATION_UNKNOWN_NOTE
            ),
            Some(false) => {}
        }
        levels.push(result);
    }

    let durability = match durability {
        Some(durability) => durability,
        None => bail!("no appender level ran, so no durability settings were read"),
    };
    let contaminated = fold_contamination(levels.iter().map(|level| level.contaminated));
    Ok(Measurement {
        postgres_version,
        postgres_server_version,
        replication_mode,
        durability,
        levels,
        share_count: total_shares,
        slowest_shares_per_second: slowest,
        passed_minimum: passed_all,
        contaminated,
    })
}

async fn run_level(
    db: &Database,
    config: &Config,
    plan: &WindowPlan,
    ledgers: &[Ledger],
    appenders: u32,
    next_index: &mut u64,
) -> Result<LevelResult> {
    let split = distribute(config.shares_per_level, appenders);
    let sampler_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&db.url)
        .await
        .context("opening the ORDER_LOCK sampling connection")?;
    reset_lock_statements(&sampler_pool).await;
    let stop = Arc::new(SamplerStop::default());
    let sampler = tokio::spawn(sample_order_lock(
        sampler_pool.clone(),
        config.lock_sample,
        config.application_name.clone(),
        stop.clone(),
    ));

    // The main task is the extra participant, so every appender is released at
    // the same instant and the clock starts there rather than at the first
    // `spawn`, which would have charged task startup to the append rate.
    let barrier = Arc::new(Barrier::new(ledgers.len() + 1));
    let mut handles = Vec::new();
    for (position, ledger) in ledgers.iter().enumerate() {
        let count = split[position];
        let first = *next_index;
        *next_index += count;
        let ledger = ledger.clone();
        let plan = plan.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            for index in first..first + count {
                let result = ledger
                    .append(plan.share(index), None)
                    .await
                    .with_context(|| {
                        format!("appending share index {index} as {}", ledger.instance_id)
                    })?;
                // A share that was already there is not throughput. It would
                // also mean the index allocation above had collided, which is
                // worth failing the level over rather than counting.
                ensure!(
                    result.inserted,
                    "share index {index} was already present, so appender {} measured a \
                     no-op instead of an insert",
                    ledger.instance_id
                );
            }
            Ok::<(), anyhow::Error>(())
        }));
    }
    barrier.wait().await;
    let started = Instant::now();
    let mut failure: Option<anyhow::Error> = None;
    for handle in handles {
        // Every appender is awaited even after one has failed, so the level
        // ends with no task still holding a connection or the advisory lock.
        let outcome = match handle.await {
            Ok(outcome) => outcome,
            Err(error) => Err(anyhow::anyhow!("an appender task panicked: {error}")),
        };
        if let Err(error) = outcome {
            if failure.is_none() {
                failure = Some(error);
            }
        }
    }
    let append_seconds = started.elapsed().as_secs_f64();
    // Signalled, not just flagged: the sampler is woken out of its sleep, so
    // the level ends when the appends end rather than one interval later.
    stop.stop();
    let lock_summary = match sampler.await {
        Ok(summary) => summary,
        Err(error) => LockWaitSummary {
            error: Some(format!("the ORDER_LOCK sampler panicked: {error}")),
            ..LockWaitSummary::default()
        },
    };
    let statements = read_lock_statements(&sampler_pool).await;
    sampler_pool.close().await;
    if let Some(error) = failure {
        return Err(error.context(format!("level of {appenders} appender(s) failed")));
    }

    let mut per_appender = Vec::new();
    let mut share_count = 0i64;
    for ledger in ledgers {
        let count = committed_count(&ledger.pool, &ledger.instance_id).await?;
        share_count += count;
        per_appender.push((ledger.instance_id.clone(), count));
    }
    let expected = i64::try_from(config.shares_per_level)?;
    ensure!(
        share_count == expected,
        "level of {appenders} appender(s) committed {share_count} shares, expected {expected}"
    );
    let shares_per_second = share_count as f64 / append_seconds.max(f64::MIN_POSITIVE);
    let contaminated = lock_summary.contaminated();
    let mut order_lock = lock_summary.to_json(config.lock_sample);
    if let Some(object) = order_lock.as_object_mut() {
        object.insert("pg_stat_statements".to_owned(), statements);
    }
    Ok(LevelResult {
        appenders,
        share_count,
        append_seconds,
        shares_per_second,
        passed_minimum: shares_per_second >= config.minimum,
        contaminated,
        per_appender,
        order_lock,
    })
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

/// `serde_json` cannot hold a non-finite number, and silently turning one into
/// `null` would hide it. Every rate here is finite by construction, but a
/// division that surprises us should surface as `null` in the report rather
/// than as a panic in the middle of a measured run.
fn json_f64(value: f64) -> Value {
    serde_json::Number::from_f64(value).map_or(Value::Null, Value::Number)
}

/// `GITHUB_SHA` when CI set it, otherwise the checked-out commit, otherwise
/// `null`. A missing commit is recorded as missing: a report that cannot say
/// what it measured should say so.
fn commit_id() -> Value {
    if let Ok(Some(sha)) = env_raw("GITHUB_SHA") {
        let sha = sha.trim();
        if !sha.is_empty() {
            return Value::from(sha);
        }
    }
    let output = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output();
    match output {
        Ok(output) if output.status.success() => {
            let sha = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            if sha.is_empty() {
                Value::Null
            } else {
                Value::from(sha)
            }
        }
        _ => Value::Null,
    }
}

/// Host facts, from procfs on Linux and `sysctl` on macOS. Anything this host
/// does not expose is `null`, never a plausible-looking default: a throughput
/// number whose machine is unknown must read as unknown.
fn machine() -> Value {
    let mut object = Map::new();
    object.insert("os".to_owned(), Value::from(std::env::consts::OS));
    let (cpu_model, cpu_count, memory_bytes) = match std::env::consts::OS {
        "linux" => (
            proc_cpuinfo_field("model name"),
            proc_cpu_count(),
            proc_meminfo_total_bytes(),
        ),
        "macos" => (
            sysctl("machdep.cpu.brand_string"),
            sysctl("hw.ncpu").and_then(|value| value.parse::<u64>().ok()),
            sysctl("hw.memsize").and_then(|value| value.parse::<u64>().ok()),
        ),
        _ => (None, None, None),
    };
    object.insert(
        "cpu_model".to_owned(),
        cpu_model.map_or(Value::Null, Value::from),
    );
    object.insert(
        "cpu_count".to_owned(),
        cpu_count.map_or(Value::Null, Value::from),
    );
    object.insert(
        "memory_bytes".to_owned(),
        memory_bytes.map_or(Value::Null, Value::from),
    );
    Value::Object(object)
}

fn proc_cpuinfo_field(field: &str) -> Option<String> {
    let text = std::fs::read_to_string("/proc/cpuinfo").ok()?;
    text.lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(key, _)| key.trim() == field)
        .map(|(_, value)| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn proc_cpu_count() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/cpuinfo").ok()?;
    let count = text
        .lines()
        .filter(|line| {
            line.split_once(':')
                .is_some_and(|(k, _)| k.trim() == "processor")
        })
        .count() as u64;
    (count > 0).then_some(count)
}

fn proc_meminfo_total_bytes() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    let line = text.lines().find(|line| line.starts_with("MemTotal:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    kib.checked_mul(1024)
}

fn sysctl(name: &str) -> Option<String> {
    let output = std::process::Command::new("sysctl")
        .args(["-n", name])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    (!value.is_empty()).then_some(value)
}

/// The fields that identify a run rather than describe its outcome: who ran,
/// how it was configured, on what, from which commit.
///
/// Both reports start here — the measured one and the one an errored run leaves
/// behind — so a failed run is identifiable by exactly the same fields as a
/// successful one, and a reader never has to guess which test or which
/// configuration produced the file in front of them.
fn report_identity(config: &Config) -> Map<String, Value> {
    let mut root = Map::new();
    root.insert("schema".to_owned(), Value::from(REPORT_SCHEMA));
    root.insert(
        "generated_at".to_owned(),
        Value::from(Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)),
    );
    root.insert("test".to_owned(), Value::from(config.test_name.clone()));
    root.insert(
        "application_name".to_owned(),
        Value::from(config.application_name.clone()),
    );
    root.insert(
        "window_shares".to_owned(),
        Value::from(config.window_shares),
    );
    root.insert(
        "appended_shares_per_level".to_owned(),
        Value::from(config.shares_per_level),
    );
    root.insert("min_shares_per_second".to_owned(), json_f64(config.minimum));
    root.insert(
        "min_shares_per_second_source".to_owned(),
        Value::from(config.minimum_source.clone()),
    );
    root.insert("commit".to_owned(), commit_id());
    root.insert(
        "build_profile".to_owned(),
        Value::from(if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        }),
    );
    root.insert("machine".to_owned(), machine());
    root
}

/// The report an errored run leaves behind, so a run that never reached a
/// measurement still says who it was, how it was configured and what went
/// wrong, instead of leaving a stale report from an earlier run as the only
/// file on disk.
///
/// `results` is an empty array, not a missing key: a consumer that iterates the
/// levels finds none rather than tripping over the shape of the file, and
/// `passed_minimum` is `false` because a run that could not measure certainly
/// did not clear the floor.
fn build_error_report(config: &Config, error: &anyhow::Error) -> Value {
    let mut root = report_identity(config);
    // `{:#}` renders the whole anyhow chain, outermost first, so the report
    // keeps the context each layer added rather than only the final cause.
    root.insert("error".to_owned(), Value::from(format!("{error:#}")));
    root.insert("passed_minimum".to_owned(), Value::from(false));
    root.insert("results".to_owned(), Value::Array(Vec::new()));
    Value::Object(root)
}

fn build_report(config: &Config, measurement: &Measurement) -> Value {
    let mut root = report_identity(config);
    root.insert("error".to_owned(), Value::Null);
    root.insert(
        "share_count".to_owned(),
        Value::from(measurement.share_count),
    );
    root.insert(
        "shares_per_second".to_owned(),
        json_f64(measurement.slowest_shares_per_second),
    );
    root.insert(
        "passed_minimum".to_owned(),
        Value::from(measurement.passed_minimum),
    );
    root.insert(
        "contaminated".to_owned(),
        measurement.contaminated.map_or(Value::Null, Value::from),
    );
    root.insert(
        "contamination_note".to_owned(),
        match measurement.contaminated {
            Some(true) => Value::from(CONTAMINATION_NOTE),
            None => Value::from(CONTAMINATION_UNKNOWN_NOTE),
            Some(false) => Value::Null,
        },
    );
    root.insert(
        "postgres_version".to_owned(),
        Value::from(measurement.postgres_version.clone()),
    );
    root.insert(
        "postgres_server_version".to_owned(),
        Value::from(measurement.postgres_server_version.clone()),
    );
    root.insert(
        "replication_mode".to_owned(),
        Value::from(measurement.replication_mode.clone()),
    );
    let mut durability = Map::new();
    durability.insert(
        "fsync".to_owned(),
        Value::from(measurement.durability.fsync.clone()),
    );
    durability.insert(
        "full_page_writes".to_owned(),
        Value::from(measurement.durability.full_page_writes.clone()),
    );
    durability.insert(
        "synchronous_commit".to_owned(),
        Value::from(measurement.durability.synchronous_commit.clone()),
    );
    root.insert("durability".to_owned(), Value::Object(durability));
    let results = measurement
        .levels
        .iter()
        .map(|level| {
            let mut entry = Map::new();
            entry.insert("appenders".to_owned(), Value::from(level.appenders));
            entry.insert("share_count".to_owned(), Value::from(level.share_count));
            entry.insert("append_seconds".to_owned(), json_f64(level.append_seconds));
            entry.insert(
                "shares_per_second".to_owned(),
                json_f64(level.shares_per_second),
            );
            entry.insert(
                "passed_minimum".to_owned(),
                Value::from(level.passed_minimum),
            );
            entry.insert(
                "contaminated".to_owned(),
                level.contaminated.map_or(Value::Null, Value::from),
            );
            let per_appender = level
                .per_appender
                .iter()
                .map(|(writer_id, count)| {
                    let mut one = Map::new();
                    one.insert("writer_id".to_owned(), Value::from(writer_id.clone()));
                    one.insert("share_count".to_owned(), Value::from(*count));
                    Value::Object(one)
                })
                .collect::<Vec<_>>();
            entry.insert("per_appender".to_owned(), Value::Array(per_appender));
            entry.insert("order_lock".to_owned(), level.order_lock.clone());
            Value::Object(entry)
        })
        .collect::<Vec<_>>();
    root.insert("results".to_owned(), Value::Array(results));
    root.insert("notes".to_owned(), Value::from(NOTES));
    Value::Object(root)
}

/// Writes the report and returns its path. Called before the floor assertion so
/// that a run which fails the floor still leaves the evidence behind.
fn write_report(config: &Config, measurement: &Measurement) -> Result<()> {
    write_json(config, &build_report(config, measurement))
}

fn write_json(config: &Config, report: &Value) -> Result<()> {
    if let Some(parent) = config.report_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let mut text = serde_json::to_string_pretty(report)?;
    text.push('\n');
    std::fs::write(&config.report_path, text)
        .with_context(|| format!("writing {}", config.report_path.display()))?;
    println!(
        "throughput_floor: report written to {}",
        config.report_path.display()
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Test bodies
// ---------------------------------------------------------------------------

async fn run_floor(test_name: &str, defaults: Defaults) -> Result<()> {
    // Configuration is validated first, so a malformed variable costs nothing:
    // no connection, no schema, no seeding. Nothing below this line can run
    // with an out-of-range sampling interval or an uncomparable floor
    // (EP-VALIDATION).
    let config = Config::from_env(test_name, defaults)?;
    let Some(raw) = database_url(test_name)? else {
        return Ok(());
    };
    println!(
        "throughput_floor: window={} shares/level={} appenders={:?} floor={:.1} ({}) \
         application_name={}",
        config.window_shares,
        config.shares_per_level,
        config.appenders,
        config.minimum,
        config.minimum_source,
        config.application_name,
    );
    let measurement = match measure_run(&config, &raw).await {
        Ok(measurement) => measurement,
        Err(error) => {
            // A run that dies connecting, seeding or appending still owes the
            // operator a file saying what it was and what went wrong; without
            // one, the newest report on disk belongs to some earlier run and
            // reads as though this one never happened. The write can only add
            // context to the original error, never replace it (EP-ERRORS).
            return Err(
                match write_json(&config, &build_error_report(&config, &error)) {
                    Ok(()) => error,
                    Err(write_error) => error.context(format!(
                        "the error report could not be written either: {write_error:#}"
                    )),
                },
            );
        }
    };
    write_report(&config, &measurement)?;
    for level in &measurement.levels {
        ensure!(
            level.passed_minimum,
            "share-append throughput floor: the level of {} appender(s) sustained {:.2} \
             shares/s, below the floor of {:.2} shares/s ({}).{} The report is at {}.",
            level.appenders,
            level.shares_per_second,
            config.minimum,
            config.minimum_source,
            match level.contaminated {
                Some(true) => format!(" NOTE: {CONTAMINATION_NOTE}"),
                None => format!(" NOTE: {CONTAMINATION_UNKNOWN_NOTE}"),
                Some(false) => String::new(),
            },
            config.report_path.display()
        );
    }
    Ok(())
}

/// Opens the run's schema, measures it, and drops the schema on the way out.
///
/// Split out of `run_floor` so that every error from the connection onwards
/// arrives at one place, where the error report is written.
async fn measure_run(config: &Config, raw: &str) -> Result<Measurement> {
    let db = Database::open(raw, &config.application_name).await?;
    let measured = measure(&db, config).await;
    // The schema goes away either way. A cleanup failure is attached to the
    // measurement error rather than replacing it, so a leaked schema can never
    // hide the failure that actually mattered.
    let cleanup = db.close().await;
    match measured {
        Ok(measurement) => {
            cleanup?;
            Ok(measurement)
        }
        Err(error) => Err(match cleanup {
            Ok(()) => error,
            Err(cleanup) => error.context(format!("schema cleanup also failed: {cleanup}")),
        }),
    }
}

/// The CI-sized floor: small enough to finish well inside the
/// `prism-native-postgres` job's 20-minute budget on a 2-vCPU runner, large
/// enough that a real regression in the append path moves it.
///
/// This test is deliberately not `#[ignore]`d. `prism-native-postgres` runs
/// `cargo test --locked -p qbit-prism-server --all-targets` against a
/// PostgreSQL 16 service, so the floor enters CI with no workflow change; the
/// database-free `rust-tests` job runs the same target and skips through the
/// guard above.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn share_append_throughput_floor() -> Result<()> {
    run_floor(
        "share_append_throughput_floor",
        Defaults {
            window_shares: 20_000,
            shares_per_level: 2_000,
        },
    )
    .await
}

/// The full-size floor, for a host with time to spare. `test/test-prism-postgres-throughput.sh`
/// runs this one, in release, against a disposable server.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full-size run: 100k seeded shares and 20k appends per level; run it with \
            test/test-prism-postgres-throughput.sh or --ignored"]
async fn share_append_throughput_floor_full_size() -> Result<()> {
    run_floor(
        "share_append_throughput_floor_full_size",
        Defaults {
            window_shares: 100_000,
            shares_per_level: 20_000,
        },
    )
    .await
}

// ---------------------------------------------------------------------------
// Database-free unit tests
// ---------------------------------------------------------------------------

#[test]
fn environment_parsers_reject_what_cannot_be_a_floor() {
    // A `nan` floor would compare false against every rate and pass every run;
    // an infinite one would fail every run. Both are configuration errors, not
    // extreme floors.
    for bad in [
        "", "   ", "nan", "NaN", "inf", "-inf", "abc", "-1", "-0.5", "1e",
    ] {
        assert!(
            parse_minimum(Some(bad), 1.0).is_err(),
            "{MIN_SHARES_VAR}={bad:?} must be refused"
        );
    }
    for (good, expected) in [("0", 0.0), ("25", 25.0), (" 12.5 ", 12.5), ("1e3", 1000.0)] {
        assert_eq!(
            parse_minimum(Some(good), 1.0).expect("valid floor"),
            expected
        );
    }
    assert_eq!(parse_minimum(None, 7.5).expect("fallback"), 7.5);
}

#[test]
fn environment_parsers_reject_impossible_counts() {
    for bad in ["", "0", "-1", "abc", "1.5"] {
        assert!(
            parse_positive_u64(SHARES_VAR, Some(bad), 10).is_err(),
            "{SHARES_VAR}={bad:?} must be refused"
        );
    }
    assert_eq!(
        parse_positive_u64(SHARES_VAR, Some(" 2000 "), 10).expect("valid count"),
        2_000
    );
    assert_eq!(
        parse_positive_u64(SHARES_VAR, None, 10).expect("fallback"),
        10
    );

    // 30000 does not divide 8_000_000, so the seeded window would not be
    // 30000 shares wide.
    for bad in ["30000", "0", "3", "8000001", "abc"] {
        assert!(
            parse_window_shares(Some(bad), 20_000).is_err(),
            "{WINDOW_SHARES_VAR}={bad:?} must be refused"
        );
    }
    for good in ["20000", "100000", "400000", "1"] {
        assert!(
            parse_window_shares(Some(good), 20_000).is_ok(),
            "{WINDOW_SHARES_VAR}={good:?} divides {WINDOW_WEIGHT} and must be accepted"
        );
    }
}

#[test]
fn lock_sample_interval_is_bounded_at_both_ends() {
    // Zero is not an interval, and neither is a negative or fractional one.
    for bad in ["", "0", "-1", "1.5", "abc"] {
        assert!(
            parse_lock_sample_ms(Some(bad), DEFAULT_LOCK_SAMPLE_MS).is_err(),
            "{LOCK_SAMPLE_VAR}={bad:?} must be refused"
        );
    }
    // Above a second the poll can no longer resolve a millisecond lock wait,
    // which is the only thing it exists to measure.
    for bad in ["1001", "20000", "18446744073709551615"] {
        let error = parse_lock_sample_ms(Some(bad), DEFAULT_LOCK_SAMPLE_MS)
            .expect_err("an interval above the ceiling must be refused")
            .to_string();
        assert!(
            error.contains("coarser than a second"),
            "the rejection must say why a coarse interval is useless, got {error:?}"
        );
    }
    for (good, expected) in [("1", 1), (" 10 ", 10), ("1000", 1_000)] {
        assert_eq!(
            parse_lock_sample_ms(Some(good), DEFAULT_LOCK_SAMPLE_MS).expect("in range"),
            expected
        );
    }
    assert_eq!(
        parse_lock_sample_ms(None, DEFAULT_LOCK_SAMPLE_MS).expect("fallback"),
        DEFAULT_LOCK_SAMPLE_MS
    );
    assert!(
        (MIN_LOCK_SAMPLE_MS..=MAX_LOCK_SAMPLE_MS).contains(&DEFAULT_LOCK_SAMPLE_MS),
        "the compiled-in default must itself be inside the range it enforces"
    );
}

#[test]
fn a_level_that_never_sampled_reports_unknown_rather_than_zero() {
    let interval = Duration::from_millis(10);

    let never_polled = LockWaitSummary::default();
    let json = never_polled.to_json(interval);
    assert_eq!(
        json["samples"],
        Value::from(0u64),
        "the count is known: none"
    );
    for unknown in [
        "mean_waiters",
        "max_waiters",
        "episodes_lower_bound",
        "estimated_waiter_seconds",
        "foreign_waiters_max",
        "foreign_backends_seen",
    ] {
        assert_eq!(
            json[unknown],
            Value::Null,
            "{unknown} must be null when nobody looked, never 0"
        );
    }
    assert_eq!(
        json["error"],
        Value::from("the level ended before the sampler completed a poll")
    );
    assert_eq!(
        json["contaminated"],
        Value::Null,
        "an unsampled level cannot claim the database was quiet"
    );

    // A sampler that failed outright does not even know how many polls it got
    // through, so its sample count is null as well.
    let failed = LockWaitSummary {
        error: Some("ORDER_LOCK sampling failed: connection closed".to_owned()),
        samples: 4,
        ..LockWaitSummary::default()
    };
    let json = failed.to_json(interval);
    assert_eq!(json["samples"], Value::Null);
    assert_eq!(json["contaminated"], Value::Null);

    // A sampler that did its job reports real numbers, including honest zeros:
    // here a level that was genuinely alone on the lock.
    let quiet = LockWaitSummary {
        samples: 6,
        total_waiters: 3,
        max_waiters: 1,
        waiter_seconds: 0.03,
        ..LockWaitSummary::default()
    };
    let json = quiet.to_json(interval);
    assert_eq!(json["samples"], Value::from(6u64));
    assert_eq!(json["mean_waiters"], json_f64(0.5));
    assert_eq!(json["max_waiters"], Value::from(1u64));
    assert_eq!(json["foreign_backends_seen"], Value::from(0u64));
    assert_eq!(json["error"], Value::Null);
    assert_eq!(json["contaminated"], Value::from(false));

    let shared = LockWaitSummary {
        samples: 6,
        foreign_waiters_max: 2,
        foreign_backends: [4242, 4243].into_iter().collect(),
        ..LockWaitSummary::default()
    };
    let json = shared.to_json(interval);
    assert_eq!(json["foreign_waiters_max"], Value::from(2u64));
    assert_eq!(json["foreign_backends_seen"], Value::from(2u64));
    assert_eq!(json["contaminated"], Value::from(true));
}

#[test]
fn contamination_folds_true_over_unknown_over_false() {
    assert_eq!(fold_contamination([]), Some(false));
    assert_eq!(fold_contamination([Some(false), Some(false)]), Some(false));
    assert_eq!(fold_contamination([Some(false), None]), None);
    assert_eq!(fold_contamination([None, Some(true)]), Some(true));
    assert_eq!(
        fold_contamination([Some(true), Some(false)]),
        Some(true),
        "one shared level makes the whole run's numbers shared"
    );
}

#[test]
fn the_sampler_stop_signal_is_not_lost_before_the_sleep() {
    // `notify_one` stores a permit, so a stop raised while the sampler is
    // querying rather than sleeping still ends the very next sleep. If this
    // ever regressed to `notify_waiters`, the signal would be dropped and the
    // level would stall for a whole interval, which is the bug this replaced.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("a current-thread runtime");
    runtime.block_on(async {
        let stop = SamplerStop::default();
        stop.stop();
        assert!(stop.stopped());
        let started = Instant::now();
        tokio::time::timeout(
            Duration::from_secs(5),
            stop.sleep_until_stopped(Duration::from_secs(3_600)),
        )
        .await
        .expect("a signalled stop must not wait out the interval");
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "the sleep returned after {:?}, so the stop permit was lost",
            started.elapsed()
        );
    });
}

#[test]
fn appender_levels_are_distinct_and_bounded() {
    assert_eq!(parse_appenders(None).expect("default"), vec![1, 2, 4]);
    assert_eq!(
        parse_appenders(Some(" 1, 8 ")).expect("spaced"),
        vec![1u32, 8]
    );
    for bad in ["", "0", "65", "-1", "1,,2", "1,1", "four", "1,2,1"] {
        assert!(
            parse_appenders(Some(bad)).is_err(),
            "{APPENDERS_VAR}={bad:?} must be refused"
        );
    }
}

#[test]
fn share_budgets_split_deterministically() {
    assert_eq!(distribute(2_000, 4), vec![500, 500, 500, 500]);
    assert_eq!(distribute(10, 4), vec![3, 3, 2, 2]);
    assert_eq!(distribute(7, 1), vec![7]);
    for (total, appenders) in [(2_000u64, 4u32), (10, 4), (20_001, 3), (7, 7)] {
        let split = distribute(total, appenders);
        assert_eq!(split.len(), appenders as usize);
        assert_eq!(
            split.iter().sum::<u64>(),
            total,
            "the whole budget is spent"
        );
        assert!(split.iter().all(|&count| count > 0), "no idle appender");
    }
}

#[test]
fn replication_mode_is_derived_from_what_the_server_reports() {
    assert_eq!(derive_replication_mode(&[], ""), "none");
    assert_eq!(
        derive_replication_mode(&["async".to_owned()], ""),
        "async",
        "a streaming standby that holds no commit is async"
    );
    assert_eq!(
        derive_replication_mode(&["async".to_owned(), "sync".to_owned()], "standby1"),
        "sync"
    );
    assert_eq!(
        derive_replication_mode(&["quorum".to_owned()], "ANY 1 (a, b)"),
        "sync",
        "quorum is a synchronous state"
    );
    assert_eq!(
        derive_replication_mode(&[], "standby1"),
        "sync",
        "a configured synchronous standby that has not connected still blocks commits"
    );
    assert_eq!(
        derive_replication_mode(&["potential".to_owned()], "   "),
        "async",
        "whitespace is not a standby name"
    );
}
