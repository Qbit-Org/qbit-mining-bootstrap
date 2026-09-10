//! JSONB ceiling gate for the PRISM payout window (issue #264, workstream #261).
//!
//! PostgreSQL refuses a JSONB container whose elements exceed 268,435,455
//! bytes. Four native writes still embed the whole payout window, so they grow
//! linearly with the share count and walk into that wall. This gate drives the
//! five window-carrying phases (refresh, enqueue, claim, landing, import)
//! against a real PostgreSQL 16, measures every JSONB column it can discover,
//! projects each write to the target share count, and compares the set of
//! writes that cross 25% of the hard limit against a checked-in list.
//!
//! The gate is a **ratchet**: it passes only when the crossing set equals
//! `KNOWN_VIOLATIONS` exactly. A new crossing write fails it, and so does a
//! listed write that stops crossing - that entry then has to be deleted.
//!
//! # Running it
//!
//! Reduced sizes (the default, what CI runs in the `prism-native-postgres`
//! job; the database-free `rust-tests` job skips this test by name):
//!
//! ```text
//! cargo test --locked -p qbit-prism-server --test jsonb_ceiling_gate
//! ```
//!
//! Full size (400,000 shares) against a disposable cluster started by the
//! repository script - it starts and cleans up its own PostgreSQL:
//!
//! ```text
//! test/prism-native-tests.sh cargo-args --locked -p qbit-prism-server \
//!   --test jsonb_ceiling_gate -- --ignored --nocapture jsonb_ceiling_ratchet_at_full_size
//! ```
//!
//! Full size against a PostgreSQL you already have:
//!
//! ```text
//! PRISM_TEST_DATABASE_URL=postgresql://user@127.0.0.1:5432/postgres \
//!   cargo test --locked -p qbit-prism-server --test jsonb_ceiling_gate \
//!   -- --ignored --nocapture jsonb_ceiling_ratchet_at_full_size
//! ```
//!
//! The baseline sweep behind the measurement document:
//!
//! ```text
//! PRISM_JSONB_GATE_BASELINE_SIZES=50000,100000,200000 \
//!   cargo test --locked -p qbit-prism-server --test jsonb_ceiling_gate \
//!   -- --ignored --nocapture jsonb_ceiling_baseline_sweep
//! ```
//!
//! # Settings
//!
//! | variable | default | meaning |
//! | --- | --- | --- |
//! | `PRISM_TEST_DATABASE_URL` | none | PostgreSQL to test against. Missing plus `CI` set is a failure, never a skip. |
//! | `PRISM_JSONB_GATE_TARGET_SHARES` | `400000` | share count the reduced run projects to |
//! | `PRISM_JSONB_GATE_N1` | `5000` | smaller reduced size |
//! | `PRISM_JSONB_GATE_N2` | `20000` | larger reduced size |
//! | `PRISM_JSONB_GATE_STATEMENT_TIMEOUT_MS` | `600000` | value exported as `PRISM_DATABASE_STATEMENT_TIMEOUT_MS` before the first `Ledger::connect` |
//! | `PRISM_JSONB_GATE_BASELINE_SIZES` | `50000,100000,200000` | sizes for the baseline sweep: at least two, strictly increasing, each dividing the window weight; checked before anything runs |
//!
//! Every share count must divide the window weight (8,000,000) exactly, and
//! `0 < n1 < n2 <= target` must hold. A malformed or out-of-range value is a
//! loud failure; nothing silently falls back to a default.

use anyhow::{bail, ensure, Context, Result};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{AcceptedShare, AuditBundle, AuditVerificationReport, FoundBlock, PayoutPolicy};
use qbit_prism_server::{
    coordinator::Coordinator,
    ledger::{Candidate, CandidateClaim, Ledger, Snapshot},
};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt::Write as _,
    time::Instant,
};

#[path = "support/fake_qbitd.rs"]
mod fake_qbitd;
#[path = "support/window_fixture.rs"]
mod window_fixture;

use fake_qbitd::{coinbase_suffix, coordinator_config, FakeNode};
use window_fixture::{WindowPlan, FAKE_NODE_NETWORK_DIFFICULTY, WINDOW_WEIGHT};

// ---------------------------------------------------------------------------
// Thresholds and the ratchet list
// ---------------------------------------------------------------------------

/// PostgreSQL's hard ceiling on the total size of a JSONB container's
/// elements, from `JENTRY_OFFLENMASK` in `src/include/utils/jsonb.h`. Exceeding
/// it raises `total size of jsonb object elements exceeds the maximum of
/// 268435455 bytes`.
const JSONB_ELEMENT_LIMIT: i64 = 268_435_455;

/// The gate fires at 25% of the hard limit, so a write is caught long before a
/// production window can reach the ceiling.
const GATE_THRESHOLD: i64 = JSONB_ELEMENT_LIMIT / 4; // 67,108,863 bytes

/// `canonical_audit_bytes` is bytea, so the JSONB ceiling does not apply to it.
/// It is reported against 25% of the 1 GiB varlena limit instead, and never
/// enters the ratchet.
const BYTEA_REPORT_THRESHOLD: i64 = 1_073_741_824 / 4;

/// The most negative intercept a fit accepts, as a fraction of the `n1`
/// measurement. Measured window-carrying writes sit under 0.3%; anything past
/// 5% is growth faster than linear, which a straight line under-projects.
const MAX_NEGATIVE_INTERCEPT: f64 = 0.05;

const PHASE_REFRESH: &str = "refresh";
const PHASE_ENQUEUE: &str = "enqueue";
const PHASE_CLAIM: &str = "claim";
const PHASE_LANDING: &str = "landing";
const PHASE_IMPORT: &str = "import";
const PHASES: &[&str] = &[
    PHASE_REFRESH,
    PHASE_ENQUEUE,
    PHASE_CLAIM,
    PHASE_LANDING,
    PHASE_IMPORT,
];

/// Number of JSONB columns the base schema has once `Ledger::connect(.., true)`
/// has applied `001_share_ledger.sql` and migrations 002-005. A test that only
/// applied `migrations/*.sql` would see 13 fewer, so this is a floor, not an
/// equality: a new column must be discovered and measured, never skipped.
const EXPECTED_JSONB_COLUMNS: usize = 17;

/// A write that embeds the payout window and is expected to cross the gate
/// threshold today. Keyed by the table, column and phase that writes it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Violation {
    table: &'static str,
    column: &'static str,
    phase: &'static str,
}

/// The four known window-carrying writes at this base commit. Each entry names
/// the issue that removes it; when that lands, the gate fails until the entry
/// is deleted.
const KNOWN_VIOLATIONS: &[Violation] = &[
    // 3 window copies (2 x AcceptedShare + 1 x CountedShare). Removed by #273.
    Violation {
        table: "qbit_prism_jobs",
        column: "payload",
        phase: PHASE_REFRESH,
    },
    // 2 window copies (bundle.shares + bundle.reward_manifest.shares). Removed by #265.
    Violation {
        table: "qbit_block_candidate_outbox",
        column: "candidate",
        phase: PHASE_ENQUEUE,
    },
    // 1 window copy: reward_manifest.shares survives the `remove("shares")`. Removed by #267.
    Violation {
        table: "qbit_pool_audit_bundles",
        column: "audit_bundle",
        phase: PHASE_LANDING,
    },
    // 2 window copies: the legacy import writes the full inline body. Removed by #265.
    Violation {
        table: "qbit_pool_audit_bundles",
        column: "audit_bundle",
        phase: PHASE_IMPORT,
    },
];

/// Phases the gate is knowingly unable to drive. Empty at this base commit:
/// every phase runs against the in-process fake node and the ledger API, so
/// none of them needs `QBITD_BIN`. An `unreached` phase that is not listed
/// here fails the gate rather than reading as zero bytes.
const UNREACHED_PHASES: &[&str] = &[];

// ---------------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------------

#[cfg(unix)]
fn report_sink() -> Option<std::sync::Mutex<std::fs::File>> {
    use std::os::fd::AsFd;
    // A *duplicate* of the process's real stderr. It shares the open file
    // description, and therefore the file offset, with whatever the harness
    // inherited, so the report interleaves correctly with the harness's own
    // output. Reopening `/dev/stderr` instead would start a second offset at
    // byte zero and overwrite a redirected log.
    std::io::stderr()
        .as_fd()
        .try_clone_to_owned()
        .ok()
        .map(|fd| std::sync::Mutex::new(std::fs::File::from(fd)))
}

#[cfg(not(unix))]
fn report_sink() -> Option<std::sync::Mutex<std::fs::File>> {
    None
}

/// libtest captures `println!` for passing tests, but the ratchet report has to
/// be readable in CI logs exactly when the gate passes. Writing through a
/// duplicated stderr descriptor bypasses that capture.
fn emit(line: &str) {
    use std::io::Write;
    static SINK: std::sync::OnceLock<Option<std::sync::Mutex<std::fs::File>>> =
        std::sync::OnceLock::new();
    match SINK.get_or_init(report_sink) {
        Some(file) => {
            if let Ok(mut file) = file.lock() {
                let _ = writeln!(file, "{line}");
            }
        }
        None => println!("{line}"),
    }
}

macro_rules! report {
    ($($arg:tt)*) => { emit(&format!($($arg)*)) };
}

// ---------------------------------------------------------------------------
// Settings (EP-CONFIG / EP-VALIDATION)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Setting {
    name: &'static str,
    value: u64,
    from_env: bool,
}

impl Setting {
    fn source(&self) -> &'static str {
        if self.from_env {
            "env"
        } else {
            "default"
        }
    }
}

/// Read one numeric setting from the real entry point. Missing means the
/// default; present-but-empty, non-numeric, negative, zero and out-of-range all
/// fail loudly instead of falling back.
fn setting(name: &'static str, default: u64, min: u64, max: u64) -> Result<Setting> {
    let raw = match std::env::var(name) {
        Ok(raw) => Some(raw),
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => bail!("{name} is not readable: {error}"),
    };
    parse_setting(name, raw.as_deref(), default, min, max)
}

fn parse_setting(
    name: &'static str,
    raw: Option<&str>,
    default: u64,
    min: u64,
    max: u64,
) -> Result<Setting> {
    let Some(raw) = raw else {
        return Ok(Setting {
            name,
            value: default,
            from_env: false,
        });
    };
    let trimmed = raw.trim();
    ensure!(
        !trimmed.is_empty(),
        "{name} is set but empty; unset it to use the default of {default}"
    );
    let value: u64 = trimmed.parse().with_context(|| {
        format!("{name}={raw:?} is not a non-negative integer (expected {min}..={max})")
    })?;
    ensure!(
        (min..=max).contains(&value),
        "{name}={value} is out of range; expected {min}..={max}"
    );
    Ok(Setting {
        name,
        value,
        from_env: true,
    })
}

#[derive(Clone, Debug)]
struct GateSettings {
    target: Setting,
    n1: Setting,
    n2: Setting,
    statement_timeout_ms: Setting,
}

impl GateSettings {
    fn load() -> Result<Self> {
        let max_shares = u64::try_from(WINDOW_WEIGHT)?;
        let settings = Self {
            target: setting("PRISM_JSONB_GATE_TARGET_SHARES", 400_000, 1, max_shares)?,
            n1: setting("PRISM_JSONB_GATE_N1", 5_000, 1, max_shares)?,
            n2: setting("PRISM_JSONB_GATE_N2", 20_000, 1, max_shares)?,
            // `Ledger::connect` caps this at 600000 ms itself.
            statement_timeout_ms: setting(
                "PRISM_JSONB_GATE_STATEMENT_TIMEOUT_MS",
                600_000,
                1,
                600_000,
            )?,
        };
        settings.cross_check()?;
        Ok(settings)
    }

    /// `0 < n1 < n2 <= target`, and every size has to be one the fixture can
    /// fill exactly. Checked at the entry point, not deep inside a run.
    fn cross_check(&self) -> Result<()> {
        ensure!(
            self.n1.value < self.n2.value,
            "{}={} must be smaller than {}={}",
            self.n1.name,
            self.n1.value,
            self.n2.name,
            self.n2.value
        );
        ensure!(
            self.n2.value <= self.target.value,
            "{}={} must not exceed {}={}",
            self.n2.name,
            self.n2.value,
            self.target.name,
            self.target.value
        );
        for size in [&self.n1, &self.n2, &self.target] {
            WindowPlan::new(size.value).with_context(|| format!("{}={}", size.name, size.value))?;
        }
        Ok(())
    }

    fn describe(&self) -> String {
        let mut text = String::new();
        for entry in [&self.target, &self.n1, &self.n2, &self.statement_timeout_ms] {
            let _ = writeln!(
                text,
                "  {} = {} ({})",
                entry.name,
                entry.value,
                entry.source()
            );
        }
        text
    }

    /// The value the gate validates has to be the value it uses: exporting it
    /// here, before any `Ledger::connect`, is the only way a per-connection
    /// `statement_timeout` reaches the pool (`ledger.rs`, `timeout_setting`).
    fn apply_statement_timeout(&self) {
        std::env::set_var(
            "PRISM_DATABASE_STATEMENT_TIMEOUT_MS",
            self.statement_timeout_ms.value.to_string(),
        );
    }
}

const BASELINE_SIZES_VAR: &str = "PRISM_JSONB_GATE_BASELINE_SIZES";
const DEFAULT_BASELINE_SIZES: &str = "50000,100000,200000";

/// Read the sweep's sizes from the real entry point. Only a missing variable
/// takes the default; one that is not valid Unicode fails like every other
/// setting.
fn baseline_sizes() -> Result<Vec<u64>> {
    let raw = match std::env::var(BASELINE_SIZES_VAR) {
        Ok(raw) => Some(raw),
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => bail!("{BASELINE_SIZES_VAR} is not readable: {error}"),
    };
    parse_baseline_sizes(raw.as_deref())
}

/// Every entry must be a share count the fixture can fill exactly (it divides
/// the 8,000,000 window weight, so it is at most that), the list must be
/// strictly increasing, and a fit needs at least two entries.
fn parse_baseline_sizes(raw: Option<&str>) -> Result<Vec<u64>> {
    let raw = raw.unwrap_or(DEFAULT_BASELINE_SIZES);
    let max = u64::try_from(WINDOW_WEIGHT)?;
    let mut sizes: Vec<u64> = Vec::new();
    for piece in raw.split(',') {
        let piece = piece.trim();
        ensure!(
            !piece.is_empty(),
            "{BASELINE_SIZES_VAR}={raw:?} has an empty entry"
        );
        let size: u64 = piece.parse().with_context(|| {
            format!("{BASELINE_SIZES_VAR}={raw:?}: entry {piece:?} is not a non-negative integer")
        })?;
        ensure!(
            (1..=max).contains(&size),
            "{BASELINE_SIZES_VAR}={raw:?}: entry {size} is out of range; expected 1..={max}"
        );
        WindowPlan::new(size)
            .with_context(|| format!("{BASELINE_SIZES_VAR}={raw:?}: entry {size}"))?;
        if let Some(&previous) = sizes.last() {
            ensure!(
                size > previous,
                "{BASELINE_SIZES_VAR}={raw:?} must be strictly increasing; {size} follows \
                 {previous}"
            );
        }
        sizes.push(size);
    }
    ensure!(
        sizes.len() >= 2,
        "{BASELINE_SIZES_VAR}={raw:?} needs at least two sizes to fit a line"
    );
    Ok(sizes)
}

/// Trap 3: existing native tests return early when the URL is missing. This one
/// must never do that silently, and must never do it at all in CI.
fn database_url() -> Result<Option<String>> {
    let url = std::env::var("PRISM_TEST_DATABASE_URL")
        .ok()
        .filter(|value| !value.trim().is_empty());
    if url.is_some() {
        return Ok(url);
    }
    let ci = std::env::var("CI").ok().filter(|value| !value.is_empty());
    if ci.is_some() {
        bail!(
            "PRISM_TEST_DATABASE_URL is unset or empty while CI is set. The JSONB ceiling gate \
             must not silently skip in CI: point it at a PostgreSQL 16 instance."
        );
    }
    report!(
        "SKIPPED: jsonb_ceiling_gate did not run. Set PRISM_TEST_DATABASE_URL to a PostgreSQL 16 \
         instance (CI is unset, so skipping is allowed here)."
    );
    Ok(None)
}

// ---------------------------------------------------------------------------
// Per-test schema
// ---------------------------------------------------------------------------

struct Database {
    admin: PgPool,
    schema: String,
    url: String,
}

impl Database {
    async fn open(raw: &str) -> Result<Self> {
        let admin = PgPool::connect(raw).await?;
        let schema = format!("prism_jsonb_gate_{}", uuid::Uuid::new_v4().simple());
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

    /// EP-ERRORS: the schema goes away on success and on failure alike.
    async fn close(self, ledgers: Vec<&Ledger>) -> Result<()> {
        for ledger in ledgers {
            ledger.pool.close().await;
        }
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await?;
        self.admin.close().await;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// JSONB discovery, measurement and phase attribution
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct WriteKey {
    table: String,
    column: String,
    phase: &'static str,
}

/// One measured row of one JSONB column.
#[derive(Clone, Debug)]
struct RowMeasure {
    xmin: String,
    /// `pg_column_size(col)`: what the row occupies, TOAST-compressed.
    stored: i64,
    /// `octet_length(col::text)`: the JSON text form.
    text_len: i64,
    /// `pg_column_size(col::text::jsonb)`: the uncompressed JSONB container.
    uncompressed: i64,
    digest: String,
    /// The gate wrote this value itself in place of a rejected production write.
    substitute: bool,
}

/// Who owns the JSONB values `Inventory::observe` finds new or changed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Observe {
    /// Setup the gate did itself (schema, fixture, legacy reshaping): cached,
    /// never attributed to a phase.
    Baseline,
    /// The production write of this phase.
    Phase(&'static str),
    /// A row the gate wrote itself after PostgreSQL rejected this phase's
    /// production write. It is kept apart and never enters the ratchet.
    Substitute(&'static str),
}

impl Observe {
    fn phase(self) -> Option<&'static str> {
        match self {
            Self::Baseline => None,
            Self::Phase(phase) | Self::Substitute(phase) => Some(phase),
        }
    }

    fn label(self) -> String {
        match self {
            Self::Baseline => "baseline".into(),
            Self::Phase(phase) => phase.into(),
            Self::Substitute(phase) => format!("{phase} (gate-written substitute)"),
        }
    }
}

/// The largest value a phase wrote into one column.
#[derive(Clone, Debug)]
struct PhaseWrite {
    rows: usize,
    stored: i64,
    text_len: i64,
    uncompressed: i64,
}

struct Inventory {
    schema: String,
    columns: Vec<(String, String)>,
    row_key: HashMap<String, String>,
    keyless: BTreeSet<String>,
    cache: HashMap<(String, String), HashMap<String, RowMeasure>>,
    /// The attribution trap: tuples rewritten by a phase whose JSONB value did
    /// not change (the claim UPDATE carries the old TOAST pointer forward).
    rewritten_unchanged: Vec<String>,
}

impl Inventory {
    async fn discover(pool: &PgPool, schema: &str) -> Result<Self> {
        // Never hard-code the column list: a new table must be measured too.
        let columns: Vec<(String, String)> = sqlx::query(
            "SELECT c.table_name,c.column_name FROM information_schema.columns c \
             JOIN information_schema.tables t \
               ON t.table_schema=c.table_schema AND t.table_name=c.table_name \
             WHERE c.table_schema=$1 AND c.data_type='jsonb' AND t.table_type='BASE TABLE' \
             ORDER BY c.table_name,c.column_name",
        )
        .bind(schema)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|row| -> Result<(String, String)> {
            Ok((row.try_get("table_name")?, row.try_get("column_name")?))
        })
        .collect::<Result<_>>()?;
        ensure!(
            columns.len() >= EXPECTED_JSONB_COLUMNS,
            "discovered only {} JSONB columns in {schema}; expected at least \
             {EXPECTED_JSONB_COLUMNS}. Was the schema initialized through Ledger::connect?",
            columns.len()
        );
        let mut row_key = HashMap::new();
        let mut keyless = BTreeSet::new();
        for (table, _) in &columns {
            if row_key.contains_key(table) {
                continue;
            }
            let pk: Vec<String> = sqlx::query(
                "SELECT a.attname FROM pg_index i \
                 JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey) \
                 WHERE i.indrelid=format('%I.%I',$1,$2)::regclass AND i.indisprimary \
                 ORDER BY a.attnum",
            )
            .bind(schema)
            .bind(table)
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|row| row.try_get::<String, _>("attname"))
            .collect::<Result<_, _>>()?;
            let expression = if pk.is_empty() {
                keyless.insert(table.clone());
                "t.ctid::text".to_owned()
            } else {
                format!(
                    "concat_ws('|',{})",
                    pk.iter()
                        .map(|column| format!("t.\"{column}\"::text"))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            };
            row_key.insert(table.clone(), expression);
        }
        Ok(Self {
            schema: schema.to_owned(),
            columns,
            row_key,
            keyless,
            cache: HashMap::new(),
            rewritten_unchanged: Vec::new(),
        })
    }

    /// Cheap pass: stored size and tuple version for every non-null value.
    /// `pg_column_size` on a TOAST pointer does not detoast, so this is safe to
    /// run after every phase even at 400,000 shares.
    async fn probe(
        &self,
        pool: &PgPool,
        table: &str,
        column: &str,
    ) -> Result<Vec<(String, String, i64)>> {
        let key = &self.row_key[table];
        let sql = format!(
            "SELECT {key} AS row_key,t.xmin::text AS xmin,\
             pg_column_size(t.\"{column}\") AS stored \
             FROM \"{}\".\"{table}\" t WHERE t.\"{column}\" IS NOT NULL",
            self.schema
        );
        sqlx::query(&sql)
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|row| -> Result<(String, String, i64)> {
                Ok((
                    row.try_get("row_key")?,
                    row.try_get("xmin")?,
                    i64::from(row.try_get::<i32, _>("stored")?),
                ))
            })
            .collect()
    }

    /// Expensive pass, only for rows the probe says may have changed.
    ///
    /// `pg_column_size(col)` reports the compressed on-disk size, which is not
    /// what PostgreSQL compares against its 268,435,455-byte ceiling. Casting
    /// through `text` back to `jsonb` builds a fresh in-memory datum, and a
    /// computed datum is never TOAST-compressed, so its `pg_column_size` is the
    /// uncompressed container size the limit actually applies to.
    async fn measure(
        &self,
        pool: &PgPool,
        table: &str,
        column: &str,
        rows: &[String],
    ) -> Result<HashMap<String, (i64, i64, String)>> {
        if rows.is_empty() {
            return Ok(HashMap::new());
        }
        let key = &self.row_key[table];
        let sql = format!(
            "SELECT {key} AS row_key,octet_length(t.\"{column}\"::text)::bigint AS text_len,\
             pg_column_size(t.\"{column}\"::text::jsonb) AS uncompressed,\
             md5(t.\"{column}\"::text) AS digest \
             FROM \"{}\".\"{table}\" t \
             WHERE t.\"{column}\" IS NOT NULL AND ({key})=ANY($1)",
            self.schema
        );
        sqlx::query(&sql)
            .bind(rows)
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|row| -> Result<(String, (i64, i64, String))> {
                Ok((
                    row.try_get("row_key")?,
                    (
                        row.try_get("text_len")?,
                        i64::from(row.try_get::<i32, _>("uncompressed")?),
                        row.try_get("digest")?,
                    ),
                ))
            })
            .collect()
    }

    /// Refresh the cache and return the values this phase actually wrote.
    ///
    /// A value belongs to a phase when its row is new, or when its value
    /// changed. Attributing by `xmin` alone would blame the claim UPDATE for
    /// the candidate written at enqueue, because that UPDATE touches only the
    /// lease columns and the new tuple version carries the same TOAST pointer.
    async fn observe(
        &mut self,
        pool: &PgPool,
        mode: Observe,
    ) -> Result<BTreeMap<WriteKey, PhaseWrite>> {
        let mut written = BTreeMap::new();
        for (table, column) in self.columns.clone() {
            let probed = self.probe(pool, &table, &column).await?;
            let cached = self
                .cache
                .entry((table.clone(), column.clone()))
                .or_default();
            let mut stale = Vec::new();
            for (row_key, xmin, stored) in &probed {
                match cached.get(row_key) {
                    Some(previous) if previous.stored == *stored && previous.xmin == *xmin => {}
                    _ => stale.push(row_key.clone()),
                }
            }
            let fresh = self.measure(pool, &table, &column, &stale).await?;
            let cached = self
                .cache
                .get_mut(&(table.clone(), column.clone()))
                .expect("cached");
            let mut attributed: Vec<RowMeasure> = Vec::new();
            let mut next: HashMap<String, RowMeasure> = HashMap::new();
            for (row_key, xmin, stored) in probed {
                let previous = cached.get(&row_key).cloned();
                let mut measure = match fresh.get(&row_key) {
                    Some((text_len, uncompressed, digest)) => RowMeasure {
                        xmin: xmin.clone(),
                        stored,
                        text_len: *text_len,
                        uncompressed: *uncompressed,
                        digest: digest.clone(),
                        substitute: false,
                    },
                    None => {
                        let previous = previous
                            .clone()
                            .context("probe reported an unchanged row that was never measured")?;
                        RowMeasure {
                            xmin: xmin.clone(),
                            stored,
                            ..previous
                        }
                    }
                };
                let changed = previous
                    .as_ref()
                    .is_none_or(|previous| previous.digest != measure.digest);
                // A value keeps its owner until a later write replaces it.
                measure.substitute = if changed {
                    matches!(mode, Observe::Substitute(_))
                } else {
                    previous
                        .as_ref()
                        .is_some_and(|previous| previous.substitute)
                };
                match &previous {
                    _ if changed => attributed.push(measure.clone()),
                    Some(previous) if previous.xmin != measure.xmin => {
                        self.rewritten_unchanged.push(format!(
                            "{table}.{column} row {row_key}{} rewritten by phase {} \
                             (xmin {} -> {}) with an unchanged {} B value",
                            if measure.substitute {
                                " (gate-written substitute, not a production write)"
                            } else {
                                ""
                            },
                            mode.label(),
                            previous.xmin,
                            measure.xmin,
                            measure.uncompressed
                        ));
                    }
                    _ => {}
                }
                next.insert(row_key, measure);
            }
            *cached = next;
            let (Some(phase), false) = (mode.phase(), attributed.is_empty()) else {
                continue;
            };
            written.insert(
                WriteKey {
                    table: table.clone(),
                    column: column.clone(),
                    phase,
                },
                PhaseWrite {
                    rows: attributed.len(),
                    stored: attributed.iter().map(|m| m.stored).max().unwrap_or(0),
                    text_len: attributed.iter().map(|m| m.text_len).max().unwrap_or(0),
                    uncompressed: attributed.iter().map(|m| m.uncompressed).max().unwrap_or(0),
                },
            );
        }
        Ok(written)
    }
}

// ---------------------------------------------------------------------------
// Phase bookkeeping
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PhaseStatus {
    Ran,
    /// PostgreSQL refused the write with its JSONB ceiling error.
    Rejected,
    Unreached,
}

impl PhaseStatus {
    fn label(self) -> &'static str {
        match self {
            Self::Ran => "ran",
            Self::Rejected => "rejected",
            Self::Unreached => "unreached",
        }
    }
}

#[derive(Clone, Debug)]
struct PhaseStat {
    name: &'static str,
    status: PhaseStatus,
    seconds: f64,
    wal_bytes: Option<i64>,
    /// `VmHWM` of the test process for this phase alone: `clear_refs` resets
    /// the high-water mark when the phase starts. It covers the gate and the
    /// native library, never the PostgreSQL backend. `None` when this OS has
    /// no resettable `VmHWM`; unknown is never 0.
    peak_rss_kib: Option<u64>,
    note: String,
}

#[derive(Debug)]
struct Pipeline {
    n: u64,
    writes: BTreeMap<WriteKey, PhaseWrite>,
    rejections: BTreeMap<WriteKey, String>,
    /// Rows the gate wrote itself in place of a rejected production write.
    /// Reported under their own label and never fed to the ratchet.
    substitutes: BTreeMap<WriteKey, PhaseWrite>,
    phases: Vec<PhaseStat>,
    canonical_audit_bytes: Option<i64>,
    average_share_bytes: f64,
    load_seconds: f64,
    load_rows_per_second: f64,
    /// The largest per-phase peak; `None` when any phase's peak is unknown.
    peak_rss_kib: Option<u64>,
    seconds: f64,
    ratio_probe: Option<(String, String, i64, i64, i64)>,
    rewritten_unchanged: Vec<String>,
    jsonb_columns: usize,
}

/// A write PostgreSQL refused because of the JSONB container ceiling is a
/// result, not a crash. Anything else is a real failure and is propagated.
fn ceiling_rejection(error: &anyhow::Error) -> Option<String> {
    let text = format!("{error:#}");
    (text.contains("jsonb") && text.contains("exceeds the maximum of")).then_some(text)
}

/// Printed wherever a peak RSS could not be measured, in place of a number.
const RSS_NOT_MEASURED: &str = "not measured on this OS";

/// `VmHWM` of the test process since the matching `reset_peak_rss`. `None`
/// when that reset failed, so a reading would span earlier phases too, or when
/// this OS has no `/proc/self/status` `VmHWM` (macOS). Unknown is never 0.
fn peak_rss_kib(reset: bool) -> Option<u64> {
    if !reset {
        return None;
    }
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

/// Writing `5` to `clear_refs` resets `VmHWM`, so the next reading covers only
/// the phase that follows. It measures the *test process*, not the PostgreSQL
/// backend. Returns whether the reset took.
fn reset_peak_rss() -> bool {
    std::fs::write("/proc/self/clear_refs", "5").is_ok()
}

fn rss_cell(kib: Option<u64>) -> String {
    kib.map_or_else(|| RSS_NOT_MEASURED.to_owned(), |kib| format!("{kib} KiB"))
}

/// One row of the per-phase table. WAL and peak RSS carry their units, and an
/// unknown value is named rather than printed as a number.
fn phase_line(phase: &PhaseStat) -> String {
    format!(
        "  {:<9} {:<9} {:>8.2} s  WAL {:>14}  peak RSS {:>23}  {}",
        phase.name,
        phase.status.label(),
        phase.seconds,
        phase
            .wal_bytes
            .map_or_else(|| "unavailable".to_owned(), |bytes| format!("{bytes} B")),
        rss_cell(phase.peak_rss_kib),
        phase.note
    )
}

async fn wal_lsn(pool: &PgPool) -> Option<String> {
    sqlx::query_scalar::<_, String>("SELECT pg_current_wal_lsn()::text")
        .fetch_one(pool)
        .await
        .ok()
}

async fn wal_since(pool: &PgPool, before: &Option<String>) -> Option<i64> {
    let before = before.as_ref()?;
    sqlx::query_scalar::<_, i64>("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),$1::pg_lsn)::bigint")
        .bind(before)
        .fetch_one(pool)
        .await
        .ok()
}

// ---------------------------------------------------------------------------
// Candidate construction
// ---------------------------------------------------------------------------

fn keys(
    config: &qbit_prism_server::config::Config,
) -> Result<(ManifestSigningKey, ManifestSigningKey)> {
    Ok((
        ManifestSigningKey::from_seed_hex(&config.manifest_seed)?,
        ManifestSigningKey::from_seed_hex(&config.ledger_seed)?,
    ))
}

/// Build the bundle the refresh phase would build for this snapshot, with the
/// same keys, coinbase suffix and found-block metadata the coordinator uses
/// (`Coordinator::build_bundle`). Production embeds exactly this bundle in the
/// candidate it enqueues.
fn build_window_bundle(
    snapshot: &Snapshot,
    config: &qbit_prism_server::config::Config,
) -> Result<AuditBundle> {
    let (manifest_key, ledger_key) = keys(config)?;
    Ok(
        qbit_prism::build_audit_bundle_with_coinbase_script_sig_suffix(
            snapshot.shares.clone(),
            FoundBlock {
                block_height: 101,
                coinbase_value_sats: 5_000_000_000,
                network_difficulty: FAKE_NODE_NETWORK_DIFFICULTY,
                anchor_job_issued_at_ms: snapshot.anchor_ms,
            },
            snapshot.prior_balances.clone(),
            PayoutPolicy::day_one_default(),
            Some(coinbase_suffix(config)),
            &manifest_key,
            &ledger_key,
        )?,
    )
}

/// The `candidate_with_bundle` recipe from `tests/ledger_postgres.rs`: an
/// 80-byte header whose double SHA-256 is the candidate's `block_hash`, with
/// the verified coinbase transaction as the block's first transaction.
fn candidate_with_bundle(
    bundle: AuditBundle,
    payout_revision: i64,
    ledger_public_key: &str,
    nonce: u32,
) -> Result<(Candidate, AuditVerificationReport)> {
    let report =
        qbit_prism::verify_audit_bundle_with_ledger_public_key(&bundle, ledger_public_key)?;
    let mut block = vec![0u8; 80];
    block[..4].copy_from_slice(&0x2000_0000u32.to_le_bytes());
    block[4..36].fill(0x22);
    let mut txid = hex::decode(&report.coinbase_txid)?;
    txid.reverse();
    block[36..68].copy_from_slice(&txid);
    block[68..72].copy_from_slice(&1_800_000_000u32.to_le_bytes());
    block[72..76].copy_from_slice(&0x207f_ffffu32.to_le_bytes());
    block[76..80].copy_from_slice(&nonce.to_le_bytes());
    let mut hash = Sha256::digest(Sha256::digest(&block)).to_vec();
    hash.reverse();
    block.push(1);
    block.extend(hex::decode(&report.coinbase_tx_hex)?);
    Ok((
        Candidate {
            block_hash: hex::encode(hash),
            block_hex: hex::encode(block),
            job_id: "jsonb-gate-job".into(),
            payout_revision,
            bundle,
            deferred_share: None,
            coinbase_suffix_hex: None,
        },
        report,
    ))
}

/// The share the enqueue phase commits together with its candidate, exactly as
/// `Coordinator::process_candidate` does for a block-winning submission.
fn winning_share(plan: &WindowPlan) -> AcceptedShare {
    let mut share = plan.share(plan.share_count());
    share.share_seq = 0;
    share.share_id = "jsonb-gate:winning-share".into();
    share.job_issued_at_ms = 1;
    share.accepted_at_ms = 0;
    share
}

// ---------------------------------------------------------------------------
// The pipeline
// ---------------------------------------------------------------------------

async fn run_pipeline(raw_url: &str, n: u64, settings: &GateSettings) -> Result<Pipeline> {
    let db = Database::open(raw_url).await?;
    let node = FakeNode::open().await?;
    let config = coordinator_config(db.url.clone(), &node, "jsonb-gate")?;
    let coordinator = match Coordinator::new(config).await {
        Ok(coordinator) => coordinator,
        Err(error) => {
            let _ = db.close(Vec::new()).await;
            return Err(error);
        }
    };
    let outcome = pipeline_body(&db, &coordinator, n, settings).await;
    // EP-ERRORS: clean up on the failure path too, and keep the original error.
    let closed = db.close(vec![&coordinator.ledger]).await;
    match outcome {
        Ok(pipeline) => closed.map(|()| pipeline),
        Err(error) => {
            if let Err(cleanup) = closed {
                report!("cleanup after a failed pipeline also failed: {cleanup:#}");
            }
            Err(error)
        }
    }
}

async fn pipeline_body(
    db: &Database,
    coordinator: &std::sync::Arc<Coordinator>,
    n: u64,
    settings: &GateSettings,
) -> Result<Pipeline> {
    let started = Instant::now();
    let ledger = &coordinator.ledger;
    let pool = ledger.pool.clone();
    let config = coordinator.config.clone();
    let plan = WindowPlan::new(n)?;
    let mut inventory = Inventory::discover(&pool, &db.schema).await?;
    let jsonb_columns = inventory.columns.len();
    report!(
        "[n={n}] schema {} carries {jsonb_columns} JSONB columns; \
         share_difficulty={} statement_timeout={} ms",
        db.schema,
        plan.share_difficulty(),
        settings.statement_timeout_ms.value
    );
    if !inventory.keyless.is_empty() {
        report!(
            "[n={n}] note: {:?} have no primary key; attribution there falls back to ctid",
            inventory.keyless
        );
    }
    // Baseline: everything Ledger::connect and Coordinator::new already wrote.
    inventory.observe(&pool, Observe::Baseline).await?;

    let load = plan.load(&pool, "jsonb-gate").await?;
    plan.verify_round_trip(&pool, &[1, n.div_ceil(2), n])
        .await?;
    let average_share_bytes = plan.average_share_bytes()?;
    report!(
        "[n={n}] loaded {} shares in {:.2} s ({:.0} rows/s, {:.1} B/share native, {:.1} MiB)",
        load.rows,
        load.seconds,
        load.rows_per_second,
        average_share_bytes,
        load.serialized_bytes as f64 / 1_048_576.0
    );
    // The fixture load is not a phase; fold its writes into the baseline.
    inventory.observe(&pool, Observe::Baseline).await?;

    let mut writes: BTreeMap<WriteKey, PhaseWrite> = BTreeMap::new();
    let mut rejections: BTreeMap<WriteKey, String> = BTreeMap::new();
    let mut phases: Vec<PhaseStat> = Vec::new();

    // --- phase: refresh --------------------------------------------------
    let rss_reset = reset_peak_rss();
    let before = wal_lsn(&pool).await;
    let clock = Instant::now();
    let refresh = coordinator.refresh_once().await;
    let seconds = clock.elapsed().as_secs_f64();
    let wal = wal_since(&pool, &before).await;
    let peak_rss = peak_rss_kib(rss_reset);
    // The note for a refused refresh is the error PostgreSQL returned for this
    // write, taken here rather than looked up later, so it can never be empty.
    let refused = match &refresh {
        Ok(()) => None,
        Err(error) => match ceiling_rejection(error) {
            Some(text) => {
                rejections.insert(
                    WriteKey {
                        table: "qbit_prism_jobs".into(),
                        column: "payload".into(),
                        phase: PHASE_REFRESH,
                    },
                    text.clone(),
                );
                Some(text)
            }
            None => return Err(refresh.unwrap_err()).context("refresh phase failed"),
        },
    };
    let status = if refused.is_some() {
        PhaseStatus::Rejected
    } else {
        PhaseStatus::Ran
    };
    let note = if let Some(text) = refused {
        format!("PostgreSQL refused the prepared job: {text}")
    } else {
        let prepared = coordinator.prepared.read().await;
        let prepared = prepared
            .as_ref()
            .context("refresh produced no prepared job")?;
        ensure!(
            prepared.snapshot.shares.len() as u64 == n,
            "refresh saw a {}-share window, expected exactly {n}",
            prepared.snapshot.shares.len()
        );
        format!(
            "prepared job {} with a {n}-share window",
            prepared.storage_key
        )
    };
    writes.append(
        &mut inventory
            .observe(&pool, Observe::Phase(PHASE_REFRESH))
            .await?,
    );
    phases.push(PhaseStat {
        name: PHASE_REFRESH,
        status,
        seconds,
        wal_bytes: wal,
        peak_rss_kib: peak_rss,
        note,
    });
    report!("[n={n}] refresh: {} in {seconds:.2} s", status.label());

    // The candidate the enqueue phase commits carries the full job bundle, as
    // production does. Building it here keeps every later phase reachable even
    // when the refresh write was rejected.
    let snapshot = ledger.snapshot(plan.window_network_difficulty()).await?;
    ensure!(
        snapshot.shares.len() as u64 == n,
        "snapshot window is {} shares, expected exactly {n}",
        snapshot.shares.len()
    );
    let payout_revision = snapshot.payout_revision;
    let bundle = build_window_bundle(&snapshot, &config)?;
    // The window now lives in the bundle; a second copy of 400,000 shares is
    // pure peak RSS.
    drop(snapshot);
    let (candidate, report) =
        candidate_with_bundle(bundle, payout_revision, &config.ledger_public_key, 0x0264)?;
    // Bundle construction touches no JSONB column; keep the cache in step.
    inventory.observe(&pool, Observe::Baseline).await?;

    // --- phase: enqueue --------------------------------------------------
    let rss_reset = reset_peak_rss();
    let before = wal_lsn(&pool).await;
    let clock = Instant::now();
    let enqueued = ledger
        .append(winning_share(&plan), Some(candidate.clone()))
        .await
        .map(|_| ());
    let seconds = clock.elapsed().as_secs_f64();
    let wal = wal_since(&pool, &before).await;
    let peak_rss = peak_rss_kib(rss_reset);
    let status = match &enqueued {
        Ok(()) => PhaseStatus::Ran,
        Err(error) => match ceiling_rejection(error) {
            Some(text) => {
                rejections.insert(
                    WriteKey {
                        table: "qbit_block_candidate_outbox".into(),
                        column: "candidate".into(),
                        phase: PHASE_ENQUEUE,
                    },
                    text,
                );
                PhaseStatus::Rejected
            }
            None => return Err(enqueued.unwrap_err()).context("enqueue phase failed"),
        },
    };
    // Observed before any substitute exists, so only production writes are
    // attributed to enqueue.
    writes.append(
        &mut inventory
            .observe(&pool, Observe::Phase(PHASE_ENQUEUE))
            .await?,
    );
    let mut substitutes: BTreeMap<WriteKey, PhaseWrite> = BTreeMap::new();
    let note = if status == PhaseStatus::Rejected {
        scaffold_outbox(&pool, &candidate).await?;
        substitutes = inventory
            .observe(&pool, Observe::Substitute(PHASE_ENQUEUE))
            .await?;
        let [(key, substitute)] = substitutes.iter().collect::<Vec<_>>()[..] else {
            bail!(
                "the substitute outbox row wrote {} JSONB values, expected exactly 1",
                substitutes.len()
            );
        };
        format!(
            "PostgreSQL rejected the candidate; SUBSTITUTE, not the enqueue write: the gate \
             wrote a window-free {}.{} row itself ({} B uncompressed) so the claim, landing \
             and import writes stay measurable",
            key.table, key.column, substitute.uncompressed
        )
    } else {
        format!(
            "candidate {} committed with its share",
            candidate.block_hash
        )
    };
    phases.push(PhaseStat {
        name: PHASE_ENQUEUE,
        status,
        seconds,
        wal_bytes: wal,
        peak_rss_kib: peak_rss,
        note,
    });
    report!("[n={n}] enqueue: {} in {seconds:.2} s", status.label());

    // --- phase: claim ----------------------------------------------------
    // Without its own reset, claim's peak would include enqueue's.
    let rss_reset = reset_peak_rss();
    let before = wal_lsn(&pool).await;
    let clock = Instant::now();
    let claimed = ledger.claim_candidate(600).await?;
    let seconds = clock.elapsed().as_secs_f64();
    let wal = wal_since(&pool, &before).await;
    let peak_rss = peak_rss_kib(rss_reset);
    let claimed = claimed.context("claim phase found no pending candidate")?;
    ensure!(
        claimed.candidate.block_hash == candidate.block_hash,
        "claim returned a different candidate"
    );
    // The claim UPDATE never rewrites `candidate`; the in-memory bundle is the
    // authority for the landing write, exactly as it is in production. Moving
    // the gate's own candidate in here drops the copy the claim deserialized.
    let block_hash = candidate.block_hash.clone();
    let claim = CandidateClaim {
        candidate,
        claim_token: claimed.claim_token,
    };
    drop(claimed.candidate);
    writes.append(
        &mut inventory
            .observe(&pool, Observe::Phase(PHASE_CLAIM))
            .await?,
    );
    phases.push(PhaseStat {
        name: PHASE_CLAIM,
        status: PhaseStatus::Ran,
        seconds,
        wal_bytes: wal,
        peak_rss_kib: peak_rss,
        note: if substitutes.is_empty() {
            "lease columns only; no JSONB value is written".into()
        } else {
            "lease columns only; no JSONB value is written. It claimed the gate-written \
             SUBSTITUTE outbox row, so its seconds, WAL and peak RSS describe claiming the \
             substitute, not a production candidate"
                .into()
        },
    });
    report!("[n={n}] claim: ran in {seconds:.2} s");

    // --- phase: landing --------------------------------------------------
    let rss_reset = reset_peak_rss();
    let before = wal_lsn(&pool).await;
    let clock = Instant::now();
    let landed = async {
        ledger
            .land_candidate(&claim, &config.ledger_public_key)
            .await?;
        ledger.finish_candidate(&claim, true, None).await
    }
    .await;
    let seconds = clock.elapsed().as_secs_f64();
    let wal = wal_since(&pool, &before).await;
    let peak_rss = peak_rss_kib(rss_reset);
    let status = match &landed {
        Ok(()) => PhaseStatus::Ran,
        Err(error) => match ceiling_rejection(error) {
            Some(text) => {
                rejections.insert(
                    WriteKey {
                        table: "qbit_pool_audit_bundles".into(),
                        column: "audit_bundle".into(),
                        phase: PHASE_LANDING,
                    },
                    text,
                );
                PhaseStatus::Rejected
            }
            None => return Err(landed.unwrap_err()).context("landing phase failed"),
        },
    };
    writes.append(
        &mut inventory
            .observe(&pool, Observe::Phase(PHASE_LANDING))
            .await?,
    );
    phases.push(PhaseStat {
        name: PHASE_LANDING,
        status,
        seconds,
        wal_bytes: wal,
        peak_rss_kib: peak_rss,
        note: "audit body keeps reward_manifest.shares after remove(\"shares\")".into(),
    });
    report!("[n={n}] landing: {} in {seconds:.2} s", status.label());

    // --- phase: import ---------------------------------------------------
    let directory = tempfile::tempdir()?;
    let envelope_path = directory.path().join("legacy-audit.json");
    // Landing is done, so the in-memory window has no reader left. Move it into
    // the envelope writer and drop it: at 400,000 shares the import phase
    // rebuilds the whole bundle itself, and keeping a second copy alive is pure
    // peak RSS.
    let mut bundle = claim.candidate.bundle;
    write_legacy_envelope(&envelope_path, &mut bundle, &report)?;
    drop(bundle);
    legacy_shape(&pool, &block_hash, &report, &envelope_path).await?;
    inventory.observe(&pool, Observe::Baseline).await?;
    let rss_reset = reset_peak_rss();
    let before = wal_lsn(&pool).await;
    let clock = Instant::now();
    let imported = ledger
        .import_legacy_audits(Some(directory.path()), &config.ledger_public_key)
        .await;
    let seconds = clock.elapsed().as_secs_f64();
    let wal = wal_since(&pool, &before).await;
    let peak_rss = peak_rss_kib(rss_reset);
    let status = match &imported {
        Ok(count) => {
            ensure!(
                *count == 1,
                "import wrote {count} audits, expected exactly 1"
            );
            PhaseStatus::Ran
        }
        Err(error) => match ceiling_rejection(error) {
            Some(text) => {
                rejections.insert(
                    WriteKey {
                        table: "qbit_pool_audit_bundles".into(),
                        column: "audit_bundle".into(),
                        phase: PHASE_IMPORT,
                    },
                    text,
                );
                PhaseStatus::Rejected
            }
            None => return Err(imported.unwrap_err()).context("import phase failed"),
        },
    };
    writes.append(
        &mut inventory
            .observe(&pool, Observe::Phase(PHASE_IMPORT))
            .await?,
    );
    // Report-only: bytea is not bound by the JSONB ceiling, only by the 1 GiB
    // varlena limit, so it never enters the ratchet.
    let canonical_audit_bytes: Option<i64> = sqlx::query_scalar(
        "SELECT max(octet_length(canonical_audit_bytes))::bigint FROM qbit_pool_audit_bundles",
    )
    .fetch_one(&pool)
    .await?;
    phases.push(PhaseStat {
        name: PHASE_IMPORT,
        status,
        seconds,
        wal_bytes: wal,
        peak_rss_kib: peak_rss,
        note: format!(
            "canonical_audit_bytes (bytea, report-only) max octet_length = {}",
            canonical_audit_bytes.map_or_else(
                || "not written (no row holds a value)".to_owned(),
                |bytes| format!("{bytes} B")
            )
        ),
    });
    report!("[n={n}] import: {} in {seconds:.2} s", status.label());

    // Method check: the uncompressed measurement must be close to the text
    // length and clearly larger than the TOAST-compressed stored size.
    let ratio_probe = writes
        .iter()
        .max_by_key(|(_, write)| write.uncompressed)
        .map(|(key, write)| {
            (
                key.table.clone(),
                key.column.clone(),
                write.stored,
                write.text_len,
                write.uncompressed,
            )
        });

    // One unknown phase makes the maximum unknown too.
    let peak_rss_kib = phases
        .iter()
        .map(|phase| phase.peak_rss_kib)
        .collect::<Option<Vec<u64>>>()
        .and_then(|peaks| peaks.into_iter().max());
    // EP-OBSERVABILITY: a rejected write has no size. A value attributed to
    // the same column and phase would be some other write posing as it.
    for key in rejections.keys() {
        ensure!(
            !writes.contains_key(key),
            "{} was rejected by PostgreSQL, yet a value in that column was attributed to \
             the same phase; the gate cannot tell which write produced it",
            label(key)
        );
    }
    Ok(Pipeline {
        n,
        writes,
        rejections,
        substitutes,
        phases,
        canonical_audit_bytes,
        average_share_bytes,
        load_seconds: load.seconds,
        load_rows_per_second: load.rows_per_second,
        peak_rss_kib,
        seconds: started.elapsed().as_secs_f64(),
        ratio_probe,
        rewritten_unchanged: inventory.rewritten_unchanged.clone(),
        jsonb_columns,
    })
}

/// Only used when PostgreSQL refused the production enqueue write. Writes an
/// outbox row whose candidate is the same block with an emptied window, so the
/// downstream phases still exercise their own production writes instead of
/// being reported as unreached.
async fn scaffold_outbox(pool: &PgPool, candidate: &Candidate) -> Result<()> {
    let mut lean = candidate.clone();
    lean.bundle.shares.clear();
    lean.bundle.reward_manifest.shares.clear();
    let payload = serde_json::to_value(&lean)?;
    let digest = hex::encode(Sha256::digest(serde_json::to_vec(&lean)?));
    sqlx::query(
        "INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256) \
         VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
    )
    .bind(&candidate.block_hash)
    .bind(payload)
    .bind(digest)
    .execute(pool)
    .await?;
    Ok(())
}

/// The `audit-body-ref.v1` envelope the legacy importer reads, written straight
/// to disk so a 400,000-share body never has to be buffered twice.
fn write_legacy_envelope(
    path: &std::path::Path,
    bundle: &mut AuditBundle,
    report: &AuditVerificationReport,
) -> Result<()> {
    use std::io::Write;
    // Streamed rather than built as one `serde_json::Value`: a 400,000-share
    // bundle costs gigabytes as a Value tree. `bundle_without_shares` may carry
    // an empty `shares` array; the reader overwrites that key when it splices
    // the share parts back in (`qbit-prism/src/audit_body_ref.rs`).
    let shares = std::mem::take(&mut bundle.shares);
    let count = shares.len();
    let first = shares.first().context("empty window")?.share_seq;
    let last = shares.last().context("empty window")?.share_seq;
    let mut file = std::io::BufWriter::new(std::fs::File::create(path)?);
    write!(file, "{{\"schema\":")?;
    serde_json::to_writer(&mut file, qbit_prism::AUDIT_BODY_REF_SCHEMA)?;
    write!(file, ",\"audit_bundle_sha256\":")?;
    serde_json::to_writer(&mut file, &report.audit_bundle_sha256_hex)?;
    write!(file, ",\"share_count\":{count},\"bundle_without_shares\":")?;
    serde_json::to_writer(&mut file, &*bundle)?;
    write!(
        file,
        ",\"share_parts\":[{{\"kind\":\"inline\",\"first_share_seq\":{first},\
         \"last_share_seq\":{last},\"share_count\":{count},\"shares\":"
    )?;
    serde_json::to_writer(&mut file, &shares)?;
    write!(file, "}}]}}")?;
    file.flush()?;
    Ok(())
}

/// Turn the landed row into the legacy external-body shape the importer looks
/// for, satisfying `qbit_pool_audit_bundles_body_present_check`. The row is
/// inserted rather than updated when the landing write itself was refused.
async fn legacy_shape(
    pool: &PgPool,
    block_hash: &str,
    report: &AuditVerificationReport,
    envelope: &std::path::Path,
) -> Result<()> {
    let uri = envelope.to_str().context("non-UTF-8 envelope path")?;
    sqlx::query(
        "INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,\
         payout_manifest_sha256) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING",
    )
    .bind(block_hash)
    .bind(i64::try_from(report.block_height)?)
    .bind("22".repeat(32))
    .bind(&report.coinbase_txid)
    .bind(&report.coinbase_manifest_sha256_hex)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO qbit_pool_audit_bundles(block_hash,audit_bundle,audit_bundle_sha256,\
         coinbase_tx_hex,body_uri) VALUES($1,NULL,$2,$3,$4) \
         ON CONFLICT(block_hash) DO UPDATE SET audit_bundle=NULL,share_snapshot_sha256=NULL,\
         canonical_audit_bytes=NULL,body_uri=EXCLUDED.body_uri",
    )
    .bind(block_hash)
    .bind(&report.audit_bundle_sha256_hex)
    .bind(&report.coinbase_tx_hex)
    .bind(uri)
    .execute(pool)
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Fit, projection and the ratchet
// ---------------------------------------------------------------------------

/// One size cell of the ratchet table. A rejected write has no size to show.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Size {
    Bytes(i64),
    Rejected,
    NotWritten,
}

/// What the ratchet decides a write from.
#[derive(Clone, Debug)]
enum Verdict {
    /// PostgreSQL refused the write at `n` shares; that crosses on its own.
    Rejected { n: u64, text: String },
    /// Reduced mode: projected linearly to the target from two measured sizes.
    Projected {
        slope: f64,
        intercept: f64,
        bytes: f64,
    },
    /// Full-size mode: measured at the target itself.
    Measured(i64),
}

#[derive(Clone, Debug)]
struct FitRow {
    key: WriteKey,
    /// One cell per size column of the ratchet table.
    sizes: Vec<Size>,
    verdict: Verdict,
}

impl FitRow {
    fn crosses(&self) -> bool {
        match self.verdict {
            Verdict::Rejected { .. } => true,
            Verdict::Projected { bytes, .. } => bytes > GATE_THRESHOLD as f64,
            Verdict::Measured(bytes) => bytes > GATE_THRESHOLD,
        }
    }
}

/// The production measurements of one pipeline run. Substitutes are not part
/// of it, so they can never reach the ratchet.
#[derive(Clone, Copy)]
struct Sample<'a> {
    n: u64,
    writes: &'a BTreeMap<WriteKey, PhaseWrite>,
    rejections: &'a BTreeMap<WriteKey, String>,
}

impl Sample<'_> {
    /// A rejection wins over any value found in that column: the rejected
    /// write itself never produced one.
    fn size(&self, key: &WriteKey) -> Size {
        if self.rejections.contains_key(key) {
            Size::Rejected
        } else {
            self.writes
                .get(key)
                .map_or(Size::NotWritten, |write| Size::Bytes(write.uncompressed))
        }
    }

    fn keys(&self) -> BTreeSet<WriteKey> {
        self.writes
            .keys()
            .chain(self.rejections.keys())
            .cloned()
            .collect()
    }
}

impl Pipeline {
    fn sample(&self) -> Sample<'_> {
        Sample {
            n: self.n,
            writes: &self.writes,
            rejections: &self.rejections,
        }
    }
}

fn label(key: &WriteKey) -> String {
    format!("{}.{} @ {}", key.table, key.column, key.phase)
}

/// Fit `size(n) = a + b*n` through the two reduced sizes and project to the
/// target. A rejection at either size counts as crossing on its own.
fn fit(reduced: [Sample<'_>; 2], target: u64) -> Result<Vec<FitRow>> {
    let [low, high] = reduced;
    ensure!(low.n < high.n, "n1 must be smaller than n2");
    let span = (high.n - low.n) as f64;
    let mut keys = low.keys();
    keys.extend(high.keys());
    let mut rows = Vec::new();
    for key in keys {
        // A pair with a refused write is not a pair: keep projections only for
        // sizes that were actually written, and report the refusal instead.
        let sizes = vec![low.size(&key), high.size(&key)];
        let rejected = [low, high].into_iter().find_map(|sample| {
            sample.rejections.get(&key).map(|text| Verdict::Rejected {
                n: sample.n,
                text: text.clone(),
            })
        });
        if let Some(verdict) = rejected {
            rows.push(FitRow {
                key,
                sizes,
                verdict,
            });
            continue;
        }
        // EP-OBSERVABILITY: a write seen at one size but not the other is an
        // unexplained measurement, never a zero.
        let (Size::Bytes(s1), Size::Bytes(s2)) = (sizes[0], sizes[1]) else {
            let low_measured = matches!(sizes[0], Size::Bytes(_));
            bail!(
                "{} was measured at n={} but not at n={}; the pipeline is not comparable \
                 across sizes and the gate cannot project it",
                label(&key),
                if low_measured { low.n } else { high.n },
                if low_measured { high.n } else { low.n }
            );
        };
        ensure!(
            s2 >= s1,
            "degenerate fit for {}: {s1} B at n={} but {s2} B at n={} is a negative slope",
            label(&key),
            low.n,
            high.n
        );
        let slope = (s2 - s1) as f64 / span;
        let intercept = s1 as f64 - slope * low.n as f64;
        // With a non-negative slope the intercept can never exceed `s1`; a
        // positive one up to `s1` is a fixed per-write overhead (a
        // constant-size write has intercept == s1). Only a large negative one
        // is degenerate: the write grew faster than linearly between the two
        // sizes, so the projection would understate it.
        ensure!(
            intercept >= -(s1 as f64) * MAX_NEGATIVE_INTERCEPT,
            "degenerate fit for {}: intercept {intercept:.0} B is more negative than {:.0}% \
             of the n={} measurement of {s1} B, so the write grows faster than linearly and \
             a linear projection would understate it",
            label(&key),
            MAX_NEGATIVE_INTERCEPT * 100.0,
            low.n
        );
        rows.push(FitRow {
            key,
            sizes,
            verdict: Verdict::Projected {
                slope,
                intercept,
                bytes: intercept + slope * target as f64,
            },
        });
    }
    Ok(rows)
}

/// Full-size mode: compare the measured size against the threshold directly.
/// A rejected write stays rejected even if its column holds some other value.
fn absolute(sample: Sample<'_>) -> Vec<FitRow> {
    sample
        .keys()
        .into_iter()
        .map(|key| {
            // A refused write has no size to report, even when a substitute row
            // left a small unrelated value in the same column.
            let size = sample.size(&key);
            let verdict = match (sample.rejections.get(&key), size) {
                (Some(text), _) => Verdict::Rejected {
                    n: sample.n,
                    text: text.clone(),
                },
                (None, Size::Bytes(bytes)) => Verdict::Measured(bytes),
                (None, _) => unreachable!("every key comes from writes or rejections"),
            };
            FitRow {
                key,
                sizes: vec![size],
                verdict,
            }
        })
        .collect()
}

/// How the ratchet table was produced, which decides its column headers.
#[derive(Clone, Copy, Debug)]
enum RatchetMode {
    Projected { n1: u64, n2: u64, target: u64 },
    FullSize { target: u64 },
}

impl RatchetMode {
    fn describe(self) -> String {
        match self {
            Self::Projected { n1, n2, target } => {
                format!("projected to {target} shares from measurements at {n1} and {n2} shares")
            }
            Self::FullSize { target } => format!("measured at full size, {target} shares"),
        }
    }

    fn size_headers(self) -> Vec<String> {
        match self {
            Self::Projected { n1, n2, .. } => vec![format!("n={n1} B"), format!("n={n2} B")],
            Self::FullSize { target } => vec![format!("n={target} B")],
        }
    }

    fn verdict_header(self) -> &'static str {
        match self {
            Self::Projected { .. } => "projection B",
            Self::FullSize { .. } => "measured B",
        }
    }
}

fn size_cell(size: Size) -> String {
    match size {
        Size::Bytes(bytes) => bytes.to_string(),
        Size::Rejected => "rejected".into(),
        Size::NotWritten => "not written".into(),
    }
}

fn verdict_cell(verdict: &Verdict) -> String {
    match verdict {
        Verdict::Rejected { n, .. } => format!("refused at n={n}"),
        Verdict::Projected { bytes, .. } => format!("{bytes:.0}"),
        Verdict::Measured(bytes) => bytes.to_string(),
    }
}

fn ratchet_row(row: &FitRow) -> String {
    let mut line = format!(
        "{:<32} {:<28} {:<9}",
        row.key.table, row.key.column, row.key.phase
    );
    for size in &row.sizes {
        let _ = write!(line, " {:>14}", size_cell(*size));
    }
    let (slope, intercept) = match row.verdict {
        Verdict::Projected {
            slope, intercept, ..
        } => (format!("{slope:.3}"), format!("{intercept:.0}")),
        _ => ("-".to_owned(), "-".to_owned()),
    };
    let _ = write!(
        line,
        " {slope:>12} {intercept:>12} {:>20} {:>8}",
        verdict_cell(&row.verdict),
        if row.crosses() { "YES" } else { "no" }
    );
    line
}

/// The ratchet table, one row per write, then the text of every rejection.
fn ratchet_lines(rows: &[FitRow], mode: RatchetMode) -> Vec<String> {
    let mut lines = vec![
        String::new(),
        format!("--- JSONB ceiling ratchet ({}) ---", mode.describe()),
        format!(
            "threshold {GATE_THRESHOLD} B = 25% of PostgreSQL's {JSONB_ELEMENT_LIMIT} B jsonb \
             container limit; sizes are uncompressed JSONB bytes"
        ),
    ];
    let mut header = format!("{:<32} {:<28} {:<9}", "table", "column", "phase");
    for title in mode.size_headers() {
        let _ = write!(header, " {title:>14}");
    }
    let _ = write!(
        header,
        " {:>12} {:>12} {:>20} {:>8}",
        "B/share",
        "intercept B",
        mode.verdict_header(),
        "crosses"
    );
    lines.push(header);
    lines.extend(rows.iter().map(ratchet_row));
    for row in rows {
        if let Verdict::Rejected { n, text } = &row.verdict {
            lines.push(format!("  refused at n={n}: {} -> {text}", label(&row.key)));
        }
    }
    lines
}

/// The "JSONB writes measured" table. Production writes, rejected production
/// writes and gate-written substitutes each carry their own source label; a
/// rejected write shows no size, and a substitute never reads as the write it
/// stands in for.
fn write_table_lines(
    sample: Sample<'_>,
    substitutes: &BTreeMap<WriteKey, PhaseWrite>,
    jsonb_columns: usize,
) -> Vec<String> {
    let row = |key: (&str, &str, &str), cells: [String; 4], source: String| {
        let [rows, stored, text, uncompressed] = cells;
        format!(
            "{:<32} {:<28} {:<9} {rows:>5} {stored:>14} {text:>14} {uncompressed:>14}  {source}",
            key.0, key.1, key.2
        )
    };
    let sized = |write: &PhaseWrite| {
        [
            write.rows.to_string(),
            write.stored.to_string(),
            write.text_len.to_string(),
            write.uncompressed.to_string(),
        ]
    };
    let mut lines = vec![
        String::new(),
        format!(
            "--- JSONB writes measured at n={} ({jsonb_columns} JSONB columns discovered) ---",
            sample.n
        ),
        row(
            ("table", "column", "phase"),
            ["rows", "stored B", "text B", "uncompressed B"].map(str::to_owned),
            "source".into(),
        ),
    ];
    let mut entries: Vec<(&WriteKey, u8, String)> = Vec::new();
    for (key, write) in sample.writes {
        entries.push((
            key,
            0,
            row(
                (&key.table, &key.column, key.phase),
                sized(write),
                "production write".into(),
            ),
        ));
    }
    for key in sample.rejections.keys() {
        entries.push((
            key,
            1,
            row(
                (&key.table, &key.column, key.phase),
                ["-", "rejected", "rejected", "rejected"].map(str::to_owned),
                "production write REJECTED by PostgreSQL; it has no size".into(),
            ),
        ));
    }
    for (key, write) in substitutes {
        entries.push((
            key,
            2,
            row(
                (&key.table, &key.column, key.phase),
                sized(write),
                format!(
                    "SUBSTITUTE written by the gate, not the {} write; never enters the ratchet",
                    key.phase
                ),
            ),
        ));
    }
    entries.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
    lines.extend(entries.into_iter().map(|(_, _, line)| line));
    for phase in PHASES {
        let touched = sample
            .writes
            .keys()
            .chain(sample.rejections.keys())
            .chain(substitutes.keys())
            .any(|key| key.phase == *phase);
        if !touched {
            lines.push(row(
                ("(none)", "(none)", phase),
                ["0", "-", "-", "-"].map(str::to_owned),
                "measured: this phase wrote no JSONB value".into(),
            ));
        }
    }
    lines
}

fn print_measurements(pipeline: &Pipeline) {
    for line in write_table_lines(
        pipeline.sample(),
        &pipeline.substitutes,
        pipeline.jsonb_columns,
    ) {
        emit(&line);
    }
    report!("");
    report!("--- phases at n={} ---", pipeline.n);
    for phase in &pipeline.phases {
        emit(&phase_line(phase));
    }
    report!(
        "  total {:.2} s, fixture load {:.2} s ({:.0} rows/s), {:.1} B/share, \
         largest per-phase peak RSS {} (test process)",
        pipeline.seconds,
        pipeline.load_seconds,
        pipeline.load_rows_per_second,
        pipeline.average_share_bytes,
        rss_cell(pipeline.peak_rss_kib)
    );
    report!(
        "  qbit_pool_audit_bundles.canonical_audit_bytes (bytea, report-only): {}; \
         25% of the 1 GiB varlena limit is {} B",
        pipeline.canonical_audit_bytes.map_or_else(
            || "not written (no row holds a value)".to_owned(),
            |bytes| format!("{bytes} B")
        ),
        BYTEA_REPORT_THRESHOLD
    );
    if let Some((table, column, stored, text, uncompressed)) = &pipeline.ratio_probe {
        let compression = *uncompressed as f64 / (*stored).max(1) as f64;
        report!(
            "  uncompressed-size method check on {table}.{column}: \
             uncompressed/text = {:.3}, uncompressed/stored = {compression:.2}",
            *uncompressed as f64 / (*text).max(1) as f64
        );
        // The fixture repeats a handful of miner identities and a padded
        // miner_id, so TOAST compresses it far better than production data
        // would. The gate's threshold is on the uncompressed container and is
        // unaffected, but stored sizes and WAL bytes are optimistic.
        report!(
            "  caveat: synthetic window compresses {compression:.0}:1 in TOAST, so the stored \
             sizes and WAL bytes above are optimistic; do not extrapolate them to production \
             data without restating that ratio"
        );
    }
    for line in &pipeline.rewritten_unchanged {
        report!("  attribution: {line}");
    }
}

/// EP-OBSERVABILITY: a phase that did not run is named, never shown as zero.
fn assert_phases_reached(pipeline: &Pipeline) -> Result<()> {
    for phase in PHASES {
        let stat = pipeline.phases.iter().find(|stat| stat.name == *phase);
        let unreached = match stat {
            None => true,
            Some(stat) => stat.status == PhaseStatus::Unreached,
        };
        if unreached {
            report!("UNREACHED phase {phase} at n={}", pipeline.n);
            ensure!(
                UNREACHED_PHASES.contains(phase),
                "phase {phase} could not run at n={} and is not on UNREACHED_PHASES",
                pipeline.n
            );
        }
    }
    Ok(())
}

fn assert_ratchet(rows: &[FitRow], mode: RatchetMode) -> Result<()> {
    for line in ratchet_lines(rows, mode) {
        emit(&line);
    }

    let crossing: BTreeSet<(String, String, &'static str)> = rows
        .iter()
        .filter(|row| row.crosses())
        .map(|row| (row.key.table.clone(), row.key.column.clone(), row.key.phase))
        .collect();
    let known: BTreeSet<(String, String, &'static str)> = KNOWN_VIOLATIONS
        .iter()
        .map(|entry| (entry.table.to_owned(), entry.column.to_owned(), entry.phase))
        .collect();
    let new: Vec<_> = crossing.difference(&known).cloned().collect();
    let fixed: Vec<_> = known.difference(&crossing).cloned().collect();
    let still: Vec<_> = known.intersection(&crossing).cloned().collect();
    report!("");
    report!("known violations still crossing ({}):", still.len());
    for entry in &still {
        report!("  {}.{} @ {}", entry.0, entry.1, entry.2);
    }
    report!("new violations ({}):", new.len());
    for entry in &new {
        report!("  {}.{} @ {}", entry.0, entry.1, entry.2);
    }
    report!("fixed, delete from KNOWN_VIOLATIONS ({}):", fixed.len());
    for entry in &fixed {
        report!("  {}.{} @ {}", entry.0, entry.1, entry.2);
    }
    report!("");

    let mut failures = Vec::new();
    for entry in &new {
        failures.push(format!(
            "NEW VIOLATION: {}.{} written at phase {} crosses the {GATE_THRESHOLD} B gate \
             threshold. A write that embeds the payout window was added or grew; shrink it \
             rather than adding it to KNOWN_VIOLATIONS.",
            entry.0, entry.1, entry.2
        ));
    }
    for entry in &fixed {
        failures.push(format!(
            "FIXED: {}.{} at phase {} no longer crosses the {GATE_THRESHOLD} B gate threshold. \
             Delete that entry from KNOWN_VIOLATIONS in \
             crates/qbit-prism-server/tests/jsonb_ceiling_gate.rs so the ratchet keeps holding.",
            entry.0, entry.1, entry.2
        ));
    }
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    report!(
        "ratchet holds: the crossing set is exactly the {} known violations",
        known.len()
    );
    Ok(())
}

fn print_settings(settings: &GateSettings, mode: &str) {
    report!("");
    report!("=== PRISM JSONB ceiling gate ({mode}) ===");
    report!(
        "build: {}, host: {}/{}, host CPUs: {}",
        if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        std::env::consts::OS,
        std::env::consts::ARCH,
        std::thread::available_parallelism()
            .map_or_else(|_| "unknown".to_owned(), |count| count.get().to_string())
    );
    report!(
        "peak RSS: {}",
        if peak_rss_kib(reset_peak_rss()).is_some() {
            "VmHWM of the test process, reset at the start of each phase"
        } else {
            "not measured on this OS (no resettable /proc/self/status VmHWM); \
             every peak RSS cell says so instead of printing a number"
        }
    );
    report!("settings:");
    for line in settings.describe().lines() {
        report!("{line}");
    }
    report!(
        "  PRISM_DATABASE_STATEMENT_TIMEOUT_MS = {} (exported by the gate)",
        std::env::var("PRISM_DATABASE_STATEMENT_TIMEOUT_MS").unwrap_or_else(|_| "unset".into())
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// The CI-sized gate: two reduced pipelines, a linear fit, and the ratchet.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jsonb_ceiling_ratchet_at_reduced_sizes() -> Result<()> {
    let settings = GateSettings::load()?;
    let Some(url) = database_url()? else {
        return Ok(());
    };
    settings.apply_statement_timeout();
    print_settings(&settings, "reduced sizes");
    let low = run_pipeline(&url, settings.n1.value, &settings).await?;
    print_measurements(&low);
    assert_phases_reached(&low)?;
    let high = run_pipeline(&url, settings.n2.value, &settings).await?;
    print_measurements(&high);
    assert_phases_reached(&high)?;
    let rows = fit([low.sample(), high.sample()], settings.target.value)?;
    report!(
        "reduced-size wall clock: n={} in {:.1} s, n={} in {:.1} s, {:.1} s total",
        low.n,
        low.seconds,
        high.n,
        high.seconds,
        low.seconds + high.seconds
    );
    assert_ratchet(
        &rows,
        RatchetMode::Projected {
            n1: low.n,
            n2: high.n,
            target: settings.target.value,
        },
    )
}

/// The full-size run. Ignored by default: at 400,000 shares the writes below
/// are refused outright and the pipeline needs minutes and gigabytes.
///
/// ```text
/// cargo test --locked -p qbit-prism-server --test jsonb_ceiling_gate \
///   -- --ignored --nocapture --exact jsonb_ceiling_ratchet_at_full_size
/// ```
///
/// Name the test: `--ignored` alone also starts the baseline sweep in the same
/// process, against the same cluster.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full-size 400k-share run; minutes of wall clock and gigabytes of RAM"]
async fn jsonb_ceiling_ratchet_at_full_size() -> Result<()> {
    let settings = GateSettings::load()?;
    let Some(url) = database_url()? else {
        return Ok(());
    };
    settings.apply_statement_timeout();
    print_settings(&settings, "full size");
    let pipeline = run_pipeline(&url, settings.target.value, &settings).await?;
    print_measurements(&pipeline);
    assert_phases_reached(&pipeline)?;
    let rows = absolute(pipeline.sample());
    assert_ratchet(&rows, RatchetMode::FullSize { target: pipeline.n })
}

/// Baseline sweep for `docs/prism-payout-artifact-measurement.md`. Ignored by
/// default; each size runs the whole pipeline once, one size at a time.
///
/// ```text
/// PRISM_JSONB_GATE_BASELINE_SIZES=50000,100000,200000 \
///   cargo test --locked -p qbit-prism-server --test jsonb_ceiling_gate \
///   -- --ignored --nocapture jsonb_ceiling_baseline_sweep
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "baseline sweep for the measurement document"]
async fn jsonb_ceiling_baseline_sweep() -> Result<()> {
    let settings = GateSettings::load()?;
    // Validated before the database check and before any pipeline runs, so a
    // bad list fails in seconds instead of after the whole sweep.
    let sizes = baseline_sizes()?;
    let Some(url) = database_url()? else {
        return Ok(());
    };
    settings.apply_statement_timeout();
    print_settings(&settings, "baseline sweep");
    report!("  {BASELINE_SIZES_VAR} = {sizes:?}");
    let mut runs = Vec::new();
    for size in sizes {
        let pipeline = run_pipeline(&url, size, &settings).await?;
        print_measurements(&pipeline);
        assert_phases_reached(&pipeline)?;
        runs.push(pipeline);
    }
    // Fit stability: the projection from the CI pair has to agree with the one
    // from the largest pair measured here.
    let large = fit(
        [runs[runs.len() - 2].sample(), runs[runs.len() - 1].sample()],
        settings.target.value,
    )?;
    report!("");
    report!(
        "--- projections from the largest baseline pair (n={} and n={}) ---",
        runs[runs.len() - 2].n,
        runs[runs.len() - 1].n
    );
    for row in &large {
        let value = match &row.verdict {
            Verdict::Rejected { n, .. } => format!("refused at n={n}"),
            verdict => format!("{} B", verdict_cell(verdict)),
        };
        report!("  {:<48} {value:>20}", label(&row.key));
    }
    Ok(())
}

/// EP-VALIDATION: every numeric setting rejects missing-but-empty, malformed,
/// zero, negative and out-of-range input at the entry point. `parse_setting` is
/// the same function the env reader calls, so this covers the real path without
/// mutating process-wide environment variables under a parallel test runner.
#[test]
fn settings_reject_every_bad_numeric_input() {
    let ok = parse_setting("PRISM_JSONB_GATE_N1", None, 5_000, 1, 8_000_000).unwrap();
    assert_eq!((ok.value, ok.from_env), (5_000, false));
    let ok = parse_setting("PRISM_JSONB_GATE_N1", Some(" 250000 "), 5_000, 1, 8_000_000).unwrap();
    assert_eq!((ok.value, ok.from_env), (250_000, true));
    for bad in [
        "", "   ", "abc", "12.5", "-5", "0", "8000001", "1e5", "0x20", "20_000",
    ] {
        assert!(
            parse_setting("PRISM_JSONB_GATE_N1", Some(bad), 5_000, 1, 8_000_000).is_err(),
            "{bad:?} must be rejected, not silently replaced by the default"
        );
    }
}

#[test]
fn settings_reject_inconsistent_sizes() {
    let build = |target: u64, n1: u64, n2: u64| GateSettings {
        target: Setting {
            name: "PRISM_JSONB_GATE_TARGET_SHARES",
            value: target,
            from_env: true,
        },
        n1: Setting {
            name: "PRISM_JSONB_GATE_N1",
            value: n1,
            from_env: true,
        },
        n2: Setting {
            name: "PRISM_JSONB_GATE_N2",
            value: n2,
            from_env: true,
        },
        statement_timeout_ms: Setting {
            name: "PRISM_JSONB_GATE_STATEMENT_TIMEOUT_MS",
            value: 600_000,
            from_env: false,
        },
    };
    build(400_000, 5_000, 20_000).cross_check().unwrap();
    // n1 >= n2, n2 > target, and a size the window cannot hold exactly.
    assert!(build(400_000, 20_000, 20_000).cross_check().is_err());
    assert!(build(400_000, 20_000, 5_000).cross_check().is_err());
    assert!(build(10_000, 5_000, 20_000).cross_check().is_err());
    assert!(build(400_000, 5_000, 20_001).cross_check().is_err());
    assert!(build(400_001, 5_000, 20_000).cross_check().is_err());
}

/// EP-OBSERVABILITY: an unknown WAL or peak RSS is named, never printed as a
/// number, while a real measured zero still prints as zero.
#[test]
fn unmeasured_values_never_print_as_numbers() {
    let stat = |wal_bytes, peak_rss_kib| PhaseStat {
        name: PHASE_CLAIM,
        status: PhaseStatus::Ran,
        seconds: 0.41,
        wal_bytes,
        peak_rss_kib,
        note: "note".into(),
    };
    let unknown = phase_line(&stat(None, None));
    assert!(unknown.contains("WAL    unavailable"), "{unknown}");
    assert!(
        unknown.contains("peak RSS not measured on this OS"),
        "{unknown}"
    );
    assert!(
        !unknown.contains("KiB") && !unknown.contains(" B "),
        "{unknown}"
    );
    let zero = phase_line(&stat(Some(0), Some(0)));
    assert!(zero.contains(" 0 B ") && zero.contains(" 0 KiB "), "{zero}");
    let real = phase_line(&stat(Some(840), Some(1_101_840)));
    assert!(
        real.contains(" 840 B ") && real.contains(" 1101840 KiB "),
        "{real}"
    );
    // A failed reset means the reading would span earlier phases.
    assert_eq!(peak_rss_kib(false), None);
    #[cfg(not(target_os = "linux"))]
    assert_eq!(peak_rss_kib(reset_peak_rss()), None);
    assert_eq!(rss_cell(None), RSS_NOT_MEASURED);
}

/// EP-VALIDATION: the sweep's size list is checked before anything runs, with
/// the variable named in every message.
#[test]
fn baseline_sizes_are_validated_up_front() {
    assert_eq!(
        parse_baseline_sizes(None).unwrap(),
        [50_000, 100_000, 200_000]
    );
    assert_eq!(
        parse_baseline_sizes(Some(" 5000, 20000 ")).unwrap(),
        [5_000, 20_000]
    );
    for (bad, why) in [
        ("", "empty entry"),
        ("50000,,100000", "empty entry"),
        ("50000,abc", "not a non-negative integer"),
        ("50000,-5", "not a non-negative integer"),
        ("0,50000", "out of range"),
        ("50000,8000001", "out of range"),
        ("30000,50000", "entry 30000"),
        ("100000,50000", "strictly increasing"),
        ("50000,50000", "strictly increasing"),
        ("50000", "at least two"),
    ] {
        let error = format!("{:#}", parse_baseline_sizes(Some(bad)).unwrap_err());
        assert!(
            error.contains(BASELINE_SIZES_VAR) && error.contains(why),
            "{bad:?}: {error}"
        );
    }
}

/// A superlinear pair fails the fit instead of being projected silently,
/// while the measured CI pair and a constant-size write still fit.
#[test]
fn superlinear_fit_is_refused() {
    let key = test_key("qbit_prism_jobs", "payload", PHASE_REFRESH);
    let none = BTreeMap::new();
    let project = |s1: i64, s2: i64| {
        let low = BTreeMap::from([(key.clone(), test_write(s1))]);
        let high = BTreeMap::from([(key.clone(), test_write(s2))]);
        fit(
            [
                Sample {
                    n: 5_000,
                    writes: &low,
                    rejections: &none,
                },
                Sample {
                    n: 20_000,
                    writes: &high,
                    rejections: &none,
                },
            ],
            400_000,
        )
    };
    // 100 B at n1 but 3.3 MB at n2: the intercept is about -1.1 MB.
    let error = project(100, 3_300_100).unwrap_err().to_string();
    assert!(
        error.contains("degenerate fit for qbit_prism_jobs.payload @ refresh"),
        "{error}"
    );
    // The measured CI pair: intercept -11,833 B, 0.19% of n1.
    let rows = project(6_174_820, 24_734_780).unwrap();
    assert!(
        matches!(rows[0].verdict, Verdict::Projected { bytes, .. } if (bytes - 494_920_433.0).abs() < 1.0),
        "{rows:?}"
    );
    // A constant-size write: intercept == s1, a fixed overhead.
    let rows = project(76, 76).unwrap();
    assert!(
        matches!(rows[0].verdict, Verdict::Projected { bytes, .. } if bytes == 76.0),
        "{rows:?}"
    );
    // A shrinking write is still refused.
    assert!(project(200, 100).is_err());
}

fn test_key(table: &str, column: &str, phase: &'static str) -> WriteKey {
    WriteKey {
        table: table.into(),
        column: column.into(),
        phase,
    }
}

fn test_write(bytes: i64) -> PhaseWrite {
    PhaseWrite {
        rows: 1,
        stored: bytes,
        text_len: bytes,
        uncompressed: bytes,
    }
}

/// EP-OBSERVABILITY: the 400,000-share shape. PostgreSQL rejects the enqueue
/// write, and the gate's own window-free substitute (14,880 B) sits in that
/// column. No table may print that size, or any size, for the rejected write.
#[test]
fn rejected_write_never_prints_a_byte_count() {
    let outbox = test_key("qbit_block_candidate_outbox", "candidate", PHASE_ENQUEUE);
    let landing = test_key("qbit_pool_audit_bundles", "audit_bundle", PHASE_LANDING);
    let text = "error returned from database: total size of jsonb object elements exceeds \
                the maximum of 268435455 bytes";
    let fields = |lines: &[String], prefix: &str| -> Vec<String> {
        let matching: Vec<&String> = lines.iter().filter(|l| l.starts_with(prefix)).collect();
        assert_eq!(matching.len(), 1, "one {prefix} row in {lines:#?}");
        matching[0].split_whitespace().map(str::to_owned).collect()
    };
    let rejections = BTreeMap::from([(outbox.clone(), text.to_owned())]);

    // Full size, even with a stray value attributed to the rejected column.
    let stray = BTreeMap::from([
        (outbox.clone(), test_write(14_880)),
        (landing.clone(), test_write(235_173_306)),
    ]);
    let full = absolute(Sample {
        n: 400_000,
        writes: &stray,
        rejections: &rejections,
    });
    let lines = ratchet_lines(&full, RatchetMode::FullSize { target: 400_000 });
    assert_eq!(
        fields(&lines, "qbit_block_candidate_outbox"),
        [
            "qbit_block_candidate_outbox",
            "candidate",
            "enqueue",
            "rejected",
            "-",
            "-",
            "refused",
            "at",
            "n=400000",
            "YES"
        ]
    );
    assert!(lines.contains(&format!(
        "  refused at n=400000: qbit_block_candidate_outbox.candidate @ enqueue -> {text}"
    )));
    // Control: a write that ran still prints its measured size.
    assert_eq!(
        fields(&lines, "qbit_pool_audit_bundles")[3..],
        ["235173306", "-", "-", "235173306", "YES"]
    );

    // Reduced sizes, rejected only at n2: the real n1 measurement stays, the
    // rejected cell and the projection carry no size.
    let low_writes = BTreeMap::from([(outbox.clone(), test_write(61_934_708))]);
    let no_rejections = BTreeMap::new();
    let error = fit(
        [
            Sample {
                n: 5_000,
                writes: &low_writes,
                rejections: &no_rejections,
            },
            Sample {
                n: 20_000,
                writes: &stray,
                rejections: &rejections,
            },
        ],
        400_000,
    )
    .unwrap_err()
    .to_string();
    // `landing` appears at n2 only, which the fit refuses to project.
    assert!(
        error.contains("was measured at n=20000 but not at n=5000"),
        "{error}"
    );
    let high_writes = BTreeMap::new();
    let rows = fit(
        [
            Sample {
                n: 5_000,
                writes: &low_writes,
                rejections: &no_rejections,
            },
            Sample {
                n: 20_000,
                writes: &high_writes,
                rejections: &rejections,
            },
        ],
        400_000,
    )
    .unwrap();
    let lines = ratchet_lines(
        &rows,
        RatchetMode::Projected {
            n1: 5_000,
            n2: 20_000,
            target: 400_000,
        },
    );
    assert_eq!(
        fields(&lines, "qbit_block_candidate_outbox")[3..],
        ["61934708", "rejected", "-", "-", "refused", "at", "n=20000", "YES"]
    );
    assert!(lines.contains(&format!(
        "  refused at n=20000: qbit_block_candidate_outbox.candidate @ enqueue -> {text}"
    )));

    // The measured-writes table: the rejected write has no size, and the
    // substitute is labelled as one and never as the enqueue write.
    let substitutes = BTreeMap::from([(outbox.clone(), test_write(14_880))]);
    let only_landing = BTreeMap::from([(landing, test_write(235_173_306))]);
    let table = write_table_lines(
        Sample {
            n: 400_000,
            writes: &only_landing,
            rejections: &rejections,
        },
        &substitutes,
        17,
    );
    let rejected: Vec<&String> = table.iter().filter(|l| l.contains("REJECTED")).collect();
    assert_eq!(rejected.len(), 1, "{table:#?}");
    assert!(
        !rejected[0].chars().any(|c| c.is_ascii_digit()),
        "{}",
        rejected[0]
    );
    assert_eq!(
        rejected[0]
            .split_whitespace()
            .filter(|field| *field == "rejected")
            .count(),
        3
    );
    let sized: Vec<&String> = table.iter().filter(|l| l.contains("14880")).collect();
    assert_eq!(sized.len(), 1, "{table:#?}");
    assert!(
        sized[0].contains("SUBSTITUTE") && sized[0].contains("not the enqueue write"),
        "{}",
        sized[0]
    );
    assert!(!sized[0].contains("production"), "{}", sized[0]);
    // Measured zero-write phases are named, not blank.
    assert!(table
        .iter()
        .any(|l| l.starts_with("(none)") && l.contains(" claim ")));
}
