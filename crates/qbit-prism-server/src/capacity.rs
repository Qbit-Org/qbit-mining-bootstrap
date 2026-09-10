//! Strict, externally bound validation of production capacity evidence.
//! Decimal comparisons use arbitrary precision rational arithmetic so boundary
//! qualification never depends on floating point rounding.
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Duration, Utc};
use clap::Args as ClapArgs;
use num_bigint::BigUint;
use num_traits::{One, Zero};
use serde::{
    de::{self, MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer,
};
use serde_json::{Map, Value};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
};

pub const SCHEMA: &str = "qbit-prism-capacity-evidence/v2";
pub const DEFAULT_MAX_AGE_SECONDS: i64 = 86400;
pub const DIFFICULTY_CONFIGURATION_KEYS: &[&str] = &[
    "PRISM_STRATUM_SHARE_DIFF",
    "PRISM_STRATUM_VARDIFF_MIN_DIFF",
    "PRISM_STRATUM_VARDIFF_START_DIFF",
    "PRISM_STRATUM_VARDIFF_MAX_DIFF",
];
pub const DECIMAL_CONFIGURATION_KEYS: &[&str] = &[
    "PRISM_STRATUM_SHARE_DIFF",
    "PRISM_STRATUM_VARDIFF_MIN_DIFF",
    "PRISM_STRATUM_VARDIFF_START_DIFF",
    "PRISM_STRATUM_VARDIFF_MAX_DIFF",
    "PRISM_STRATUM_VARDIFF_TARGET_SECONDS",
    "PRISM_STRATUM_VARDIFF_RETARGET_SECONDS",
    "PRISM_STRATUM_VARDIFF_MAX_STEP_UP",
    "PRISM_STRATUM_VARDIFF_MAX_STEP_DOWN",
    "PRISM_STRATUM_VARDIFF_EWMA_ALPHA",
    "PRISM_STRATUM_VARDIFF_RETARGET_TOLERANCE",
    "PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS",
    "PRISM_SHARE_COMMIT_TIMEOUT_SECONDS",
    "PRISM_STRATUM_SEND_TIMEOUT_SECONDS",
];
pub const INTEGER_CONFIGURATION_KEYS: &[&str] = &[
    "PRISM_STRATUM_VARDIFF",
    "PRISM_SHARE_COMMIT_BATCH_SIZE",
    "PRISM_SHARE_COMMIT_LINGER_MILLISECONDS",
];
pub const CONFIGURATION_KEYS: &[&str] = &[
    "PRISM_STRATUM_SHARE_DIFF",
    "PRISM_STRATUM_VARDIFF_MIN_DIFF",
    "PRISM_STRATUM_VARDIFF_START_DIFF",
    "PRISM_STRATUM_VARDIFF_MAX_DIFF",
    "PRISM_STRATUM_VARDIFF_TARGET_SECONDS",
    "PRISM_STRATUM_VARDIFF_RETARGET_SECONDS",
    "PRISM_STRATUM_VARDIFF_MAX_STEP_UP",
    "PRISM_STRATUM_VARDIFF_MAX_STEP_DOWN",
    "PRISM_STRATUM_VARDIFF_EWMA_ALPHA",
    "PRISM_STRATUM_VARDIFF_RETARGET_TOLERANCE",
    "PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS",
    "PRISM_SHARE_COMMIT_TIMEOUT_SECONDS",
    "PRISM_STRATUM_SEND_TIMEOUT_SECONDS",
    "PRISM_STRATUM_VARDIFF",
    "PRISM_SHARE_COMMIT_BATCH_SIZE",
    "PRISM_SHARE_COMMIT_LINGER_MILLISECONDS",
];
pub const SUBJECT_KEYS: &[&str] = &[
    "coordinator_revision",
    "coordinator_image_digest",
    "postgres_server_version",
    "database_profile_sha256",
];
pub const REQUIRED_PHASES: &[&str] = &["steady_state", "reconnect", "slow_database"];
const TOP_KEYS: &[&str] = &[
    "schema",
    "artifact_kind",
    "test_path",
    "generated_at",
    "run_id",
    "subject",
    "durability",
    "configuration",
    "forecast_peak_shares_per_second",
    "test_duration_seconds",
    "offered_valid_shares",
    "acknowledged_shares",
    "postgres_unique_committed_shares",
    "rejected_valid_shares",
    "missing_acknowledged_share_ids",
    "unexpected_committed_share_ids",
    "acknowledged_share_ids_sha256",
    "postgres_share_ids_sha256",
    "ack_latency_milliseconds",
    "ack_p99_limit_milliseconds",
    "phases",
];
const PHASE_KEYS: &[&str] = &[
    "completed",
    "duration_seconds",
    "offered_valid_shares",
    "acknowledged_shares",
    "postgres_unique_committed_shares",
    "rejected_valid_shares",
    "missing_acknowledged_share_ids",
    "unexpected_committed_share_ids",
    "acknowledged_share_ids_sha256",
    "postgres_share_ids_sha256",
    "ack_latency_milliseconds",
];

#[derive(Clone, Debug)]
pub struct Decimal {
    n: BigUint,
    d: BigUint,
}
impl Decimal {
    fn integer(v: impl Into<BigUint>) -> Self {
        Self {
            n: v.into(),
            d: BigUint::one(),
        }
    }
    fn multiply(&self, other: &Self) -> Self {
        Self {
            n: &self.n * &other.n,
            d: &self.d * &other.d,
        }
    }
    fn divide(&self, other: &Self) -> Self {
        Self {
            n: &self.n * &other.d,
            d: &self.d * &other.n,
        }
    }
    fn add(&self, other: &Self) -> Self {
        Self {
            n: &self.n * &other.d + &other.n * &self.d,
            d: &self.d * &other.d,
        }
    }
}
impl PartialEq for Decimal {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Decimal {}
impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Decimal {
    fn cmp(&self, other: &Self) -> Ordering {
        (&self.n * &other.d).cmp(&(&other.n * &self.d))
    }
}
impl FromStr for Decimal {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let s = s.trim().strip_prefix('+').unwrap_or(s.trim());
        let parts: Vec<&str> = s.split(['e', 'E']).collect();
        ensure!(parts.len() <= 2, "invalid decimal");
        let exp = if parts.len() == 2 {
            parts[1].parse::<i32>()?
        } else {
            0
        };
        ensure!(
            exp.unsigned_abs() <= 10000,
            "decimal exponent exceeds supported limit"
        );
        let parts: Vec<&str> = parts[0].split('.').collect();
        ensure!(parts.len() <= 2, "invalid decimal");
        let joined = parts.concat();
        ensure!(
            !joined.is_empty() && joined.bytes().all(|b| b.is_ascii_digit()),
            "invalid decimal"
        );
        ensure!(joined.len() <= 10000, "decimal is too long");
        let n = joined.parse::<BigUint>()?;
        let scale = parts.get(1).map_or(0, |s| s.len()) as i32 - exp;
        let ten = BigUint::from(10u8);
        Ok(if scale >= 0 {
            Self {
                n,
                d: ten.pow(scale as u32),
            }
        } else {
            Self {
                n: n * ten.pow((-scale) as u32),
                d: BigUint::one(),
            }
        })
    }
}
impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let whole = &self.n / &self.d;
        let rem = &self.n % &self.d;
        if rem.is_zero() {
            return write!(f, "{whole}");
        }
        let digits = (rem * BigUint::from(10u8).pow(28) / &self.d).to_string();
        let digits = format!("{digits:0>28}");
        write!(f, "{whole}.{}", digits.trim_end_matches('0'))
    }
}

#[derive(Clone, Debug)]
pub struct CapacityEvidenceSummary {
    pub generated_at: DateTime<Utc>,
    pub measured_shares_per_second: Decimal,
    pub capacity_multiple: Decimal,
    pub acknowledged_shares: BigUint,
    pub ack_p50_milliseconds: Decimal,
    pub ack_p99_milliseconds: Decimal,
    pub ack_p99_limit_milliseconds: Decimal,
}
#[derive(Clone, Debug)]
pub struct ValidationOptions {
    pub expected_configuration: Option<BTreeMap<String, String>>,
    pub expected_subject: Option<BTreeMap<String, String>>,
    pub expected_forecast_peak_shares_per_second: Option<String>,
    pub expected_ack_p99_limit_milliseconds: Option<String>,
    pub current_time: Option<DateTime<Utc>>,
    pub max_age_seconds: i64,
    pub max_future_skew_seconds: i64,
    pub enforce_freshness: bool,
    pub allow_example: bool,
}
impl Default for ValidationOptions {
    fn default() -> Self {
        Self {
            expected_configuration: None,
            expected_subject: None,
            expected_forecast_peak_shares_per_second: None,
            expected_ack_p99_limit_milliseconds: None,
            current_time: None,
            max_age_seconds: DEFAULT_MAX_AGE_SECONDS,
            max_future_skew_seconds: 300,
            enforce_freshness: true,
            allow_example: false,
        }
    }
}
#[derive(Clone, Debug, ClapArgs)]
pub struct Args {
    pub evidence_file: PathBuf,
    #[arg(long = "expect", value_name = "NAME=VALUE")]
    pub expected: Vec<String>,
    #[arg(long)]
    pub expect_coordinator_revision: Option<String>,
    #[arg(long)]
    pub expect_coordinator_image_digest: Option<String>,
    #[arg(long)]
    pub expect_postgres_server_version: Option<String>,
    #[arg(long)]
    pub expect_database_profile_sha256: Option<String>,
    #[arg(long)]
    pub forecast_peak_shares_per_second: Option<String>,
    #[arg(long)]
    pub ack_p99_limit_milliseconds: Option<String>,
    #[arg(long,default_value_t=DEFAULT_MAX_AGE_SECONDS)]
    pub max_age_seconds: i64,
    #[arg(long)]
    pub allow_example_evidence_for_tests: bool,
}
pub fn run(args: Args) -> Result<()> {
    let mut configuration = BTreeMap::new();
    for value in args.expected {
        let (key, v) = value
            .split_once('=')
            .filter(|(k, v)| !k.is_empty() && !v.is_empty())
            .with_context(|| format!("--expect must use NAME=VALUE, got {value:?}"))?;
        ensure!(
            CONFIGURATION_KEYS.contains(&key),
            "--expect contains unknown configuration keys: {key}"
        );
        configuration.insert(key.into(), v.into());
    }
    let subject = SUBJECT_KEYS
        .iter()
        .zip([
            args.expect_coordinator_revision,
            args.expect_coordinator_image_digest,
            args.expect_postgres_server_version,
            args.expect_database_profile_sha256,
        ])
        .filter_map(|(key, value)| value.map(|v| ((*key).into(), v)))
        .collect::<BTreeMap<_, _>>();
    let options = ValidationOptions {
        expected_configuration: (!configuration.is_empty()).then_some(configuration),
        expected_subject: (!subject.is_empty()).then_some(subject),
        expected_forecast_peak_shares_per_second: args.forecast_peak_shares_per_second,
        expected_ack_p99_limit_milliseconds: args.ack_p99_limit_milliseconds,
        max_age_seconds: args.max_age_seconds,
        allow_example: args.allow_example_evidence_for_tests,
        ..Default::default()
    };
    let summary = load_capacity_evidence(&args.evidence_file, &options)
        .context("capacity evidence invalid")?;
    println!(
        "capacity evidence valid: rate={} shares/s capacity={}x ACK p50={}ms p99={}ms committed={}",
        summary.measured_shares_per_second,
        summary.capacity_multiple,
        summary.ack_p50_milliseconds,
        summary.ack_p99_milliseconds,
        summary.acknowledged_shares
    );
    Ok(())
}
fn mapping<'a>(value: &'a Value, field: &str) -> Result<&'a Map<String, Value>> {
    value
        .as_object()
        .with_context(|| format!("{field} must be a JSON object"))
}
fn keys(value: &Map<String, Value>, expected: &[&str], field: &str) -> Result<()> {
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    let actual = value.keys().map(String::as_str).collect::<BTreeSet<_>>();
    let missing = expected.difference(&actual).copied().collect::<Vec<_>>();
    let unknown = actual.difference(&expected).copied().collect::<Vec<_>>();
    ensure!(
        missing.is_empty(),
        "{field} is missing required fields: {}",
        missing.join(", ")
    );
    ensure!(
        unknown.is_empty(),
        "{field} contains unknown fields: {}",
        unknown.join(", ")
    );
    Ok(())
}
fn text(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| value.to_string())
}
fn decimal(value: &Value, field: &str, zero: bool) -> Result<Decimal> {
    let parsed = text(value)
        .parse::<Decimal>()
        .with_context(|| format!("{field} must be a finite decimal number"))?;
    ensure!(
        zero || !parsed.n.is_zero(),
        "{field} must be a finite positive decimal number"
    );
    Ok(parsed)
}
fn integer(value: &Value, field: &str, zero: bool) -> Result<BigUint> {
    let s = text(value);
    let parsed = s
        .trim()
        .parse::<BigUint>()
        .with_context(|| format!("{field} must be an integer"))?;
    ensure!(parsed.to_string() == s.trim(), "{field} must be an integer");
    ensure!(
        zero || !parsed.is_zero(),
        "{field} must be a positive integer"
    );
    Ok(parsed)
}
fn is_hex(value: &str, n: usize) -> bool {
    value.len() == n
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn sha<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    let v = value
        .as_str()
        .filter(|v| is_hex(v, 64))
        .with_context(|| format!("{field} must be 64 lowercase hex characters"))?;
    Ok(v)
}
fn normalized_configuration(value: &Value, field: &str) -> Result<BTreeMap<String, Decimal>> {
    let cfg = mapping(value, field)?;
    keys(cfg, CONFIGURATION_KEYS, field)?;
    let mut normalized = BTreeMap::new();
    for key in DECIMAL_CONFIGURATION_KEYS {
        let zero = matches!(
            *key,
            "PRISM_STRATUM_VARDIFF_RETARGET_TOLERANCE"
                | "PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS"
                | "PRISM_STRATUM_SEND_TIMEOUT_SECONDS"
        );
        let v = decimal(&cfg[*key], &format!("{field}.{key}"), zero)?;
        ensure!(
            !DIFFICULTY_CONFIGURATION_KEYS.contains(key) || v != Decimal::from_str("1e-9")?,
            "{field}.{key} uses the lab-only 1e-9 difficulty"
        );
        normalized.insert((*key).into(), v);
    }
    for key in INTEGER_CONFIGURATION_KEYS {
        let zero = matches!(
            *key,
            "PRISM_STRATUM_VARDIFF" | "PRISM_SHARE_COMMIT_LINGER_MILLISECONDS"
        );
        normalized.insert(
            (*key).into(),
            Decimal::integer(integer(&cfg[*key], &format!("{field}.{key}"), zero)?),
        );
    }
    ensure!(
        normalized["PRISM_STRATUM_VARDIFF"] <= Decimal::integer(1u8),
        "{field}.PRISM_STRATUM_VARDIFF must be 0 or 1"
    );
    ensure!(
        normalized["PRISM_STRATUM_VARDIFF_MIN_DIFF"]
            <= normalized["PRISM_STRATUM_VARDIFF_START_DIFF"]
            && normalized["PRISM_STRATUM_VARDIFF_START_DIFF"]
                <= normalized["PRISM_STRATUM_VARDIFF_MAX_DIFF"],
        "{field} vardiff values must satisfy minimum <= start <= maximum"
    );
    for key in [
        "PRISM_STRATUM_VARDIFF_MAX_STEP_UP",
        "PRISM_STRATUM_VARDIFF_MAX_STEP_DOWN",
    ] {
        ensure!(
            normalized[key] >= Decimal::integer(1u8),
            "{field}.{key} must be at least 1"
        );
    }
    ensure!(
        normalized["PRISM_STRATUM_VARDIFF_EWMA_ALPHA"] <= Decimal::integer(1u8),
        "{field}.PRISM_STRATUM_VARDIFF_EWMA_ALPHA must not exceed 1"
    );
    Ok(normalized)
}
fn latency(value: &Value, field: &str, limit: &Decimal) -> Result<(Decimal, Decimal)> {
    let v = mapping(value, field)?;
    keys(v, &["p50", "p99"], field)?;
    let p50 = decimal(&v["p50"], &format!("{field}.p50"), true)?;
    let p99 = decimal(&v["p99"], &format!("{field}.p99"), true)?;
    ensure!(p50 <= p99, "{field} p50 latency cannot exceed p99 latency");
    ensure!(
        &p99 <= limit,
        "{field} p99 latency {p99}ms exceeds the required {limit}ms"
    );
    Ok((p50, p99))
}
fn counts(value: &Map<String, Value>, field: &str) -> Result<(BigUint, BigUint, BigUint)> {
    let offered = integer(
        &value["offered_valid_shares"],
        &format!("{field}.offered_valid_shares"),
        false,
    )?;
    let ack = integer(
        &value["acknowledged_shares"],
        &format!("{field}.acknowledged_shares"),
        false,
    )?;
    let committed = integer(
        &value["postgres_unique_committed_shares"],
        &format!("{field}.postgres_unique_committed_shares"),
        false,
    )?;
    let rejected = integer(
        &value["rejected_valid_shares"],
        &format!("{field}.rejected_valid_shares"),
        true,
    )?;
    let missing = integer(
        &value["missing_acknowledged_share_ids"],
        &format!("{field}.missing_acknowledged_share_ids"),
        true,
    )?;
    let unexpected = integer(
        &value["unexpected_committed_share_ids"],
        &format!("{field}.unexpected_committed_share_ids"),
        true,
    )?;
    let ack_digest = sha(
        &value["acknowledged_share_ids_sha256"],
        &format!("{field}.acknowledged_share_ids_sha256"),
    )?;
    let committed_digest = sha(
        &value["postgres_share_ids_sha256"],
        &format!("{field}.postgres_share_ids_sha256"),
    )?;
    ensure!(rejected.is_zero()&&offered==ack,"{field} did not acknowledge every offered valid share: offered={offered} acknowledged={ack} rejected={rejected}");
    ensure!(ack==committed&&missing.is_zero()&&unexpected.is_zero(),"{field} failed ACK-to-Postgres reconciliation: acknowledged={ack} committed={committed} missing={missing} unexpected={unexpected}");
    ensure!(
        ack_digest == committed_digest,
        "{field} ACK and Postgres share-identifier digests differ"
    );
    Ok((offered, ack, committed))
}
struct Phase {
    duration: Decimal,
    offered: BigUint,
    ack: BigUint,
    committed: BigUint,
}
fn phase(name: &str, value: &Value, forecast: &Decimal, limit: &Decimal) -> Result<Phase> {
    let field = format!("phases.{name}");
    let v = mapping(value, &field)?;
    let mut expected = PHASE_KEYS.to_vec();
    match name {
        "reconnect" => expected.push("reconnect_events"),
        "slow_database" => expected.push("database_delay_milliseconds"),
        _ => {}
    }
    keys(v, &expected, &field)?;
    ensure!(
        v["completed"] == Value::Bool(true),
        "{field}.completed must be true"
    );
    let duration = decimal(
        &v["duration_seconds"],
        &format!("{field}.duration_seconds"),
        false,
    )?;
    ensure!(
        duration >= Decimal::integer(60u8),
        "{field}.duration_seconds must be at least 60"
    );
    let (offered, ack, committed) = counts(v, &field)?;
    latency(
        &v["ack_latency_milliseconds"],
        &format!("{field}.ack_latency_milliseconds"),
        limit,
    )?;
    let rate = Decimal::integer(ack.clone()).divide(&duration);
    ensure!(rate>=forecast.multiply(&Decimal::integer(2u8)),"{field} sustained rate must be at least 2x forecast peak: measured={rate} forecast={forecast}");
    if name == "reconnect" {
        ensure!(
            integer(
                &v["reconnect_events"],
                &format!("{field}.reconnect_events"),
                false
            )? >= BigUint::from(10u8),
            "{field}.reconnect_events must be at least 10"
        );
    }
    if name == "slow_database" {
        ensure!(
            decimal(
                &v["database_delay_milliseconds"],
                &format!("{field}.database_delay_milliseconds"),
                false
            )? >= Decimal::integer(10u8),
            "{field}.database_delay_milliseconds must be at least 10"
        );
    }
    Ok(Phase {
        duration,
        offered,
        ack,
        committed,
    })
}
pub fn validate_capacity_evidence(
    payload: &Value,
    options: &ValidationOptions,
) -> Result<CapacityEvidenceSummary> {
    let v = mapping(payload, "document")?;
    keys(v, TOP_KEYS, "document")?;
    ensure!(v["schema"] == SCHEMA, "schema must be '{SCHEMA}'");
    ensure!(
        v["test_path"] == "stratum-to-postgres",
        "test_path must be 'stratum-to-postgres'"
    );
    let kind = v["artifact_kind"].as_str().unwrap_or("");
    ensure!(
        matches!(kind, "example" | "qualification"),
        "artifact_kind must be 'qualification' or 'example'"
    );
    ensure!(
        kind != "example" || options.allow_example,
        "example capacity evidence is rejected outside explicit test validation"
    );
    let uuid = uuid::Uuid::parse_str(&text(&v["run_id"])).context("run_id must be a UUID")?;
    ensure!(
        kind != "qualification" || !uuid.is_nil(),
        "qualification evidence requires a non-zero run_id"
    );
    let timestamp = v["generated_at"]
        .as_str()
        .context("generated_at must be an RFC 3339 timestamp")?;
    // Chrono also accepts spaces and lowercase t; the contract requires literal T and an explicit timezone.
    ensure!(
        timestamp.as_bytes().get(10) == Some(&b'T')
            && (timestamp.ends_with('Z')
                || timestamp.len() >= 6
                    && matches!(timestamp.as_bytes()[timestamp.len() - 6], b'+' | b'-')
                    && timestamp.as_bytes()[timestamp.len() - 3] == b':'),
        "generated_at must be an RFC 3339 timestamp"
    );
    let generated_at = DateTime::parse_from_rfc3339(timestamp)
        .context("generated_at must be an RFC 3339 timestamp")?
        .with_timezone(&Utc);
    if kind == "qualification" && options.enforce_freshness {
        ensure!(
            options.max_age_seconds > 0,
            "capacity evidence maximum age must be positive"
        );
        ensure!(
            options.max_future_skew_seconds >= 0,
            "capacity evidence future skew must be non-negative"
        );
        let now = options.current_time.unwrap_or_else(Utc::now);
        let future = now
            .checked_add_signed(
                Duration::try_seconds(options.max_future_skew_seconds)
                    .context("invalid future skew")?,
            )
            .context("invalid future skew")?;
        let past = now
            .checked_sub_signed(
                Duration::try_seconds(options.max_age_seconds).context("invalid maximum age")?,
            )
            .context("invalid maximum age")?;
        ensure!(
            generated_at <= future,
            "capacity evidence generated_at is too far in the future"
        );
        ensure!(
            generated_at >= past,
            "capacity evidence is older than the allowed {} seconds",
            options.max_age_seconds
        );
    }
    let subject = mapping(&v["subject"], "subject")?;
    keys(subject, SUBJECT_KEYS, "subject")?;
    ensure!(
        subject["coordinator_revision"]
            .as_str()
            .is_some_and(|v| is_hex(v, 40)),
        "subject.coordinator_revision must be 40 lowercase hex characters"
    );
    ensure!(
        subject["coordinator_image_digest"]
            .as_str()
            .and_then(|v| v.strip_prefix("sha256:"))
            .is_some_and(|v| is_hex(v, 64)),
        "subject.coordinator_image_digest must be sha256 followed by 64 lowercase hex characters"
    );
    ensure!(
        subject["postgres_server_version"]
            .as_str()
            .is_some_and(|v| !v.trim().is_empty()),
        "subject.postgres_server_version must be a non-empty string"
    );
    sha(
        &subject["database_profile_sha256"],
        "subject.database_profile_sha256",
    )?;
    if let Some(expected) = &options.expected_subject {
        for key in SUBJECT_KEYS {
            let expected = expected
                .get(*key)
                .filter(|v| !v.is_empty())
                .with_context(|| format!("expected deployment subject is missing {key}"))?;
            ensure!(
                subject[*key].as_str() == Some(expected),
                "subject.{key} does not match the deployment value"
            );
        }
    } else {
        ensure!(
            options.allow_example,
            "expected deployment subject is required for qualification evidence"
        );
    }
    let durability = mapping(&v["durability"], "durability")?;
    keys(
        durability,
        &["fsync", "full_page_writes", "synchronous_commit"],
        "durability",
    )?;
    for key in ["fsync", "full_page_writes", "synchronous_commit"] {
        ensure!(durability[key] == "on", "durability.{key} must be 'on'");
    }
    let cfg = normalized_configuration(&v["configuration"], "configuration")?;
    if let Some(expected) = &options.expected_configuration {
        let expected =
            normalized_configuration(&serde_json::to_value(expected)?, "expected configuration")?;
        for key in CONFIGURATION_KEYS {
            ensure!(
                cfg[*key] == expected[*key],
                "configuration.{key} does not match the deployment value"
            );
        }
    } else {
        ensure!(
            options.allow_example,
            "expected deployment configuration is required for qualification evidence"
        );
    }
    let forecast = decimal(
        &v["forecast_peak_shares_per_second"],
        "forecast_peak_shares_per_second",
        false,
    )?;
    if let Some(expected) = &options.expected_forecast_peak_shares_per_second {
        ensure!(
            forecast
                == decimal(
                    &Value::String(expected.clone()),
                    "expected forecast peak shares per second",
                    false
                )?,
            "forecast_peak_shares_per_second does not match the deployment value"
        );
    } else {
        ensure!(
            options.allow_example,
            "externally configured forecast peak share rate is required"
        );
    }
    let limit = decimal(
        &v["ack_p99_limit_milliseconds"],
        "ack_p99_limit_milliseconds",
        false,
    )?;
    if let Some(expected) = &options.expected_ack_p99_limit_milliseconds {
        ensure!(
            limit
                == decimal(
                    &Value::String(expected.clone()),
                    "expected ACK p99 limit milliseconds",
                    false
                )?,
            "ack_p99_limit_milliseconds does not match the deployment value"
        );
    } else {
        ensure!(
            options.allow_example,
            "externally configured ACK p99 limit is required"
        );
    }
    ensure!(
        limit <= cfg["PRISM_SHARE_COMMIT_TIMEOUT_SECONDS"].multiply(&Decimal::integer(1000u32)),
        "ack_p99_limit_milliseconds cannot exceed PRISM_SHARE_COMMIT_TIMEOUT_SECONDS"
    );
    let duration = decimal(&v["test_duration_seconds"], "test_duration_seconds", false)?;
    let (offered, ack, committed) = counts(v, "capacity run")?;
    let rate = Decimal::integer(ack.clone()).divide(&duration);
    let multiple = rate.divide(&forecast);
    ensure!(multiple>=Decimal::integer(2u8),"measured sustained rate must be at least 2x forecast peak: measured={rate} forecast={forecast}");
    let (p50, p99) = latency(
        &v["ack_latency_milliseconds"],
        "ack_latency_milliseconds",
        &limit,
    )?;
    let phases = mapping(&v["phases"], "phases")?;
    keys(phases, REQUIRED_PHASES, "phases")?;
    let phases = REQUIRED_PHASES
        .iter()
        .map(|name| phase(name, &phases[*name], &forecast, &limit))
        .collect::<Result<Vec<_>>>()?;
    ensure!(
        phases
            .iter()
            .fold(Decimal::integer(0u8), |sum, p| sum.add(&p.duration))
            == duration,
        "phase durations must equal test_duration_seconds"
    );
    ensure!(
        phases.iter().map(|p| p.offered.clone()).sum::<BigUint>() == offered,
        "phase offered-share totals must equal the capacity-run total"
    );
    ensure!(
        phases.iter().map(|p| p.ack.clone()).sum::<BigUint>() == ack,
        "phase acknowledged-share totals must equal the capacity-run total"
    );
    ensure!(
        phases.iter().map(|p| p.committed.clone()).sum::<BigUint>() == committed,
        "phase committed-share totals must equal the capacity-run total"
    );
    Ok(CapacityEvidenceSummary {
        generated_at,
        measured_shares_per_second: rate,
        capacity_multiple: multiple,
        acknowledged_shares: ack,
        ack_p50_milliseconds: p50,
        ack_p99_milliseconds: p99,
        ack_p99_limit_milliseconds: limit,
    })
}

/// The preliminary visitor rejects duplicate keys at every nesting depth.
/// Parsing directly into Value would silently let the last duplicate win.
struct UniqueJson;
impl<'de> Deserialize<'de> for UniqueJson {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct UniqueVisitor;
        impl<'de> Visitor<'de> for UniqueVisitor {
            type Value = UniqueJson;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "JSON with unique object keys")
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut keys = BTreeSet::new();
                while let Some(key) = map.next_key::<String>()? {
                    if !keys.insert(key.clone()) {
                        return Err(de::Error::custom(format!(
                            "capacity evidence contains duplicate JSON key {key:?}"
                        )));
                    }
                    map.next_value::<UniqueJson>()?;
                }
                Ok(UniqueJson)
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                while seq.next_element::<UniqueJson>()?.is_some() {}
                Ok(UniqueJson)
            }
            fn visit_str<E: de::Error>(self, _: &str) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJson)
            }
            fn visit_bool<E: de::Error>(self, _: bool) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJson)
            }
            fn visit_i64<E: de::Error>(self, _: i64) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJson)
            }
            fn visit_u64<E: de::Error>(self, _: u64) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJson)
            }
            fn visit_f64<E: de::Error>(self, _: f64) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJson)
            }
            fn visit_unit<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJson)
            }
        }
        d.deserialize_any(UniqueVisitor)
    }
}
pub fn load_capacity_evidence(
    path: &Path,
    options: &ValidationOptions,
) -> Result<CapacityEvidenceSummary> {
    let input =
        std::fs::read_to_string(path).with_context(|| format!("cannot read {}", path.display()))?;
    serde_json::from_str::<UniqueJson>(&input)
        .with_context(|| format!("{} is not valid capacity evidence JSON", path.display()))?;
    validate_capacity_evidence(&serde_json::from_str::<Value>(&input)?, options)
}
