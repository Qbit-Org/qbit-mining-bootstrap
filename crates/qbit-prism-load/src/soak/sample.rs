//! What a soak sample holds, and how the harness reads it from a process's
//! `/proc` entries and from the database.

use super::SAMPLE_SCHEMA;
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::path::Path;

/// One server process at one sample.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ProcessPoint {
    pub instance: String,
    /// The process ID; for a deployment read through Prometheus, which does
    /// not export it, the ordinal of the process's lifetime (1, 2, ...) as
    /// the resets of its acknowledged-share counter reveal them.
    pub pid: Option<u32>,
    pub rss_bytes: Option<u64>,
    pub open_fds: Option<u64>,
    pub threads: Option<u64>,
    /// The process's acknowledged-share counter, where it is read from
    /// Prometheus (`qbit_prism_accepted_shares_total`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted_total: Option<u64>,
    /// Why a `null` above is unknown.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unknown: Vec<String>,
}

/// One share ledger partition as the catalog and PostgreSQL held it.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct PartitionPoint {
    pub name: String,
    pub state: String,
    pub lower_seq: Option<i64>,
    pub upper_seq: i64,
    /// `pg_total_relation_size`; `null` once the relation is gone.
    pub bytes: Option<u64>,
}

/// The database at one sample.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct DatabasePoint {
    /// Connections to this database by client ([`CONNECTION_KEY`]); `null`
    /// when `pg_stat_activity` could not be read. An empty map is a read
    /// that found no client.
    pub connections: Option<BTreeMap<String, u64>>,
    /// Connections idle inside a transaction for over a minute; `null` when
    /// the sampling role cannot see other roles' session states.
    pub idle_in_transaction_over_60s: Option<u64>,
    pub wal_bytes: Option<u64>,
    pub next_share_seq: Option<i64>,
    pub database_bytes: Option<u64>,
    #[serde(default)]
    pub partitions: Vec<PartitionPoint>,
    /// The largest relations of the schema, total size in bytes.
    #[serde(default)]
    pub relations: BTreeMap<String, u64>,
    /// Payout divergence rows (#478), for a deployment soak's reconciliation.
    pub payout_divergences: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unknown: Vec<String>,
}

/// Share acknowledgement latency over the interval that ended at the sample.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct LatencyPoint {
    /// Where the figure came from: the harness's own clients, or the servers'
    /// `share_ack_seconds` histogram.
    pub source: String,
    pub interval_seconds: f64,
    pub acknowledged: Option<u64>,
    pub p50_ms: Option<f64>,
    pub p99_ms: Option<f64>,
}

/// Acknowledged against committed shares, for a soak that cannot see its
/// miners' side (a deployment). Cumulative from the first sample, which is
/// the baseline.
///
/// Both sides cover the same span: the acknowledged side is the servers'
/// counters from one reading to the next, and the committed side is the
/// accepted ledger rows between the two readings' scrape times (the earliest
/// scrape among the processes). Processes scraped a little later than the
/// earliest acknowledged a few more shares than that span holds; those rows
/// are `tolerance_rows`, and the check is `committed + tolerance >=
/// acknowledged`. A counter that reset since its last reading is a restart,
/// and what the old process acknowledged after that reading cannot be
/// counted: `acknowledged_gaps` counts those, which can hide a loss but never
/// invent one.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct LedgerPoint {
    /// Shares the servers counted as accepted (acknowledged to the miner)
    /// since the baseline, summed over processes and across restarts.
    pub acknowledged_since_start: Option<u64>,
    /// Restarts whose old process's last acknowledgements could not be
    /// counted.
    #[serde(default)]
    pub acknowledged_gaps: u64,
    /// Accepted rows the ledger held with `accepted_at` in the same span,
    /// counted interval by interval while each interval's rows were live,
    /// so rows the operator's retention later removes stay counted.
    pub committed_since_start: Option<u64>,
    /// The scrape time the span ends at; the next sample counts from here.
    #[serde(default)]
    pub committed_through: Option<DateTime<Utc>>,
    /// Accepted rows between this sample's earliest and latest counter
    /// scrape: the most the processes scraped later can be ahead by.
    pub tolerance_rows: Option<u64>,
    /// That spread, in seconds.
    pub tolerance_seconds: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Sample {
    pub schema: String,
    pub at: DateTime<Utc>,
    /// Seconds since the soak's first sample.
    pub elapsed_seconds: f64,
    /// The harness phase the sample was taken in, when there is one.
    pub phase: Option<String>,
    /// Whether the session population was the base population (no rental
    /// churn in flight), so descriptor and memory counts compare across
    /// samples. A deployment's samples are all steady.
    pub steady: bool,
    pub processes: Vec<ProcessPoint>,
    pub database: DatabasePoint,
    pub latency: Option<LatencyPoint>,
    pub ledger: Option<LedgerPoint>,
}

pub fn append_line<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("opening {}", path.display()))?;
    writeln!(file, "{}", serde_json::to_string(value)?)?;
    file.sync_data()?;
    Ok(())
}

pub fn read_samples(path: &Path) -> Result<Vec<Sample>> {
    let file = std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut samples = Vec::new();
    for (number, line) in std::io::BufReader::new(file).lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let sample: Sample = serde_json::from_str(&line)
            .with_context(|| format!("{}:{}", path.display(), number + 1))?;
        ensure!(
            sample.schema == SAMPLE_SCHEMA,
            "{}:{}: schema {:?}, not {SAMPLE_SCHEMA}",
            path.display(),
            number + 1,
            sample.schema
        );
        samples.push(sample);
    }
    Ok(samples)
}

// --- process sampling (Linux /proc) ----------------------------------------

/// Resident memory, open descriptors and threads of `pid`, each `None` with
/// the reason when it cannot be read.
pub fn process_point(instance: &str, pid: Option<u32>) -> ProcessPoint {
    let mut point = ProcessPoint {
        instance: instance.to_owned(),
        pid,
        ..ProcessPoint::default()
    };
    let Some(pid) = pid else {
        point.unknown.push("the process is not running".into());
        return point;
    };
    match std::fs::read_to_string(format!("/proc/{pid}/status")) {
        Ok(status) => {
            point.rss_bytes = status_field_kib(&status, "VmRSS:").map(|kib| kib * 1024);
            point.threads = status_field_kib(&status, "Threads:");
            if point.rss_bytes.is_none() {
                point.unknown.push("VmRSS missing from /proc status".into());
            }
        }
        Err(error) => point.unknown.push(format!("/proc/{pid}/status: {error}")),
    }
    match std::fs::read_dir(format!("/proc/{pid}/fd")) {
        Ok(entries) => point.open_fds = Some(entries.count() as u64),
        Err(error) => point.unknown.push(format!("/proc/{pid}/fd: {error}")),
    }
    point
}

fn status_field_kib(status: &str, key: &str) -> Option<u64> {
    status
        .lines()
        .find_map(|line| line.strip_prefix(key))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|value| value.parse().ok())
}

// --- database sampling -----------------------------------------------------

/// How connections are attributed to a client: the `application_name` when
/// the client set one (the harness names each frontend's), else the role
/// and address it connected from.
pub const CONNECTION_KEY: &str = "CASE WHEN application_name <> '' THEN application_name \
     ELSE usename || '@' || COALESCE(host(client_addr), 'local') END";

/// Everything [`DatabasePoint`] holds, each part independently: a part the
/// role may not read is `null` with its reason, and the rest still reads.
/// `only`, when given, keeps the connections of those clients and no others
/// (the harness's frontends, not its own side pools).
pub async fn database_point(pool: &PgPool, only: Option<&[String]>) -> DatabasePoint {
    let mut point = DatabasePoint::default();
    let connections = sqlx::query(&format!(
        "SELECT {CONNECTION_KEY} AS client, count(*)::bigint AS n, \
         count(*) FILTER (WHERE state IS NULL)::bigint AS hidden, \
         count(*) FILTER (WHERE state LIKE 'idle in transaction%' \
             AND clock_timestamp() - state_change > interval '60 seconds')::bigint AS stuck \
         FROM pg_stat_activity WHERE datname = current_database() AND backend_type = 'client backend' \
         AND pid <> pg_backend_pid() GROUP BY 1"
    ))
    .fetch_all(pool)
    .await;
    match connections {
        Ok(rows) => {
            let mut stuck = 0u64;
            let mut hidden = 0u64;
            let mut clients = BTreeMap::new();
            for row in rows {
                let client: String = row.try_get("client").unwrap_or_default();
                if only.is_some_and(|keep| !keep.contains(&client)) {
                    continue;
                }
                let n: i64 = row.try_get("n").unwrap_or(0);
                hidden += row.try_get::<i64, _>("hidden").unwrap_or(0) as u64;
                stuck += row.try_get::<i64, _>("stuck").unwrap_or(0) as u64;
                clients.insert(client, n as u64);
            }
            point.connections = Some(clients);
            if hidden == 0 {
                point.idle_in_transaction_over_60s = Some(stuck);
            } else {
                point.unknown.push(format!(
                    "idle_in_transaction_over_60s: {hidden} session state(s) are hidden from \
                     this role (grant pg_read_all_stats)"
                ));
            }
        }
        Err(error) => point.unknown.push(format!("connections: {error}")),
    }
    match sqlx::query_scalar::<_, Option<i64>>("SELECT sum(size)::bigint FROM pg_ls_waldir()")
        .fetch_one(pool)
        .await
    {
        Ok(bytes) => point.wal_bytes = bytes.map(|b| b as u64),
        Err(error) => point.unknown.push(format!(
            "wal_bytes: pg_ls_waldir() refused ({error}); grant pg_monitor"
        )),
    }
    match sqlx::query_scalar::<_, i64>("SELECT pg_database_size(current_database())")
        .fetch_one(pool)
        .await
    {
        Ok(bytes) => point.database_bytes = Some(bytes as u64),
        Err(error) => point.unknown.push(format!("database_bytes: {error}")),
    }
    match sqlx::query_scalar::<_, i64>("SELECT qbit_prism_share_next_seq()")
        .fetch_one(pool)
        .await
    {
        Ok(next) => point.next_share_seq = Some(next),
        Err(error) => point.unknown.push(format!("next_share_seq: {error}")),
    }
    match sqlx::query(
        "SELECT partition_name, state, lower_seq, upper_seq, \
         CASE WHEN to_regclass(partition_name) IS NULL THEN NULL \
              ELSE pg_total_relation_size(to_regclass(partition_name)) END AS bytes \
         FROM qbit_prism_share_partitions ORDER BY upper_seq",
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => {
            for row in rows {
                point.partitions.push(PartitionPoint {
                    name: row.try_get("partition_name").unwrap_or_default(),
                    state: row.try_get("state").unwrap_or_default(),
                    lower_seq: row.try_get("lower_seq").unwrap_or(None),
                    upper_seq: row.try_get("upper_seq").unwrap_or(0),
                    bytes: row
                        .try_get::<Option<i64>, _>("bytes")
                        .unwrap_or(None)
                        .map(|b| b as u64),
                });
            }
        }
        Err(error) => point.unknown.push(format!("partitions: {error}")),
    }
    // Partitions are reported above; every other relation of the schema by
    // total size, the largest twenty.
    match sqlx::query(
        "SELECT c.relname::text AS name, pg_total_relation_size(c.oid)::bigint AS bytes \
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relkind IN ('r','p') AND NOT c.relispartition \
         AND n.nspname = current_schema() AND c.relname NOT LIKE 'qbit_share_ledger_p%' \
         ORDER BY 2 DESC LIMIT 20",
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => {
            for row in rows {
                let name: String = row.try_get("name").unwrap_or_default();
                let bytes: i64 = row.try_get("bytes").unwrap_or(0);
                point.relations.insert(name, bytes as u64);
            }
        }
        Err(error) => point.unknown.push(format!("relations: {error}")),
    }
    match sqlx::query_scalar::<_, i64>("SELECT count(*)::bigint FROM qbit_prism_payout_divergences")
        .fetch_one(pool)
        .await
    {
        Ok(rows) => point.payout_divergences = Some(rows as u64),
        Err(error) => point.unknown.push(format!("payout_divergences: {error}")),
    }
    point
}
