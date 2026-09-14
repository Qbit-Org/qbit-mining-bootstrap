//! Ledger advisory-lock and pool-acquire wait observations (#328).
//! PRISM_TEST_DATABASE_URL=postgres://user@127.0.0.1:5432/postgres cargo test -p qbit-prism-server --test ledger_wait_metrics
use anyhow::{Context, Result};
use qbit_prism::AcceptedShare;
use qbit_prism_server::{
    ledger::{Ledger, SignerKeys},
    metrics::{collectors, Metrics},
};
use qbit_prism_test_gate as gate;
use serde_json::json;
use sqlx::{Connection, PgConnection, PgPool};
use std::{
    sync::{Arc, LazyLock},
    time::Duration,
};
use uuid::Uuid;

/// `ORDER_LOCK` is scoped to the database, not to a test's throwaway schema,
/// and the failure cases here hold it from a second session. Every gated test
/// in this binary therefore runs one at a time.
/// The signing keys `configure` pins alongside the fingerprint since #265.
/// This suite measures lock and pool waits, not signing, so one fixed pair is
/// enough; it only has to be the same pair on every call.
fn signer_keys() -> SignerKeys {
    SignerKeys {
        manifest_key_hex: "11".repeat(32),
        ledger_key_hex: "22".repeat(32),
    }
}

static SERIAL: LazyLock<tokio::sync::Mutex<()>> = LazyLock::new(tokio::sync::Mutex::default);

/// A short `lock_timeout` keeps the failure cases fast while staying far above
/// the cancellation deadline they are distinguished from.
const LOCK_TIMEOUT_MS: u64 = 200;

/// The lock timeout is read by `Ledger::connect*` from the environment, which
/// is process-wide; this test binary is its own process, and both gated tests
/// want the same value. Forcing it before anything else reads the environment
/// keeps the one write ordered ahead of every read in the binary.
static LOCK_TIMEOUT: LazyLock<()> = LazyLock::new(|| {
    std::env::set_var(
        "PRISM_DATABASE_LOCK_TIMEOUT_MS",
        LOCK_TIMEOUT_MS.to_string(),
    );
});

/// `ORDER_LOCK`, from `ledger.rs`. A second session must name the key by value
/// because the ledger's constants are private to the crate.
const ORDER_LOCK: i64 = 0x505249534d000002;

const POOL_SUCCESS: &str = "qbit_prism_database_pool_acquire_seconds_count{result=\"success\"}";
const POOL_FAILURE: &str = "qbit_prism_database_pool_acquire_seconds_count{result=\"failure\"}";
const POOL_FAILURE_SUM: &str = "qbit_prism_database_pool_acquire_seconds_sum{result=\"failure\"}";

/// The ledger pools here are small enough to exhaust deliberately.
const POOL_CONNECTIONS: u32 = 4;

#[tokio::test]
async fn advisory_lock_and_pool_waits_are_recorded_by_outcome() -> Result<()> {
    LazyLock::force(&LOCK_TIMEOUT);
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let _serial = SERIAL.lock().await;
    let database = Database::open(&raw).await?;
    let mut pools = Vec::new();
    let result = recorded_by_outcome(&database, &mut pools).await;
    let cleanup = database.close(pools).await;
    result.and(cleanup)
}

async fn recorded_by_outcome(database: &Database, pools: &mut Vec<PgPool>) -> Result<()> {
    let metrics = Arc::new(Metrics::default());
    let ledger = Ledger::connect_with_metrics(
        &database.url,
        "wait-metrics".into(),
        POOL_CONNECTIONS,
        true,
        Some(metrics.clone()),
    )
    .await?;
    pools.push(ledger.pool.clone());

    // 1. Schema creation takes the migration lock. The fresh-schema cutover
    // also takes settlement and order once, so everything below compares
    // deltas rather than absolute counts.
    assert!(sample(&metrics.render(), &lock_count("migration", "success")) >= 1.);

    // 2. One share append takes ORDER_LOCK exactly once.
    let before = metrics.render();
    ledger.append(share(1), None).await?;
    let after = metrics.render();
    assert_eq!(delta(&before, &after, &lock_count("order", "success")), 1.);

    // 3. A second session holds ORDER_LOCK past the lock timeout. The session
    // is deliberately not from the ledger's pool, so the append cannot inherit
    // the holder's lock.
    let mut holder = PgConnection::connect(&database.url).await?;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(ORDER_LOCK)
        .execute(&mut holder)
        .await?;
    let before = metrics.render();
    let error = ledger
        .append(share(2), None)
        .await
        .err()
        .context("an append must fail while another session holds ORDER_LOCK")?;
    let failure = error
        .downcast_ref::<sqlx::Error>()
        .context("expected a PostgreSQL lock timeout")?;
    assert_eq!(
        failure
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("55P03")
    );
    let after = metrics.render();
    assert_eq!(delta(&before, &after, &lock_count("order", "failure")), 1.);
    assert!(
        delta(&before, &after, &lock_sum("order", "failure")) >= LOCK_TIMEOUT_MS as f64 / 1000.,
        "a timed-out wait must be attributed its whole duration"
    );
    assert_eq!(delta(&before, &after, &lock_count("order", "success")), 0.);

    // 4. Still held: a cancelled wait really waited, and is never dropped.
    // The deadline also covers the pool acquisition and the BEGIN round trip
    // that precede the lock wait, so an idle connection is warmed first and the
    // deadline is set well above the 0.05 s the recorded wait must exceed. It
    // stays well below the lock timeout, so this is a cancellation, not an
    // expiry, which the upper bound asserts.
    warm_pool(&ledger).await?;
    let before = metrics.render();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), ledger.append(share(3), None))
            .await
            .is_err(),
        "the append must still be waiting when the deadline elapses"
    );
    let after = metrics.render();
    assert_eq!(delta(&before, &after, &lock_count("order", "failure")), 1.);
    let cancelled = delta(&before, &after, &lock_sum("order", "failure"));
    assert!(cancelled >= 0.05, "a cancelled wait recorded {cancelled}s");
    assert!(
        cancelled < LOCK_TIMEOUT_MS as f64 / 1000.,
        "a cancelled wait must not be attributed a whole lock timeout"
    );

    // 5 is deliberately absent. That a metrics guard is never held across the
    // wait is enforced at compile time, not here: holding one would make the
    // future `!Send`, which the coordinator's `tokio::spawn` of the share path
    // rejects. A timing assertion could only restate that more weakly.

    // 6. Releasing the holder lets the next append through.
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(ORDER_LOCK)
        .execute(&mut holder)
        .await?;
    holder.close().await?;
    let before = metrics.render();
    ledger.append(share(5), None).await?;
    let after = metrics.render();
    assert_eq!(delta(&before, &after, &lock_count("order", "success")), 1.);

    // 7. `save_job` takes SETTLEMENT_LOCK and nothing else.
    let revision = ledger.payout_revision().await?;
    let before = metrics.render();
    ledger
        .save_job(
            "wait-metrics-job",
            &json!({"template": "wait-metrics"}),
            revision,
            &"aa".repeat(32),
            60,
        )
        .await?;
    let after = metrics.render();
    assert_eq!(
        delta(&before, &after, &lock_count("settlement", "success")),
        1.
    );
    assert_eq!(delta(&before, &after, &lock_count("order", "success")), 0.);

    // 8. Every ledger transaction observes its pool acquisition exactly once,
    // and the collector's own observation is unchanged.
    let fingerprint = "wait-metrics-fingerprint";
    let before = metrics.render();
    for _ in 0..5 {
        ledger.configure(fingerprint, &signer_keys()).await?;
    }
    let after = metrics.render();
    assert_eq!(delta(&before, &after, POOL_SUCCESS), 5.);
    collectors::database(&ledger.pool, &metrics).await?;
    assert_eq!(delta(&after, &metrics.render(), POOL_SUCCESS), 1.);

    // 8b. A pool acquisition abandoned by cancellation is a failure too. The
    // direct acquisitions that exhaust the pool do not go through the timed
    // helper and record nothing, so the only observation is the cancelled one.
    let mut held = Vec::new();
    for _ in 0..POOL_CONNECTIONS {
        held.push(ledger.pool.acquire().await?);
    }
    let before = metrics.render();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(100),
            ledger.configure(fingerprint, &signer_keys())
        )
        .await
        .is_err(),
        "the transaction must still be waiting for a connection"
    );
    let after = metrics.render();
    assert_eq!(delta(&before, &after, POOL_FAILURE), 1.);
    assert_eq!(delta(&before, &after, POOL_SUCCESS), 0.);
    assert!(
        delta(&before, &after, POOL_FAILURE_SUM) >= 0.05,
        "a cancelled acquisition must carry the wait it actually spent"
    );
    // Cancellation released the waiter: the pool works again once freed.
    drop(held);
    let before = metrics.render();
    ledger.configure(fingerprint, &signer_keys()).await?;
    assert_eq!(delta(&before, &metrics.render(), POOL_SUCCESS), 1.);

    // 9. A failed acquisition is recorded as a failure, not lost. Last,
    // because it closes the pool.
    ledger.pool.close().await;
    let before = metrics.render();
    assert!(ledger.configure(fingerprint, &signer_keys()).await.is_err());
    assert_eq!(delta(&before, &metrics.render(), POOL_FAILURE), 1.);
    Ok(())
}

#[tokio::test]
async fn unattached_ledger_keeps_results_and_error_shape() -> Result<()> {
    LazyLock::force(&LOCK_TIMEOUT);
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let _serial = SERIAL.lock().await;
    let database = Database::open(&raw).await?;
    let mut pools = Vec::new();
    let result = unattached(&database, &mut pools).await;
    let cleanup = database.close(pools).await;
    result.and(cleanup)
}

/// The same sequence as the recorded test, on a ledger with no handle: every
/// result and every error must be what it is at the base. There is deliberately
/// no assertion about a registry here, because a registry no ledger can reach
/// has nothing to record into and could not fail one.
async fn unattached(database: &Database, pools: &mut Vec<PgPool>) -> Result<()> {
    let ledger = Ledger::connect(
        &database.url,
        "wait-metrics-unattached".into(),
        POOL_CONNECTIONS,
        true,
    )
    .await?;
    pools.push(ledger.pool.clone());

    ledger.append(share(1), None).await?;

    let mut holder = PgConnection::connect(&database.url).await?;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(ORDER_LOCK)
        .execute(&mut holder)
        .await?;
    let error = ledger
        .append(share(2), None)
        .await
        .err()
        .context("an append must fail while another session holds ORDER_LOCK")?;
    let failure = error
        .downcast_ref::<sqlx::Error>()
        .context("expected a PostgreSQL lock timeout")?;
    assert_eq!(
        failure
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("55P03")
    );

    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(ORDER_LOCK)
        .execute(&mut holder)
        .await?;
    holder.close().await?;
    ledger.append(share(3), None).await?;

    let revision = ledger.payout_revision().await?;
    ledger
        .save_job(
            "wait-metrics-job",
            &json!({"template": "wait-metrics"}),
            revision,
            &"aa".repeat(32),
            60,
        )
        .await?;

    Ok(())
}

/// Every ledger transaction and advisory lock has to go through the timed
/// helpers, or a new call site would silently stop being observed.
#[test]
fn ledger_transactions_and_advisory_locks_use_the_timed_helpers() -> Result<()> {
    let root = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/src"));
    let mut sources = vec![root.join("ledger.rs")];
    // Recursive: a nested module directory is exactly where an untimed
    // transaction would hide from a single-level scan.
    let mut pending = vec![root.join("ledger")];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory)? {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
                continue;
            }
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default()
                .to_owned();
            if name.ends_with(".rs") && !name.ends_with("tests.rs") {
                sources.push(path);
            }
        }
    }
    let mut advisory = Vec::new();
    for path in &sources {
        let name = path
            .strip_prefix(root)
            .unwrap_or(path)
            .to_string_lossy()
            .into_owned();
        for (index, line) in std::fs::read_to_string(path)?.lines().enumerate() {
            assert!(
                !line.contains("pool.begin()"),
                "{name}:{}: ledger transactions must begin through the timed helper",
                index + 1
            );
            for _ in line.matches("pg_advisory_xact_lock") {
                advisory.push(format!("{name}:{}", index + 1));
            }
        }
    }
    assert_eq!(
        advisory.len(),
        1,
        "every advisory lock must go through the one timed helper, found {advisory:?}"
    );
    assert!(
        advisory[0].starts_with("ledger/connect.rs:"),
        "the one advisory lock statement must be the timed helper's, found {}",
        advisory[0]
    );
    Ok(())
}

struct Database {
    admin: PgPool,
    schema: String,
    url: String,
}

impl Database {
    async fn open(raw: &str) -> Result<Self> {
        let admin = PgPool::connect(raw).await?;
        let schema = format!("prism_wait_metrics_{}", Uuid::new_v4().simple());
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

    async fn close(self, pools: Vec<PgPool>) -> Result<()> {
        for pool in pools {
            pool.close().await;
        }
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await?;
        self.admin.close().await;
        Ok(())
    }
}

/// Open and check every pool slot, then let the connections settle back into
/// the idle queue. The lock-timeout error in step 3 leaves its connection
/// unusable, and replacing it costs tens of milliseconds; a cancellation
/// deadline must measure the lock wait, not that replacement.
async fn warm_pool(ledger: &Ledger) -> Result<()> {
    let mut warm = Vec::new();
    for _ in 0..POOL_CONNECTIONS {
        warm.push(ledger.pool.acquire().await?);
    }
    drop(warm);
    tokio::time::sleep(Duration::from_millis(20)).await;
    Ok(())
}

fn lock_count(lock: &str, result: &str) -> String {
    format!("qbit_prism_database_advisory_lock_wait_seconds_count{{lock=\"{lock}\",result=\"{result}\"}}")
}

fn lock_sum(lock: &str, result: &str) -> String {
    format!(
        "qbit_prism_database_advisory_lock_wait_seconds_sum{{lock=\"{lock}\",result=\"{result}\"}}"
    )
}

/// A rendered sample, counting a series with no observations yet as the zero
/// observations it is: an unobserved histogram renders no lines at all.
fn sample(body: &str, key: &str) -> f64 {
    body.lines()
        .find_map(|line| line.strip_prefix(&format!("{key} ")))
        .map_or(0., |value| value.parse().unwrap())
}

fn delta(before: &str, after: &str, key: &str) -> f64 {
    sample(after, key) - sample(before, key)
}

fn share(id: u64) -> AcceptedShare {
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
