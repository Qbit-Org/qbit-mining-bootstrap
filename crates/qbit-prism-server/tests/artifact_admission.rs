//! Admission on the public artifact route (#267, deferred half).
//!
//! `/public/v1/artifacts/<sha256>` serves a block's audit either from sealed
//! canonical bytes, which it digests and parses, or by rebuilding an unsealed
//! native block's window from the share ledger. Both are proportional to the
//! payout window and both outlive the read connection, so they run under their
//! own limit (`PRISM_PUBLIC_AUDIT_REBUILD_CONCURRENCY`) rather than the read
//! concurrency, behind a per-route in-flight cap
//! (`PRISM_PUBLIC_AUDIT_ARTIFACT_MAX_IN_FLIGHT`) that refuses the next
//! distinct request at once instead of letting it spend its read deadline
//! queued.
//!
//! ```text
//! PRISM_TEST_DATABASE_URL=postgresql://user@127.0.0.1:5432/postgres \
//!   cargo test --locked -p qbit-prism-server --test artifact_admission
//! ```
//!
//! The load criterion runs at `PRISM_ARTIFACT_ADMISSION_SHARES` shares
//! (default 5,000; the count must divide the 8,000,000 window weight) for
//! `PRISM_ARTIFACT_ADMISSION_SECONDS` seconds (default 12). The `#[ignore]`
//! variant is the same test at a production-shaped window:
//!
//! ```text
//! PRISM_TEST_DATABASE_URL=... PRISM_ARTIFACT_ADMISSION_SHARES=100000 \
//!   cargo test --locked -p qbit-prism-server --test artifact_admission \
//!   -- --ignored --nocapture artifact_route_load_at_a_large_window
//! ```
use anyhow::{ensure, Context, Result};
use axum::{
    body::Body,
    http::{Request, StatusCode},
    routing::post,
    Json, Router,
};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{
    build_audit_bundle, canonical_audit_bundle_bytes, AcceptedShare, AuditBundle, FoundBlock,
    PayoutPolicy,
};
use qbit_prism_server::{
    api::{public_service, router, ApiConfig, ApiState},
    ledger::{Candidate, CandidateClaim, Ledger, ShareRange, SignerKeys, Snapshot, WindowRef},
    metrics::Metrics,
};
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tower::ServiceExt;

#[allow(dead_code)]
#[path = "support/window_fixture.rs"]
mod window_fixture;
use window_fixture::WindowPlan;

#[path = "support/ledger_database.rs"]
#[allow(dead_code)]
mod ledger_database;
use ledger_database::FixtureDatabase;

#[path = "support/ledger_execution_proxy.rs"]
mod execution_proxy;
use execution_proxy::ExecutionProxy;

// ---------------------------------------------------------------------------
// Fixture: one database, one ledger, one fake node
// ---------------------------------------------------------------------------

/// The landing recipe below is `tests/audit_body_normalization.rs`'s, which is
/// in turn `tests/ledger_postgres.rs`'s: an 80-byte header whose double
/// SHA-256 is the candidate's `block_hash`, with the verified coinbase
/// transaction as the block's first transaction.
struct Fixture {
    database: FixtureDatabase,
    ledger: Ledger,
    /// A default-size pool on the fixture database, separate from the ledger's
    /// and from the API's: the tests below watch `pg_stat_activity` and take
    /// table locks through it.
    admin: PgPool,
    rpc: String,
    node: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        Ok(Some(Self::open_raw(&raw).await?))
    }

    /// The `#[ignore]` load variant is asked for explicitly, so a missing URL
    /// fails it whatever the switch says.
    async fn open_required() -> Result<Self> {
        Self::open_raw(&gate::required_database_url(gate::site!())?).await
    }

    async fn open_raw(raw: &str) -> Result<Self> {
        let database = FixtureDatabase::open(raw, "artifact_admission_").await?;
        let opened = async {
            let ledger = Ledger::connect(&database.url, "artifact-admission".into(), 8, true)
                .await
                .context("fixture ledger")?;
            let admin = PgPool::connect(&database.url).await?;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let rpc = format!("http://{}/", listener.local_addr()?);
            let node = tokio::spawn(async move {
                let app = Router::new().route("/", post(node_reply));
                let _ = axum::serve(listener, app).await;
            });
            Ok::<_, anyhow::Error>((ledger, admin, rpc, node))
        }
        .await;
        match opened {
            Ok((ledger, admin, rpc, node)) => Ok(Self {
                database,
                ledger,
                admin,
                rpc,
                node,
            }),
            Err(error) => Err(database.abandon(error).await),
        }
    }

    fn config(&self) -> ApiConfig {
        ApiConfig {
            rpc_url: self.rpc.clone(),
            ..ApiConfig::default()
        }
    }

    fn state(&self, config: ApiConfig) -> ApiState {
        ApiState::new(
            self.ledger.pool.clone(),
            config,
            Arc::new(Metrics::default()),
        )
    }

    /// EP-ERRORS: the database goes away on success and on failure alike.
    async fn close(self, result: Result<()>) -> Result<()> {
        self.node.abort();
        self.admin.close().await;
        self.ledger.pool.close().await;
        self.database.close(result).await
    }
}

async fn node_reply(Json(input): Json<Value>) -> Json<Value> {
    let result = match input["method"].as_str().unwrap_or("") {
        "getblockchaininfo" => {
            json!({"chain":"regtest","blocks":100,"bestblockhash":"ab".repeat(32),"initialblockdownload":false})
        }
        "getblocktemplate" => json!({"bits":"207fffff","coinbasevalue":5000000000u64}),
        "getnetworkinfo" => json!({"connections":2}),
        "getnetworkhashps" => json!("1234567890123.125"),
        _ => Value::Null,
    };
    Json(json!({"id":input["id"],"result":result,"error":null}))
}

fn keys() -> (ManifestSigningKey, ManifestSigningKey) {
    (
        ManifestSigningKey::from_seed_hex(&"52".repeat(32)).unwrap(),
        ManifestSigningKey::from_seed_hex(&"53".repeat(32)).unwrap(),
    )
}

fn ledger_public_key() -> String {
    keys().1.public_key_hex()
}

fn candidate_with_bundle(
    bundle: &AuditBundle,
    window: WindowRef,
    revision: i64,
) -> Result<Candidate> {
    let report =
        qbit_prism::verify_audit_bundle_with_ledger_public_key(bundle, &ledger_public_key())?;
    let mut block = vec![0u8; 80];
    block[..4].copy_from_slice(&0x2000_0000u32.to_le_bytes());
    block[4..36].fill(0x26);
    let mut txid = hex::decode(&report.coinbase_txid)?;
    txid.reverse();
    block[36..68].copy_from_slice(&txid);
    block[68..72].copy_from_slice(&1_800_000_000u32.to_le_bytes());
    block[72..76].copy_from_slice(&0x207f_ffffu32.to_le_bytes());
    block[76..80].copy_from_slice(&0x0267u32.to_le_bytes());
    let mut hash = Sha256::digest(Sha256::digest(&block)).to_vec();
    hash.reverse();
    block.push(1);
    block.extend(hex::decode(&report.coinbase_tx_hex)?);
    let (coinbase_key, ledger_key) = keys();
    Ok(Candidate {
        block_hash: hex::encode(hash),
        block_sha256: Candidate::block_digest_hex(&block),
        job_id: "artifact-admission-job".into(),
        payout_revision: revision,
        window,
        bootstrap_share: None,
        found_block: bundle.found_block.clone(),
        payout_policy: bundle.payout_policy.clone(),
        ctv: None,
        audit_builder_version: qbit_prism::AUDIT_BUILDER_VERSION,
        signer_keys: SignerKeys::of(&coinbase_key, &ledger_key),
        leased: false,
        coinbase_suffix_hex: bundle
            .coinbase_script_sig_suffix_hex
            .clone()
            .unwrap_or_else(|| "00".repeat(12)),
        deferred_share: None,
        block_bytes: block,
        as_issued_balances: Vec::new(),
    })
}

fn window_ref_for(shares: &[AcceptedShare], snapshot: &Snapshot) -> Result<WindowRef> {
    let range = match (shares.first(), shares.last()) {
        (Some(first), Some(last)) => Some(ShareRange {
            first_share_seq: first.share_seq,
            last_share_seq: last.share_seq,
            share_count: u64::try_from(shares.len())?,
            snapshot_sha256: Sha256::digest(serde_json::to_vec(shares)?).into(),
        }),
        _ => None,
    };
    Ok(WindowRef {
        anchor_ms: snapshot.anchor_ms,
        prior_balances_digest: qbit_prism::prior_balances_digest(&snapshot.prior_balances),
        shares: range,
    })
}

/// One landed native block whose audit row is unsealed: every artifact read
/// of it rebuilds the window.
struct Landed {
    block_hash: String,
    /// The canonical artifact the candidate was built from, which the route
    /// must reproduce byte for byte.
    canonical: Vec<u8>,
    sha256: String,
}

/// Load `plan`'s window, land it through the production path, and leave the
/// ledger one share past the landed block's anchored range, so a read that
/// rebuilt from present-day state instead of the snapshot would differ.
async fn land_window(ledger: &Ledger, plan: &WindowPlan) -> Result<Landed> {
    plan.load(&ledger.pool, "artifact-admission").await?;
    let snapshot = ledger.snapshot(plan.window_network_difficulty()).await?;
    ensure!(
        snapshot.shares.len() as u64 == plan.share_count(),
        "snapshot window is {} shares, expected {}",
        snapshot.shares.len(),
        plan.share_count()
    );
    let reference = window_ref_for(&snapshot.shares, &snapshot)?;
    let revision = snapshot.payout_revision;
    let anchor_ms = snapshot.anchor_ms;
    let (coinbase_key, ledger_key) = keys();
    let bundle = build_audit_bundle(
        snapshot.shares,
        FoundBlock {
            block_height: 101,
            coinbase_value_sats: 5_000_000_000,
            network_difficulty: plan.window_network_difficulty(),
            anchor_job_issued_at_ms: anchor_ms,
        },
        snapshot.prior_balances,
        PayoutPolicy::day_one_default(),
        &coinbase_key,
        &ledger_key,
    )?;
    let canonical = canonical_audit_bundle_bytes(&bundle)?;
    let sha256 = hex::encode(Sha256::digest(&canonical));
    let candidate = candidate_with_bundle(&bundle, reference, revision)?;
    let block_hash = candidate.block_hash.clone();
    let mut winning = plan.share(plan.share_count() + 1);
    winning.share_seq = 0;
    winning.share_id = format!("artifact-admission:winning-share:{block_hash}");
    winning.job_issued_at_ms = 1;
    winning.accepted_at_ms = 0;
    ledger.append(winning, Some(candidate.clone())).await?;
    let claim: CandidateClaim = ledger
        .claim_candidate(600)
        .await?
        .context("claim found no pending candidate")?
        .with_bundle(bundle);
    ledger
        .land_candidate(&claim, &ledger_key.public_key_hex())
        .await?;
    Ok(Landed {
        block_hash,
        canonical,
        sha256,
    })
}

/// Store a block's canonical bytes, exactly as `archive::seal` does.
async fn seal(pool: &PgPool, landed: &Landed) -> Result<()> {
    let rows = sqlx::query(
        "UPDATE qbit_pool_audit_bundles SET canonical_audit_bytes=$2 WHERE block_hash=$1 \
         AND canonical_audit_bytes IS NULL AND audit_bundle_sha256=$3",
    )
    .bind(&landed.block_hash)
    .bind(landed.canonical.as_slice())
    .bind(&landed.sha256)
    .execute(pool)
    .await?
    .rows_affected();
    ensure!(rows == 1, "sealing the landed block wrote {rows} rows");
    Ok(())
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

struct Reply {
    status: StatusCode,
    headers: axum::http::HeaderMap,
    bytes: Vec<u8>,
}

impl Reply {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.bytes).unwrap_or(Value::Null)
    }
}

async fn fetch(app: &Router, path: &str) -> Reply {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .expect("router reply");
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 30)
        .await
        .expect("response body")
        .to_vec();
    Reply {
        status,
        headers,
        bytes,
    }
}

fn artifact_path(sha256: &str) -> String {
    format!("/public/v1/artifacts/{sha256}")
}

/// Poll until `condition` holds, or fail after `limit`.
async fn until(limit: Duration, what: &str, mut condition: impl FnMut() -> bool) -> Result<()> {
    let deadline = Instant::now() + limit;
    while Instant::now() < deadline {
        if condition() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    ensure!(condition(), "timed out waiting for {what}");
    Ok(())
}

fn environment_number(name: &str, default: u64) -> Result<u64> {
    match std::env::var(name) {
        Ok(raw) => raw
            .trim()
            .parse()
            .with_context(|| format!("{name}={raw:?} is not a number")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error).context(format!("{name} is not readable")),
    }
}

/// `/proc/self/status` in KiB; `None` when the kernel does not report it.
fn status_kib(field: &str) -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix(field))
        .and_then(|rest| rest.trim().strip_suffix("kB"))
        .and_then(|kib| kib.trim().parse().ok())
}

/// Writing `5` to `clear_refs` resets `VmHWM`, so the next reading covers only
/// the phase that follows. It measures the test process, not PostgreSQL.
fn reset_peak_rss() -> bool {
    std::fs::write("/proc/self/clear_refs", "5").is_ok()
}

fn peak_rss_kib(reset: bool) -> Option<u64> {
    reset.then(|| status_kib("VmHWM:")).flatten()
}

// ---------------------------------------------------------------------------
// B1: the load criterion
// ---------------------------------------------------------------------------

/// Requests per second the load criterion drives at the artifact route.
const ARTIFACT_RATE: u64 = 50;
/// Sealed artifacts of the same size as the landed block's, one distinct
/// content address each. Identical addresses collapse into one computation in
/// the response cache, so distinct ones are what make the load real work; the
/// cycle below is wider than the rebuild limit and narrower than the in-flight
/// cap, so the route both queues and, at these rates, admits every request.
const DECOYS: u8 = 16;

async fn artifact_route_load(f: &Fixture, shares: u64, seconds: u64) -> Result<()> {
    let plan = WindowPlan::new(shares)?;
    let landed = land_window(&f.ledger, &plan).await?;
    let canonical_len = landed.canonical.len();

    // The public read pool, the rebuild limit and the in-flight cap are the
    // shipped defaults; only the fake node URL differs.
    let state = f.state(f.config());
    let app = router(state.clone());
    // Pool summary is sampled through a second view of the same state, with
    // the response cache off, so every sample is a real read of the same
    // public pool rather than a cached body.
    let mut uncached = state.clone();
    uncached.config = Arc::new(ApiConfig {
        cache_enabled: false,
        ..f.config()
    });
    let summary_app = router(uncached);

    // One isolated rebuild, and one isolated decode of the same artifact once
    // it is sealed, with the test process's peak resident memory across each:
    // the per-rebuild figure the storage sizing bound is built from.
    let rebuild_reset = reset_peak_rss();
    let rebuild_before = status_kib("VmRSS:");
    let clock = Instant::now();
    let reply = fetch(&app, &artifact_path(&landed.sha256)).await;
    let rebuild_seconds = clock.elapsed().as_secs_f64();
    let rebuild_peak = peak_rss_kib(rebuild_reset);
    ensure!(
        reply.status == StatusCode::OK && reply.bytes == landed.canonical,
        "unsealed artifact read failed: {}",
        reply.status
    );
    seal(&f.ledger.pool, &landed).await?;
    let decode_reset = reset_peak_rss();
    let decode_before = status_kib("VmRSS:");
    let clock = Instant::now();
    let reply = fetch(&app, &artifact_path(&landed.sha256)).await;
    let decode_seconds = clock.elapsed().as_secs_f64();
    let decode_peak = peak_rss_kib(decode_reset);
    ensure!(
        reply.status == StatusCode::OK && reply.bytes == landed.canonical,
        "sealed artifact read failed: {}",
        reply.status
    );
    // Back to the unsealed shape: the load below must rebuild, which is the
    // expensive read this route has to survive.
    sqlx::query(
        "UPDATE qbit_pool_audit_bundles SET canonical_audit_bytes=NULL WHERE block_hash=$1",
    )
    .bind(&landed.block_hash)
    .execute(&f.ledger.pool)
    .await?;

    // Distinct hashes matter: identical ones collapse into one computation in
    // the response cache. Each sealed decoy carries a full-size artifact of
    // its own, so a distinct hash is real work rather than a 404.
    let mut hashes = vec![landed.sha256.clone()];
    for index in 0..DECOYS {
        let mut bytes = landed.canonical.clone();
        bytes.extend(vec![b' '; usize::from(index) + 1]);
        let sha256 = hex::encode(Sha256::digest(&bytes));
        let block_hash = format!("{index:02x}").repeat(32);
        sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state,found_at,maturity_state) VALUES($1,$2,repeat('0',64),repeat('1',64),repeat('2',64),'confirmed',clock_timestamp(),'immature')")
            .bind(&block_hash)
            .bind(200 + i64::from(index))
            .execute(&f.ledger.pool)
            .await?;
        sqlx::query("INSERT INTO qbit_pool_audit_bundles(block_hash,audit_bundle,audit_bundle_sha256,canonical_audit_bytes,coinbase_tx_hex) VALUES($1,$2,$3,$4,'00')")
            .bind(&block_hash)
            .bind(json!({"schema":"decoy"}))
            .bind(&sha256)
            .bind(bytes.as_slice())
            .execute(&f.ledger.pool)
            .await?;
        hashes.push(sha256);
    }

    // The load runs as its own task, so pool-summary is sampled while the
    // artifact route is under it rather than after it drains.
    let statuses: Arc<Mutex<BTreeMap<u16, u64>>> = Arc::default();
    let requests = ARTIFACT_RATE * seconds;
    let interval = Duration::from_micros(1_000_000 / ARTIFACT_RATE);
    let started = Instant::now();
    let driver = {
        let statuses = statuses.clone();
        let app = app.clone();
        let hashes = hashes.clone();
        tokio::spawn(async move {
            let mut load = tokio::task::JoinSet::new();
            for index in 0..requests {
                let app = app.clone();
                let path = artifact_path(&hashes[(index % hashes.len() as u64) as usize]);
                let statuses = statuses.clone();
                load.spawn(async move {
                    let reply = fetch(&app, &path).await;
                    *statuses
                        .lock()
                        .expect("status counts")
                        .entry(reply.status.as_u16())
                        .or_default() += 1;
                });
                tokio::time::sleep_until(
                    (started + interval * u32::try_from(index + 1).expect("request index")).into(),
                )
                .await;
            }
            load.join_all().await;
        })
    };
    let mut samples = Vec::new();
    let mut summary_failures = 0u64;
    while !driver.is_finished() || samples.is_empty() {
        let clock = Instant::now();
        let reply = fetch(&summary_app, "/public/v1/pool-summary").await;
        let elapsed = clock.elapsed();
        if reply.status != StatusCode::OK {
            summary_failures += 1;
            eprintln!(
                "[load] pool-summary answered {} after {:.3} s: {}",
                reply.status,
                elapsed.as_secs_f64(),
                reply.json()
            );
        }
        samples.push(elapsed);
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    driver.await?;
    let elapsed = started.elapsed();

    samples.sort();
    let at = |q: f64| samples[((samples.len() - 1) as f64 * q) as usize];
    let counts = statuses.lock().expect("status counts").clone();
    let settings: Vec<(String, String)> = sqlx::query(
        "SELECT name,setting FROM pg_settings WHERE name IN \
         ('max_connections','shared_buffers','work_mem','server_version')",
    )
    .fetch_all(&f.admin)
    .await?
    .into_iter()
    .map(|row| (row.get("name"), row.get("setting")))
    .collect();
    let deadline = state.config.read_timeout;
    eprintln!(
        "[load n={shares}] artifact route at {ARTIFACT_RATE} req/s for {:.1} s: {} requests by \
         status {:?}, {} distinct hashes ({} unsealed rebuild, {} sealed decoys)",
        elapsed.as_secs_f64(),
        requests,
        counts,
        hashes.len(),
        1,
        hashes.len() - 1,
    );
    eprintln!(
        "[load n={shares}] pool-summary latency over {} samples: p50 {:.3} s, p99 {:.3} s, max \
         {:.3} s, read deadline {:.1} s, {summary_failures} non-200",
        samples.len(),
        at(0.5).as_secs_f64(),
        at(0.99).as_secs_f64(),
        samples.last().expect("one sample").as_secs_f64(),
        deadline.as_secs_f64(),
    );
    eprintln!(
        "[load n={shares}] canonical artifact {canonical_len} B ({:.1} B/share); one uncontended \
         rebuild {rebuild_seconds:.3} s, one sealed decode {decode_seconds:.3} s",
        canonical_len as f64 / shares as f64,
    );
    eprintln!(
        "[load n={shares}] test-process resident memory: {} before the rebuild, {} peak across it; \
         {} before the sealed decode, {} peak across it (VmHWM reset before each)",
        cell(rebuild_before),
        cell(rebuild_peak),
        cell(decode_before),
        cell(decode_peak),
    );
    eprintln!(
        "[load n={shares}] read_concurrency {}, audit rebuild concurrency {}, artifact in-flight \
         cap {}, PostgreSQL {settings:?}",
        state.config.read_concurrency,
        state.config.audit_rebuild_concurrency,
        state.config.audit_artifact_max_in_flight,
    );

    ensure!(
        summary_failures == 0,
        "pool-summary failed {summary_failures} of {} samples under artifact load",
        samples.len()
    );
    ensure!(
        *samples.last().expect("one sample") < deadline,
        "pool-summary reached {:.3} s, its read deadline is {:.1} s",
        samples.last().expect("one sample").as_secs_f64(),
        deadline.as_secs_f64()
    );
    ensure!(
        counts.keys().all(|status| [200, 503].contains(status)),
        "unexpected artifact statuses: {counts:?}"
    );
    Ok(())
}

fn cell(kib: Option<u64>) -> String {
    kib.map_or("unmeasured".to_owned(), |kib| {
        format!("{:.1} MiB", kib as f64 / 1024.0)
    })
}

fn load_shares(default: u64) -> Result<u64> {
    environment_number("PRISM_ARTIFACT_ADMISSION_SHARES", default)
}

fn load_seconds() -> Result<u64> {
    environment_number("PRISM_ARTIFACT_ADMISSION_SECONDS", 12)
}

/// #267: "Artifact route at 50 req/s does not push pool-summary past its read
/// deadline in a load test."
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn artifact_route_load_keeps_pool_summary_within_its_read_deadline() -> Result<()> {
    let Some(f) = Fixture::open().await? else {
        return Ok(());
    };
    let (shares, seconds) = (load_shares(5_000)?, load_seconds()?);
    let result = artifact_route_load(&f, shares, seconds).await;
    f.close(result).await
}

/// The same criterion at a production-shaped window; prints its numbers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "load harness; lands PRISM_ARTIFACT_ADMISSION_SHARES shares (default 100,000) and prints the numbers"]
async fn artifact_route_load_at_a_large_window() -> Result<()> {
    let f = Fixture::open_required().await?;
    let (shares, seconds) = (load_shares(100_000)?, load_seconds()?);
    let result = artifact_route_load(&f, shares, seconds).await;
    f.close(result).await
}

// ---------------------------------------------------------------------------
// B2: permit discipline
// ---------------------------------------------------------------------------

/// Run one read while the runtime's only blocking thread is occupied: the
/// request's deadline expires before its blocking job can start, and the job
/// that holds the permit outlives the request that asked for it.
async fn gate_a_read_and_let_it_time_out(
    app: &Router,
    path: &str,
    rebuilds: &Arc<tokio::sync::Semaphore>,
    shape: &str,
) -> Result<()> {
    let (release, blocked) = std::sync::mpsc::channel::<()>();
    let gate = tokio::task::spawn_blocking(move || {
        let _ = blocked.recv();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let clock = Instant::now();
    let reply = fetch(app, path).await;
    ensure!(
        reply.status == StatusCode::SERVICE_UNAVAILABLE
            && reply.json()["error"]["code"] == "read_timeout",
        "a {shape} read that outlives the deadline is a read timeout, got {}: {}",
        reply.status,
        reply.json()
    );
    ensure!(clock.elapsed() < Duration::from_secs(5), "deadline overrun");
    // The request is gone; the blocking job that holds the permit is not.
    ensure!(
        rebuilds.available_permits() == 0,
        "a cancelled {shape} read freed the rebuild permit before its blocking work ended"
    );
    let _ = release.send(());
    gate.await?;
    until(
        Duration::from_secs(10),
        "the rebuild permit to come back",
        || rebuilds.available_permits() == 1,
    )
    .await
}

async fn permit_discipline(f: &Fixture) -> Result<()> {
    let plan = WindowPlan::new(64)?;
    let landed = land_window(&f.ledger, &plan).await?;
    let state = f.state(ApiConfig {
        cache_enabled: false,
        read_concurrency: 1,
        read_timeout: Duration::from_millis(500),
        ..f.config()
    });
    let app = router(state.clone());
    let rebuilds = state.audit_rebuild_limit();
    let admission = state.audit_artifact_admission();
    let path = artifact_path(&landed.sha256);

    // Warm the read pool: its one connection, and any name resolution behind
    // it, must not want the blocking thread the gate below occupies.
    let reply = fetch(&app, &path).await;
    ensure!(reply.status == StatusCode::OK, "warm-up read failed");
    ensure!(
        rebuilds.available_permits() == 1
            && admission.available_permits() as u32 == state.config.audit_artifact_max_in_flight
    );

    // Both shapes hold the permit in their own blocking job: the rebuild of an
    // unsealed row, which the row still is, and the decode of a sealed one.
    // The rebuild's job is inside the materialization, so a permit kept by the
    // request frame instead would be freed here by the cancellation.
    gate_a_read_and_let_it_time_out(&app, &path, &rebuilds, "unsealed rebuild").await?;
    ensure!(
        admission.available_permits() as u32 == state.config.audit_artifact_max_in_flight,
        "the in-flight cap is released with the request"
    );
    seal(&f.ledger.pool, &landed).await?;
    gate_a_read_and_let_it_time_out(&app, &path, &rebuilds, "sealed decode").await?;
    ensure!(
        admission.available_permits() as u32 == state.config.audit_artifact_max_in_flight,
        "the in-flight cap is released with the request"
    );

    // A failed read releases the permit too.
    sqlx::query(
        "UPDATE qbit_pool_audit_bundles SET canonical_audit_bytes=decode(repeat('00',32),'hex') \
         WHERE block_hash=$1",
    )
    .bind(&landed.block_hash)
    .execute(&f.ledger.pool)
    .await?;
    let reply = fetch(&app, &path).await;
    ensure!(
        reply.status == StatusCode::INTERNAL_SERVER_ERROR,
        "corrupt sealed bytes must refuse the row, got {}",
        reply.status
    );
    until(
        Duration::from_secs(10),
        "a failed read to free its permit",
        || rebuilds.available_permits() == 1,
    )
    .await?;
    ensure!(admission.available_permits() as u32 == state.config.audit_artifact_max_in_flight);

    // Waiting for a rebuild slot spends the request's own deadline and ends
    // as a read timeout, not as an admission refusal.
    let held = rebuilds.clone().acquire_owned().await?;
    let clock = Instant::now();
    let reply = fetch(&app, &path).await;
    let waited = clock.elapsed();
    ensure!(
        reply.status == StatusCode::SERVICE_UNAVAILABLE
            && reply.json()["error"]["code"] == "read_timeout",
        "waiting past the deadline is a read timeout, got {}: {}",
        reply.status,
        reply.json()
    );
    ensure!(
        waited >= state.config.read_timeout && waited < state.config.read_timeout * 8,
        "the permit wait spent the request deadline, not more: {waited:?}"
    );
    drop(held);
    ensure!(admission.available_permits() as u32 == state.config.audit_artifact_max_in_flight);
    Ok(())
}

/// The rebuild permit is held by the blocking work, not by the request: a
/// cancelled read cannot free it early, a failed one always frees it, and
/// waiting for it is bounded by the request's own deadline.
#[test]
fn artifact_permits_outlive_cancellation_and_survive_failure() -> Result<()> {
    // One blocking thread makes "the job has not started yet" deterministic.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let Some(f) = Fixture::open().await? else {
            return Ok(());
        };
        let result = permit_discipline(&f).await;
        f.close(result).await
    })
}

// ---------------------------------------------------------------------------
// B3: the refusal
// ---------------------------------------------------------------------------

/// An over-cap request is refused after the route's two indexed point lookups
/// and before any audit read: 503 `audit_artifact_busy`, `Retry-After`, and
/// one count in the public service's refusal family.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_over_cap_artifact_request_is_refused_before_any_audit_read() -> Result<()> {
    let Some(f) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let plan = WindowPlan::new(64)?;
        let landed = land_window(&f.ledger, &plan).await?;
        // Every API statement crosses the observer, so "no audit read" is a
        // count of what the server was asked to run, not an inference.
        let raw = url::Url::parse(&f.database.url)?;
        let upstream = tokio::net::lookup_host((
            raw.host_str().context("database URL names a host")?,
            raw.port().unwrap_or(5432),
        ))
        .await?
        .next()
        .context("database host resolves")?;
        let proxy = ExecutionProxy::start(upstream).await?;
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&proxy.rewrite_url(&f.database.url)?)
            .await?;
        let state = ApiState::new(
            pool.clone(),
            ApiConfig {
                cache_enabled: false,
                audit_artifact_max_in_flight: 1,
                ..f.config()
            },
            Arc::new(Metrics::default()),
        );
        let (app, _service) = public_service::router(
            state.clone(),
            public_service::ServiceConfig {
                read_concurrency: 4,
                ..Default::default()
            },
        );
        let path = artifact_path(&landed.sha256);
        // The one admitted slot, taken as a concurrent request would.
        let admitted = state.audit_artifact_admission().try_acquire_owned()?;

        let mark = proxy.mark();
        let reply = fetch(&app, &path).await;
        ensure!(
            reply.status == StatusCode::SERVICE_UNAVAILABLE
                && reply.json()["error"]["code"] == "audit_artifact_busy",
            "over-cap request must be refused, got {}: {}",
            reply.status,
            reply.json()
        );
        ensure!(
            reply.headers["retry-after"] == "5" && reply.headers["cache-control"] == "no-store",
            "a refusal carries Retry-After and is never stored: {:?}",
            reply.headers
        );
        // Session settings the pool applies on checkout are not reads of this
        // request's subject; every other execution is.
        let executions: Vec<String> = proxy
            .executions_since(mark)?
            .into_iter()
            .map(|execution| execution.sql)
            .filter(|sql| !sql.contains("set_config"))
            .collect();
        ensure!(
            executions.len() == 2
                && executions[0].contains("qbit_ctv_fanout_sets")
                && executions[1].contains("audit_bundle_sha256=$1"),
            "a refused request runs the two point lookups and nothing else: {executions:?}"
        );
        ensure!(
            !executions
                .iter()
                .any(|sql| sql.contains("qbit_share_ledger")
                    || sql.contains("qbit_prism_audit_snapshots")
                    || sql.contains("canonical_audit_bytes")),
            "a refused request read the audit: {executions:?}"
        );

        let metrics = String::from_utf8(fetch(&app, "/metrics").await.bytes)?;
        ensure!(
            metrics.contains(
                "# TYPE qbit_prism_public_audit_artifact_refusals_total counter\n\
                 qbit_prism_public_audit_artifact_refusals_total 1\n"
            ) && metrics.contains("# HELP qbit_prism_public_audit_artifact_refusals_total "),
            "the refusal is counted with HELP and TYPE: {metrics}"
        );

        // The cap is a refusal, not a state change: the next request served
        // under a free slot succeeds and is byte-identical.
        drop(admitted);
        let reply = fetch(&app, &path).await;
        ensure!(
            reply.status == StatusCode::OK && reply.bytes == landed.canonical,
            "a refusal must not poison the route: {}",
            reply.status
        );
        let metrics = String::from_utf8(fetch(&app, "/metrics").await.bytes)?;
        ensure!(
            metrics.contains("qbit_prism_public_audit_artifact_refusals_total 1\n"),
            "a served request is not a refusal"
        );
        pool.close().await;
        proxy.finish().await
    }
    .await;
    f.close(result).await
}

/// A refusal is not a result: with the response cache on, neither the request
/// that is refused nor one sharing its computation may be served a stored or
/// shared success, and the first read after the cap frees up must compute the
/// artifact rather than find a refusal in the cache.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_artifact_is_never_cached_nor_shared_as_a_success() -> Result<()> {
    let Some(f) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let plan = WindowPlan::new(128)?;
        let landed = land_window(&f.ledger, &plan).await?;
        // The artifact is well under `cache_max_bytes`, so a success here is
        // cacheable: a cached refusal would be visible as a hit below.
        ensure!(
            landed.canonical.len() < ApiConfig::default().cache_max_bytes,
            "the fixture artifact must be small enough to be cached"
        );
        let state = f.state(ApiConfig {
            cache_enabled: true,
            cache_debug_headers: true,
            audit_artifact_max_in_flight: 1,
            ..f.config()
        });
        let app = router(state.clone());
        let path = artifact_path(&landed.sha256);
        let cache_state = |reply: &Reply| {
            reply.headers["x-prism-public-cache"]
                .to_str()
                .unwrap_or_default()
                .to_owned()
        };

        // The one slot, taken as a concurrent read of another artifact would.
        let held = state.audit_artifact_admission().try_acquire_owned()?;
        let mut refused = tokio::task::JoinSet::new();
        for _ in 0..4 {
            let app = app.clone();
            let path = path.clone();
            refused.spawn(async move { fetch(&app, &path).await });
        }
        // Requests for one artifact share a computation, so a follower is
        // answered by the leader's refusal; it must be that refusal.
        for reply in refused.join_all().await {
            ensure!(
                reply.status == StatusCode::SERVICE_UNAVAILABLE
                    && reply.json()["error"]["code"] == "audit_artifact_busy"
                    && reply.headers["cache-control"] == "no-store",
                "a refused or shared request must be the refusal, got {}: {}",
                reply.status,
                reply.json()
            );
        }
        // Still refused, and from the cap rather than from a stored refusal.
        let reply = fetch(&app, &path).await;
        ensure!(
            reply.status == StatusCode::SERVICE_UNAVAILABLE
                && reply.json()["error"]["code"] == "audit_artifact_busy",
            "the cap still refuses, got {}: {}",
            reply.status,
            reply.json()
        );

        drop(held);
        let reply = fetch(&app, &path).await;
        ensure!(
            reply.status == StatusCode::OK && reply.bytes == landed.canonical,
            "the read after the cap frees up must serve the artifact, got {}",
            reply.status
        );
        ensure!(
            cache_state(&reply) != "HIT",
            "the first read after a refusal came from the cache: {}",
            cache_state(&reply)
        );
        // The success is cacheable, which is what makes the assertion above a
        // real one rather than a route that never caches anything.
        let reply = fetch(&app, &path).await;
        ensure!(
            reply.status == StatusCode::OK
                && reply.bytes == landed.canonical
                && cache_state(&reply) == "HIT",
            "a served artifact is cached: {} {}",
            reply.status,
            cache_state(&reply)
        );
        Ok(())
    }
    .await;
    f.close(result).await
}

// ---------------------------------------------------------------------------
// B4: isolation from the read concurrency
// ---------------------------------------------------------------------------

/// Backends of the public read pool waiting on the locked snapshot table:
/// one per rebuild that is actually running.
async fn rebuilds_waiting(admin: &PgPool) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity WHERE application_name='prism-public-read' \
         AND state='active' AND wait_event_type='Lock' \
         AND query LIKE '%qbit_prism_audit_snapshots%'",
    )
    .fetch_one(admin)
    .await?)
}

/// With the read concurrency raised, at most
/// `PRISM_PUBLIC_AUDIT_REBUILD_CONCURRENCY` rebuilds run at once and the rest
/// wait without a read connection, so pool-summary keeps its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rebuilds_are_bounded_independently_of_the_read_concurrency() -> Result<()> {
    let Some(f) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        const READERS: u32 = 4;
        let plan = WindowPlan::new(64)?;
        let landed = land_window(&f.ledger, &plan).await?;
        for rebuild_concurrency in [1u32, 2] {
            let state = f
                .state(ApiConfig {
                    cache_enabled: false,
                    read_concurrency: READERS,
                    audit_rebuild_concurrency: rebuild_concurrency,
                    read_timeout: Duration::from_secs(4),
                    ..f.config()
                })
                .with_read_concurrency(READERS);
            let app = router(state.clone());
            // Warm every connection of the read pool, so a rebuild's wait is
            // the lock below and never connection setup.
            for _ in 0..READERS {
                ensure!(
                    fetch(&app, "/public/v1/pool-summary").await.status == StatusCode::OK,
                    "warm-up pool-summary failed"
                );
            }

            // A rebuild's first statement reads the snapshot row; holding the
            // table makes every admitted rebuild visibly in progress.
            let mut lock = f.admin.begin().await?;
            sqlx::query("LOCK TABLE qbit_prism_audit_snapshots IN ACCESS EXCLUSIVE MODE")
                .execute(&mut *lock)
                .await?;
            let mut reads = tokio::task::JoinSet::new();
            for _ in 0..READERS {
                let app = app.clone();
                let path = artifact_path(&landed.sha256);
                reads.spawn(async move { fetch(&app, &path).await.status });
            }
            // The observation is the database's, not the semaphore's: a
            // rebuild that is running holds a read connection and is blocked
            // on the snapshot table, whichever limit admitted it.
            let deadline = Instant::now() + Duration::from_secs(5);
            while rebuilds_waiting(&f.admin).await? == 0 {
                ensure!(Instant::now() < deadline, "no rebuild reached the snapshot");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
            let waiting = rebuilds_waiting(&f.admin).await?;
            ensure!(
                waiting == i64::from(rebuild_concurrency),
                "{waiting} rebuilds hold a read connection at rebuild concurrency \
                 {rebuild_concurrency} with read concurrency {READERS}"
            );
            ensure!(
                state.audit_rebuild_limit().available_permits() == 0,
                "the running rebuilds did not come from the artifact rebuild limit"
            );

            // The read pool still has connections for an ordinary read.
            let clock = Instant::now();
            let reply = fetch(&app, "/public/v1/pool-summary").await;
            let elapsed = clock.elapsed();
            ensure!(
                reply.status == StatusCode::OK,
                "pool-summary answered {} while {READERS} artifact reads were in flight: {}",
                reply.status,
                reply.json()
            );
            ensure!(
                elapsed < Duration::from_secs(1),
                "pool-summary took {elapsed:?} behind the artifact reads"
            );
            lock.rollback().await?;
            let statuses = reads.join_all().await;
            ensure!(
                statuses.iter().all(|status| *status == StatusCode::OK
                    || *status == StatusCode::SERVICE_UNAVAILABLE),
                "unexpected artifact statuses: {statuses:?}"
            );
        }
        Ok(())
    }
    .await;
    f.close(result).await
}

// ---------------------------------------------------------------------------
// B5: byte identity
// ---------------------------------------------------------------------------

/// Admission changes no byte of either artifact shape.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sealed_and_unsealed_artifacts_are_byte_identical_under_admission() -> Result<()> {
    let Some(f) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let plan = WindowPlan::new(128)?;
        let landed = land_window(&f.ledger, &plan).await?;
        let app = router(f.state(f.config()));
        let path = artifact_path(&landed.sha256);
        let unsealed = fetch(&app, &path).await;
        ensure!(
            unsealed.status == StatusCode::OK && unsealed.bytes == landed.canonical,
            "the rebuilt artifact is not the candidate's canonical bytes"
        );
        ensure!(
            unsealed.headers["etag"] == format!("\"{}\"", landed.sha256),
            "etag is the content address: {:?}",
            unsealed.headers["etag"]
        );
        ensure!(
            hex::encode(Sha256::digest(&unsealed.bytes)) == landed.sha256,
            "served bytes do not hash to the requested artifact"
        );
        seal(&f.ledger.pool, &landed).await?;
        // A fresh state, so the answer comes from the sealed row rather than
        // from the response cache.
        let app = router(f.state(f.config()));
        let sealed = fetch(&app, &path).await;
        ensure!(
            sealed.status == StatusCode::OK && sealed.bytes == unsealed.bytes,
            "the sealed artifact differs from the rebuilt one"
        );
        ensure!(
            f.ledger.audit_bundle(&landed.block_hash).await?.is_some(),
            "the audit bundle route still serves the block"
        );
        Ok(())
    }
    .await;
    f.close(result).await
}

// ---------------------------------------------------------------------------
// B6: the settings
// ---------------------------------------------------------------------------

/// Serializes the process environment between the cases below.
static ENVIRONMENT: Mutex<()> = Mutex::new(());

/// Both settings are validated where the process reads them, and the value
/// that passes validation is the one the runtime uses.
#[tokio::test]
async fn artifact_admission_settings_are_validated_at_the_entry_point() -> Result<()> {
    let _guard = ENVIRONMENT
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    for (name, valid, rejected, expected) in [
        (
            "PRISM_PUBLIC_AUDIT_REBUILD_CONCURRENCY",
            "3",
            ["0", "65", "two", "-1", "1.5"],
            "PRISM_PUBLIC_AUDIT_REBUILD_CONCURRENCY must be 1..64",
        ),
        (
            "PRISM_PUBLIC_AUDIT_ARTIFACT_MAX_IN_FLIGHT",
            "7",
            ["0", "4097", "many", "-1", "1.5"],
            "PRISM_PUBLIC_AUDIT_ARTIFACT_MAX_IN_FLIGHT must be 1..4096",
        ),
    ] {
        for value in rejected {
            std::env::set_var(name, value);
            let error = ApiConfig::from_public_env()
                .err()
                .with_context(|| format!("{name}={value:?} was accepted"))?;
            let message = format!("{error:#}");
            ensure!(
                message.contains(expected) || message.contains(name),
                "{name}={value:?} refused without naming its setting: {message}"
            );
            std::env::remove_var(name);
        }
        std::env::set_var(name, valid);
        let config = ApiConfig::from_public_env()?;
        let effective = match name {
            "PRISM_PUBLIC_AUDIT_REBUILD_CONCURRENCY" => config.audit_rebuild_concurrency,
            _ => config.audit_artifact_max_in_flight,
        };
        ensure!(
            effective.to_string() == valid,
            "{name}={valid} reached the runtime as {effective}"
        );
        std::env::remove_var(name);
    }
    // An empty value is unset everywhere in this configuration, not a parse
    // error, so it keeps the default rather than refusing startup.
    for name in [
        "PRISM_PUBLIC_AUDIT_REBUILD_CONCURRENCY",
        "PRISM_PUBLIC_AUDIT_ARTIFACT_MAX_IN_FLIGHT",
    ] {
        std::env::set_var(name, "  ");
        let config = ApiConfig::from_public_env()?;
        ensure!(
            config.audit_rebuild_concurrency == 1 && config.audit_artifact_max_in_flight == 32,
            "an empty {name} is unset, not a new value"
        );
        std::env::remove_var(name);
    }
    // The defaults keep the shipped behaviour of one rebuild at a time.
    let defaults = ApiConfig::from_public_env()?;
    ensure!(defaults.audit_rebuild_concurrency == 1 && defaults.audit_artifact_max_in_flight == 32);
    let state = ApiState::new(
        PgPoolOptions::new()
            .connect_lazy("postgres://invalid@127.0.0.1:1/invalid")
            .unwrap(),
        ApiConfig {
            audit_rebuild_concurrency: 3,
            audit_artifact_max_in_flight: 7,
            ..ApiConfig::default()
        },
        Arc::new(Metrics::default()),
    );
    ensure!(
        state.audit_rebuild_limit().available_permits() == 3
            && state.audit_artifact_admission().available_permits() == 7,
        "the validated values are the ones the state is built with"
    );
    ensure!(
        state
            .with_read_concurrency(16)
            .audit_rebuild_limit()
            .available_permits()
            == 3,
        "a larger read concurrency must not admit more rebuilds"
    );
    Ok(())
}
