//! Qualification measurements for the candidate window switch (#265, slice 3),
//! against a real PostgreSQL and in-process frontends.
//!
//! Two measurements live here; the claim-side ones that need the coordinator's
//! private lease (incident 1) and rebuild entry point (canonical identity) are
//! in `src/coordinator/window_incident_tests.rs`.
//!
//! * **Incident 2, ACK latency while one frontend solves a large-window block.**
//!   Two `Coordinator`s share one schema and one fake node that adopts the
//!   blocks it is sent. Both submit ordinary shares at a fixed rate through
//!   `MiningBackend::submit`; frontend 1 then submits a block-solving share
//!   whose window holds N shares, and its own `submit_loop` claims, offers,
//!   rebuilds and lands that block while both keep submitting.
//! * **`ORDER_LOCK` hold time on a solve does not depend on the window size.**
//!   A sampler on a connection outside every frontend pool watches `pg_locks`
//!   for `ORDER_LOCK` while a block-solving share is appended, at two window
//!   sizes and with no other traffic.
//!
//! # What "ACK latency" means here
//!
//! #271's socket-level load harness does not exist yet, so ACK latency is
//! measured **in process**: the wall-clock time of one `MiningBackend::submit`
//! call, which is everything the Stratum server waits on before it writes the
//! reply. It leaves out socket I/O, JSON framing and Stratum session work, and
//! both frontends share this test's tokio runtime rather than running as two
//! processes. The comparison is between phases of the same run, so both
//! omissions are the same on each side of it. #342 (`qbit-prism-load`) adds a
//! socket-level harness; these tests do not depend on it.
//!
//! # Acceptance is not the assertion
//!
//! The latency percentiles are. From #324 (`e13051cf`) on, a share-pass append
//! carrying a found-block candidate has its own `block_only_ack_timeout`, so
//! a slow solve no longer shows up as `ledger-confirmation-failed`; it shows
//! up as a late acknowledgement counted in
//! `qbit_prism_late_confirmed_shares_total`, or at worst as
//! `ledger-outcome-unknown`, and an "every share was accepted" check would
//! pass with the stall still present. This base carries `e13051cf`, so
//! incident 2 asserts that counter is zero on both frontends and that no ACK,
//! the solve included, is `ledger-outcome-unknown`.
//!
//! # Settings
//!
//! | variable | default | meaning |
//! | --- | --- | --- |
//! | `PRISM_WINDOW_QUALIFY_SHARES` | `20000` | window size of the reduced runs, and the larger size of the reduced lock-hold pair |
//! | `PRISM_WINDOW_QUALIFY_SMALL_SHARES` | `5000` | smaller size of the lock-hold pair, reduced and full |
//! | `PRISM_WINDOW_QUALIFY_FULL_SHARES` | `400000` | window size of the `#[ignore]`d full-size runs |
//!
//! Every size must divide the window weight (8,000,000) exactly; a malformed
//! value fails the test instead of falling back to the default.
//!
//! # Running it
//!
//! ```text
//! PRISM_TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5432/postgres \
//!   cargo test --locked -p qbit-prism-server --test candidate_window_qualification -- --nocapture
//! ```
//!
//! The 400,000-share variants need gigabytes of RAM and are ignored; select
//! them by name on a large host:
//!
//! ```text
//! PRISM_TEST_DATABASE_URL=... cargo test --locked -p qbit-prism-server \
//!   --test candidate_window_qualification -- --ignored --nocapture --exact \
//!   incident_2_ack_latency_at_full_size order_lock_hold_at_full_size
//! ```

use anyhow::{anyhow, bail, ensure, Context, Result};
use axum::{extract::State, routing::post, Json, Router};
use qbit_prism_server::{
    codec::{self, Submission},
    config::Config,
    coordinator::{Coordinator, JobContext},
    stratum::{MiningBackend, MiningJob, Worker},
};
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use sqlx::{Connection, PgConnection, PgPool};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, watch, Mutex},
    task::JoinHandle,
};

// Only `WindowPlan::new` and `load` are used here; the gate uses the rest.
#[allow(dead_code)]
#[path = "support/window_fixture.rs"]
mod window_fixture;

use window_fixture::WindowPlan;

/// The ledger's advisory locks are per database, not per schema, so every
/// test here serializes: a lock-hold sample must see only its own solve, and
/// a latency baseline must not include another test's load.
static TEST_LOCK: Mutex<()> = Mutex::const_new(());

/// `ORDER_LOCK` from `src/ledger.rs`. `pg_advisory_xact_lock(bigint)` stores
/// the key's high 32 bits in `pg_locks.classid`, its low 32 bits in `objid`,
/// and marks the one-bigint form with `objsubid = 1`.
const ORDER_LOCK: i64 = 0x5052_4953_4d00_0002;

const TEMPLATE_BITS: &str = "207fffff";
const EXTRANONCE2_SIZE: usize = 8;
const SHARE_COMMIT_TIMEOUT: Duration = Duration::from_secs(15);
/// "Well under the 15 s share-commit timeout": a third of it.
const ACK_CEILING: Duration = Duration::from_secs(5);
/// A share difficulty far below the network's, so almost every nonce passes
/// the share target while about half miss the block target.
const ORDINARY_DIFFICULTY: f64 = 1e-12;

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------

const SHARES_VAR: &str = "PRISM_WINDOW_QUALIFY_SHARES";
const SMALL_SHARES_VAR: &str = "PRISM_WINDOW_QUALIFY_SMALL_SHARES";
const FULL_SHARES_VAR: &str = "PRISM_WINDOW_QUALIFY_FULL_SHARES";

fn share_count(name: &str, default: u64) -> Result<u64> {
    let count = match std::env::var(name) {
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => bail!("{name} is unreadable: {error}"),
        Ok(raw) => raw
            .trim()
            .parse()
            .with_context(|| format!("{name}={raw:?} is not a share count"))?,
    };
    WindowPlan::new(count).with_context(|| format!("{name}={count} is not a usable window"))?;
    Ok(count)
}

// ---------------------------------------------------------------------------
// Fake node
// ---------------------------------------------------------------------------

struct Chain {
    /// Height to hash on the active chain, genesis included.
    hashes: BTreeMap<u64, String>,
    chainwork: u64,
    submissions: usize,
    /// When the first `submitblock` was adopted.
    adopted_at: Option<Instant>,
    /// Template bits served by `getblocktemplate`; a retarget changes them.
    bits: String,
}

impl Chain {
    fn height(&self) -> u64 {
        *self
            .hashes
            .keys()
            .next_back()
            .expect("genesis is always kept")
    }

    fn tip(&self) -> String {
        self.hashes[&self.height()].clone()
    }
}

/// One node both frontends talk to. `submitblock` adopts the block as the new
/// tip, as a node that accepts it would.
struct FakeNode {
    url: String,
    chain: Arc<Mutex<Chain>>,
    task: JoinHandle<()>,
}

impl Drop for FakeNode {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl FakeNode {
    async fn open() -> Result<Self> {
        let chain = Arc::new(Mutex::new(Chain {
            hashes: BTreeMap::from([(0, "00".repeat(32)), (100, "ab".repeat(32))]),
            chainwork: 1,
            submissions: 0,
            adopted_at: None,
            bits: TEMPLATE_BITS.to_owned(),
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/", listener.local_addr()?);
        let app = Router::new()
            .route("/", post(answer))
            .with_state(chain.clone());
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self { url, chain, task })
    }

    /// Serve `bits` from now on, as a per-block retarget does.
    async fn retarget(&self, bits: &str) {
        self.chain.lock().await.bits = bits.to_owned();
    }
}

async fn answer(State(chain): State<Arc<Mutex<Chain>>>, Json(request): Json<Value>) -> Json<Value> {
    let mut chain = chain.lock().await;
    let height = chain.height();
    let now = chrono::Utc::now().timestamp();
    let result = match request["method"].as_str().unwrap_or_default() {
        "getblockchaininfo" => json!({"chain":"test","initialblockdownload":false,
            "blocks":height,"headers":height,"bestblockhash":chain.tip(),
            "chainwork":format!("{:064x}",chain.chainwork)}),
        "getnetworkinfo" => json!({"connections":2}),
        "getbestblockhash" => json!(chain.tip()),
        "getblockhash" => request["params"][0]
            .as_u64()
            .and_then(|height| chain.hashes.get(&height))
            .map_or(Value::Null, |hash| json!(hash)),
        "getblockheader" => json!({"previousblockhash":"cd".repeat(32)}),
        "getblocktemplate" => json!({"height":height+1,"coinbasevalue":5_000_000_000u64,
            "previousblockhash":chain.tip(),"version":0x2000_0000u32,"bits":chain.bits,
            "curtime":now,"mintime":now-1,"transactions":[]}),
        "submitblock" => {
            let block = request["params"][0]
                .as_str()
                .and_then(|block| hex::decode(block).ok())
                .filter(|block| block.len() >= 80);
            let Some(block) = block else {
                return Json(json!({"id":request["id"],"result":null,
                    "error":{"code":-22,"message":"block decode failed"}}));
            };
            chain.submissions += 1;
            chain.hashes.insert(
                height + 1,
                codec::hash_display(&codec::double_sha256(&block[..80])),
            );
            chain.chainwork += 1;
            chain.adopted_at.get_or_insert_with(Instant::now);
            Value::Null
        }
        _ => {
            return Json(json!({"id":request["id"],"result":null,
                "error":{"code":-32601,"message":"unexpected RPC"}}))
        }
    };
    Json(json!({"id":request["id"],"result":result,"error":null}))
}

/// The `tests/support/fake_qbitd.rs` configuration, with every freshness
/// budget long enough that a full-size refresh cannot age the work out between
/// the two phases: submit admission then uses the published tip in both phases,
/// never an RPC in one and not the other.
fn frontend_config(database_url: &str, node: &FakeNode, instance_id: &str) -> Result<Config> {
    Ok(Config {
        database_url: database_url.to_owned(),
        instance_id: instance_id.to_owned(),
        database_connections: 8,
        initialize_schema: true,
        chain: "testnet".into(),
        expected_genesis_hash: None,
        min_peers: 1,
        template_max_age: Duration::from_secs(3600),
        submit_tip_max_age: Duration::from_secs(3600),
        template_refresh_failure_exit: Duration::from_secs(3600),
        rpc_url: node.url.clone(),
        rpc_user: "test".into(),
        rpc_password: "test".into(),
        rpc_timeout: Duration::from_secs(30),
        block_submit_timeout: Duration::from_secs(10),
        poll_interval: Duration::from_secs(1),
        blockwait: false,
        build_workers: 2,
        runtime_workers: 2,
        snapshot_interval: Duration::from_secs(3600),
        health_timeout: Duration::from_secs(3600),
        share_commit_timeout: SHARE_COMMIT_TIMEOUT,
        share_commit_grace: Duration::from_secs(5),
        block_only_ack_timeout: Duration::from_secs(60),
        candidate_orphan_confirmations: 6,
        capture_overpay_ceiling_bps: 100,
        extranonce2_size: EXTRANONCE2_SIZE,
        coinbase_tag: "/PRISM/".into(),
        manifest_seed: "11".repeat(32),
        ledger_seed: "22".repeat(32),
        ledger_public_key: qbit_pool_builder::ManifestSigningKey::from_seed_hex(&"22".repeat(32))?
            .public_key_hex(),
        username_fallback: None,
        payout_policy: qbit_prism::PayoutPolicy::day_one_default(),
        fee_address: None,
        ctv_enabled: false,
        ctv_config: qbit_prism::SettlementModeConfig::default(),
        ctv_direct_floor: 10_485_760,
        ctv_fee: None,
        ctv_fee_premium_bps: 12000,
        ctv_broadcast: false,
        ctv_broadcast_interval: Duration::from_secs(10),
        version_mask: codec::VERSION_ROLLING_MASK,
        audit_bind: "127.0.0.1".into(),
        audit_port: 0,
    })
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

struct Database {
    admin: PgPool,
    schema: String,
    url: String,
}

impl Database {
    async fn open(raw: &str) -> Result<Self> {
        let admin = PgPool::connect(raw).await?;
        let schema = format!("prism_window_qualify_{}", uuid::Uuid::new_v4().simple());
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

    /// Close the frontends' pools and drop the schema, every step bounded.
    async fn close(self, frontends: &[&Arc<Coordinator>]) -> Result<()> {
        for frontend in frontends {
            let _ =
                tokio::time::timeout(Duration::from_secs(10), frontend.ledger.pool.close()).await;
        }
        let dropped = tokio::time::timeout(
            Duration::from_secs(60),
            sqlx::query(&format!("DROP SCHEMA IF EXISTS {} CASCADE", self.schema))
                .execute(&self.admin),
        )
        .await;
        self.admin.close().await;
        dropped.map_err(|_| anyhow!("dropping {} did not finish within 60 s", self.schema))??;
        Ok(())
    }
}

/// The test's own failure wins; a cleanup failure is reported only on success.
fn settle<T>(outcome: Result<T>, cleanup: Result<()>) -> Result<T> {
    match (outcome, cleanup) {
        (Ok(value), cleanup) => cleanup.map(|()| value),
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(cleanup)) => Err(anyhow!("{error:#}\n(cleanup also failed: {cleanup:#})")),
    }
}

async fn open_frontend(db: &Database, node: &FakeNode, instance: &str) -> Result<Arc<Coordinator>> {
    Coordinator::new(
        frontend_config(&db.url, node, instance)?,
        Arc::new(qbit_prism_server::metrics::Metrics::default()),
    )
    .await
}

/// Load an `n`-share production-shaped window and refresh every frontend over
/// it, checking each published exactly that window.
async fn load_and_refresh(frontends: &[&Arc<Coordinator>], n: u64) -> Result<Duration> {
    let plan = WindowPlan::new(n)?;
    let pool = &frontends.first().context("no frontend")?.ledger.pool;
    let load = plan.load(pool, "window-qualify").await?;
    println!(
        "[n={n}] loaded {} shares in {:.2} s ({:.1} MiB serialized)",
        load.rows,
        load.seconds,
        load.serialized_bytes as f64 / 1_048_576.0
    );
    let clock = Instant::now();
    for frontend in frontends {
        frontend.refresh_once().await?;
        let prepared = frontend.prepared.read().await.clone();
        let prepared = prepared.context("the refresh published no work")?;
        ensure!(
            prepared.window.shares.map_or(0, |range| range.share_count) == n
                && prepared.bundle.is_some(),
            "the refresh published a {}-share window, expected {n}",
            prepared.window.shares.map_or(0, |range| range.share_count)
        );
    }
    Ok(clock.elapsed())
}

// ---------------------------------------------------------------------------
// Mining and submitting
// ---------------------------------------------------------------------------

struct Frontend {
    coordinator: Arc<Coordinator>,
    worker: Worker,
    job: MiningJob<JobContext>,
}

impl Frontend {
    async fn open(coordinator: &Arc<Coordinator>, name: &str, extranonce1: &str) -> Result<Self> {
        let worker = Worker {
            username: format!("{name}.rig"),
            payout_address: name.to_owned(),
            worker_name: Some("rig".into()),
            p2mr_program_hex: "11".repeat(32),
        };
        let job = coordinator
            .build_job(&worker, extranonce1, ORDINARY_DIFFICULTY, 0.0)
            .await
            .map_err(|error| anyhow!("{name}: job build failed: {error}"))?;
        Ok(Self {
            coordinator: coordinator.clone(),
            worker,
            job,
        })
    }

    async fn submit(&self, proof: Submission) -> Result<(), String> {
        MiningBackend::submit(
            &*self.coordinator,
            &self.worker,
            &self.job,
            proof,
            false.into(),
        )
        .await
        .map_err(|error| format!("{}: {}", error.reason_id.as_deref().unwrap_or("?"), error))
    }

    /// `count` distinct proofs from this frontend's job: ordinary shares that
    /// miss the block target, or blocks. `pool` keeps pools of one job apart.
    async fn mine(&self, pool: u64, count: usize, block: bool) -> Result<Vec<Submission>> {
        let job = self.job.wire.clone();
        tokio::task::spawn_blocking(move || {
            let extranonce2 = format!("{pool:0width$x}", width = EXTRANONCE2_SIZE * 2);
            let ntime = format!("{:08x}", job.ntime);
            let mut found = Vec::with_capacity(count);
            for nonce in 0..u32::MAX {
                if found.len() == count {
                    return Ok(found);
                }
                let proof = job.assemble_submission(
                    &extranonce2,
                    &ntime,
                    &format!("{nonce:08x}"),
                    None,
                    0,
                )?;
                if proof.share_pass && proof.block_pass == block {
                    found.push(proof);
                }
            }
            bail!("the nonce space ran out before {count} proofs were found")
        })
        .await?
    }

    /// Ordinary-share proofs mined on a blocking thread as they are taken, so
    /// a phase of any length never runs out. Mining stops when the receiver
    /// is dropped.
    fn ordinary_proofs(&self, pool: u64) -> mpsc::Receiver<Submission> {
        let job = self.job.wire.clone();
        let (sender, receiver) = mpsc::channel(4 * RATE_PER_FRONTEND as usize);
        tokio::task::spawn_blocking(move || {
            let extranonce2 = format!("{pool:0width$x}", width = EXTRANONCE2_SIZE * 2);
            let ntime = format!("{:08x}", job.ntime);
            for nonce in 0..u32::MAX {
                let Ok(proof) =
                    job.assemble_submission(&extranonce2, &ntime, &format!("{nonce:08x}"), None, 0)
                else {
                    return;
                };
                if proof.share_pass && !proof.block_pass && sender.blocking_send(proof).is_err() {
                    return;
                }
            }
        });
        receiver
    }
}

#[derive(Clone, Debug)]
struct Ack {
    started: Instant,
    finished: Instant,
    outcome: Result<(), String>,
}

impl Ack {
    fn latency(&self) -> Duration {
        self.finished - self.started
    }
}

/// Submit proofs at `rate` per second, one task per call so a slow call never
/// delays the next one, until `stop` is set. Returns every call's ACK.
async fn submit_at_rate(
    frontend: Arc<Frontend>,
    mut proofs: mpsc::Receiver<Submission>,
    rate: u32,
    stop: Arc<AtomicBool>,
) -> Result<Vec<Ack>> {
    let mut tick = tokio::time::interval(Duration::from_secs(1) / rate);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut calls = tokio::task::JoinSet::new();
    let mut exhausted = false;
    loop {
        tick.tick().await;
        if stop.load(Ordering::SeqCst) {
            break;
        }
        let Some(proof) = proofs.recv().await else {
            exhausted = true;
            break;
        };
        let frontend = frontend.clone();
        calls.spawn(async move {
            let started = Instant::now();
            let outcome = frontend.submit(proof).await;
            Ack {
                started,
                finished: Instant::now(),
                outcome,
            }
        });
    }
    let mut acks = Vec::new();
    while let Some(ack) = calls.join_next().await {
        acks.push(ack?);
    }
    ensure!(
        !exhausted,
        "{}: the proof miner stopped before the phase ended",
        frontend.worker.username
    );
    Ok(acks)
}

#[derive(Clone, Copy, Debug)]
struct Latency {
    count: usize,
    p50: Duration,
    p99: Duration,
    max: Duration,
}

impl Latency {
    /// Nearest-rank percentiles; `None` for no samples, never zeros.
    fn of(samples: impl IntoIterator<Item = Duration>) -> Option<Self> {
        let mut samples: Vec<Duration> = samples.into_iter().collect();
        samples.sort();
        let rank = |quantile: f64| {
            let index = ((quantile * samples.len() as f64).ceil() as usize).max(1) - 1;
            samples[index]
        };
        (!samples.is_empty()).then(|| Self {
            count: samples.len(),
            p50: rank(0.50),
            p99: rank(0.99),
            max: *samples.last().expect("not empty"),
        })
    }

    fn line(label: &str, latency: Option<Self>) -> String {
        match latency {
            Some(latency) => format!(
                "{label}: {} ACKs, p50 {:.2} ms, p99 {:.2} ms, max {:.2} ms",
                latency.count,
                ms(latency.p50),
                ms(latency.p99),
                ms(latency.max)
            ),
            None => format!("{label}: no ACKs (not measured)"),
        }
    }
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

/// `max(2 x phase-A p99 + 50 ms, 250 ms)`.
fn latency_bound(baseline_p99: Duration) -> Duration {
    (baseline_p99 * 2 + Duration::from_millis(50)).max(Duration::from_millis(250))
}

// ---------------------------------------------------------------------------
// Incident 2
// ---------------------------------------------------------------------------

const RATE_PER_FRONTEND: u32 = 50;
const PHASE_A: Duration = Duration::from_secs(4);
/// Phase B submits for this long before the solve, so both frontends are at
/// their steady rate when it arrives.
const PHASE_B_LEAD: Duration = Duration::from_millis(500);
/// The longest phase B may wait for the solved block to settle. An
/// unoptimized build rebuilds a 20,000-share window in about 20 s.
const PHASE_B_CEILING: Duration = Duration::from_secs(600);
/// Fewer frontend-2 samples than this between the solve and settlement
/// cannot support a p99.
const MIN_PHASE_B_SAMPLES: usize = 5;

async fn incident_2(raw: &str, n: u64) -> Result<()> {
    // `submit` hides the reason a share was not confirmed from the miner and
    // logs it instead; keep it next to the failure.
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_test_writer()
        .try_init();
    let db = Database::open(raw).await?;
    let node = FakeNode::open().await?;
    let first = match open_frontend(&db, &node, "qualify-frontend-1").await {
        Ok(first) => first,
        Err(error) => return settle(Err(error), db.close(&[]).await),
    };
    let second = match open_frontend(&db, &node, "qualify-frontend-2").await {
        Ok(second) => second,
        Err(error) => return settle(Err(error), db.close(&[&first]).await),
    };
    let outcome = incident_2_body(&node, &first, &second, n).await;
    settle(outcome, db.close(&[&first, &second]).await)
}

async fn run_phase_until<F: std::future::Future<Output = Result<T>>, T>(
    frontends: [&Arc<Frontend>; 2],
    pools: [mpsc::Receiver<Submission>; 2],
    during: F,
) -> Result<(T, [Vec<Ack>; 2])> {
    let stop = Arc::new(AtomicBool::new(false));
    let [first_pool, second_pool] = pools;
    let one = tokio::spawn(submit_at_rate(
        frontends[0].clone(),
        first_pool,
        RATE_PER_FRONTEND,
        stop.clone(),
    ));
    let two = tokio::spawn(submit_at_rate(
        frontends[1].clone(),
        second_pool,
        RATE_PER_FRONTEND,
        stop.clone(),
    ));
    let result = during.await;
    stop.store(true, Ordering::SeqCst);
    let drain = |handle: JoinHandle<Result<Vec<Ack>>>| async move {
        tokio::time::timeout(SHARE_COMMIT_TIMEOUT * 2, handle)
            .await
            .context("in-flight submissions did not drain within twice the commit timeout")??
    };
    let (one, two) = (drain(one).await, drain(two).await);
    Ok((result?, [one?, two?]))
}

async fn incident_2_body(
    node: &FakeNode,
    first: &Arc<Coordinator>,
    second: &Arc<Coordinator>,
    n: u64,
) -> Result<()> {
    let refresh = load_and_refresh(&[first, second], n).await?;
    println!(
        "[n={n}] both frontends refreshed in {:.2} s",
        refresh.as_secs_f64()
    );
    let one = Arc::new(Frontend::open(first, "frontend1", "00000001").await?);
    let two = Arc::new(Frontend::open(second, "frontend2", "00000002").await?);
    let rate = RATE_PER_FRONTEND as usize;
    let pools_a = [one.ordinary_proofs(1), two.ordinary_proofs(1)];
    let pools_b = [one.ordinary_proofs(2), two.ordinary_proofs(2)];
    let solving = one.mine(3, 1, true).await?.remove(0);

    // Only frontend 1 polls the outbox, so the frontend that solves is the one
    // that claims, rebuilds and lands, and frontend 2 is purely a bystander.
    let (shutdown, receiver) = watch::channel(false);
    let submit_loop = tokio::spawn(first.clone().submit_loop(receiver));
    let measured = async {
        // Phase A: the baseline, no solve.
        let ((), phase_a) =
            run_phase_until([&one, &two], pools_a, async {
                tokio::time::sleep(PHASE_A).await;
                Ok(())
            })
            .await?;
        // Phase B: frontend 1 solves while both keep submitting.
        let ((solve_started, solve, completed), phase_b) = run_phase_until([&one, &two], pools_b, async {
            tokio::time::sleep(PHASE_B_LEAD).await;
            let started = Instant::now();
            let outcome = one.submit(solving.clone()).await;
            let solve = Ack {
                started,
                finished: Instant::now(),
                outcome,
            };
            // #266 offers before the expensive read and rebuild. Keep load
            // running through settlement so phase B measures that work even
            // when the node receives the block before five samples arrive.
            let ceiling = Instant::now() + PHASE_B_CEILING;
            loop {
                if solve.outcome.is_err() {
                    break;
                }
                let state: String = sqlx::query_scalar(
                    "SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$1",
                )
                .bind(&solving.block_hash_hex)
                .fetch_one(&first.ledger.pool)
                .await?;
                if state == "submitted" {
                    break;
                }
                let adopted = node.chain.lock().await.adopted_at;
                ensure!(
                    ["pending", "offer_reserved", "offered", "reconciliation"]
                        .contains(&state.as_str())
                        && adopted.is_none_or(|at| at.elapsed() < Duration::from_secs(30)),
                    "the solved candidate finished as {state}, or not within 30 s of submitblock"
                );
                if Instant::now() >= ceiling || submit_loop.is_finished() {
                    let row: Option<(String, Option<String>)> = sqlx::query_as(
                        "SELECT state,last_error FROM qbit_block_candidate_outbox WHERE block_hash=$1",
                    )
                    .bind(&solving.block_hash_hex)
                    .fetch_optional(&first.ledger.pool)
                    .await?;
                    bail!(
                        "the solved block did not settle within {} s (submit loop running: {}); outbox row: {row:?}",
                        PHASE_B_CEILING.as_secs(),
                        !submit_loop.is_finished()
                    );
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Ok((started, solve, Instant::now()))
        })
        .await?;
        Ok::<_, anyhow::Error>((phase_a, solve_started, solve, phase_b, completed))
    }
    .await;
    let settled = async {
        let (phase_a, solve_started, solve, phase_b, completed) = measured?;
        solve
            .outcome
            .clone()
            .map_err(|error| anyhow!("the block-solving share was not acknowledged: {error}"))?;
        let adopted = node
            .chain
            .lock()
            .await
            .adopted_at
            .context("the block never reached the node")?;
        Ok::<_, anyhow::Error>((phase_a, solve_started, solve, phase_b, adopted, completed))
    }
    .await;
    let _ = shutdown.send(true);
    let stopped = tokio::time::timeout(Duration::from_secs(30), submit_loop).await;
    let (phase_a, solve_started, solve, phase_b, adopted, completed) = settled?;
    stopped.context("the submit loop did not stop within 30 s")??;

    // Every phase-A ACK, and every phase-B ACK that finished before the block
    // moved the tip, is an acceptance. A call still running when the node
    // adopted the block may be refused by the fences that tip move trips:
    // admission's `stale-job`, or the append's payout-revision check, which
    // the miner sees as `ledger-confirmation-failed`. Nothing else.
    for (frontend, acks) in phase_a.iter().enumerate() {
        for ack in acks {
            ack.outcome.clone().map_err(|error| {
                anyhow!(
                    "frontend {}: a phase-A share was refused: {error}",
                    frontend + 1
                )
            })?;
        }
        ensure!(
            acks.len() >= rate * PHASE_A.as_secs() as usize * 3 / 4,
            "frontend {} made only {} phase-A calls in {} s at {rate}/s",
            frontend + 1,
            acks.len(),
            PHASE_A.as_secs()
        );
    }
    for (frontend, acks) in phase_b.iter().enumerate() {
        for ack in acks {
            match &ack.outcome {
                Ok(()) => {}
                Err(error)
                    if ack.finished >= adopted
                        && (error.starts_with("stale-job")
                            || error.starts_with("ledger-confirmation-failed")) => {}
                Err(error) => bail!(
                    "frontend {}: a phase-B share was refused {:.2} ms after it started, {:+.2} ms from submitblock: {error}",
                    frontend + 1,
                    ms(ack.latency()),
                    if ack.finished >= adopted {
                        ms(ack.finished - adopted)
                    } else {
                        -ms(adopted - ack.finished)
                    }
                ),
            }
        }
    }
    // #324's commit gate means a slow solve is never refused at the share
    // deadline any more. It is answered late instead, counted in
    // `qbit_prism_late_confirmed_shares_total`, or answered
    // `ledger-outcome-unknown`. Either would satisfy the acceptance checks
    // above while the serialization stall this test exists to catch was still
    // present, so neither is allowed to pass quietly.
    for (frontend, coordinator) in [(1, &one.coordinator), (2, &two.coordinator)] {
        let late = coordinator
            .metrics
            .render()
            .lines()
            .find_map(|line| line.strip_prefix("qbit_prism_late_confirmed_shares_total "))
            .map_or(0.0, |value| value.trim().parse::<f64>().unwrap_or(-1.0));
        ensure!(
            late == 0.0,
            "frontend {frontend}: qbit_prism_late_confirmed_shares_total is {late}, so a share \
             was confirmed only after its deadline had passed"
        );
    }
    for (label, acks) in [
        ("phase A, frontend 1", &phase_a[0]),
        ("phase A, frontend 2", &phase_a[1]),
        ("phase B, frontend 1", &phase_b[0]),
        ("phase B, frontend 2", &phase_b[1]),
    ] {
        for ack in acks {
            if let Err(error) = &ack.outcome {
                ensure!(
                    !error.starts_with("ledger-outcome-unknown"),
                    "{label}: a share was answered {error} after {:.2} ms",
                    ms(ack.latency())
                );
            }
        }
    }
    if let Err(error) = &solve.outcome {
        ensure!(
            !error.starts_with("ledger-outcome-unknown"),
            "the block-solving share was answered {error} after {:.2} ms",
            ms(solve.latency())
        );
    }
    let during = |acks: &[Ack]| {
        Latency::of(
            acks.iter()
                .filter(|ack| ack.started >= solve_started && ack.started < completed)
                .map(Ack::latency),
        )
    };
    let a1 = Latency::of(phase_a[0].iter().map(Ack::latency))
        .context("frontend 1 made no phase-A calls")?;
    let a2 = Latency::of(phase_a[1].iter().map(Ack::latency))
        .context("frontend 2 made no phase-A calls")?;
    let b1 = during(&phase_b[0]);
    let b2 = during(&phase_b[1]);
    let window = completed - solve_started;
    println!("[n={n}] incident 2, in-process ACK latency (MiningBackend::submit wall clock; no socket harness):");
    println!("  {}", Latency::line("phase A frontend 1", Some(a1)));
    println!("  {}", Latency::line("phase A frontend 2", Some(a2)));
    println!(
        "  block-solving share ACK (frontend 1): {:.2} ms; solve to submitblock {:.2} ms; solve to settlement {:.2} ms",
        ms(solve.latency()),
        ms(adopted - solve_started),
        ms(window)
    );
    println!(
        "  {}",
        Latency::line("phase B frontend 1 (solve to settlement)", b1)
    );
    println!(
        "  {}",
        Latency::line("phase B frontend 2 (solve to settlement)", b2)
    );
    println!(
        "  bounds: solve ACK <= {:.2} ms, frontend 2 phase-B p99 <= {:.2} ms, every ACK < {} ms",
        ms(latency_bound(a1.p99)),
        ms(latency_bound(a2.p99)),
        ACK_CEILING.as_millis()
    );

    let b2 = b2.with_context(|| {
        format!(
            "frontend 2 made no call between the solve and settlement ({:.2} ms)",
            ms(window)
        )
    })?;
    ensure!(
        b2.count >= MIN_PHASE_B_SAMPLES,
        "frontend 2 made only {} calls in the {:.2} ms between the solve and settlement; a p99 needs {MIN_PHASE_B_SAMPLES}",
        b2.count,
        ms(window)
    );
    // A fast stale-job response must not masquerade as an append under
    // rebuild load. The bystander must still accept real post-offer shares.
    let accepted_after_offer = phase_b[1]
        .iter()
        .filter(|ack| ack.started >= adopted && ack.started < completed && ack.outcome.is_ok())
        .count();
    println!("  frontend 2 accepted {accepted_after_offer} post-offer shares before settlement");
    ensure!(
        accepted_after_offer >= MIN_PHASE_B_SAMPLES,
        "frontend 2 accepted only {accepted_after_offer} post-offer shares before settlement; expected at least {MIN_PHASE_B_SAMPLES} real appends during rebuild"
    );
    ensure!(
        solve.latency() <= latency_bound(a1.p99),
        "the block-solving share's ACK took {:.2} ms, over max(2 x frontend 1 phase-A p99 {:.2} ms + 50 ms, 250 ms)",
        ms(solve.latency()),
        ms(a1.p99)
    );
    ensure!(
        b2.p99 <= latency_bound(a2.p99),
        "frontend 2's phase-B p99 was {:.2} ms, over max(2 x its phase-A p99 {:.2} ms + 50 ms, 250 ms)",
        ms(b2.p99),
        ms(a2.p99)
    );
    let slowest = phase_a
        .iter()
        .chain(phase_b.iter())
        .flatten()
        .chain(std::iter::once(&solve))
        .map(Ack::latency)
        .max()
        .unwrap_or_default();
    ensure!(
        slowest < ACK_CEILING,
        "an ACK took {:.2} ms, not well under the {} s share-commit timeout",
        ms(slowest),
        SHARE_COMMIT_TIMEOUT.as_secs()
    );

    let (rows, landed): (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM qbit_block_candidate_outbox),\
         (SELECT count(*) FROM qbit_pool_audit_bundles WHERE block_hash=$1)",
    )
    .bind(&solving.block_hash_hex)
    .fetch_one(&first.ledger.pool)
    .await?;
    ensure!(
        rows == 1,
        "the solve enqueued {rows} outbox rows, expected exactly 1"
    );
    ensure!(
        landed == 1,
        "the claim did not land the rebuilt audit before settlement"
    );
    ensure!(
        node.chain.lock().await.submissions == 1,
        "the block was not submitted exactly once"
    );
    Ok(())
}

/// Incident 2 at the reduced size.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incident_2_ack_latency_stays_flat_while_a_frontend_solves_a_large_window() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    incident_2(&raw, share_count(SHARES_VAR, 20_000)?).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full-size window: needs gigabytes of RAM, and cannot reach 400k until #273 stops the job payload copying the window: three copies cross PostgreSQL's 256 MiB jsonb limit above about 150k shares. The coordinator runs it with --ignored, at a PRISM_WINDOW_QUALIFY_FULL_SHARES that divides the window weight; 125000 is the largest that fits today"]
async fn incident_2_ack_latency_at_full_size() -> Result<()> {
    let raw = gate::required_database_url(gate::site!())?;
    let _serial = TEST_LOCK.lock().await;
    incident_2(&raw, share_count(FULL_SHARES_VAR, 400_000)?).await
}

// ---------------------------------------------------------------------------
// ORDER_LOCK hold time
// ---------------------------------------------------------------------------

/// Solves measured per size; the median is compared.
const HOLD_REPEATS: usize = 7;
/// The fixed allowance the larger window's median hold may exceed the smaller
/// one's by. Serializing and digesting the window under the lock would cost
/// tens of milliseconds more at 20,000 shares than at 5,000, and seconds at
/// 400,000; a single small transaction varies by a few milliseconds.
const HOLD_SLACK: Duration = Duration::from_millis(15);
/// The candidate document may differ by the digits of its share range.
const DOCUMENT_TOLERANCE_BYTES: i64 = 64;
const ONE_MIB: i64 = 1_048_576;

/// When a sample was answered, and the pid holding `ORDER_LOCK` then.
type LockSample = (Instant, Option<i32>);

/// A connection outside every frontend pool that polls `pg_locks` for a
/// granted `ORDER_LOCK` as fast as it can answer.
struct LockSampler {
    stop: Arc<AtomicBool>,
    task: JoinHandle<Result<Vec<LockSample>>>,
}

impl LockSampler {
    async fn start(raw: &str) -> Result<Self> {
        let mut connection = PgConnection::connect(raw).await?;
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let task = tokio::spawn(async move {
            let mut samples = Vec::new();
            while !stopping.load(Ordering::SeqCst) {
                let holder: Option<i32> = sqlx::query_scalar(
                    "SELECT pid FROM pg_locks WHERE locktype='advisory' AND granted \
                     AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) \
                     AND classid=$1::bigint::oid AND objid=$2::bigint::oid AND objsubid=1 LIMIT 1",
                )
                .bind(ORDER_LOCK >> 32)
                .bind(ORDER_LOCK & 0xffff_ffff)
                .fetch_optional(&mut connection)
                .await?;
                samples.push((Instant::now(), holder));
            }
            connection.close().await?;
            Ok(samples)
        });
        // Let the first samples land before the measured work starts.
        tokio::time::sleep(Duration::from_millis(20)).await;
        Ok(Self { stop, task })
    }

    /// Every continuous run of samples that saw the lock held, as `(pid,
    /// observed hold)`, and the median sampling period.
    async fn finish(self) -> Result<(Vec<(i32, Duration)>, Duration)> {
        tokio::time::sleep(Duration::from_millis(20)).await;
        self.stop.store(true, Ordering::SeqCst);
        let samples = tokio::time::timeout(Duration::from_secs(10), self.task)
            .await
            .context("the lock sampler did not stop")???;
        ensure!(
            samples.len() >= 3,
            "the lock sampler took only {} samples",
            samples.len()
        );
        let mut periods: Vec<Duration> = samples
            .windows(2)
            .map(|pair| pair[1].0 - pair[0].0)
            .collect();
        periods.sort();
        let period = periods[periods.len() / 2];
        let mut runs = Vec::new();
        let mut current: Option<(i32, Instant, Instant)> = None;
        for (at, holder) in samples {
            current = match (current, holder) {
                (Some((pid, first, _)), Some(holder)) if pid == holder => Some((pid, first, at)),
                (current, holder) => {
                    if let Some((pid, first, last)) = current {
                        runs.push((pid, last - first));
                    }
                    holder.map(|pid| (pid, at, at))
                }
            };
        }
        if let Some((pid, first, last)) = current {
            runs.push((pid, last - first));
        }
        Ok((runs, period))
    }
}

#[derive(Debug)]
struct HoldMeasure {
    n: u64,
    median_hold: Duration,
    max_hold: Duration,
    period: Duration,
    median_ack: Duration,
    document_bytes: (i64, i64),
}

fn median(mut values: Vec<Duration>) -> Duration {
    values.sort();
    values[values.len() / 2]
}

async fn measure_hold(raw: &str, n: u64) -> Result<HoldMeasure> {
    let db = Database::open(raw).await?;
    let node = FakeNode::open().await?;
    let frontend = match open_frontend(&db, &node, "qualify-lock").await {
        Ok(frontend) => frontend,
        Err(error) => return settle(Err(error), db.close(&[]).await),
    };
    let outcome = measure_hold_body(raw, &db, &frontend, n).await;
    settle(outcome, db.close(&[&frontend]).await)
}

async fn measure_hold_body(
    raw: &str,
    db: &Database,
    coordinator: &Arc<Coordinator>,
    n: u64,
) -> Result<HoldMeasure> {
    load_and_refresh(&[coordinator], n).await?;
    let frontend = Frontend::open(coordinator, "lockprobe", "00000001").await?;
    let proofs = frontend.mine(1, HOLD_REPEATS, true).await?;

    // The sampler must see a hold it knows, or a zero below is meaningless.
    let sampler = LockSampler::start(raw).await?;
    let mut known = db.admin.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(ORDER_LOCK)
        .execute(&mut *known)
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    known.commit().await?;
    let (runs, period) = sampler.finish().await?;
    let known_hold = runs.iter().map(|run| run.1).max().unwrap_or_default();
    ensure!(
        runs.len() == 1 && known_hold >= Duration::from_millis(40),
        "the sampler did not observe a known 50 ms ORDER_LOCK hold as one run of at least 40 ms: {runs:?} (period {period:?})"
    );

    let mut holds = Vec::new();
    let mut acks = Vec::new();
    let mut periods = Vec::new();
    for proof in proofs {
        let hash = proof.block_hash_hex.clone();
        let sampler = LockSampler::start(raw).await?;
        let started = Instant::now();
        let outcome = frontend.submit(proof).await;
        acks.push(started.elapsed());
        let (runs, period) = sampler.finish().await?;
        outcome.map_err(|error| anyhow!("the block-solving share was refused: {error}"))?;
        ensure!(
            runs.len() <= 1,
            "ORDER_LOCK was taken {} times around one solve with no other traffic: {runs:?}",
            runs.len()
        );
        // A hold shorter than one sampling period can fall between samples;
        // it is then at most that period.
        holds.push(runs.first().map_or(period, |run| run.1));
        periods.push(period);
        let queued: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM qbit_block_candidate_outbox WHERE block_hash=$1)",
        )
        .bind(&hash)
        .fetch_one(&coordinator.ledger.pool)
        .await?;
        ensure!(queued, "the solve enqueued no candidate");
    }
    let (rows, windowed, smallest, largest): (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT count(*),count(*) FILTER (WHERE candidate ? 'bundle' OR candidate ? 'shares'),\
         min(octet_length(candidate::text))::bigint,max(octet_length(candidate::text))::bigint \
         FROM qbit_block_candidate_outbox",
    )
    .fetch_one(&coordinator.ledger.pool)
    .await?;
    ensure!(
        rows == HOLD_REPEATS as i64,
        "{rows} candidates enqueued, expected {HOLD_REPEATS}"
    );
    ensure!(
        windowed == 0,
        "{windowed} candidate documents carry a window value"
    );
    Ok(HoldMeasure {
        n,
        max_hold: *holds.iter().max().expect("repeats"),
        median_hold: median(holds),
        period: median(periods),
        median_ack: median(acks),
        document_bytes: (smallest, largest),
    })
}

async fn order_lock_hold(raw: &str, small: u64, large: u64) -> Result<()> {
    ensure!(
        small < large,
        "the lock-hold sizes must increase: {small} then {large}"
    );
    let low = measure_hold(raw, small).await?;
    let high = measure_hold(raw, large).await?;
    println!("ORDER_LOCK hold of a block-solving append, sampled from pg_locks on a separate connection, {HOLD_REPEATS} solves per size, no other traffic:");
    for measure in [&low, &high] {
        println!(
            "  n={}: median hold {:.2} ms, max {:.2} ms (sampling period {:.3} ms); median ACK {:.2} ms; candidate document {}..{} B",
            measure.n,
            ms(measure.median_hold),
            ms(measure.max_hold),
            ms(measure.period),
            ms(measure.median_ack),
            measure.document_bytes.0,
            measure.document_bytes.1
        );
    }
    println!(
        "  growth: {:+.3} ms across {} more shares (slack {} ms)",
        ms(high.median_hold) - ms(low.median_hold),
        large - small,
        HOLD_SLACK.as_millis()
    );
    ensure!(
        high.median_hold <= low.median_hold + HOLD_SLACK,
        "the median ORDER_LOCK hold grew from {:.2} ms at {small} shares to {:.2} ms at {large}, past the {} ms slack",
        ms(low.median_hold),
        ms(high.median_hold),
        HOLD_SLACK.as_millis()
    );
    for measure in [&low, &high] {
        ensure!(
            measure.document_bytes.1 < ONE_MIB,
            "the candidate document at n={} is {} B, not under 1 MiB",
            measure.n,
            measure.document_bytes.1
        );
    }
    ensure!(
        (high.document_bytes.1 - low.document_bytes.1).abs() <= DOCUMENT_TOLERANCE_BYTES
            && (high.document_bytes.0 - low.document_bytes.0).abs() <= DOCUMENT_TOLERANCE_BYTES,
        "the candidate document is {:?} B at {small} shares and {:?} B at {large}; it must be the same size within {DOCUMENT_TOLERANCE_BYTES} B",
        low.document_bytes,
        high.document_bytes
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_lock_hold_on_a_solve_does_not_grow_with_the_window() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    order_lock_hold(
        &raw,
        share_count(SMALL_SHARES_VAR, 5_000)?,
        share_count(SHARES_VAR, 20_000)?,
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full-size window: needs gigabytes of RAM, and cannot reach 400k until #273 stops the job payload copying the window: three copies cross PostgreSQL's 256 MiB jsonb limit above about 150k shares. The coordinator runs it with --ignored, at a PRISM_WINDOW_QUALIFY_FULL_SHARES that divides the window weight; 125000 is the largest that fits today"]
async fn order_lock_hold_at_full_size() -> Result<()> {
    let raw = gate::required_database_url(gate::site!())?;
    let _serial = TEST_LOCK.lock().await;
    order_lock_hold(
        &raw,
        share_count(SMALL_SHARES_VAR, 5_000)?,
        share_count(FULL_SHARES_VAR, 400_000)?,
    )
    .await
}

#[test]
fn nearest_rank_percentiles_and_the_latency_bound() {
    let samples = (1..=200).map(Duration::from_millis);
    let latency = Latency::of(samples).expect("samples");
    assert_eq!(latency.count, 200);
    assert_eq!(latency.p50, Duration::from_millis(100));
    assert_eq!(latency.p99, Duration::from_millis(198));
    assert_eq!(latency.max, Duration::from_millis(200));
    assert!(Latency::of(Vec::new()).is_none());
    assert_eq!(
        latency_bound(Duration::from_millis(10)),
        Duration::from_millis(250)
    );
    assert_eq!(
        latency_bound(Duration::from_millis(150)),
        Duration::from_millis(350)
    );
}

// ---------------------------------------------------------------------------
// Landing on a delta-built window
// ---------------------------------------------------------------------------

/// Bits 0.78% harder than the stock `207fffff`: the same exponent with the
/// mantissa lowered by one step of the harness's retarget walk.
const RETARGET_BITS: &str = "207f0000";

/// The value of one `qbit_prism_refresh_window_acquisitions_total` outcome
/// in the coordinator's own registry.
fn acquisitions(metrics: &qbit_prism_server::metrics::Metrics, outcome: &str) -> f64 {
    let key = format!("qbit_prism_refresh_window_acquisitions_total{{outcome=\"{outcome}\"}} ");
    metrics
        .render()
        .lines()
        .find_map(|line| line.strip_prefix(&key))
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(f64::NAN)
}

/// Append `count` fixture-shaped rows above the ledger's current top, with
/// the plan's share difficulty, so they extend the window like live shares.
async fn append_fixture_rows(pool: &PgPool, plan: &WindowPlan, count: i64) -> Result<(i64, i64)> {
    let top: i64 = sqlx::query_scalar("SELECT COALESCE(max(share_seq),0) FROM qbit_share_ledger")
        .fetch_one(pool)
        .await?;
    let (first, last) = (top + 1, top + count);
    sqlx::query(
        "INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,\
         p2mr_program,share_difficulty,network_difficulty,template_height,job_id,\
         job_issued_at,ntime,accepted_at,credit_policy,accepted,writer_id,writer_epoch) \
         SELECT i,'extra:'||i::text,'m0','k',decode(repeat('aa',32),'hex'),\
         $3::text::numeric,1000,100,'seed-job',\
         to_timestamp((1700000000000+i)::double precision/1000),1700000000,\
         to_timestamp((1700000000001+i)::double precision/1000),NULL,true,'delta-land',0 \
         FROM generate_series($1::bigint,$2::bigint) AS g(i)",
    )
    .bind(first)
    .bind(last)
    .bind(plan.share_difficulty().to_string())
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO qbit_prism_share_hashes(header_hash,share_id) \
         SELECT encode(sha256(convert_to(share_id,'UTF8')),'hex'),share_id \
         FROM qbit_share_ledger WHERE share_seq BETWEEN $1 AND $2 ON CONFLICT DO NOTHING",
    )
    .bind(first)
    .bind(last)
    .execute(pool)
    .await?;
    sqlx::query("SELECT setval(pg_get_serial_sequence('qbit_share_ledger','share_seq'),$1)")
        .bind(last)
        .execute(pool)
        .await?;
    Ok((first, last))
}

async fn delta_built_window_lands_body(
    node: &FakeNode,
    frontend: &Arc<Coordinator>,
    metrics: &qbit_prism_server::metrics::Metrics,
) -> Result<()> {
    // A seeded window plus a tenth more newer rows: the window at the stock
    // bits is the newest 2,000 rows, with 200 older rows below it for a
    // harder target to reach into.
    let n = 2_000u64;
    let plan = WindowPlan::new(n)?;
    let pool = &frontend.ledger.pool;
    plan.load(pool, "delta-land").await?;
    append_fixture_rows(pool, &plan, 200).await?;
    frontend.refresh_once().await?;
    let first_work = frontend
        .prepared
        .read()
        .await
        .clone()
        .context("the first refresh published no work")?;
    let first_range = first_work
        .window
        .shares
        .context("the first window is empty")?;
    ensure!(
        first_range.share_count == n && first_range.first_share_seq == 201,
        "the first refresh published {}..={} ({} rows), expected the newest {n}",
        first_range.first_share_seq,
        first_range.last_share_seq,
        first_range.share_count
    );
    ensure!(
        acquisitions(metrics, "no_prior") == 1.0 && acquisitions(metrics, "advanced") == 0.0,
        "the first refresh was not the full scan with no retired window: {}",
        metrics.render()
    );

    // The node retargets harder and ten more shares arrive; the refresh must
    // advance the retained window by the delta path: a margin below for the
    // heavier target, a delta above for the new shares.
    node.retarget(RETARGET_BITS).await;
    let (delta_first, delta_last) = append_fixture_rows(pool, &plan, 10).await?;
    frontend.refresh_once().await?;
    let work = frontend
        .prepared
        .read()
        .await
        .clone()
        .context("the second refresh published no work")?;
    let range = work.window.shares.context("the second window is empty")?;
    ensure!(
        acquisitions(metrics, "advanced") == 1.0,
        "the retargeted refresh did not advance by the delta path: {}",
        metrics.render()
    );
    ensure!(
        range.last_share_seq == u64::try_from(delta_last)?
            && range.first_share_seq < first_range.first_share_seq
            && range.share_count > n,
        "the advanced window is {}..={} ({} rows); expected a margin below {} and the delta {}..={} above",
        range.first_share_seq,
        range.last_share_seq,
        range.share_count,
        first_range.first_share_seq,
        delta_first,
        delta_last
    );
    ensure!(
        work.template["bits"].as_str() == Some(RETARGET_BITS),
        "the published work does not carry the retargeted bits"
    );

    // A block found on that work is claimed, offered, rebuilt and landed
    // through the coordinator's own submit loop, against exactly that window.
    let miner = Frontend::open(frontend, "delta-land", "00000001").await?;
    let solving = miner.mine(3, 1, true).await?.remove(0);
    let (shutdown, receiver) = watch::channel(false);
    let submit_loop = tokio::spawn(frontend.clone().submit_loop(receiver));
    let outcome = async {
        miner
            .submit(solving.clone())
            .await
            .map_err(|error| anyhow!("the block-solving share was not acknowledged: {error}"))?;
        let ceiling = Instant::now() + Duration::from_secs(120);
        loop {
            let row: Option<(String, Option<String>)> = sqlx::query_as(
                "SELECT state,last_error FROM qbit_block_candidate_outbox WHERE block_hash=$1",
            )
            .bind(&solving.block_hash_hex)
            .fetch_optional(pool)
            .await?;
            let state = row.as_ref().map(|row| row.0.as_str());
            if state == Some("submitted") {
                break;
            }
            ensure!(
                matches!(
                    state,
                    Some("pending" | "offer_reserved" | "offered" | "reconciliation")
                ) && Instant::now() < ceiling
                    && !submit_loop.is_finished(),
                "the block did not land and submit within 120 s; outbox row: {row:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let _ = shutdown.send(true);
    let stopped = tokio::time::timeout(Duration::from_secs(30), submit_loop).await;
    outcome?;
    stopped.context("the submit loop did not stop within 30 s")??;

    let (rows, landed): (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM qbit_block_candidate_outbox),\
         (SELECT count(*) FROM qbit_pool_audit_bundles WHERE block_hash=$1)",
    )
    .bind(&solving.block_hash_hex)
    .fetch_one(pool)
    .await?;
    ensure!(
        rows == 1 && landed == 1,
        "outbox rows {rows}, landed audits {landed}; expected 1 and 1"
    );
    ensure!(
        node.chain.lock().await.submissions == 1,
        "the block was not submitted exactly once"
    );
    // The landed audit's share snapshot is the delta-built window, re-derived
    // by landing at the retargeted difficulty: same range, same count.
    let (snapshot_first, snapshot_last, snapshot_count): (i64, i64, i64) = sqlx::query_as(
        "SELECT first_share_seq,last_share_seq,share_count FROM qbit_prism_audit_snapshots \
         WHERE snapshot_sha256=$1",
    )
    .bind(hex::encode(range.snapshot_sha256))
    .fetch_one(pool)
    .await?;
    ensure!(
        (snapshot_first, snapshot_last, snapshot_count)
            == (
                i64::try_from(range.first_share_seq)?,
                i64::try_from(range.last_share_seq)?,
                i64::try_from(range.share_count)?
            ),
        "the landed snapshot {snapshot_first}..={snapshot_last} ({snapshot_count} rows) is not the published window"
    );
    Ok(())
}

/// A found block lands through the coordinator on a window the delta path
/// built after a retarget. The first refresh full-scans a seeded window with
/// older history below it; the node then serves harder bits and new shares
/// arrive, so the second refresh advances the retained window (a margin
/// below, a delta above) instead of scanning; the block found on that work is
/// claimed, offered, rebuilt and landed against exactly that window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_found_on_a_delta_built_window_after_a_retarget_lands() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let db = Database::open(&raw).await?;
    let node = FakeNode::open().await?;
    let metrics = Arc::new(qbit_prism_server::metrics::Metrics::default());
    let frontend = Coordinator::new(
        frontend_config(&db.url, &node, "delta-land")?,
        metrics.clone(),
    )
    .await?;
    let outcome = delta_built_window_lands_body(&node, &frontend, &metrics).await;
    settle(outcome, db.close(&[&frontend]).await)
}
