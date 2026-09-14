//! Claim-side qualification of the candidate window switch (#265, slice 3),
//! against a real PostgreSQL, a fake node and a production-shaped window of N
//! shares (`tests/support/window_fixture.rs`).
//!
//! * **Incident 1: a claim's rebuild never delays its lease renewal.** A
//!   candidate whose window holds N shares is claimed and processed under the
//!   short lease `candidate_lease_tests` uses (1 s lease, 100 ms interval,
//!   300 ms timeout). Every renewal is read back from the row's
//!   `claim_expires_at`, which the renewal sets to its own `clock_timestamp()`
//!   plus the lease, so the gaps are the database's, not a poller's.
//! * **Canonical hashes are unchanged by the switch.** The bundle a claim
//!   rebuilds through `Coordinator::read_window` and the borrowing builder has
//!   the same canonical bytes as the bundle `refresh_once` built for the job
//!   the block was found on, and the landed audit carries that bundle's digest.
//!
//! The ACK-path measurements (incident 2 and `ORDER_LOCK` hold time) are in
//! `tests/candidate_window_qualification.rs`.
//!
//! Window size: `PRISM_WINDOW_QUALIFY_SHARES` (default 20,000) for the gated
//! tests, `PRISM_WINDOW_QUALIFY_FULL_SHARES` (default 400,000) for the
//! `#[ignore]`d full-size variants, which need gigabytes of RAM. The count
//! must divide the window weight exactly, and until #273 stops the job
//! payload copying the window it must also stay under about 150,000: the
//! payload holds three copies, which cross PostgreSQL's 256 MiB jsonb object
//! limit at 400,000. 125,000 is the largest count that satisfies both today.
//!
//! ```text
//! PRISM_TEST_DATABASE_URL=... cargo test -p qbit-prism-server --lib window_incident_tests -- --nocapture
//! PRISM_TEST_DATABASE_URL=... cargo test -p qbit-prism-server --lib window_incident_tests -- --ignored --nocapture
//! ```

use super::d2_test_support::*;
use super::*;
use crate::ledger::BalanceSource;
use anyhow::{anyhow, bail};
use axum::{extract::State, routing::post, Json, Router};
use std::collections::BTreeMap;
use tokio_util::task::AbortOnDropHandle;

// Only `WindowPlan::new` and `load` are used here; the JSONB gate uses the rest.
#[allow(dead_code)]
#[path = "../../tests/support/window_fixture.rs"]
mod window_fixture;

use window_fixture::WindowPlan;

/// The ledger's advisory locks are cluster-wide constants, not schema-scoped.
use super::test_serial::TEST_LOCK;

/// `candidate_lease_tests`' `SHORT_LEASE`.
const SHORT_LEASE: CandidateLease = CandidateLease {
    seconds: 1,
    interval: Duration::from_millis(100),
    timeout: Duration::from_millis(300),
    rebuild_deadline: Duration::from_secs(60),
};

/// How long a claim at the full size may run before the test stops waiting.
const PROCESS_CEILING: Duration = Duration::from_secs(900);

const SHARES_VAR: &str = "PRISM_WINDOW_QUALIFY_SHARES";
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

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

// ---------------------------------------------------------------------------
// Fake node
// ---------------------------------------------------------------------------

struct Chain {
    hashes: BTreeMap<u64, String>,
    chainwork: u64,
    submissions: usize,
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

/// A node on the window fixture's height whose `submitblock` adopts the block.
async fn node_reply(
    State(chain): State<Arc<Mutex<Chain>>>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let mut chain = chain.lock().await;
    let height = chain.height();
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
        "getblocktemplate" => json!({"version":0x2000_0000u32,"bits":TEMPLATE_BITS,
            "height":height+1,"coinbasevalue":5_000_000_000u64,
            "curtime":unix_now().expect("the host clock precedes the epoch"),
            "previousblockhash":chain.tip(),"transactions":[]}),
        "submitblock" => {
            let block = hex::decode(request["params"][0].as_str().unwrap_or_default())
                .expect("submitblock carries a hex block");
            chain.submissions += 1;
            chain.hashes.insert(
                height + 1,
                codec::hash_display(&codec::double_sha256(&block[..80])),
            );
            chain.chainwork += 1;
            Value::Null
        }
        method => panic!("unexpected window-incident RPC {method}"),
    };
    Json(json!({"id":request["id"],"result":result,"error":null}))
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

struct Fixture {
    schema: TestSchema,
    coordinator: Arc<Coordinator>,
    chain: Arc<Mutex<Chain>>,
    _server: AbortOnDropHandle<()>,
}

/// A block-solving share on the refreshed job, already submitted, and the
/// work it was found on.
struct Solved {
    prepared: Arc<Prepared>,
    block_hash: String,
    coinbase_suffix_hex: String,
}

impl Fixture {
    async fn open(raw: &str) -> Result<Self> {
        let schema = TestSchema::create(raw, "prism_window_incident").await?;
        let opened = async {
            let chain = Arc::new(Mutex::new(Chain {
                hashes: BTreeMap::from([(0, "00".repeat(32)), (100, "ab".repeat(32))]),
                chainwork: 1,
                submissions: 0,
            }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let rpc_url = format!("http://{}/", listener.local_addr()?);
            let app = Router::new()
                .route("/", post(node_reply))
                .with_state(chain.clone());
            let server = AbortOnDropHandle::new(tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            }));
            let mut config = test_config(
                schema.url(),
                rpc_url,
                "window-incident",
                Duration::from_secs(15),
            )?;
            // A full-size refresh must not age the work out before the solve.
            config.database_connections = 8;
            config.build_workers = 2;
            config.template_max_age = Duration::from_secs(3600);
            config.submit_tip_max_age = Duration::from_secs(3600);
            config.template_refresh_failure_exit = Duration::from_secs(3600);
            config.snapshot_interval = Duration::from_secs(3600);
            config.health_timeout = Duration::from_secs(3600);
            let coordinator =
                Coordinator::new(config, Arc::new(crate::metrics::Metrics::default())).await?;
            Ok::<_, anyhow::Error>((coordinator, chain, server))
        }
        .await;
        match opened {
            Ok((coordinator, chain, server)) => Ok(Self {
                schema,
                coordinator,
                chain,
                _server: server,
            }),
            Err(error) => Err(schema.abandon(error).await),
        }
    }

    /// Load an `n`-share window, refresh over it, and submit one
    /// block-solving share through `MiningBackend::submit`. The extranonces
    /// are all zero, so the candidate's coinbase suffix is exactly the
    /// placeholder suffix the refresh built its bundle with.
    async fn solve(&self, n: u64) -> Result<Solved> {
        let load = WindowPlan::new(n)?
            .load(&self.coordinator.ledger.pool, "window-incident")
            .await?;
        let clock = Instant::now();
        self.coordinator.refresh_once().await?;
        let prepared = self
            .coordinator
            .prepared
            .read()
            .await
            .clone()
            .context("the refresh published no work")?;
        ensure!(
            prepared.snapshot.shares.len() as u64 == n && prepared.bundle.is_some(),
            "the refresh published a {}-share window, expected {n}",
            prepared.snapshot.shares.len()
        );
        println!(
            "[n={n}] loaded in {:.2} s, refreshed in {:.2} s",
            load.seconds,
            clock.elapsed().as_secs_f64()
        );
        let worker = Worker {
            username: "incident.rig".into(),
            payout_address: "incident".into(),
            worker_name: Some("rig".into()),
            p2mr_program_hex: "11".repeat(32),
        };
        let job = MiningBackend::build_job(&*self.coordinator, &worker, EXTRANONCE1, 1e-12, 0.0)
            .await
            .map_err(|error| anyhow!("job build failed: {error}"))?;
        let wire = job.wire.clone();
        let proof = tokio::task::spawn_blocking(move || {
            (0..u32::MAX).find_map(|nonce| {
                let proof = wire
                    .assemble_submission(
                        &"00".repeat(EXTRANONCE2_SIZE),
                        &format!("{:08x}", wire.ntime),
                        &format!("{nonce:08x}"),
                        None,
                        0,
                    )
                    .ok()?;
                (proof.share_pass && proof.block_pass).then_some(proof)
            })
        })
        .await?
        .context("no block proof in the nonce space")?;
        let block_hash = proof.block_hash_hex.clone();
        MiningBackend::submit(&*self.coordinator, &worker, &job, proof, false.into())
            .await
            .map_err(|error| anyhow!("the block-solving share was refused: {error}"))?;
        let coinbase_suffix_hex: String = sqlx::query_scalar(
            "SELECT candidate->>'coinbase_suffix_hex' FROM qbit_block_candidate_outbox WHERE block_hash=$1",
        )
        .bind(&block_hash)
        .fetch_one(&self.coordinator.ledger.pool)
        .await?;
        Ok(Solved {
            prepared,
            block_hash,
            coinbase_suffix_hex,
        })
    }

    async fn close(self) -> Result<()> {
        let _ = tokio::time::timeout(
            Duration::from_secs(10),
            self.coordinator.ledger.pool.close(),
        )
        .await;
        match tokio::time::timeout(Duration::from_secs(60), self.schema.remove()).await {
            Ok(removed) => removed,
            Err(_) => Err(anyhow!("the test schema could not be dropped within 60 s")),
        }
    }
}

// ---------------------------------------------------------------------------
// Incident 1
// ---------------------------------------------------------------------------

/// One poll of the claimed row.
struct RowPoll {
    state: String,
    token: Option<String>,
    expires_micros: Option<i64>,
    live: bool,
}

async fn incident_1(raw: &str, n: u64) -> Result<()> {
    let fixture = Fixture::open(raw).await?;
    let outcome = incident_1_body(&fixture, n).await;
    settle(outcome, fixture.close().await)
}

async fn incident_1_body(fixture: &Fixture, n: u64) -> Result<()> {
    let solved = fixture.solve(n).await?;
    let coordinator = &fixture.coordinator;
    let successor = Ledger::connect(
        fixture.schema.url(),
        "window-incident-successor".into(),
        2,
        false,
    )
    .await?;
    let claim = coordinator
        .ledger
        .claim_candidate(SHORT_LEASE.seconds)
        .await?
        .context("the solved candidate could not be claimed")?;
    ensure!(claim.candidate.block_hash == solved.block_hash);
    ensure!(
        claim.candidate.window.shares.map(|range| range.share_count) == Some(n),
        "the claimed candidate does not reference the {n}-share window"
    );
    let process = AbortOnDropHandle::new(tokio::spawn({
        let coordinator = coordinator.clone();
        let claim = claim.clone();
        async move {
            coordinator
                .process_candidate_with_lease(&claim, SHORT_LEASE)
                .await
        }
    }));
    let started = Instant::now();
    let mut expiries: Vec<(i64, Instant)> = Vec::new();
    let mut lapses = Vec::new();
    let mut stolen = None;
    let mut rebuild: Option<(Instant, Instant)> = None;
    let mut last_probe = Instant::now();
    let monitored = async {
        while !process.is_finished() {
            ensure!(
                started.elapsed() < PROCESS_CEILING,
                "the claim did not finish within {} s",
                PROCESS_CEILING.as_secs()
            );
            if coordinator.build_slots.available_permits() < coordinator.config.build_workers {
                let now = Instant::now();
                rebuild = Some(rebuild.map_or((now, now), |(first, _)| (first, now)));
            }
            let (state, token, expires_micros, live) = sqlx::query_as("SELECT state,claim_token,(extract(epoch FROM claim_expires_at)*1000000)::bigint,COALESCE(claim_expires_at>clock_timestamp(),false) FROM qbit_block_candidate_outbox WHERE block_hash=$1")
                .bind(&solved.block_hash)
                .fetch_one(&successor.pool)
                .await?;
            let poll = RowPoll {
                state,
                token,
                expires_micros,
                live,
            };
            if poll.state == "pending" && poll.token.as_deref() == Some(&claim.claim_token) {
                let expires = poll.expires_micros.context("a claimed row has no expiry")?;
                if expiries.last().is_none_or(|(last, _)| *last != expires) {
                    expiries.push((expires, Instant::now()));
                }
                if !poll.live {
                    lapses.push(started.elapsed());
                }
            }
            if last_probe.elapsed() >= Duration::from_millis(100) {
                last_probe = Instant::now();
                if let Some(taken) = successor.claim_candidate(10).await? {
                    stolen = Some(taken.claim_token);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    successor.pool.close().await;
    monitored?;
    ensure!(
        stolen.is_none(),
        "a successor Ledger took the claim while it was processing"
    );
    tokio::time::timeout(Duration::from_secs(30), process)
        .await
        .context("the claim did not stop")?
        .context("the claim task failed")?
        .context("the claim failed")?;
    let elapsed = started.elapsed();

    // Every value `claim_expires_at` took is a renewal at `expiry - lease`, so
    // the difference of two expiries is the gap between their renewals.
    let gaps: Vec<(Duration, Instant)> = expiries
        .windows(2)
        .map(|pair| {
            (
                Duration::from_micros(u64::try_from(pair[1].0 - pair[0].0).unwrap_or(0)),
                pair[1].1,
            )
        })
        .collect();
    let bound = SHORT_LEASE.interval + SHORT_LEASE.timeout * 2;
    let max_gap = gaps.iter().map(|gap| gap.0).max().unwrap_or_default();
    let (rebuild_line, during) = match rebuild {
        Some((first, last)) => {
            let during: Vec<Duration> = gaps
                .iter()
                .filter(|gap| gap.1 >= first && gap.1 <= last + bound)
                .map(|gap| gap.0)
                .collect();
            (
                format!(
                    "build slot held for {:.0} ms ({} renewals landed in it, max gap {:.2} ms)",
                    ms(last - first),
                    during.len(),
                    ms(during.iter().copied().max().unwrap_or_default())
                ),
                during.len(),
            )
        }
        None => ("the rebuild was shorter than the 10 ms poll".into(), 0),
    };
    println!(
        "[n={n}] incident 1: claim processed in {:.2} s; {} renewals observed, max gap {:.2} ms (bound {:.0} ms); {rebuild_line}",
        elapsed.as_secs_f64(),
        expiries.len().saturating_sub(1),
        ms(max_gap),
        ms(bound),
    );
    ensure!(
        expiries.len() >= 2,
        "no renewal was observed during a {:.2} s claim",
        elapsed.as_secs_f64()
    );
    ensure!(
        expiries.windows(2).all(|pair| pair[1].0 > pair[0].0),
        "claim_expires_at moved backwards: {:?}",
        expiries.iter().map(|expiry| expiry.0).collect::<Vec<_>>()
    );
    ensure!(
        max_gap < bound,
        "the largest gap between renewals was {:.2} ms, not below interval + 2 x timeout = {:.0} ms",
        ms(max_gap),
        ms(bound)
    );
    ensure!(
        lapses.is_empty(),
        "the claimed lease was observed expired {} times, first at {:?}",
        lapses.len(),
        lapses.first()
    );
    let (state, submissions) = (
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$1",
        )
        .bind(&solved.block_hash)
        .fetch_one(&coordinator.ledger.pool)
        .await?,
        fixture.chain.lock().await.submissions,
    );
    ensure!(
        state == "submitted" && submissions == 1,
        "the claim finished as {state} after {submissions} submissions"
    );
    if rebuild.is_some() && elapsed > bound * 4 {
        ensure!(
            during > 0,
            "a rebuild that held the build slot saw no renewal"
        );
    }
    Ok(())
}

/// Incident 1 at the reduced size.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incident_1_lease_renewal_is_not_delayed_by_a_large_window_claim() -> Result<()> {
    let Some(raw) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    incident_1(&raw, share_count(SHARES_VAR, 20_000)?).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full-size window: needs gigabytes of RAM, and cannot reach 400k until #273 stops the job payload copying the window: three copies cross PostgreSQL's 256 MiB jsonb limit above about 150k shares. The coordinator runs it with --ignored, at a PRISM_WINDOW_QUALIFY_FULL_SHARES that divides the window weight; 125000 is the largest that fits today"]
async fn incident_1_lease_renewal_at_full_size() -> Result<()> {
    let raw = qbit_prism_test_gate::required_database_url(qbit_prism_test_gate::site!())?;
    let _serial = TEST_LOCK.lock().await;
    incident_1(&raw, share_count(FULL_SHARES_VAR, 400_000)?).await
}

// ---------------------------------------------------------------------------
// Canonical bytes
// ---------------------------------------------------------------------------

async fn canonical_identity(raw: &str, n: u64) -> Result<()> {
    let fixture = Fixture::open(raw).await?;
    let outcome = canonical_identity_body(&fixture, n).await;
    settle(outcome, fixture.close().await)
}

async fn canonical_identity_body(fixture: &Fixture, n: u64) -> Result<()> {
    let solved = fixture.solve(n).await?;
    let coordinator = &fixture.coordinator;
    let bundle = solved
        .prepared
        .bundle
        .clone()
        .context("the refresh built no bundle")?;
    ensure!(
        bundle.coinbase_script_sig_suffix_hex.as_deref() == Some(solved.coinbase_suffix_hex.as_str()),
        "the zero-extranonce candidate's coinbase suffix {} is not the refresh bundle's {:?}; the bundles would differ by design",
        solved.coinbase_suffix_hex,
        bundle.coinbase_script_sig_suffix_hex
    );
    let claim = coordinator
        .ledger
        .claim_candidate(120)
        .await?
        .context("the solved candidate could not be claimed")?;
    ensure!(claim.candidate.block_hash == solved.block_hash);
    ensure!(
        claim.candidate.window == solved.prepared.window,
        "the candidate does not carry the refresh's window reference"
    );
    // The claim's own rebuild: `build_slots`, `Coordinator::read_window` under
    // its permit, then `build_claim_parts` on a blocking thread.
    let parts = match coordinator
        .rebuild_claim_parts(&claim, BalanceSource::Current, CANDIDATE_LEASE)
        .await?
    {
        Ok(parts) => parts,
        Err(failure) => bail!("the claim's rebuild failed: {failure:?}"),
    };
    let (expected, rebuilt) = tokio::task::spawn_blocking(move || {
        let expected = qbit_prism::canonical_audit_bundle_bytes(&bundle)?;
        let rebuilt =
            qbit_prism::canonical_audit_bundle_bytes_from_parts(&parts.body, &parts.shares)?;
        drop(parts);
        Ok::<_, anyhow::Error>((expected, rebuilt))
    })
    .await??;
    println!(
        "[n={n}] canonical audit bytes: refresh {} B sha256 {}, claim rebuild {} B sha256 {}",
        expected.len(),
        hex::encode(Sha256::digest(&expected)),
        rebuilt.len(),
        hex::encode(Sha256::digest(&rebuilt))
    );
    ensure!(
        expected == rebuilt,
        "the claim rebuilt different canonical bytes than refresh_once built for the same job"
    );
    drop((expected, rebuilt));

    // The landed digest is the refresh bundle's.
    let report = {
        let bundle = solved.prepared.bundle.clone().context("bundle")?;
        let key = coordinator.config.ledger_public_key.clone();
        tokio::task::spawn_blocking(move || {
            qbit_prism::verify_audit_bundle_with_ledger_public_key(&bundle, &key)
        })
        .await??
    };
    coordinator.process_candidate(&claim).await?;
    let landed: String = sqlx::query_scalar(
        "SELECT audit_bundle_sha256 FROM qbit_pool_audit_bundles WHERE block_hash=$1",
    )
    .bind(&solved.block_hash)
    .fetch_one(&coordinator.ledger.pool)
    .await?;
    ensure!(
        landed == report.audit_bundle_sha256_hex,
        "the landed audit digest {landed} is not the refresh bundle's {}",
        report.audit_bundle_sha256_hex
    );
    Ok(())
}

/// A claim rebuilds byte-identical canonical audit bytes, at the reduced size.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn claim_rebuild_has_the_canonical_bytes_refresh_built_for_the_job() -> Result<()> {
    let Some(raw) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    canonical_identity(&raw, share_count(SHARES_VAR, 20_000)?).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full-size window: needs gigabytes of RAM, and cannot reach 400k until #273 stops the job payload copying the window: three copies cross PostgreSQL's 256 MiB jsonb limit above about 150k shares. The coordinator runs it with --ignored, at a PRISM_WINDOW_QUALIFY_FULL_SHARES that divides the window weight; 125000 is the largest that fits today"]
async fn claim_rebuild_canonical_bytes_at_full_size() -> Result<()> {
    let raw = qbit_prism_test_gate::required_database_url(qbit_prism_test_gate::site!())?;
    let _serial = TEST_LOCK.lock().await;
    canonical_identity(&raw, share_count(FULL_SHARES_VAR, 400_000)?).await
}
