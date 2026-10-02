//! #575 item 5: wall-clock jumps while the pool is mining.
//!
//! Both servers run under libfaketime, each with its own offset file, and so
//! does the test's own PostgreSQL 16 cluster, whose clock drives every lease
//! and claim expiry (`clock_timestamp()`) and the ledger clock. Only the
//! realtime clock moves (`FAKETIME_DONT_FAKE_MONOTONIC=1`), as an NTP step
//! does: every timeout and deadline, which the servers take from the
//! monotonic clock, is unaffected. The node keeps the real clock.
//!
//! Both servers reach the node through a proxy that records every
//! `submitblock` and can hold one, so a found block can be mid-landing, its
//! claim live, while the database clock jumps.
//!
//! Phases, each mining shares and a block on every server that must serve:
//! 1. baseline;
//! 2. server 0 ten minutes behind, then ten minutes ahead; server 1 three
//!    hours behind, then three hours ahead (minutes and hours, back and
//!    forward). In each, the other server lands a block first, so the moved
//!    one must build work on a template it has not seen. Behind, it serves.
//!    Ahead of the node by more than `PRISM_TEMPLATE_MAX_AGE_SECONDS` (120 s)
//!    it calls every template stale and serves no current work (fail-safe);
//!    at three hours ahead the test watches it for 200 s, with a miner
//!    retrying, and `PrismWorkRefreshStalledCritical` must fire. Corrected,
//!    it serves again;
//! 3. the database two hours ahead while server 0's block is held
//!    mid-landing, so the database clock calls every live claim expired;
//!    since #581 no frontend takes a claim over by the database clock, so
//!    the holder keeps it;
//! 4. the database back to the real clock (two hours back) while server 1's
//!    block is held mid-landing;
//! 5. every clock real again.
//!
//! It asserts that each server's HTTP `Date` and the database's
//! `clock_timestamp()` really moved; every acknowledged share is in the
//! ledger, credited once, with `accepted_at` non-decreasing in ledger order;
//! every accepted block was offered exactly once and landed once, as one
//! confirmed pool block on the node's chain whose coinbase matches its audit
//! bundle; and the carry-forward integrity report is clean. Every outbox row
//! is `submitted` (#581): neither held block is taken over across its
//! database step, so each records its own one offer as accepted, rather
//! than phase 3's block being recovered as an unknown offer and stranded in
//! reconciliation with a retry two hours out, as it was before #581.
use super::alert_rules::{sample, scrape, snapshot_stale, Mirror, Verdict};
use super::host_tools::program;
use super::private_postgres::{ClusterOptions, PrivateCluster};
use super::share_client::{start_share_only_servers, Proof, ShareClient, Submitted};
use super::submit_proxy::SubmitProxy;
use super::*;
use std::{collections::BTreeMap, path::Path};

/// Shares each serving server mines per phase.
const PHASE_SHARES: usize = 5;
/// How long a server has to serve work on the current tip in a phase.
const WORK_SECONDS: u64 = 30;
/// How long a held landing stays held after its clock jump.
const HOLD_AFTER_JUMP: Duration = Duration::from_secs(8);
/// Phases 3 and 4, by the label their records carry.
const HELD_FORWARD: &str = "database +2 h mid-landing";
const HELD_BACK: &str = "database back 2 h mid-landing";
/// A generous `submitblock` deadline, so a held call outlives the jump.
const SUBMIT_TIMEOUT_SECONDS: &str = "60";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "nightly #575: server and database wall-clock jumps under mining; needs libfaketime"]
async fn wall_clock_jumps_keep_windows_claims_and_payouts_and_land_every_block_once() -> Result<()>
{
    // Selected explicitly, so a missing input fails rather than skipping.
    let inputs = gate::required_inputs(
        gate::site!(),
        &[
            gate::Input::QbitdBin,
            gate::Input::DatabaseUrl,
            gate::Input::PgBinDir,
        ],
    )?;
    let library = faketime_library()?;
    drop(SERIAL.lock().await);
    let home = tempfile::Builder::new().prefix("b575c-").tempdir()?;
    let clocks = Clocks::new(home.path())?;
    let cluster = PrivateCluster::start(ClusterOptions {
        bin: PathBuf::from(&inputs[2]),
        data: home.path().join("data"),
        home: home.path().to_path_buf(),
        initdb: &[],
        settings: "",
        env: clocks
            .environment(&library, &clocks.database)
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value))
            .collect(),
    })?;
    let Some(mut fixture) = Fixture::open_on_database(false, false, Some(&cluster.url())).await?
    else {
        bail!("the live fixture's inputs were required above");
    };
    let mut proxy = None;
    let result = async {
        let started = SubmitProxy::start(fixture.rpc_port).await?;
        let proxy = proxy.insert(started);
        jumps(&mut fixture, &clocks, &library, proxy).await
    }
    .await;
    if result.is_err() {
        eprintln!("{}", fixture.diagnostics());
        eprintln!("{}", cluster.diagnostics());
    }
    // Real clocks for cleanup.
    let _ = clocks.set_all(0);
    if let Some(proxy) = &proxy {
        proxy.release_all();
    }
    let cleanup = fixture.cleanup().await;
    drop(proxy);
    drop(cluster);
    drop(home);
    result.and(cleanup)
}

/// libfaketime, found next to the `faketime` wrapper on `PATH`, as the
/// Debian/Ubuntu `faketime` and `libfaketime` packages install it.
fn faketime_library() -> Result<PathBuf> {
    let wrapper = program("faketime").context("install the faketime and libfaketime packages")?;
    let prefix = wrapper
        .parent()
        .and_then(Path::parent)
        .context("faketime has no prefix")?;
    let mut candidates = vec![prefix.join("lib/faketime/libfaketime.so.1")];
    for entry in std::fs::read_dir(prefix.join("lib"))? {
        candidates.push(entry?.path().join("faketime/libfaketime.so.1"));
    }
    candidates
        .into_iter()
        .find(|candidate| candidate.is_file())
        .with_context(|| format!("libfaketime.so.1 not found under {}", prefix.display()))
}

/// The offset files: one per server and one for the database. libfaketime
/// re-reads a file at most once a second.
struct Clocks {
    servers: [PathBuf; 2],
    database: PathBuf,
}

impl Clocks {
    fn new(home: &Path) -> Result<Self> {
        let clocks = Self {
            servers: [home.join("clock-server-0"), home.join("clock-server-1")],
            database: home.join("clock-database"),
        };
        clocks.set_all(0)?;
        Ok(clocks)
    }

    fn environment(&self, library: &Path, file: &Path) -> Vec<(&'static str, String)> {
        vec![
            ("LD_PRELOAD", library.display().to_string()),
            ("FAKETIME_TIMESTAMP_FILE", file.display().to_string()),
            ("FAKETIME_CACHE_DURATION", "1".into()),
            ("FAKETIME_DONT_FAKE_MONOTONIC", "1".into()),
        ]
    }

    /// Replace an offset atomically, so a reader never sees half a write.
    fn set(file: &Path, seconds: i64) -> Result<()> {
        let staging = file.with_extension("new");
        std::fs::write(&staging, format!("{seconds:+}\n"))?;
        std::fs::rename(staging, file)?;
        Ok(())
    }

    fn set_all(&self, seconds: i64) -> Result<()> {
        for file in self.servers.iter().chain([&self.database]) {
            Self::set(file, seconds)?;
        }
        Ok(())
    }
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_secs() as i64
}

/// Move server `index`'s clock to `offset` seconds from real time and wait
/// until it has. The server's HTTP `Date` shows its clock, but hyper caches
/// that header until its clock passes the cached second, so it moves only
/// when the clock passes its highest value so far (`highest`). A move that
/// does not is given twice libfaketime's one-second re-read; the first move
/// of each server, forward, has proven the library reads its file. (The
/// server makes itself non-dumpable, so its `/proc` maps are unreadable.)
async fn move_server(
    f: &Fixture,
    clocks: &Clocks,
    highest: &mut [i64; 2],
    index: usize,
    offset: i64,
) -> Result<()> {
    Clocks::set(&clocks.servers[index], offset)?;
    if offset <= highest[index] {
        tokio::time::sleep(Duration::from_millis(2500)).await;
        return Ok(());
    }
    highest[index] = offset;
    until(
        &format!("server {index} clock at {offset:+}s"),
        15,
        || async {
            let response = f
                .client
                .get(format!("http://127.0.0.1:{}/healthz", f.api[index]))
                .send()
                .await?;
            let date = response
                .headers()
                .get("date")
                .context("no Date header")?
                .to_str()?;
            let served = chrono::DateTime::parse_from_rfc2822(date)?.timestamp();
            Ok((served - unix_now() - offset).abs() <= 3)
        },
    )
    .await
}

/// Wait until the database's `clock_timestamp()` is `offset` seconds from
/// real time.
async fn database_offset(f: &Fixture, offset: i64) -> Result<()> {
    until(&format!("database clock at {offset:+}s"), 15, || async {
        let served: f64 =
            sqlx::query_scalar("SELECT extract(epoch FROM clock_timestamp())::float8")
                .fetch_one(&f.pool)
                .await?;
        Ok((served as i64 - unix_now() - offset).abs() <= 3)
    })
    .await
}

/// What one phase did on one server.
enum Served {
    /// Its shares and the block, which landed.
    Mined {
        submitted: Vec<Submitted>,
        block: String,
    },
    /// No current work reached a miner: why.
    NoWork(String),
}

/// Refusals of ordinary shares that the phase after the database clock
/// steps back may show (seen once in four runs before #581, beside the
/// stranded outbox row #581 fixed; nothing here has shown that they are
/// gone, so they stay allowed); every other phase must accept every share.
const REFUSED_AFTER_STEP_BACK: [&str; 2] = ["stale-job", "ledger-confirmation-failed"];

/// Mine `PHASE_SHARES` shares and one block on `server`, on the current
/// tip, and wait for the block on the node. Every share must be accepted,
/// or refused for one of `refusable`; the block must be accepted.
async fn mine_on(f: &Fixture, server: usize, label: &str, refusable: &[&str]) -> Result<Served> {
    let tip = f.rpc("getbestblockhash", json!([])).await?;
    let tip = tip.as_str().context("tip missing")?.to_owned();
    let username = format!("{}.clock-{server}", f.address);
    let current = async {
        let mut client = ShareClient::connect(f.stratum[server], &username).await?;
        client
            .work_on(&tip, Duration::from_secs(WORK_SECONDS))
            .await?;
        Ok::<_, anyhow::Error>(client)
    };
    let mut client = match current.await {
        Ok(client) => client,
        Err(error) => return Ok(Served::NoWork(format!("{error:#}"))),
    };
    let mut submitted = Vec::new();
    for _ in 0..PHASE_SHARES {
        let share = client.submit(Proof::Share).await?;
        ensure!(
            share.answer.accepted()
                || share
                    .answer
                    .reason_id()
                    .is_some_and(|reason| refusable.contains(&reason)),
            "{label}: server {server} answered a share {}",
            share.answer
        );
        submitted.push(share);
    }
    let block = client.submit(Proof::Block).await?;
    ensure!(
        block.answer.accepted(),
        "{label}: server {server} refused its block: {}",
        block.answer
    );
    let hash = block.hash.clone();
    submitted.push(block);
    until(
        &format!("{label}: server {server}'s block on the node"),
        60,
        || async { Ok(f.rpc("getbestblockhash", json!([])).await? == json!(hash)) },
    )
    .await?;
    Ok(Served::Mined {
        submitted,
        block: hash,
    })
}

/// Record of every phase, for the final checks and the report.
#[derive(Default)]
struct Run {
    submitted: Vec<(String, Submitted)>,
    /// Held blocks answered ledger-outcome-unknown, whose shares must still
    /// be credited once their landing commits.
    pending: Vec<String>,
    blocks: Vec<(String, String)>,
    notes: Vec<String>,
}

impl Run {
    /// Mine on `server` in a phase where it must serve work, and keep what
    /// it did.
    async fn mine(
        &mut self,
        f: &Fixture,
        server: usize,
        label: &str,
        refusable: &[&str],
    ) -> Result<()> {
        match mine_on(f, server, label, refusable).await? {
            Served::NoWork(why) => bail!("{label}: server {server} served no current work: {why}"),
            Served::Mined { submitted, block } => {
                self.submitted.extend(
                    submitted
                        .into_iter()
                        .map(|submitted| (label.to_owned(), submitted)),
                );
                self.blocks.push((label.to_owned(), block));
                Ok(())
            }
        }
    }
}

/// `-10 min`, `+3 h`: an offset as the phase names show it.
fn span(seconds: i64) -> String {
    if seconds.abs() >= 3600 {
        format!("{:+} h", seconds / 3600)
    } else {
        format!("{:+} min", seconds / 60)
    }
}

/// How long a server that serves no work is watched for an alert: longer
/// than the stall threshold plus the `for` of every rule below.
const UNSERVED_WATCH: Duration = Duration::from_secs(200);

/// While `server` serves no work, keep a miner connecting and authorizing
/// to it (as a real one would) and report which paging rules fire on it.
/// The refresh stall is what this state is, so its rule must.
async fn unserved_alerts(f: &Fixture, server: usize) -> Result<String> {
    let mirrors = [
        Mirror::new(
            "PrismWorkRefreshStalledCritical",
            &["qbit_prism_work_refresh_stalled_seconds", ">= bool 120"],
            |_, s| {
                s.gate_open()
                    && s.value("qbit_prism_work_refresh_stalled_seconds")
                        .is_some_and(|stalled| stalled >= 120.0)
            },
        )?,
        Mirror::new(
            "PrismCurrentWorkGapHigh",
            &[
                "qbit_prism_stratum_current_tip_coverage_gap_seconds",
                "> bool 15",
            ],
            |_, s| {
                s.gate_open()
                    && s.value("qbit_prism_stratum_current_tip_coverage_gap_seconds")
                        .is_some_and(|gap| gap > 15.0)
                    && s.value("qbit_prism_authorized_clients")
                        .is_some_and(|n| n > 0.0)
            },
        )?,
        Mirror::new(
            "PrismSemanticWorkCoverageLoss",
            &[
                "qbit_prism_stratum_semantic_current_work_ratio",
                "< bool 0.95",
            ],
            |_, s| {
                s.gate_open()
                    && s.value("qbit_prism_stratum_semantic_current_work_ratio")
                        .is_some_and(|ratio| (0.0..0.95).contains(&ratio))
                    && s.value("qbit_prism_authorized_clients")
                        .is_some_and(|n| n > 0.0)
            },
        )?,
        Mirror::new(
            "PrismTimeToUsableWorkHigh",
            &[
                "qbit_prism_stratum_oldest_pending_initial_job_seconds",
                "> bool 15",
            ],
            |_, s| {
                s.gate_open()
                    && s.value("qbit_prism_stratum_oldest_pending_initial_job_seconds")
                        .is_some_and(|age| age > 15.0)
                    && s.value("qbit_prism_stratum_pending_initial_jobs")
                        .is_some_and(|n| n > 0.0)
            },
        )?,
        snapshot_stale()?,
    ];
    let port = f.stratum[server];
    let username = format!("{}.clock-waiting-{server}", f.address);
    let waiting = tokio::spawn(async move {
        loop {
            // Waits for work that never comes, until the server closes it.
            let _ = ShareClient::connect(port, &username).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });
    let scrapes = sample(&f.client, &[(server, f.api[server])], UNSERVED_WATCH).await;
    waiting.abort();
    let verdict = Verdict::of(&scrapes, &[server], &mirrors);
    let report = format!(
        "server {server} unserved for {UNSERVED_WATCH:?}: {}",
        verdict.report.replace('\n', ", ")
    );
    ensure!(
        verdict.fired[&server].contains("PrismWorkRefreshStalledCritical"),
        "no work-refresh page while a server served no work: {report}"
    );
    Ok(report)
}

/// Phases 3 and 4: hold `holder`'s next found block at the proxy, move the
/// database clock to `offset` while it is held, then release it and wait
/// for it on the node.
async fn held_landing(
    f: &Fixture,
    clocks: &Clocks,
    proxy: &SubmitProxy,
    run: &mut Run,
    holder: usize,
    offset: i64,
    label: &str,
) -> Result<()> {
    let held = proxy.arm();
    let tip = f.rpc("getbestblockhash", json!([])).await?;
    let tip = tip.as_str().context("tip missing")?.to_owned();
    let username = format!("{}.clock-held-{holder}", f.address);
    let mut client = ShareClient::connect(f.stratum[holder], &username).await?;
    client
        .work_on(&tip, Duration::from_secs(WORK_SECONDS))
        .await?;
    // The block's answer waits for its landing, which the proxy holds:
    // submit it concurrently.
    let submit = tokio::spawn(async move { client.submit(Proof::Block).await });
    let hit = tokio::time::timeout(Duration::from_secs(30), held)
        .await
        .context("no submitblock reached the proxy")??;
    let reserved: String =
        sqlx::query_scalar("SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$1")
            .bind(&hit.block)
            .fetch_one(&f.pool)
            .await?;
    Clocks::set(&clocks.database, offset)?;
    database_offset(f, offset).await?;
    tokio::time::sleep(HOLD_AFTER_JUMP).await;
    let offers_while_held = proxy.offers(&hit.block);
    let _ = hit.release.send(());
    let block = submit.await??;
    ensure!(
        block.hash == hit.block,
        "{label}: the held block {} is not the one submitted, {}",
        hit.block,
        block.hash
    );
    run.notes.push(format!(
        "{label}: held block was {reserved} at the jump; {offers_while_held} submitblock call(s) while held; answer {}",
        block.answer
    ));
    // The answer waits for the landing up to the share commit timeout
    // (#577); a hold that outlasts it is answered ledger-outcome-unknown,
    // and the block still lands and its share is credited once.
    ensure!(
        block.answer.accepted() || block.answer.reason_id() == Some("ledger-outcome-unknown"),
        "{label}: the held block was refused: {}",
        block.answer
    );
    if !block.answer.accepted() {
        run.pending.push(block.share_id.clone());
    }
    let hash = block.hash.clone();
    run.submitted.push((label.to_owned(), block));
    run.blocks.push((label.to_owned(), hash.clone()));
    until(
        &format!("{label}: the held block on the node"),
        60,
        || async { Ok(f.rpc("getbestblockhash", json!([])).await? == json!(hash)) },
    )
    .await
}

async fn jumps(
    f: &mut Fixture,
    clocks: &Clocks,
    library: &Path,
    proxy: &SubmitProxy,
) -> Result<()> {
    let mut servers = Vec::new();
    for index in 0..2 {
        let mut overrides: Vec<(&str, String)> = vec![
            ("QBIT_RPC_PORT", proxy.port.to_string()),
            (
                "PRISM_BLOCK_SUBMIT_RPC_TIMEOUT_SECONDS",
                SUBMIT_TIMEOUT_SECONDS.into(),
            ),
        ];
        overrides.extend(clocks.environment(library, &clocks.servers[index]));
        servers.push((index, overrides));
    }
    start_share_only_servers(f, &servers).await?;
    let mut run = Run::default();

    // 1. Baseline, and proof that the clocks are the injected ones.
    let mut highest = [-1i64; 2];
    for index in 0..2 {
        move_server(f, clocks, &mut highest, index, 0).await?;
    }
    database_offset(f, 0).await?;
    for server in 0..2 {
        run.mine(f, server, "baseline", &[]).await?;
    }

    // 2. Each server alone, minutes then hours: behind, then ahead.
    // In each phase the other server lands a block first, so the moved one
    // must build work on a template it has not seen.
    for (server, seconds, sample_alerts) in [(0usize, 600i64, false), (1, 3 * 3600, true)] {
        let other = 1 - server;
        let behind = format!("server {server} {}", span(-seconds));
        move_server(f, clocks, &mut highest, server, -seconds).await?;
        run.mine(f, other, &behind, &[]).await?;
        run.mine(f, server, &behind, &[]).await?;

        // Ahead of the node by more than PRISM_TEMPLATE_MAX_AGE_SECONDS
        // (120 s), every template the server reads looks stale: it builds no
        // work on the new tip (fail-safe), until its clock is corrected.
        let ahead = format!("server {server} {}", span(seconds));
        move_server(f, clocks, &mut highest, server, seconds).await?;
        run.mine(f, other, &ahead, &[]).await?;
        let Served::NoWork(why) = mine_on(f, server, &ahead, &[]).await? else {
            bail!("{ahead}: server {server} served work on a template its clock calls stale");
        };
        run.notes.push(format!(
            "{ahead}: server {server} served no current work, as expected: {why}"
        ));
        if sample_alerts {
            run.notes.push(unserved_alerts(f, server).await?);
        }
        move_server(f, clocks, &mut highest, server, 0).await?;
        run.mine(f, server, &format!("server {server} corrected"), &[])
            .await?;
    }

    // 3 and 4. The database clock jumps while a block is mid-landing.
    held_landing(f, clocks, proxy, &mut run, 0, 2 * 3600, HELD_FORWARD).await?;
    for server in 0..2 {
        run.mine(f, server, HELD_FORWARD, &[]).await?;
    }
    held_landing(f, clocks, proxy, &mut run, 1, 0, HELD_BACK).await?;
    for server in 0..2 {
        run.mine(f, server, HELD_BACK, &REFUSED_AFTER_STEP_BACK)
            .await?;
    }

    // 5. Every clock real again: every share is accepted again.
    for index in 0..2 {
        move_server(f, clocks, &mut highest, index, 0).await?;
    }
    Clocks::set(&clocks.database, 0)?;
    database_offset(f, 0).await?;
    for server in 0..2 {
        run.mine(f, server, "real again", &[]).await?;
    }
    f.quiesce().await?;
    verify(f, &mut run, proxy).await
}

async fn verify(f: &Fixture, run: &mut Run, proxy: &SubmitProxy) -> Result<()> {
    // Shares: every acknowledgement is durable and credited once, and the
    // ledger clock never ran backwards.
    let ledger: BTreeMap<String, bool> =
        sqlx::query_as::<_, (String, bool)>("SELECT share_id, accepted FROM qbit_share_ledger")
            .fetch_all(&f.pool)
            .await?
            .into_iter()
            .collect();
    let lost: Vec<_> = run
        .submitted
        .iter()
        .filter(|(_, submitted)| submitted.answer.accepted())
        .filter(|(_, submitted)| ledger.get(&submitted.share_id) != Some(&true))
        .map(|(label, submitted)| format!("{label}: {}", submitted.share_id))
        .collect();
    ensure!(lost.is_empty(), "acknowledged shares missing: {lost:?}");
    let uncredited: Vec<_> = run
        .pending
        .iter()
        .filter(|share_id| ledger.get(*share_id) != Some(&true))
        .collect();
    ensure!(
        uncredited.is_empty(),
        "held blocks answered ledger-outcome-unknown were never credited: {uncredited:?}"
    );
    let (accepted, credited): (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM qbit_share_ledger WHERE accepted),(SELECT count(*) FROM qbit_prism_share_hashes)",
    )
    .fetch_one(&f.pool)
    .await?;
    ensure!(
        accepted == credited,
        "duplicate headers credited: {accepted} accepted shares, {credited} credited headers"
    );
    let regressions: Vec<(i64, String, String)> = sqlx::query_as(
        "SELECT share_seq, accepted_at::text, previous::text FROM (SELECT share_seq, accepted_at, lag(accepted_at) OVER (ORDER BY share_seq) AS previous FROM qbit_share_ledger WHERE accepted) s WHERE accepted_at < previous",
    )
    .fetch_all(&f.pool)
    .await?;
    ensure!(
        regressions.is_empty(),
        "the ledger clock ran backwards: {regressions:?}"
    );

    // Blocks: each landed exactly once, on the node's chain, with a coinbase
    // that matches its audit bundle.
    let key = ManifestSigningKey::from_seed_hex(&"22".repeat(32))?.public_key_hex();
    let mut offers = BTreeMap::new();
    for (label, hash) in &run.blocks {
        // A row the server released into reconciliation (a post-offer step
        // that lost a race, say) finishes on its retry, 10 s per attempt
        // later; quiesce does not wait for that. Since #581 a retry the
        // database clock's step back left two hours out is due at once, so
        // every row finishes.
        let rows = || async {
            Ok::<_, anyhow::Error>(
                sqlx::query_as::<_, (String, i32, Option<String>)>(
                    "SELECT state, attempt_count, offer_outcome FROM qbit_block_candidate_outbox WHERE block_hash=$1",
                )
                .bind(hash)
                .fetch_all(&f.pool)
                .await?,
            )
        };
        until(
            &format!("{label}: block {hash}'s outbox row settled"),
            120,
            || async {
                let rows = rows().await?;
                Ok(rows.len() == 1 && rows[0].0 == "submitted")
            },
        )
        .await?;
        let rows: Vec<(String, i32)> = rows()
            .await?
            .into_iter()
            .map(|(state, attempts, _)| (state, attempts))
            .collect();
        let landed: Vec<String> =
            sqlx::query_scalar("SELECT chain_state FROM qbit_pool_blocks WHERE block_hash=$1")
                .bind(hash)
                .fetch_all(&f.pool)
                .await?;
        ensure!(
            landed == ["confirmed"],
            "{label}: block {hash} pool-block rows {landed:?}"
        );
        ensure!(
            rows.len() == 1 && rows[0].0 == "submitted",
            "{label}: block {hash} outbox rows {rows:?}"
        );
        if label == HELD_FORWARD || label == HELD_BACK {
            // #581: the held block's holder kept its claim across the
            // database step and recorded its one offer itself. Before #581
            // the +2 h step handed phase 3's live claim to a second frontend,
            // which recovered the reservation as an unknown offer.
            let (outcome,): (Option<String>,) = sqlx::query_as(
                "SELECT offer_outcome FROM qbit_block_candidate_outbox WHERE block_hash=$1",
            )
            .bind(hash)
            .fetch_one(&f.pool)
            .await?;
            ensure!(
                outcome.as_deref() == Some("accepted") && proxy.offers(hash) == 1,
                "{label}: block {hash} offer outcome {outcome:?}, {} submitblock call(s): its claim was taken over across the database step",
                proxy.offers(hash)
            );
        }
        let block = f.rpc("getblock", json!([hash, 2])).await?;
        ensure!(
            block["confirmations"].as_i64().is_some_and(|c| c >= 1),
            "{label}: block {hash} is not on the node's active chain"
        );
        let body: Value = f
            .client
            .get(format!(
                "http://127.0.0.1:{}/audit/blocks/{hash}/bundle",
                f.api[0]
            ))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let bundle: AuditBundle = serde_json::from_value(body["audit_bundle"].clone())?;
        let coinbase = f
            .rpc(
                "getrawtransaction",
                json!([block["tx"][0]["txid"], false, hash]),
            )
            .await?;
        verify_audit_bundle_against_coinbase_tx_hex(
            &bundle,
            coinbase.as_str().context("node coinbase missing")?,
            &key,
        )
        .with_context(|| format!("{label}: block {hash} coinbase differs from its audit bundle"))?;
        offers.insert(hash.clone(), (label.clone(), rows[0].1, proxy.offers(hash)));
    }
    // Only the pool mines, so every block on the chain above the first is
    // one of the pool's.
    let pool_blocks: i64 =
        sqlx::query_scalar("SELECT count(*) FROM qbit_pool_blocks WHERE chain_state='confirmed'")
            .fetch_one(&f.pool)
            .await?;
    ensure!(
        pool_blocks == run.blocks.len() as i64,
        "{pool_blocks} confirmed pool blocks for {} accepted blocks",
        run.blocks.len()
    );
    let double: Vec<_> = offers
        .iter()
        .filter(|(_, (_, _, submitted))| *submitted != 1)
        .collect();
    f.integrity().await?;
    let acknowledged = run
        .submitted
        .iter()
        .filter(|(_, submitted)| submitted.answer.accepted())
        .count();
    let refused: BTreeMap<String, usize> = run
        .submitted
        .iter()
        .filter(|(_, submitted)| !submitted.answer.accepted())
        .fold(BTreeMap::new(), |mut counts, (label, submitted)| {
            *counts
                .entry(format!("{label}: {}", submitted.answer))
                .or_default() += 1;
            counts
        });
    eprintln!(
        "clock jumps: {acknowledged} acknowledged, refused {refused:?}; {} blocks landed once each; offers per block {:?}",
        run.blocks.len(),
        offers
    );
    // Why shares were refused, from each server's own account.
    for (server, port) in f.api.into_iter().enumerate() {
        let causes = scrape(&f.client, server, port)
            .await?
            .by_label("qbit_prism_stale_job_rejections_total", "cause");
        let causes: BTreeMap<_, _> = causes.into_iter().filter(|(_, n)| *n > 0.0).collect();
        let log = std::fs::read_to_string(f.directory.path().join(format!("server-{server}.log")))
            .unwrap_or_default();
        let failures: Vec<_> = log
            .lines()
            .filter(|line| line.contains("share persistence failed"))
            .take(3)
            .collect();
        eprintln!("clock jumps: server {server} stale-job causes {causes:?}; persistence failures {failures:?}");
    }
    for note in &run.notes {
        eprintln!("clock jumps: {note}");
    }
    ensure!(
        double.is_empty(),
        "blocks offered other than exactly once: {double:?}"
    );
    Ok(())
}
