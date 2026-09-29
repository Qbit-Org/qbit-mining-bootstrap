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
//!    mid-landing, so every live claim looks expired;
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
//! is `submitted`, except the one documented current behaviour (#581):
//! phase 3's held block, recovered as an unknown offer while held, stays in
//! reconciliation after phase 4, its retry two hours out.
use super::alert_rules::{longest, rule, sample, scrape, Rule, Scrape};
use super::private_postgres::{ClusterOptions, PrivateCluster};
use super::share_client::{start_share_only_servers, Proof, ShareClient, Submitted};
use super::*;
use qbit_prism_server::codec::{double_sha256, hash_display};
use std::{collections::BTreeMap, path::Path, sync::Arc};
use tokio::{
    io::AsyncReadExt,
    net::TcpStream,
    sync::{oneshot, watch},
};

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
    let wrapper = super::private_postgres::program("faketime")
        .context("install the faketime and libfaketime packages")?;
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
#[derive(Default)]
struct Served {
    submitted: Vec<Submitted>,
    blocks: Vec<String>,
    /// Why the server served no current work in this phase, if it did not.
    unserved: Option<String>,
}

/// Mine `PHASE_SHARES` shares and one block on `server`, on the current
/// tip, and wait for the block on the node.
async fn mine_on(f: &Fixture, server: usize, label: &str) -> Result<Served> {
    let mut served = Served::default();
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
        Err(error) => {
            served.unserved = Some(format!("{error:#}"));
            return Ok(served);
        }
    };
    for _ in 0..PHASE_SHARES {
        served.submitted.push(client.submit(Proof::Share).await?);
    }
    let block = client.submit(Proof::Block).await?;
    let accepted = block.answer.accepted();
    served.submitted.push(block.clone());
    ensure!(
        accepted,
        "{label}: server {server} refused its block: {:?}",
        block.answer
    );
    served.blocks.push(block.hash.clone());
    until(
        &format!("{label}: server {server}'s block on the node"),
        60,
        || async { Ok(f.rpc("getbestblockhash", json!([])).await? == json!(block.hash)) },
    )
    .await?;
    Ok(served)
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
    /// Keep what a server that must serve work did; fail if it served none.
    fn add(&mut self, label: &str, server: usize, served: Served) -> Result<()> {
        if let Some(why) = served.unserved {
            bail!("{label}: server {server} served no current work: {why}");
        }
        self.submitted.extend(
            served
                .submitted
                .into_iter()
                .map(|submitted| (label.to_owned(), submitted)),
        );
        self.blocks.extend(
            served
                .blocks
                .into_iter()
                .map(|block| (label.to_owned(), block)),
        );
        Ok(())
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
/// to it (as a real one would) and report which paging rules fire on it:
/// each rule's condition, mirrored here, held on every scrape for its `for`.
async fn unserved_alerts(f: &Fixture, server: usize) -> Result<String> {
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
    type Condition = Box<dyn Fn(&Scrape) -> bool>;
    let rules: Vec<(Rule, Condition)> = vec![
        (
            rule(
                "PrismWorkRefreshStalledCritical",
                &["qbit_prism_work_refresh_stalled_seconds", ">= bool 120"],
            )?,
            Box::new(|s| {
                s.gate_open()
                    && s.value("qbit_prism_work_refresh_stalled_seconds")
                        .is_some_and(|stalled| stalled >= 120.0)
            }),
        ),
        (
            rule(
                "PrismCurrentWorkGapHigh",
                &[
                    "qbit_prism_stratum_current_tip_coverage_gap_seconds",
                    "> bool 15",
                ],
            )?,
            Box::new(|s| {
                s.gate_open()
                    && s.value("qbit_prism_stratum_current_tip_coverage_gap_seconds")
                        .is_some_and(|gap| gap > 15.0)
                    && s.value("qbit_prism_authorized_clients")
                        .is_some_and(|n| n > 0.0)
            }),
        ),
        (
            rule(
                "PrismSemanticWorkCoverageLoss",
                &[
                    "qbit_prism_stratum_semantic_current_work_ratio",
                    "< bool 0.95",
                ],
            )?,
            Box::new(|s| {
                s.gate_open()
                    && s.value("qbit_prism_stratum_semantic_current_work_ratio")
                        .is_some_and(|ratio| (0.0..0.95).contains(&ratio))
                    && s.value("qbit_prism_authorized_clients")
                        .is_some_and(|n| n > 0.0)
            }),
        ),
        (
            rule(
                "PrismTimeToUsableWorkHigh",
                &[
                    "qbit_prism_stratum_oldest_pending_initial_job_seconds",
                    "> bool 15",
                ],
            )?,
            Box::new(|s| {
                s.gate_open()
                    && s.value("qbit_prism_stratum_oldest_pending_initial_job_seconds")
                        .is_some_and(|age| age > 15.0)
                    && s.value("qbit_prism_stratum_pending_initial_jobs")
                        .is_some_and(|n| n > 0.0)
            }),
        ),
        (
            rule(
                "PrismMetricsSnapshotStale",
                &["qbit_prism_metrics_snapshot_stale", "> bool 0"],
            )?,
            Box::new(|s| {
                s.value("qbit_prism_metrics_snapshot_stale")
                    .is_some_and(|stale| stale > 0.0)
            }),
        ),
    ];
    let mut report = Vec::new();
    let mut stalled_fires = false;
    for (rule, condition) in &rules {
        let held = longest(&scrapes, condition);
        stalled_fires |= rule.title == "PrismWorkRefreshStalledCritical" && held >= rule.hold;
        report.push(format!(
            "{} (for {:?}) {:.0}s{}",
            rule.title,
            rule.hold,
            held.as_secs_f64(),
            if held >= rule.hold { " FIRES" } else { "" }
        ));
    }
    let report = format!(
        "server {server} unserved for {UNSERVED_WATCH:?}: {}",
        report.join(", ")
    );
    // The refresh stall is what this state is: the rule for it must page.
    ensure!(
        stalled_fires,
        "no work-refresh page while a server served no work: {report}"
    );
    Ok(report)
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
        run.add("baseline", server, mine_on(f, server, "baseline").await?)?;
    }

    // 2. Each server alone, minutes then hours: behind, then ahead.
    // In each phase the other server lands a block first, so the moved one
    // must build work on a template it has not seen.
    for (server, seconds, sample_alerts) in [(0usize, 600i64, false), (1, 3 * 3600, true)] {
        let other = 1 - server;
        let behind = format!("server {server} {}", span(-seconds));
        move_server(f, clocks, &mut highest, server, -seconds).await?;
        run.add(&behind, other, mine_on(f, other, &behind).await?)?;
        run.add(&behind, server, mine_on(f, server, &behind).await?)?;

        // Ahead of the node by more than PRISM_TEMPLATE_MAX_AGE_SECONDS
        // (120 s), every template the server reads looks stale: it builds no
        // work on the new tip (fail-safe), until its clock is corrected.
        let ahead = format!("server {server} {}", span(seconds));
        move_server(f, clocks, &mut highest, server, seconds).await?;
        run.add(&ahead, other, mine_on(f, other, &ahead).await?)?;
        let refused = mine_on(f, server, &ahead).await?;
        ensure!(
            refused.unserved.is_some() && refused.submitted.is_empty(),
            "{ahead}: server {server} served work on a template its clock calls stale"
        );
        run.notes.push(format!(
            "{ahead}: server {server} served no current work, as expected: {}",
            refused.unserved.as_deref().unwrap_or_default()
        ));
        if sample_alerts {
            run.notes.push(unserved_alerts(f, server).await?);
        }
        move_server(f, clocks, &mut highest, server, 0).await?;
        let corrected = format!("server {server} corrected");
        run.add(&corrected, server, mine_on(f, server, &corrected).await?)?;
    }

    // 3 and 4. The database clock jumps while a block is mid-landing.
    for (holder, offset, label) in [(0usize, 2 * 3600i64, HELD_FORWARD), (1, 0, HELD_BACK)] {
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
            block.answer.reason()
        ));
        // The answer waits for the landing up to the share commit timeout
        // (#577); a hold that outlasts it is answered ledger-outcome-unknown,
        // and the block still lands and its share is credited once.
        let answered = block.answer.accepted() || block.answer.reason() == "ledger-outcome-unknown";
        if !block.answer.accepted() {
            run.pending.push(block.share_id.clone());
        }
        run.submitted.push((label.to_owned(), block.clone()));
        ensure!(
            answered,
            "{label}: the held block was refused: {:?}",
            block.answer
        );
        run.blocks.push((label.to_owned(), block.hash.clone()));
        until(
            &format!("{label}: the held block on the node"),
            60,
            || async { Ok(f.rpc("getbestblockhash", json!([])).await? == json!(block.hash)) },
        )
        .await?;
        for server in 0..2 {
            run.add(label, server, mine_on(f, server, label).await?)?;
        }
    }

    // 5. Every clock real again.
    for index in 0..2 {
        move_server(f, clocks, &mut highest, index, 0).await?;
    }
    Clocks::set(&clocks.database, 0)?;
    database_offset(f, 0).await?;
    for server in 0..2 {
        run.add(
            "real again",
            server,
            mine_on(f, server, "real again").await?,
        )?;
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
        let rows: Vec<(String, i32)> = sqlx::query_as(
            "SELECT state, attempt_count FROM qbit_block_candidate_outbox WHERE block_hash=$1",
        )
        .bind(hash)
        .fetch_all(&f.pool)
        .await?;
        let landed: Vec<String> =
            sqlx::query_scalar("SELECT chain_state FROM qbit_pool_blocks WHERE block_hash=$1")
                .bind(hash)
                .fetch_all(&f.pool)
                .await?;
        ensure!(
            landed == ["confirmed"],
            "{label}: block {hash} pool-block rows {landed:?}"
        );
        if !(rows.len() == 1 && rows[0].0 == "submitted") {
            // #581: the held block whose claim the +2 h database jump
            // expired is recovered as an unknown offer before it is on the
            // chain, and parked in reconciliation with a retry at the
            // database's +2 h clock. Once that clock steps back the retry is
            // two hours away, so the row stays in reconciliation although
            // the block landed once and its pool block is confirmed. Only
            // that row may be left so, and only in that state.
            let (outcome,): (Option<String>,) = sqlx::query_as(
                "SELECT offer_outcome FROM qbit_block_candidate_outbox WHERE block_hash=$1",
            )
            .bind(hash)
            .fetch_one(&f.pool)
            .await?;
            ensure!(
                label == HELD_FORWARD
                    && rows.len() == 1
                    && rows[0].0 == "reconciliation"
                    && outcome.as_deref() == Some("unknown")
                    && proxy.offers(hash) == 1,
                "{label}: block {hash} outbox rows {rows:?}, offer outcome {outcome:?}, {} submitblock call(s)",
                proxy.offers(hash)
            );
            run.notes.push(format!(
                "{label}: block {hash} landed once and is confirmed, its outbox row left in reconciliation (#581)"
            ));
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
                .entry(format!("{label}: {}", submitted.answer.reason()))
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

/// A held `submitblock`: its block and the release.
struct Hit {
    block: String,
    release: oneshot::Sender<()>,
}

#[derive(Default)]
struct ProxyState {
    upstream: u16,
    submitted: std::sync::Mutex<Vec<String>>,
    trap: std::sync::Mutex<Option<oneshot::Sender<Hit>>>,
    /// Flipped to release every held call at cleanup.
    released: Option<watch::Sender<bool>>,
}

/// An HTTP/1.1 JSON-RPC proxy in front of the node that records every
/// `submitblock` and, when armed, holds the next one until its release while
/// every other call passes.
struct SubmitProxy {
    port: u16,
    state: Arc<ProxyState>,
    task: tokio::task::JoinHandle<()>,
}

impl SubmitProxy {
    async fn start(upstream: u16) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let state = Arc::new(ProxyState {
            upstream,
            released: Some(watch::Sender::new(false)),
            ..ProxyState::default()
        });
        let shared = state.clone();
        let task = tokio::spawn(async move {
            while let Ok((downstream, _)) = listener.accept().await {
                let state = shared.clone();
                tokio::spawn(async move {
                    let _ = relay(state, downstream).await;
                });
            }
        });
        Ok(Self { port, state, task })
    }

    /// Hold the next `submitblock`.
    fn arm(&self) -> oneshot::Receiver<Hit> {
        let (sender, receiver) = oneshot::channel();
        *self.state.trap.lock().unwrap() = Some(sender);
        receiver
    }

    /// How many `submitblock` calls for `block` reached the proxy.
    fn offers(&self, block: &str) -> usize {
        self.state
            .submitted
            .lock()
            .unwrap()
            .iter()
            .filter(|submitted| *submitted == block)
            .count()
    }

    fn release_all(&self) {
        if let Some(released) = &self.state.released {
            released.send_replace(true);
        }
    }
}

impl Drop for SubmitProxy {
    fn drop(&mut self) {
        self.release_all();
        self.task.abort();
    }
}

/// Relays one server connection, one request and reply at a time. Each
/// request goes to the node on a new connection, opened after any hold: a
/// held call can outlast qbitd's idle timeout, and must not be written into
/// a connection the node has closed meanwhile.
async fn relay(state: Arc<ProxyState>, downstream: TcpStream) -> Result<()> {
    let (down_read, mut down_write) = downstream.into_split();
    let mut down_read = BufReader::new(down_read);
    loop {
        let Some((request, body)) = read_http(&mut down_read).await? else {
            return Ok(());
        };
        if let Some(block) = submitted_block(&body) {
            state.submitted.lock().unwrap().push(block.clone());
            let trap = state.trap.lock().unwrap().take();
            if let Some(sender) = trap {
                let (release, released) = oneshot::channel();
                let mut cleanup = state
                    .released
                    .as_ref()
                    .context("proxy release missing")?
                    .subscribe();
                if sender.send(Hit { block, release }).is_ok() {
                    tokio::select! {
                        _ = released => {}
                        _ = cleanup.wait_for(|released| *released) => {}
                    }
                }
            }
        }
        let upstream = TcpStream::connect(("127.0.0.1", state.upstream)).await?;
        let (up_read, mut up_write) = upstream.into_split();
        up_write.write_all(&request).await?;
        let Some((reply, _)) = read_http(&mut BufReader::new(up_read)).await? else {
            return Ok(());
        };
        down_write.write_all(&reply).await?;
    }
}

/// The block hash of a `submitblock` request body, if it is one.
fn submitted_block(body: &[u8]) -> Option<String> {
    let request: Value = serde_json::from_slice(body).ok()?;
    if request["method"] != "submitblock" {
        return None;
    }
    let block = hex::decode(request["params"][0].as_str()?).ok()?;
    Some(hash_display(&double_sha256(block.get(..80)?)))
}

/// One HTTP/1.1 message framed by `Content-Length`, as qbitd and the
/// servers' client frame every message: the raw bytes and the body.
async fn read_http<R>(reader: &mut BufReader<R>) -> Result<Option<(Vec<u8>, Vec<u8>)>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut raw = Vec::new();
    let mut length = None;
    loop {
        let start = raw.len();
        if reader.read_until(b'\n', &mut raw).await? == 0 {
            ensure!(raw.is_empty(), "connection closed inside an HTTP header");
            return Ok(None);
        }
        let line = std::str::from_utf8(&raw[start..])?.trim_end();
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                length = Some(value.trim().parse::<usize>()?);
            }
        }
    }
    let mut body = vec![0; length.context("HTTP message without Content-Length")?];
    reader.read_exact(&mut body).await?;
    raw.extend_from_slice(&body);
    Ok(Some((raw, body)))
}
