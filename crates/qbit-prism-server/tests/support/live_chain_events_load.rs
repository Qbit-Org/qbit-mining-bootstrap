//! #553 (#487 S15): chain events on the real node under session load, L4.
//!
//! #521's fixtures run here with their assertions unchanged (#487 decision
//! 12): each case opens the fixture as its own test does, starts
//! [`SessionLoad`] against the fixture's frontends, and calls the scenario's
//! own function. The servers run with [`load_server_env`], so the load's
//! shares can miss the regtest block target. The load adds its own checks
//! (see `live_session_load.rs`) and reports #481's time to usable work per
//! frontend. Scenario 2 pauses the load's shares, not its sessions, from its
//! seed blocks until branch B's own blocks are paid, since its balance
//! comparison needs every own share in the window (`deep_reorg`); its reorg
//! runs under the whole load. Scenarios 1, 2 and 4 run at 2,000 sessions,
//! D1's session count, as does scenario 5's soak; scenario 7 at the real-node
//! wallet cap of 2,000 payees (#487 decision 7), whose workers are its load,
//! waits on #621 and #622 and runs in no lane yet.
//!
//! #553 adds three scenarios, with the pass criteria set here:
//! - two frontends on two nodes that briefly disagree about the tip;
//! - the pool's node losing its only peer while miners are connected, which
//!   regtest, exempt from the peer floor, shows as an isolated node;
//! - external tips at qbit's 60 s target spacing, with the time to usable
//!   work per frontend.
//!
//! None of these runs per PR. The nightly set (test/prism-nightly-gated-tests.txt)
//! is scenarios 1 and 2, the crash case of scenario 4, and the three new
//! scenarios with 5 minutes of external tips, all at 100 shares/s, and #598's
//! guard, 2,000 sessions on one frontend at the fixture's 1 s reanchor. The
//! weekly set (test/prism-weekly-gated-tests.txt) is the other scenario-4
//! cases, the soak and 45 minutes of external tips at 400 shares/s,
//! mainnet-floor's peak.
//!
//! For a local run, `PRISM_L4_SESSIONS` and `PRISM_L4_SHARE_RATE` override a
//! case's load, `PRISM_L4_REANCHOR_SECONDS` the frontends' reanchor, and
//! `PRISM_L4_SERVER_RUST_LOG` their log filter; `PRISM_L4_LOG_DIR` keeps the
//! processes' logs there.
use super::dense_soak_tests::{setting, soak};
use super::node_outage_tests::{outage_case_on, Fault, Hold};
use super::session_load::{
    load_server_env, load_settings, under_load, LoadPlan, SessionLoad, DELIVERY_BOUND,
};
use super::two_node_tests::{
    assert_no_duplicate_headers, chain_state, credits, deep_reorg, finish, lost_race, node_a_best,
    node_a_mine, own_block, server_ready, start_frontend, LoadShares, PeerNode, LOAD_SHARES,
    READY_GATE,
};
use super::weighted_recipients_tests::{weighted_case, Scenario};
use super::*;
use rand::{rngs::StdRng, Rng, SeedableRng};

/// D1's session count, which L4 runs every scenario at.
const SESSIONS: usize = 2_000;
/// The nightly set's offered rate: 2,000 sessions each sharing every 20 s.
const NIGHTLY_RATE: f64 = 100.0;
/// The weekly set's: mainnet-floor's peak (#521's shapes; mainnet's worst
/// five-minute rate was 394 shares/s).
const WEEKLY_RATE: f64 = 400.0;
/// The #598 guard's offered rate, #598's reproducer. Before the fix, at 5
/// shares/s the 2nd and 3rd tips reached almost no session in every debug
/// run; at 100 some runs left only ten sessions without work, and one none.
const STARVATION_RATE: f64 = 5.0;
/// qbit's target spacing.
const TARGET_SPACING_SECONDS: f64 = 60.0;
/// How long a frontend may take to report itself ready again after a
/// disagreement. The servers poll their node every 0.2 s
/// (`PRISM_BLOCKPOLL_SECONDS`).
const READINESS_BOUND: u64 = 30;
/// How long a disagreement or a partition is held: past the delivery bound,
/// so a tip on either side is one the load's check holds to it.
const HOLD: Duration = Duration::from_secs(20);

/// #521 scenario 7 at the real-node wallet cap: 2,000 payees through six
/// blocks. The shape is the Zipf tail without a whale: a whale near 85%, as
/// mainnet-floor has, leaves all but about 160 of 2,000 below the payout
/// floor (measured), which the scenario's own check refuses as degenerate;
/// without one about 600 are paid, and carried dust brings more across.
const WALLET_CAP: Scenario = Scenario {
    name: "wallet-cap-2000",
    seed: 0x0553_2000,
    payees: 2_000,
    blocks: 6,
    whales: &[],
    near_zero: 40,
    ctv: true,
    wallets: 8,
};

/// Open the fixture without servers, with the load's server settings.
async fn open(ctv: bool, sessions: usize) -> Result<Fixture> {
    gate::required_inputs(
        gate::site!(),
        &[gate::Input::QbitdBin, gate::Input::DatabaseUrl],
    )?;
    let Some(mut fixture) = Fixture::open_with_servers(ctv, false).await? else {
        bail!("the live fixture's inputs were required above");
    };
    fixture.server_env = load_server_env(sessions);
    if let Ok(seconds) = std::env::var("PRISM_L4_REANCHOR_SECONDS") {
        fixture
            .server_env
            .push(("PRISM_PAYOUT_ARTIFACT_REANCHOR_SECONDS".into(), seconds));
    }
    if let Ok(filter) = std::env::var("PRISM_L4_SERVER_RUST_LOG") {
        fixture.server_env.push(("RUST_LOG".into(), filter));
    }
    Ok(fixture)
}

/// Copy the fixture's process logs to `PRISM_L4_LOG_DIR`, when it is set,
/// before cleanup removes them: a local diagnosis aid.
fn keep_logs(fixture: &Fixture) {
    let Ok(target) = std::env::var("PRISM_L4_LOG_DIR") else {
        return;
    };
    let target = std::path::Path::new(&target).join(fixture.schema.as_str());
    if std::fs::create_dir_all(&target).is_err() {
        return;
    }
    if let Ok(entries) = std::fs::read_dir(fixture.directory.path()) {
        for entry in entries.flatten() {
            if entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "log")
            {
                let _ = std::fs::copy(entry.path(), target.join(entry.file_name()));
            }
        }
    }
    eprintln!("live-load: logs kept in {}", target.display());
}

/// The load's own payout identity: a wallet address no scenario pays or
/// reads, so no scenario's balances include it.
async fn load_address(fixture: &Fixture) -> Result<String> {
    Ok(fixture
        .rpc("getnewaddress", json!(["load", "p2mr"]))
        .await?
        .as_str()
        .context("load address missing")?
        .to_owned())
}

/// While alive, #521's `server_ready` waits after each server turns ready
/// until 99% of the load's sessions hold work: scenarios 1 and 2 start their
/// own servers, and their first events follow within seconds.
struct ReadyGateGuard;

impl ReadyGateGuard {
    fn install(load: &SessionLoad) -> Result<Self> {
        *READY_GATE
            .lock()
            .map_err(|_| anyhow::anyhow!("ready gate poisoned"))? = Some(load.holding(0.99));
        Ok(Self)
    }
}

impl Drop for ReadyGateGuard {
    fn drop(&mut self) {
        if let Ok(mut gate) = READY_GATE.lock() {
            *gate = None;
        }
    }
}

/// While alive, #521 scenario 2 can pause the load's shares around its own
/// blocks (see `deep_reorg`): a load share weighs about half an own block on
/// regtest.
struct LoadSharesGuard;

impl LoadSharesGuard {
    fn install(load: &SessionLoad) -> Result<Self> {
        *LOAD_SHARES
            .lock()
            .map_err(|_| anyhow::anyhow!("load shares poisoned"))? = Some(LoadShares {
            pause: load.share_pause(),
            answered: load.answered(),
        });
        Ok(Self)
    }
}

impl Drop for LoadSharesGuard {
    fn drop(&mut self) {
        if let Ok(mut shares) = LOAD_SHARES.lock() {
            *shares = None;
        }
    }
}

/// After a #521 scenario, one more tip on the fixture's node and the
/// delivery bound, so the load's check holds every frontend still running to
/// at least one tip: a scenario often ends seconds after its last one.
async fn closing_tip(fixture: &Fixture) -> Result<()> {
    let address = load_address(fixture).await?;
    fixture
        .rpc("generatetoaddress", json!([1, address]))
        .await?;
    tokio::time::sleep(DELIVERY_BOUND + Duration::from_secs(1)).await;
    Ok(())
}

/// The load report's delivery of `tip` on `frontend`.
fn tip_delivery<'a>(report: &'a Value, frontend: usize, tip: &str) -> Result<&'a Value> {
    report["tips"]
        .as_array()
        .context("no tips in the report")?
        .iter()
        .find(|delivery| delivery["frontend"] == frontend && delivery["tip"] == tip)
        .with_context(|| format!("no delivery of {tip} on frontend {frontend}: {report}"))
}

/// Every frontend in `frontends` had at least one tip held to the bound:
/// gated, and lasting the bound, so no session could count as replaced.
fn held_to_the_bound(report: &Value, frontends: &[usize]) -> Result<()> {
    for frontend in frontends {
        ensure!(
            report["time_to_usable_work"][frontend]["held_tips"].as_u64() >= Some(1),
            "the load's check held no tip of frontend {frontend} to the bound: {report}"
        );
    }
    Ok(())
}

/// A load on the two-node fixture: node A (the fixture's) is watched node 0
/// and node B watched node 1.
async fn two_node_plan(
    fixture: &Fixture,
    peer: &PeerNode,
    name: &str,
    sessions: usize,
    rate: f64,
    node_of: [Option<usize>; 2],
) -> Result<LoadPlan> {
    let mut plan =
        LoadPlan::on_fixture(fixture, name, sessions, rate, load_address(fixture).await?);
    plan.nodes = vec![fixture.rpc_port, peer.rpc_port];
    plan.node_of = node_of.to_vec();
    Ok(plan)
}

/// Run `case` against a fixture with a second node, then report both nodes
/// on failure and clean up.
async fn two_node_case<F>(ctv: bool, sessions: usize, case: F) -> Result<()>
where
    F: AsyncFnOnce(&mut Fixture, &PeerNode) -> Result<()>,
{
    let mut fixture = open(ctv, sessions).await?;
    let peer = PeerNode::start(&fixture).await;
    let result = match &peer {
        Ok(peer) => case(&mut fixture, peer).await,
        Err(error) => Err(anyhow::anyhow!("{error:#}")),
    };
    keep_logs(&fixture);
    finish(fixture, peer, result).await
}

/// Run `case` against a one-node fixture and clean up.
async fn one_node_case<F>(ctv: bool, sessions: usize, case: F) -> Result<()>
where
    F: AsyncFnOnce(&mut Fixture) -> Result<()>,
{
    let mut fixture = open(ctv, sessions).await?;
    let result = case(&mut fixture).await;
    keep_logs(&fixture);
    if result.is_err() {
        eprintln!("{}", fixture.diagnostics());
    }
    let cleanup = fixture.cleanup().await;
    result.and(cleanup)
}

// ---------------------------------------------------------------------------
// #521's scenarios, unchanged, under load.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "#553 L4 nightly: #521 scenario 1 under 2,000 sessions"]
async fn nightly_lost_race_under_2000_sessions_orphans_once_and_serves_every_session() -> Result<()>
{
    let (sessions, rate) = load_settings(SESSIONS, NIGHTLY_RATE)?;
    two_node_case(false, sessions, async |fixture, peer| {
        let plan = two_node_plan(
            fixture,
            peer,
            "scenario-1-lost-race",
            sessions,
            rate,
            [Some(0), Some(0)],
        )
        .await?;
        let report = under_load(fixture, plan, async |fixture, load| {
            let _gate = ReadyGateGuard::install(load)?;
            lost_race(fixture, peer).await?;
            closing_tip(fixture).await
        })
        .await?;
        held_to_the_bound(&report, &[0])
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "#553 L4 nightly: #521 scenario 2 under 2,000 sessions"]
async fn nightly_deep_reorg_under_2000_sessions_keeps_balances_exact_and_serves_every_session(
) -> Result<()> {
    let (sessions, rate) = load_settings(SESSIONS, NIGHTLY_RATE)?;
    two_node_case(true, sessions, async |fixture, peer| {
        // Frontend 1 is started against node B by the scenario.
        let plan = two_node_plan(
            fixture,
            peer,
            "scenario-2-deep-reorg",
            sessions,
            rate,
            [Some(0), Some(1)],
        )
        .await?;
        let report = under_load(fixture, plan, async |fixture, load| {
            let _gate = ReadyGateGuard::install(load)?;
            let _shares = LoadSharesGuard::install(load)?;
            deep_reorg(fixture, peer).await?;
            closing_tip(fixture).await
        })
        .await?;
        // The scenario stops frontend 0 midway; its sessions fail over to
        // frontend 1 and stay there, so frontend 0 serves none at the end.
        held_to_the_bound(&report, &[1])
    })
    .await
}

async fn outage_under_load(name: &str, fault: Fault, hold: Hold, rate: f64) -> Result<()> {
    let (sessions, rate) = load_settings(SESSIONS, rate)?;
    one_node_case(false, sessions, async |fixture| {
        let plan =
            LoadPlan::on_fixture(fixture, name, sessions, rate, load_address(fixture).await?);
        let report = under_load(fixture, plan, async |fixture, load| {
            outage_case_on(
                fixture,
                fault,
                hold,
                false,
                // The case starts the servers; the fault waits for the load.
                async |_| load.until_connected(0.99, 120).await,
                async |fixture| closing_tip(fixture).await,
            )
            .await
        })
        .await?;
        held_to_the_bound(&report, &[0, 1])
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "#553 L4 nightly: #521 scenario 4's crash after acceptance under 2,000 sessions"]
async fn nightly_node_killed_after_accepting_under_2000_sessions_reconciles_the_lost_block_once(
) -> Result<()> {
    outage_under_load(
        "scenario-4-killed-after-accepting",
        Fault::Kill,
        Hold::Reply,
        NIGHTLY_RATE,
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "#553 L4 weekly: #521 scenario 4 under 2,000 sessions"]
async fn weekly_node_stopped_before_submitblock_under_2000_sessions_lands_the_block_once(
) -> Result<()> {
    outage_under_load(
        "scenario-4-stopped-before-submitblock",
        Fault::Stop,
        Hold::Request,
        WEEKLY_RATE,
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "#553 L4 weekly: #521 scenario 4 under 2,000 sessions"]
async fn weekly_node_stopped_after_accepting_under_2000_sessions_lands_the_block_once() -> Result<()>
{
    outage_under_load(
        "scenario-4-stopped-after-accepting",
        Fault::Stop,
        Hold::Reply,
        WEEKLY_RATE,
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "#553 L4 weekly: #521 scenario 4 under 2,000 sessions"]
async fn weekly_node_killed_before_submitblock_under_2000_sessions_lands_the_block_once(
) -> Result<()> {
    outage_under_load(
        "scenario-4-killed-before-submitblock",
        Fault::Kill,
        Hold::Reservation,
        WEEKLY_RATE,
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "#553 L4 weekly: #521 scenario 4 under 2,000 sessions"]
async fn weekly_node_restarted_into_warmup_under_2000_sessions_lands_the_block_once() -> Result<()>
{
    outage_under_load(
        "scenario-4-restarted-into-warmup",
        Fault::Kill,
        Hold::Warmup,
        WEEKLY_RATE,
    )
    .await
}

/// #521 scenario 5's soak, for `PRISM_DENSE_SOAK_SECONDS` (default 1,200
/// here, the top of #521's 10-20 minutes), under 2,000 sessions. Under the
/// share-only settings a soak miner's share is a block about half the time
/// rather than always; the soak's own check that the cadence stayed dense
/// still holds it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "#553 L4 weekly: #521 scenario 5 under 2,000 sessions"]
async fn weekly_dense_soak_under_2000_sessions_lands_every_block_and_holds_rss_flat() -> Result<()>
{
    let (sessions, rate) = load_settings(SESSIONS, WEEKLY_RATE)?;
    let seconds: u64 = setting("PRISM_DENSE_SOAK_SECONDS", 1_200)?;
    ensure!(
        seconds >= 60,
        "PRISM_DENSE_SOAK_SECONDS must be at least 60, not {seconds}"
    );
    let budget: f64 = setting("PRISM_DENSE_SOAK_REBUILD_P99_SECONDS", 5.0)?;
    ensure!(
        budget.is_finite() && budget > 0.0,
        "PRISM_DENSE_SOAK_REBUILD_P99_SECONDS must be finite and positive"
    );
    one_node_case(false, sessions, async |fixture| {
        for index in 0..2 {
            let process = fixture.start_server(index)?;
            fixture.servers.push(process);
        }
        for index in 0..2 {
            server_ready(fixture, index).await?;
        }
        let plan = LoadPlan::on_fixture(
            fixture,
            "scenario-5-dense-soak",
            sessions,
            rate,
            load_address(fixture).await?,
        );
        let report = under_load(fixture, plan, async |fixture, load| {
            load.until_connected(0.99, 120).await?;
            soak(fixture, seconds, budget).await?;
            // The soak's own blocks come seconds apart, under the bound.
            closing_tip(fixture).await
        })
        .await?;
        held_to_the_bound(&report, &[0, 1])
    })
    .await
}

/// #521 scenario 7 at the real-node wallet cap (#487 decision 7), expected
/// to fail on #621 until it is fixed. Its ~3,000 worker sessions, about
/// 1,500 per frontend, are the load, and the fixture opens them one at a
/// time. Since #604's rebuild lane every session opens, but at the fixture's
/// 1 s reanchor each debug frontend builds far fewer jobs per second than it
/// has sessions, so every session always has a rebuild queued, and a session
/// answers a submit only after its queued rebuild completes, which outlasts
/// the fixture's 30 s wait for a submit's reply (#621). At the production
/// 60 s every economic assertion passes, but the fixture's rounds rely on a
/// fast reanchor to put each round into its block's window, so the
/// scenario's own degeneracy check fails there. The case passes while
/// submits wait behind rebuilds; once #621 is fixed it runs the scenario
/// unchanged and says so, and then the expectation goes. A worker that never
/// gets its first job is #604 again, and fails the case. On a slower host
/// the rounds can instead outlast the fixture's mock-clock horizon (qbitd's
/// mock time is set about 24 minutes ahead; past it the template ages beyond
/// 120 s and a share is refused because the current chain state is
/// unavailable), which also fails the case: that is the fixture running out
/// of time, not a new finding.
///
/// In no lane: that holds in debug, but in release (the weekly job's build)
/// a later round's new tip reaches some worker late, with `tip polling
/// stale` deferrals (#622). Run it by hand until both are fixed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "#553 L4, by hand: #521 scenario 7 at 2,000 wallets, #621's expected failure (#622 in release)"]
async fn wallets_2000_share_to_spend_waits_on_submit_acks_until_621_is_fixed() -> Result<()> {
    gate::required_inputs(
        gate::site!(),
        &[gate::Input::QbitdBin, gate::Input::DatabaseUrl],
    )?;
    match weighted_case(&WALLET_CAP).await {
        Err(error) if format!("{error:#}").contains("got no reply to a submit") => {
            eprintln!("#621 reproduced at 2,000 wallets: {error:#}");
            Ok(())
        }
        Err(error) => {
            Err(error.context("the 2,000-wallet case failed for a reason other than #621"))
        }
        Ok(()) => bail!(
            "#621 looks fixed: #521 scenario 7 passed at 2,000 wallets; drop this expectation and, \
             once #622 is fixed too, list the case weekly"
        ),
    }
}

/// #598's regression guard: 2,000 sessions on one frontend at the live
/// fixture's 1 s reanchor. Before #598's fix each session's job build was
/// superseded by the next publication before it completed, so a new tip
/// reached almost no session; now every one of three external tips, 20 s
/// apart, must reach every session within the delivery bound, with the
/// load's other checks. Nightly, in debug: in release the fan-out outruns
/// the reanchor and even the unfixed server passes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "#553 L4 nightly: #598's guard, 2,000 sessions on one frontend at a 1 s reanchor"]
async fn nightly_2000_sessions_on_one_frontend_at_a_1s_reanchor_get_work_on_every_tip() -> Result<()>
{
    let (sessions, rate) = load_settings(SESSIONS, STARVATION_RATE)?;
    two_node_case(false, sessions, async |fixture, peer| {
        fixture
            .server_env
            .push(("PRISM_PAYOUT_ARTIFACT_REANCHOR_SECONDS".into(), "1".into()));
        peer.heal(fixture).await?;
        fixture.servers.push(fixture.start_server(0)?);
        server_ready(fixture, 0).await?;
        let mut plan = two_node_plan(
            fixture,
            peer,
            "one-frontend-1s-reanchor",
            sessions,
            rate,
            [None, None],
        )
        .await?;
        plan.stratum.truncate(1);
        plan.api.truncate(1);
        plan.node_of = vec![Some(0)];
        let report = under_load(fixture, plan, async |fixture, load| {
            load.until_connected(0.99, 120).await?;
            for _ in 0..3 {
                external_tip(fixture, peer).await?;
                tokio::time::sleep(DELIVERY_BOUND + Duration::from_secs(5)).await;
            }
            Ok(())
        })
        .await?;
        ensure!(
            report["time_to_usable_work"][0]["held_tips"].as_u64() >= Some(3),
            "the load's check held too few of the three tips to the bound: {report}"
        );
        Ok(())
    })
    .await
}

// ---------------------------------------------------------------------------
// #553's scenarios.

async fn health(fixture: &Fixture, index: usize) -> Result<(bool, Value)> {
    let response = fixture
        .client
        .get(format!("http://127.0.0.1:{}/healthz", fixture.api[index]))
        .timeout(Duration::from_secs(5))
        .send()
        .await?;
    let ok = response.status().is_success();
    Ok((ok, response.json().await.unwrap_or(Value::Null)))
}

async fn until_health(
    fixture: &Fixture,
    index: usize,
    ready: bool,
    label: &str,
) -> Result<Duration> {
    let started = Instant::now();
    until(
        &format!("frontend {index} {label}"),
        READINESS_BOUND,
        || async {
            Ok(health(fixture, index)
                .await
                .is_ok_and(|(ok, _)| ok == ready))
        },
    )
    .await?;
    Ok(started.elapsed())
}

/// Mine one block on node B and wait until node A holds it.
async fn external_tip(fixture: &Fixture, peer: &PeerNode) -> Result<String> {
    let tip = peer.mine(1, &fixture.address).await?.remove(0);
    until("node A on node B's tip", 30, || async {
        Ok(node_a_best(fixture).await? == tip)
    })
    .await?;
    Ok(tip)
}

async fn cluster_tip(fixture: &Fixture) -> Result<Option<String>> {
    Ok(
        sqlx::query_scalar("SELECT best_tip_hash FROM qbit_prism_cluster WHERE singleton")
            .fetch_one(&fixture.pool)
            .await?,
    )
}

async fn payout_divergences(fixture: &Fixture) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT count(*) FROM qbit_prism_payout_divergences")
            .fetch_one(&fixture.pool)
            .await?,
    )
}

/// Both frontends on node A, node B its only peer, and B then partitioned
/// away while 2,000 sessions mine. On regtest the peer floor
/// (`PRISM_MIN_PEERS`) does not apply (`readiness.rs`; pinned by
/// `readiness_rpc.rs`'s `regtest_needs_no_peers_but_rejects_explicit_header_lag`,
/// and the floor itself by its public-chain tests), so what a regtest node
/// can show is the rest: the pool keeps mining on its isolated node while
/// the network moves on without it. B mines two blocks it cannot relay; the
/// pool finds an own block on A's stale tip, which becomes A's tip. When the
/// peer comes back, A reorganizes onto B's longer chain. The own block must
/// end inactive and credited exactly once, with no payout divergence,
/// duplicate header or integrity drift. Every session must have work on the
/// isolated own block and on B's chain after the heal within the delivery
/// bound, and the load's own checks hold throughout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "#553 L4 nightly: the pool's node loses its peer under 2,000 sessions"]
async fn nightly_node_loses_its_peer_under_2000_sessions_orphans_the_isolated_block_and_recovers(
) -> Result<()> {
    let (sessions, rate) = load_settings(SESSIONS, NIGHTLY_RATE)?;
    two_node_case(false, sessions, async |fixture, peer| {
        peer.heal(fixture).await?;
        for index in 0..2 {
            let process = fixture.start_server(index)?;
            fixture.servers.push(process);
        }
        for index in 0..2 {
            server_ready(fixture, index).await?;
        }
        let plan =
            two_node_plan(fixture, peer, "node-loses-peer", sessions, rate, [Some(0), Some(0)])
                .await?;
        let mut isolated_tip: Option<String> = None;
        let report = under_load(fixture, plan, async |fixture, load| {
            load.until_connected(0.99, 120).await?;
            let stale = external_tip(fixture, peer).await?;
            tokio::time::sleep(DELIVERY_BOUND + Duration::from_secs(1)).await;

            peer.partition(fixture).await?;
            let unseen = peer.mine(2, &fixture.address).await?;
            let miner = format!("{}.highdiff", fixture.address);
            let isolated = own_block(fixture, 0, &miner, &stale, 1).await?;
            ensure!(
                node_a_best(fixture).await? == isolated,
                "the isolated own block is not node A's tip"
            );
            isolated_tip = Some(isolated.clone());
            tokio::time::sleep(HOLD).await;
            let ready_while_isolated = [
                health(fixture, 0).await?.0,
                health(fixture, 1).await?.0,
            ];

            let healed = peer.heal(fixture).await?;
            ensure!(
                Some(&healed) == unseen.last(),
                "node A did not reorganize onto node B's chain: {healed}"
            );
            until("the isolated own block deactivated", 60, || async {
                Ok(chain_state(fixture, &isolated).await?.as_deref() == Some("inactive"))
            })
            .await?;
            ensure!(
                credits(fixture, &miner, &isolated).await? == 1,
                "the isolated own block is not credited exactly once"
            );
            tokio::time::sleep(DELIVERY_BOUND + Duration::from_secs(1)).await;
            load.until_connected(0.99, 60).await?;
            fixture.integrity().await?;
            ensure!(
                payout_divergences(fixture).await? == 0,
                "the peer loss recorded a payout divergence"
            );
            assert_no_duplicate_headers(fixture).await?;
            eprintln!(
                "node-loses-peer: own block {isolated} on the isolated node, deactivated when node A \
                 reorganized onto {healed}; frontends ready while isolated: {ready_while_isolated:?}"
            );
            Ok(())
        })
        .await?;
        let isolated = isolated_tip.context("the isolated block was not recorded")?;
        for frontend in 0..2 {
            ensure!(
                report["time_to_usable_work"][frontend]["gated_tips"].as_u64() >= Some(2),
                "the load's check held too few of frontend {frontend}'s tips to the bound: {report}"
            );
            // The isolated block is the case: beyond the count above, it
            // must be gated and reach every eligible session in the bound.
            let delivery = tip_delivery(&report, frontend, &isolated)?;
            ensure!(
                delivery["gated"] == true
                    && delivery["eligible"].as_u64() > Some(0)
                    && delivery["served"] == delivery["eligible"]
                    && delivery["all_sessions_s"]
                        .as_f64()
                        .is_some_and(|seconds| seconds <= DELIVERY_BOUND.as_secs_f64()),
                "frontend {frontend} did not give every session work on the isolated block within \
                 {DELIVERY_BOUND:?}: {delivery}"
            );
        }
        Ok(())
    })
    .await
}

/// Frontend 0 on node A and frontend 1 on node B, 1,000 sessions each. Every
/// tip of B's reaches A only after a propagation delay, so the frontends
/// briefly disagree three times; then a partition makes them disagree for
/// [`HOLD`]: A and B each mine a block at the same height, two tips of equal
/// work. The ledger's chain view never rolls back and refuses a conflicting
/// equal-work view (`Ledger::observe_chain_view`), so exactly one of the two
/// tips becomes the cluster's; the frontend whose node holds the other must
/// report itself unready and give its sessions no work on that tip, and no
/// payout divergence or duplicate header may follow. B then outgrows the
/// split and the partition heals: both nodes and both frontends converge on
/// B's branch, the cluster takes its tip, and every session on both
/// frontends gets work on it within the delivery bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "#553 L4 nightly: two frontends on two nodes disagree about the tip under 2,000 sessions"]
async fn nightly_two_frontends_on_two_nodes_disagree_briefly_and_converge_under_2000_sessions(
) -> Result<()> {
    let (sessions, rate) = load_settings(SESSIONS, NIGHTLY_RATE)?;
    two_node_case(false, sessions, async |fixture, peer| {
        peer.heal(fixture).await?;
        fixture.servers.push(fixture.start_server(0)?);
        fixture.servers.push(start_frontend(fixture, 1, peer.rpc_port)?);
        for index in 0..2 {
            server_ready(fixture, index).await?;
        }
        let plan = two_node_plan(
            fixture,
            peer,
            "two-frontends-disagree",
            sessions,
            rate,
            [Some(0), Some(1)],
        )
        .await?;
        // (the split's losing tip, its frontend, the converged tip)
        let mut split: Option<(String, usize, String)> = None;
        let report = under_load(fixture, plan, async |fixture, load| {
            load.until_connected(0.99, 120).await?;
            for _ in 0..3 {
                external_tip(fixture, peer).await?;
                tokio::time::sleep(DELIVERY_BOUND + Duration::from_secs(1)).await;
            }

            peer.partition(fixture).await?;
            // Another payee than node A's, or the two blocks of one second
            // on one parent are the same block.
            let other = load_address(fixture).await?;
            let tip_a = node_a_mine(fixture, 1).await?.remove(0);
            let tip_b = peer.mine(1, &other).await?.remove(0);
            ensure!(tip_a != tip_b, "the split did not fork");
            tokio::time::sleep(HOLD).await;
            // The first observer's tip is the cluster's; the other frontend's
            // node follows a conflicting equal-work tip, which the ledger
            // refuses: that frontend reports itself unready and gives no work
            // on its node's tip, while the first stays ready.
            let cluster = cluster_tip(fixture).await?;
            let (accepted, refused, loser) = if cluster.as_deref() == Some(tip_a.as_str()) {
                (0, tip_b.clone(), 1)
            } else if cluster.as_deref() == Some(tip_b.as_str()) {
                (1, tip_a.clone(), 0)
            } else {
                bail!("the cluster's tip {cluster:?} is neither side of the split ({tip_a}, {tip_b})");
            };
            let during = [health(fixture, 0).await?, health(fixture, 1).await?];
            ensure!(
                during[accepted].0 && !during[loser].0,
                "during the split frontend {accepted} (the cluster's tip) must be ready and frontend \
                 {loser} (the refused tip) unready: {during:?}"
            );

            let winner = peer.mine(1, &other).await?.remove(0);
            let healed = peer.heal(fixture).await?;
            ensure!(
                healed == winner,
                "the nodes converged on {healed}, not node B's longer branch {winner}"
            );
            for index in 0..2 {
                until_health(fixture, index, true, "ready after convergence").await?;
            }
            until("the cluster on the converged tip", 30, || async {
                Ok(cluster_tip(fixture).await?.as_deref() == Some(winner.as_str()))
            })
            .await?;
            tokio::time::sleep(DELIVERY_BOUND + Duration::from_secs(1)).await;
            load.until_connected(0.99, 60).await?;
            fixture.integrity().await?;
            ensure!(
                payout_divergences(fixture).await? == 0,
                "the disagreement recorded a payout divergence"
            );
            assert_no_duplicate_headers(fixture).await?;
            eprintln!(
                "two-frontends-disagree: split {tip_a} (A) / {tip_b} (B), the cluster took \
                 {cluster:?} and frontend {loser} stood down; converged on {winner}"
            );
            split = Some((refused, loser, winner));
            Ok(())
        })
        .await?;
        let (refused, loser, winner) = split.context("the split was not recorded")?;
        let delivery = |frontend: usize, tip: &str| tip_delivery(&report, frontend, tip);
        let refused = delivery(loser, &refused)?;
        ensure!(
            refused["served"] == 0 && refused["eligible"].as_u64() > Some(0),
            "frontend {loser} gave work on the tip the ledger refused: {refused}"
        );
        // A frontend that was unready when the converged tip arrived is not
        // held to the bound by the load's check; hold it here.
        for frontend in 0..2 {
            let converged = delivery(frontend, &winner)?;
            ensure!(
                converged["served"] == converged["eligible"]
                    && converged["all_sessions_s"]
                        .as_f64()
                        .is_some_and(|seconds| seconds <= DELIVERY_BOUND.as_secs_f64()),
                "frontend {frontend} did not give every session work on the converged tip within \
                 {DELIVERY_BOUND:?}: {converged}"
            );
            ensure!(
                report["time_to_usable_work"][frontend]["gated_tips"].as_u64() >= Some(3),
                "the load's check held too few of frontend {frontend}'s tips to the bound: {report}"
            );
        }
        Ok(())
    })
    .await
}

/// External tips from node B at qbit's 60 s target spacing, exponential
/// gaps from a fixed seed, for `seconds`, to 2,000 sessions on two
/// frontends on node A. About one gap in ten is under 6 s, #481's
/// replaced-tip case. Every tip that lasts the delivery bound must reach
/// every session on both frontends within it, and the load's report gives
/// the time to usable work per frontend and per tip (#481's figure; #275
/// owns a tighter objective, so it is reported, not gated).
async fn external_tips(name: &str, seconds: u64, rate: f64) -> Result<()> {
    let (sessions, rate) = load_settings(SESSIONS, rate)?;
    let seconds: u64 = setting("PRISM_L4_TIPS_SECONDS", seconds)?;
    two_node_case(false, sessions, async |fixture, peer| {
        peer.heal(fixture).await?;
        for index in 0..2 {
            let process = fixture.start_server(index)?;
            fixture.servers.push(process);
        }
        for index in 0..2 {
            server_ready(fixture, index).await?;
        }
        let plan = two_node_plan(fixture, peer, name, sessions, rate, [Some(0), Some(0)]).await?;
        let mut minted = 0usize;
        let report = under_load(fixture, plan, async |fixture, load| {
            load.until_connected(0.99, 120).await?;
            let mut rng = StdRng::seed_from_u64(0x0553_0060);
            let started = Instant::now();
            loop {
                let gap = -TARGET_SPACING_SECONDS * (1.0 - rng.gen::<f64>()).ln();
                let at = started.elapsed().as_secs_f64() + gap;
                if at >= seconds as f64 {
                    break;
                }
                tokio::time::sleep(Duration::from_secs_f64(gap)).await;
                external_tip(fixture, peer).await?;
                minted += 1;
            }
            tokio::time::sleep(
                Duration::from_secs(seconds).saturating_sub(started.elapsed())
                    + DELIVERY_BOUND
                    + Duration::from_secs(1),
            )
            .await;
            fixture.integrity().await
        })
        .await?;
        ensure!(minted > 0, "no external tip in {seconds} s");
        eprintln!("{name}: {minted} external tips in {seconds} s");
        held_to_the_bound(&report, &[0, 1])
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "#553 L4 nightly: 5 minutes of external tips under 2,000 sessions"]
async fn nightly_external_tips_at_target_spacing_reach_every_session_on_both_frontends(
) -> Result<()> {
    external_tips("external-tips-5m", 300, NIGHTLY_RATE).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "#553 L4 weekly: 45 minutes of external tips under 2,000 sessions"]
async fn weekly_external_tips_at_target_spacing_reach_every_session_on_both_frontends() -> Result<()>
{
    external_tips("external-tips-45m", 2_700, WEEKLY_RATE).await
}
