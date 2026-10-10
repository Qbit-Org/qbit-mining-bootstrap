//! The scenario matrix of CONTRACT.md §5, and the checker's own control.
//!
//! Every scenario follows one shape: start the pair, offer load through the
//! balancer, find own blocks on chosen nodes, inject its fault, heal it,
//! settle, then run the invariant checker and its own expectations. The
//! report (`report.json`, `report.md`) and every process's log go to the
//! scenario's directory whether it passes or fails.
//!
//! **Bounds.** Every wait has a bound, stated where it is made; none is a
//! retry of the behaviour under test. A wait for something the scenario
//! caused (a block to land, a frontend to restart) is a setup budget, and
//! the measured time is reported. A bound that is the behaviour under test
//! (the miner-visible gap) is an expectation, with its measured value.

use crate::{
    balancer::Routing,
    faults::Fault,
    frontend::Node,
    invariants::{self, CheckOptions, InvariantReport, Status},
    load::ShareRecord,
    relay::LinkState,
    report::{self, Expectation, FoundBlock, GapReport, ScenarioReport},
    sim::{Inputs, Sim, SimConfig, Topology},
};
use anyhow::{Context, Result};
use serde_json::json;
use std::time::{Duration, Instant};

/// A block found on purpose must land and confirm within this: its share is
/// answered after commit, then the frontend offers it to its node, lands
/// its audit and confirms it once its node's tip shows it (0.2 s polls).
pub const LANDING_BOUND: Duration = Duration::from_secs(60);
/// How long the balancer may take to mark a dead node down: `fall` (3)
/// failed checks 2 s apart, each failing at once (refused) or after its 1 s
/// timeout (blackholed or frozen): 4 to 7 s.
pub const MARK_DOWN_BOUND: Duration = Duration::from_secs(12);
/// How long routing may take to settle on the preferred node before a
/// fault: both nodes up, and every session back on A after a failback.
pub const ROUTING_SETTLE_BOUND: Duration = Duration::from_secs(30);
/// The miner-visible gap a node's death may cost: detection, the
/// reconnect, the survivor's first job and a first accepted share. The
/// existing CI bar for a failover is a share within 30 s.
pub const FAILOVER_GAP_BOUND_MS: u64 = 30_000;
/// The whole settle: answers, candidates, chain, replication or sync.
pub const SETTLE_BOUND: Duration = Duration::from_secs(180);

/// How node A dies in S2.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Death {
    /// SIGKILL of its frontend; its database stays up.
    Kill9,
    /// SIGKILL of its PostgreSQL; its frontend stays up without a database.
    PostgresKill,
    /// SIGSTOP of its frontend: sockets open, nothing answered.
    Freeze,
    /// Every link to the node blackholed and its qbitd cut off.
    NetworkDrop,
}

impl Death {
    fn fault(self, node: Node) -> Fault {
        match self {
            Death::Kill9 => Fault::FrontendKill9(node),
            Death::PostgresKill => Fault::PostgresKill(node),
            Death::Freeze => Fault::FrontendFreeze(node),
            Death::NetworkDrop => Fault::NetworkDrop(node, LinkState::Blackholed),
        }
    }

    fn id(self) -> &'static str {
        match self {
            Death::Kill9 => "kill9",
            Death::PostgresKill => "postgres-kill",
            Death::Freeze => "freeze",
            Death::NetworkDrop => "network-drop",
        }
    }
}

/// Every scenario the harness runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scenario {
    /// The checker's negative control: two unsynced single writers both
    /// mining must fail invariant 3 and the share-presence check.
    CheckerControl,
    /// S1: A preferred, B idle; A's shares and blocks reach B within the
    /// sync interval and windows match.
    S01SteadyState,
    /// S2: A dies; miners move to B, B finds carry-free blocks, A returns,
    /// ingests them and resumes paydowns.
    S02ADies(Death),
    /// S3: B dies; A is unaffected, and B catches up when it returns.
    S03BDies,
    /// S4: the link between the databases is cut with both nodes alive.
    S04LinkCut,
    /// S5: both nodes take miners and write at once.
    S05BothWrite,
    /// S10: dual mode off, the 3.0 pair, A's frontend killed under load.
    S10SingleWriter,
}

impl Scenario {
    /// The fast lane: what every pull request in the stack runs.
    pub const FAST: [Scenario; 5] = [
        Scenario::S01SteadyState,
        Scenario::S02ADies(Death::Kill9),
        Scenario::S04LinkCut,
        Scenario::S05BothWrite,
        Scenario::S10SingleWriter,
    ];

    pub fn id(self) -> String {
        match self {
            Scenario::CheckerControl => "checker-control".into(),
            Scenario::S01SteadyState => "s01-steady-state".into(),
            Scenario::S02ADies(death) => format!("s02-a-dies-{}", death.id()),
            Scenario::S03BDies => "s03-b-dies".into(),
            Scenario::S04LinkCut => "s04-link-cut".into(),
            Scenario::S05BothWrite => "s05-both-write".into(),
            Scenario::S10SingleWriter => "s10-single-writer".into(),
        }
    }

    pub fn title(self) -> String {
        match self {
            Scenario::CheckerControl => {
                "Two unsynced single writers fail the checker exactly where they should".into()
            }
            Scenario::S01SteadyState => {
                "Steady state: A's shares and blocks reach B within the sync interval".into()
            }
            Scenario::S02ADies(death) => format!(
                "A dies ({}): miners move to B, B mines carry-free, A returns and pays down",
                death.id()
            ),
            Scenario::S03BDies => "B dies: A is unaffected and B catches up on return".into(),
            Scenario::S04LinkCut => {
                "Link cut with both alive: no mining impact, sync catches up on heal".into()
            }
            Scenario::S05BothWrite => {
                "Both nodes write at once: invariants hold and B's blocks stay carry-free".into()
            }
            Scenario::S10SingleWriter => {
                "Single-writer regression: the 3.0 pair keeps every share and pays exactly through a frontend kill".into()
            }
        }
    }

    fn topology(self) -> Topology {
        match self {
            Scenario::CheckerControl => Topology::Unsynced,
            Scenario::S10SingleWriter => Topology::SingleWriter,
            _ => Topology::DualWriter,
        }
    }

    fn config(self) -> Result<SimConfig> {
        let mut config = SimConfig::new(&self.id(), self.topology())?;
        if matches!(self, Scenario::CheckerControl | Scenario::S05BothWrite) {
            config.balancer.routing = Routing::RoundRobin;
        }
        Ok(config)
    }
}

/// What a scenario's body hands the driver.
#[derive(Default)]
struct Body {
    expectations: Vec<Expectation>,
    gaps: Vec<GapReport>,
    blocks: Vec<FoundBlock>,
    options: CheckOptions,
    /// Checks this scenario expects to fail (the control); every other
    /// check must pass.
    expected_failures: Vec<&'static str>,
}

impl Body {
    fn expect(&mut self, name: &str, passed: bool, detail: String) {
        self.expectations.push(Expectation {
            name: name.to_owned(),
            passed,
            detail,
        });
    }
}

/// Run `scenario` end to end and write its report. The report's `passed`
/// is the verdict; an error is a scenario that could not run to its checks
/// (also reported).
pub async fn run(scenario: Scenario, inputs: Inputs) -> Result<ScenarioReport> {
    let started = Instant::now();
    let config = scenario.config()?;
    let report_dir = inputs.report_root.join(scenario.id());
    let parameters = serde_json::to_value(&config)?;
    let mut sim = match Sim::start(config, inputs).await {
        Ok(sim) => sim,
        Err(error) => {
            let report = failed_report(scenario, started, parameters, &error, None);
            report.write(&report_dir)?;
            return Err(error.context(format!("{} could not start", scenario.id())));
        }
    };
    let outcome = async {
        let mut body = match scenario {
            Scenario::CheckerControl => checker_control(&mut sim).await?,
            Scenario::S01SteadyState => s01_steady_state(&mut sim).await?,
            Scenario::S02ADies(death) => s02_a_dies(&mut sim, death).await?,
            Scenario::S03BDies => s03_b_dies(&mut sim).await?,
            Scenario::S04LinkCut => s04_link_cut(&mut sim).await?,
            Scenario::S05BothWrite => s05_both_write(&mut sim).await?,
            Scenario::S10SingleWriter => s10_single_writer(&mut sim).await?,
        };
        let records = sim.load()?.records();
        let invariants = invariants::check(&sim, &records, &body.options).await?;
        invariants::write(&invariants, &sim.report_dir)?;
        judge_invariants(&mut body, &invariants);
        anyhow::Ok((body, records, invariants))
    }
    .await;
    let report = match outcome {
        Ok((body, records, invariants)) => {
            let mut shares = report::summarize(&records);
            if let Some(check) = invariants.get("inv4-acked-shares-present") {
                if check.status == Status::Fail && body.expected_failures.is_empty() {
                    shares.lost = check.problem_count;
                }
                shares.excused_tail =
                    check.data["excused_missing"]
                        .as_object()
                        .map_or(0, |reasons| {
                            reasons
                                .values()
                                .filter_map(|count| count.as_u64())
                                .sum::<u64>() as usize
                        });
            }
            let passed = body.expectations.iter().all(|e| e.passed);
            ScenarioReport {
                scenario: scenario.id(),
                title: scenario.title(),
                passed,
                duration_ms: started.elapsed().as_millis() as u64,
                parameters,
                timeline: sim.timeline(),
                gaps: body.gaps,
                shares,
                blocks: body.blocks,
                expectations: body.expectations,
                invariants: Some(invariants),
                balancer: Some(sim.balancer.report()),
                links: sim.links.stats(),
                error: None,
            }
        }
        Err(error) => failed_report(scenario, started, parameters, &error, Some(&sim)),
    };
    let records = sim
        .load
        .as_ref()
        .map(|load| load.records())
        .unwrap_or_default();
    write_share_log(&sim.report_dir, &records)?;
    report.write(&sim.report_dir)?;
    let keep = !report.passed;
    sim.shutdown(keep).await?;
    Ok(report)
}

fn failed_report(
    scenario: Scenario,
    started: Instant,
    parameters: serde_json::Value,
    error: &anyhow::Error,
    sim: Option<&Sim>,
) -> ScenarioReport {
    ScenarioReport {
        scenario: scenario.id(),
        title: scenario.title(),
        passed: false,
        duration_ms: started.elapsed().as_millis() as u64,
        parameters,
        timeline: sim.map(Sim::timeline).unwrap_or_default(),
        gaps: Vec::new(),
        shares: sim
            .and_then(|sim| sim.load.as_ref())
            .map(|load| report::summarize(&load.records()))
            .unwrap_or_default(),
        blocks: Vec::new(),
        expectations: Vec::new(),
        invariants: None,
        balancer: sim.map(|sim| sim.balancer.report()),
        links: sim.map(|sim| sim.links.stats()).unwrap_or_default(),
        error: Some(format!("{error:#}")),
    }
}

/// Hold the checker's verdicts to what the scenario expects: every check
/// passes, except the ones it names, which must fail.
fn judge_invariants(body: &mut Body, report: &InvariantReport) {
    for check in &report.checks {
        let expected_fail = body.expected_failures.contains(&check.id.as_str());
        let passed = match check.status {
            Status::Pass | Status::Skip => !expected_fail,
            Status::Fail => expected_fail,
        };
        let detail = if expected_fail {
            format!(
                "expected to fail: {:?} with {} problems; {}",
                check.status, check.problem_count, check.summary
            )
        } else {
            format!("{:?}: {}", check.status, check.summary)
        };
        body.expect(&format!("checker: {}", check.id), passed, detail);
    }
}

fn write_share_log(dir: &std::path::Path, records: &[ShareRecord]) -> Result<()> {
    use std::io::Write;
    std::fs::create_dir_all(dir)?;
    let mut file = std::io::BufWriter::new(std::fs::File::create(dir.join("shares.jsonl"))?);
    for record in records {
        serde_json::to_writer(&mut file, record)?;
        file.write_all(b"\n")?;
    }
    file.flush()?;
    Ok(())
}

// --- shared steps ------------------------------------------------------------

/// Let the load run for `seconds`.
async fn steady(sim: &Sim, seconds: u64) {
    sim.mark(&format!("steady load for {seconds} s"));
    tokio::time::sleep(Duration::from_secs(seconds)).await;
}

/// How many times a scheduled block is solved again when the work it was
/// solved on turned out superseded. A tip or payout-revision change between
/// the finder's job and its submit makes the pool refuse the share
/// (`stale-job`) or accept it under the stale grace without a candidate;
/// neither is the behaviour under test, and each retry starts from settled
/// work, so three attempts are plenty. Every retry is on the timeline.
pub const FIND_ATTEMPTS: usize = 3;
/// How long a node may take to settle on its tip and payout revision before
/// a block is solved on its work.
pub const NODE_SETTLE_BOUND: Duration = Duration::from_secs(60);
/// How soon an accepted block-solving share becomes a candidate.
pub const CANDIDATE_BOUND: Duration = Duration::from_secs(15);

/// Find an own block on `node` and wait until it is confirmed in the
/// databases `confirm_in` (default: wherever the node writes). The finder
/// solves on settled, freshly delivered work; see [`FIND_ATTEMPTS`].
async fn find_block(
    sim: &Sim,
    body: &mut Body,
    node: Node,
    note: &str,
    confirm_in: Option<&[Node]>,
) -> Result<String> {
    let mut attempts = Vec::new();
    for attempt in 1..=FIND_ATTEMPTS {
        sim.wait_node_settled(node, NODE_SETTLE_BOUND).await?;
        let load = sim.load()?;
        load.refresh_finder(node, Duration::from_secs(30)).await?;
        let record = load.find_block(node, LANDING_BOUND).await?;
        let hash = record.header_hash().to_owned();
        if !record.accepted() {
            let reason = record.reason.clone().unwrap_or_default();
            attempts.push(format!("attempt {attempt}: refused ({reason})"));
            if matches!(reason.as_str(), "stale-job" | "unknown-job") {
                sim.mark(&format!(
                    "block on node {node:?} refused as {reason}; solving again"
                ));
                continue;
            }
            anyhow::bail!(
                "node {node:?} refused its own block ({}: {reason})",
                record.outcome
            );
        }
        if !sim.wait_candidate(node, &hash, CANDIDATE_BOUND).await? {
            attempts.push(format!(
                "attempt {attempt}: {hash} accepted but never a candidate"
            ));
            sim.mark(&format!(
                "block {hash} on node {node:?} was solved on superseded work; solving again"
            ));
            continue;
        }
        let found_at_ms = record.answered_ms.unwrap_or_else(|| sim.clock.now_ms());
        sim.mark(&format!("block {hash} found on node {node:?}: {note}"));
        let ledger = [sim.ledger_node(node)];
        let took = sim
            .wait_confirmed(&hash, confirm_in.unwrap_or(&ledger), LANDING_BOUND)
            .await
            .with_context(|| format!("block {hash} found on node {node:?} did not confirm"))?;
        sim.mark(&format!(
            "block {hash} confirmed after {:.1} s",
            took.as_secs_f64()
        ));
        body.blocks.push(FoundBlock {
            node,
            hash: hash.clone(),
            found_at_ms,
            note: note.to_owned(),
        });
        return Ok(hash);
    }
    anyhow::bail!(
        "node {node:?} found no landable block in {FIND_ATTEMPTS} attempts: {}",
        attempts.join("; ")
    )
}

/// Wait until the balancer has both nodes up and, with preferred routing,
/// no session left on B: the state a fault is injected into, so what the
/// fault costs is not mixed with an earlier failover or failback.
async fn routing_settled(sim: &Sim) -> Result<()> {
    let started = Instant::now();
    loop {
        let sessions = sim.balancer.sessions();
        let preferred = sim.config.balancer.routing == Routing::Preferred;
        if sim.balancer.is_up("a")
            && sim.balancer.is_up("b")
            && (!preferred || sessions.get("b").copied().unwrap_or(0) == 0)
        {
            return Ok(());
        }
        anyhow::ensure!(
            started.elapsed() < ROUTING_SETTLE_BOUND,
            "routing did not settle on A within {ROUTING_SETTLE_BOUND:?}: A up {}, B up {}, sessions {sessions:?}",
            sim.balancer.is_up("a"),
            sim.balancer.is_up("b")
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Accepted shares on jobs issued by `node`, answered after `after_ms`.
fn accepted_from(records: &[ShareRecord], node: Node, after_ms: u64) -> usize {
    records
        .iter()
        .filter(|r| {
            r.accepted()
                && !r.scheduled_block
                && r.issuer == Some(node)
                && r.answered_ms.is_some_and(|at| at > after_ms)
        })
        .count()
}

/// The `dual_writer` object of a node's `/healthz`, if it reports one.
async fn dual_writer_health(sim: &Sim, node: Node) -> Result<Option<serde_json::Value>> {
    let (_, body) = sim.frontend(node).health().await?;
    Ok(body.get("dual_writer").cloned())
}

// --- the checker's negative control ------------------------------------------

/// Two single-writer nodes with independent databases and nothing between
/// them, both taking miners (round-robin routing), each finding a block.
/// Each database lacks the other's block and shares, so the checker must
/// fail invariant 3's landing rows and fanouts, the share-presence check,
/// window reproducibility and the owner's balance view, and pass the rest.
/// A checker that passed this would prove nothing elsewhere.
async fn checker_control(sim: &mut Sim) -> Result<Body> {
    let mut body = Body::default();
    sim.load()?.resume();
    steady(sim, 8).await;
    let a = find_block(sim, &mut body, Node::A, "on A's unsynced database", None).await?;
    let b = find_block(sim, &mut body, Node::B, "on B's unsynced database", None).await?;
    steady(sim, 3).await;
    sim.settle(SETTLE_BOUND).await?;
    let records = sim.load()?.records();
    for node in Node::BOTH {
        body.expect(
            &format!("node {node:?} took miners"),
            accepted_from(&records, node, 0) > 0,
            format!(
                "{} accepted shares on its jobs",
                accepted_from(&records, node, 0)
            ),
        );
    }
    body.expected_failures = vec![
        "inv3-landing-rows",
        "inv3-fanouts-identical",
        "inv4-acked-shares-present",
        "inv4-windows-reproducible",
    ];
    // A's balance view lacks B's block, which only shows when B's block left
    // some account a carry (a delta); with every account paid in full both
    // views are empty and agree.
    let b_deltas: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM qbit_payout_carry_forward WHERE gross_amount_sats <> onchain_amount_sats",
    )
    .fetch_one(&sim.pool(Node::B).await?)
    .await?;
    if b_deltas > 0 {
        body.expected_failures.push("owner-balances-match-chain");
    }
    sim.mark(&format!(
        "B's block left {b_deltas} accounts with a carry delta"
    ));
    // The failures must be the right ones: each block missing on the other
    // node. The driver adds the per-check verdicts after the checker runs.
    body.options.excused_missing.clear();
    sim.mark(&format!("control blocks: A {a}, B {b}"));
    Ok(body)
}

// --- dual-writer scenarios -----------------------------------------------------

/// How long a share committed on one node may take to appear on the other
/// in steady state: the sync interval (250 ms), one pull and its apply, and
/// the sampler's 100 ms resolution, with room for a loaded runner.
pub const SYNC_LAG_BOUND_MS: u64 = 2_000;
/// How long the databases may take to agree after a cut heals or a node
/// returns: every row the cut held back, pulled in batches of 5,000.
pub const CATCH_UP_BOUND: Duration = Duration::from_secs(60);
/// The longest stretch with no accepted share that ordinary load shows
/// (20 shares/s across the sessions), used where a fault must cost miners
/// nothing.
pub const NO_IMPACT_GAP_MS: u64 = 2_000;

/// Confirmed in both databases.
async fn confirm_on_both(sim: &Sim, hash: &str) -> Result<Duration> {
    sim.wait_confirmed(hash, &Node::BOTH, LANDING_BOUND).await
}

/// The `dual_writer` health object of both nodes, as an expectation: each
/// reports dual mode with its own index and role, its own log caught up and
/// the local writer path (CONTRACT.md §3, "Health").
async fn expect_dual_health(sim: &Sim, body: &mut Body) -> Result<()> {
    for node in Node::BOTH {
        let dual = dual_writer_health(sim, node).await?;
        let ok = dual.as_ref().is_some_and(|dual| {
            dual["node_index"] == json!(node.index())
                && dual["carry_owner"] == json!(node == Node::A)
                && dual["own_log_caught_up"] == json!(true)
                && dual["writer_path"] == json!("local")
        });
        body.expect(
            &format!("node {node:?} reports its dual-writer identity"),
            ok,
            format!("/healthz dual_writer: {dual:?}"),
        );
    }
    Ok(())
}

/// Per program, the sum of `gross - onchain` over every confirmed pool
/// block below `height` in the owner's database: R1's prior at a block of
/// that height, which the owner's work must have used.
async fn truth_below(sim: &Sim, height: i64) -> Result<std::collections::BTreeMap<String, i128>> {
    let pool = sim.pool(Node::A).await?;
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT encode(c.p2mr_program, 'hex'), \
                sum(c.gross_amount_sats::numeric - c.onchain_amount_sats::numeric)::text \
         FROM qbit_payout_carry_forward c JOIN qbit_pool_blocks b ON b.block_hash = c.block_hash \
         WHERE b.chain_state = 'confirmed' AND b.block_height < $1 AND c.maturity_state <> 'reversed' \
         GROUP BY c.p2mr_program",
    )
    .bind(height)
    .fetch_all(&pool)
    .await?;
    pool.close().await;
    let mut truth = std::collections::BTreeMap::new();
    for (program, sum) in rows {
        let sum: i128 = sum.parse()?;
        if sum != 0 {
            truth.insert(program, sum);
        }
    }
    Ok(truth)
}

/// The owner's block `hash` paid from exactly the chain's balances below
/// it, and some of them were carry: its priors equal R1's sum, and at least
/// one is positive, so the paydown is real.
async fn expect_owner_paid_down(sim: &Sim, body: &mut Body, hash: &str) -> Result<()> {
    let pool = sim.pool(Node::A).await?;
    let (height, priors): (i64, serde_json::Value) = sqlx::query_as(
        "SELECT b.block_height, a.audit_bundle->'prior_balances' FROM qbit_pool_blocks b \
         JOIN qbit_pool_audit_bundles a ON a.block_hash = b.block_hash WHERE b.block_hash = $1",
    )
    .bind(hash)
    .fetch_one(&pool)
    .await?;
    pool.close().await;
    let mut issued = std::collections::BTreeMap::new();
    for prior in priors.as_array().cloned().unwrap_or_default() {
        let program = prior["p2mr_program_hex"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let sats: i128 = prior["balance_sats"]
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| prior["balance_sats"].to_string())
            .parse()
            .unwrap_or(0);
        if sats != 0 {
            *issued.entry(program).or_insert(0) += sats;
        }
    }
    let truth = truth_below(sim, height).await?;
    body.expect(
        "the owner paid from the chain's balances, the peer's carry included",
        issued == truth,
        format!("block {hash} at {height}: issued priors {issued:?}, chain sums {truth:?}"),
    );
    body.expect(
        "the owner's block paid down carry",
        issued.values().any(|sats| *sats > 0),
        format!(
            "positive priors in {hash}: {:?}",
            issued.values().filter(|s| **s > 0).count()
        ),
    );
    Ok(())
}

/// S1. A is preferred and B idle: every miner is on A. A's shares and
/// blocks reach B within the sync interval, B confirms A's blocks from its
/// own chain view, and every window is the same on both nodes.
async fn s01_steady_state(sim: &mut Sim) -> Result<Body> {
    let mut body = Body::default();
    sim.load()?.resume();
    steady(sim, 4).await;
    let sampler = crate::measure::LagSampler::start(
        sim.pool(Node::A).await?,
        sim.pool(Node::B).await?,
        sim.clock,
    );
    for note in ["A's first block", "A's second block"] {
        let hash = find_block(sim, &mut body, Node::A, note, None).await?;
        let took = confirm_on_both(sim, &hash).await?;
        sim.mark(&format!(
            "{hash} confirmed on both nodes after {:.1} s",
            took.as_secs_f64()
        ));
        steady(sim, 4).await;
    }
    let (lag, _) = sampler.stop().await?;
    body.expect(
        "A's shares reach B within the sync interval",
        lag.resolved > 0 && lag.p95_ms.is_some_and(|ms| ms <= SYNC_LAG_BOUND_MS),
        format!(
            "{} of {} samples seen on B; p50 {:?} ms, p95 {:?} ms, max {:?} ms (interval {} ms, bound {SYNC_LAG_BOUND_MS} ms, resolution {} ms)",
            lag.resolved, lag.samples, lag.p50_ms, lag.p95_ms, lag.max_ms,
            sim.config.sync_interval_ms, lag.resolution_ms
        ),
    );
    sim.settle(SETTLE_BOUND).await?;
    let records = sim.load()?.records();
    body.expect(
        "B stays idle while A is up",
        accepted_from(&records, Node::B, 0) == 0,
        format!(
            "{} shares accepted on B's jobs",
            accepted_from(&records, Node::B, 0)
        ),
    );
    expect_dual_health(sim, &mut body).await?;
    Ok(body)
}

/// S2. A dies (`death`) under load. Miners move to B within the failover
/// bound; B mines and finds two blocks, carry-free; the shares A had
/// acknowledged but B had not pulled are measured as A's tail. A returns,
/// ingests B's blocks and confirms them from its own chain view, and its
/// next block pays the carry B's blocks accrued: its priors are the chain's
/// sums. Every acknowledged share ends on both nodes (A's disk survived).
async fn s02_a_dies(sim: &mut Sim, death: Death) -> Result<Body> {
    let mut body = Body::default();
    sim.load()?.resume();
    steady(sim, 6).await;
    let a1 = find_block(sim, &mut body, Node::A, "A before it dies", None).await?;
    confirm_on_both(sim, &a1).await?;
    routing_settled(sim).await?;

    let fault = death.fault(Node::A);
    let fault_at = sim.inject(fault).await?;
    // B's last pull from A's database finishes or fails within two sync
    // intervals; what B holds then is what it had when A went.
    tokio::time::sleep(Duration::from_millis(2 * sim.config.sync_interval_ms + 500)).await;
    let held = crate::measure::credited_headers(&sim.pool(Node::B).await?).await?;
    sim.balancer
        .wait_state("a", false, MARK_DOWN_BOUND)
        .await
        .context("the balancer kept A up")?;
    sim.mark("balancer marked A down");
    steady(sim, 8).await;
    let mut b_blocks = Vec::new();
    for note in ["B while A is down", "B again while A is down"] {
        b_blocks.push(find_block(sim, &mut body, Node::B, note, Some(&[Node::B])).await?);
    }
    let records = sim.load()?.records();
    let tail = crate::measure::tail(&records, Node::A, fault_at, &held);
    sim.mark(&format!(
        "A's unsynced tail when it died: {} shares",
        tail.count
    ));
    body.expect(
        "miners moved to B",
        accepted_from(&records, Node::B, fault_at) > 0,
        format!(
            "{} shares accepted on B's jobs after the fault",
            accepted_from(&records, Node::B, fault_at)
        ),
    );

    sim.heal(fault).await?;
    if !sim.frontend(Node::A).running() {
        // A frontend that lost its database may have exited: start it.
        sim.frontend_mut(Node::A).start()?;
    }
    sim.frontend(Node::A)
        .wait_ready(Duration::from_secs(120))
        .await?;
    sim.balancer
        .wait_state("a", true, Duration::from_secs(60))
        .await?;
    sim.mark("balancer marked A up");
    for hash in &b_blocks {
        let took = sim.wait_confirmed(hash, &[Node::A], CATCH_UP_BOUND).await?;
        sim.mark(&format!(
            "A confirmed B's {hash} after {:.1} s",
            took.as_secs_f64()
        ));
    }
    steady(sim, 6).await;
    let a2 = find_block(sim, &mut body, Node::A, "A after it returns", None).await?;
    confirm_on_both(sim, &a2).await?;
    steady(sim, 2).await;
    sim.settle(SETTLE_BOUND).await?;

    let records = sim.load()?.records();
    let gap = report::gap(&records, fault_at);
    body.expect(
        "miner-visible gap within the failover bound",
        gap.fault_to_first_accept_ms.is_some_and(|ms| ms <= FAILOVER_GAP_BOUND_MS),
        format!(
            "first accepted share {:?} ms after the fault (bound {FAILOVER_GAP_BOUND_MS} ms); gap {:?} ms",
            gap.fault_to_first_accept_ms, gap.gap_ms
        ),
    );
    body.gaps.push(gap);
    body.expect(
        "A's tail is measured, and returns with A",
        true,
        format!(
            "{} shares acknowledged by A were not on B when A went (answered {:?}..{:?} ms); A's disk survived, so none may be missing at the end",
            tail.count, tail.first_answered_ms, tail.last_answered_ms
        ),
    );
    expect_owner_paid_down(sim, &mut body, &a2).await?;
    expect_dual_health(sim, &mut body).await?;
    Ok(body)
}

/// S3. B dies (its frontend and its database at once, a host crash) while
/// every miner is on A. A's miners see nothing; A keeps mining and finds a
/// block. B returns and catches up: A's block and shares, landed on B.
async fn s03_b_dies(sim: &mut Sim) -> Result<Body> {
    let mut body = Body::default();
    sim.load()?.resume();
    steady(sim, 5).await;
    let a1 = find_block(sim, &mut body, Node::A, "A before B dies", None).await?;
    confirm_on_both(sim, &a1).await?;
    routing_settled(sim).await?;
    let fault_at = sim.inject(Fault::FrontendKill9(Node::B)).await?;
    sim.inject(Fault::PostgresKill(Node::B)).await?;
    steady(sim, 8).await;
    let a2 = find_block(
        sim,
        &mut body,
        Node::A,
        "A while B is down",
        Some(&[Node::A]),
    )
    .await?;
    steady(sim, 2).await;
    let records = sim.load()?.records();
    let gap = report::gap(&records, fault_at);
    body.expect(
        "A's miners see no gap",
        gap.gap_ms.is_some_and(|ms| ms <= NO_IMPACT_GAP_MS),
        format!(
            "gap {:?} ms across B's death (bound {NO_IMPACT_GAP_MS} ms)",
            gap.gap_ms
        ),
    );
    body.gaps.push(gap);
    sim.heal(Fault::PostgresKill(Node::B)).await?;
    sim.heal(Fault::FrontendKill9(Node::B)).await?;
    let started = Instant::now();
    sim.wait_confirmed(&a2, &[Node::B], CATCH_UP_BOUND).await?;
    sim.wait_synced(CATCH_UP_BOUND).await?;
    body.expect(
        "B catches up after it returns",
        true,
        format!(
            "B held A's block and every share {:.1} s after its frontend was ready",
            started.elapsed().as_secs_f64()
        ),
    );
    sim.settle(SETTLE_BOUND).await?;
    expect_dual_health(sim, &mut body).await?;
    Ok(body)
}

/// S4. The link between the databases is blackholed while both nodes are
/// up and every miner is on A: mining does not notice, A finds a block B
/// cannot see, and on heal the sync resumes and B catches up.
async fn s04_link_cut(sim: &mut Sim) -> Result<Body> {
    let mut body = Body::default();
    sim.load()?.resume();
    steady(sim, 5).await;
    routing_settled(sim).await?;
    let fault = Fault::LinkCut(LinkState::Blackholed);
    let fault_at = sim.inject(fault).await?;
    steady(sim, 4).await;
    let hidden = find_block(
        sim,
        &mut body,
        Node::A,
        "A during the cut",
        Some(&[Node::A]),
    )
    .await?;
    steady(sim, 4).await;
    let b_has =
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM qbit_pool_blocks WHERE block_hash = $1")
            .bind(&hidden)
            .fetch_one(&sim.pool(Node::B).await?)
            .await?;
    body.expect(
        "the cut really separated the databases",
        b_has == 0,
        format!("B held A's block during the cut: {}", b_has != 0),
    );
    let healed_at = sim.heal(fault).await?;
    let started = Instant::now();
    confirm_on_both(sim, &hidden).await?;
    sim.wait_synced(CATCH_UP_BOUND).await?;
    body.expect(
        "the sync catches up after the heal",
        true,
        format!(
            "B held everything {:.1} s after the heal",
            started.elapsed().as_secs_f64()
        ),
    );
    steady(sim, 2).await;
    sim.settle(SETTLE_BOUND).await?;
    let records = sim.load()?.records();
    for (at, what) in [(fault_at, "the cut"), (healed_at, "the heal")] {
        let gap = report::gap(&records, at);
        body.expect(
            &format!("no mining impact at {what}"),
            gap.gap_ms.is_some_and(|ms| ms <= NO_IMPACT_GAP_MS),
            format!("gap {:?} ms (bound {NO_IMPACT_GAP_MS} ms)", gap.gap_ms),
        );
        body.gaps.push(gap);
    }
    expect_dual_health(sim, &mut body).await?;
    Ok(body)
}

/// S5. A misrouting balancer sends miners to both nodes, so both write at
/// once. Blocks are found on both, interleaved; every one lands on both
/// nodes, B's are carry-free, and nothing is credited or paid twice.
async fn s05_both_write(sim: &mut Sim) -> Result<Body> {
    let mut body = Body::default();
    sim.load()?.resume();
    steady(sim, 6).await;
    for (node, note) in [
        (Node::A, "A, both writing"),
        (Node::B, "B, both writing"),
        (Node::A, "A again"),
        (Node::B, "B again"),
    ] {
        let hash = find_block(sim, &mut body, node, note, Some(&[node])).await?;
        confirm_on_both(sim, &hash).await?;
        steady(sim, 3).await;
    }
    sim.settle(SETTLE_BOUND).await?;
    let records = sim.load()?.records();
    for node in Node::BOTH {
        body.expect(
            &format!("node {node:?} took miners"),
            accepted_from(&records, node, 0) > 0,
            format!(
                "{} shares accepted on its jobs",
                accepted_from(&records, node, 0)
            ),
        );
    }
    expect_dual_health(sim, &mut body).await?;
    Ok(body)
}

// --- S10 ---------------------------------------------------------------------

/// The 3.0 pair (dual mode off): A's database is the writer, B's its
/// streaming standby, both frontends write A's. Under load: a block on
/// each node, A's frontend killed, miners move to B, B finds a block, A
/// returns and miners fail back, A finds a block. Every acknowledged share
/// is in the writer, every block is landed, audited and paid exactly, and
/// the standby holds them all; nothing reports a dual-writer mode.
async fn s10_single_writer(sim: &mut Sim) -> Result<Body> {
    let mut body = Body::default();
    sim.load()?.resume();
    steady(sim, 8).await;
    find_block(sim, &mut body, Node::A, "A before the fault", None).await?;
    find_block(sim, &mut body, Node::B, "B before the fault", None).await?;

    routing_settled(sim).await?;
    let fault = Fault::FrontendKill9(Node::A);
    let fault_at = sim.inject(fault).await?;
    sim.balancer
        .wait_state("a", false, MARK_DOWN_BOUND)
        .await
        .context("the balancer kept A up after its frontend died")?;
    sim.mark("balancer marked A down");
    steady(sim, 8).await;
    find_block(sim, &mut body, Node::B, "B while A is down", None).await?;
    let records = sim.load()?.records();
    let on_b = accepted_from(&records, Node::B, fault_at);
    body.expect(
        "miners moved to B",
        on_b > 0,
        format!("{on_b} shares accepted on B's jobs after the fault"),
    );

    sim.heal(fault).await?;
    sim.balancer
        .wait_state("a", true, Duration::from_secs(30))
        .await?;
    sim.mark("balancer marked A up");
    steady(sim, 6).await;
    find_block(sim, &mut body, Node::A, "A after its restart", None).await?;
    steady(sim, 2).await;
    sim.settle(SETTLE_BOUND).await?;

    let records = sim.load()?.records();
    let gap = report::gap(&records, fault_at);
    body.expect(
        "miner-visible gap within the failover bound",
        gap.fault_to_first_accept_ms
            .is_some_and(|ms| ms <= FAILOVER_GAP_BOUND_MS),
        format!(
            "first accepted share {:?} ms after the kill (bound {FAILOVER_GAP_BOUND_MS} ms); \
             gap {:?} ms",
            gap.fault_to_first_accept_ms, gap.gap_ms
        ),
    );
    body.gaps.push(gap);
    let back_on_a = accepted_from(&records, Node::A, sim.clock.now_ms().saturating_sub(4_000));
    body.expect(
        "miners failed back to A",
        back_on_a > 0,
        format!("{back_on_a} shares accepted on A's jobs in the last 4 s of load"),
    );
    for node in Node::BOTH {
        let dual = dual_writer_health(sim, node).await?;
        body.expect(
            &format!("node {node:?} reports no dual-writer mode"),
            dual.as_ref()
                .is_none_or(|dual| dual.get("enabled") == Some(&json!(false))),
            format!("/healthz dual_writer: {dual:?}"),
        );
    }
    standby_holds_every_landing(sim, &mut body).await?;
    Ok(body)
}

/// The 3.0 standby must hold every landing the writer holds.
async fn standby_holds_every_landing(sim: &Sim, body: &mut Body) -> Result<()> {
    let digest =
        "SELECT md5(string_agg(block_hash || ':' || chain_state, ',' ORDER BY block_hash)) \
                  FROM qbit_pool_blocks";
    let writer: Option<String> = sqlx::query_scalar(digest)
        .fetch_one(&sim.pool(Node::A).await?)
        .await?;
    let standby: Option<String> = sqlx::query_scalar(digest)
        .fetch_one(&sim.pool(Node::B).await?)
        .await?;
    body.expect(
        "the standby holds every landing",
        writer == standby && writer.is_some(),
        format!("writer {writer:?}, standby {standby:?}"),
    );
    Ok(())
}

/// Link states a scenario can name.
pub fn link_state(name: &str) -> Option<LinkState> {
    match name {
        "reset" => Some(LinkState::Reset),
        "blackhole" => Some(LinkState::Blackholed),
        _ => None,
    }
}
