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
/// How long S2's frozen variant keeps A frozen while B mines. A frozen peer
/// must never stall the survivor's writes: D1's sync barrier, held by a
/// puller frozen mid-pull, would block every job and landing on B.
pub const FREEZE_HOLD: Duration = Duration::from_secs(60);
/// How long B may take, while A is frozen, to record new jobs after a new
/// tip or one of its own landings: its block poll, the template, the
/// prepared record and the jobs it issues.
pub const NEW_WORK_BOUND: Duration = Duration::from_secs(15);
/// The short freezes of S2's frozen-during-pulls variant.
pub const SHORT_FREEZES: usize = 20;
/// How long a dead puller's backend on its peer may keep a snapshot or an
/// open transaction: "a few seconds" (the coordinator's review of D1's
/// engine, P1 2). Longer, and every share append on the peer pays for it
/// (#738).
pub const IDLE_BOUND: Duration = Duration::from_secs(10);
/// The offered share rate of S2's puller-dies-mid-read variant: where a
/// snapshot held on a node shows in its share appends (#738).
pub const PULLER_DEATH_RATE: f64 = 250.0;
/// How long a restarted node whose peer answers but cannot be read may take
/// to serve (D-8: with the peer unreachable the latch is true unless there is
/// evidence of a rollback).
pub const UNREADABLE_READY_BOUND: Duration = Duration::from_secs(120);
/// How long A keeps its landing tables locked after B's block lands, in S4's
/// transient-apply variant: past A's 5 s lock_timeout twice over.
pub const TRANSIENT_HOLD: Duration = Duration::from_secs(12);

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

/// Where a block found by a dying node got to (S8).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeathAtFind {
    /// The node died with the block's `submitblock` in flight: it never
    /// reached the chain.
    Lost,
    /// The node's `qbitd` accepted the block and the node died before it
    /// heard: the block is on the chain and its finder never landed it. With
    /// D-19's peer-ingest wait on (the default) or off.
    Accepted { wait: bool },
}

/// How a peer that accepts connections cannot be read (S6's unreadable-peer
/// variant).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unreadable {
    /// A transaction on the peer holds ACCESS EXCLUSIVE on its share ledger:
    /// every read of it waits until the peer role's lock_timeout.
    Locked,
    /// The peer role's SELECT on the share ledger is revoked: every read of it
    /// fails at once.
    Refused,
}

impl Unreadable {
    fn id(self) -> &'static str {
        match self {
            Unreadable::Locked => "locked",
            Unreadable::Refused => "refused",
        }
    }
}

/// CONTRACT.md D-19's peer-ingest wait before a found block's submitblock,
/// in milliseconds (default 250 in dual mode, 0 turns it off). D1 names the
/// setting in status/D1.md.
pub const PEER_INGEST_WAIT_SETTING: &str = "PRISM_PEER_INGEST_WAIT_MS";

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
    /// S2, frozen mid-pull: A's frontend is frozen 20 times while its
    /// puller works on B's database; B never stalls.
    S02FrozenDuringPulls,
    /// S2, A's puller dies mid-read where B never sees the close: B keeps no
    /// snapshot of it and its appends stay flat.
    S02PullerDiesMidRead,
    /// S3: B dies; A is unaffected, and B catches up when it returns.
    S03BDies,
    /// S4: the link between the databases is cut with both nodes alive.
    S04LinkCut,
    /// S4, a transient failure while A applies B's block: retried, never
    /// skipped.
    S04TransientApply,
    /// S5: both nodes take miners and write at once.
    S05BothWrite,
    /// S6: a node restored from an older base backup recovers its own rows
    /// from the peer before it is ready.
    S06Restore(Node),
    /// S6, A restarts while B answers but cannot be read: A serves.
    S06PeerUnreadable(Unreadable),
    /// S7: a node's disk replaced and rebuilt from its peer.
    S07DiskReplaced(Node),
    /// S8: a block found at the instant its node dies.
    S08BlockAtDeath(DeathAtFind),
    /// S9: a 3.0 single-writer ledger with history, carry and blocks cut
    /// over to dual mode.
    S09Migration,
    /// S11: the carry-owner transfer drill.
    S11CarryOwnerTransfer,
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
            Scenario::S02FrozenDuringPulls => "s02-a-frozen-during-pulls".into(),
            Scenario::S02PullerDiesMidRead => "s02-a-puller-dies-mid-read".into(),
            Scenario::S03BDies => "s03-b-dies".into(),
            Scenario::S04LinkCut => "s04-link-cut".into(),
            Scenario::S04TransientApply => "s04-transient-apply-failure".into(),
            Scenario::S05BothWrite => "s05-both-write".into(),
            Scenario::S06Restore(node) => format!("s06-restore-{}", node.label()),
            Scenario::S06PeerUnreadable(how) => format!("s06-a-restarts-peer-{}", how.id()),
            Scenario::S07DiskReplaced(node) => format!("s07-disk-replaced-{}", node.label()),
            Scenario::S08BlockAtDeath(DeathAtFind::Lost) => "s08-block-at-death-lost".into(),
            Scenario::S08BlockAtDeath(DeathAtFind::Accepted { wait: true }) => {
                "s08-block-at-death-accepted".into()
            }
            Scenario::S08BlockAtDeath(DeathAtFind::Accepted { wait: false }) => {
                "s08-block-at-death-accepted-no-wait".into()
            }
            Scenario::S09Migration => "s09-migration".into(),
            Scenario::S11CarryOwnerTransfer => "s11-carry-owner-transfer".into(),
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
            Scenario::S02FrozenDuringPulls => {
                "A frozen again and again mid-pull: B keeps preparing work and landing blocks".into()
            }
            Scenario::S02PullerDiesMidRead => {
                "A's puller dies mid-read unseen by B: no snapshot left on B, appends stay flat".into()
            }
            Scenario::S03BDies => "B dies: A is unaffected and B catches up on return".into(),
            Scenario::S04LinkCut => {
                "Link cut with both alive: no mining impact, sync catches up on heal".into()
            }
            Scenario::S04TransientApply => {
                "A transient failure applying B's block on A: retried until it lands, never skipped"
                    .into()
            }
            Scenario::S05BothWrite => {
                "Both nodes write at once: invariants hold and B's blocks stay carry-free".into()
            }
            Scenario::S06Restore(node) => format!(
                "Node {node:?} restored from an older base backup recovers its own rows before it is ready"
            ),
            Scenario::S06PeerUnreadable(how) => format!(
                "A restarts while B answers but cannot be read ({}): A serves on its own evidence",
                how.id()
            ),
            Scenario::S07DiskReplaced(node) => format!(
                "Node {node:?}'s disk replaced: rebuilt from its peer, only its unsynced tail lost"
            ),
            Scenario::S08BlockAtDeath(case) => format!(
                "A block found at the instant its node dies ({case:?}) is landed or adopted once, never paid twice"
            ),
            Scenario::S09Migration => {
                "3.0 to 3.1 cutover: balances and audits unchanged, then dual-mode mining".into()
            }
            Scenario::S11CarryOwnerTransfer => {
                "Carry-owner transfer: refused while an own block is unknown, done once it is landed".into()
            }
            Scenario::S10SingleWriter => {
                "Single-writer regression: the 3.0 pair keeps every share and pays exactly through a frontend kill".into()
            }
        }
    }

    fn topology(self) -> Topology {
        match self {
            Scenario::CheckerControl => Topology::Unsynced,
            Scenario::S10SingleWriter | Scenario::S09Migration => Topology::SingleWriter,
            _ => Topology::DualWriter,
        }
    }

    fn config(self) -> Result<SimConfig> {
        let mut config = SimConfig::new(&self.id(), self.topology())?;
        if let Scenario::S08BlockAtDeath(case) = self {
            config.rpc_gates = true;
            if case == (DeathAtFind::Accepted { wait: false }) {
                for node in Node::BOTH {
                    config
                        .overrides
                        .entry(node)
                        .or_default()
                        .push((PEER_INGEST_WAIT_SETTING.into(), "0".into()));
                }
            }
        }
        // S2's puller death runs at the share rate where a snapshot held on
        // a node shows in its appends (#738).
        if self == Scenario::S02PullerDiesMidRead {
            config.load.rate = PULLER_DEATH_RATE;
        }
        // Both nodes take miners where each must originate rows (S6, S7),
        // where both writing is the point (S5), and where the survivor must
        // be busy while its peer is frozen, dies or fails to apply (S2's
        // mid-pull and puller-death variants, S4's transient apply).
        if matches!(
            self,
            Scenario::CheckerControl
                | Scenario::S02FrozenDuringPulls
                | Scenario::S02PullerDiesMidRead
                | Scenario::S04TransientApply
                | Scenario::S05BothWrite
                | Scenario::S06Restore(_)
                | Scenario::S07DiskReplaced(_)
        ) {
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
            Scenario::S02FrozenDuringPulls => s02_frozen_during_pulls(&mut sim).await?,
            Scenario::S02PullerDiesMidRead => s02_puller_dies_mid_read(&mut sim).await?,
            Scenario::S03BDies => s03_b_dies(&mut sim).await?,
            Scenario::S04LinkCut => s04_link_cut(&mut sim).await?,
            Scenario::S04TransientApply => s04_transient_apply(&mut sim).await?,
            Scenario::S05BothWrite => s05_both_write(&mut sim).await?,
            Scenario::S06Restore(node) => s06_restore(&mut sim, node).await?,
            Scenario::S06PeerUnreadable(how) => s06_peer_unreadable(&mut sim, how).await?,
            Scenario::S07DiskReplaced(node) => s07_disk_replaced(&mut sim, node).await?,
            Scenario::S08BlockAtDeath(case) => s08_block_at_death(&mut sim, case).await?,
            Scenario::S09Migration => s09_migration(&mut sim).await?,
            Scenario::S11CarryOwnerTransfer => s11_carry_owner_transfer(&mut sim).await?,
            Scenario::S10SingleWriter => s10_single_writer(&mut sim).await?,
        };
        let records = sim.load()?.records();
        let invariants = invariants::check(&sim, &records, &body.options).await?;
        invariants::write(&invariants, &sim.report_dir)?;
        judge_invariants(&mut body, &invariants);
        if scenario == Scenario::CheckerControl {
            // Last, since it tampers with A's ledger.
            late_row_control(&sim, &mut body).await?;
        }
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

/// Settle `node` and hand its finder fresh work: the state a block is solved
/// from.
async fn prepare_finder(sim: &Sim, node: Node) -> Result<()> {
    sim.wait_node_settled(node, NODE_SETTLE_BOUND).await?;
    sim.load()?
        .refresh_finder(node, Duration::from_secs(30))
        .await
}

/// One attempt: solve a block on the finder's current work and wait until it
/// is confirmed in the databases `confirm_in` (default: wherever the node
/// writes). `Ok(Err(why))` when the work turned out superseded, which a
/// retry from settled work resolves.
async fn solve_and_land(
    sim: &Sim,
    node: Node,
    note: &str,
    confirm_in: Option<&[Node]>,
) -> Result<std::result::Result<FoundBlock, String>> {
    let record = sim.load()?.find_block(node, LANDING_BOUND).await?;
    let hash = record.header_hash().to_owned();
    if !record.accepted() {
        let reason = record.reason.clone().unwrap_or_default();
        if matches!(reason.as_str(), "stale-job" | "unknown-job") {
            sim.mark(&format!(
                "block on node {node:?} refused as {reason}; solving again"
            ));
            return Ok(Err(format!("refused ({reason})")));
        }
        anyhow::bail!(
            "node {node:?} refused its own block ({}: {reason})",
            record.outcome
        );
    }
    if !sim.wait_candidate(node, &hash, CANDIDATE_BOUND).await? {
        sim.mark(&format!(
            "block {hash} on node {node:?} was solved on superseded work; solving again"
        ));
        return Ok(Err(format!("{hash} accepted but never a candidate")));
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
    Ok(Ok(FoundBlock {
        node,
        hash,
        found_at_ms,
        note: note.to_owned(),
    }))
}

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
        prepare_finder(sim, node).await?;
        match solve_and_land(sim, node, note, confirm_in).await? {
            Ok(found) => {
                let hash = found.hash.clone();
                body.blocks.push(found);
                return Ok(hash);
            }
            Err(why) => attempts.push(format!("attempt {attempt}: {why}")),
        }
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
        .filter(|r| r.accepted_on(node, after_ms))
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
    truth_below_on(sim, Node::A, height).await
}

/// [`truth_below`], from `ledger`'s confirmed set.
async fn truth_below_on(
    sim: &Sim,
    ledger: Node,
    height: i64,
) -> Result<std::collections::BTreeMap<String, i128>> {
    let pool = sim.pool(ledger).await?;
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
/// it: its priors equal R1's sum.
async fn expect_owner_priors_are_truth(sim: &Sim, body: &mut Body, hash: &str) -> Result<()> {
    expect_priors_are_truth(sim, body, Node::A, hash).await
}

/// `owner`'s block `hash` paid from exactly the chain's balances below it,
/// as `owner`'s ledger holds them.
async fn expect_priors_are_truth(
    sim: &Sim,
    body: &mut Body,
    owner: Node,
    hash: &str,
) -> Result<()> {
    let (issued, truth, height) = owner_priors_on(sim, owner, hash).await?;
    body.expect(
        "the owner paid from the chain's balances",
        issued == truth,
        format!("block {hash} at {height}: issued priors {issued:?}, chain sums {truth:?}"),
    );
    Ok(())
}

type Balances = std::collections::BTreeMap<String, i128>;

/// A block's issued priors (from its audit) and R1's sums below it, both
/// read from `owner`'s ledger.
async fn owner_priors_on(sim: &Sim, owner: Node, hash: &str) -> Result<(Balances, Balances, i64)> {
    let pool = sim.pool(owner).await?;
    let (height, priors): (i64, serde_json::Value) = sqlx::query_as(
        "SELECT b.block_height, a.audit_bundle->'prior_balances' FROM qbit_pool_blocks b \
         JOIN qbit_pool_audit_bundles a ON a.block_hash = b.block_hash WHERE b.block_hash = $1",
    )
    .bind(hash)
    .fetch_one(&pool)
    .await?;
    pool.close().await;
    let mut issued = Balances::new();
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
    Ok((issued, truth_below_on(sim, owner, height).await?, height))
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
    let order =
        crate::measure::LandingOrderSampler::start(sim.pool(Node::B).await?, Node::A, sim.clock)
            .await?;
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
    expect_landing_order(&mut body, Node::B, order.stop().await?);
    body.expect(
        "A's shares reach B within the sync interval",
        lag.resolved > 0 && lag.p95_ms.is_some_and(|ms| ms <= SYNC_LAG_BOUND_MS),
        format!(
            "{} of {} samples seen on B; p50 {:?} ms, p95 {:?} ms, max {:?} ms (interval {} ms, bound {SYNC_LAG_BOUND_MS} ms, resolution {} ms, {} failed polls)",
            lag.resolved, lag.samples, lag.p50_ms, lag.p95_ms, lag.max_ms,
            sim.config.sync_interval_ms, lag.resolution_ms, lag.failed_polls
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

/// D-5 and D-10, for the peer blocks one sampler watched arrive on `on`: none
/// was there before its window's shares or without its window record, and at
/// least one was seen whole on a poll that followed a good one, so the order
/// was shown, not merely not contradicted.
fn expect_landing_order(body: &mut Body, on: Node, report: crate::measure::LandingOrderReport) {
    use crate::measure::LandingVerdict;
    let bad: Vec<&crate::measure::LandingOrder> = report
        .samples
        .iter()
        .filter(|sample| {
            matches!(
                sample.verdict(),
                LandingVerdict::Violated | LandingVerdict::Torn
            )
        })
        .collect();
    let in_order = report.count(LandingVerdict::InOrder);
    body.expect(
        &format!(
            "node {:?}'s blocks never land on node {on:?} before their window's shares (D-5) \
             or without their audit (D-10)",
            on.peer()
        ),
        bad.is_empty() && in_order > 0,
        format!(
            "{} blocks seen arriving ({} already there at the start): {in_order} with every \
             window share there, {} before their shares, {} without their window record, {} \
             unresolved (inline, or first seen after a failed poll); out of order: {bad:?}. \
             {} of {} polls failed {:?}; resolution {} ms",
            report.samples.len(),
            report.baseline,
            report.count(LandingVerdict::Violated),
            report.count(LandingVerdict::Torn),
            report.count(LandingVerdict::Unresolved),
            report.failed_polls,
            report.polls,
            report.errors,
            report.resolution_ms
        ),
    );
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
    if death == Death::Freeze {
        hold_frozen(sim, &mut body, fault_at, &mut b_blocks).await?;
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

/// The database's clock now: every time compared with a row's timestamp is
/// taken from the database that wrote the row.
async fn db_now(pool: &sqlx::PgPool) -> Result<chrono::DateTime<chrono::Utc>> {
    Ok(sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(pool)
        .await?)
}

/// The jobs `node`'s frontend recorded after `after`, and how many of them
/// are prepared records (`prepared:` keys; the rest are issued jobs). Every
/// one takes D1's sync barrier (`sync_seq`'s default), as a landing does.
async fn jobs_after(
    pool: &sqlx::PgPool,
    node: Node,
    after: chrono::DateTime<chrono::Utc>,
) -> Result<(i64, i64)> {
    Ok(sqlx::query_as(
        "SELECT count(*), count(*) FILTER (WHERE job_id LIKE 'prepared:%') \
         FROM qbit_prism_jobs WHERE instance_id = $1 AND created_at > $2",
    )
    .bind(node.instance_id())
    .bind(after)
    .fetch_one(pool)
    .await?)
}

/// Wait for `node`'s first job built on `parent`: how long it took, or why
/// it did not come within `limit`.
async fn first_job_on(
    pool: &sqlx::PgPool,
    node: Node,
    parent: &str,
    limit: Duration,
) -> Result<std::result::Result<u64, String>> {
    let started = Instant::now();
    loop {
        let jobs: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM qbit_prism_jobs WHERE instance_id = $1 AND parent_hash = $2",
        )
        .bind(node.instance_id())
        .bind(parent)
        .fetch_one(pool)
        .await?;
        if jobs > 0 {
            return Ok(Ok(started.elapsed().as_millis() as u64));
        }
        if started.elapsed() >= limit {
            return Ok(Err(format!(
                "no job of node {node:?}'s on {parent} within {} s",
                limit.as_secs()
            )));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// S2's frozen variant, once B has taken the miners: keep A frozen for
/// `FREEZE_HOLD` while B finds a block every few seconds. Each must land,
/// and B must build work on each (a job whose parent is the block) before
/// A thaws; A's frontend must still be the stopped process it froze.
async fn hold_frozen(
    sim: &mut Sim,
    body: &mut Body,
    fault_at: u64,
    b_blocks: &mut Vec<String>,
) -> Result<()> {
    let frozen_pid = sim
        .frontend(Node::A)
        .pid()
        .context("A's frontend has no process")?;
    let pool = sim.pool(Node::B).await?;
    let since = db_now(&pool).await?
        - chrono::Duration::milliseconds(sim.clock.now_ms().saturating_sub(fault_at) as i64);
    while Duration::from_millis(sim.clock.now_ms().saturating_sub(fault_at)) < FREEZE_HOLD {
        steady(sim, 5).await;
        b_blocks.push(
            find_block(
                sim,
                body,
                Node::B,
                "B while A stays frozen",
                Some(&[Node::B]),
            )
            .await?,
        );
    }
    // Work on each of B's blocks before the thaw. Every block but the last
    // was solved on such work; the last gets the bound.
    let started = Instant::now();
    let without = loop {
        let without: Vec<String> = sqlx::query_scalar(
            "SELECT b.hash FROM unnest($2::text[]) AS b(hash) \
             WHERE NOT EXISTS (SELECT 1 FROM qbit_prism_jobs j \
                               WHERE j.instance_id = $1 AND j.parent_hash = b.hash)",
        )
        .bind(Node::B.instance_id())
        .bind(&b_blocks[..])
        .fetch_all(&pool)
        .await?;
        if without.is_empty() || started.elapsed() >= NEW_WORK_BOUND {
            break without;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    let still_frozen =
        sim.frontend(Node::A).pid() == Some(frozen_pid) && crate::process::stopped(frozen_pid);
    let held = Duration::from_millis(sim.clock.now_ms().saturating_sub(fault_at));
    let (jobs, prepared) = jobs_after(&pool, Node::B, since).await?;
    pool.close().await;
    body.expect(
        "B keeps landing blocks and building work on each while A stays frozen",
        still_frozen && without.is_empty() && prepared > 0,
        format!(
            "A frozen {:.1} s (target at least {} s; still the stopped process it froze at the \
             end: {still_frozen}); B landed {} blocks meanwhile and recorded {jobs} jobs \
             ({prepared} prepared records) since the freeze; blocks B built no work on within \
             {} s: {without:?}",
            held.as_secs_f64(),
            FREEZE_HOLD.as_secs(),
            b_blocks.len(),
            NEW_WORK_BOUND.as_secs()
        ),
    );
    Ok(())
}

/// What A's puller was doing on B's database: its backends there (the peer
/// role's), how many were running a query or had a transaction open, and how
/// many advisory locks they held (D1's sync barrier is one).
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
struct PullState {
    backends: i64,
    busy: i64,
    advisory_locks: i64,
}

impl PullState {
    fn mid_pull(&self) -> bool {
        self.busy > 0 || self.advisory_locks > 0
    }
}

async fn pull_state(pool: &sqlx::PgPool) -> Result<PullState> {
    let (backends, busy, advisory_locks): (i64, i64, i64) = sqlx::query_as(
        "SELECT count(*), \
                count(*) FILTER (WHERE a.state IN ('active', 'idle in transaction', \
                                                   'idle in transaction (aborted)')), \
                coalesce(sum(held.locks), 0)::bigint \
         FROM pg_stat_activity a \
         LEFT JOIN LATERAL (SELECT count(*) AS locks FROM pg_locks l \
                            WHERE l.pid = a.pid AND l.locktype = 'advisory' AND l.granted) held \
           ON true \
         WHERE a.usename = $1 AND a.datname = current_database()",
    )
    .bind(crate::sim::PEER_ROLE)
    .fetch_one(pool)
    .await?;
    Ok(PullState {
        backends,
        busy,
        advisory_locks,
    })
}

/// How often the freeze timing looks for a pull: coarse enough that reading
/// `pg_locks` adds no contention to the database whose stalls it measures.
const PULL_POLL: Duration = Duration::from_millis(20);

/// Wait, up to `limit`, until A's puller is seen mid-pull on B's database.
async fn wait_for_a_pull(pool: &sqlx::PgPool, limit: Duration) -> Result<bool> {
    let started = Instant::now();
    while started.elapsed() < limit {
        if pull_state(pool).await?.mid_pull() {
            return Ok(true);
        }
        tokio::time::sleep(PULL_POLL).await;
    }
    Ok(false)
}

/// A small seeded generator for the freeze schedule; its seed is reported.
struct Schedule(u64);

impl Schedule {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    /// A number in `0..n` (xorshift64).
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n.max(1)
    }
}

/// What B did while A was frozen in one round of `s02_frozen_during_pulls`.
#[derive(Clone, Debug, serde::Serialize)]
enum RoundWork {
    /// A new tip from C, minted once A was frozen: how long the mint took,
    /// and B's first job on the tip, timed from the mint's return.
    NewTip {
        tip: String,
        mint_ms: u64,
        job_ms: std::result::Result<u64, String>,
    },
    /// B's block, solved on work B handed its finder before the freeze: how
    /// long from asking the finder to solve it, through its acceptance and
    /// landing, to its confirmation on B, and B's first job on it, timed from
    /// the confirmation.
    Landed {
        block: String,
        found_and_confirmed_ms: u64,
        job_ms: std::result::Result<u64, String>,
    },
    /// B could not land its block while A was frozen.
    LandingFailed { why: String },
    /// The block was solved on work superseded between the hand-out and the
    /// solve (a tip or revision change): not a stall, and not judged.
    Superseded { why: String },
}

impl RoundWork {
    /// Why B stalled in this round, if it did.
    fn stall(&self) -> Option<String> {
        match self {
            RoundWork::NewTip {
                job_ms: Err(why), ..
            }
            | RoundWork::Landed {
                job_ms: Err(why), ..
            }
            | RoundWork::LandingFailed { why } => Some(why.clone()),
            _ => None,
        }
    }
}

/// One short freeze of A in `s02_frozen_during_pulls`.
#[derive(Clone, Debug, serde::Serialize)]
struct FreezeRound {
    round: usize,
    at_ms: u64,
    held_ms: u64,
    /// A's puller was seen mid-pull on B just before the stop.
    timed_to_a_pull: bool,
    /// What it held on B right after the stop.
    after_stop: PullState,
    work: RoundWork,
}

/// The work of one round, while A is frozen: B must record jobs on a new tip,
/// or land a block and build work on it.
async fn freeze_round(
    sim: &Sim,
    pool: &sqlx::PgPool,
    landing: bool,
) -> Result<(PullState, RoundWork, Option<FoundBlock>)> {
    let after_stop = pull_state(pool).await?;
    if landing {
        let started = Instant::now();
        return Ok(
            match solve_and_land(
                sim,
                Node::B,
                "B while A is frozen mid-pull",
                Some(&[Node::B]),
            )
            .await
            {
                Ok(Ok(found)) => {
                    let found_and_confirmed_ms = started.elapsed().as_millis() as u64;
                    let job_ms = first_job_on(pool, Node::B, &found.hash, NEW_WORK_BOUND).await?;
                    let work = RoundWork::Landed {
                        block: found.hash.clone(),
                        found_and_confirmed_ms,
                        job_ms,
                    };
                    (after_stop, work, Some(found))
                }
                Ok(Err(why)) => (after_stop, RoundWork::Superseded { why }, None),
                Err(error) => {
                    let why = format!("{error:#}");
                    (after_stop, RoundWork::LandingFailed { why }, None)
                }
            },
        );
    }
    let minting = Instant::now();
    let tip = sim
        .chain
        .mint(1)
        .await?
        .pop()
        .context("the mint made no block")?;
    let mint_ms = minting.elapsed().as_millis() as u64;
    let job_ms = first_job_on(pool, Node::B, &tip, NEW_WORK_BOUND).await?;
    Ok((
        after_stop,
        RoundWork::NewTip {
            tip,
            mint_ms,
            job_ms,
        },
        None,
    ))
}

/// S2, frozen mid-pull. Both nodes take miners. A's frontend is frozen
/// `SHORT_FREEZES` times for 1 to 4 s, each time, where it can be seen
/// within 3 s, the moment its puller is working on B's database (a query
/// running, a transaction open or an advisory lock held there). While A is
/// frozen a new tip arrives, and B must record jobs on it within the bound;
/// every fifth time B instead lands a block solved on work it handed out
/// before the freeze, and must build work on it. A puller frozen holding
/// D1's sync barrier on B would stall exactly these writes. A round lasts
/// longer than its target only while B is still at its work.
async fn s02_frozen_during_pulls(sim: &mut Sim) -> Result<Body> {
    let mut body = Body::default();
    sim.load()?.resume();
    steady(sim, 6).await;
    routing_settled(sim).await?;
    let pool = sim.pool(Node::B).await?;
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos() as u64;
    let mut schedule = Schedule::new(seed);
    let mut rounds = Vec::new();
    for round in 1..=SHORT_FREEZES {
        let landing = round % 5 == 0;
        if landing {
            prepare_finder(sim, Node::B).await?;
        }
        let timed_to_a_pull = wait_for_a_pull(&pool, Duration::from_secs(3)).await?;
        let hold = Duration::from_millis(1_000 + schedule.below(3_000));
        let freeze = Fault::FrontendFreeze(Node::A);
        let at_ms = sim.inject(freeze).await?;
        let frozen = Instant::now();
        // Whatever the round's work does, A is thawed before it is judged.
        let outcome = freeze_round(sim, &pool, landing).await;
        if let Some(rest) = hold.checked_sub(frozen.elapsed()) {
            tokio::time::sleep(rest).await;
        }
        let held_ms = frozen.elapsed().as_millis() as u64;
        sim.heal(freeze).await?;
        let (after_stop, work, found) = outcome?;
        body.blocks.extend(found);
        rounds.push(FreezeRound {
            round,
            at_ms,
            held_ms,
            timed_to_a_pull,
            after_stop,
            work,
        });
        tokio::time::sleep(Duration::from_millis(500 + schedule.below(2_500))).await;
    }
    pool.close().await;
    std::fs::write(
        sim.report_dir.join("freezes.json"),
        serde_json::to_vec_pretty(&json!({"seed": seed, "rounds": rounds}))?,
    )?;
    let stalled: Vec<&FreezeRound> = rounds.iter().filter(|r| r.work.stall().is_some()).collect();
    let mut tip_jobs: Vec<u64> = rounds
        .iter()
        .filter_map(|r| match &r.work {
            RoundWork::NewTip { job_ms: Ok(ms), .. } => Some(*ms),
            _ => None,
        })
        .collect();
    tip_jobs.sort_unstable();
    let landings: Vec<(u64, u64)> = rounds
        .iter()
        .filter_map(|r| match &r.work {
            RoundWork::Landed {
                found_and_confirmed_ms,
                ..
            } => Some((*found_and_confirmed_ms, r.held_ms)),
            _ => None,
        })
        .collect();
    let timed = rounds.iter().filter(|r| r.timed_to_a_pull).count();
    let caught = rounds.iter().filter(|r| r.after_stop.mid_pull()).count();
    body.expect(
        "B records jobs and lands blocks while A is frozen mid-pull",
        stalled.is_empty() && !landings.is_empty(),
        format!(
            "{} freezes ({timed} timed to one of A's pulls on B; {caught} left A with a query, a \
             transaction or an advisory lock open there). New tips: B's first job on each after \
             {:?} ms (median) and {:?} ms (max) from the mint, bound {} s. Landings: {} blocks \
             found, landed and confirmed on B during freezes, as (ms from the solve request to \
             confirmation, freeze ms): {landings:?}. \
             Stalled: {stalled:?}. Schedule seed {seed}; every round in freezes.json",
            rounds.len(),
            tip_jobs.get(tip_jobs.len() / 2),
            tip_jobs.last(),
            NEW_WORK_BOUND.as_secs(),
            landings.len()
        ),
    );
    sim.settle(SETTLE_BOUND).await?;
    expect_dual_health(sim, &mut body).await?;
    Ok(body)
}

/// How long S2's puller-dies-mid-read variant samples B for a pull that
/// holds a transaction open across a round trip, and how often.
pub const PULL_SAMPLE: Duration = Duration::from_secs(20);
const PULL_SAMPLE_EVERY: Duration = Duration::from_millis(10);
/// How long it watches B after A's death: past the peer role's 60 s
/// idle-in-transaction timeout, so a snapshot held until then is measured.
pub const LINGER_WATCH: Duration = Duration::from_secs(70);

/// The p95 answer latency, in milliseconds, of the shares accepted on
/// `node`'s jobs and answered within `from_ms..to_ms`, and how many there were.
fn answer_p95(
    records: &[ShareRecord],
    node: Node,
    from_ms: u64,
    to_ms: u64,
) -> (Option<u64>, usize) {
    let mut latencies: Vec<u64> = records
        .iter()
        .filter(|r| r.accepted() && !r.scheduled_block && r.issuer == Some(node))
        .filter_map(|r| {
            let answered = r.answered_ms?;
            (from_ms..to_ms)
                .contains(&answered)
                .then(|| answered.saturating_sub(r.sent_ms))
        })
        .collect();
    latencies.sort_unstable();
    let count = latencies.len();
    let p95 = (count > 0).then(|| latencies[((count * 95).div_ceil(100)).clamp(1, count) - 1]);
    (p95, count)
}

/// S2, A's puller dies mid-read (the coordinator's review of D1's engine,
/// P1 2). Both nodes take miners at `PULLER_DEATH_RATE`, where a snapshot
/// held on B shows in B's share appends (#738).
///
/// 1. While both run, no pull of A's holds a transaction open on B across a
///    round trip: sampled every 10 ms for `PULL_SAMPLE`, B never shows A's
///    backend idle in transaction.
/// 2. A's database link discards (B's replies drain and no close reaches
///    it, as when A's host or its VLAN dies; a plain kill -9 sends a FIN
///    that B handles at once), and A's frontend is killed, timed to a moment
///    its puller is busy on B. A's backends on B must hold no snapshot and
///    no open transaction beyond `IDLE_BOUND`.
/// 3. B takes every miner meanwhile, and its share answer latency stays
///    flat: its p95 45 to 60 s after the death is at most twice its p95 5 to
///    20 s after A was marked down, plus 50 ms.
///
/// Then the link heals, A restarts and catches up.
async fn s02_puller_dies_mid_read(sim: &mut Sim) -> Result<Body> {
    let mut body = Body::default();
    sim.load()?.resume();
    steady(sim, 8).await;
    routing_settled(sim).await?;
    let pool = sim.pool(Node::B).await?;

    let sampling = Instant::now();
    let (mut samples, mut idle_in_transaction, mut longest_ms) = (0u64, 0u64, 0f64);
    while sampling.elapsed() < PULL_SAMPLE {
        let (idle, oldest_ms): (i64, Option<f64>) = sqlx::query_as(
            "SELECT count(*) FILTER (WHERE state LIKE 'idle in transaction%'), \
                    max(extract(epoch FROM clock_timestamp() - xact_start) * 1000)::float8 \
             FROM pg_stat_activity WHERE usename = $1 AND datname = current_database()",
        )
        .bind(crate::sim::PEER_ROLE)
        .fetch_one(&pool)
        .await?;
        samples += 1;
        idle_in_transaction += u64::from(idle > 0);
        longest_ms = longest_ms.max(oldest_ms.unwrap_or(0.0));
        tokio::time::sleep(PULL_SAMPLE_EVERY).await;
    }
    body.expect(
        "A's pulls never hold a transaction open on B across a round trip",
        idle_in_transaction == 0,
        format!(
            "{idle_in_transaction} of {samples} samples of B's pg_stat_activity over {} s showed \
             A's backend idle in transaction; the longest transaction seen had run {longest_ms:.0} ms",
            PULL_SAMPLE.as_secs()
        ),
    );

    let timed = wait_for_a_pull(&pool, Duration::from_secs(5)).await?;
    let cut_at = sim.clock.now_ms();
    sim.links.set("peer-a-to-b", LinkState::Discard)?;
    let fault_at = sim.inject(Fault::FrontendKill9(Node::A)).await?;
    sim.mark(
        "A's database link discards as its frontend dies: B's replies drain, no close reaches it",
    );
    let died = Instant::now();
    let (mut last_holding, mut most) = (None, 0i64);
    while died.elapsed() < LINGER_WATCH {
        let holding: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity \
             WHERE usename = $1 AND datname = current_database() \
               AND (backend_xmin IS NOT NULL OR xact_start IS NOT NULL)",
        )
        .bind(crate::sim::PEER_ROLE)
        .fetch_one(&pool)
        .await?;
        if holding > 0 {
            last_holding = Some(died.elapsed());
            most = most.max(holding);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    pool.close().await;
    body.expect(
        "A's dead puller leaves B no snapshot and no open transaction beyond the bound",
        last_holding.is_none_or(|held| held <= IDLE_BOUND),
        format!(
            "killed {} one of A's pulls was seen busy on B; up to {most} of A's backends on B held \
             a snapshot or a transaction until {:?} after the death (bound {} s, watched {} s; the \
             peer role's idle-in-transaction timeout is the one status/D1.md drafts)",
            if timed { "as" } else { "without" },
            last_holding,
            IDLE_BOUND.as_secs(),
            LINGER_WATCH.as_secs()
        ),
    );

    // Flat appends: B's p95 early in the minute after the death, once A's
    // miners have moved to it, against its p95 late in that minute, while a
    // snapshot A left behind would still be held.
    let report = sim.balancer.report();
    let marked_down = report
        .transitions
        .iter()
        .find(|t| t.backend == "a" && !t.up && t.at_ms >= cut_at)
        .map(|t| t.at_ms);
    let records = sim.load()?.records();
    let flat = match marked_down {
        Some(down) if down + 20_000 <= fault_at + 45_000 => {
            let (early, early_n) = answer_p95(&records, Node::B, down + 5_000, down + 20_000);
            let (late, late_n) = answer_p95(&records, Node::B, fault_at + 45_000, fault_at + 60_000);
            let passed = matches!((early, late), (Some(early), Some(late)) if late <= 2 * early + 50);
            (
                passed,
                format!(
                    "p95 answer latency of shares on B's jobs: {early:?} ms over {early_n} shares \
                     5 to 20 s after A was marked down ({} ms after the death), {late:?} ms over \
                     {late_n} shares 45 to 60 s after the death (bound: twice the first, plus \
                     50 ms), at {PULLER_DEATH_RATE} offered shares/s",
                    down as i64 - fault_at as i64
                ),
            )
        }
        other => (
            false,
            format!("A was marked down at {other:?} ms, too late for an early window (death at {fault_at} ms)"),
        ),
    };
    body.expect(
        "B's share appends stay flat while A's puller is dead",
        flat.0,
        flat.1,
    );
    body.gaps.push(report::gap(&records, fault_at));

    sim.links.set("peer-a-to-b", LinkState::Open)?;
    sim.heal(Fault::FrontendKill9(Node::A)).await?;
    sim.balancer
        .wait_state("a", true, Duration::from_secs(60))
        .await?;
    sim.wait_synced(CATCH_UP_BOUND).await?;
    steady(sim, 2).await;
    sim.settle(SETTLE_BOUND).await?;
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
/// up and every miner is on A: mining does not notice, neither node is
/// withdrawn (D-8), A finds a block B cannot see, and on heal the sync
/// resumes and B catches up.
async fn s04_link_cut(sim: &mut Sim) -> Result<Body> {
    let mut body = Body::default();
    sim.load()?.resume();
    steady(sim, 5).await;
    routing_settled(sim).await?;
    // Both nodes are up from here (each mark is recorded before the state
    // it sets is visible); D-8 below needs them still up at the cut.
    let settled_at = sim.clock.now_ms();
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
    // D-8: the latch is for a node's start. A node serving when its peer is
    // lost keeps serving, so the balancer keeps it up from the cut until the
    // catch-up, and for long enough after it that a mark-down whose failing
    // checks began before it would have landed. Checks that failed while a
    // node stayed up (fewer than `fall` in a row, as a landed block's work
    // rebuild can cause) are counted, not failed.
    let horizon = crate::balancer::BalancerReport::mark_down_horizon(&sim.config.balancer);
    tokio::time::sleep(horizon).await;
    let until = sim.clock.now_ms();
    let report = sim.balancer.report();
    let names: Vec<&str> = Node::BOTH.iter().map(|node| node.label()).collect();
    // Serving at the cut: up once routing settled and still up when the
    // link was cut. Withdrawn: marked down after the cut.
    let serving: Vec<&str> = names
        .iter()
        .copied()
        .filter(|name| report.up_throughout(name, settled_at, fault_at))
        .collect();
    let withdrawn: Vec<&str> = serving
        .iter()
        .copied()
        .filter(|name| !report.up_throughout(name, fault_at, until))
        .collect();
    let failed_while_up: std::collections::BTreeMap<&str, usize> = names
        .iter()
        .map(|&name| {
            let count = report
                .failed_checks
                .iter()
                .filter(|f| {
                    f.backend == name && f.while_up && (fault_at..=until).contains(&f.at_ms)
                })
                .count();
            (name, count)
        })
        .collect();
    body.expect(
        "a link loss never withdraws a serving node (D-8)",
        serving.len() == names.len() && withdrawn.is_empty(),
        format!(
            "serving from when routing settled ({settled_at} ms) to the cut ({fault_at} ms): \
             {serving:?} (both must be); withdrawn after the cut, until {} ms after the catch-up \
             ({until} ms): {withdrawn:?}; checks failed while \
             serving: {failed_while_up:?}; transitions: {:?}",
            horizon.as_millis(),
            report.transitions
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

/// One of A's statements seen waiting on the locked table in S4's
/// transient apply: its backend, when the statement started, and its text.
type WaitingApply = (i32, String, String);

/// S4, a transient failure applying a peer block (the coordinator's review
/// of D1's engine, P1 3). A transaction on A holds EXCLUSIVE on its audit
/// bundles, so every landing insert on A waits until A's lock_timeout (5 s)
/// and fails while reads go on. B lands a block, and the lock stays for
/// `TRANSIENT_HOLD` more, long enough for two failed applies; at least one
/// of A's statements must be seen waiting on the lock, or the retry was
/// never exercised. Once the lock clears, the block must land on A within
/// the catch-up bound, and A must record no sync conflict for it: a
/// transient failure is retried, never skipped (D-10 skips only a block
/// whose held facts differ).
async fn s04_transient_apply(sim: &mut Sim) -> Result<Body> {
    let mut body = Body::default();
    sim.load()?.resume();
    steady(sim, 6).await;
    routing_settled(sim).await?;
    let a = sim.pool(Node::A).await?;
    let mut lock = a.begin().await?;
    sqlx::query("LOCK TABLE qbit_pool_audit_bundles IN EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await?;
    sim.mark("A's audit bundles locked: landing inserts wait until lock_timeout, reads go on");
    // Whatever happens while the lock is held, it is released before an
    // error is raised.
    let held = async {
        let block = find_block(
            sim,
            &mut body,
            Node::B,
            "B while A cannot apply its blocks",
            Some(&[Node::B]),
        )
        .await?;
        // Each distinct (backend, statement start) of A's frontend seen
        // waiting on the locked table is one attempt.
        let mut attempts: std::collections::BTreeSet<WaitingApply> = Default::default();
        let holding = Instant::now();
        while holding.elapsed() < TRANSIENT_HOLD {
            let waiting: Vec<WaitingApply> = sqlx::query_as(
                "SELECT a.pid, a.query_start::text, left(a.query, 120) \
                 FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid \
                 WHERE NOT l.granted \
                   AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database()) \
                   AND l.relation = 'qbit_pool_audit_bundles'::regclass \
                   AND a.usename = $1 AND a.query_start IS NOT NULL",
            )
            .bind(crate::sim::OWNER_ROLE)
            .fetch_all(&a)
            .await?;
            attempts.extend(waiting);
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let on_a: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM qbit_pool_blocks WHERE block_hash = $1)",
        )
        .bind(&block)
        .fetch_one(&a)
        .await?;
        anyhow::Ok((block, attempts, on_a))
    }
    .await;
    lock.rollback().await?;
    sim.mark("A's audit bundles unlocked");
    let (block, attempts, held_while_locked) = held?;
    let landed = sim.wait_confirmed(&block, &[Node::A], CATCH_UP_BOUND).await;
    let conflicts: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT source_table, row_key, detail FROM qbit_prism_peer_sync_conflicts \
         WHERE row_key LIKE '%' || $1 || '%'",
    )
    .bind(&block)
    .fetch_all(&a)
    .await?;
    a.close().await;
    body.expect(
        "B's block, which A failed to apply while locked, lands on A once the lock clears",
        !attempts.is_empty() && !held_while_locked && landed.is_ok() && conflicts.is_empty(),
        format!(
            "{} statements of A's frontend waited on the lock during the {} s hold (at least one \
             must, or the retry was never exercised): {attempts:?}; on A while locked: \
             {held_while_locked} (the lock must keep it out); confirmed on A after the release: \
             {}; sync conflicts recorded for it: {conflicts:?}",
            attempts.len(),
            TRANSIENT_HOLD.as_secs(),
            match &landed {
                Ok(took) => format!("after {:.1} s", took.as_secs_f64()),
                Err(error) => format!("not within {CATCH_UP_BOUND:?} ({error:#})"),
            }
        ),
    );
    steady(sim, 2).await;
    sim.settle(SETTLE_BOUND).await?;
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
    let on_a =
        crate::measure::LandingOrderSampler::start(sim.pool(Node::A).await?, Node::B, sim.clock)
            .await?;
    let on_b =
        crate::measure::LandingOrderSampler::start(sim.pool(Node::B).await?, Node::A, sim.clock)
            .await?;
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
    expect_landing_order(&mut body, Node::A, on_a.stop().await?);
    expect_landing_order(&mut body, Node::B, on_b.stop().await?);
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

/// Whether `node` admits miners now: its `/readyz` answers 200 (D-7), or,
/// without a readiness listener, its `/healthz` says `ok`.
async fn node_ready(sim: &Sim, node: Node) -> bool {
    let frontend = sim.frontend(node);
    match frontend.readyz(true).await {
        Ok(Some(status)) => status == 200,
        Ok(None) => matches!(frontend.health().await, Ok((200, ref body)) if body["ok"] == true),
        Err(_) => false,
    }
}

/// The share ids and pool blocks a database holds that `origin` originated
/// (CONTRACT.md §3, `origin_node`).
async fn origin_rows(sim: &Sim, ledger: Node, origin: Node) -> Result<(Vec<String>, Vec<String>)> {
    let pool = sim.pool(ledger).await?;
    let shares: Vec<String> = sqlx::query_scalar(
        "SELECT share_id FROM qbit_share_ledger WHERE origin_node = $1 ORDER BY share_seq",
    )
    .bind(origin.index() as i16)
    .fetch_all(&pool)
    .await
    .context("reading origin_node: the 3.1 stack's migration 027 is required")?;
    let blocks: Vec<String> = sqlx::query_scalar(
        "SELECT block_hash FROM qbit_pool_blocks WHERE origin_node = $1 ORDER BY block_hash",
    )
    .bind(origin.index() as i16)
    .fetch_all(&pool)
    .await?;
    pool.close().await;
    Ok((shares, blocks))
}

/// How long a node restored with the peer unreachable must stay unready to
/// show it does not serve on a rolled-back log (D-8, D-17): several health
/// publications and balancer checks.
pub const UNREADY_HOLD: Duration = Duration::from_secs(20);
/// How long a restored node may take to pull its own rows back and report
/// ready once its peer is reachable.
pub const RECOVERY_BOUND: Duration = Duration::from_secs(120);

/// S6. Node `x` is restored from a base backup older than its newest rows,
/// which only its peer holds. With both nodes writing:
///
/// 1. a plain PostgreSQL restart of `x` with the peer unreachable is not a
///    rollback, so `x` serves again (D-17);
/// 2. a restore onto a new timeline with the peer unreachable is, so `x`
///    stays unready (D-8, D-17);
/// 3. once the peer is reachable, `x` pulls its own missing rows back, and
///    only then reports ready: at its first ready reading it holds every own
///    row the peer holds. Nothing is lost or duplicated afterwards.
async fn s06_restore(sim: &mut Sim, x: Node) -> Result<Body> {
    let mut body = Body::default();
    let y = x.peer();
    sim.load()?.resume();
    steady(sim, 5).await;
    let before = find_block(sim, &mut body, x, "before the backup", Some(&[x])).await?;
    confirm_on_both(sim, &before).await?;
    sim.base_backup(x, "old")?;
    steady(sim, 5).await;
    let after = find_block(sim, &mut body, x, "after the backup", Some(&[x])).await?;
    confirm_on_both(sim, &after).await?;
    sim.wait_synced(CATCH_UP_BOUND).await?;
    // Nothing originates while the restarts and the restore run, so every
    // own row of x is on y when x's database goes back in time.
    sim.load()?.pause();
    sim.load()?.wait_answered(Duration::from_secs(40)).await?;
    sim.wait_synced(CATCH_UP_BOUND).await?;

    let cut = Fault::LinkCut(LinkState::Reset);
    sim.inject(cut).await?;
    sim.frontend_mut(x).kill9()?;
    sim.pg_mut(x).stop_fast()?;
    sim.pg_mut(x).start()?;
    sim.frontend_mut(x).start()?;
    let restarted = sim.frontend(x).wait_ready(Duration::from_secs(120)).await;
    body.expect(
        "a plain restart with the peer unreachable serves (D-17)",
        restarted.is_ok() && node_ready(sim, x).await,
        format!("{restarted:?}"),
    );

    sim.frontend_mut(x).kill9()?;
    let backup = sim.backups.get("old").context("the old backup")?.clone();
    sim.pg_mut(x).restore_from(&backup, true).await?;
    let (timeline, _) = sim.pg[&x].lineage().await?;
    sim.mark(&format!(
        "node {x:?} restored from the old backup onto timeline {timeline}"
    ));
    sim.frontend_mut(x).start()?;
    let held_from = Instant::now();
    let mut served = Vec::new();
    while held_from.elapsed() < UNREADY_HOLD {
        if node_ready(sim, x).await {
            served.push(sim.clock.now_ms());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    body.expect(
        "a restore with the peer unreachable stays unready (D-8, D-17)",
        served.is_empty(),
        format!("ready at {served:?} ms during the {UNREADY_HOLD:?} hold"),
    );

    sim.heal(cut).await?;
    let started = Instant::now();
    while !node_ready(sim, x).await {
        anyhow::ensure!(
            started.elapsed() < RECOVERY_BOUND,
            "node {x:?} did not become ready within {RECOVERY_BOUND:?} of the heal"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (own_on_x, own_blocks_on_x) = origin_rows(sim, x, x).await?;
    let (own_on_y, own_blocks_on_y) = origin_rows(sim, y, x).await?;
    let missing: Vec<&String> = own_on_y
        .iter()
        .filter(|id| !own_on_x.contains(id))
        .collect();
    let missing_blocks: Vec<&String> = own_blocks_on_y
        .iter()
        .filter(|hash| !own_blocks_on_x.contains(hash))
        .collect();
    body.expect(
        "the restored node held every own row of the peer's before it was ready",
        missing.is_empty() && missing_blocks.is_empty(),
        format!(
            "ready {:.1} s after the heal; {} own shares and {} own blocks on the peer, {} and {} of them missing on node {x:?}",
            started.elapsed().as_secs_f64(),
            own_on_y.len(),
            own_blocks_on_y.len(),
            missing.len(),
            missing_blocks.len()
        ),
    );
    sim.load()?.resume();
    steady(sim, 5).await;
    find_block(sim, &mut body, x, "after the restore", None).await?;
    steady(sim, 2).await;
    sim.settle(SETTLE_BOUND).await?;
    expect_dual_health(sim, &mut body).await?;
    Ok(body)
}

/// What a read of `node`'s share ledger gets as the peer role: whether its
/// PostgreSQL answers, and whether the read succeeds, fails or hangs.
async fn peer_read_probe(sim: &Sim, node: Node) -> (bool, String) {
    let url = sim.pg[&node].url(crate::sim::PEER_ROLE, crate::sim::DATABASE);
    let pool = match sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&url)
        .await
    {
        Ok(pool) => pool,
        Err(error) => return (false, format!("could not connect: {error}")),
    };
    let read = tokio::time::timeout(
        Duration::from_secs(10),
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM (SELECT 1 FROM qbit_share_ledger LIMIT 1) s",
        )
        .fetch_one(&pool),
    )
    .await;
    pool.close().await;
    match read {
        Err(_) => (true, "connected; the read hung for 10 s".into()),
        Ok(Err(error)) => (true, format!("connected; the read failed: {error}")),
        Ok(Ok(_)) => (false, "connected; the read succeeded".into()),
    }
}

/// Wait until shares accepted on `node`'s jobs, answered after `after_ms`,
/// appear; how many there were when they did or the bound passed.
async fn wait_accepted_on(sim: &Sim, node: Node, after_ms: u64, limit: Duration) -> Result<usize> {
    let started = Instant::now();
    loop {
        let accepted = sim.load()?.accepted_on(node, after_ms);
        if accepted > 0 || started.elapsed() >= limit {
            return Ok(accepted);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// S6's unreadable-peer restart, with B unreadable: probe B as the peer
/// role, restart A's frontend, and judge whether it serves.
async fn restart_beside_unreadable_peer(sim: &mut Sim, body: &mut Body) -> Result<()> {
    let (unreadable, probe) = peer_read_probe(sim, Node::B).await;
    let fault_at = sim.inject(Fault::FrontendKill9(Node::A)).await?;
    sim.frontend_mut(Node::A).start()?;
    let restarted_at = sim.clock.now_ms();
    let ready = sim
        .frontend(Node::A)
        .wait_ready(UNREADABLE_READY_BOUND)
        .await;
    let accepted = match &ready {
        Ok(_) => wait_accepted_on(sim, Node::A, restarted_at, Duration::from_secs(60)).await?,
        Err(_) => 0,
    };
    body.expect(
        "A restarts and serves while its peer answers but cannot be read (D-8, D-17)",
        unreadable && ready.is_ok() && accepted > 0,
        format!(
            "B as the peer role: {probe}; A {}; shares accepted on A's jobs after its restart: \
             {accepted}",
            match &ready {
                Ok(took) => format!("ready {:.1} s after its restart", took.as_secs_f64()),
                Err(error) => format!("not ready within {UNREADABLE_READY_BOUND:?} ({error:#})"),
            }
        ),
    );
    body.gaps
        .push(report::gap(&sim.load()?.records(), fault_at));
    Ok(())
}

/// S6, the peer answers but cannot be read (the coordinator's review of D1's
/// engine, P1 1). B's PostgreSQL accepts the peer role's connections, but
/// every read of its share ledger hangs (`Locked`) or fails (`Refused`); a
/// probe as the peer role shows it. A's frontend is restarted with its
/// database untouched, so its D-17 evidence matches: D-8 makes the latch
/// true without the peer, and A must report ready and take miners within
/// `UNREADABLE_READY_BOUND`. Then B is made readable again (and restarted if
/// its frontend gave up), and the pair catches up.
async fn s06_peer_unreadable(sim: &mut Sim, how: Unreadable) -> Result<Body> {
    let mut body = Body::default();
    sim.load()?.resume();
    steady(sim, 6).await;
    let a1 = find_block(sim, &mut body, Node::A, "A before its restart", None).await?;
    confirm_on_both(sim, &a1).await?;
    routing_settled(sim).await?;

    let b = sim.pool(Node::B).await?;
    let mut lock = None;
    match how {
        Unreadable::Locked => {
            let mut transaction = b.begin().await?;
            sqlx::query("LOCK TABLE qbit_share_ledger IN ACCESS EXCLUSIVE MODE")
                .execute(&mut *transaction)
                .await?;
            lock = Some(transaction);
        }
        Unreadable::Refused => {
            sqlx::query(&format!(
                "REVOKE SELECT ON qbit_share_ledger FROM {}",
                crate::sim::PEER_ROLE
            ))
            .execute(&b)
            .await?;
        }
    }
    sim.mark(&format!(
        "B answers, but its share ledger cannot be read ({})",
        how.id()
    ));
    // Whatever the restart does, B is made readable again before an error is
    // raised.
    let outcome = restart_beside_unreadable_peer(sim, &mut body).await;
    let restored = async {
        match lock.take() {
            Some(transaction) => transaction.rollback().await?,
            None => {
                sqlx::query(&format!(
                    "GRANT SELECT ON qbit_share_ledger TO {}",
                    crate::sim::PEER_ROLE
                ))
                .execute(&b)
                .await?;
            }
        }
        anyhow::Ok(())
    }
    .await;
    b.close().await;
    // The restart's own failure is the root cause; a failure to restore B
    // is added to it, never put in its place.
    match (outcome, restored) {
        (Err(error), Err(restore)) => {
            return Err(error.context(format!("and making B readable again failed: {restore:#}")))
        }
        (Err(error), Ok(())) | (Ok(()), Err(error)) => return Err(error),
        (Ok(()), Ok(())) => {}
    }
    sim.mark("B readable again");
    for node in Node::BOTH {
        if !sim.frontend(node).running() {
            sim.frontend_mut(node).start()?;
            sim.frontend(node).wait_ready(RECOVERY_BOUND).await?;
            sim.mark(&format!(
                "node {node:?}'s frontend restarted after it gave up"
            ));
        }
    }
    sim.balancer
        .wait_state("a", true, Duration::from_secs(60))
        .await?;
    sim.wait_synced(CATCH_UP_BOUND).await?;
    steady(sim, 4).await;
    let a2 = find_block(sim, &mut body, Node::A, "A once B is readable again", None).await?;
    confirm_on_both(sim, &a2).await?;
    sim.settle(SETTLE_BOUND).await?;
    expect_dual_health(sim, &mut body).await?;
    Ok(body)
}

/// S7. Node `x`'s host dies (frontend and PostgreSQL at once) with both
/// nodes writing, and its disk is replaced: the database is rebuilt from a
/// physical copy of the peer, promoted and re-personalised (D-16), and the
/// node starts. What x had acknowledged but the peer had not pulled when it
/// died is its unsynced tail: measured, reported and excused, and nothing
/// else may be missing.
async fn s07_disk_replaced(sim: &mut Sim, x: Node) -> Result<Body> {
    let mut body = Body::default();
    let y = x.peer();
    sim.load()?.resume();
    steady(sim, 6).await;
    let early = find_block(sim, &mut body, x, "before the disk is lost", Some(&[x])).await?;
    confirm_on_both(sim, &early).await?;
    steady(sim, 4).await;

    let fault_at = sim.inject(Fault::FrontendKill9(x)).await?;
    sim.inject(Fault::PostgresKill(x)).await?;
    tokio::time::sleep(Duration::from_millis(2 * sim.config.sync_interval_ms + 500)).await;
    let held = crate::measure::credited_headers(&sim.pool(y).await?).await?;
    steady(sim, 6).await;
    find_block(sim, &mut body, y, "while x is gone", Some(&[y])).await?;

    sim.pg_mut(x).wipe()?;
    sim.mark(&format!("node {x:?}'s disk replaced"));
    let link = sim
        .links
        .get(&format!("peer-{}-to-{}", x.label(), y.label()))?
        .port();
    let user = sim.pg[&y].superuser().to_owned();
    sim.pg_mut(x).rebuild_from_peer(&user, link).await?;
    let repersonalise = sim
        .frontend(x)
        .tool(
            &[
                "node-identity",
                "repersonalise",
                "--index",
                &x.index().to_string(),
            ],
            Duration::from_secs(120),
        )
        .await?;
    body.expect(
        "the rebuilt database is re-personalised (D-16)",
        repersonalise.success,
        format!(
            "exit {:?}: {} {}",
            repersonalise.code,
            repersonalise.stdout.trim(),
            repersonalise.stderr.trim()
        ),
    );
    sim.frontend_mut(x).start()?;
    sim.frontend(x).wait_ready(RECOVERY_BOUND).await?;
    sim.mark(&format!("node {x:?} rebuilt from its peer and serving"));

    let records = sim.load()?.records();
    let tail = crate::measure::tail(&records, x, fault_at, &held);
    body.expect(
        "only the dead node's unsynced tail is lost (measured)",
        true,
        format!(
            "{} shares node {x:?} acknowledged were not on its peer when it died (answered {:?}..{:?} ms); they are excused, and any other missing acknowledged share fails invariant 4",
            tail.count, tail.first_answered_ms, tail.last_answered_ms
        ),
    );
    body.options.excused_missing = crate::measure::excuse(
        &tail,
        "S7: the replaced disk's unsynced tail when its node died",
    );
    sim.balancer
        .wait_state(x.label(), true, Duration::from_secs(60))
        .await?;
    steady(sim, 4).await;
    find_block(sim, &mut body, x, "after the rebuild", None).await?;
    steady(sim, 2).await;
    sim.settle(SETTLE_BOUND).await?;
    expect_dual_health(sim, &mut body).await?;
    Ok(body)
}

/// What a ledger holds that a cutover must not change: balances, the
/// integrity and divergence reports, and a digest of every landing and share
/// table over its immutable columns.
async fn ledger_digest(pool: &sqlx::PgPool) -> Result<serde_json::Value> {
    let mut digest = serde_json::Map::new();
    let balances: Vec<(String, String)> = sqlx::query_as(
        "SELECT encode(p2mr_program, 'hex'), balance_sats::text \
         FROM qbit_current_carry_forward_balances() ORDER BY 1",
    )
    .fetch_all(pool)
    .await?;
    digest.insert("balances".into(), json!(balances));
    let integrity: serde_json::Value =
        sqlx::query_scalar("SELECT qbit_carry_forward_integrity_report()")
            .fetch_one(pool)
            .await?;
    digest.insert(
        "integrity".into(),
        json!({"mismatch_count": integrity["mismatch_count"], "current_drift_count": integrity["current_drift_count"]}),
    );
    for (name, sql) in [
        ("shares", "SELECT count(*)::text || ':' || coalesce(md5(string_agg(concat_ws('|', share_seq, share_id, miner_id, share_difficulty, floor(extract(epoch FROM accepted_at) * 1000)), ',' ORDER BY share_seq)), '') FROM qbit_share_ledger"),
        ("share_hashes", "SELECT count(*)::text || ':' || coalesce(md5(string_agg(header_hash || share_id, ',' ORDER BY header_hash)), '') FROM qbit_prism_share_hashes"),
        ("blocks", "SELECT count(*)::text || ':' || coalesce(md5(string_agg(concat_ws('|', block_hash, block_height, coinbase_txid, payout_manifest_sha256, as_issued_audit_sha256), ',' ORDER BY block_hash)), '') FROM qbit_pool_blocks"),
        ("audits", "SELECT count(*)::text || ':' || coalesce(md5(string_agg(block_hash || audit_bundle_sha256, ',' ORDER BY block_hash)), '') FROM qbit_pool_audit_bundles"),
        ("snapshots", "SELECT count(*)::text || ':' || coalesce(md5(string_agg(concat_ws('|', snapshot_sha256, first_share_seq, last_share_seq, share_count), ',' ORDER BY snapshot_sha256)), '') FROM qbit_prism_audit_snapshots"),
        ("payouts", "SELECT count(*)::text || ':' || coalesce(md5(string_agg(concat_ws('|', block_hash, miner_id, encode(p2mr_program, 'hex'), onchain_amount_sats, carry_forward_balance_sats, action), ',' ORDER BY block_hash, miner_id, p2mr_program)), '') FROM qbit_pool_payout_entries"),
        ("carry", "SELECT count(*)::text || ':' || coalesce(md5(string_agg(concat_ws('|', block_hash, encode(p2mr_program, 'hex'), gross_amount_sats, prior_balance_sats, onchain_amount_sats, carry_forward_balance_sats), ',' ORDER BY block_hash, p2mr_program, miner_id)), '') FROM qbit_payout_carry_forward"),
        ("fanouts", "SELECT count(*)::text || ':' || coalesce(md5(string_agg(fanout_txid || md5(fanout_tx_hex), ',' ORDER BY fanout_txid)), '') FROM qbit_ctv_fanout_artifacts"),
    ] {
        let value: String = sqlx::query_scalar(sql).fetch_one(pool).await?;
        digest.insert(name.into(), json!(value));
    }
    Ok(serde_json::Value::Object(digest))
}

/// CONTRACT.md D-12: once a database is a dual-writer node's, a
/// single-writer start on it is refused unless the downgrade flag is set.
/// The node's frontend is stopped, started once in single-writer mode (it
/// must exit, refusing), then started again as it was.
async fn expect_single_writer_refused(sim: &mut Sim, node: Node, body: &mut Body) -> Result<()> {
    sim.frontend_mut(node).stop(Duration::from_secs(30))?;
    let mut spec = sim.frontend(node).spec.clone();
    spec.dual = None;
    spec.readiness = None;
    let probe = crate::frontend::Frontend::new(spec, &sim.logs)?;
    let attempt = probe.tool(&["run"], Duration::from_secs(45)).await;
    let refused = match &attempt {
        Ok(run) => !run.success,
        // Still serving after 45 s: it did not refuse.
        Err(_) => false,
    };
    body.expect(
        "a single-writer start on a dual-mode database is refused (D-12)",
        refused,
        match attempt {
            Ok(run) => format!(
                "exit {:?}: {}",
                run.code,
                run.stderr.trim().lines().last().unwrap_or("")
            ),
            Err(error) => format!("{error:#}"),
        },
    );
    sim.frontend_mut(node).start()?;
    sim.frontend(node)
        .wait_ready(Duration::from_secs(120))
        .await?;
    Ok(())
}

/// S9. The 3.0 pair (one writer, B its standby) mines a history with carry
/// and blocks on both frontends, then is cut over to dual mode as the
/// playbook does it. Both nodes' ledgers then equal the 3.0 writer's (every
/// balance, report, landing and share), every pre-cutover row is node 0, a
/// single-writer start on either database is refused (D-12), the owner's
/// first block pays from the 3.0 balances, and the pair mines in dual mode
/// with every invariant holding across the cutover.
async fn s09_migration(sim: &mut Sim) -> Result<Body> {
    let mut body = Body::default();
    sim.load()?.resume();
    steady(sim, 8).await;
    for (node, note) in [
        (Node::A, "3.0 history on A"),
        (Node::B, "3.0 history on B"),
        (Node::A, "3.0 history on A again"),
    ] {
        find_block(sim, &mut body, node, note, None).await?;
        steady(sim, 3).await;
    }
    sim.load()?.pause();
    sim.settle(SETTLE_BOUND).await?;
    let before = ledger_digest(&sim.pool(Node::A).await?).await?;
    let cutover = sim.cutover_to_dual().await?;
    for node in Node::BOTH {
        let after = ledger_digest(&sim.pool(node).await?).await?;
        body.expect(
            &format!("node {node:?}'s ledger is the 3.0 writer's after the cutover"),
            after == before,
            if after == before {
                format!(
                    "{} balances; {}",
                    before["balances"].as_array().map_or(0, Vec::len),
                    before["blocks"]
                )
            } else {
                format!("before {before}, after {after}")
            },
        );
        let pool = sim.pool(node).await?;
        let mut foreign = 0i64;
        for table in [
            "qbit_share_ledger",
            "qbit_prism_share_hashes",
            "qbit_pool_blocks",
            "qbit_pool_audit_bundles",
            "qbit_prism_audit_snapshots",
            "qbit_pool_payout_entries",
            "qbit_payout_carry_forward",
            "qbit_ctv_fanout_sets",
            "qbit_ctv_fanout_artifacts",
        ] {
            foreign += sqlx::query_scalar::<_, i64>(&format!(
                "SELECT count(*) FROM {table} WHERE origin_node <> 0"
            ))
            .fetch_one(&pool)
            .await?;
        }
        body.expect(
            &format!("every pre-cutover row on node {node:?} is node 0's"),
            foreign == 0,
            format!("{foreign} rows of another origin"),
        );
        pool.close().await;
    }
    expect_single_writer_refused(sim, Node::B, &mut body).await?;
    sim.load()?.resume();
    steady(sim, 5).await;
    let first = find_block(
        sim,
        &mut body,
        Node::A,
        "the owner's first block after the cutover",
        None,
    )
    .await?;
    confirm_on_both(sim, &first).await?;
    expect_owner_priors_are_truth(sim, &mut body, &first).await?;
    steady(sim, 3).await;
    let b = find_block(
        sim,
        &mut body,
        Node::B,
        "B's first block in dual mode",
        Some(&[Node::B]),
    )
    .await?;
    confirm_on_both(sim, &b).await?;
    steady(sim, 3).await;
    sim.settle(SETTLE_BOUND).await?;
    body.options.ownership = vec![(cutover + 1, Some(Node::A))];
    expect_dual_health(sim, &mut body).await?;
    Ok(body)
}

/// How long the survivor may take to adopt a dead node's block that the
/// chain holds (D-10, D-11): its reconciler's next look at the chain, the
/// synced prepared record, a landing.
pub const ADOPTION_BOUND: Duration = Duration::from_secs(90);

/// S8. Node A finds a block and dies at that instant, with its
/// `submitblock` held at the gate in front of its `qbitd`:
///
/// - [`DeathAtFind::Lost`]: the call never reaches the node. The block never
///   reaches the chain; whatever A's restart does with its candidate, it is
///   never paid twice.
/// - [`DeathAtFind::Accepted`]: the node accepts the block and A dies before
///   it hears. B adopts the block from A's synced prepared record (D-10,
///   D-11, with D-19's wait on, or measured with it off); when A returns with
///   its disk, its own landing and B's adoption leave one set of landing
///   rows per node, identical, and the block's deltas count once.
async fn s08_block_at_death(sim: &mut Sim, case: DeathAtFind) -> Result<Body> {
    let mut body = Body::default();
    sim.load()?.resume();
    steady(sim, 5).await;
    let warm = find_block(sim, &mut body, Node::A, "A before it dies", None).await?;
    confirm_on_both(sim, &warm).await?;
    routing_settled(sim).await?;
    sim.wait_node_settled(Node::A, NODE_SETTLE_BOUND).await?;
    sim.load()?
        .refresh_finder(Node::A, Duration::from_secs(30))
        .await?;
    let gate = sim.gates.get(&Node::A).context("S8 runs with RPC gates")?;
    gate.set_mode(match case {
        DeathAtFind::Lost => crate::rpc_gate::GateMode::Hold,
        DeathAtFind::Accepted { .. } => crate::rpc_gate::GateMode::Withhold,
    });
    let seen = gate.submissions();
    let record = sim.load()?.find_block(Node::A, LANDING_BOUND).await?;
    anyhow::ensure!(
        record.accepted(),
        "A refused its own block: {:?}",
        record.reason
    );
    let hash = gate.next_submission(seen, Duration::from_secs(60)).await?;
    if matches!(case, DeathAtFind::Accepted { .. }) {
        gate.node_answered(&hash, Duration::from_secs(30)).await?;
    }
    let fault_at = sim.inject(Fault::FrontendKill9(Node::A)).await?;
    sim.gates[&Node::A].discard_held();
    sim.mark(&format!("A died at the instant it found {hash}"));
    sim.balancer.wait_state("a", false, MARK_DOWN_BOUND).await?;
    steady(sim, 6).await;
    let on_chain = sim
        .chain
        .c
        .rpc("getblockheader", json!([hash]))
        .await
        .is_ok();
    match case {
        DeathAtFind::Lost => body.expect(
            "the lost block never reached the chain",
            !on_chain,
            format!("{hash} known to the network: {on_chain}"),
        ),
        DeathAtFind::Accepted { wait } => {
            let adopted = sim.wait_confirmed(&hash, &[Node::B], ADOPTION_BOUND).await;
            body.expect(
                &format!(
                    "the survivor adopts the dying node's block ({} wait)",
                    if wait { "with the" } else { "without the" }
                ),
                adopted.is_ok() || !wait,
                match &adopted {
                    Ok(took) => format!("confirmed on B {:.1} s after A died", took.as_secs_f64()),
                    Err(error) => format!("not adoptable: {error:#}"),
                },
            );
        }
    }
    find_block(
        sim,
        &mut body,
        Node::B,
        "B while A is down",
        Some(&[Node::B]),
    )
    .await?;
    sim.heal(Fault::FrontendKill9(Node::A)).await?;
    sim.balancer
        .wait_state("a", true, Duration::from_secs(60))
        .await?;
    if on_chain {
        sim.wait_confirmed(&hash, &Node::BOTH, CATCH_UP_BOUND)
            .await?;
        for node in Node::BOTH {
            let pool = sim.pool(node).await?;
            let (blocks, carry): (i64, i64) = sqlx::query_as(
                "SELECT (SELECT count(*) FROM qbit_pool_blocks WHERE block_hash = $1), \
                        (SELECT count(*) FROM qbit_payout_carry_forward WHERE block_hash = $1)",
            )
            .bind(&hash)
            .fetch_one(&pool)
            .await?;
            pool.close().await;
            body.expect(
                &format!("node {node:?} holds one landing of the block (D-10)"),
                blocks == 1,
                format!("{blocks} block rows, {carry} carry rows"),
            );
        }
    }
    let records = sim.load()?.records();
    body.gaps.push(report::gap(&records, fault_at));
    steady(sim, 3).await;
    sim.settle(SETTLE_BOUND).await?;
    expect_dual_health(sim, &mut body).await?;
    Ok(body)
}

/// S11. The carry-owner transfer drill (D3's operator CLI, status/D3.md).
/// With B's frontend (and so its sync) stopped, the owner A lands an own
/// block B lacks and releases ownership; six blocks later B's
/// `carry-owner transfer` is refused, its chain scan naming that block.
/// Once B is back and has landed it, the transfer succeeds, the operator
/// swaps `PRISM_CARRY_OWNER` and restarts both, and from then on B pays
/// carry and A builds carry-free work.
async fn s11_carry_owner_transfer(sim: &mut Sim) -> Result<Body> {
    let mut body = Body::default();
    let tool_bound = Duration::from_secs(180);
    sim.load()?.resume();
    steady(sim, 6).await;
    let early = find_block(sim, &mut body, Node::A, "carry accrues under A", None).await?;
    confirm_on_both(sim, &early).await?;
    steady(sim, 3).await;

    sim.frontend_mut(Node::B).stop(Duration::from_secs(30))?;
    sim.mark("B stopped: its ledger stops learning A's landings");
    let unknown = find_block(
        sim,
        &mut body,
        Node::A,
        "an own block B lacks",
        Some(&[Node::A]),
    )
    .await?;
    let release = sim
        .frontend(Node::A)
        .tool(
            &[
                "carry-owner",
                "release",
                "--reason",
                "S11 drill",
                "--confirm",
            ],
            tool_bound,
        )
        .await?;
    body.expect(
        "the owner releases ownership",
        release.success,
        format!("exit {:?}: {}", release.code, release.stdout.trim()),
    );
    sim.chain.mint(7).await?;
    let refused = sim
        .frontend(Node::B)
        .tool(
            &[
                "carry-owner",
                "transfer",
                "--reason",
                "S11 drill",
                "--confirm",
            ],
            tool_bound,
        )
        .await?;
    body.expect(
        "the transfer is refused while an own block is unknown to the new owner",
        !refused.success
            && refused.stdout.contains("chain_scan")
            && refused.stdout.contains(&unknown),
        format!("exit {:?}: {}", refused.code, refused.stdout.trim()),
    );

    sim.frontend_mut(Node::B).start()?;
    sim.frontend(Node::B)
        .wait_ready(Duration::from_secs(120))
        .await?;
    sim.wait_confirmed(&unknown, &[Node::B], CATCH_UP_BOUND)
        .await?;
    let transferred = sim
        .frontend(Node::B)
        .tool(
            &[
                "carry-owner",
                "transfer",
                "--reason",
                "S11 drill",
                "--confirm",
            ],
            tool_bound,
        )
        .await?;
    body.expect(
        "the transfer succeeds once the block is landed",
        transferred.success,
        format!("exit {:?}: {}", transferred.code, transferred.stdout.trim()),
    );
    let (transfer_height, _, _) = sim.chain.c.tip().await?;

    for (node, owner) in [(Node::A, false), (Node::B, true)] {
        let frontend = sim.frontend_mut(node);
        frontend.stop(Duration::from_secs(30))?;
        if let Some(dual) = frontend.spec.dual.as_mut() {
            dual.carry_owner = owner;
        }
    }
    for node in [Node::A, Node::B] {
        sim.frontend_mut(node).start()?;
    }
    for node in Node::BOTH {
        sim.frontend(node)
            .wait_ready(Duration::from_secs(120))
            .await?;
    }
    sim.mark(&format!("ownership moved to B at height {transfer_height}"));
    routing_settled(sim).await?;
    steady(sim, 4).await;
    let b_block = find_block(sim, &mut body, Node::B, "B as the new owner", None).await?;
    confirm_on_both(sim, &b_block).await?;
    expect_priors_are_truth(sim, &mut body, Node::B, &b_block).await?;
    let a_block = find_block(sim, &mut body, Node::A, "A as the non-owner", None).await?;
    confirm_on_both(sim, &a_block).await?;
    steady(sim, 2).await;
    sim.settle(SETTLE_BOUND).await?;
    body.options.ownership = vec![(0, Some(Node::A)), (transfer_height + 1, Some(Node::B))];
    Ok(body)
}

/// CONTRACT.md §4.4 and D-14's negative control: a row that arrives after a
/// window was built, and is eligible for it, must be caught. A copy of one of
/// A's shares is planted above A's newest recorded window with a stamp at
/// that window's anchor (a late row that would have joined it), and the
/// windows check must name that window. The planted row breaks A's ledger
/// for anything after this, so this runs last.
async fn late_row_control(sim: &Sim, body: &mut Body) -> Result<()> {
    let pool = sim.pool(Node::A).await?;
    let windows = invariants::recorded_windows(&pool).await?;
    let window = windows
        .iter()
        .max_by_key(|window| window.last)
        .context("A recorded no window to plant a late row against")?
        .clone();
    let planted: i64 = sqlx::query_scalar(
        "INSERT INTO qbit_share_ledger (share_seq, share_id, miner_id, payout_order_key, \
             p2mr_program, share_difficulty, network_difficulty, template_height, job_id, \
             job_issued_at, ntime, accepted_at, accepted, writer_id, writer_epoch) \
         SELECT (SELECT max(share_seq) + 1 FROM qbit_share_ledger), 'late-row-control:' || share_id, \
                miner_id, payout_order_key, p2mr_program, share_difficulty, network_difficulty, \
                template_height, job_id, to_timestamp($1::double precision / 1000), ntime, \
                to_timestamp($1::double precision / 1000), true, writer_id, writer_epoch \
         FROM qbit_share_ledger WHERE accepted AND share_seq = $2 \
         RETURNING share_seq",
    )
    .bind(window.anchor_ms)
    .bind(window.last)
    .fetch_one(&pool)
    .await
    .context("planting the late row")?;
    sim.mark(&format!(
        "planted a late row at share_seq {planted}, eligible for window {}",
        window.snapshot
    ));
    let pools = std::collections::BTreeMap::from([(Node::A, pool)]);
    let check = invariants::windows_unchanged(&pools).await?;
    let named = check.problems.iter().any(|problem| {
        problem.contains(&window.snapshot) && problem.contains(&planted.to_string())
    });
    body.expect(
        "checker: a late row eligible for a built window is caught (D-14)",
        check.status == Status::Fail && named,
        format!("{:?}: {:?}", check.status, check.problems),
    );
    for pool in pools.values() {
        pool.close().await;
    }
    Ok(())
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
