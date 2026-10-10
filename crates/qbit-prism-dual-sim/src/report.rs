//! What a scenario reports: the miner-visible gap, the shares lost, every
//! invariant's result and the scenario's own expectations, as
//! `report.json` and `report.md` in its directory, with the logs beside
//! them.

use crate::{
    balancer::BalancerReport,
    frontend::Node,
    invariants::{InvariantReport, Status},
    load::ShareRecord,
    relay::RelayStats,
    sim::TimelineEvent,
};
use anyhow::Result;
use serde::Serialize;
use serde_json::Value;
use std::{collections::BTreeMap, fmt::Write as _, path::Path};

/// What miners saw around one moment (a fault): when shares stopped being
/// accepted and when they started again.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct GapReport {
    pub fault_at_ms: u64,
    /// The last accepted answer at or before the fault.
    pub last_accept_before_ms: Option<u64>,
    /// The first accepted answer after the fault.
    pub first_accept_after_ms: Option<u64>,
    /// `first_accept_after - last_accept_before`: the stretch with no share
    /// accepted anywhere that spans the fault. A later stall is not in it:
    /// [`longest_gap`] measures a whole window.
    pub gap_ms: Option<u64>,
    /// `first_accept_after - fault`.
    pub fault_to_first_accept_ms: Option<u64>,
    /// Per session that had an accepted share before the fault: the time
    /// from the fault to its next accepted share.
    pub sessions: usize,
    pub sessions_returned: usize,
    pub session_p50_ms: Option<u64>,
    pub session_p95_ms: Option<u64>,
    pub session_max_ms: Option<u64>,
}

/// The gap around `fault_at_ms`, from load sessions' answers only (block
/// finders excluded).
pub fn gap(records: &[ShareRecord], fault_at_ms: u64) -> GapReport {
    let accepted: Vec<(usize, u64)> = records
        .iter()
        .filter(|record| record.accepted() && !record.scheduled_block)
        .filter_map(|record| Some((record.session, record.answered_ms?)))
        .collect();
    let last_before = accepted
        .iter()
        .map(|(_, at)| *at)
        .filter(|at| *at <= fault_at_ms)
        .max();
    let first_after = accepted
        .iter()
        .map(|(_, at)| *at)
        .filter(|at| *at > fault_at_ms)
        .min();
    let mut by_session: BTreeMap<usize, (bool, Option<u64>)> = BTreeMap::new();
    for (session, at) in &accepted {
        let entry = by_session.entry(*session).or_insert((false, None));
        if *at <= fault_at_ms {
            entry.0 = true;
        } else if entry.1.is_none_or(|first| *at < first) {
            entry.1 = Some(*at);
        }
    }
    let mut waits: Vec<u64> = Vec::new();
    let mut sessions = 0;
    for (before, after) in by_session.values() {
        if !before {
            continue;
        }
        sessions += 1;
        if let Some(after) = after {
            waits.push(after - fault_at_ms);
        }
    }
    waits.sort_unstable();
    let percentile = |p: f64| -> Option<u64> {
        if waits.is_empty() {
            return None;
        }
        let rank = ((p * waits.len() as f64).ceil() as usize).clamp(1, waits.len());
        Some(waits[rank - 1])
    };
    GapReport {
        fault_at_ms,
        last_accept_before_ms: last_before,
        first_accept_after_ms: first_after,
        gap_ms: match (last_before, first_after) {
            (Some(before), Some(after)) => Some(after - before),
            _ => None,
        },
        fault_to_first_accept_ms: first_after.map(|after| after - fault_at_ms),
        sessions,
        sessions_returned: waits.len(),
        session_p50_ms: percentile(0.50),
        session_p95_ms: percentile(0.95),
        session_max_ms: waits.last().copied(),
    }
}

/// The longest stretch from `from_ms` to `to_ms` with no share accepted
/// anywhere (load sessions only, block finders excluded): between the
/// window's start, every accepted answer in it, and its end. The whole
/// window when nothing was accepted in it. `to_ms` must be a moment the load
/// was still running, or the stretch to it counts as a stall.
pub fn longest_gap(records: &[ShareRecord], from_ms: u64, to_ms: u64) -> u64 {
    let mut moments: Vec<u64> = records
        .iter()
        .filter(|record| record.accepted() && !record.scheduled_block)
        .filter_map(|record| record.answered_ms)
        .filter(|at| (from_ms..=to_ms).contains(at))
        .collect();
    moments.push(from_ms);
    moments.push(to_ms.max(from_ms));
    moments.sort_unstable();
    moments
        .windows(2)
        .map(|pair| pair[1] - pair[0])
        .max()
        .unwrap_or(0)
}

/// How the miners of a dead node moved to the survivor: every session with
/// a share accepted at or before the fault, and when it first had a share
/// accepted on the survivor's work after the fault, up to `until_ms` (the
/// heal, or now if earlier). A session with none by then is stranded: a
/// failover is every miner's, not the first one's.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct FailoverReport {
    pub fault_at_ms: u64,
    pub survivor: Option<Node>,
    pub until_ms: u64,
    pub sessions: usize,
    /// The sessions with no share accepted on the survivor's work by
    /// `until_ms`.
    pub stranded: Vec<usize>,
    /// From the fault to each moved session's first share accepted on the
    /// survivor's work.
    pub p50_ms: Option<u64>,
    pub p95_ms: Option<u64>,
    pub max_ms: Option<u64>,
}

impl FailoverReport {
    /// Every session moved, the slowest within `bound_ms`.
    pub fn within(&self, bound_ms: u64) -> bool {
        self.sessions > 0
            && self.stranded.is_empty()
            && self.max_ms.is_some_and(|ms| ms <= bound_ms)
    }
}

pub fn failover(
    records: &[ShareRecord],
    fault_at_ms: u64,
    survivor: Node,
    until_ms: u64,
) -> FailoverReport {
    let mut by_session: BTreeMap<usize, (bool, Option<u64>)> = BTreeMap::new();
    for record in records
        .iter()
        .filter(|record| record.accepted() && !record.scheduled_block)
    {
        let Some(at) = record.answered_ms else {
            continue;
        };
        let entry = by_session.entry(record.session).or_insert((false, None));
        if at <= fault_at_ms {
            entry.0 = true;
        } else if at <= until_ms
            && record.issuer == Some(survivor)
            && entry.1.is_none_or(|first| at < first)
        {
            entry.1 = Some(at);
        }
    }
    let mut waits = Vec::new();
    let mut stranded = Vec::new();
    let mut sessions = 0;
    for (session, (before, moved)) in &by_session {
        if !before {
            continue;
        }
        sessions += 1;
        match moved {
            Some(at) => waits.push(at - fault_at_ms),
            None => stranded.push(*session),
        }
    }
    waits.sort_unstable();
    let percentile = |p: f64| -> Option<u64> {
        if waits.is_empty() {
            return None;
        }
        let rank = ((p * waits.len() as f64).ceil() as usize).clamp(1, waits.len());
        Some(waits[rank - 1])
    };
    FailoverReport {
        fault_at_ms,
        survivor: Some(survivor),
        until_ms,
        sessions,
        stranded,
        p50_ms: percentile(0.50),
        p95_ms: percentile(0.95),
        max_ms: waits.last().copied(),
    }
}

/// Every submit, by outcome and reason, and by the node that issued the
/// job.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ShareSummary {
    /// Shares submitted (an outage's offers with no session to take them are
    /// `unplaced`, not here).
    pub offered: usize,
    pub accepted: usize,
    pub accepted_by_issuer: BTreeMap<String, usize>,
    pub rejected: BTreeMap<String, usize>,
    pub no_response: BTreeMap<String, usize>,
    /// Acknowledged shares missing from a checked database, not excused.
    pub lost: usize,
    /// Acknowledged shares missing and excused as a documented tail.
    pub excused_tail: usize,
    /// Offers the load made while no session could take one (every session
    /// down or busy, as during a failover): never submitted.
    pub unplaced: u64,
}

pub fn summarize(records: &[ShareRecord]) -> ShareSummary {
    let mut summary = ShareSummary::default();
    for record in records.iter().filter(|record| !record.scheduled_block) {
        summary.offered += 1;
        let reason = record.reason.clone().unwrap_or_default();
        match record.outcome.as_str() {
            "accepted" => {
                summary.accepted += 1;
                let issuer = record
                    .issuer
                    .map_or_else(|| "unknown".to_owned(), |node| node.label().to_owned());
                *summary.accepted_by_issuer.entry(issuer).or_default() += 1;
            }
            "rejected" => *summary.rejected.entry(reason).or_default() += 1,
            _ => *summary.no_response.entry(reason).or_default() += 1,
        }
    }
    summary
}

/// One block a scenario found on purpose.
#[derive(Clone, Debug, Serialize)]
pub struct FoundBlock {
    pub node: Node,
    pub hash: String,
    pub found_at_ms: u64,
    pub note: String,
}

/// A scenario-specific assertion.
#[derive(Clone, Debug, Serialize)]
pub struct Expectation {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ScenarioReport {
    pub scenario: String,
    pub title: String,
    pub passed: bool,
    pub duration_ms: u64,
    pub parameters: Value,
    pub timeline: Vec<TimelineEvent>,
    pub gaps: Vec<GapReport>,
    pub shares: ShareSummary,
    pub blocks: Vec<FoundBlock>,
    pub expectations: Vec<Expectation>,
    pub invariants: Option<InvariantReport>,
    pub balancer: Option<BalancerReport>,
    pub links: Vec<RelayStats>,
    /// Set when the scenario failed before its checks: the error chain.
    pub error: Option<String>,
}

impl ScenarioReport {
    /// Whether every expectation held and, when the scenario expects the
    /// invariants to hold, every check passed.
    pub fn verdict(expectations: &[Expectation], invariants: Option<&InvariantReport>) -> bool {
        expectations.iter().all(|e| e.passed) && invariants.is_none_or(InvariantReport::passed)
    }

    /// `report.json` and `report.md` in `dir`.
    pub fn write(&self, dir: &Path) -> Result<()> {
        std::fs::create_dir_all(dir)?;
        std::fs::write(dir.join("report.json"), serde_json::to_vec_pretty(self)?)?;
        std::fs::write(dir.join("report.md"), self.markdown())?;
        Ok(())
    }

    pub fn markdown(&self) -> String {
        let mut out = String::new();
        let verdict = if self.passed { "PASS" } else { "FAIL" };
        let _ = writeln!(out, "# {}: {} ({verdict})\n", self.scenario, self.title);
        let _ = writeln!(
            out,
            "Duration: {:.1} s.\n",
            self.duration_ms as f64 / 1000.0
        );
        if let Some(error) = &self.error {
            let _ = writeln!(out, "**Error:** `{}`\n", error.replace('`', "'"));
        }
        let _ = writeln!(out, "## Miner-visible gap\n");
        if self.gaps.is_empty() {
            let _ = writeln!(out, "No fault in this scenario.\n");
        }
        for gap in &self.gaps {
            let _ = writeln!(
                out,
                "- Fault at {} ms: gap {} (last accept {} ms, first after {} ms); first accept {} after the fault; {} of {} sessions returned, p50 {}, p95 {}, max {}.",
                gap.fault_at_ms,
                ms(gap.gap_ms),
                opt(gap.last_accept_before_ms),
                opt(gap.first_accept_after_ms),
                ms(gap.fault_to_first_accept_ms),
                gap.sessions_returned,
                gap.sessions,
                ms(gap.session_p50_ms),
                ms(gap.session_p95_ms),
                ms(gap.session_max_ms),
            );
        }
        let _ = writeln!(out, "\n## Shares\n");
        let shares = &self.shares;
        let _ = writeln!(
            out,
            "Offered {} (and {} offers no session could take), accepted {} (by issuing node: {:?}), rejected {:?}, no answer {:?}. Lost (acknowledged, missing, not excused): **{}**. Excused documented tail: {}.\n",
            shares.offered,
            shares.unplaced,
            shares.accepted,
            shares.accepted_by_issuer,
            shares.rejected,
            shares.no_response,
            shares.lost,
            shares.excused_tail
        );
        let _ = writeln!(out, "## Blocks found\n");
        for block in &self.blocks {
            let _ = writeln!(
                out,
                "- {} on node {:?} at {} ms: {}",
                block.hash, block.node, block.found_at_ms, block.note
            );
        }
        let _ = writeln!(out, "\n## Expectations\n");
        let _ = writeln!(
            out,
            "| Expectation | Result | Detail |\n| --- | --- | --- |"
        );
        for expectation in &self.expectations {
            let _ = writeln!(
                out,
                "| {} | {} | {} |",
                expectation.name,
                if expectation.passed {
                    "pass"
                } else {
                    "**FAIL**"
                },
                expectation.detail.replace('|', "\\|")
            );
        }
        if let Some(invariants) = &self.invariants {
            let _ = writeln!(out, "\n## Invariants (CONTRACT.md §4)\n");
            let _ = writeln!(out, "| Check | Result | Summary |\n| --- | --- | --- |");
            for check in &invariants.checks {
                let status = match check.status {
                    Status::Pass => "pass".to_owned(),
                    Status::Skip => "skip".to_owned(),
                    Status::Fail => format!("**FAIL** ({})", check.problem_count),
                };
                let _ = writeln!(
                    out,
                    "| {} | {status} | {} |",
                    check.id,
                    check.summary.replace('|', "\\|")
                );
            }
            for check in invariants
                .checks
                .iter()
                .filter(|c| c.status == Status::Fail)
            {
                let _ = writeln!(out, "\n### {} problems\n", check.id);
                for problem in &check.problems {
                    let _ = writeln!(out, "- {}", problem.replace('`', "'"));
                }
            }
        }
        let _ = writeln!(out, "\n## Timeline\n");
        for event in &self.timeline {
            let _ = writeln!(out, "- {} ms: {}", event.at_ms, event.event);
        }
        out
    }
}

fn opt(value: Option<u64>) -> String {
    value.map_or_else(|| "none".into(), |v| v.to_string())
}

fn ms(value: Option<u64>) -> String {
    value.map_or_else(|| "none".into(), |v| format!("{v} ms"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(session: usize, answered: u64, outcome: &str) -> ShareRecord {
        ShareRecord {
            share_id: format!("u.w:{session}{answered}"),
            session,
            account: "a".into(),
            job_id: "dual-sim-a-x".into(),
            issuer: Some(Node::A),
            outcome: outcome.into(),
            reason: (outcome != "accepted").then(|| "stale-job".into()),
            sent_ms: answered.saturating_sub(5),
            answered_ms: Some(answered),
            scheduled_block: false,
        }
    }

    fn on(session: usize, answered: u64, issuer: Node) -> ShareRecord {
        ShareRecord {
            issuer: Some(issuer),
            ..record(session, answered, "accepted")
        }
    }

    #[test]
    fn the_longest_gap_finds_a_stall_after_the_first_accept_past_the_fault() {
        // The fault at 1 000: a share at 1 040 (the gap across the fault is
        // short), then nothing until 6 000, then steady again.
        let mut records = vec![record(0, 900, "accepted"), record(1, 1_040, "accepted")];
        records.extend((0..20u64).map(|i| record((i % 3) as usize, 6_000 + i * 100, "accepted")));
        records.push(record(2, 3_000, "rejected"));
        assert_eq!(gap(&records, 1_000).gap_ms, Some(140));
        assert_eq!(longest_gap(&records, 0, 8_000), 4_960);
        // Edges count: nothing accepted between the window's start and the
        // first answer, or between the last answer and the window's end.
        assert_eq!(longest_gap(&records, 0, 900), 900);
        assert_eq!(longest_gap(&records, 7_900, 9_000), 1_100);
        assert_eq!(longest_gap(&[], 100, 600), 500);
    }

    #[test]
    fn a_failover_is_every_session_on_the_survivors_work() {
        let records = vec![
            on(0, 900, Node::A),
            on(1, 950, Node::A),
            on(2, 980, Node::A),
            // Session 0 answered by A just after the fault was stamped: not
            // a move to the survivor.
            on(0, 1_010, Node::A),
            on(0, 5_000, Node::B),
            on(1, 6_000, Node::B),
            // Session 2 reaches B only after the heal at 9 000.
            on(2, 9_500, Node::B),
            // Session 3 mined only after the fault: not one of A's miners.
            on(3, 4_000, Node::B),
        ];
        let report = failover(&records, 1_000, Node::B, 9_000);
        assert_eq!(report.sessions, 3);
        assert_eq!(report.stranded, vec![2]);
        assert_eq!(report.max_ms, Some(5_000));
        assert!(!report.within(30_000));
        let all = failover(&records, 1_000, Node::B, 10_000);
        assert!(all.stranded.is_empty());
        assert_eq!(all.max_ms, Some(8_500));
        assert!(all.within(8_500));
        assert!(!all.within(8_499));
        assert!(!failover(&[], 1_000, Node::B, 9_000).within(30_000));
    }

    #[test]
    fn the_gap_spans_the_last_accept_before_and_the_first_after_the_fault() {
        let records = vec![
            record(0, 900, "accepted"),
            record(1, 950, "accepted"),
            record(0, 1_100, "no-response"),
            record(1, 1_200, "rejected"),
            record(0, 4_000, "accepted"),
            record(1, 5_500, "accepted"),
            record(2, 6_000, "accepted"),
        ];
        let gap = gap(&records, 1_000);
        assert_eq!(gap.last_accept_before_ms, Some(950));
        assert_eq!(gap.first_accept_after_ms, Some(4_000));
        assert_eq!(gap.gap_ms, Some(3_050));
        assert_eq!(gap.fault_to_first_accept_ms, Some(3_000));
        // Session 2 had nothing accepted before the fault.
        assert_eq!((gap.sessions, gap.sessions_returned), (2, 2));
        assert_eq!(gap.session_p50_ms, Some(3_000));
        assert_eq!(gap.session_max_ms, Some(4_500));
    }

    #[test]
    fn a_session_that_never_returns_is_counted_and_a_block_is_not_a_share() {
        let mut block = record(9, 2_000, "accepted");
        block.scheduled_block = true;
        let records = vec![
            record(0, 500, "accepted"),
            record(1, 600, "accepted"),
            block,
        ];
        let gap = gap(&records, 1_000);
        assert_eq!(gap.first_accept_after_ms, None);
        assert_eq!((gap.sessions, gap.sessions_returned), (2, 0));
        let summary = summarize(&records);
        assert_eq!(summary.offered, 2);
        assert_eq!(summary.accepted_by_issuer["a"], 2);
    }
}
