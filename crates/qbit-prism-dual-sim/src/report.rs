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
    /// `first_accept_after - last_accept_before`: the longest stretch with
    /// no share accepted anywhere, across the fault.
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

/// Every submit, by outcome and reason, and by the node that issued the
/// job.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ShareSummary {
    pub offered: usize,
    pub accepted: usize,
    pub accepted_by_issuer: BTreeMap<String, usize>,
    pub rejected: BTreeMap<String, usize>,
    pub no_response: BTreeMap<String, usize>,
    /// Acknowledged shares missing from a checked database, not excused.
    pub lost: usize,
    /// Acknowledged shares missing and excused as a documented tail.
    pub excused_tail: usize,
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
            "Offered {}, accepted {} (by issuing node: {:?}), rejected {:?}, no answer {:?}. Lost (acknowledged, missing, not excused): **{}**. Excused documented tail: {}.\n",
            shares.offered,
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
