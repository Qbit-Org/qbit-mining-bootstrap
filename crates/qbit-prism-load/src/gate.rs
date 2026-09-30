//! The pass/fail gate over a harness run (#521): the nightly workflow's
//! verdict and the per-PR smoke test's assertions, from the side report and
//! the harness's exit code.
//!
//! Every run is held to: exit 0 (completed and reconciled exactly), no
//! durability finding, and, in each gated phase, `shortfall` within budget.
//! A preset can add #473's D1 rule per gated phase -- no valid share refused
//! and no submit unanswered -- and a budget for the slowest session's time
//! to usable work on each tip. A D1 preset also gets #473's verdict table,
//! with its columns, so a nightly result reads against #473's matrix.
//!
//! EP-OBSERVABILITY: a figure the report could not measure fails its check
//! with the reason; it is never read as a pass. A tip some session never got
//! work on has no tip-to-last-notify time, and that is a failure, not a
//! missing sample.

use serde_json::Value;

/// What a run is held to. The preset's `gates` block supplies the defaults;
/// the workflow's dispatch inputs may override the two budgets.
#[derive(Clone, Debug, PartialEq)]
pub struct Budgets {
    /// Phases the per-phase checks gate; `None` is every phase the run drove.
    pub phases: Option<Vec<String>>,
    pub max_shortfall: u64,
    /// #473's rule: valid shares the server refused, per gated phase.
    pub max_rejected_valid_shares: Option<u64>,
    /// #473's rule: submits that got no answer, per gated phase.
    pub max_unanswered_submits: Option<u64>,
    /// `None` reports the tip times without gating on them.
    pub tip_last_notify_p99_ms: Option<f64>,
    /// Print #473's D1 verdict table for `steady_state` and `burst`.
    pub d1_verdict_table: bool,
    /// The churn phase's tip delivery, over the sessions connected at each
    /// tip; `None` does not gate on it.
    pub churn_tip_last_notify_p99_ms: Option<f64>,
    /// The churn phase's new sessions' time to first job; `None` does not
    /// gate on it.
    pub new_session_first_job_p99_ms: Option<f64>,
}

impl From<&crate::preset::Gates> for Budgets {
    fn from(gates: &crate::preset::Gates) -> Self {
        Self {
            phases: gates.phases.clone(),
            max_shortfall: gates.max_shortfall,
            max_rejected_valid_shares: gates.max_rejected_valid_shares,
            max_unanswered_submits: gates.max_unanswered_submits,
            tip_last_notify_p99_ms: gates.tip_last_notify_p99_budget_ms,
            d1_verdict_table: gates.d1_verdict_table,
            churn_tip_last_notify_p99_ms: gates.churn_tip_last_notify_p99_budget_ms,
            new_session_first_job_p99_ms: gates.new_session_first_job_p99_budget_ms,
        }
    }
}

/// One row of the verdict. `pass` is `None` for an informational row.
#[derive(Clone, Debug, PartialEq)]
pub struct Check {
    pub name: String,
    pub observed: String,
    pub budget: String,
    pub pass: Option<bool>,
}

impl Check {
    fn gate(name: impl Into<String>, observed: String, budget: String, pass: bool) -> Self {
        Self {
            name: name.into(),
            observed,
            budget,
            pass: Some(pass),
        }
    }

    fn info(name: impl Into<String>, observed: String) -> Self {
        Self {
            name: name.into(),
            observed,
            budget: String::new(),
            pass: None,
        }
    }
}

/// Nearest-rank percentile of a non-empty sample.
pub fn nearest_rank(values: &[f64], quantile: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let rank = ((quantile * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
    Some(sorted[rank - 1])
}

/// Every tip's slowest-session time, or the tips it is missing on.
pub fn tip_last_notify(report: &Value) -> Result<Vec<f64>, String> {
    let Some(tips) = report["time_to_usable_work"]["tips"].as_array() else {
        return Err("the report has no time_to_usable_work.tips".into());
    };
    if tips.is_empty() {
        return Err("the run minted no tip".into());
    }
    let mut times = Vec::new();
    let mut missing = Vec::new();
    for (index, tip) in tips.iter().enumerate() {
        match tip["all_sessions_milliseconds"].as_f64() {
            Some(millis) => times.push(millis),
            None => missing.push(format!(
                "tip {index}: {}",
                tip["all_sessions_unavailable_reason"]
                    .as_str()
                    .unwrap_or("no figure")
            )),
        }
    }
    if missing.is_empty() {
        Ok(times)
    } else {
        Err(missing.join("; "))
    }
}

/// One phase's figures for #473's rule; `None` where the report has none.
#[derive(Clone, Debug, PartialEq)]
pub struct PhaseFigures {
    pub name: String,
    pub shortfall: Option<u64>,
    pub rejected_valid_shares: Option<u64>,
    /// No-response records of the phase. A phase absent from
    /// `rejections.no_response_by_phase` recorded none: that map is keyed by
    /// the phases that had one, and the phase's submits were all recorded.
    pub unanswered: Option<u64>,
    /// Rejections in races the server is entitled to lose (class
    /// `expected`).
    pub entitled_races: Option<u64>,
}

pub fn phase_figures(report: &Value, phase: &Value) -> PhaseFigures {
    let name = phase["name"].as_str().unwrap_or("?").to_owned();
    let no_response = &report["rejections"]["no_response_by_phase"];
    let unanswered = no_response
        .as_object()
        .map(|map| map.get(&name).and_then(Value::as_u64).unwrap_or(0));
    let entitled_races = report["rejections"]["by_phase_reason_and_message"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .filter(|row| row["phase"] == name.as_str() && row["class"] == "expected")
                .filter_map(|row| row["count"].as_u64())
                .sum()
        });
    PhaseFigures {
        shortfall: phase["shortfall"].as_u64(),
        rejected_valid_shares: phase["rejected_valid_shares"].as_u64(),
        unanswered,
        entitled_races,
        name,
    }
}

fn count_check(checks: &mut Vec<Check>, name: String, observed: Option<u64>, budget: u64) {
    checks.push(Check::gate(
        name,
        observed.map_or("not reported".into(), |n| n.to_string()),
        format!("<= {budget}"),
        observed.is_some_and(|n| n <= budget),
    ));
}

/// The verdict on one run. `exit_code` is the harness's; `None` when the
/// caller did not capture it, which fails the exit-code check.
pub fn evaluate(report: &Value, exit_code: Option<i32>, budgets: &Budgets) -> Vec<Check> {
    let mut checks = Vec::new();
    checks.push(Check::gate(
        "harness exit code (0: completed and reconciled exactly)",
        exit_code.map_or("not captured".into(), |code| code.to_string()),
        "0".into(),
        exit_code == Some(0),
    ));
    let failed = report.get("failed").filter(|value| !value.is_null());
    let aborted = report.get("aborted").filter(|value| !value.is_null());
    checks.push(Check::gate(
        "run completed",
        match (failed, aborted) {
            (Some(failed), _) => format!("failed: {failed}"),
            (None, Some(aborted)) => format!("aborted: {aborted}"),
            (None, None) => "completed".into(),
        },
        "completed".into(),
        failed.is_none() && aborted.is_none(),
    ));
    let findings = report["durability_findings"].as_array().map(Vec::len);
    checks.push(Check::gate(
        "acknowledged shares lost or unexplained (durability findings)",
        findings.map_or("not reported".into(), |n| n.to_string()),
        "0".into(),
        findings == Some(0),
    ));
    let phases = report["phases"].as_array().cloned().unwrap_or_default();
    let mut missing = Some(0u64);
    let mut unexpected = Some(0u64);
    for phase in &phases {
        let add = |total: Option<u64>, key: &str| {
            total.and_then(|t| phase["reconciliation"][key].as_u64().map(|n| t + n))
        };
        missing = add(missing, "missing");
        unexpected = add(unexpected, "unexpected");
    }
    if phases.is_empty() {
        (missing, unexpected) = (None, None);
    }
    checks.push(Check::gate(
        "reconciliation: acknowledged shares missing from PostgreSQL",
        missing.map_or("not reported".into(), |m| m.to_string()),
        "0".into(),
        missing == Some(0),
    ));
    // A committed share the client holds no acknowledgement for is not a
    // loss. The harness explains each one -- an answer that never came back
    // before the drain ended, or one the server refused as unconfirmed --
    // and one it cannot explain is a durability finding, gated above, while
    // a divergence inside the run exits 5, gated by the exit code. So the
    // count is reported beside the explanations, not gated a second time.
    let explained = |key: &str| report[key]["count"].as_u64().unwrap_or(0);
    checks.push(Check::info(
        "reconciliation: committed without an acknowledgement (no-response / divergence / \
         unknown-outcome)",
        format!(
            "{} ({} / {} / {})",
            unexpected.map_or("not reported".into(), |u| u.to_string()),
            explained("no_response_commits"),
            explained("ack_commit_divergence"),
            explained("unknown_outcome_commits"),
        ),
    ));
    let gated: Vec<&Value> = match &budgets.phases {
        None => phases.iter().collect(),
        Some(names) => {
            for name in names {
                if !phases.iter().any(|phase| phase["name"] == name.as_str()) {
                    checks.push(Check::gate(
                        format!("{name}: phase ran"),
                        "absent from the report".into(),
                        "present".into(),
                        false,
                    ));
                }
            }
            phases
                .iter()
                .filter(|phase| names.iter().any(|name| phase["name"] == name.as_str()))
                .collect()
        }
    };
    if gated.is_empty() {
        checks.push(Check::gate(
            "gated phases",
            "none ran".into(),
            "at least one".into(),
            false,
        ));
    }
    for phase in &gated {
        let figures = phase_figures(report, phase);
        count_check(
            &mut checks,
            format!("{}: shortfall (offers no session could take)", figures.name),
            figures.shortfall,
            budgets.max_shortfall,
        );
        if let Some(budget) = budgets.max_rejected_valid_shares {
            count_check(
                &mut checks,
                format!("{}: valid shares refused", figures.name),
                figures.rejected_valid_shares,
                budget,
            );
        }
        if let Some(budget) = budgets.max_unanswered_submits {
            count_check(
                &mut checks,
                format!("{}: submits unanswered", figures.name),
                figures.unanswered,
                budget,
            );
        }
    }
    for phase in &phases {
        if gated.iter().any(|g| g["name"] == phase["name"]) {
            continue;
        }
        let figures = phase_figures(report, phase);
        checks.push(Check::info(
            format!(
                "{}: shortfall / valid refused / unanswered (not gated)",
                figures.name
            ),
            format!(
                "{} / {} / {}",
                opt(figures.shortfall),
                opt(figures.rejected_valid_shares),
                opt(figures.unanswered)
            ),
        ));
    }
    let name = "tip to last session's notify, p99 over tips";
    let budget = budgets
        .tip_last_notify_p99_ms
        .map_or("not gated".into(), |ms| format!("<= {ms} ms"));
    match (tip_last_notify(report), budgets.tip_last_notify_p99_ms) {
        (Ok(times), limit) => {
            let p99 = nearest_rank(&times, 0.99);
            let observed = p99.map_or("no tips".into(), |p| {
                format!(
                    "{p:.0} ms over {} tips (max {:.0} ms)",
                    times.len(),
                    max(&times)
                )
            });
            match limit {
                Some(limit) => checks.push(Check::gate(
                    name,
                    observed,
                    budget,
                    p99.is_some_and(|p| p <= limit),
                )),
                None => checks.push(Check::info(name, observed)),
            }
        }
        (Err(reason), Some(_)) => checks.push(Check::gate(
            name,
            format!("unmeasured: {reason}"),
            budget,
            false,
        )),
        (Err(reason), None) => checks.push(Check::info(name, format!("unmeasured: {reason}"))),
    }
    churn_checks(report, budgets, &mut checks);
    let population = &report["population"];
    checks.push(Check::info(
        "payout addresses / sessions",
        format!(
            "{} / {}",
            population["recipients"], report["topology"]["sessions"]
        ),
    ));
    checks.push(Check::info(
        "live accepted work: top-1 / top-10 address share",
        format!(
            "{} / {}",
            share(&population["live_accepted_work_per_recipient_concentration"]["top1_share"]),
            share(&population["live_accepted_work_per_recipient_concentration"]["top10_share"]),
        ),
    ));
    checks.push(Check::info(
        "session difficulty spread (orders of magnitude)",
        population["session_difficulty_spread_orders_of_magnitude"]
            .as_f64()
            .map_or("n/a".into(), |orders| format!("{orders:.2}")),
    ));
    for phase in &phases {
        checks.push(Check::info(
            format!(
                "{}: offered rate CV 1 s / 60 s, max 1 s",
                phase["name"].as_str().unwrap_or("?")
            ),
            format!(
                "{} / {}, {}",
                ratio(&phase["arrival"]["offered_per_second_cv_1s"]),
                ratio(&phase["arrival"]["offered_per_second_cv_60s"]),
                phase["arrival"]["offered_max_1s"],
            ),
        ));
    }
    checks
}

/// Every churn tip's slowest served session among those connected at the
/// tip, or why a tip has none.
pub fn churn_tip_last_notify(report: &Value) -> Result<Vec<f64>, String> {
    churn_section_tip_last_notify(&report["churn"])
}

/// [`churn_tip_last_notify`] over one churn section.
fn churn_section_tip_last_notify(churn: &Value) -> Result<Vec<f64>, String> {
    if churn["ran"] != true {
        return Err(format!(
            "the churn phase did not run: {}",
            churn["reason"].as_str().unwrap_or("no churn section")
        ));
    }
    let Some(tips) = churn["tip_delivery"]["tips"].as_array() else {
        return Err("the churn section has no tip_delivery.tips".into());
    };
    if tips.is_empty() {
        return Err("the churn phase minted no tip".into());
    }
    let mut times = Vec::new();
    let mut missing = Vec::new();
    for (index, tip) in tips.iter().enumerate() {
        match tip["last_served_milliseconds"].as_f64() {
            Some(millis) => times.push(millis),
            None => missing.push(format!(
                "tip {index}: {} of {} connected sessions unserved",
                tip["unserved"], tip["connected_at_tip"]
            )),
        }
    }
    if missing.is_empty() {
        Ok(times)
    } else {
        Err(missing.join("; "))
    }
}

fn churn_checks(report: &Value, budgets: &Budgets, checks: &mut Vec<Check>) {
    churn_section_checks(&report["churn"], "churn", budgets, checks);
    // A soak drives a churn phase every cycle (#575); the section above is
    // the first, and each of them is held to the same budgets.
    for phase in report["soak"]["churn_phases"]
        .as_array()
        .into_iter()
        .flatten()
    {
        let label = format!("{}: churn", phase["phase"].as_str().unwrap_or("?"));
        churn_section_checks(&phase["report"], &label, budgets, checks);
    }
}

fn churn_section_checks(churn: &Value, label: &str, budgets: &Budgets, checks: &mut Vec<Check>) {
    if churn["ran"] != true
        && budgets.churn_tip_last_notify_p99_ms.is_none()
        && budgets.new_session_first_job_p99_ms.is_none()
    {
        return;
    }
    let name = format!("{label}: tip to last notify, sessions connected at the tip, p99 over tips");
    let budget = budgets
        .churn_tip_last_notify_p99_ms
        .map_or("not gated".into(), |ms| format!("<= {ms} ms"));
    let (observed, pass) = match churn_section_tip_last_notify(churn) {
        Ok(times) => {
            let p99 = nearest_rank(&times, 0.99);
            (
                p99.map_or("no tips".into(), |p| {
                    format!(
                        "{p:.0} ms over {} tips (max {:.0} ms)",
                        times.len(),
                        max(&times)
                    )
                }),
                p99.zip(budgets.churn_tip_last_notify_p99_ms)
                    .is_some_and(|(p, limit)| p <= limit),
            )
        }
        Err(reason) => (format!("unmeasured: {reason}"), false),
    };
    checks.push(match budgets.churn_tip_last_notify_p99_ms {
        Some(_) => Check::gate(name, observed, budget, pass),
        None => Check::info(name, observed),
    });
    let first = &churn["time_to_first_job"]["new_sessions"];
    let p99 = first["p99"].as_f64();
    let observed = match p99 {
        Some(p) => format!(
            "{p:.0} ms over {} connections (max {:.0} ms)",
            first["samples"],
            first["max"].as_f64().unwrap_or(p)
        ),
        None => format!(
            "unmeasured: {}",
            first["unavailable_reason"]
                .as_str()
                .unwrap_or("the churn phase did not run")
        ),
    };
    let name = format!("{label}: new session time to first job, p99");
    checks.push(match budgets.new_session_first_job_p99_ms {
        Some(limit) => Check::gate(
            name,
            observed,
            format!("<= {limit} ms"),
            p99.is_some_and(|p| p <= limit),
        ),
        None => Check::info(name, observed),
    });
    let realised = &churn["realised"];
    if churn["ran"] == true {
        checks.push(Check::info(
            format!("{label}: connects/s max, concurrent sessions min-max, storms, rentals"),
            format!(
                "{} / {}-{} / {} / {} spawned, {} departed",
                realised["connects_per_second_max"],
                realised["concurrent_sessions_min"],
                realised["concurrent_sessions_max"],
                realised["storms"].as_array().map_or(0, Vec::len),
                realised["rentals_spawned"],
                realised["rentals_departed"],
            ),
        ));
    }
}

fn opt(value: Option<u64>) -> String {
    value.map_or("n/a".into(), |n| n.to_string())
}

fn max(values: &[f64]) -> f64 {
    values.iter().copied().fold(f64::NEG_INFINITY, f64::max)
}

fn share(value: &Value) -> String {
    value
        .as_f64()
        .map_or("n/a".into(), |v| format!("{:.1}%", v * 100.0))
}

fn ratio(value: &Value) -> String {
    value.as_f64().map_or("n/a".into(), |v| format!("{v:.2}"))
}

/// Whether every gating check passed.
pub fn passed(checks: &[Check]) -> bool {
    checks.iter().all(|check| check.pass != Some(false))
}

/// The verdict as a Markdown table, for a job summary.
pub fn markdown(title: &str, checks: &[Check]) -> String {
    let mut out = format!(
        "### {title}: {}\n\n| Check | Observed | Budget | Result |\n|---|---|---|---|\n",
        if passed(checks) { "PASS" } else { "FAIL" }
    );
    for check in checks {
        let result = match check.pass {
            Some(true) => "pass",
            Some(false) => "**FAIL**",
            None => "info",
        };
        out.push_str(&format!(
            "| {} | {} | {} | {result} |\n",
            check.name,
            check.observed.replace('|', "\\|"),
            check.budget
        ));
    }
    out
}

// --- #473's D1 verdict table ---------------------------------------------

/// #473's verdict-table header, column for column
/// (`docs/prism-throughput-measurements.md`, "D1 verdict").
pub const D1_COLUMNS: &[&str] = &[
    "window",
    "fe",
    "repl",
    "plan",
    "n",
    "target /s",
    "offered /s",
    "achieved /s",
    "shortfall tokens",
    "rejected valid shares, per run",
    "unanswered submits, per run",
    "entitled-race rejections, per run",
    "verdict",
    "ACK p99 ms",
    "ACK p99 within the validator limit",
    "ORDER_LOCK max / mean waiters",
    "same-configuration runs outside the medians",
];

/// Thousands separators and #473's precision: one decimal, trailing `.0`
/// dropped, and three decimals for a rate that would otherwise round to its
/// target without being it.
pub fn number(value: f64, target: Option<f64>) -> String {
    let mut text = format!("{value:.1}");
    if let Some(target) = target {
        if text.parse::<f64>().ok() == Some(target) && value != target {
            text = format!("{value:.3}");
        }
    }
    if let Some(stripped) = text.strip_suffix(".0") {
        text = stripped.to_owned();
    }
    let (whole, fraction) = match text.split_once('.') {
        Some((whole, fraction)) => (whole.to_owned(), Some(fraction.to_owned())),
        None => (text.clone(), None),
    };
    let (sign, digits) = match whole.strip_prefix('-') {
        Some(digits) => ("-", digits),
        None => ("", whole.as_str()),
    };
    let mut grouped = String::new();
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index) % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    match fraction {
        Some(fraction) => format!("{sign}{grouped}.{fraction}"),
        None => format!("{sign}{grouped}"),
    }
}

/// The per-run verdict text for one phase, in #473's words.
pub fn d1_verdict(figures: &PhaseFigures) -> String {
    let (Some(shortfall), Some(rejected), Some(unanswered)) = (
        figures.shortfall,
        figures.rejected_valid_shares,
        figures.unanswered,
    ) else {
        return "no verdict: a figure of the rule is unreported".into();
    };
    let mut reasons = Vec::new();
    if shortfall > 0 {
        reasons.push(format!(
            "shortfall in 1 of 1 repeats, at most {} per run",
            number(shortfall as f64, None)
        ));
    }
    if rejected > 0 {
        reasons.push(format!(
            "valid shares refused in 1 of 1 repeats, at most {} per run",
            number(rejected as f64, None)
        ));
    }
    if unanswered > 0 {
        reasons.push(format!(
            "submits unanswered in 1 of 1 repeats, at most {} per run",
            number(unanswered as f64, None)
        ));
    }
    if reasons.is_empty() {
        "**met** (1 of 1)".into()
    } else {
        format!("**not met** (0 of 1): {}", reasons.join("; "))
    }
}

/// #473's verdict table for this run's `steady_state` and `burst` phases:
/// one row each, `n` = 1. A run that did not exit 0 is kept out, as #473
/// kept runs with a finding out of its medians, and named in the last
/// column.
pub fn d1_table(report: &Value, exit_code: Option<i32>, run_name: &str) -> String {
    let mut out = String::new();
    let topology = &report["topology"];
    let window = report["window"]["requested_window_shares"]
        .as_u64()
        .map_or("n/a".into(), |w| {
            if w.is_multiple_of(1000) {
                format!("{}k", w / 1000)
            } else {
                w.to_string()
            }
        });
    let replication = report["database"]["replication"]["declared"]
        .as_str()
        .unwrap_or("n/a");
    let mut plan = topology["plan"].as_str().unwrap_or("n/a").to_owned();
    let blocks = report["phases"].as_array().map_or(0, |phases| {
        phases
            .iter()
            .filter_map(|phase| phase["scheduled_blocks"].as_u64())
            .sum::<u64>()
    });
    if report["dense_cadence"]["ran"] == true {
        plan.push_str(&format!(" +dense({blocks})"));
    } else if blocks > 0 {
        plan.push_str(&format!(" +{blocks} blocks"));
    }
    let limit = report["validator"]["ack_p99_limit_used_milliseconds"].as_f64();
    for (phase_name, title) in [
        ("steady_state", "The 500 shares/s phase (`steady_state`)"),
        ("burst", "The 2,000 shares/s phase (`burst`)"),
    ] {
        let Some(phase) = report["phases"]
            .as_array()
            .and_then(|phases| phases.iter().find(|p| p["name"] == phase_name))
        else {
            continue;
        };
        out.push_str(&format!(
            "#### {title}\n\n| {} |\n|{}\n",
            D1_COLUMNS.join(" | "),
            "---|".repeat(D1_COLUMNS.len())
        ));
        let target = phase["target_rate_shares_per_second"].as_f64();
        let fmt = |value: &Value, target: Option<f64>| {
            value.as_f64().map_or("n/a".into(), |v| number(v, target))
        };
        let head = [
            window.clone(),
            topology["frontends"].to_string(),
            replication.to_owned(),
            plan.clone(),
        ];
        let row: Vec<String> = if exit_code != Some(0) {
            let mut row = head.to_vec();
            row.push("0".into());
            row.extend(std::iter::repeat_n("n/a".to_owned(), 7));
            row.push("no verdict: no run in the medians".into());
            row.extend(std::iter::repeat_n("n/a".to_owned(), 3));
            row.push(format!(
                "{run_name} (exit {})",
                exit_code.map_or("not captured".into(), |c| c.to_string())
            ));
            row
        } else {
            let figures = phase_figures(report, phase);
            let p99 = phase["client_ack_latency"]["p99"].as_f64();
            let order = &phase["order_lock"];
            let mut row = head.to_vec();
            row.extend([
                "1".into(),
                fmt(&phase["target_rate_shares_per_second"], None),
                fmt(&phase["offered_rate_shares_per_second"], target),
                fmt(&phase["achieved_rate_shares_per_second"], target),
                // #473 groups the shortfall's digits and prints the three
                // per-run counts bare.
                figures
                    .shortfall
                    .map_or("n/a".into(), |n| number(n as f64, None)),
                opt(figures.rejected_valid_shares),
                opt(figures.unanswered),
                opt(figures.entitled_races),
                d1_verdict(&figures),
                p99.map_or("n/a".into(), |p| number(p, None)),
                match (p99, limit) {
                    (Some(p), Some(l)) => {
                        format!("{} of 1 (limit {} ms)", u8::from(p <= l), number(l, None))
                    }
                    _ => "n/a".into(),
                },
                format!(
                    "{} / {}",
                    order["max_waiters"],
                    order["mean_waiters"]
                        .as_f64()
                        .map_or("n/a".into(), |m| format!("{m:.2}"))
                ),
                "–".into(),
            ]);
            row
        };
        out.push_str(&format!("| {} |\n\n", row.join(" | ")));
    }
    out
}
