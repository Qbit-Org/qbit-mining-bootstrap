//! Holding a soak's samples to its gates, and the Markdown verdict.

use super::{read_samples, Gates, ProcessPoint, Sample, Spec, REPORT_FILE, SAMPLES_FILE};
use crate::gate::Check;
use std::collections::BTreeMap;
use std::path::Path;

/// Least-squares slope of `(x, y)` in y-units per x-unit; `None` under two
/// distinct x values.
pub fn slope(points: &[(f64, f64)]) -> Option<f64> {
    if points.len() < 2 {
        return None;
    }
    let n = points.len() as f64;
    let mean_x = points.iter().map(|p| p.0).sum::<f64>() / n;
    let mean_y = points.iter().map(|p| p.1).sum::<f64>() / n;
    let sxx: f64 = points.iter().map(|p| (p.0 - mean_x).powi(2)).sum();
    if sxx <= 0.0 {
        return None;
    }
    let sxy: f64 = points.iter().map(|p| (p.0 - mean_x) * (p.1 - mean_y)).sum();
    Some(sxy / sxx)
}

fn mean(values: &[f64]) -> Option<f64> {
    (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
}

fn fail(name: impl Into<String>, observed: String, budget: String) -> Check {
    Check {
        name: name.into(),
        observed,
        budget,
        pass: Some(false),
    }
}

fn verdict(name: impl Into<String>, observed: String, budget: String, pass: bool) -> Check {
    Check {
        name: name.into(),
        observed,
        budget,
        pass: Some(pass),
    }
}

fn info(name: impl Into<String>, observed: String) -> Check {
    Check {
        name: name.into(),
        observed,
        budget: String::new(),
        pass: None,
    }
}

/// Every process instance any sample names, in first-seen order.
fn instances(samples: &[Sample]) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    for sample in samples {
        for process in &sample.processes {
            if !names.contains(&process.instance) {
                names.push(process.instance.clone());
            }
        }
    }
    names
}

/// What one trend gate reads and holds: a process figure, divided by
/// `scale` into `unit`, whose slope per hour must be at most `max`. With a
/// `window`, the slope is fitted over each whole window's peak instead of
/// over every sample.
struct Trend<'a, F> {
    label: &'a str,
    unit: &'a str,
    max: f64,
    scale: f64,
    window: Option<(f64, u64)>,
    value: F,
}

/// The peak of each whole `window_hours` window of `(hours, value)` points,
/// at the window's middle. Windows start at the first point; a trailing
/// partial window is left out, since its peak would be read over less time.
pub fn window_peaks(points: &[(f64, f64)], window_hours: f64) -> Vec<(f64, f64)> {
    let Some(&(start, _)) = points.first() else {
        return Vec::new();
    };
    let end = points.last().map_or(start, |p| p.0);
    let whole = ((end - start) / window_hours).floor() as usize;
    (0..whole)
        .filter_map(|index| {
            let from = start + index as f64 * window_hours;
            let to = from + window_hours;
            points
                .iter()
                .filter(|p| p.0 >= from && p.0 < to)
                .map(|p| p.1)
                .reduce(f64::max)
                .map(|peak| (from + window_hours / 2.0, peak))
        })
        .collect()
}

/// A per-process trend over the steady post-warm-up samples, in units per
/// hour, held to the trend's `max`.
fn trend_check<F: Fn(&ProcessPoint) -> Option<u64>>(
    trend: Trend<'_, F>,
    instance: &str,
    gated: &[&Sample],
    gates: &Gates,
) -> Check {
    let Trend {
        label,
        unit,
        max,
        scale,
        window,
        value,
    } = trend;
    let name = format!("{label} slope after warm-up, {instance}");
    let budget = match window {
        Some((minutes, windows)) => {
            format!("<= {max} {unit}/h over the peaks of >= {windows} whole {minutes} min windows")
        }
        None => format!(
            "<= {max} {unit}/h over >= {} samples",
            gates.min_gated_samples
        ),
    };
    let mut points = Vec::new();
    let mut unknown = 0usize;
    for sample in gated {
        let Some(process) = sample.processes.iter().find(|p| p.instance == instance) else {
            unknown += 1;
            continue;
        };
        match value(process) {
            Some(v) => points.push((sample.elapsed_seconds / 3600.0, v as f64 / scale)),
            None => unknown += 1,
        }
    }
    // A process that has gone dark is judged on nothing it did since.
    let dark_at_end = gated.last().is_some_and(|sample| {
        sample
            .processes
            .iter()
            .find(|p| p.instance == instance)
            .is_none_or(|p| value(p).is_none())
    });
    if dark_at_end {
        return fail(
            name,
            "unknown: the latest sample could not read it".into(),
            budget,
        );
    }
    if (points.len() as u64) < gates.min_gated_samples {
        return fail(
            name,
            format!(
                "unknown: {} readable steady sample(s) after warm-up ({unknown} unreadable)",
                points.len()
            ),
            budget,
        );
    }
    let (fitted, what) = match window {
        Some((minutes, windows)) => {
            let peaks = window_peaks(&points, minutes / 60.0);
            if (peaks.len() as u64) < windows {
                return fail(
                    name,
                    format!(
                        "unknown: {} whole window(s) after warm-up from {} sample(s)",
                        peaks.len(),
                        points.len()
                    ),
                    budget,
                );
            }
            let what = format!("window peaks of {} samples", points.len());
            (peaks, what)
        }
        None => (points.clone(), "samples".to_owned()),
    };
    match slope(&fitted) {
        Some(per_hour) => {
            let first = fitted.first().map(|p| p.1).unwrap_or_default();
            let last = fitted.last().map(|p| p.1).unwrap_or_default();
            verdict(
                name,
                format!(
                    "{per_hour:+.2} {unit}/h over {} {what} ({first:.1} -> {last:.1} {unit}){}",
                    fitted.len(),
                    if unknown > 0 {
                        format!(", {unknown} unreadable")
                    } else {
                        String::new()
                    }
                ),
                budget,
                per_hour <= max,
            )
        }
        None => fail(name, "unknown: the samples span no time".into(), budget),
    }
}

/// Partition bounds the share sequence crossed between the first and the last
/// sample that read it.
pub fn rollovers(samples: &[Sample]) -> Option<(u64, i64, i64)> {
    let first = samples.iter().find_map(|s| s.database.next_share_seq)?;
    let last = samples
        .iter()
        .rev()
        .find_map(|s| s.database.next_share_seq)?;
    let mut bounds: Vec<i64> = samples
        .iter()
        .flat_map(|s| s.database.partitions.iter().map(|p| p.upper_seq))
        .collect();
    bounds.sort_unstable();
    bounds.dedup();
    let crossed = bounds
        .iter()
        .filter(|bound| first < **bound && **bound <= last)
        .count() as u64;
    Some((crossed, first, last))
}

/// Partitions that were in the ledger at the first sample that listed any
/// and were dropped by the last.
pub fn archive_cycles(samples: &[Sample]) -> Option<Vec<String>> {
    let first = samples.iter().find(|s| !s.database.partitions.is_empty())?;
    let last = samples
        .iter()
        .rev()
        .find(|s| !s.database.partitions.is_empty())?;
    let before: BTreeMap<&str, &str> = first
        .database
        .partitions
        .iter()
        .map(|p| (p.name.as_str(), p.state.as_str()))
        .collect();
    Some(
        last.database
            .partitions
            .iter()
            .filter(|p| p.state == "dropped")
            .filter(|p| {
                before
                    .get(p.name.as_str())
                    .is_none_or(|state| *state != "dropped")
            })
            .map(|p| p.name.clone())
            .collect(),
    )
}

/// Hold a soak's samples to its gates. Checks only one driver can make (the
/// harness's retention commands, say) are added by that driver's caller:
/// see [`gate_harness_run`].
pub fn evaluate(samples: &[Sample], gates: &Gates) -> Vec<Check> {
    let Some(last) = samples.last() else {
        return vec![fail(
            "soak samples",
            "unknown: no sample was recorded".into(),
            "at least one".into(),
        )];
    };
    let warmup = gates.warmup_minutes * 60.0;
    let after: Vec<&Sample> = samples
        .iter()
        .filter(|s| s.elapsed_seconds >= warmup)
        .collect();
    let mut checks = vec![info(
        "soak span",
        format!(
            "{} samples over {:.2} h ({:.2} h after a {:.0} min warm-up)",
            samples.len(),
            last.elapsed_seconds / 3600.0,
            ((last.elapsed_seconds - warmup) / 3600.0).max(0.0),
            gates.warmup_minutes
        ),
    )];
    let names = instances(samples);
    if names.is_empty() {
        checks.push(fail(
            "server processes",
            "unknown: no sample names a server process".into(),
            "at least one".into(),
        ));
    }
    let mut processes = Vec::new();
    for instance in &names {
        processes.extend(process_checks(samples, instance, gates));
    }
    if let Some(issue) = &gates.rss_expected_failure {
        expect_rss_failure(&mut processes, issue);
    }
    checks.extend(processes);
    checks.extend(connection_checks(samples, &after, gates));
    checks.push(peak_check(
        samples,
        "idle in transaction over 60 s",
        0,
        "0".into(),
        |peak| format!("peak {peak}"),
        |s| s.database.idle_in_transaction_over_60s,
    ));
    checks.push(peak_check(
        samples,
        "WAL size",
        gates.wal_bytes_max,
        format!("<= {} MiB", gates.wal_bytes_max / (1024 * 1024)),
        |peak| format!("peak {} MiB", peak / (1024 * 1024)),
        |s| s.database.wal_bytes,
    ));
    checks.extend(ledger_checks(samples, gates));
    checks.extend(latency_info(&after));
    checks
}

/// One server process: its lifetimes, and its memory and descriptor trends
/// over the steady post-warm-up samples of its last lifetime. The warm-up is
/// the lifetime's own: a process restarted mid-soak warms up again, and its
/// cold start is neither fitted as a trend nor compared with a warm-up it
/// was not alive for.
fn process_checks(samples: &[Sample], instance: &str, gates: &Gates) -> Vec<Check> {
    let mut checks = Vec::new();
    let mut pids: Vec<u32> = samples
        .iter()
        .flat_map(|s| s.processes.iter())
        .filter(|p| p.instance == instance)
        .filter_map(|p| p.pid)
        .collect();
    pids.dedup();
    let lifetimes = format!("{} PID(s): {pids:?}", pids.len());
    checks.push(if gates.one_lifetime {
        verdict(
            format!("one server lifetime, {instance}"),
            lifetimes,
            "1".into(),
            pids.len() == 1,
        )
    } else {
        info(format!("server lifetimes, {instance}"), lifetimes)
    });
    // A restart restarts every trend: only the last lifetime is fitted.
    let current = pids.last().copied();
    let point = |s: &Sample| {
        s.processes
            .iter()
            .find(|p| p.instance == instance && p.pid == current)
            .cloned()
    };
    let warmed_at = samples
        .iter()
        .filter(|s| point(s).is_some())
        .map(|s| s.elapsed_seconds)
        .fold(f64::INFINITY, f64::min)
        + gates.warmup_minutes * 60.0;
    if let Some(multiple) = gates.rss_warmup_peak_multiple_max {
        let peak = |early: bool| {
            samples
                .iter()
                .filter(|s| (s.elapsed_seconds < warmed_at) == early)
                .filter_map(|s| point(s)?.rss_bytes)
                .max()
        };
        let name = format!("resident memory against the warm-up peak, {instance}");
        let budget = format!("<= {multiple}x the warm-up peak");
        checks.push(match (peak(true), peak(false)) {
            (Some(warm), Some(later)) => verdict(
                name,
                format!(
                    "peak {:.1} MiB after, {:.1} MiB in the warm-up ({:.2}x)",
                    later as f64 / 1048576.0,
                    warm as f64 / 1048576.0,
                    later as f64 / warm.max(1) as f64
                ),
                budget,
                later as f64 <= multiple * warm as f64,
            ),
            _ => fail(
                name,
                "unknown: no readable sample on one side of the warm-up".into(),
                budget,
            ),
        });
    }
    let lifetime: Vec<&Sample> = samples
        .iter()
        .filter(|s| s.elapsed_seconds >= warmed_at && s.steady && point(s).is_some())
        .collect();
    checks.push(trend_check(
        Trend {
            label: "resident memory",
            unit: "MiB",
            max: gates.rss_slope_mib_per_hour_max,
            scale: 1024.0 * 1024.0,
            window: Some((gates.rss_trend_window_minutes, gates.min_trend_windows)),
            value: |p: &ProcessPoint| p.rss_bytes,
        },
        instance,
        &lifetime,
        gates,
    ));
    checks.push(trend_check(
        Trend {
            label: "open file descriptors",
            unit: "fds",
            max: gates.fd_slope_per_hour_max,
            scale: 1.0,
            window: None,
            value: |p: &ProcessPoint| p.open_fds,
        },
        instance,
        &lifetime,
        gates,
    ));
    checks
}

/// A known resident-memory failure (see [`Gates::rss_expected_failure`]),
/// over every process's rows at once: one frontend's warm-up ratio can pass
/// while its slope, or another frontend's rows, fail. A measured failure is
/// reported, not gated. When no row failed and none was unknown, the issue
/// looks fixed, and that fails until the key is removed. An unknown row is
/// left failing: it is no evidence either way.
fn expect_rss_failure(checks: &mut Vec<Check>, issue: &str) {
    let rss = |check: &Check| check.name.starts_with("resident memory ");
    let unknown = |check: &Check| check.observed.starts_with("unknown");
    let mut expected = 0usize;
    let mut unread = false;
    for check in checks.iter_mut().filter(|check| rss(check)) {
        match check.pass {
            Some(false) if unknown(check) => unread = true,
            Some(false) => {
                check.pass = None;
                check.observed = format!("expected failure ({issue}): {}", check.observed);
                expected += 1;
            }
            _ => {}
        }
    }
    if expected == 0 && !unread {
        checks.push(fail(
            format!("resident memory expected failure, {issue}"),
            format!("{issue} looks fixed: every resident-memory row passed"),
            format!(
                "a resident-memory row fails as {issue} describes; once it is \
                 resolved, set rss_expected_failure to null"
            ),
        ));
    }
}

/// Connections per client key: a server process's key is its instance name
/// when the samples come from the harness, and whatever the deployment's
/// clients connect as otherwise. Each client's peak, and its drift from the
/// first quarter of the post-warm-up samples to the last.
fn connection_checks(samples: &[Sample], after: &[&Sample], gates: &Gates) -> Vec<Check> {
    let mut checks = Vec::new();
    let unread = samples
        .iter()
        .filter(|s| s.database.connections.is_none())
        .count();
    if unread > 0 {
        checks.push(fail(
            "database connections",
            format!(
                "unknown: {unread} of {} sample(s) could not read pg_stat_activity",
                samples.len()
            ),
            format!("<= {} per client", gates.pool_connections_max),
        ));
    }
    let mut clients: Vec<&String> = samples
        .iter()
        .filter_map(|s| s.database.connections.as_ref())
        .flat_map(|c| c.keys())
        .collect();
    clients.sort();
    clients.dedup();
    for client in clients {
        let peak = samples
            .iter()
            .filter_map(|s| s.database.connections.as_ref()?.get(client))
            .max()
            .copied()
            .unwrap_or(0);
        checks.push(verdict(
            format!("database connections, {client}"),
            format!("peak {peak}"),
            format!("<= {}", gates.pool_connections_max),
            peak <= gates.pool_connections_max,
        ));
        // A read that did not list the client is a real zero for it.
        let series: Vec<f64> = after
            .iter()
            .filter_map(|s| s.database.connections.as_ref())
            .map(|clients| *clients.get(client).unwrap_or(&0) as f64)
            .collect();
        let quarter = series.len() / 4;
        let name = format!("database connection drift, {client}");
        let budget = format!(
            "<= {} (last quarter mean - first quarter mean)",
            gates.pool_connections_drift_max
        );
        checks.push(if quarter == 0 {
            fail(
                name,
                format!("unknown: {} post-warm-up sample(s)", series.len()),
                budget,
            )
        } else {
            let first = mean(&series[..quarter]).unwrap_or_default();
            let last = mean(&series[series.len() - quarter..]).unwrap_or_default();
            verdict(
                name,
                format!("{:+.2} ({first:.2} -> {last:.2})", last - first),
                budget,
                last - first <= gates.pool_connections_drift_max,
            )
        });
    }
    checks
}

/// A figure every sample must read, whose peak is held to `max`. One sample
/// that could not read it fails the gate: a peak over the samples that
/// happened to read is not the soak's peak.
fn peak_check(
    samples: &[Sample],
    name: &str,
    max: u64,
    budget: String,
    render: impl Fn(u64) -> String,
    value: impl Fn(&Sample) -> Option<u64>,
) -> Check {
    let values: Vec<Option<u64>> = samples.iter().map(value).collect();
    let unknown = values.iter().filter(|v| v.is_none()).count();
    match values.iter().flatten().max().copied() {
        _ if unknown > 0 => fail(
            name,
            format!("unknown in {unknown} of {} sample(s)", samples.len()),
            budget,
        ),
        Some(peak) => verdict(name, render(peak), budget, peak <= max),
        None => fail(name, "unknown: no sample".into(), budget),
    }
}

/// The share ledger over the soak: partition rollovers, partitions that left
/// through retention, payout divergences, and, for a deployment, every
/// acknowledged share committed.
fn ledger_checks(samples: &[Sample], gates: &Gates) -> Vec<Check> {
    let mut checks = vec![
        match rollovers(samples) {
            Some((crossed, first, last)) => verdict(
                "share partition rollovers",
                format!("{crossed} bound(s) crossed (share_seq {first} -> {last})"),
                format!(">= {}", gates.min_rollovers),
                crossed >= gates.min_rollovers,
            ),
            None => fail(
                "share partition rollovers",
                "unknown: no sample read the share sequence".into(),
                format!(">= {}", gates.min_rollovers),
            ),
        },
        match archive_cycles(samples) {
            Some(dropped) => verdict(
                "share partitions archived and dropped",
                if dropped.is_empty() {
                    "none".into()
                } else {
                    format!("{}: {}", dropped.len(), dropped.join(", "))
                },
                format!(">= {}", gates.min_archive_cycles),
                dropped.len() as u64 >= gates.min_archive_cycles,
            ),
            None => fail(
                "share partitions archived and dropped",
                "unknown: no sample read the partition catalog".into(),
                format!(">= {}", gates.min_archive_cycles),
            ),
        },
    ];
    if let Some(ledger) = samples.iter().rev().find_map(|s| s.ledger.as_ref()) {
        let name = "acknowledged shares in the ledger";
        let budget = "committed + scrape spread >= acknowledged".to_owned();
        checks.push(
            match (
                ledger.acknowledged_since_start,
                ledger.committed_since_start,
                ledger.tolerance_rows,
            ) {
                (Some(acked), Some(committed), Some(tolerance)) => verdict(
                    name,
                    format!(
                        "{committed} committed of {acked} acknowledged (+{tolerance} rows of a \
                         {:.0} s scrape spread){}",
                        ledger.tolerance_seconds,
                        if ledger.acknowledged_gaps > 0 {
                            format!(
                                "; {} restart(s) whose last acknowledgements are uncounted",
                                ledger.acknowledged_gaps
                            )
                        } else {
                            String::new()
                        }
                    ),
                    budget,
                    committed + tolerance >= acked,
                ),
                _ => fail(
                    name,
                    "unknown: the last sample could not read both counts".into(),
                    budget,
                ),
            },
        );
    }
    checks.push(
        match samples
            .iter()
            .map(|s| s.database.payout_divergences)
            .collect::<Option<Vec<u64>>>()
        {
            Some(counts) => {
                let first = counts.first().copied().unwrap_or(0);
                let last = counts.last().copied().unwrap_or(0);
                verdict(
                    "payout divergences recorded during the soak",
                    format!("{}", last.saturating_sub(first)),
                    "0".into(),
                    last <= first,
                )
            }
            None => fail(
                "payout divergences recorded during the soak",
                format!(
                    "unknown in {} of {} sample(s)",
                    samples
                        .iter()
                        .filter(|s| s.database.payout_divergences.is_none())
                        .count(),
                    samples.len()
                ),
                "0".into(),
            ),
        },
    );
    checks
}

/// Latency is reported, not gated: the harness's per-phase gates and the
/// deployment's alerting hold it; a soak shows whether it drifted.
fn latency_info(after: &[&Sample]) -> Option<Check> {
    let latencies: Vec<(f64, f64)> = after
        .iter()
        .filter_map(|s| {
            let p99 = s.latency.as_ref()?.p99_ms?;
            Some((s.elapsed_seconds / 3600.0, p99))
        })
        .collect();
    let per_hour = slope(&latencies)?;
    let worst = latencies.iter().map(|p| p.1).fold(0.0, f64::max);
    Some(info(
        "share ACK p99 per sample after warm-up",
        format!(
            "slope {per_hour:+.1} ms/h over {} samples, worst {worst:.1} ms",
            latencies.len()
        ),
    ))
}

/// The Markdown report: the verdict table, then the trend of each process.
pub fn markdown(title: &str, samples: &[Sample], checks: &[Check]) -> String {
    let mut text = crate::gate::markdown(title, checks);
    text.push_str("\n#### Samples\n\n| elapsed h | phase | ");
    let names = instances(samples);
    for name in &names {
        text.push_str(&format!("{name} RSS MiB | {name} fds | "));
    }
    text.push_str("connections | WAL MiB | next share_seq |\n|---|---|");
    for _ in &names {
        text.push_str("---|---|");
    }
    text.push_str("---|---|---|\n");
    // At most 48 rows: evenly spaced, always including the last.
    let step = samples.len().div_ceil(48).max(1);
    for (index, sample) in samples.iter().enumerate() {
        if index % step != 0 && index + 1 != samples.len() {
            continue;
        }
        text.push_str(&format!(
            "| {:.2} | {} | ",
            sample.elapsed_seconds / 3600.0,
            sample.phase.as_deref().unwrap_or("")
        ));
        for name in &names {
            let process = sample.processes.iter().find(|p| p.instance == *name);
            let rss = process
                .and_then(|p| p.rss_bytes)
                .map(|b| format!("{:.1}", b as f64 / 1048576.0))
                .unwrap_or_else(|| "unknown".into());
            let fds = process
                .and_then(|p| p.open_fds)
                .map(|n| n.to_string())
                .unwrap_or_else(|| "unknown".into());
            text.push_str(&format!("{rss} | {fds} | "));
        }
        let connections = sample
            .database
            .connections
            .as_ref()
            .map(|clients| clients.values().sum::<u64>().to_string())
            .unwrap_or_else(|| "unknown".into());
        text.push_str(&format!(
            "{connections} | {} | {} |\n",
            sample
                .database
                .wal_bytes
                .map(|b| format!("{:.0}", b as f64 / 1048576.0))
                .unwrap_or_else(|| "unknown".into()),
            sample
                .database
                .next_share_seq
                .map(|n| n.to_string())
                .unwrap_or_else(|| "unknown".into()),
        ));
    }
    text
}

/// The gate over a harness soak: the samples beside the report, held to the
/// preset's soak gates, plus the retention driver's own record. The
/// harness's exit code and reconciliation are the ordinary gate's.
/// Writes the Markdown report beside the samples and returns the checks.
pub fn gate_harness_run(
    report: &serde_json::Value,
    report_dir: &Path,
    spec: &Spec,
    title: &str,
) -> Vec<Check> {
    let soak = &report["soak"];
    let mut checks = Vec::new();
    if !soak.is_object() || soak.get("samples").is_none() {
        checks.push(fail(
            "soak driver",
            format!(
                "unknown: the report has no soak record ({})",
                soak["reason"].as_str().unwrap_or("not a --plan soak run")
            ),
            "a completed soak".into(),
        ));
        return checks;
    }
    // Read from beside the report, wherever the run's directory now is.
    let path = report_dir.join(SAMPLES_FILE);
    let samples = match read_samples(&path) {
        Ok(samples) => samples,
        Err(error) => {
            checks.push(fail(
                "soak samples",
                format!("unknown: {error:#}"),
                "readable".into(),
            ));
            return checks;
        }
    };
    let errors = soak["retention_errors"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    checks.push(verdict(
        "rollover and retention commands",
        if errors.is_empty() {
            format!(
                "{} retention step(s), {} rollover(s), no error",
                soak["retention_steps"],
                soak["rollovers"].as_array().map_or(0, Vec::len)
            )
        } else {
            format!(
                "{} error(s); first: {}",
                errors.len(),
                errors[0]["error"].as_str().unwrap_or("unknown")
            )
        },
        "no error".into(),
        errors.is_empty(),
    ));
    checks.extend(evaluate(&samples, &spec.gates));
    let text = markdown(title, &samples, &checks);
    let _ = std::fs::write(report_dir.join(REPORT_FILE), &text);
    checks
}
