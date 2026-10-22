//! The load harness's `--plan soak` (#575 item 2): one server lifetime,
//! hours long, looping checked-in presets' workloads over it.
//!
//! **Plan.** A soak preset carries a `soak` block ([`crate::soak::Spec`])
//! naming the presets to loop. Each looped preset contributes its
//! [`WORKLOAD_FLAGS`](crate::soak::WORKLOAD_FLAGS) -- its plan, rates,
//! arrival, tips, blocks, churn -- over the soak preset's own server,
//! cluster and population, and its phases run exactly as its own run would
//! drive them, with three changes: no phase restarts or kills a frontend
//! (the reconnect phase keeps its client reconnects; `--mid-flight-kill` is
//! refused), the deliberately degraded `slow_database` phase is left out,
//! and every phase is named `c<cycle>.<preset>.<phase>` so each cycle's
//! numbers stay its own. Whole cycles are planned while they fit in
//! `soak.minutes`.
//!
//! **Samples.** Every `soak.sample_seconds` the [sampler](SoakDriver) writes
//! one [`Sample`](crate::soak::Sample) line: each frontend's resident memory,
//! open descriptors and threads from `/proc`, the database's connections per
//! frontend, WAL size, partition and relation sizes, the share sequence, and
//! the ACK latency of the shares answered since the last sample.
//!
//! **Rollover.** A share ledger partition is 2^24 rows (`partition_rows`),
//! days of a soak's load, so at each of `soak.rollover_minutes` the share
//! sequence is advanced to `soak.rollover_margin_rows` below the bound of the
//! partition it is in, as the partition tests do (`setval` is
//! nontransactional and only ever moves the sequence up here). The live load
//! then crosses the bound itself, into the lead partition, and the
//! frontends' maintenance attaches a new lead within
//! `soak.partition_ensure_interval_seconds`. `share_seq` already has gaps
//! (a rolled-back append's value is never reused), and every reader walks it
//! in order rather than by count.
//!
//! **Retention.** Every `soak.archive_every_minutes` the operator's own
//! `qbit-prism-server share-archive` commands run against the live cluster,
//! as the retention procedure describes: `seal`, `archive` and `verify` each
//! partition the sequence has passed, then `detach` and `drop` those `plan`
//! reports eligible at `soak.archive_window_multiple` and
//! `soak.archive_retention_days`. A command that fails is an event with its
//! output, and the gate fails the soak on any.

use crate::cli::{Args, PhasePlan};
use crate::soak::{self, Sample, Spec};
use anyhow::{bail, ensure, Context, Result};
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::watch;

/// The application name the retention commands connect with, so the sampler
/// can tell them from the frontends.
pub const ARCHIVE_INSTANCE: &str = "load-soak-archive";

/// The longest one `share-archive` command may take before it is killed and
/// counted as a failure: a hung command would otherwise hold the soak's
/// teardown until the job's own timeout (EP-ERRORS). Every step is
/// resumable, so a killed one leaves nothing a later pass cannot finish.
pub const SHARE_ARCHIVE_TIMEOUT: Duration = Duration::from_secs(600);

/// The phase a soak leaves out of every looped preset.
pub const SLOW_DATABASE: &str = "slow_database";

/// One planned soak phase: the phase, and the arguments of the preset whose
/// workload it runs.
#[derive(Clone, Debug)]
pub struct SoakPhase {
    pub plan: PhasePlan,
    pub args: Arc<Args>,
}

/// The soak's phases and what one cycle is.
#[derive(Clone, Debug)]
pub struct SoakPlan {
    pub spec: Spec,
    pub phases: Vec<SoakPhase>,
    pub cycles: usize,
    pub cycle_seconds: u64,
}

/// The soak preset's flags with one looped preset's workload flags laid over
/// them, parsed and validated as a command line.
pub fn segment_args(
    soak_args: &BTreeMap<String, Value>,
    looped: &crate::preset::Preset,
) -> Result<Args> {
    use clap::Parser;
    let mut merged = soak_args.clone();
    for flag in soak::WORKLOAD_FLAGS {
        let value = looped
            .args
            .get(*flag)
            .with_context(|| format!("preset {} does not pin {flag}", looped.name))?;
        merged.insert((*flag).to_owned(), value.clone());
    }
    let preset = crate::preset::Preset {
        args: merged,
        ..looped.clone()
    };
    let mut argv = vec!["qbit-prism-load".to_owned()];
    argv.extend(preset.argv()?);
    let args = Args::try_parse_from(&argv)
        .with_context(|| format!("the {} workload over the soak preset", looped.name))?;
    args.validate()
        .with_context(|| format!("the {} workload over the soak preset", looped.name))?;
    Ok(args)
}

/// Plan a soak from its preset: the looped presets are read from the soak
/// preset's own directory.
pub fn plan(run_args: &Args, preset: &crate::preset::Preset) -> Result<SoakPlan> {
    let spec = preset
        .soak
        .clone()
        .context("--plan soak needs a preset with a soak block")?;
    let dir = preset
        .path
        .parent()
        .context("the soak preset has no directory")?;
    let mut cycle: Vec<(String, Vec<PhasePlan>, Arc<Args>)> = Vec::new();
    for name in &spec.presets {
        let looped = crate::preset::Preset::load(&dir.join(format!("{name}.json")))?;
        ensure!(
            looped.soak.is_none(),
            "soak preset {} loops {name}, which is itself a soak",
            preset.name
        );
        let args = segment_args(&preset.args, &looped)?;
        ensure!(
            !args.mid_flight_kill,
            "{name} kills a frontend (--mid-flight-kill), which would end the soak's one \
             server lifetime"
        );
        ensure!(
            args.faults.is_none(),
            "{name} injects faults (--faults), which a soak cycle does not drive yet (#556)"
        );
        // The frontends are launched once, with listener limits sized from
        // the soak preset's own flags; a looped workload that could connect
        // more sessions at once than those limits admit is refused here
        // rather than turned away by the server mid-soak (EP-CONFIG).
        ensure!(
            args.peak_sessions() <= run_args.peak_sessions(),
            "{name} can connect {} sessions at once, more than the {} the soak preset's \
             frontends are sized for; raise the soak preset's --rental-bursts or \
             --churn-seconds",
            args.peak_sessions(),
            run_args.peak_sessions()
        );
        // The deliberately degraded database phase is left out: its backlog
        // is its own measurement, and in a loop it has to drain before the
        // next cycle starts, which it can outlast (the harness then aborts
        // rather than let its submits finish under the next phase).
        let phases: Vec<PhasePlan> = crate::cli::phases(&args)?
            .into_iter()
            .filter(|phase| phase.kind != SLOW_DATABASE)
            .collect();
        cycle.push((name.clone(), phases, Arc::new(args)));
    }
    let cycle_seconds: u64 = cycle
        .iter()
        .flat_map(|(_, phases, _)| phases.iter().map(|phase| phase.seconds))
        .sum();
    ensure!(cycle_seconds > 0, "the looped presets drive no phase");
    let cycles = (spec.minutes * 60 / cycle_seconds) as usize;
    ensure!(
        cycles >= 1,
        "soak.minutes ({}) is shorter than one cycle of the looped presets ({} s)",
        spec.minutes,
        cycle_seconds
    );
    let mut phases = Vec::new();
    for index in 1..=cycles {
        for (name, plans, args) in &cycle {
            for plan in plans {
                let kind = plan.kind.clone();
                phases.push(SoakPhase {
                    plan: PhasePlan {
                        name: format!("c{index:02}.{name}.{kind}"),
                        kind,
                        in_artifact: false,
                        restart_frontend: false,
                        ..plan.clone()
                    },
                    args: args.clone(),
                });
            }
        }
    }
    Ok(SoakPlan {
        spec,
        phases,
        cycles,
        cycle_seconds,
    })
}

/// Whether a sample taken during `phase` sees the base session population:
/// not during churn, when rentals come and go.
pub fn steady_phase(phase: &str) -> bool {
    !phase.ends_with(&format!(".{}", crate::churn::PHASE))
        && phase != crate::churn::PHASE
        && phase != "setup"
        && phase != "teardown"
}

#[derive(Serialize)]
struct Event<'a> {
    at: chrono::DateTime<chrono::Utc>,
    elapsed_seconds: f64,
    kind: &'a str,
    #[serde(flatten)]
    detail: Value,
}

/// What the background tasks share with the phase loop.
pub struct SoakInputs {
    pub spec: Spec,
    pub out: PathBuf,
    pub archive_dir: PathBuf,
    /// Each frontend's instance id, PID and metrics URL.
    pub frontends: Vec<(String, Option<u32>, String)>,
    pub side: PgPool,
    pub direct_url: String,
    pub server_bin: PathBuf,
    /// `share-archive --network-difficulty`.
    pub network_difficulty: String,
    pub collected: Arc<Mutex<crate::run::Collected>>,
    pub phase: Arc<crate::client::SessionShared>,
}

/// What the tasks did, for the side report and the gate.
#[derive(Clone, Debug, Default, Serialize)]
pub struct SoakSummary {
    pub samples: usize,
    pub rollovers: Vec<Value>,
    pub retention_steps: usize,
    pub retention_errors: Vec<Value>,
    pub dropped: Vec<String>,
    /// The partitions that left the ledger, read by the run's
    /// reconciliation: detached ones as they stand, dropped ones restored
    /// from their archives, un-attached.
    pub restored: Vec<String>,
}

/// The sampler and the rollover/retention task, running until stopped.
pub struct SoakDriver {
    stop: watch::Sender<bool>,
    sampler: tokio::task::JoinHandle<Result<usize>>,
    maintenance: tokio::task::JoinHandle<SoakSummary>,
    events: PathBuf,
    started: Instant,
}

impl SoakDriver {
    pub fn start(inputs: SoakInputs) -> Result<Self> {
        std::fs::create_dir_all(&inputs.archive_dir)
            .with_context(|| format!("creating {}", inputs.archive_dir.display()))?;
        let samples = inputs.out.join(soak::SAMPLES_FILE);
        let events = inputs.out.join(soak::EVENTS_FILE);
        for path in [&samples, &events] {
            std::fs::write(path, b"").with_context(|| format!("creating {}", path.display()))?;
        }
        let (stop, stopped) = watch::channel(false);
        let started = Instant::now();
        let inputs = Arc::new(inputs);
        let sampler = tokio::spawn(sample_loop(
            inputs.clone(),
            samples,
            started,
            stopped.clone(),
        ));
        let maintenance = tokio::spawn(maintenance_loop(inputs, events.clone(), started, stopped));
        Ok(Self {
            stop,
            sampler,
            maintenance,
            events,
            started,
        })
    }

    /// Stop both tasks and return what they did. A sampler that failed is an
    /// error: its samples would be the gate's whole input.
    pub async fn finish(self) -> Result<SoakSummary> {
        let _ = self.stop.send(true);
        let samples = self
            .sampler
            .await
            .context("the soak sampler panicked")?
            .context("the soak sampler failed")?;
        let mut summary = self
            .maintenance
            .await
            .context("the soak retention task panicked")?;
        summary.samples = samples;
        let _ = soak::append_line(
            &self.events,
            &Event {
                at: chrono::Utc::now(),
                elapsed_seconds: self.started.elapsed().as_secs_f64(),
                kind: "finished",
                detail: json!({"samples": samples}),
            },
        );
        Ok(summary)
    }
}

async fn sample_loop(
    inputs: Arc<SoakInputs>,
    path: PathBuf,
    started: Instant,
    mut stopped: watch::Receiver<bool>,
) -> Result<usize> {
    let interval = Duration::from_secs(inputs.spec.sample_seconds);
    let names: Vec<String> = inputs
        .frontends
        .iter()
        .map(|(name, _, _)| name.clone())
        .collect();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let mut cursor = 0usize;
    let mut last = started;
    let mut count = 0usize;
    loop {
        let phase = inputs
            .phase
            .phase
            .read()
            .map(|phase| phase.clone())
            .unwrap_or_default();
        let mut processes = Vec::new();
        for (name, pid, metrics_url) in &inputs.frontends {
            let mut point = soak::process_point(name, *pid);
            if point.open_fds.is_none() {
                // The server makes itself non-dumpable, so its descriptor
                // directory is closed to every other process of its user;
                // it counts its own and exports the count.
                match own_open_fds(&http, metrics_url).await {
                    Ok(fds) => {
                        point.open_fds = Some(fds);
                        point.unknown.retain(|note| !note.contains("/fd"));
                    }
                    Err(error) => point.unknown.push(format!("open_fds: {error:#}")),
                }
            }
            processes.push(point);
        }
        let database = soak::database_point(&inputs.side, Some(&names)).await;
        let now = Instant::now();
        let latency = {
            let collected = inputs.collected.lock().expect("collector lock");
            let fresh = &collected.submits[cursor.min(collected.submits.len())..];
            cursor = collected.submits.len();
            let latencies: Vec<f64> = fresh
                .iter()
                .filter(|record| matches!(record.outcome, crate::client::Outcome::Accepted))
                .filter_map(|record| record.latency_millis)
                .collect();
            soak::LatencyPoint {
                source: "harness clients: mining.submit sent to answer read".into(),
                interval_seconds: now.duration_since(last).as_secs_f64(),
                acknowledged: Some(latencies.len() as u64),
                p50_ms: crate::gate::nearest_rank(&latencies, 0.50),
                p99_ms: crate::gate::nearest_rank(&latencies, 0.99),
            }
        };
        last = now;
        let sample = Sample {
            schema: soak::SAMPLE_SCHEMA.into(),
            at: chrono::Utc::now(),
            elapsed_seconds: started.elapsed().as_secs_f64(),
            steady: steady_phase(&phase),
            phase: Some(phase),
            processes,
            database,
            latency: Some(latency),
            ledger: None,
        };
        soak::append_line(&path, &sample)?;
        count += 1;
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = stopped.changed() => return Ok(count),
        }
    }
}

async fn maintenance_loop(
    inputs: Arc<SoakInputs>,
    events: PathBuf,
    started: Instant,
    mut stopped: watch::Receiver<bool>,
) -> SoakSummary {
    let mut summary = SoakSummary::default();
    let mut rollovers = inputs.spec.rollover_minutes.clone().into_iter().peekable();
    let archive_every = Duration::from_secs_f64(inputs.spec.archive_every_minutes * 60.0);
    let mut next_archive = archive_every;
    let log = |kind: &str, detail: Value| {
        let _ = soak::append_line(
            &events,
            &Event {
                at: chrono::Utc::now(),
                elapsed_seconds: started.elapsed().as_secs_f64(),
                kind,
                detail,
            },
        );
    };
    loop {
        let elapsed = started.elapsed();
        if let Some(minute) = rollovers.peek().copied() {
            if elapsed.as_secs_f64() >= minute * 60.0 {
                rollovers.next();
                match rollover(&inputs.side, inputs.spec.rollover_margin_rows).await {
                    Ok(detail) => {
                        log("rollover", detail.clone());
                        summary.rollovers.push(detail);
                    }
                    Err(error) => {
                        let detail = json!({"step": "rollover", "error": format!("{error:#}")});
                        log("error", detail.clone());
                        summary.retention_errors.push(detail);
                    }
                }
            }
        }
        if elapsed >= next_archive {
            next_archive += archive_every;
            retention_pass(&inputs, &mut summary, &log).await;
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            _ = stopped.changed() => return summary,
        }
    }
}

/// The frontend's own `qbit_prism_process_open_fds`; an unknown (-1) or
/// missing gauge is an error, never zero.
async fn own_open_fds(http: &reqwest::Client, url: &str) -> Result<u64> {
    let body = http
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let value: f64 = body
        .lines()
        .find_map(|line| line.strip_prefix("qbit_prism_process_open_fds "))
        .context("no qbit_prism_process_open_fds sample")?
        .trim()
        .parse()?;
    ensure!(
        value >= 0.0,
        "the server reports its descriptors unknown ({value})"
    );
    Ok(value as u64)
}

/// Advance the share sequence to `margin` rows below the bound of the
/// partition it is in. Never moves it down.
pub async fn rollover(side: &PgPool, margin: i64) -> Result<Value> {
    let next: i64 = sqlx::query_scalar("SELECT qbit_prism_share_next_seq()")
        .fetch_one(side)
        .await?;
    let bound: i64 = sqlx::query_scalar(
        "SELECT upper_seq FROM qbit_prism_share_partitions WHERE state = 'attached' \
         AND (lower_seq IS NULL OR lower_seq <= $1) AND upper_seq > $1 \
         ORDER BY upper_seq LIMIT 1",
    )
    .bind(next)
    .fetch_optional(side)
    .await?
    .context("no attached partition holds the next share_seq")?;
    let target = bound - margin;
    ensure!(
        target > next,
        "the share sequence ({next}) is already within {margin} rows of its partition's bound \
         ({bound})"
    );
    // setval(v) makes the next nextval return v + 1. Nothing else moves the
    // sequence but nextval, which only raises it, so a value above the one
    // just read never hands out a share_seq twice.
    sqlx::query("SELECT setval('qbit_share_ledger_share_seq_seq', $1)")
        .bind(target - 1)
        .execute(side)
        .await?;
    let after: i64 = sqlx::query_scalar("SELECT qbit_prism_share_next_seq()")
        .fetch_one(side)
        .await?;
    ensure!(
        after >= target,
        "the share sequence reads {after} after it was advanced to {target}"
    );
    Ok(json!({"from_next_share_seq": next, "to_next_share_seq": after, "bound": bound}))
}

/// One `qbit-prism-server share-archive` command, its JSON result or the
/// reason it failed.
async fn share_archive(inputs: &SoakInputs, words: &[&str]) -> Result<Value> {
    share_archive_command(&inputs.server_bin, &inputs.direct_url, words).await
}

async fn share_archive_command(
    server_bin: &Path,
    direct_url: &str,
    words: &[&str],
) -> Result<Value> {
    let command = tokio::process::Command::new(server_bin)
        .arg("share-archive")
        .args(words)
        .env(
            "PRISM_DATABASE_URL",
            crate::run::with_application_name(direct_url, ARCHIVE_INSTANCE),
        )
        .env("PRISM_INSTANCE_ID", ARCHIVE_INSTANCE)
        .kill_on_drop(true)
        .output();
    // kill_on_drop: the timeout dropping the future kills the child.
    let output = tokio::time::timeout(SHARE_ARCHIVE_TIMEOUT, command)
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "share-archive {} did not finish within {} s",
                words.join(" "),
                SHARE_ARCHIVE_TIMEOUT.as_secs()
            )
        })?
        .context("running qbit-prism-server share-archive")?;
    if !output.status.success() {
        bail!(
            "share-archive {} exited {}: {}",
            words.join(" "),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    serde_json::from_slice(&output.stdout).with_context(|| {
        format!(
            "share-archive {} printed no JSON: {}",
            words.join(" "),
            String::from_utf8_lossy(&output.stdout).trim()
        )
    })
}

async fn plan_partitions(inputs: &SoakInputs) -> Result<Value> {
    let multiple = inputs.spec.archive_window_multiple.to_string();
    let days = inputs.spec.archive_retention_days.to_string();
    share_archive(
        inputs,
        &[
            "plan",
            "--network-difficulty",
            &inputs.network_difficulty,
            "--retention-days",
            &days,
            "--window-multiple",
            &multiple,
        ],
    )
    .await
}

/// The operator's retention procedure, once over every partition it can
/// advance; the first failure ends the pass.
async fn retention_pass(
    inputs: &SoakInputs,
    summary: &mut SoakSummary,
    log: &impl Fn(&str, Value),
) {
    let dir = inputs.archive_dir.display().to_string();
    let multiple = inputs.spec.archive_window_multiple.to_string();
    let days = inputs.spec.archive_retention_days.to_string();
    let result: Result<()> = async {
        let plan = plan_partitions(inputs).await?;
        let next = plan["next_share_seq"]
            .as_i64()
            .context("share-archive plan reported no next_share_seq")?;
        let partitions = plan["partitions"]
            .as_array()
            .context("share-archive plan reported no partitions")?
            .clone();
        // Seal, archive and verify, in bound order, every attached partition
        // the sequence has passed: archive refuses one out of order.
        for partition in &partitions {
            // `plan` flattens each catalog record into its partition object.
            let record = partition;
            let name = record["partition_name"].as_str().unwrap_or_default();
            let upper = record["upper_seq"].as_i64().unwrap_or(i64::MAX);
            if record["state"] != "attached" || upper > next {
                continue;
            }
            if record["sealed_at"].is_null() {
                share_archive(inputs, &["seal", name]).await?;
                summary.retention_steps += 1;
                log("seal", json!({"partition": name}));
            }
            if record["archived_at"].is_null() {
                let result = share_archive(inputs, &["archive", name, "--dir", &dir]).await?;
                summary.retention_steps += 1;
                log("archive", json!({"partition": name, "result": result}));
            }
            if record["archive_verified_at"].is_null() {
                share_archive(inputs, &["verify", name, "--dir", &dir]).await?;
                summary.retention_steps += 1;
                log("verify", json!({"partition": name}));
            }
        }
        // Detach what the plan now clears, then drop every detached one.
        let plan = plan_partitions(inputs).await?;
        for partition in plan["partitions"].as_array().into_iter().flatten() {
            // `plan` flattens each catalog record into its partition object.
            let record = partition;
            let name = record["partition_name"].as_str().unwrap_or_default();
            if record["state"] == "attached" && partition["eligible"] == true {
                share_archive(
                    inputs,
                    &[
                        "detach",
                        name,
                        "--network-difficulty",
                        &inputs.network_difficulty,
                        "--retention-days",
                        &days,
                        "--window-multiple",
                        &multiple,
                    ],
                )
                .await?;
                summary.retention_steps += 1;
                log("detach", json!({"partition": name}));
                share_archive(inputs, &["drop", name, "--dir", &dir]).await?;
                summary.retention_steps += 1;
                summary.dropped.push(name.to_owned());
                log("drop", json!({"partition": name}));
            } else if record["state"] == "detached" {
                share_archive(inputs, &["drop", name, "--dir", &dir]).await?;
                summary.retention_steps += 1;
                summary.dropped.push(name.to_owned());
                log("drop", json!({"partition": name}));
            } else if record["state"] == "attached"
                && record["upper_seq"]
                    .as_i64()
                    .is_some_and(|upper| upper <= next)
            {
                log(
                    "waiting",
                    json!({"partition": name, "blockers": partition["blockers"]}),
                );
            }
        }
        Ok(())
    }
    .await;
    if let Err(error) = result {
        let detail = json!({"step": "retention", "error": format!("{error:#}")});
        log("error", detail.clone());
        summary.retention_errors.push(detail);
    }
}

/// Every partition that left the ledger during the soak, readable again for
/// the run's reconciliation: a detached one is still a standalone table, and
/// a dropped one is restored from its archive, un-attached, with the
/// operator's `share-archive restore`. The run then reads them beside the
/// live ledger, so a share acknowledged during the soak has to be in
/// PostgreSQL or in a verified archive that restores row for row. Returns
/// the tables, in bound order.
pub async fn restore_dropped(
    server_bin: &Path,
    direct_url: &str,
    archive_dir: &Path,
    side: &PgPool,
) -> Result<Vec<String>> {
    let left: Vec<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT partition_name, state, archive_uri FROM qbit_prism_share_partitions \
         WHERE state IN ('detached', 'dropped') ORDER BY upper_seq",
    )
    .fetch_all(side)
    .await
    .context("reading the partitions that left the ledger")?;
    let dir = archive_dir.display().to_string();
    let mut tables = Vec::new();
    for (name, state, uri) in left {
        ensure!(
            is_partition_name(&name),
            "the catalog names a partition {name:?} this harness will not interpolate"
        );
        if state == "dropped" {
            let uri =
                uri.with_context(|| format!("{name} was dropped with no archive recorded"))?;
            share_archive_command(server_bin, direct_url, &["restore", &uri, "--dir", &dir])
                .await
                .with_context(|| format!("restoring {name} for reconciliation"))?;
        }
        tables.push(name);
    }
    Ok(tables)
}

/// `qbit_share_ledger_p<n>`, the only relation names a partition carries.
pub fn is_partition_name(name: &str) -> bool {
    name.strip_prefix("qbit_share_ledger_p")
        .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
}

/// The soak section of the side report.
pub fn report(plan: &SoakPlan, summary: &SoakSummary, out: &Path) -> Value {
    json!({
        "spec": plan.spec,
        "cycles": plan.cycles,
        "cycle_seconds": plan.cycle_seconds,
        "phases": plan.phases.len(),
        "samples_file": out.join(soak::SAMPLES_FILE).display().to_string(),
        "events_file": out.join(soak::EVENTS_FILE).display().to_string(),
        "samples": summary.samples,
        "rollovers": summary.rollovers,
        "retention_steps": summary.retention_steps,
        "retention_errors": summary.retention_errors,
        "dropped": summary.dropped,
        "restored_for_reconciliation": summary.restored,
    })
}
