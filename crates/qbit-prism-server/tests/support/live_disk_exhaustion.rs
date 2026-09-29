//! #575 item 5: PostgreSQL runs out of disk while the pool is mining.
//!
//! The test's own PostgreSQL 16 cluster keeps its whole data directory
//! (tables, indexes and `pg_wal`) on the disk-full injector's small
//! filesystem, created without root (see `disk_full_injector.rs`); its log
//! and socket live off it, as an operator's would. Both servers of the live
//! fixture use that cluster.
//!
//! Test clients mine shares that are not blocks on both servers while the
//! injector fills the filesystem to `ENOSPC`, keep mining through the full
//! state for longer than the paging rules' `for`, then the injector frees the
//! space. If PostgreSQL itself stopped (here it does: a `PANIC` on a WAL
//! write, then a crash recovery that cannot write either), it is started
//! again as its supervisor would; the PRISM servers are never restarted or
//! repaired.
//!
//! It asserts that:
//! - every share a client saw acknowledged, before, during and after the full
//!   state, is in the ledger as accepted: no acknowledgement outran its
//!   durability;
//! - the full state is refused with explicit rejections, some with the
//!   reasons `PrismShareAppendFailures` counts, and that rule's input rises;
//!   no share refused as `ledger-confirmation-failed` (the ledger did not
//!   record it) is credited; no submission waited past the answer bound;
//! - a paging rule fires on every server: its condition, mirrored from
//!   `docs/prism-native-alert-rules.json`, holds on every scrape for its
//!   `for`. With PostgreSQL down that is
//!   `PrismBlockCandidateMetricsUnavailable`; the share-refusal rules and
//!   `PrismMetricsSnapshotStale` flap with the snapshot's freshness
//!   (#581);
//! - once space is freed, both original server processes accept shares again
//!   within a bound, a block found afterwards lands on the node, and the
//!   carry-forward integrity report is clean.
use super::alert_rules::{longest, rule, scrape, Rule, Scrape};
use super::disk_full_injector::DiskFullInjector;
use super::private_postgres::{ClusterOptions, PrivateCluster};
use super::share_client::{start_share_only_servers, Answer, Proof, ShareClient, Submitted};
use super::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

/// The filesystem's size. `initdb` with 1 MiB WAL segments leaves most of it
/// free for the fixture's schema, and the ballast takes the rest.
const VOLUME_MIB: u64 = 160;
/// Test clients per server.
const CLIENTS_PER_SERVER: usize = 2;
/// Shares each client must have acknowledged before the fill and after the
/// recovery.
const STEADY_SHARES: usize = 10;
/// How long mining continues once the first refusal was seen.
/// Longer than the 3-minute `for` of the rules that can page for it.
const FULL_HOLD: Duration = Duration::from_secs(200);
/// How long the fill may take to produce the first refusal.
const FILL_BOUND: Duration = Duration::from_secs(180);
/// How long both servers have to accept shares again once space is freed.
const RECOVERY_BOUND: Duration = Duration::from_secs(120);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "nightly #575: PostgreSQL's data volume fills under mining; needs fuse2fs and /dev/fuse"]
async fn postgres_volume_full_refuses_shares_cleanly_and_recovers_once_space_is_freed() -> Result<()>
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
    let bin = PathBuf::from(&inputs[2]);
    let reasons = alert_reasons()?;
    // As in the failover drills: wait for the fixture guard once before the
    // cluster's loopback connections start.
    drop(SERIAL.lock().await);
    let disk = DiskFullInjector::mount(VOLUME_MIB)?;
    let data = disk.volume().join("data");
    std::fs::create_dir(&data)?;
    // 1 MiB WAL segments, so WAL grows and recycles in small steps; the log
    // and socket stay off the volume.
    let mut cluster = PrivateCluster::start(ClusterOptions {
        bin,
        data,
        home: disk.home().to_path_buf(),
        initdb: &["--wal-segsize=1"],
        settings: "-c min_wal_size=4MB -c max_wal_size=16MB",
        env: Vec::new(),
    })?;
    let Some(mut fixture) = Fixture::open_on_database(false, false, Some(&cluster.url())).await?
    else {
        bail!("the live fixture's inputs were required above");
    };
    let result = exhaust(&mut fixture, &disk, &mut cluster, &reasons).await;
    if result.is_err() {
        eprintln!("{}", fixture.diagnostics());
        eprintln!("{}", cluster.diagnostics());
    }
    // The ballast must be gone, and the cluster up, for cleanup's DROP SCHEMA.
    let _ = disk.stop();
    let _ = cluster.ensure_running();
    let cleanup = fixture.cleanup().await;
    drop(cluster);
    drop(disk);
    result.and(cleanup)
}

/// One submission, the server it went to and the phase it was made in.
struct Record {
    server: usize,
    phase: Phase,
    submitted: Submitted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Phase {
    Before,
    Full,
    After,
}

async fn exhaust(
    f: &mut Fixture,
    disk: &DiskFullInjector,
    cluster: &mut PrivateCluster,
    reasons: &BTreeSet<String>,
) -> Result<()> {
    start_share_only_servers(f, &[(0, Vec::new()), (1, Vec::new())]).await?;
    let mut records = Vec::new();
    let mut clients = Vec::new();
    for server in 0..2 {
        for slot in 0..CLIENTS_PER_SERVER {
            let username = format!("{}.disk-{server}-{slot}", f.address);
            clients.push((server, username, None));
        }
    }

    // Before: every client mines on a healthy database.
    let stop = Arc::new(AtomicBool::new(false));
    clients = mine(
        f,
        clients,
        Phase::Before,
        Some(STEADY_SHARES),
        &stop,
        &mut records,
    )
    .await?;
    let refused_before = alert_input(f, reasons).await?;
    ensure!(
        records
            .iter()
            .all(|record| record.submitted.answer.accepted()),
        "a share was refused before the volume filled: {}",
        summary(&records)
    );

    // Full: fill the volume, keep mining until the first refusal plus the
    // hold, then stop.
    let mut baseline = Vec::new();
    for (server, port) in f.api.into_iter().enumerate() {
        baseline.push(scrape(&f.client, server, port).await?);
    }
    let free_before = disk.free_bytes()?;
    let ballast = disk.start()?;
    eprintln!(
        "disk exhaustion: {free_before} bytes were free; ballast {ballast} bytes; {} left",
        disk.free_bytes()?
    );
    let (clients, samples) = mine_until_refused(f, clients, &stop, &mut records, baseline).await?;
    let refused_full = alert_input(f, reasons).await?;
    let postgres_alive_when_full = cluster.running()?;

    // After: free the space. PostgreSQL is started again only if it stopped.
    disk.stop()?;
    let restarted = cluster.ensure_running()?;
    stop.store(false, Ordering::SeqCst);
    for (index, server) in f.servers.iter().enumerate() {
        ensure!(
            server.child.try_wait()?.is_none(),
            "server {index} exited during the full state"
        );
    }
    let recovery_started = Instant::now();
    // The bound stops the clients themselves, so none outlives the phase.
    let bound = {
        let stop = stop.clone();
        tokio::spawn(async move {
            tokio::time::sleep(RECOVERY_BOUND).await;
            stop.store(true, Ordering::SeqCst);
        })
    };
    let clients = mine(
        f,
        clients,
        Phase::After,
        Some(STEADY_SHARES),
        &stop,
        &mut records,
    )
    .await;
    bound.abort();
    let clients = clients?;
    let accepted_after = |server: usize, username: &str| {
        records
            .iter()
            .filter(|record| record.phase == Phase::After && record.server == server)
            .filter(|record| {
                record
                    .submitted
                    .share_id
                    .starts_with(&format!("{username}:"))
            })
            .filter(|record| record.submitted.answer.accepted())
            .count()
    };
    ensure!(
        clients
            .iter()
            .all(|(server, username, _)| accepted_after(*server, username) >= STEADY_SHARES),
        "servers did not accept shares within {RECOVERY_BOUND:?} of freeing space: {}",
        summary(&records)
    );
    let recovered_in = recovery_started.elapsed();
    drop(clients);

    // A block found after the recovery lands.
    let mut miner =
        ShareClient::connect(f.stratum[0], &format!("{}.disk-block", f.address)).await?;
    let block = miner.submit(Proof::Block).await?;
    ensure!(
        block.answer.accepted(),
        "post-recovery block: {:?}",
        block.answer
    );
    until("the post-recovery block on the node", 60, || async {
        Ok(f.rpc("getbestblockhash", json!([])).await? == json!(block.hash))
    })
    .await?;
    f.quiesce().await?;

    // Every acknowledged share is durable and accepted; no definite failure
    // is credited.
    let ledger: BTreeMap<String, bool> =
        sqlx::query_as::<_, (String, bool)>("SELECT share_id, accepted FROM qbit_share_ledger")
            .fetch_all(&f.pool)
            .await?
            .into_iter()
            .collect();
    let lost: Vec<_> = records
        .iter()
        .filter(|record| record.submitted.answer.accepted())
        .filter(|record| ledger.get(&record.submitted.share_id) != Some(&true))
        .map(|record| format!("{:?} {}", record.phase, record.submitted.share_id))
        .collect();
    ensure!(
        lost.is_empty(),
        "acknowledged shares missing from the ledger: {lost:?}"
    );
    let credited_failures: Vec<_> = records
        .iter()
        .filter(|record| record.submitted.answer.reason() == "ledger-confirmation-failed")
        .filter(|record| ledger.get(&record.submitted.share_id) == Some(&true))
        .map(|record| record.submitted.share_id.clone())
        .collect();
    ensure!(
        credited_failures.is_empty(),
        "shares refused as ledger-confirmation-failed were credited: {credited_failures:?}"
    );

    // The full state was refused cleanly and the alert's input rose.
    let full_records: Vec<_> = records
        .iter()
        .filter(|record| record.phase == Phase::Full)
        .collect();
    let alerted = full_records
        .iter()
        .filter(|record| reasons.contains(&record.submitted.answer.reason()))
        .count();
    ensure!(
        alerted > 0,
        "no share was refused with a reason {reasons:?} alerts on: {}",
        summary(&records)
    );
    let timed_out: Vec<_> = full_records
        .iter()
        .filter(|record| {
            matches!(&record.submitted.answer, Answer::Unanswered(why) if why.contains("no Stratum message"))
        })
        .map(|record| record.submitted.share_id.clone())
        .collect();
    ensure!(
        timed_out.is_empty(),
        "submissions waited past the answer bound: {timed_out:?}"
    );
    ensure!(
        refused_full > refused_before,
        "the PrismShareAppendFailures input did not rise: {refused_before} before, {refused_full} when full"
    );
    let alerts = alert_conditions(&samples, reasons)?;
    f.integrity().await?;
    eprintln!("disk exhaustion alert conditions:\n{alerts}");
    eprintln!(
        "disk exhaustion: {}; alert input {refused_before} -> {refused_full}; PostgreSQL {} when full{}; recovered in {:.1}s; {} ledger rows",
        summary(&records),
        if postgres_alive_when_full { "up" } else { "down" },
        if restarted { ", restarted after freeing space" } else { "" },
        recovered_in.as_secs_f64(),
        ledger.len(),
    );
    eprintln!("disk exhaustion PostgreSQL log:\n{}", cluster.errors());
    Ok(())
}

/// Every client mines on its server until it has `target` more
/// acknowledgements (or, with `None`, until `stop`), reconnecting after a
/// lost session. Returns the clients for the next phase.
async fn mine(
    f: &Fixture,
    clients: Vec<(usize, String, Option<ShareClient>)>,
    phase: Phase,
    target: Option<usize>,
    stop: &Arc<AtomicBool>,
    records: &mut Vec<Record>,
) -> Result<Vec<(usize, String, Option<ShareClient>)>> {
    let mut tasks = Vec::new();
    for (server, username, client) in clients {
        let port = f.stratum[server];
        let stop = stop.clone();
        tasks.push(tokio::spawn(async move {
            let mut client = client;
            let mut submitted = Vec::new();
            let mut accepted = 0;
            while !stop.load(Ordering::SeqCst) && target.is_none_or(|target| accepted < target) {
                let mut session = match client.take() {
                    Some(session) => session,
                    None => match ShareClient::connect(port, &username).await {
                        Ok(session) => session,
                        Err(_) => {
                            tokio::time::sleep(Duration::from_millis(500)).await;
                            continue;
                        }
                    },
                };
                let outcome = session.submit(Proof::Share).await?;
                if outcome.answer.accepted() {
                    accepted += 1;
                } else {
                    // A refused miner keeps mining; this only paces the log.
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                // A lost session is replaced; a live one is kept.
                if !matches!(outcome.answer, Answer::Unanswered(_)) {
                    client = Some(session);
                }
                submitted.push(outcome);
            }
            Ok::<_, anyhow::Error>((server, username, client, submitted))
        }));
    }
    let mut next = Vec::new();
    for task in tasks {
        let (server, username, client, submitted) = task.await??;
        records.extend(submitted.into_iter().map(|submitted| Record {
            server,
            phase,
            submitted,
        }));
        next.push((server, username, client));
    }
    Ok(next)
}

/// Mine in the full phase until the servers' first refusal plus
/// [`FULL_HOLD`], or fail after [`FILL_BOUND`] without one. Returns the
/// clients and every scrape taken meanwhile, after one per server from
/// before the fill.
async fn mine_until_refused(
    f: &Fixture,
    clients: Vec<(usize, String, Option<ShareClient>)>,
    stop: &Arc<AtomicBool>,
    records: &mut Vec<Record>,
    baseline: Vec<Scrape>,
) -> Result<(Vec<(usize, String, Option<ShareClient>)>, Vec<Scrape>)> {
    let started = Instant::now();
    // The refusal is observed on the servers' own counters, independent of
    // the clients' answers.
    let sampler = {
        let stop = stop.clone();
        let ports = f.api;
        let client = f.client.clone();
        tokio::spawn(async move {
            // Each server's refusals before the fill, and when it first
            // refused more: the hold runs from the later server's.
            let before: Vec<f64> = (0..2)
                .map(|server| {
                    baseline
                        .iter()
                        .filter(|s| s.server == server)
                        .map(|s| s.sum(REJECTIONS))
                        .fold(0.0, f64::max)
                })
                .collect();
            let mut scrapes = baseline;
            let mut refused_at = [None; 2];
            while !stop.load(Ordering::SeqCst) {
                for (server, port) in ports.into_iter().enumerate() {
                    if let Ok(scrape) = scrape(&client, server, port).await {
                        if scrape.sum(REJECTIONS) > before[server] && refused_at[server].is_none() {
                            refused_at[server] = Some(Instant::now());
                        }
                        scrapes.push(scrape);
                    }
                }
                let last = refused_at[0].zip(refused_at[1]).map(|(a, b)| a.max(b));
                let expired = last.is_some_and(|at| at.elapsed() >= FULL_HOLD)
                    || (last.is_none() && started.elapsed() >= FILL_BOUND);
                if expired {
                    stop.store(true, Ordering::SeqCst);
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            (refused_at.iter().all(Option::is_some), scrapes)
        })
    };
    let clients = mine(f, clients, Phase::Full, None, stop, records).await;
    stop.store(true, Ordering::SeqCst);
    let (refused, scrapes) = sampler.await?;
    let clients = clients?;
    ensure!(
        refused,
        "both servers did not refuse within {FILL_BOUND:?} of filling the volume: {}",
        summary(records)
    );
    Ok((clients, scrapes))
}

const REJECTIONS: &str = "qbit_prism_rejections_total";
/// The `[5m]` range of the rules' `increase`.
const INCREASE_WINDOW: Duration = Duration::from_secs(300);
const ACCEPTED: &str = "qbit_prism_accepted_shares_total";

/// How each server's snapshot gate and database collector changed.
fn gates(scrapes: &[Scrape]) -> String {
    (0..2)
        .map(|server| {
            let mut changes = Vec::new();
            let mut previous = None;
            for scrape in scrapes.iter().filter(|s| s.server == server) {
                let state = (
                    scrape.value("qbit_prism_metrics_snapshot_available"),
                    scrape.value("qbit_prism_metrics_snapshot_stale"),
                    scrape.labelled("qbit_prism_collector_available", "collector", "database"),
                );
                if previous != Some(state) {
                    changes.push(format!("{state:?}"));
                    previous = Some(state);
                }
            }
            format!(
                "server-{server} (snapshot available, stale, database collector): {}",
                changes.join(" -> ")
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The rules that can page for this state, and whether each fires on each
/// server: its condition, mirrored here and evaluated on every scrape, holds
/// for at least its `for`. Their `[5m]` increases are measured over a
/// sliding five minutes, from the pre-fill scrape at first. At least one
/// must fire on every server.
///
/// With PostgreSQL down, only `PrismBlockCandidateMetricsUnavailable` holds:
/// the metrics snapshot is published by the health probe, which still
/// publishes when it fails, so its freshness flaps and every rule gated on a
/// fresh snapshot flaps with it (#581).
fn alert_conditions(scrapes: &[Scrape], reasons: &BTreeSet<String>) -> Result<String> {
    type Condition<'a> = Box<dyn Fn(&Scrape, &Scrape) -> bool + 'a>;
    let increase = |base: &Scrape, s: &Scrape, reason: &str| {
        s.by_label(REJECTIONS, "reason_id")
            .get(reason)
            .copied()
            .unwrap_or(0.0)
            - base
                .by_label(REJECTIONS, "reason_id")
                .get(reason)
                .copied()
                .unwrap_or(0.0)
    };
    let rules: Vec<(Rule, Condition)> = vec![
        (
            rule(
                "PrismShareAppendFailures",
                &[REJECTIONS, "> bool 0", "qbit_prism_metrics_snapshot_stale"],
            )?,
            Box::new(|base, s| {
                reasons.iter().any(|reason| increase(base, s, reason) > 0.0) && s.gate_open()
            }),
        ),
        (
            rule(
                "PrismRejectRatioByReasonHigh",
                &["> bool 0.05", ">= 100", "qbit_prism_metrics_snapshot_stale"],
            )?,
            Box::new(|base, s| {
                let refused = s.by_label(REJECTIONS, "reason_id");
                let total = (s.sum(ACCEPTED) - base.sum(ACCEPTED))
                    + (s.sum(REJECTIONS) - base.sum(REJECTIONS));
                total >= 100.0
                    && s.gate_open()
                    && refused
                        .keys()
                        .any(|reason| increase(base, s, reason) / total > 0.05)
            }),
        ),
        (
            rule(
                "PrismMetricsSnapshotStale",
                &["qbit_prism_metrics_snapshot_stale", "> bool 0"],
            )?,
            Box::new(|_, s| {
                s.value("qbit_prism_metrics_snapshot_stale")
                    .is_some_and(|stale| stale > 0.0)
            }),
        ),
        (
            rule(
                "PrismBlockCandidateMetricsUnavailable",
                &["collector=\"database\"} == bool 0"],
            )?,
            Box::new(|_, s| {
                s.labelled("qbit_prism_collector_available", "collector", "database") == Some(0.0)
            }),
        ),
    ];
    let mut report = Vec::new();
    let mut firing = [false; 2];
    for (rule, condition) in &rules {
        let mut spans = Vec::new();
        for (server, fired) in firing.iter_mut().enumerate() {
            let series: Vec<_> = scrapes.iter().filter(|s| s.server == server).collect();
            let first = *series
                .first()
                .with_context(|| format!("server {server} was never scraped"))?;
            // `increase(...[5m])` at a scrape: from the last scrape at least
            // five minutes older, or the pre-fill one while there is none.
            let window_base = |s: &Scrape| {
                series
                    .iter()
                    .copied()
                    .take_while(|older| older.at + INCREASE_WINDOW <= s.at)
                    .last()
                    .unwrap_or(first)
            };
            let held = longest(series.iter().copied(), |s| condition(window_base(s), s));
            *fired |= held >= rule.hold;
            spans.push(format!(
                "server-{server} {:.0}s{}",
                held.as_secs_f64(),
                if held >= rule.hold { " FIRES" } else { "" }
            ));
        }
        report.push(format!(
            "{} (for {:?}): {}",
            rule.title,
            rule.hold,
            spans.join(", ")
        ));
    }
    ensure!(
        firing.iter().all(|fired| *fired),
        "no paging rule fires on every server for the full state:\n{}\n{}",
        report.join("\n"),
        gates(scrapes)
    );
    Ok(report.join("\n"))
}

/// The `PrismShareAppendFailures` input across both servers.
async fn alert_input(f: &Fixture, reasons: &BTreeSet<String>) -> Result<f64> {
    let mut total = 0.0;
    for (server, port) in f.api.into_iter().enumerate() {
        let refused = scrape(&f.client, server, port)
            .await?
            .by_label(REJECTIONS, "reason_id");
        total += reasons
            .iter()
            .filter_map(|reason| refused.get(reason))
            .sum::<f64>();
    }
    Ok(total)
}

/// The rejection reasons `PrismShareAppendFailures` counts, from its rule.
fn alert_reasons() -> Result<BTreeSet<String>> {
    let rule = rule("PrismShareAppendFailures", &[REJECTIONS, "reason_id=~\""])?;
    let (_, rest) = rule
        .expr
        .split_once("reason_id=~\"")
        .context("PrismShareAppendFailures has no reason_id matcher")?;
    let (alternatives, _) = rest.split_once('"').context("unterminated matcher")?;
    Ok(alternatives.split('|').map(str::to_owned).collect())
}

/// Counts per phase and answer, for reports and failures.
fn summary(records: &[Record]) -> String {
    let mut counts: BTreeMap<(Phase, usize, String), usize> = BTreeMap::new();
    for record in records {
        let reason = match &record.submitted.answer {
            Answer::Unanswered(why) if why.contains("no Stratum message") => {
                "unanswered: timeout".into()
            }
            Answer::Unanswered(_) => "unanswered: connection lost".into(),
            answer => answer.reason(),
        };
        *counts
            .entry((record.phase, record.server, reason))
            .or_default() += 1;
    }
    counts
        .iter()
        .map(|((phase, server, reason), count)| {
            format!("{phase:?}/server-{server} {reason}={count}")
        })
        .collect::<Vec<_>>()
        .join(", ")
}
