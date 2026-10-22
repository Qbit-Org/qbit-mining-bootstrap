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
//! state until a paging rule has fired on each server (bounded), then the
//! injector frees the space. If PostgreSQL itself stopped (here it does: a
//! `PANIC` on a WAL write, then a crash recovery that cannot write either),
//! it is started again as its supervisor would; the PRISM servers are never
//! restarted or repaired. Every phase is bounded.
//!
//! It asserts that:
//! - every share a client saw acknowledged, before, during and after the full
//!   state, is in the ledger as accepted: no acknowledgement outran its
//!   durability;
//! - the full state is refused with explicit rejections, some with the
//!   reasons `PrismShareAppendFailures` counts, and that rule's input rises;
//!   no share refused as `ledger-confirmation-failed` (the ledger did not
//!   record it) is credited; no submission waited past the answer bound;
//! - the outage pages on every server through its own rule and at least one
//!   more: each condition, mirrored from `docs/prism-native-alert-rules.json`,
//!   holds on every scrape for its `for`. Since #581 that is
//!   `PrismDatabaseUnavailable` on the live database collector, beside the
//!   share-refusal rules, whose counters are rendered at scrape time and no
//!   longer gated on the snapshot's freshness, and
//!   `PrismBlockCandidateMetricsUnavailable`; only `PrismMetricsSnapshotStale`
//!   still flaps with the snapshot's freshness;
//! - once space is freed, both original server processes accept shares again
//!   within a bound, a block found afterwards lands on the node, and the
//!   carry-forward integrity report is clean.
use super::alert_rules::{rule, scrape, snapshot_stale, Mirror, Scrape, Verdict};
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
/// recovery, each within [`STEADY_BOUND`].
const STEADY_SHARES: usize = 10;
const STEADY_BOUND: Duration = Duration::from_secs(120);
/// How long the fill may take to be refused on both servers.
const FILL_BOUND: Duration = Duration::from_secs(180);
/// How long the full state may last once both servers refuse, waiting for
/// the outage's rules to fire on each (see [`outage_paged`]): their
/// 3-minute `for` at most, plus time for PostgreSQL to go down after the
/// first refusal.
const FULL_BOUND: Duration = Duration::from_secs(360);

const REJECTIONS: &str = "qbit_prism_rejections_total";
const ACCEPTED: &str = "qbit_prism_accepted_shares_total";

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
    let reasons = rule("PrismShareAppendFailures", &[REJECTIONS, "reason_id=~"])?
        .label_alternatives("reason_id")?;
    let mirrors = paging_mirrors(&reasons)?;
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
    let result = exhaust(&mut fixture, &disk, &mut cluster, &reasons, &mirrors).await;
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

/// A test client: its server, its worker, its live session if it has one,
/// and the shares acknowledged in the current phase.
struct Miner {
    server: usize,
    username: String,
    session: Option<ShareClient>,
    accepted: usize,
}

async fn exhaust(
    f: &mut Fixture,
    disk: &DiskFullInjector,
    cluster: &mut PrivateCluster,
    reasons: &BTreeSet<String>,
    mirrors: &[Mirror],
) -> Result<()> {
    start_share_only_servers(f, &[(0, Vec::new()), (1, Vec::new())]).await?;
    let mut records = Vec::new();
    let mut miners = Vec::new();
    for server in 0..2 {
        for slot in 0..CLIENTS_PER_SERVER {
            miners.push(Miner {
                server,
                username: format!("{}.disk-{server}-{slot}", f.address),
                session: None,
                accepted: 0,
            });
        }
    }

    // Before: every client mines on a healthy database.
    let miners = steady(f, miners, Phase::Before, &mut records).await?;
    let refused_before = alert_input(f, reasons).await?;
    ensure!(
        records
            .iter()
            .all(|record| record.submitted.answer.accepted()),
        "a share was refused before the volume filled: {}",
        summary(&records)
    );

    // Full: fill the volume and keep mining until a paging rule fires on
    // both servers.
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
    let (miners, scrapes) = full(f, miners, mirrors, baseline, &mut records).await?;
    let refused_full = alert_input(f, reasons).await?;
    let postgres_alive_when_full = cluster.running()?;

    // After: free the space. PostgreSQL is started again only if it stopped.
    disk.stop()?;
    let restarted = cluster.ensure_running()?;
    for (index, server) in f.servers.iter().enumerate() {
        ensure!(
            server.child.try_wait()?.is_none(),
            "server {index} exited during the full state"
        );
    }
    let recovery_started = Instant::now();
    let miners = steady(f, miners, Phase::After, &mut records).await?;
    let recovered_in = recovery_started.elapsed();
    drop(miners);

    // A block found after the recovery lands.
    let mut miner =
        ShareClient::connect(f.stratum[0], &format!("{}.disk-block", f.address)).await?;
    let block = miner.submit(Proof::Block).await?;
    ensure!(
        block.answer.accepted(),
        "post-recovery block: {}",
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
        .filter(|record| record.submitted.answer.reason_id() == Some("ledger-confirmation-failed"))
        .filter(|record| ledger.get(&record.submitted.share_id) == Some(&true))
        .map(|record| record.submitted.share_id.clone())
        .collect();
    ensure!(
        credited_failures.is_empty(),
        "shares refused as ledger-confirmation-failed were credited: {credited_failures:?}"
    );

    // The full state was refused cleanly, the alert's input rose, and a
    // paging rule fired on every server.
    let full_records: Vec<_> = records
        .iter()
        .filter(|record| record.phase == Phase::Full)
        .collect();
    ensure!(
        full_records.iter().any(|record| record
            .submitted
            .answer
            .reason_id()
            .is_some_and(|reason| reasons.contains(reason))),
        "no share was refused with a reason {reasons:?} alerts on: {}",
        summary(&records)
    );
    let timed_out: Vec<_> = full_records
        .iter()
        .filter(|record| matches!(record.submitted.answer, Answer::TimedOut))
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
    let verdict = Verdict::of(&scrapes, &[0, 1], mirrors);
    ensure!(
        outage_paged(&verdict),
        "the outage did not page on every server through PrismDatabaseUnavailable and at least one more rule (#581):\n{}\n{}",
        verdict.report,
        gates(&scrapes)
    );
    f.integrity().await?;
    eprintln!("disk exhaustion alert conditions:\n{}", verdict.report);
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

/// Every miner mines on its server until it has `target` acknowledgements
/// in this phase (or, with `None`, until `stop`), reconnecting after a lost
/// session, and stops at `deadline` whatever it has.
async fn mine(
    f: &Fixture,
    miners: Vec<Miner>,
    phase: Phase,
    target: Option<usize>,
    deadline: Instant,
    stop: &Arc<AtomicBool>,
    records: &mut Vec<Record>,
) -> Result<Vec<Miner>> {
    let mut tasks = Vec::new();
    for mut miner in miners {
        let port = f.stratum[miner.server];
        let stop = stop.clone();
        tasks.push(tokio::spawn(async move {
            let mut submitted = Vec::new();
            miner.accepted = 0;
            while !stop.load(Ordering::SeqCst)
                && Instant::now() < deadline
                && target.is_none_or(|target| miner.accepted < target)
            {
                let mut session = match miner.session.take() {
                    Some(session) => session,
                    None => match ShareClient::connect(port, &miner.username).await {
                        Ok(session) => session,
                        Err(_) => {
                            tokio::time::sleep(Duration::from_millis(500)).await;
                            continue;
                        }
                    },
                };
                let outcome = session.submit(Proof::Share).await?;
                match outcome.answer {
                    Answer::Accepted => miner.accepted += 1,
                    // A refused miner keeps mining; this only paces the log.
                    _ => tokio::time::sleep(Duration::from_millis(20)).await,
                }
                // A session that answered is kept; a lost one is replaced.
                if matches!(outcome.answer, Answer::Accepted | Answer::Rejected(_)) {
                    miner.session = Some(session);
                }
                submitted.push(outcome);
            }
            Ok::<_, anyhow::Error>((miner, submitted))
        }));
    }
    let mut next = Vec::new();
    for task in tasks {
        let (miner, submitted) = task.await??;
        records.extend(submitted.into_iter().map(|submitted| Record {
            server: miner.server,
            phase,
            submitted,
        }));
        next.push(miner);
    }
    Ok(next)
}

/// A phase on a healthy database: every miner gets [`STEADY_SHARES`]
/// acknowledged within [`STEADY_BOUND`].
async fn steady(
    f: &Fixture,
    miners: Vec<Miner>,
    phase: Phase,
    records: &mut Vec<Record>,
) -> Result<Vec<Miner>> {
    let never = Arc::new(AtomicBool::new(false));
    let deadline = Instant::now() + STEADY_BOUND;
    let miners = mine(
        f,
        miners,
        phase,
        Some(STEADY_SHARES),
        deadline,
        &never,
        records,
    )
    .await?;
    ensure!(
        miners.iter().all(|miner| miner.accepted >= STEADY_SHARES),
        "{phase:?}: not every miner had {STEADY_SHARES} shares acknowledged within {STEADY_BOUND:?}: {}",
        summary(records)
    );
    Ok(miners)
}

/// The full state: mine while scraping both servers until a paging rule
/// fires on each, bounded by [`FILL_BOUND`] for both to refuse and
/// [`FULL_BOUND`] after that. Returns the miners and every scrape, after
/// the pre-fill `baseline`.
async fn full(
    f: &Fixture,
    miners: Vec<Miner>,
    mirrors: &[Mirror],
    baseline: Vec<Scrape>,
    records: &mut Vec<Record>,
) -> Result<(Vec<Miner>, Vec<Scrape>)> {
    let started = Instant::now();
    let stop = Arc::new(AtomicBool::new(false));
    // Refusals are observed on the servers' own counters, independent of
    // the clients' answers.
    let before: Vec<f64> = (0..2)
        .map(|server| {
            baseline
                .iter()
                .filter(|s| s.server == server)
                .map(|s| s.sum(REJECTIONS))
                .fold(0.0, f64::max)
        })
        .collect();
    let sampler = async {
        let mut scrapes = baseline;
        let mut refused_at = [None; 2];
        for round in 0u64.. {
            for (server, port) in f.api.into_iter().enumerate() {
                if let Ok(scrape) = scrape(&f.client, server, port).await {
                    if scrape.sum(REJECTIONS) > before[server] && refused_at[server].is_none() {
                        refused_at[server] = Some(Instant::now());
                    }
                    scrapes.push(scrape);
                }
            }
            let both = refused_at[0].zip(refused_at[1]).map(|(a, b)| a.max(b));
            let done = match both {
                None => started.elapsed() >= FILL_BOUND,
                // Evaluated every five seconds: the rules' `for` is minutes.
                Some(at) => {
                    at.elapsed() >= FULL_BOUND
                        || (round % 20 == 0
                            && outage_paged(&Verdict::of(&scrapes, &[0, 1], mirrors)))
                }
            };
            if done {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        stop.store(true, Ordering::SeqCst);
        (refused_at.iter().all(Option::is_some), scrapes)
    };
    let deadline = started + FILL_BOUND + FULL_BOUND + Duration::from_secs(60);
    let (miners, (refused, scrapes)) = tokio::join!(
        mine(f, miners, Phase::Full, None, deadline, &stop, records),
        sampler
    );
    let miners = miners?;
    ensure!(
        refused,
        "both servers did not refuse within {FILL_BOUND:?} of filling the volume: {}",
        summary(records)
    );
    Ok((miners, scrapes))
}

/// Whether the outage paged on every server through its own rule and at
/// least one more. Before #581 only `PrismBlockCandidateMetricsUnavailable`
/// held: the metrics snapshot is published by the health probe, which still
/// publishes when it fails, so its freshness flapped and every rule gated on
/// a fresh snapshot flapped with it.
fn outage_paged(verdict: &Verdict) -> bool {
    verdict
        .fired
        .values()
        .all(|rules| rules.contains("PrismDatabaseUnavailable") && rules.len() >= 2)
}

/// The rules that can page for this state, mirrored. With PostgreSQL down,
/// `PrismDatabaseUnavailable` holds on the database collector, which is live
/// at scrape time, as does `PrismBlockCandidateMetricsUnavailable`; the
/// share-refusal rules read the live share counters and are no longer gated
/// on the snapshot (#581); `PrismMetricsSnapshotStale` still flaps with the
/// snapshot's freshness.
fn paging_mirrors(reasons: &BTreeSet<String>) -> Result<Vec<Mirror>> {
    fn increase(base: &Scrape, s: &Scrape, reason: &str) -> f64 {
        let count = |scrape: &Scrape| {
            scrape
                .by_label(REJECTIONS, "reason_id")
                .get(reason)
                .copied()
                .unwrap_or(0.0)
        };
        count(s) - count(base)
    }
    let reasons = reasons.clone();
    Ok(vec![
        Mirror::new(
            "PrismShareAppendFailures",
            &[REJECTIONS, "> bool 0"],
            move |base, s| reasons.iter().any(|reason| increase(base, s, reason) > 0.0),
        )?,
        Mirror::new(
            "PrismRejectRatioByReasonHigh",
            &["> bool 0.05", ">= 100"],
            |base, s| {
                let total = (s.sum(ACCEPTED) - base.sum(ACCEPTED))
                    + (s.sum(REJECTIONS) - base.sum(REJECTIONS));
                total >= 100.0
                    && s.by_label(REJECTIONS, "reason_id")
                        .keys()
                        .any(|reason| increase(base, s, reason) / total > 0.05)
            },
        )?,
        snapshot_stale()?,
        Mirror::new(
            "PrismDatabaseUnavailable",
            &["collector=\"database\"} == bool 0", "up{"],
            // A scrape that answered is the rule's `up == 1`.
            |_, s| {
                s.labelled("qbit_prism_collector_available", "collector", "database") == Some(0.0)
            },
        )?,
        Mirror::new(
            "PrismBlockCandidateMetricsUnavailable",
            &["collector=\"database\"} == bool 0"],
            |_, s| {
                s.labelled("qbit_prism_collector_available", "collector", "database") == Some(0.0)
            },
        )?,
    ])
}

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

/// Counts per phase, server and answer, for reports and failures.
fn summary(records: &[Record]) -> String {
    let mut counts: BTreeMap<(Phase, usize, String), usize> = BTreeMap::new();
    for record in records {
        *counts
            .entry((
                record.phase,
                record.server,
                record.submitted.answer.to_string(),
            ))
            .or_default() += 1;
    }
    counts
        .iter()
        .map(|((phase, server, answer), count)| {
            format!("{phase:?}/server-{server} {answer}={count}")
        })
        .collect::<Vec<_>>()
        .join(", ")
}
