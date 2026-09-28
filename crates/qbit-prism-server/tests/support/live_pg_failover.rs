//! #521 scenario 3: a PostgreSQL primary failover while a found block is
//! mid-landing, on the live fixture (real qbitd, two servers, CPU miners).
//!
//! The database is a managed primary with one dedicated asynchronous standby,
//! the approved D3 policy: `synchronous_standby_names=''` on the primary and
//! `synchronous_commit=on` in writer sessions, so a positive share ACK waits
//! for local WAL durability only. Both servers reach the primary through a
//! stable writer endpoint and the standby streams through a replication link;
//! each is a TCP relay the test can fence. The failover is the operator
//! procedure in `docs/prism-ha-reference-architecture.md` ("Promotion, fencing
//! and the stable writer endpoint"): fence every writer connection, capture the
//! acknowledged-share evidence and WAL positions, stop the old primary, promote
//! the standby with `pg_promote(wait => true)`, confirm it left recovery, and
//! only then move the writer endpoint to it.
//!
//! A gate in front of qbitd holds every `submitblock` call from the first one
//! on. The first call's reservation (`offer_reserved`) is durable before the
//! call is made, so while the gate holds it the found block is mid-landing:
//! offered or about to be, with its outcome not yet recorded. Held calls reach
//! the node one at a time in arrival order once released.
//!
//! Two drills share the procedure:
//!
//! - [`Drill::AsyncLoss`] cuts replication while the miners keep receiving
//!   ACKs, so the promoted standby lacks a real unreplicated interval, and
//!   releases the held call only after the writers are fenced, so the offering
//!   frontend learns the node's answer but cannot record it. The documented
//!   consequence is asserted exactly: every share committed before the cut
//!   survives, the shares committed in the gap are lost and are counted and
//!   printed, and the reservation, which was replicated, is recovered as an
//!   unknown outcome and landed on the new primary without a second call.
//! - [`Drill::Fenced`] is the planned switch: after the fence, the standby
//!   replays through the old primary's flush LSN before promotion, so no
//!   acknowledged share may be lost, and the held call returns only after the
//!   endpoint moved, so the same attempt records the offer on the new primary.
use super::*;
use axum::{
    body::Bytes,
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use qbit_prism_server::codec::{double_sha256, hash_display};
use std::{
    collections::BTreeSet,
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU16, Ordering},
        Arc,
    },
};
use tokio::{sync::watch, task::JoinHandle};

/// The dedicated standby's `application_name` and physical slot.
const STANDBY: &str = "prism_standby_1";

/// The two drills start their clusters before `Fixture::open_on_database`
/// takes the fixture's `SERIAL` guard; this keeps a second pair from idling
/// beside the first.
static DRILLS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// When each step of the procedure happened, for the report and a failure.
struct Timeline {
    started: Instant,
    events: Vec<(Duration, &'static str)>,
}

impl Timeline {
    fn mark(&mut self, event: &'static str) {
        self.events.push((self.started.elapsed(), event));
    }

    fn render(&self) -> String {
        self.events
            .iter()
            .map(|(at, event)| format!("{:.2}s {event}", at.as_secs_f64()))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Which failover the scenario runs; see the module documentation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Drill {
    AsyncLoss,
    Fenced,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_async_failover_mid_landing_loses_only_the_replication_gap() -> Result<()> {
    run(Drill::AsyncLoss).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_fenced_failover_mid_landing_keeps_every_acknowledged_share() -> Result<()> {
    run(Drill::Fenced).await
}

async fn run(drill: Drill) -> Result<()> {
    let Some(bin) = gate::pg_bin_dir(gate::site!())? else {
        return Ok(());
    };
    let _drill = DRILLS.lock().await;
    // Wait for the fixture guard once before starting the pair: its loopback
    // connections, whose source ports the kernel picks, would otherwise race
    // the binary's start-up burst, where
    // `fixture_ports_stay_reserved_until_their_child_starts` rebinds ports it
    // has just released.
    drop(SERIAL.lock().await);
    let mut pair = Pair::start(bin.into()).await?;
    let Some(mut fixture) =
        Fixture::open_on_database(false, false, Some(&pair.url(pair.writer.port))).await?
    else {
        return Ok(());
    };
    let mut timeline = Timeline {
        started: Instant::now(),
        events: Vec::new(),
    };
    let result = async {
        let mut node = NodeGate::open(fixture.rpc_port).await?;
        let drilled = failover(&mut fixture, &mut pair, &mut node, drill, &mut timeline).await;
        node.stop();
        drilled
    }
    .await;
    if result.is_err() {
        eprintln!("{}", fixture.diagnostics());
        eprintln!("{}", pair.diagnostics());
        eprintln!("failover timeline: {}", timeline.render());
        // Let cleanup reach whichever cluster still accepts writes.
        pair.writer.route_to(pair.writable_port());
    }
    let cleanup = fixture.cleanup().await;
    drop(pair);
    result.and(cleanup)
}

async fn failover(
    f: &mut Fixture,
    pair: &mut Pair,
    node: &mut NodeGate,
    drill: Drill,
    timeline: &mut Timeline,
) -> Result<()> {
    let primary = PgPool::connect(&pair.url(pair.primary.port)).await?;
    let standby = PgPool::connect(&pair.url(pair.standby.port)).await?;
    let schema = f.schema.clone();
    let names: String = sqlx::query_scalar("SHOW synchronous_standby_names")
        .fetch_one(&primary)
        .await?;
    ensure!(
        names.is_empty(),
        "D3 requires an asynchronous standby, found {names:?}"
    );

    // The found block's `submitblock` must outlive the failover, so the call
    // deadline is raised from its one-second default. Every other setting is
    // the fixture's.
    for index in 0..2 {
        let process = f.start_server_with(
            index,
            None,
            &[
                ("QBIT_RPC_PORT", node.port.to_string()),
                ("PRISM_BLOCK_SUBMIT_RPC_TIMEOUT_SECONDS", "120".into()),
            ],
        )?;
        f.servers.push(process);
    }
    for index in 0..2 {
        until(&format!("server {index} readiness"), 30, || {
            healthy(f, index)
        })
        .await?;
    }
    node.hold()?;
    f.start_miner(0)?;
    f.start_miner(1)?;

    // The found block, reserved and held at the node.
    let block = node.first_arrival(Duration::from_secs(60)).await?;
    timeline.mark("held");
    let row_state =
        format!("SELECT state FROM {schema}.qbit_block_candidate_outbox WHERE block_hash=$1");
    let state_on = |pool: &PgPool| {
        let (pool, query, block) = (pool.clone(), row_state.clone(), block.clone());
        async move {
            Ok::<_, anyhow::Error>(
                sqlx::query_scalar::<_, String>(&query)
                    .bind(&block)
                    .fetch_optional(&pool)
                    .await?,
            )
        }
    };
    ensure!(
        state_on(&primary).await?.as_deref() == Some("offer_reserved"),
        "the held submitblock for {block} was not preceded by its durable reservation"
    );
    // The reservation must be on the standby before anything is cut: this
    // scenario is about a replicated reservation whose landing is in flight.
    until("reservation replicated to the standby", 15, || async {
        Ok(state_on(&standby).await?.as_deref() == Some("offer_reserved"))
    })
    .await?;

    let mut pre_cut = BTreeSet::new();
    let mut frozen = BTreeSet::new();
    let mut acknowledged_at_cut = None;
    if drill == Drill::AsyncLoss {
        until("pre-cut shares on the primary", 30, || async {
            Ok(shares(&primary, &schema).await?.len() >= 3)
        })
        .await?;
        pre_cut = shares(&primary, &schema).await?;
        until("standby replay of the pre-cut shares", 15, || async {
            Ok(shares(&standby, &schema).await?.is_superset(&pre_cut))
        })
        .await?;
        pair.replication.fence();
        timeline.mark("replication cut");
        let at_cut = acknowledged(f)?;
        acknowledged_at_cut = Some(at_cut);
        until("standby replay settled after the cut", 15, || async {
            Ok(sqlx::query_scalar::<_, bool>(
                "SELECT pg_last_wal_replay_lsn()=pg_last_wal_receive_lsn()",
            )
            .fetch_one(&standby)
            .await?)
        })
        .await?;
        frozen = shares(&standby, &schema).await?;
        ensure!(
            frozen.is_superset(&pre_cut),
            "the standby lost pre-cut shares"
        );
        // The unreplicated interval: shares committed and acknowledged on the
        // primary that the standby can no longer receive.
        until(
            "acknowledged shares inside the replication gap",
            30,
            || async {
                let committed = shares(&primary, &schema).await?;
                Ok(!committed.is_subset(&frozen) && acknowledged(f)? > at_cut)
            },
        )
        .await?;
    }

    if drill == Drill::Fenced {
        // The held attempt must keep its claim through the switch: fence only
        // on a fresh lease, well before the heartbeat's next renewal (every
        // 30 s), so no renewal can fall inside the outage.
        let renewed_since = format!(
            "SELECT 120-extract(epoch FROM claim_expires_at-clock_timestamp())::float8 \
             FROM {schema}.qbit_block_candidate_outbox WHERE block_hash=$1"
        );
        until("a fresh lease on the held block's claim", 40, || async {
            let since: f64 = sqlx::query_scalar(&renewed_since)
                .bind(&block)
                .fetch_one(&primary)
                .await?;
            Ok(since < 15.0)
        })
        .await?;
    }
    // Fence the old primary first: no writer can reach it from here on.
    pair.writer.fence();
    timeline.mark("writers fenced");
    for miner in &mut f.miners {
        miner.stop();
    }
    // ACKs the miners received after the cut: none can follow the fence.
    let acknowledged_in_gap = match acknowledged_at_cut {
        Some(at_cut) => acknowledged(f)? - at_cut,
        None => 0,
    };
    if drill == Drill::AsyncLoss {
        // The node accepts the block, but the offering frontend cannot record
        // the answer: the outcome is lost with the primary.
        node.release();
        node.forwarded(&block, Duration::from_secs(15)).await?;
        timeline.mark("node answered");
    }

    // The acknowledged-share evidence and WAL positions, read out of band
    // from the fenced primary, so they are final.
    let old_shares = shares(&primary, &schema).await?;
    let old_candidates = candidates(&primary, &schema).await?;
    let flush: String = sqlx::query_scalar("SELECT pg_current_wal_flush_lsn()::text")
        .fetch_one(&primary)
        .await?;
    if drill == Drill::Fenced {
        // The planned switch: replay through the captured flush LSN.
        until(
            "standby replay through the fenced primary's flush LSN",
            15,
            || async {
                Ok(
                    sqlx::query_scalar::<_, bool>("SELECT pg_last_wal_replay_lsn()>=$1::pg_lsn")
                        .bind(&flush)
                        .fetch_one(&standby)
                        .await?,
                )
            },
        )
        .await?;
    }
    let gap_bytes: i64 =
        sqlx::query_scalar("SELECT pg_wal_lsn_diff($1::pg_lsn,pg_last_wal_receive_lsn())::bigint")
            .bind(&flush)
            .fetch_one(&standby)
            .await?;
    primary.close().await;
    pair.primary.stop()?;
    timeline.mark("old primary stopped");

    // Promote the one eligible standby and confirm it left recovery.
    let promoted_at = Instant::now();
    let promoted: bool = sqlx::query_scalar("SELECT pg_promote(true,60)")
        .fetch_one(&standby)
        .await?;
    ensure!(promoted, "pg_promote did not complete within 60 seconds");
    let recovering: bool = sqlx::query_scalar("SELECT pg_is_in_recovery()")
        .fetch_one(&standby)
        .await?;
    ensure!(!recovering, "the promoted standby is still in recovery");
    pair.promoted = true;
    timeline.mark("promoted");
    let new_shares = shares(&standby, &schema).await?;
    let new_candidates = candidates(&standby, &schema).await?;
    ensure!(
        state_on(&standby).await?.as_deref() == Some("offer_reserved"),
        "the promoted primary must hold the found block's reservation"
    );
    ensure!(
        new_shares.is_subset(&old_shares),
        "the promoted primary holds shares the old primary never committed"
    );
    let lost: BTreeSet<_> = old_shares.difference(&new_shares).cloned().collect();
    let lost_candidates: BTreeSet<_> = old_candidates
        .difference(&new_candidates)
        .cloned()
        .collect();
    match drill {
        Drill::AsyncLoss => {
            ensure!(
                new_shares.is_superset(&pre_cut),
                "an acknowledged share committed before the replication cut was lost"
            );
            ensure!(
                new_shares == frozen,
                "promotion changed what the standby had received"
            );
            ensure!(
                !lost.is_empty() && gap_bytes > 0,
                "the drill did not produce an unreplicated interval"
            );
            ensure!(lost.is_disjoint(&pre_cut), "a lost share predates the cut");
        }
        Drill::Fenced => {
            ensure!(
                lost.is_empty() && lost_candidates.is_empty() && gap_bytes <= 0,
                "a fenced switch with replay through the flush LSN lost {} shares and {} candidates ({gap_bytes} WAL bytes)",
                lost.len(),
                lost_candidates.len()
            );
        }
    }

    if drill == Drill::AsyncLoss {
        // Move the endpoint only once the offering attempt has failed, so it
        // cannot record the answer on the new primary. Its claim release
        // usually fails on the same dead connections, and the recovery then
        // waits for the candidate lease (120 s) to expire; a release that
        // reaches the new primary lets it start sooner. Either way the row is
        // recovered from offer_reserved.
        until(
            "the offering attempt to fail on the fenced endpoint",
            60,
            || async { server_logged(f, &["candidate remains recoverable", &block]) },
        )
        .await?;
        timeline.mark("offering attempt failed");
    }
    // Move the stable writer endpoint to the sole primary.
    pair.writer.route_to(pair.standby.port);
    let moved_after = promoted_at.elapsed();
    timeline.mark("endpoint moved");
    if drill == Drill::Fenced {
        node.release();
        node.forwarded(&block, Duration::from_secs(15)).await?;
        timeline.mark("node answered");
    }

    // The landing completes on the new primary, from its offer_reserved row.
    until(
        "the found block's landing on the new primary",
        180,
        || async { Ok(state_on(&f.pool).await?.as_deref() == Some("submitted")) },
    )
    .await?;
    let landed_after = promoted_at.elapsed();
    timeline.mark("landed");
    let (outcome, offered_at, reserved_by): (Option<String>, Option<i64>, Option<String>) =
        sqlx::query_as(
            "SELECT offer_outcome,offered_at_ms,offer_reserved_by FROM qbit_block_candidate_outbox WHERE block_hash=$1",
        )
        .bind(&block)
        .fetch_one(&f.pool)
        .await?;
    ensure!(
        reserved_by.is_some(),
        "the landed row lost its reservation record"
    );
    match drill {
        // The answer was lost with the old primary: the recovered reservation
        // lands without an outcome or a call time ever being recorded.
        Drill::AsyncLoss => ensure!(
            outcome.is_none() && offered_at.is_none(),
            "the lost answer was recorded as {outcome:?} at {offered_at:?}"
        ),
        // The same attempt recorded the node's answer on the new primary.
        Drill::Fenced => ensure!(
            outcome.as_deref() == Some("accepted") && offered_at.is_some(),
            "the offering attempt recorded {outcome:?} at {offered_at:?}, expected its accepted answer"
        ),
    }
    let audits: i64 =
        sqlx::query_scalar("SELECT count(*) FROM qbit_pool_audit_bundles WHERE block_hash=$1")
            .bind(&block)
            .fetch_one(&f.pool)
            .await?;
    ensure!(audits == 1, "the landed block has {audits} audit bundles");
    until(
        "the landed block confirmed on the new primary",
        30,
        || async {
            Ok(sqlx::query_scalar::<_, String>(
                "SELECT chain_state FROM qbit_pool_blocks WHERE block_hash=$1",
            )
            .bind(&block)
            .fetch_optional(&f.pool)
            .await?
            .as_deref()
                == Some("confirmed"))
        },
    )
    .await?;
    let header = f.rpc("getblockheader", json!([block])).await?;
    ensure!(
        header["confirmations"].as_i64().unwrap_or(0) >= 1,
        "the landed block is not on the node's active chain: {header}"
    );

    // Job delivery resumes on both frontends against the new primary.
    for index in 0..2 {
        until(
            &format!("server {index} readiness after the move"),
            60,
            || healthy(f, index),
        )
        .await?;
    }
    let before = [f.count(0).await?, f.count(1).await?];
    f.start_miner(0)?;
    f.start_miner(1)?;
    until(
        "accepted shares on both servers after the move",
        60,
        || async { Ok(f.count(0).await? > before[0] && f.count(1).await? > before[1]) },
    )
    .await?;

    f.quiesce().await?;
    let reserved: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM qbit_block_candidate_outbox WHERE state='offer_reserved'",
    )
    .fetch_one(&f.pool)
    .await?;
    ensure!(reserved == 0, "{reserved} rows are stuck in offer_reserved");
    let (rows, ids, seqs): (i64, i64, i64) = sqlx::query_as(
        "SELECT count(*),count(DISTINCT share_id),count(DISTINCT share_seq) FROM qbit_share_ledger",
    )
    .fetch_one(&f.pool)
    .await?;
    ensure!(
        rows == ids && rows == seqs,
        "share identities or sequence numbers repeat after the failover: {rows} rows, {ids} ids, {seqs} seqs"
    );
    f.integrity().await?;
    let arrivals = node.arrivals();
    let offered: BTreeSet<_> = arrivals.iter().collect();
    ensure!(
        offered.len() == arrivals.len(),
        "a block was offered to the node more than once: {arrivals:?}"
    );
    ensure!(
        node.forward_order().first() == Some(&block),
        "the found block was not the first held call to reach the node"
    );
    // Blocks whose candidate rows the unreplicated interval took with it,
    // although the node received them.
    let lost_offered: Vec<_> = arrivals
        .iter()
        .filter(|hash| lost_candidates.contains(*hash))
        .collect();

    eprintln!(
        "live failover ({drill:?}): block {block} held at submitblock, landed on the promoted \
         primary {:.1} s after promotion with outcome {outcome:?}; {} submitblock calls, each once; replication gap \
         {gap_bytes} WAL bytes, {} committed shares lost ({} ACKs received by miners after the \
         cut), {} candidate rows lost ({} of them offered to the node); {} shares survived; \
         endpoint moved {:.1} s after promotion; lost shares: {lost:?}; lost candidates: \
         {lost_candidates:?}; timeline: {}",
        landed_after.as_secs_f64(),
        arrivals.len(),
        lost.len(),
        acknowledged_in_gap,
        lost_candidates.len(),
        lost_offered.len(),
        new_shares.len(),
        moved_after.as_secs_f64(),
        timeline.render(),
    );
    standby.close().await;
    Ok(())
}

/// Every share in the ledger of `schema`, by identity.
async fn shares(pool: &PgPool, schema: &str) -> Result<BTreeSet<String>> {
    Ok(
        sqlx::query_scalar(&format!("SELECT share_id FROM {schema}.qbit_share_ledger"))
            .fetch_all(pool)
            .await?
            .into_iter()
            .collect(),
    )
}

/// Every block candidate in the outbox of `schema`, by block hash.
async fn candidates(pool: &PgPool, schema: &str) -> Result<BTreeSet<String>> {
    Ok(sqlx::query_scalar(&format!(
        "SELECT block_hash FROM {schema}.qbit_block_candidate_outbox"
    ))
    .fetch_all(pool)
    .await?
    .into_iter()
    .collect())
}

async fn healthy(f: &Fixture, index: usize) -> Result<bool> {
    let response = f
        .client
        .get(format!("http://127.0.0.1:{}/healthz", f.api[index]))
        .send()
        .await?;
    if !response.status().is_success() {
        return Ok(false);
    }
    let body: Value = response.json().await?;
    Ok(body["ok"] == true)
}

/// The positive share ACKs the miners have printed so far.
fn acknowledged(f: &Fixture) -> Result<usize> {
    let mut count = 0;
    for entry in std::fs::read_dir(f.directory.path())? {
        let path = entry?.path();
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        if !(name.starts_with("miner-") && name.ends_with(".json")) {
            continue;
        }
        // A line still being written does not parse and is not counted yet.
        count += std::fs::read_to_string(&path)?
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|event| event["event"] == "share" && event["accepted"] == true)
            .count();
    }
    Ok(count)
}

/// Whether one line of either server's log contains every fragment.
fn server_logged(f: &Fixture, fragments: &[&str]) -> Result<bool> {
    for index in 0..f.servers.len() {
        let log = std::fs::read_to_string(f.directory.path().join(format!("server-{index}.log")))?;
        if log
            .lines()
            .any(|line| fragments.iter().all(|fragment| line.contains(fragment)))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// One disposable PostgreSQL 16 cluster.
struct Cluster {
    bin: PathBuf,
    data: PathBuf,
    port: u16,
    /// Holds `port` until the server binds it, so no concurrent test takes it.
    reservation: Option<std::net::TcpListener>,
    running: bool,
}

impl Cluster {
    fn new(bin: &Path, data: PathBuf) -> Result<Self> {
        let reservation = std::net::TcpListener::bind("127.0.0.1:0")?;
        Ok(Self {
            bin: bin.to_path_buf(),
            data,
            port: reservation.local_addr()?.port(),
            reservation: Some(reservation),
            running: false,
        })
    }

    fn command(bin: &Path, binary: &str, args: &[&str]) -> Result<()> {
        let output = Command::new(bin.join(binary)).args(args).output()?;
        ensure!(
            output.status.success(),
            "{binary} failed: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }

    fn control(&self, args: &[&str]) -> Result<()> {
        let mut all = vec![
            "-D",
            self.data.to_str().context("database path")?,
            "-t",
            "30",
        ];
        all.extend_from_slice(args);
        Self::command(&self.bin, "pg_ctl", &all)
    }

    fn start(&mut self, socket: &Path, settings: &str) -> Result<()> {
        // Marked first, so a partially successful start is still stopped.
        self.running = true;
        self.reservation = None;
        self.control(&[
            "-l",
            self.data.with_extension("log").to_str().context("log path")?,
            "-o",
            &format!(
                "-h 127.0.0.1 -p {} -k {} -c fsync=on -c full_page_writes=on -c max_connections=200 {settings}",
                self.port,
                socket.display()
            ),
            "-w",
            "start",
        ])
    }

    /// An immediate stop: for the primary, the loss of the server.
    fn stop(&mut self) -> Result<()> {
        if self.running {
            self.control(&["-m", "immediate", "-w", "stop"])?;
            self.running = false;
        }
        Ok(())
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

/// The D3 primary and its dedicated asynchronous standby, the stable writer
/// endpoint in front of the primary, and the standby's replication link.
struct Pair {
    primary: Cluster,
    standby: Cluster,
    writer: Relay,
    replication: Relay,
    promoted: bool,
    user: String,
    /// Declared last, so both clusters stop before their files go.
    directory: tempfile::TempDir,
}

impl Pair {
    async fn start(bin: PathBuf) -> Result<Self> {
        // Keep the Unix socket path short and pg_ctl's option path space-free.
        let directory = tempfile::Builder::new()
            .prefix("b521-")
            .tempdir_in("/tmp")?;
        let user = String::from_utf8(Command::new("id").arg("-un").output()?.stdout)?
            .trim()
            .to_owned();
        let mut primary = Cluster::new(&bin, directory.path().join("primary"))?;
        Cluster::command(
            &bin,
            "initdb",
            &[
                "-D",
                primary.data.to_str().context("database path")?,
                "-A",
                "trust",
                "--no-locale",
                "-E",
                "UTF8",
            ],
        )?;
        // synchronous_standby_names stays at its empty default: asynchronous.
        primary.start(
            directory.path(),
            "-c synchronous_commit=on -c wal_level=replica -c max_wal_senders=10 -c max_replication_slots=10",
        )?;
        let replication = Relay::open(primary.port).await?;
        let writer = Relay::open(primary.port).await?;
        let mut pair = Self {
            standby: Cluster::new(&bin, directory.path().join("standby"))?,
            primary,
            writer,
            replication,
            promoted: false,
            user,
            directory,
        };
        let admin = PgPool::connect(&pair.url(pair.primary.port)).await?;
        sqlx::query("SELECT pg_create_physical_replication_slot($1)")
            .bind(STANDBY)
            .execute(&admin)
            .await?;
        // The base backup and the streaming connection both go through the
        // replication link, which `-R` records as the standby's conninfo.
        let conninfo = format!(
            "host=127.0.0.1 port={} user={} application_name={STANDBY}",
            pair.replication.port, pair.user
        );
        Cluster::command(
            &bin,
            "pg_basebackup",
            &[
                "-D",
                pair.standby.data.to_str().context("database path")?,
                "-d",
                &conninfo,
                "-X",
                "stream",
                "-R",
                "-S",
                STANDBY,
                "-c",
                "fast",
            ],
        )?;
        let auto = pair.standby.data.join("postgresql.auto.conf");
        let mut settings = std::fs::read_to_string(&auto)?;
        settings.push_str(&format!(
            "\nprimary_conninfo = '{conninfo}'\nprimary_slot_name = '{STANDBY}'\n"
        ));
        std::fs::write(&auto, settings)?;
        let socket = pair.directory.path().to_path_buf();
        pair.standby.start(&socket, "-c hot_standby=on")?;
        until("asynchronous standby streaming", 60, || async {
            Ok(sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_replication WHERE application_name=$1 AND state='streaming' AND sync_state='async')",
            )
            .bind(STANDBY)
            .fetch_one(&admin)
            .await?)
        })
        .await?;
        admin.close().await;
        Ok(pair)
    }

    fn url(&self, port: u16) -> String {
        format!("postgresql://{}@127.0.0.1:{port}/postgres", self.user)
    }

    fn writable_port(&self) -> u16 {
        if self.promoted {
            self.standby.port
        } else {
            self.primary.port
        }
    }

    fn diagnostics(&self) -> String {
        let mut report = String::new();
        for (role, cluster) in [("primary", &self.primary), ("standby", &self.standby)] {
            let log = std::fs::read_to_string(cluster.data.with_extension("log"))
                .unwrap_or_else(|error| format!("unreadable: {error}"));
            let tail: Vec<_> = log.lines().rev().take(20).collect();
            report.push_str(&format!(
                "--- {role} PostgreSQL log (last 20 lines, newest first)\n"
            ));
            for line in tail {
                report.push_str(line);
                report.push('\n');
            }
        }
        report
    }
}

/// A TCP relay the test can fence: every new connection is refused and every
/// existing one closed, the network isolation the procedure's fence requires.
/// Routing it elsewhere admits connections again, to the new target.
struct Relay {
    port: u16,
    target: Arc<AtomicU16>,
    fenced: Arc<AtomicBool>,
    severed: watch::Sender<u64>,
    task: JoinHandle<()>,
}

impl Relay {
    async fn open(target: u16) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let target = Arc::new(AtomicU16::new(target));
        let fenced = Arc::new(AtomicBool::new(false));
        let (severed, _) = watch::channel(0u64);
        let task = tokio::spawn({
            let (target, fenced, severed) = (target.clone(), fenced.clone(), severed.clone());
            async move {
                while let Ok((mut client, _)) = listener.accept().await {
                    // Subscribe before reading the fence: a fence set after
                    // this point also bumps the generation and closes it.
                    let mut generation = severed.subscribe();
                    if fenced.load(Ordering::SeqCst) {
                        continue;
                    }
                    let target = target.load(Ordering::SeqCst);
                    tokio::spawn(async move {
                        tokio::select! {
                            _ = generation.changed() => {}
                            _ = async {
                                if let Ok(mut upstream) =
                                    tokio::net::TcpStream::connect(("127.0.0.1", target)).await
                                {
                                    let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
                                }
                            } => {}
                        }
                    });
                }
            }
        });
        Ok(Self {
            port,
            target,
            fenced,
            severed,
            task,
        })
    }

    fn fence(&self) {
        self.fenced.store(true, Ordering::SeqCst);
        self.severed.send_modify(|generation| *generation += 1);
    }

    fn route_to(&self, target: u16) {
        self.target.store(target, Ordering::SeqCst);
        self.fenced.store(false, Ordering::SeqCst);
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.fence();
        self.task.abort();
    }
}

/// A JSON-RPC relay in front of qbitd that counts every `submitblock` and,
/// while held, queues them; released, they reach the node one at a time in
/// arrival order.
struct NodeGate {
    port: u16,
    state: Arc<GateState>,
    hold: Option<tokio::sync::OwnedMutexGuard<()>>,
    task: JoinHandle<()>,
}

struct GateState {
    node: String,
    client: reqwest::Client,
    queue: Arc<tokio::sync::Mutex<()>>,
    /// The hash of every `submitblock` in arrival order.
    arrivals: watch::Sender<Vec<String>>,
    /// The hash of every `submitblock` the node answered, in answer order.
    forwarded: watch::Sender<Vec<String>>,
}

impl NodeGate {
    async fn open(node_port: u16) -> Result<Self> {
        let state = Arc::new(GateState {
            node: format!("http://127.0.0.1:{node_port}/"),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()?,
            queue: Arc::new(tokio::sync::Mutex::new(())),
            arrivals: watch::channel(Vec::new()).0,
            forwarded: watch::channel(Vec::new()).0,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let app = axum::Router::new()
            .fallback(relay_rpc)
            .with_state(state.clone());
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self {
            port,
            state,
            hold: None,
            task,
        })
    }

    fn hold(&mut self) -> Result<()> {
        let queue = self.state.queue.clone().try_lock_owned();
        self.hold = Some(queue.context("a submitblock was in flight before the hold")?);
        Ok(())
    }

    fn release(&mut self) {
        self.hold = None;
    }

    async fn first_arrival(&self, limit: Duration) -> Result<String> {
        let mut arrivals = self.state.arrivals.subscribe();
        let first = tokio::time::timeout(limit, arrivals.wait_for(|hashes| !hashes.is_empty()))
            .await
            .context("no block was found and offered")??;
        Ok(first[0].clone())
    }

    async fn forwarded(&self, block: &str, limit: Duration) -> Result<()> {
        let mut forwarded = self.state.forwarded.subscribe();
        tokio::time::timeout(
            limit,
            forwarded.wait_for(|hashes| hashes.iter().any(|hash| hash == block)),
        )
        .await
        .with_context(|| format!("the node never answered the held submitblock for {block}"))??;
        Ok(())
    }

    fn arrivals(&self) -> Vec<String> {
        self.state.arrivals.borrow().clone()
    }

    fn forward_order(&self) -> Vec<String> {
        self.state.forwarded.borrow().clone()
    }

    fn stop(&mut self) {
        self.release();
        self.task.abort();
    }
}

async fn relay_rpc(
    State(gate): State<Arc<GateState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let request: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let submitted = (request["method"] == "submitblock").then(|| {
        request["params"][0]
            .as_str()
            .and_then(|block| hex::decode(block).ok())
            .filter(|block| block.len() >= 80)
            .map_or_else(
                || "<undecodable block>".to_owned(),
                |block| hash_display(&double_sha256(&block[..80])),
            )
    });
    let _turn = match &submitted {
        Some(hash) => {
            gate.arrivals
                .send_modify(|hashes| hashes.push(hash.clone()));
            Some(gate.queue.lock().await)
        }
        None => None,
    };
    let mut upstream = gate
        .client
        .post(&gate.node)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body);
    if let Some(authorization) = headers.get(header::AUTHORIZATION) {
        upstream = upstream.header(header::AUTHORIZATION, authorization.clone());
    }
    let answer = async {
        let response = upstream.send().await?;
        let status = response.status();
        Ok::<_, reqwest::Error>((status, response.bytes().await?))
    }
    .await;
    match answer {
        Ok((status, bytes)) => {
            if let Some(hash) = submitted {
                gate.forwarded.send_modify(|hashes| hashes.push(hash));
            }
            (
                StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY),
                [(header::CONTENT_TYPE, "application/json")],
                bytes,
            )
                .into_response()
        }
        Err(_) => StatusCode::BAD_GATEWAY.into_response(),
    }
}
