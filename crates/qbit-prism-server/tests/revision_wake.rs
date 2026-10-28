//! #752: another frontend's landing wakes this one's refresh.
//!
//! Each pool block moves the payout revision twice on the new tip: when a
//! frontend first records the tip, and when the landing confirms the block.
//! Work a frontend publishes on the new tip in between carries the first
//! revision, and once the confirmation commits every share on it is refused
//! `stale-job` until that frontend republishes. The landing frontend wakes
//! its own refresh; before #752 every other one waited for its blockpoll
//! tick. The confirmation now announces itself with NOTIFY, and each
//! frontend's refresh loop listens on a connection of its own
//! (`coordinator::revision_wake`).
//!
//! Two frontends share one database and one fake node. The first case lands
//! a real block on frontend A while frontend B, whose tick is [`LONG_TICK`],
//! publishes new-tip work at the first revision; B must republish at the
//! confirmed revision within [`WAKE_BOUND`], and a share on that work is
//! accepted while one on its superseded work is still refused. On the base
//! B keeps the superseded work for its whole tick and the case fails at the
//! bound. The second case cuts the listeners off: a frontend keeps its tick,
//! and a listener that reconnects wakes its refresh for the announcement it
//! missed and is woken by the next one; a late announcement of an older
//! revision republishes nothing older. The third has a dual-writer node's
//! carry gate change, fenced by `ledger::bump_payout_revision`, wake B.
//!
//! Run through: test/prism-native-tests.sh cargo-args --locked -p
//! qbit-prism-server --test revision_wake -- --nocapture
use anyhow::{bail, ensure, Context, Result};
use qbit_prism_server::{
    carry_owner::seed_role,
    codec,
    config::Config,
    coordinator::{revision_wake, Coordinator, JobContext},
    ledger::Ledger,
    metrics::Metrics,
    node_identity::{NodeIdentity, NodeIndex},
    stratum::{MiningBackend, MiningJob, Worker},
};
use qbit_prism_test_gate as gate;
use sqlx::{Connection, PgConnection};
use std::{sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinHandle, time::Instant};

#[path = "support/fake_qbitd.rs"]
#[allow(dead_code)]
mod fake_qbitd;
#[path = "support/ledger_database.rs"]
#[allow(dead_code)]
mod ledger_database;
#[path = "support/window_fixture.rs"]
#[allow(dead_code)]
mod window_fixture;

const DIFFICULTY: f64 = 1e-12;
/// The tick of a frontend that must be woken. Its loop's first refresh runs
/// at once and its next one only after every case has ended (see
/// [`CASE_LIMIT`]), so no republish a case waits for can be its tick.
const LONG_TICK: Duration = Duration::from_secs(300);
/// How long a case may run.
const CASE_LIMIT: Duration = Duration::from_secs(150);
/// How soon a woken frontend republishes after the announcing commit: the
/// notification's delivery and one refresh's rebuild (about 0.1 s), with room
/// for a loaded CI host. Far below [`LONG_TICK`], so a pass is a wake and a
/// fail is a missing one.
const WAKE_BOUND: Duration = Duration::from_secs(10);
/// The production default tick, for the frontend whose listener is cut off.
const SHORT_TICK: Duration = Duration::from_secs(2);
/// How long the frontend without a listener may take to republish on its
/// tick. Generous: that it was the tick is shown by the order of events (no
/// listener session existed meanwhile), not by the time it took.
const TICK_FALLBACK_LIMIT: Duration = Duration::from_secs(30);
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct Frontends {
    node: fake_qbitd::FakeNode,
    a: Arc<Coordinator>,
    b: Arc<Coordinator>,
}

/// Two frontends on `db` and one fake node, with their ticks.
async fn frontends(
    db: &ledger_database::FixtureDatabase,
    ticks: [Duration; 2],
) -> Result<Frontends> {
    let node = fake_qbitd::FakeNode::open().await?;
    let config = |instance: &str, tick: Duration| -> Result<Config> {
        let mut config = fake_qbitd::coordinator_config(db.url.clone(), &node, instance)?;
        config.poll_interval = tick;
        // A's landing is held at its `submitblock` while B refreshes.
        config.block_submit_timeout = Duration::from_secs(30);
        Ok(config)
    };
    let a = Coordinator::new(config("wake-a", ticks[0])?, Arc::new(Metrics::default())).await?;
    let b = Coordinator::new(config("wake-b", ticks[1])?, Arc::new(Metrics::default())).await?;
    Ok(Frontends { node, a, b })
}

/// `coordinator`'s refresh loop, with its listener, until `stop` changes.
fn refresh_loop(coordinator: &Arc<Coordinator>, stop: &watch::Sender<bool>) -> JoinHandle<()> {
    let coordinator = coordinator.clone();
    let stopped = stop.subscribe();
    tokio::spawn(async move { coordinator.refresh_loop(stopped).await })
}

async fn stop_loops(stop: watch::Sender<bool>, loops: Vec<JoinHandle<()>>) -> Result<()> {
    stop.send_replace(true);
    for task in loops {
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .context("a refresh loop did not stop")??;
    }
    Ok(())
}

async fn close(frontends: Frontends) {
    for frontend in [&frontends.a, &frontends.b] {
        frontend.ledger.pool.close().await;
    }
}

/// The payout revision and parent of the work `coordinator` publishes.
async fn published(coordinator: &Coordinator) -> Result<(i64, String)> {
    let prepared = coordinator
        .prepared
        .read()
        .await
        .clone()
        .context("no published work")?;
    Ok((
        prepared.snapshot.payout_revision,
        prepared.template["previousblockhash"]
            .as_str()
            .context("published work has no parent")?
            .to_owned(),
    ))
}

/// Until `coordinator` publishes work at `revision`, within `bound`; how
/// long it took.
async fn republished(
    coordinator: &Coordinator,
    revision: i64,
    since: Instant,
    bound: Duration,
) -> Result<Duration> {
    loop {
        let (now, parent) = published(coordinator).await?;
        ensure!(
            now <= revision,
            "the frontend published revision {now}, past the expected {revision}"
        );
        if now == revision {
            return Ok(since.elapsed());
        }
        if since.elapsed() >= bound {
            bail!(
                "the frontend still publishes payout revision {now} on {parent} {:.1?} after revision {revision} committed",
                since.elapsed()
            );
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn durable_revision(connection: &mut PgConnection) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT payout_revision FROM qbit_prism_cluster WHERE singleton")
            .fetch_one(connection)
            .await?,
    )
}

fn worker(name: &str) -> Worker {
    Worker {
        username: format!("{name}.rig"),
        payout_address: name.into(),
        worker_name: Some("rig".into()),
        p2mr_program_hex: "11".repeat(32),
    }
}

async fn issue(coordinator: &Coordinator, worker: &Worker) -> Result<MiningJob<JobContext>> {
    let job = coordinator
        .build_job(worker, "1a2b3c4d", DIFFICULTY, 0.0)
        .await?;
    coordinator
        .persist_issued_job(worker, &job, 0, Duration::from_secs(30))
        .await?;
    Ok(job)
}

/// A solution on `job` meeting the share target (`block` false, and then
/// not the network's) or the network's (`block` true).
fn proof(job: &MiningJob<JobContext>, block: bool) -> Result<codec::Submission> {
    let nonces = if block {
        0..10_000u32
    } else {
        10_000..20_000u32
    };
    for nonce in nonces {
        let mut proof = job.wire.assemble_submission(
            &"00".repeat(job.wire.extranonce2_size),
            &format!("{:08x}", job.wire.ntime),
            &format!("{nonce:08x}"),
            None,
            0,
        )?;
        if block && proof.block_pass {
            return Ok(proof);
        }
        if !block && proof.share_pass && !proof.block_pass {
            proof.block_pass = false;
            return Ok(proof);
        }
    }
    bail!("no solution found in the bounded search")
}

/// Claim A's candidate and run it through the offer lifecycle, as A's
/// submit loop would.
async fn drive_candidate(a: &Coordinator, hash: &str) -> Result<()> {
    for _ in 0..600 {
        if let Some(claim) = a.ledger.claim_candidate(60).await? {
            ensure!(
                claim.candidate.block_hash == hash,
                "a different candidate was claimed"
            );
            return a.process_candidate(&claim).await;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    bail!("the block-bearing share produced no candidate to offer")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn another_frontends_landing_wakes_this_one_to_republish_at_the_confirmed_revision(
) -> Result<()> {
    let _serial = SERIAL.lock().await;
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = ledger_database::FixtureDatabase::open(&raw, "revision_wake_").await?;
    let result = async {
        let f = frontends(&db, [LONG_TICK, LONG_TICK]).await?;
        let result = tokio::time::timeout(CASE_LIMIT, landing(&f))
            .await
            .with_context(|| format!("the case exceeded {CASE_LIMIT:?}"))
            .and_then(|result| result);
        close(f).await;
        result
    }
    .await;
    db.close(result).await
}

async fn landing(f: &Frontends) -> Result<()> {
    window_fixture::WindowPlan::new(16)?
        .load(&f.a.ledger.pool, "revision-wake-fixture")
        .await?;
    f.a.refresh_once().await?;
    f.b.refresh_once().await?;
    let mut bumper = PgConnection::connect(&f.a.config.database_url).await?;
    let first = durable_revision(&mut bumper).await?;
    let (stop, _) = watch::channel(false);
    // Only B runs its loop: A is driven by hand, as its submit loop and
    // refresh would drive it.
    let loops = vec![refresh_loop(&f.b, &stop)];
    let result = async {
        // A miner on A finds a block. The node takes it, and A's landing is
        // held at the reply while B, on its own schedule, records the new
        // tip and publishes work on it: the tip's bump, the first revision.
        let solver = worker("solver");
        let job = issue(&f.a, &solver).await?;
        let block = proof(&job, true)?;
        let hash = block.block_hash_hex.clone();
        f.node.accept_blocks();
        let mut offer = f.node.pause_next("submitblock")?;
        let submit = f.a.submit(&solver, &job, block, false.into());
        let landed = async {
            let drive = drive_candidate(&f.a, &hash);
            let mut drive = std::pin::pin!(drive);
            tokio::select! {
                result = &mut drive => bail!("the landing ended before the node was asked: {result:?}"),
                entered = offer.entered() => entered?,
            }
            f.b.refresh_once().await?;
            let (tip_revision, parent) = published(&f.b).await?;
            ensure!(
                parent == hash && tip_revision == first + 1,
                "B published revision {tip_revision} on {parent}, not the tip's revision {} on the new tip {hash}",
                first + 1
            );
            let miner = worker("miner");
            let superseded = issue(&f.b, &miner).await?;
            ensure!(superseded.wire.payout_revision == tip_revision);
            offer.release();
            drive.await?;
            Ok::<_, anyhow::Error>((miner, superseded))
        };
        let (submitted, landed) = tokio::join!(submit, landed);
        submitted.map_err(|error| anyhow::anyhow!("the block-bearing share was refused: {error:?}"))?;
        let (miner, superseded) = landed?;
        // The landing confirmed the block: the second, same-tip bump.
        let confirmed = Instant::now();
        let revision = durable_revision(&mut bumper).await?;
        ensure!(
            revision == first + 2,
            "the landing left payout revision {revision}, not {}",
            first + 2
        );
        let took = republished(&f.b, revision, confirmed, WAKE_BOUND)
            .await
            .context("B was not woken by A's confirmation (#752); its tick is five minutes")?;
        eprintln!("B republished at the confirmed revision {took:?} after A's landing returned");

        // The fence is unchanged: a share on the work the confirmation
        // superseded is refused for its revision.
        let refused = f
            .b
            .submit(&miner, &superseded, proof(&superseded, false)?, false.into())
            .await
            .expect_err("a share on superseded work was accepted");
        ensure!(
            refused.reason_id.as_deref() == Some("stale-job"),
            "a share on superseded work was answered {refused:?}, not stale-job"
        );
        // A share on the work B just published is accepted, once.
        let current = issue(&f.b, &miner).await?;
        ensure!(current.wire.payout_revision == revision && current.wire.previousblockhash == hash);
        let share = proof(&current, false)?;
        let share_id = format!("{}:{}", miner.username, share.block_hash_hex);
        f.b.submit(&miner, &current, share, false.into())
            .await
            .map_err(|error| {
                anyhow::anyhow!("a share on just-published new-tip work was refused: {error:?}")
            })?;
        let credited: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM qbit_share_ledger WHERE accepted AND share_id=$1",
        )
        .bind(&share_id)
        .fetch_one(&mut bumper)
        .await?;
        ensure!(credited == 1, "the share is credited {credited} time(s)");
        Ok(())
    }
    .await;
    let stopped = stop_loops(stop, loops).await;
    result.and(stopped)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_listener_keeps_the_tick_and_wakes_again_once_it_reconnects() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = ledger_database::FixtureDatabase::open(&raw, "revision_wake_").await?;
    let result = async {
        // A ticks at the production default; B only on wakes.
        let f = frontends(&db, [SHORT_TICK, LONG_TICK]).await?;
        let result = tokio::time::timeout(CASE_LIMIT, lost_listener(&db, &f))
            .await
            .with_context(|| format!("the case exceeded {CASE_LIMIT:?}"))
            .and_then(|result| result);
        // Whatever happened, the database takes connections again before
        // anything closes it.
        let reopened = sqlx::query(&format!(
            "ALTER DATABASE \"{}\" WITH ALLOW_CONNECTIONS true",
            db.name()
        ))
        .execute(&db.admin)
        .await;
        close(f).await;
        result.and(reopened.map(|_| ()).map_err(Into::into))
    }
    .await;
    db.close(result).await
}

/// How many listener sessions are connected to the fixture database.
async fn listeners(db: &ledger_database::FixtureDatabase) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity WHERE datname=$1 AND application_name=$2 AND query LIKE 'LISTEN %'",
    )
    .bind(db.name())
    .bind(revision_wake::APPLICATION_NAME)
    .fetch_one(&db.admin)
    .await?)
}

async fn until_listeners(
    db: &ledger_database::FixtureDatabase,
    count: i64,
    bound: Duration,
) -> Result<()> {
    let started = Instant::now();
    loop {
        let now = listeners(db).await?;
        if now == count {
            return Ok(());
        }
        ensure!(
            started.elapsed() < bound,
            "{now} listener session(s) after {bound:?}, not {count}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A same-tip bump with its announcement, as `ledger/blocks.rs` runs it.
async fn bump(connection: &mut PgConnection) -> Result<i64> {
    Ok(sqlx::query_scalar(&format!(
        "UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1,updated_at=clock_timestamp() WHERE singleton RETURNING payout_revision,pg_notify('{}',payout_revision::text)",
        revision_wake::CHANNEL
    ))
    .fetch_one(connection)
    .await?)
}

async fn lost_listener(db: &ledger_database::FixtureDatabase, f: &Frontends) -> Result<()> {
    f.a.refresh_once().await?;
    f.b.refresh_once().await?;
    // Opened while the database still takes connections.
    let mut bumper = PgConnection::connect(&db.url).await?;
    let first = durable_revision(&mut bumper).await?;
    let (stop, _) = watch::channel(false);
    let loops = vec![refresh_loop(&f.a, &stop), refresh_loop(&f.b, &stop)];
    let result = async {
        until_listeners(db, 2, Duration::from_secs(10)).await?;
        // Both listeners are cut off, and cannot reconnect.
        sqlx::query(&format!(
            "ALTER DATABASE \"{}\" WITH ALLOW_CONNECTIONS false",
            db.name()
        ))
        .execute(&db.admin)
        .await?;
        let ended: i64 = sqlx::query_scalar(
            "SELECT count(*) FILTER (WHERE pg_terminate_backend(pid)) FROM pg_stat_activity WHERE datname=$1 AND application_name=$2",
        )
        .bind(db.name())
        .bind(revision_wake::APPLICATION_NAME)
        .fetch_one(&db.admin)
        .await?;
        ensure!(ended == 2, "{ended} listener session(s) ended, not 2");
        until_listeners(db, 0, Duration::from_secs(10)).await?;

        // An announcement nobody hears. A keeps its tick: the refresh is
        // never held up by its listener. No listener session exists until
        // connections are allowed again (checked below), so A's republish
        // can only be its tick.
        let missed = bump(&mut bumper).await?;
        ensure!(missed == first + 1);
        let bumped = Instant::now();
        let took = republished(&f.a, missed, bumped, TICK_FALLBACK_LIMIT)
            .await
            .context("A, its listener gone, did not republish on its tick")?;
        eprintln!("A republished on its tick {took:?} after the unheard announcement");
        let (b_revision, _) = published(&f.b).await?;
        ensure!(
            b_revision == first,
            "B republished at {b_revision} without a listener or a tick"
        );
        ensure!(
            listeners(db).await? == 0,
            "a listener reconnected while the database refused connections"
        );

        // Connections are allowed again: B's listener reconnects within its
        // longest retry and wakes its refresh for what it missed.
        sqlx::query(&format!(
            "ALTER DATABASE \"{}\" WITH ALLOW_CONNECTIONS true",
            db.name()
        ))
        .execute(&db.admin)
        .await?;
        let reopened = Instant::now();
        let took = republished(&f.b, missed, reopened, revision_wake::RETRY_LONGEST + WAKE_BOUND)
            .await
            .context("B's reconnected listener did not wake its refresh for the announcement it missed")?;
        eprintln!("B caught up {took:?} after the database took connections again");
        until_listeners(db, 2, revision_wake::RETRY_LONGEST + WAKE_BOUND).await?;

        // And the next announcement wakes it again.
        let heard = bump(&mut bumper).await?;
        let bumped = Instant::now();
        let took = republished(&f.b, heard, bumped, WAKE_BOUND)
            .await
            .context("B's reconnected listener did not wake its refresh")?;
        eprintln!("B, reconnected, republished {took:?} after the next announcement");

        // A late announcement of an older revision is only a wake: the
        // refresh it causes reads the durable revision and publishes
        // nothing older (EP-STATE).
        let woken = Instant::now();
        sqlx::query("SELECT pg_notify($1,$2)")
            .bind(revision_wake::CHANNEL)
            .bind(first.to_string())
            .execute(&mut bumper)
            .await?;
        loop {
            if f.b.work_refresh_age() < woken.elapsed() {
                break;
            }
            ensure!(
                woken.elapsed() < WAKE_BOUND,
                "the late announcement did not wake B's refresh"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let (now, _) = published(&f.b).await?;
        ensure!(
            now == heard,
            "a late announcement of revision {first} left B publishing {now}, not {heard}"
        );
        Ok(())
    }
    .await;
    let stopped = stop_loops(stop, loops).await;
    result.and(stopped)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_carry_gate_change_on_a_dual_writer_ledger_wakes_another_frontend() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    let db = ledger_database::FixtureDatabase::open(&raw, "revision_wake_").await?;
    let result = async {
        let f = frontends(&db, [LONG_TICK, LONG_TICK]).await?;
        let result = tokio::time::timeout(CASE_LIMIT, carry_gate(&db, &f))
            .await
            .with_context(|| format!("the case exceeded {CASE_LIMIT:?}"))
            .and_then(|result| result);
        close(f).await;
        result
    }
    .await;
    db.close(result).await
}

/// A carry owner guard opens its node's gate (`Ledger::set_carry_paying`),
/// which commits the fence `ledger::bump_payout_revision` writes, the bump a
/// carry-owner release or transfer writes too. B, on the same database and
/// with a tick of [`LONG_TICK`], must be woken by it.
async fn carry_gate(db: &ledger_database::FixtureDatabase, f: &Frontends) -> Result<()> {
    f.b.refresh_once().await?;
    let mut reader = PgConnection::connect(&db.url).await?;
    let first = durable_revision(&mut reader).await?;
    let (stop, _) = watch::channel(false);
    let loops = vec![refresh_loop(&f.b, &stop)];
    let result = async {
        until_listeners(db, 1, Duration::from_secs(10)).await?;
        let node = Ledger::connect(&db.url, "carry-node".into(), 2, false).await?;
        let gated = async {
            node.set_dual_writer_identity(NodeIdentity {
                node: NodeIndex::B,
                carry_owner: false,
            })?;
            seed_role(&node, NodeIndex::B.index(), true).await?;
            let opened = Instant::now();
            ensure!(
                node.set_carry_paying(true).await?,
                "opening the closed carry gate was not a change"
            );
            let revision = durable_revision(&mut reader).await?;
            ensure!(
                revision == first + 1,
                "the carry gate's fence left revision {revision}, not {}",
                first + 1
            );
            let took = republished(&f.b, revision, opened, WAKE_BOUND)
                .await
                .context("B was not woken by the carry gate's fence (#752)")?;
            eprintln!("B republished {took:?} after the carry gate's fence");
            Ok(())
        }
        .await;
        node.pool.close().await;
        gated
    }
    .await;
    let stopped = stop_loops(stop, loops).await;
    result.and(stopped)
}
