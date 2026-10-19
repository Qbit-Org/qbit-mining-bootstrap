//! R1: the 3.0 pair keeps serving on rc.7 while 3.1's `migrate` takes its
//! database to 27, 28 and 31, before the cutover (the pre-migrate).
//!
//! rc.7 accepts the migrated database: its start gate
//! (`require_schema_version` in `crates/qbit-prism-server/src/ledger/
//! migration.rs` at 3.0.0-rc.7) refuses only a database missing one of the
//! migrations it requires, 2 to 26, and names any it does not know in a
//! warning, and none of 027, 028 and 031 declares a capability, so its
//! connect-time capability gate passes too. This scenario holds the running
//! release to that, with real frontends under load.

use super::*;
use sqlx::PgPool;

/// How long 3.1's one-off `migrate` may run here. The ledger is small; the
/// time is the 031 build waiting for the reader's snapshot the scenario holds.
const MIGRATE_BOUND: Duration = Duration::from_secs(300);
/// The frontends' own lock timeout. An append that waits for 027's ledger
/// lock longer than this fails instead, and its miner sees a rejected share.
const APPEND_LOCK_TIMEOUT_MS: u64 = 5_000;
/// What a frontend's log says when one of its statements was cancelled, by
/// its lock timeout or its statement timeout.
const CANCELLED: &str = "canceling statement due to";
/// rc.7's warning for a migration it does not know.
const UNKNOWN_MIGRATIONS: &str = "database has schema migrations this server does not know";

async fn versions(pool: &PgPool) -> Result<Vec<i32>> {
    Ok(
        sqlx::query_scalar("SELECT version FROM qbit_prism_schema_migrations ORDER BY version")
            .fetch_all(pool)
            .await?,
    )
}

/// The origin index's state: whether the parent exists and is valid, and
/// whether p0's leaf exists and is valid.
async fn origin_index(pool: &PgPool) -> Result<(Option<bool>, Option<bool>)> {
    Ok(sqlx::query_as(
        "SELECT (SELECT indisvalid FROM pg_index \
                 WHERE indexrelid = to_regclass('qbit_share_ledger_origin_seq_idx')), \
                (SELECT indisvalid FROM pg_index \
                 WHERE indexrelid = to_regclass('qbit_share_ledger_p0_origin_seq_idx'))",
    )
    .fetch_one(pool)
    .await?)
}

/// Poll `probe` every 20 ms until it holds; how long that took.
async fn wait_for<F, Fut>(what: &str, limit: Duration, mut probe: F) -> Result<Duration>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<bool>>,
{
    let started = Instant::now();
    loop {
        if probe().await? {
            return Ok(started.elapsed());
        }
        anyhow::ensure!(started.elapsed() < limit, "{what} within {limit:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The lines of `node`'s frontend log that name `needle`.
fn log_lines(sim: &Sim, node: Node, needle: &str) -> Vec<String> {
    std::fs::read_to_string(sim.frontend(node).log())
        .unwrap_or_default()
        .lines()
        .filter(|line| line.contains(needle))
        .map(str::to_owned)
        .collect()
}

/// What a restart of a frontend took.
struct Restart {
    /// Whether it exited on SIGTERM within the grace, rather than by SIGKILL.
    graceful: bool,
    /// From the start to ready.
    ready_in: Duration,
    /// When the balancer had it up again, on the run's clock.
    up_at_ms: u64,
}

/// Restart `node`'s rc.7 frontend with a graceful stop, as `docker stop`
/// does, once the balancer has marked it down, and wait until it is ready
/// and the balancer has it up again.
async fn restart(sim: &mut Sim, node: Node) -> Result<Restart> {
    let graceful = sim.frontend_mut(node).stop(Duration::from_secs(20))?;
    sim.balancer
        .wait_state(node.label(), false, MARK_DOWN_BOUND)
        .await?;
    sim.frontend_mut(node).start()?;
    let ready_in = sim
        .frontend(node)
        .wait_ready(Duration::from_secs(180))
        .await?;
    sim.balancer
        .wait_state(node.label(), true, Duration::from_secs(30))
        .await?;
    Ok(Restart {
        graceful,
        ready_in,
        up_at_ms: sim.clock.now_ms(),
    })
}

/// The 3.0 pair on rc.7 (A's database the writer, B's its standby, both
/// frontends writing A's) under load. A lands a block at schema 26. Then 3.1's
/// `migrate` runs once against A's database, as the pre-migrate runs it: a
/// one-off command with A's frontend environment (its writer role and DSN),
/// beside the running frontends. A reader holds a snapshot from before it,
/// as any long read on the primary can, so 031's first CREATE INDEX
/// CONCURRENTLY waits and the build is caught part-way. Meanwhile: 027 and 028
/// commit with no share refused or cancelled and every append's wait under
/// the frontends' 5 s lock timeout; appends continue during the build; A
/// lands a block; A's frontend restarts and serves with 31 unrecorded and the
/// index invalid. The reader ends, 031 completes and is recorded, the index
/// is valid on the primary and on the standby, B's frontend restarts and
/// serves at 31, and A lands a block on the migrated schema, its rows taking
/// 027's defaults. No frontend statement is cancelled throughout.
pub(super) async fn r01_rc7_premigrate(sim: &mut Sim, body: &mut Body) -> Result<()> {
    sim.load()?.resume();
    steady(sim, 6).await;
    let first = find_block(sim, body, Node::A, "A on rc.7 at schema 26", None).await?;
    // The landing supersedes the work miners hold; their shares on it are
    // refused as stale for a moment, which is not the migrate's doing.
    steady(sim, 2).await;
    let pool = sim.pool(Node::A).await?;
    let before = versions(&pool).await?;
    body.expect(
        "the database starts at rc.7's schema",
        before.last() == Some(&26),
        format!("recorded migrations {before:?}"),
    );

    // A reader's snapshot from before the migrate. 027 and 028 take table
    // locks and do not wait for it; CREATE INDEX CONCURRENTLY does.
    let mut reader = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *reader)
        .await?;
    sqlx::query("SELECT 1").execute(&mut *reader).await?;

    // 3.1's migrate, one-off, with A's frontend environment.
    let mut spec = sim.frontend(Node::A).spec.clone();
    spec.server_bin = sim.inputs.server.clone();
    let mut command = tokio::process::Command::from(spec.command(&["migrate"]));
    command.kill_on_drop(true);
    let migrate_from_ms = sim.clock.now_ms();
    sim.mark("3.1 migrate started against A's database");
    // Aborted, and the migrate killed, if a step below fails first.
    let migrate = crate::premigrate::AbortOnDrop::spawn(async move {
        tokio::time::timeout(MIGRATE_BOUND, command.output()).await
    });

    let committed = wait_for("027 and 028 committed", MIGRATE_BOUND, || {
        let pool = pool.clone();
        async move { Ok(versions(&pool).await?.contains(&28)) }
    })
    .await?;
    let committed_ms = sim.clock.now_ms();
    sim.mark(&format!(
        "027 and 028 committed {} ms after the migrate started",
        committed.as_millis()
    ));
    wait_for(
        "031's CREATE INDEX CONCURRENTLY started",
        MIGRATE_BOUND,
        || {
            let pool = pool.clone();
            async move {
                Ok(sqlx::query_scalar(
                    "SELECT EXISTS (SELECT 1 FROM pg_stat_activity \
                 WHERE datname = current_database() AND state = 'active' \
                   AND query ILIKE 'CREATE INDEX CONCURRENTLY%')",
                )
                .fetch_one(&pool)
                .await?)
            }
        },
    )
    .await?;
    let building_ms = sim.clock.now_ms();
    sim.mark("031's build waits for the reader's snapshot");
    let mid = versions(&pool).await?;
    let (parent, leaf) = origin_index(&pool).await?;
    body.expect(
        "mid-build: 27 and 28 recorded, 31 not, the index invalid",
        mid.contains(&27)
            && mid.contains(&28)
            && !mid.contains(&31)
            && parent != Some(true)
            && leaf == Some(false),
        format!("recorded {mid:?}; parent valid {parent:?}, p0's leaf valid {leaf:?}"),
    );

    // What the miners saw while 027 and 028 held their locks: every share
    // sent from the migrate's start to just after the commit answered, none
    // refused, and none waiting as long as the frontends' lock timeout. A
    // share on superseded work (`stale-job`) is refused whatever the
    // database does, and the migrate supersedes nothing (neither the tip
    // nor the payout revision); it is counted, not judged.
    steady(sim, 4).await;
    let records = sim.load()?.records();
    let window: Vec<&ShareRecord> = records
        .iter()
        .filter(|r| r.sent_ms >= migrate_from_ms && r.sent_ms <= committed_ms + 1_000)
        .collect();
    let stale = window
        .iter()
        .filter(|r| !r.accepted() && r.reason.as_deref() == Some("stale-job"))
        .count();
    let refused: Vec<String> = window
        .iter()
        .filter(|r| !r.accepted() && r.reason.as_deref() != Some("stale-job"))
        .map(|r| format!("{} ({:?})", r.outcome, r.reason))
        .collect();
    let longest_ms = window
        .iter()
        .filter_map(|r| r.answered_ms.map(|at| at.saturating_sub(r.sent_ms)))
        .max();
    body.expect(
        "027 and 028: no share refused, every append's wait under the 5 s lock timeout",
        !window.is_empty()
            && refused.is_empty()
            && window.iter().all(|r| r.answered_ms.is_some())
            && longest_ms.is_some_and(|ms| ms < APPEND_LOCK_TIMEOUT_MS),
        format!(
            "{} shares sent from the migrate's start to 1 s after 027 and 028 committed \
             ({} ms in); longest answer {longest_ms:?} ms; refused {refused:?}; \
             {stale} on superseded work",
            window.len(),
            committed.as_millis()
        ),
    );
    let during_build = accepted_from(&records, Node::A, building_ms);
    body.expect(
        "appends continue on A while 031 builds",
        during_build > 0,
        format!("{during_build} shares accepted on A's jobs since the build began"),
    );

    let mid_build = find_block(sim, body, Node::A, "A while 031 builds", None).await?;
    let restarted = restart(sim, Node::A).await?;
    sim.mark(&format!(
        "A's rc.7 frontend restarted mid-build, ready in {:.1} s",
        restarted.ready_in.as_secs_f64()
    ));
    let still = versions(&pool).await?;
    let warned = log_lines(sim, Node::A, UNKNOWN_MIGRATIONS);
    body.expect(
        "rc.7 stops gracefully, restarts and serves mid-build, warning of 27 and 28",
        restarted.graceful
            && !still.contains(&31)
            && warned.iter().any(|line| line.contains("27, 28")),
        format!(
            "stopped gracefully {}; recorded {still:?}; A's warnings {warned:?}",
            restarted.graceful
        ),
    );
    steady(sim, 4).await;
    let after_restart = accepted_from(&sim.load()?.records(), Node::A, restarted.up_at_ms);
    body.expect(
        "A takes shares again after its mid-build restart",
        after_restart > 0,
        format!("{after_restart} shares accepted on A's jobs since the balancer had it up again"),
    );

    reader.commit().await?;
    sim.mark("the reader's snapshot ended");
    let output = migrate
        .join()
        .await
        .context("the migrate task")?
        .context("3.1's migrate ran over its bound")?
        .context("running 3.1's migrate")?;
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    std::fs::write(
        sim.logs.join("migrate-3.1.log"),
        format!(
            "{}\n--- stdout\n{stdout}\n--- stderr\n{stderr}",
            output.status
        ),
    )?;
    sim.mark("3.1 migrate finished");
    body.expect(
        "3.1's migrate succeeds and reports 31 ready",
        output.status.success() && stdout.contains("26, 27, 28, 31 ready"),
        format!("{}: {}", output.status, stdout.trim()),
    );
    let after = versions(&pool).await?;
    let (parent, leaf) = origin_index(&pool).await?;
    body.expect(
        "31 recorded and the origin index valid on A",
        after.contains(&31) && parent == Some(true) && leaf == Some(true),
        format!("recorded {after:?}; parent valid {parent:?}, p0's leaf valid {leaf:?}"),
    );

    let restarted = restart(sim, Node::B).await?;
    sim.mark(&format!(
        "B's rc.7 frontend restarted at 31, ready in {:.1} s",
        restarted.ready_in.as_secs_f64()
    ));
    let warned = log_lines(sim, Node::B, UNKNOWN_MIGRATIONS);
    body.expect(
        "rc.7 stops gracefully, restarts and serves at 31, warning of 27, 28 and 31",
        restarted.graceful && warned.iter().any(|line| line.contains("27, 28, 31")),
        format!(
            "stopped gracefully {}; B's warnings {warned:?}",
            restarted.graceful
        ),
    );
    let last = find_block(sim, body, Node::A, "A at 31", None).await?;

    // Rows rc.7 wrote after 027 take its defaults: node 0, and a sync_seq
    // from qbit_prism_next_sync_seq(), which rc.7's role may call. The block
    // landed before 027 has none.
    let blocks: Vec<(String, i16, Option<i64>)> = sqlx::query_as(
        "SELECT block_hash, origin_node, sync_seq FROM qbit_pool_blocks \
         WHERE block_hash = ANY($1) ORDER BY block_hash",
    )
    .bind(vec![first.clone(), mid_build.clone(), last.clone()])
    .fetch_all(&pool)
    .await?;
    let sync_of = |hash: &str| {
        blocks
            .iter()
            .find(|(block, _, _)| block == hash)
            .map(|(_, origin, seq)| (*origin, *seq))
    };
    body.expect(
        "rc.7's landings after 027 take its defaults",
        matches!(sync_of(&first), Some((0, None)))
            && matches!(sync_of(&mid_build), Some((0, Some(_))))
            && matches!(sync_of(&last), Some((0, Some(_)))),
        format!("(block, origin_node, sync_seq): {blocks:?}"),
    );
    let jobs: (i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE sync_seq IS NOT NULL AND origin_node = 0), \
                count(*) FILTER (WHERE origin_node <> 0) FROM qbit_prism_jobs",
    )
    .fetch_one(&pool)
    .await?;
    body.expect(
        "rc.7's jobs after 027 take its defaults",
        jobs.0 > 0 && jobs.1 == 0,
        format!(
            "{} jobs with a sync_seq, {} of another node",
            jobs.0, jobs.1
        ),
    );
    pool.close().await;

    sim.settle(SETTLE_BOUND).await?;
    let standby = sim.pool(Node::B).await?;
    let replayed = versions(&standby).await?;
    let (parent, leaf) = origin_index(&standby).await?;
    standby.close().await;
    body.expect(
        "the standby has 31 and a valid origin index by replication",
        replayed.contains(&31) && parent == Some(true) && leaf == Some(true),
        format!("standby recorded {replayed:?}; parent valid {parent:?}, p0's leaf valid {leaf:?}"),
    );
    for node in Node::BOTH {
        let cancelled = log_lines(sim, node, CANCELLED);
        body.expect(
            &format!("no statement of node {node:?}'s frontend was cancelled"),
            cancelled.is_empty(),
            format!("{cancelled:?}"),
        );
    }
    standby_holds_every_landing(sim, body).await?;
    Ok(())
}
