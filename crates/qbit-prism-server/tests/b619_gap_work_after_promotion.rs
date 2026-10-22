//! #619 without qbitd: after an asynchronous (D3) promotion, work a frontend
//! prepared on the old primary inside the replication gap must not be payout
//! authority on the promoted primary, while work whose window did replicate
//! keeps it.
//!
//! A disposable PostgreSQL 16 primary streams to one asynchronous standby
//! through a replication relay. One native `Coordinator` reaches the primary
//! through a stable writer relay and mines against the in-process fake node of
//! `support/fake_qbitd.rs`, so its offer and landing paths run for real. The
//! drill follows the HA reference's promotion procedure: fence the writers,
//! stop the old primary, promote the standby and move the writer endpoint.
//! The frontend process survives all of it, as #474 slice B's frontends do.
//! The tests call the `MiningBackend` methods a Stratum session calls
//! (`build_job`, `persist_issued_job`, `resume_job`, `submit`); the socket
//! layer is not exercised.
//!
//! In the asynchronous drill replication is cut first and 80 shares are
//! credited in the gap: more than PostgreSQL's 32 pre-logged sequence values
//! plus one 15-share window, so the promoted primary hands the gap window's
//! `share_seq` values out again to other shares.
//!
//! - **Mode 1** (#619's report): a session still holds a job issued in the
//!   gap. A block proof on it after the promotion is refused `stale-job`
//!   before its enqueue, counted under `window_not_held`, and its share is
//!   not credited. Before the fix it was offered, the node took it, and its
//!   landing failed for good with `window range incomplete`.
//! - **Mode 2** (#619's second comment): once the promoted ledger has
//!   reissued the gap window's numbers, a new session is not issued the gap
//!   work; the next refresh rebuilds under trigger `writer_timeline`, and the
//!   new session's work reads whole and its block lands.
//! - **Out of order, still honoured:** a planned switchover that replays
//!   everything keeps pre-switch work, and a held pre-switch job's block lands;
//!   a job persisted before the cut is resumed after an asynchronous promotion
//!   and its block lands.
//!
//! See `docs/prism-async-promotion-gap-blocks.md`. Run through:
//! `test/prism-native-tests.sh cargo-args --locked -p qbit-prism-server --test
//! b619_gap_work_after_promotion -- --nocapture`

use anyhow::{bail, ensure, Context, Result};
use qbit_prism::AcceptedShare;
use qbit_prism_server::{
    codec,
    coordinator::{Coordinator, JobContext, Prepared},
    ledger::{BalanceSource, WindowRef},
    metrics::Metrics,
    stratum::{MiningBackend, MiningJob, StratumError, Worker},
};
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::{
    collections::BTreeMap,
    future::Future,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicBool, AtomicU16, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle, time::sleep};

#[allow(dead_code)]
#[path = "support/fake_qbitd.rs"]
mod fake_qbitd;

/// The dedicated standby's `application_name` and physical slot.
const STANDBY: &str = "prism_standby_619";
/// The fake node's `207fffff` bits are a network difficulty of 1,000,000, and
/// the window is eight times that, so fifteen shares of this difficulty make
/// one window: the count #619 reports.
const SHARE_DIFFICULTY: u128 = 533_334;
const WINDOW_SHARES: u64 = 15;
/// Shares credited and replicated before the cut.
const REPLICATED: u64 = 20;
/// Shares credited after the cut: in the gap, or replicated by a switchover.
const GAP: u64 = 80;
/// The fake node's tip and its template's coinbase value.
const TIP_HEIGHT: u64 = 100;
const COINBASE_VALUE: u64 = 5_000_000_000;
/// A share target every proof meets, as the compact runtime fixtures use.
const DIFFICULTY: f64 = 1e-12;
const NOT_HELD: &str = "qbit_prism_stale_job_rejections_total{cause=\"window_not_held\"}";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_on_a_job_issued_in_the_gap_is_refused_stale_job_before_its_offer() -> Result<()> {
    let Some(bin) = gate::pg_bin_dir(gate::site!())? else {
        return Ok(());
    };
    let drill = Drill::promote(bin.into(), Promotion::AsyncGap).await?;
    let result = mode_one(&drill).await;
    drill.finish(result).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn work_prepared_in_the_gap_is_not_issued_to_a_new_session_after_an_async_promotion(
) -> Result<()> {
    let Some(bin) = gate::pg_bin_dir(gate::site!())? else {
        return Ok(());
    };
    let drill = Drill::promote(bin.into(), Promotion::AsyncGap).await?;
    let result = mode_two(&drill).await;
    drill.finish(result).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fenced_switchover_with_full_replay_keeps_pre_switch_work_and_lands_its_block(
) -> Result<()> {
    let Some(bin) = gate::pg_bin_dir(gate::site!())? else {
        return Ok(());
    };
    let drill = Drill::promote(bin.into(), Promotion::FencedFullReplay).await?;
    let result = fenced_switchover(&drill).await;
    drill.finish(result).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_job_persisted_before_the_cut_is_resumed_after_an_async_promotion_and_lands() -> Result<()>
{
    let Some(bin) = gate::pg_bin_dir(gate::site!())? else {
        return Ok(());
    };
    let drill = Drill::promote(bin.into(), Promotion::AsyncGap).await?;
    let result = resumed_durable_job(&drill).await;
    drill.finish(result).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refusal_that_finishes_after_the_acknowledgement_deadline_is_still_counted_once(
) -> Result<()> {
    let Some(bin) = gate::pg_bin_dir(gate::site!())? else {
        return Ok(());
    };
    let mut drill = Drill::setup(bin.into(), Promotion::AsyncGap, |config| {
        config.share_commit_timeout = Duration::from_secs(2);
        config.share_commit_grace = Duration::from_secs(1);
    })
    .await?;
    let result = match drill.fail_over().await {
        Ok(()) => late_refusal(&drill).await,
        Err(error) => Err(error),
    };
    drill.finish(result).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_candidate_accepted_before_the_promotion_cannot_be_published_after_it() -> Result<()> {
    let Some(bin) = gate::pg_bin_dir(gate::site!())? else {
        return Ok(());
    };
    let mut drill = Drill::setup(bin.into(), Promotion::AsyncGap, |_| {}).await?;
    let result = out_of_order(&mut drill).await;
    drill.finish(result).await
}

/// Mode 1: the surviving session's gap-issued job, after the frontend itself
/// has moved on to work prepared from the promoted ledger.
async fn mode_one(d: &Drill) -> Result<()> {
    // A few credits on the promoted primary, as #474 B's run had (fresh,
    // kept and replayed shares) before the gap-issued block was submitted.
    let reissued = d.credit_after_promotion(13).await?;
    // The frontend's next work is prepared from the promoted ledger, and the
    // rebuild is attributed to the new writer timeline.
    d.node.set_template(Some(template(COINBASE_VALUE + 2)));
    d.a.refresh_once().await?;
    let fresh = published(&d.a).await?;
    ensure!(
        fresh.window != d.gap_work.window,
        "the frontend kept the gap work"
    );
    let fresh_read =
        d.a.ledger
            .read_window(&fresh.window, BalanceSource::Current)
            .await;
    ensure!(
        fresh_read.is_ok(),
        "work prepared after the promotion must read whole: {fresh_read:?}"
    );

    // The session that was issued the gap job is still connected and
    // submits a block solved on it.
    d.node.accept_blocks();
    let (proof, hash) = find_block_proof(&d.held)?;
    let answer = d.a.submit(&d.worker, &d.held, proof, false.into()).await;
    let outcome = d.outcome(&hash, answer).await?;
    let credited = credited(d, &d.worker, &hash).await?;
    eprintln!(
        "#619 mode 1: replicated through share_seq {}; gap {}..={} lost; post-promotion credits reused share_seq {:?}..={:?}; \
         held job's window {}; fresh work's window {}; block {hash}: {}; window_not_held refusals {}; share credited {credited}",
        d.replicated_through,
        d.replicated_through + 1,
        d.old_last(),
        reissued.first().map(|(seq, _)| seq),
        reissued.last().map(|(seq, _)| seq),
        describe(&d.gap_work.window),
        describe(&fresh.window),
        outcome.describe(),
        metric(&d.a, NOT_HELD),
    );
    ensure!(
        outcome.refused_before_enqueue(),
        "#619 mode 1: a block on a job issued in the replication gap must be refused stale-job \
         before its enqueue: {}",
        outcome.describe()
    );
    ensure!(credited == 0, "the refused block's share was credited");
    ensure!(
        metric(&d.a, NOT_HELD) == 1.,
        "the refusal is not counted once under window_not_held: {}",
        metric(&d.a, NOT_HELD)
    );
    ensure!(
        refreshes(&d.a, "writer_timeline") == 1,
        "the rebuild was not attributed to the writer timeline"
    );
    Ok(())
}

/// Mode 2: once the promoted primary has reissued the gap window's numbers, a
/// new session is never issued the gap work; it gets work prepared from the
/// promoted ledger, which reads whole and lands.
async fn mode_two(d: &Drill) -> Result<()> {
    let range = d
        .gap_work
        .window
        .shares
        .context("the gap work has an empty window")?;
    // Credit on the promoted primary until its sequence has handed out every
    // number in the gap window again.
    let reissued = d.credit_until(range.last_share_seq).await?;
    let rows = d
        .promoted_rows(range.first_share_seq, range.last_share_seq)
        .await?;
    let anchor = d.gap_work.snapshot.anchor_ms;
    let replaced = rows
        .iter()
        .filter(|(seq, id, accepted_at)| {
            d.old.get(seq).is_some_and(|old| old != id) && *accepted_at > anchor
        })
        .count();
    ensure!(
        rows.len() as u64 == range.share_count && replaced == rows.len(),
        "every row in the gap window's range must now be another share credited after its \
         anchor: {} rows, {replaced} replaced",
        rows.len()
    );

    // Before the frontend's next refresh, the gap work is still published.
    // Its endpoints exist again, but they are not its rows: issuance refuses.
    let worker = d.a.authorize("b619.new-session").await?;
    // Its deferral WARN names the cause, window_not_held after a timeline
    // change, never the generic stale payout snapshot.
    let logs = Logs::default();
    let before_refresh = {
        let _guard = tracing::subscriber::set_default(logs.subscriber());
        d.issue(&worker, "5e6f7a8b").await
    };
    ensure!(
        before_refresh.is_err(),
        "the gap work was issued after the promotion"
    );
    let text = logs.text();
    ensure!(
        text.contains("job preparation deferred")
            && text.contains("window_not_held")
            && !text.contains("payout snapshot stale"),
        "the deferral does not name the cause: {text}"
    );
    // A job with no stored issuance proof begins one at persistence; that
    // path names the cause too.
    let unproven = MiningJob {
        wire: d.held.wire.clone(),
        context: Arc::new(JobContext {
            prepared: d.held.context.prepared.clone(),
            worker: d.held.context.worker.clone(),
            bundle: d.held.context.bundle.clone(),
            bootstrap_share: d.held.context.bootstrap_share.clone(),
            issuance_authority: None,
        }),
    };
    let logs = Logs::default();
    let persisted = {
        let _guard = tracing::subscriber::set_default(logs.subscriber());
        d.a.persist_issued_job(&d.worker, &unproven, 0, Duration::from_secs(300))
            .await
    };
    let text = logs.text();
    ensure!(
        persisted.is_err()
            && text.contains("job persistence deferred")
            && text.contains("window_not_held")
            && !text.contains("payout snapshot stale"),
        "persisting gap work must be deferred naming the cause: {persisted:?} {text}"
    );

    // The refresh rebuilds across the timeline change on an unchanged
    // template, revision and balances, inside the reanchor interval.
    d.a.refresh_once().await?;
    let rebuilt = published(&d.a).await?;
    ensure!(
        rebuilt.window != d.gap_work.window,
        "the refresh kept the gap work published"
    );
    ensure!(
        refreshes(&d.a, "writer_timeline") == 1,
        "the rebuild was not attributed to the writer timeline"
    );
    let job = d.issue(&worker, "5e6f7a8b").await?;
    let read =
        d.a.ledger
            .read_window(&job.context.prepared.window, BalanceSource::Current)
            .await
            .map(|window| window.shares.len());

    d.node.accept_blocks();
    let (proof, hash) = find_block_proof(&job)?;
    let answer = d.a.submit(&worker, &job, proof, false.into()).await;
    let outcome = d.outcome(&hash, answer).await?;
    eprintln!(
        "#619 mode 2: replicated through share_seq {}; gap {}..={} lost; post-promotion credits reused share_seq {:?}..={:?}; \
         issuance before the refresh: {:?}; new session's job {} on window {}; read on the promoted ledger: {read:?}; block {hash}: {}",
        d.replicated_through,
        d.replicated_through + 1,
        d.old_last(),
        reissued.first().map(|(seq, _)| seq),
        reissued.last().map(|(seq, _)| seq),
        before_refresh.as_ref().err(),
        job.wire.job_id,
        describe(&job.context.prepared.window),
        outcome.describe(),
    );
    ensure!(
        read.as_ref()
            .is_ok_and(|shares| *shares as u64 == WINDOW_SHARES),
        "#619 mode 2: a job issued after the promotion must name a window the promoted ledger \
         holds; window {} read {read:?}",
        describe(&job.context.prepared.window)
    );
    ensure!(
        outcome.landed(),
        "#619 mode 2: the block on fresh work must land: {}",
        outcome.describe()
    );
    Ok(())
}

/// A planned switchover that replays everything changes the writer timeline
/// but loses nothing. Pre-switch work stays authority: a new session is still
/// issued it, and the held pre-switch job's block lands.
async fn fenced_switchover(d: &Drill) -> Result<()> {
    let worker = d.a.authorize("b619.new-session").await?;
    let issued = d.issue(&worker, "5e6f7a8b").await;
    ensure!(
        issued
            .as_ref()
            .is_ok_and(|job| job.context.prepared.window == d.gap_work.window),
        "pre-switch work whose window replicated must still be issued: {:?}",
        issued.as_ref().err()
    );
    d.a.refresh_once().await?;
    d.node.accept_blocks();
    let (proof, hash) = find_block_proof(&d.held)?;
    let answer = d.a.submit(&d.worker, &d.held, proof, false.into()).await;
    let outcome = d.outcome(&hash, answer).await?;
    eprintln!(
        "#619 fenced switchover: replicated through share_seq {}; held job's window {}; block {hash}: {}",
        d.replicated_through,
        describe(&d.gap_work.window),
        outcome.describe(),
    );
    ensure!(
        outcome.landed(),
        "a block on pre-switch work whose window replicated must land: {}",
        outcome.describe()
    );
    ensure!(
        metric(&d.a, NOT_HELD) == 0.,
        "window_not_held is not exported at zero: {}",
        metric(&d.a, NOT_HELD)
    );
    ensure!(
        refreshes(&d.a, "writer_timeline") == 1,
        "the rebuild was not attributed to the writer timeline"
    );
    Ok(())
}

/// A job persisted before the cut, on work whose window replicated, is
/// resumed by a reconnecting session after an asynchronous promotion and its
/// block lands. The gap job's row was lost: resuming it is a truthful miss.
async fn resumed_durable_job(d: &Drill) -> Result<()> {
    d.a.refresh_once().await?;
    ensure!(
        d.a.resume_job(&d.worker, &d.held.wire.job_id)
            .await?
            .is_none(),
        "the gap job's row was lost; resuming it must miss"
    );
    let job =
        d.a.resume_job(&d.durable_worker, &d.durable.wire.job_id)
            .await?
            .context("the durable job could not be resumed after the promotion")?;
    ensure!(
        job.context.prepared.window == d.durable.context.prepared.window
            && job.context.prepared.storage_key == d.durable.context.prepared.storage_key,
        "the resumed job is not the durable job's stored work"
    );
    d.node.accept_blocks();
    let (proof, hash) = find_block_proof(&job)?;
    let answer =
        d.a.submit(&d.durable_worker, &job, proof, false.into())
            .await;
    let outcome = d.outcome(&hash, answer).await?;
    eprintln!(
        "#619 resumed durable job: replicated through share_seq {}; resumed window {}; block {hash}: {}",
        d.replicated_through,
        describe(&job.context.prepared.window),
        outcome.describe(),
    );
    ensure!(
        outcome.landed(),
        "a block on a resumed job whose window replicated must land: {}",
        outcome.describe()
    );
    ensure!(
        metric(&d.a, NOT_HELD) == 0.,
        "window_not_held is not exported at zero: {}",
        metric(&d.a, NOT_HELD)
    );
    Ok(())
}

/// The ledger's `ORDER_LOCK` key (`ledger.rs`): held from a second
/// connection, it stops a share append before its enqueue, deterministically.
const ORDER_LOCK: i64 = 0x505249534d000002;

/// A held gap block whose append is stopped on `ORDER_LOCK` until after the
/// miner's acknowledgement deadline: the miner is answered
/// `ledger-outcome-unknown`, and the refusal that follows is still counted
/// once under `window_not_held`, by the ledger at the decision itself.
async fn late_refusal(d: &Drill) -> Result<()> {
    d.credit_after_promotion(13).await?;
    let promoted = PgPool::connect(&d.pair.url(d.pair.standby.port)).await?;
    let mut lock = promoted.acquire().await?;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(ORDER_LOCK)
        .execute(&mut *lock)
        .await?;
    d.node.accept_blocks();
    let (proof, hash) = find_block_proof(&d.held)?;
    let answer = d.a.submit(&d.worker, &d.held, proof, false.into()).await;
    let answered_unknown = answer
        .as_ref()
        .err()
        .is_some_and(|error| error.reason_id.as_deref() == Some("ledger-outcome-unknown"));
    let before_release = metric(&d.a, NOT_HELD);
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(ORDER_LOCK)
        .execute(&mut *lock)
        .await?;
    drop(lock);
    promoted.close().await;
    ensure!(
        answered_unknown && before_release == 0.,
        "the append must still be waiting at the deadline: answer {answer:?}, refusals {before_release}"
    );
    until("the late refusal's count", 30, || async {
        Ok(metric(&d.a, NOT_HELD) >= 1.)
    })
    .await?;
    // Nothing else is in flight: one refusal, counted once.
    sleep(Duration::from_millis(500)).await;
    let outbox: i64 =
        sqlx::query_scalar("SELECT count(*) FROM qbit_block_candidate_outbox WHERE block_hash=$1")
            .bind(&hash)
            .fetch_one(&d.a.ledger.pool)
            .await?;
    let credited = credited(d, &d.worker, &hash).await?;
    eprintln!(
        "#619 late refusal: answered {answer:?}; window_not_held {}; outbox rows {outbox}; share credited {credited}",
        metric(&d.a, NOT_HELD)
    );
    ensure!(
        metric(&d.a, NOT_HELD) == 1. && outbox == 0 && credited == 0,
        "a late refusal must be counted once and leave nothing behind"
    );
    Ok(())
}

/// EP-STATE, out of order: a candidate built and accepted under the old
/// primary's authority (enqueued there, in the gap) is published again after
/// the promotion, as a retried enqueue would. The enqueue revalidates in its
/// own transaction on the promoted primary and refuses it, whatever admitted
/// it before; the refusal is counted there too.
async fn out_of_order(d: &mut Drill) -> Result<()> {
    let worker = d.a.authorize("b619.old-timeline-block").await?;
    let job = d.issue(&worker, "2b3c4d5e").await?;
    let (proof, hash) = find_block_proof(&job)?;
    d.a.submit(&worker, &job, proof, false.into())
        .await
        .map_err(|error| anyhow::anyhow!("the block was refused on the old primary: {error:?}"))?;
    let claim =
        d.a.ledger
            .claim_candidate(60)
            .await?
            .context("the old primary holds no candidate")?;
    ensure!(
        claim.candidate.block_hash == hash,
        "a different candidate was claimed"
    );
    d.fail_over().await?;
    let refused =
        d.a.ledger
            .enqueue_candidate_once(claim.candidate.clone())
            .await
            .err()
            .context("the old timeline's candidate was published on the promoted primary")?;
    ensure!(
        refused
            .downcast_ref::<qbit_prism_server::ledger::WindowNotHeld>()
            .is_some(),
        "untyped refusal: {refused:#}"
    );
    let outbox: i64 =
        sqlx::query_scalar("SELECT count(*) FROM qbit_block_candidate_outbox WHERE block_hash=$1")
            .bind(&hash)
            .fetch_one(&d.a.ledger.pool)
            .await?;
    eprintln!(
        "#619 out of order: block {hash} accepted on the old primary, refused on the promoted one: {refused:#}; outbox rows {outbox}; window_not_held {}",
        metric(&d.a, NOT_HELD)
    );
    ensure!(outbox == 0, "the refused candidate left a row");
    ensure!(
        metric(&d.a, NOT_HELD) == 1.,
        "the refusal is not counted once: {}",
        metric(&d.a, NOT_HELD)
    );
    Ok(())
}

/// A log capture for one `tracing` default, as `candidate_window_switch` uses.
#[derive(Clone, Default)]
struct Logs(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for Logs {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Logs {
    fn subscriber(&self) -> impl tracing::Subscriber + Send + Sync {
        let logs = self.clone();
        tracing_subscriber::fmt()
            .with_writer(move || logs.clone())
            .with_ansi(false)
            .finish()
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Promotion {
    /// Cut replication, credit the gap, then promote what the standby has.
    AsyncGap,
    /// The planned switch: fence, replay through the primary's flush LSN,
    /// then promote. Nothing is lost.
    FencedFullReplay,
}

/// The promoted pair, the surviving frontend and the work it held.
struct Drill {
    a: Arc<Coordinator>,
    node: fake_qbitd::FakeNode,
    /// Every share the old primary credited, by `share_seq`.
    old: BTreeMap<u64, String>,
    /// The promoted primary's last original `share_seq`.
    replicated_through: u64,
    /// The work prepared after the first 20 shares: in the gap, or replicated.
    gap_work: Arc<Prepared>,
    /// The job issued on it, held by its session.
    held: MiningJob<JobContext>,
    worker: Worker,
    /// A job persisted, with its work, before the cut and replicated.
    durable: MiningJob<JobContext>,
    durable_worker: Worker,
    next_share: std::sync::atomic::AtomicU64,
    promotion: Promotion,
    /// Direct pools to the old primary and the standby, until the failover.
    primary: Option<PgPool>,
    standby: Option<PgPool>,
    /// Declared last: both clusters stop after the frontend is gone.
    pair: Pair,
}

impl Drill {
    async fn promote(bin: PathBuf, promotion: Promotion) -> Result<Self> {
        let mut drill = Self::setup(bin, promotion, |_| {}).await?;
        drill.fail_over().await?;
        Ok(drill)
    }

    /// Everything up to the fence, on the old primary: the replicated
    /// shares, the durable job, the cut (for `AsyncGap`), 80 more shares, the
    /// work prepared over them and a job held on it.
    async fn setup(
        bin: PathBuf,
        promotion: Promotion,
        tune: impl FnOnce(&mut qbit_prism_server::config::Config),
    ) -> Result<Self> {
        let pair = Pair::start(bin).await?;
        let node = fake_qbitd::FakeNode::open().await?;
        node.set_tip(&"ab".repeat(32), &"cd".repeat(32), TIP_HEIGHT, "01");
        let mut config =
            fake_qbitd::coordinator_config(pair.url(pair.writer.port), &node, "b619-frontend")?;
        tune(&mut config);
        let a = Coordinator::new(config, Arc::new(Metrics::default())).await?;
        let primary = PgPool::connect(&pair.url(pair.primary.port)).await?;
        let standby = PgPool::connect(&pair.url(pair.standby.port)).await?;

        let mut old = BTreeMap::new();
        for index in 1..=REPLICATED {
            let appended = a.ledger.append(share(index), None).await?;
            ensure!(appended.inserted, "share {index} was not credited");
            old.insert(appended.share.share_seq, appended.share.share_id);
        }
        a.refresh_once().await?;
        // A session is issued a job on that work, persisted with it.
        let durable_worker = a.authorize("b619.durable-session").await?;
        let durable = a
            .build_job(&durable_worker, "0a0b0c0d", DIFFICULTY, 0.0)
            .await?;
        a.persist_issued_job(&durable_worker, &durable, 0, Duration::from_secs(300))
            .await?;
        replay_through_flush(&primary, &standby, "the durable job").await?;

        if promotion == Promotion::AsyncGap {
            // The cut. Everything from here on lives only on the old primary.
            pair.replication.fence();
            until("standby replay settled after the cut", 15, || async {
                Ok(sqlx::query_scalar::<_, bool>(
                    "SELECT pg_last_wal_replay_lsn()=pg_last_wal_receive_lsn()",
                )
                .fetch_one(&standby)
                .await?)
            })
            .await?;
        }
        for index in REPLICATED + 1..=REPLICATED + GAP {
            let appended = a.ledger.append(share(index), None).await?;
            ensure!(appended.inserted, "share {index} was not credited");
            old.insert(appended.share.share_seq, appended.share.share_id);
        }
        // The template moves (fees, a new transaction) and the frontend
        // prepares work over those credits, as #474 B's 1 s reanchor did.
        node.set_template(Some(template(COINBASE_VALUE + 1)));
        a.refresh_once().await?;
        let gap_work = published(&a).await?;
        let last = *old.keys().next_back().context("no shares")?;
        let range = gap_work.window.shares.context("gap work window empty")?;
        ensure!(
            range.last_share_seq == last
                && range.share_count == WINDOW_SHARES
                && range.first_share_seq > REPLICATED,
            "the gap work's window {} must lie wholly after share_seq {REPLICATED}",
            describe(&gap_work.window),
        );
        let worker = a.authorize("b619.surviving-session").await?;
        let held = a.build_job(&worker, "1a2b3c4d", DIFFICULTY, 0.0).await?;
        a.persist_issued_job(&worker, &held, 0, Duration::from_secs(300))
            .await?;
        ensure!(
            held.context.prepared.window == gap_work.window,
            "the held job was not issued on the gap work"
        );
        Ok(Self {
            a,
            node,
            old,
            replicated_through: 0,
            gap_work,
            held,
            worker,
            durable,
            durable_worker,
            next_share: std::sync::atomic::AtomicU64::new(1000),
            promotion,
            primary: Some(primary),
            standby: Some(standby),
            pair,
        })
    }

    /// Fence the old primary's writers, read the evidence out of band, stop
    /// it, promote the standby and move the writer endpoint.
    async fn fail_over(&mut self) -> Result<()> {
        let primary = self.primary.take().context("already failed over")?;
        let standby = self.standby.take().context("already failed over")?;
        let promotion = self.promotion;
        let last = self.old_last();
        self.pair.writer.fence();
        let (flush, old_timeline): (String, String) = sqlx::query_as(
            "SELECT pg_current_wal_flush_lsn()::text,left(pg_walfile_name(pg_current_wal_lsn()),8)",
        )
        .fetch_one(&primary)
        .await?;
        if promotion == Promotion::FencedFullReplay {
            replay_through_flush(&primary, &standby, "the fenced primary's flush LSN").await?;
        }
        primary.close().await;
        self.pair.primary.stop()?;
        let gap_bytes: i64 = sqlx::query_scalar(
            "SELECT pg_wal_lsn_diff($1::pg_lsn,pg_last_wal_receive_lsn())::bigint",
        )
        .bind(&flush)
        .fetch_one(&standby)
        .await?;
        let promoted: bool = sqlx::query_scalar("SELECT pg_promote(true,60)")
            .fetch_one(&standby)
            .await?;
        ensure!(promoted, "pg_promote did not complete within 60 seconds");
        standby.close().await;
        self.pair.promoted = true;
        self.pair.writer.route_to(self.pair.standby.port);
        let a = &self.a;
        until(
            "the frontend's writer on the promoted primary",
            30,
            || async { Ok(a.ledger.payout_revision().await.is_ok()) },
        )
        .await?;
        let (max_seq, gap_kept, held_kept, durable_kept, new_timeline): (
            i64,
            bool,
            bool,
            bool,
            String,
        ) = sqlx::query_as(
            "SELECT COALESCE((SELECT max(share_seq) FROM qbit_share_ledger),0),\
             EXISTS(SELECT 1 FROM qbit_prism_jobs WHERE job_id=$1),\
             EXISTS(SELECT 1 FROM qbit_prism_jobs WHERE job_id=$2),\
             EXISTS(SELECT 1 FROM qbit_prism_jobs WHERE job_id=$3)\
               AND EXISTS(SELECT 1 FROM qbit_prism_jobs WHERE job_id=$4),\
             left(pg_walfile_name(pg_current_wal_lsn()),8)",
        )
        .bind(&self.gap_work.storage_key)
        .bind(&self.held.wire.job_id)
        .bind(&self.durable.wire.job_id)
        .bind(&self.durable.context.prepared.storage_key)
        .fetch_one(&self.a.ledger.pool)
        .await?;
        let replicated_through = u64::try_from(max_seq)?;
        ensure!(
            old_timeline != new_timeline && durable_kept,
            "the promotion must move the timeline ({old_timeline} -> {new_timeline}) and keep the durable job"
        );
        match promotion {
            Promotion::AsyncGap => ensure!(
                replicated_through == REPLICATED && !gap_kept && !held_kept && gap_bytes > 0,
                "the promotion must lose the gap: shares through {replicated_through}, gap work's record kept \
                 {gap_kept}, held job's row kept {held_kept}, {gap_bytes} WAL bytes unreplicated"
            ),
            Promotion::FencedFullReplay => ensure!(
                replicated_through == last && gap_kept && held_kept && gap_bytes <= 0,
                "the switchover must lose nothing: shares through {replicated_through} of {last}, gap work's \
                 record kept {gap_kept}, held job's row kept {held_kept}, {gap_bytes} WAL bytes unreplicated"
            ),
        }
        self.replicated_through = replicated_through;
        eprintln!(
            "#619 drill ({promotion:?}): {REPLICATED} shares replicated before {GAP} more \
             ({gap_bytes} WAL bytes unreplicated); work window {}; timeline {old_timeline} -> {new_timeline}",
            describe(&self.gap_work.window),
        );
        Ok(())
    }

    /// Build and persist a job for `worker`, as a session's subscribe does.
    async fn issue(
        &self,
        worker: &Worker,
        extranonce1: &str,
    ) -> Result<MiningJob<JobContext>, StratumError> {
        let job = self
            .a
            .build_job(worker, extranonce1, DIFFICULTY, 0.0)
            .await?;
        self.a
            .persist_issued_job(worker, &job, 0, Duration::from_secs(300))
            .await?;
        Ok(job)
    }

    fn old_last(&self) -> u64 {
        self.old.keys().next_back().copied().unwrap_or_default()
    }

    /// Credit `count` new shares on the promoted primary and return their
    /// `(share_seq, share_id)`; the first must reuse a gap number.
    async fn credit_after_promotion(&self, count: u64) -> Result<Vec<(u64, String)>> {
        let mut credited = Vec::new();
        for _ in 0..count {
            credited.push(self.credit().await?);
        }
        self.check_reuse(&credited)?;
        Ok(credited)
    }

    /// Credit new shares until the promoted sequence reaches `last`.
    async fn credit_until(&self, last: u64) -> Result<Vec<(u64, String)>> {
        let mut credited = Vec::new();
        while credited.last().is_none_or(|(seq, _)| *seq < last) {
            ensure!(
                credited.len() < 200,
                "the promoted sequence never reached {last}"
            );
            credited.push(self.credit().await?);
        }
        self.check_reuse(&credited)?;
        Ok(credited)
    }

    async fn credit(&self) -> Result<(u64, String)> {
        let index = self.next_share.fetch_add(1, Ordering::SeqCst);
        let appended = self.a.ledger.append(share(index), None).await?;
        ensure!(
            appended.inserted,
            "post-promotion share {index} was not credited"
        );
        Ok((appended.share.share_seq, appended.share.share_id))
    }

    fn check_reuse(&self, credited: &[(u64, String)]) -> Result<()> {
        let first = credited.first().context("nothing credited")?.0;
        ensure!(
            first > self.replicated_through && first <= self.old_last(),
            "the promoted sequence resumed at {first}, outside the gap {}..={}",
            self.replicated_through + 1,
            self.old_last()
        );
        Ok(())
    }

    /// `(share_seq, share_id, accepted_at_ms)` of the promoted ledger's rows
    /// in `first..=last`.
    async fn promoted_rows(&self, first: u64, last: u64) -> Result<Vec<(u64, String, i64)>> {
        let rows: Vec<(i64, String, i64)> = sqlx::query_as(
            "SELECT share_seq,share_id,floor(extract(epoch FROM accepted_at)*1000)::bigint FROM qbit_share_ledger WHERE share_seq BETWEEN $1 AND $2 ORDER BY share_seq",
        )
        .bind(first as i64)
        .bind(last as i64)
        .fetch_all(&self.a.ledger.pool)
        .await?;
        rows.into_iter()
            .map(|(seq, id, at)| Ok((u64::try_from(seq)?, id, at)))
            .collect()
    }

    /// Drive the block's candidate, if one was enqueued, through the offer
    /// lifecycle exactly as the coordinator's submit loop would, then read
    /// where it ended.
    async fn outcome(&self, hash: &str, answer: Result<(), StratumError>) -> Result<Outcome> {
        let enqueued: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM qbit_block_candidate_outbox WHERE block_hash=$1)",
        )
        .bind(hash)
        .fetch_one(&self.a.ledger.pool)
        .await?;
        let mut processed = None;
        if enqueued {
            let mut claimed = None;
            for _ in 0..600 {
                if let Some(claim) = self.a.ledger.claim_candidate(60).await? {
                    ensure!(
                        claim.candidate.block_hash == hash,
                        "a different candidate was claimed"
                    );
                    claimed = Some(claim);
                    break;
                }
                sleep(Duration::from_millis(25)).await;
            }
            let claim = claimed.context("the enqueued candidate could not be claimed")?;
            processed = Some(
                self.a
                    .process_candidate(&claim)
                    .await
                    .map_err(|error| format!("{error:#}")),
            );
        }
        let row: Option<(String, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT state,offer_outcome,last_error FROM qbit_block_candidate_outbox WHERE block_hash=$1",
        )
        .bind(hash)
        .fetch_optional(&self.a.ledger.pool)
        .await?;
        let pool_block: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM qbit_pool_blocks WHERE block_hash=$1)")
                .bind(hash)
                .fetch_one(&self.a.ledger.pool)
                .await?;
        Ok(Outcome {
            answer: answer.map_err(|error| (error.reason_id.clone(), error.to_string())),
            row,
            processed,
            pool_block,
        })
    }

    async fn finish(self, result: Result<()>) -> Result<()> {
        if result.is_err() {
            eprintln!("{}", self.pair.diagnostics());
        }
        for pool in [&self.primary, &self.standby].into_iter().flatten() {
            pool.close().await;
        }
        self.a.ledger.pool.close().await;
        result
    }
}

/// Where a block proof ended: the miner's answer, the outbox row, what the
/// claim's processing returned and whether the pool recorded the block.
struct Outcome {
    answer: std::result::Result<(), (Option<String>, String)>,
    row: Option<(String, Option<String>, Option<String>)>,
    processed: Option<std::result::Result<(), String>>,
    pool_block: bool,
}

impl Outcome {
    fn refused_before_enqueue(&self) -> bool {
        self.row.is_none()
            && matches!(&self.answer, Err((Some(reason), _)) if reason == "stale-job" || reason == "unknown-job")
    }

    fn landed(&self) -> bool {
        self.pool_block
            && self
                .row
                .as_ref()
                .is_some_and(|(state, _, _)| state == "submitted")
    }

    fn describe(&self) -> String {
        format!(
            "miner answered {:?}; outbox {:?}; claim processing {:?}; qbit_pool_blocks row: {}",
            self.answer, self.row, self.processed, self.pool_block
        )
    }
}

/// Wait until the standby has replayed everything the primary has flushed.
async fn replay_through_flush(primary: &PgPool, standby: &PgPool, what: &str) -> Result<()> {
    let flush: String = sqlx::query_scalar("SELECT pg_current_wal_flush_lsn()::text")
        .fetch_one(primary)
        .await?;
    until(&format!("standby replay of {what}"), 30, || async {
        Ok(
            sqlx::query_scalar::<_, bool>("SELECT pg_last_wal_replay_lsn()>=$1::pg_lsn")
                .bind(&flush)
                .fetch_one(standby)
                .await?,
        )
    })
    .await
}

/// One rendered sample, or NaN when the series is absent.
fn metric(frontend: &Coordinator, series: &str) -> f64 {
    let body = frontend.metrics.render();
    body.lines()
        .find_map(|line| line.strip_prefix(&format!("{series} ")))
        .and_then(|value| value.parse().ok())
        .unwrap_or(f64::NAN)
}

/// Published rebuilds attributed to `trigger`, over every acquisition.
fn refreshes(frontend: &Coordinator, trigger: &str) -> u64 {
    let prefix = format!("qbit_prism_refresh_seconds_count{{trigger=\"{trigger}\",");
    frontend
        .metrics
        .render()
        .lines()
        .filter_map(|line| line.strip_prefix(&prefix))
        .filter_map(|rest| rest.rsplit_once(' '))
        .filter_map(|(_, value)| value.parse::<f64>().ok())
        .sum::<f64>() as u64
}

/// Rows crediting the share a block proof carried.
async fn credited(d: &Drill, worker: &Worker, hash: &str) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger WHERE share_id=$1")
            .bind(format!("{}:{hash}", worker.username))
            .fetch_one(&d.a.ledger.pool)
            .await?,
    )
}

async fn published(a: &Coordinator) -> Result<Arc<Prepared>> {
    a.prepared
        .read()
        .await
        .clone()
        .context("the frontend has no published work")
}

fn describe(window: &WindowRef) -> String {
    match window.shares {
        Some(range) => format!(
            "{}..={} ({} shares, anchor {})",
            range.first_share_seq, range.last_share_seq, range.share_count, window.anchor_ms
        ),
        None => format!("empty (anchor {})", window.anchor_ms),
    }
}

/// One accepted share from one of four miners with their own payout programs.
fn share(index: u64) -> AcceptedShare {
    let miner = index % 4;
    AcceptedShare {
        share_seq: 0,
        share_id: format!("b619-miner-{miner}:{index:064x}"),
        miner_id: format!("b619-miner-{miner}"),
        order_key: format!("b619-miner-{miner}"),
        p2mr_program_hex: format!("{:02x}", 0x40 + miner).repeat(32),
        share_difficulty: SHARE_DIFFICULTY,
        network_difficulty: 1_000_000,
        template_height: TIP_HEIGHT,
        job_id: "b619-job".into(),
        job_issued_at_ms: 1_700_000_000_000,
        accepted_at_ms: 0,
        ntime: 1_800_000_000,
        credit_policy: None,
    }
}

/// The fake node's own template on its tip, with another coinbase value: a
/// template change the refresh must rebuild for.
fn template(coinbase_value: u64) -> Value {
    let now = chrono::Utc::now().timestamp();
    json!({
        "height": TIP_HEIGHT + 1, "coinbasevalue": coinbase_value, "previousblockhash": "ab".repeat(32),
        "version": 0x20000000u32, "bits": fake_qbitd::TEMPLATE_BITS, "curtime": now, "mintime": now - 1,
        "transactions": []
    })
}

/// A network-target solution on `job` and its block hash.
fn find_block_proof(job: &MiningJob<JobContext>) -> Result<(codec::Submission, String)> {
    for nonce in 0..10_000u32 {
        let proof = job.wire.assemble_submission(
            &"00".repeat(job.wire.extranonce2_size),
            &format!("{:08x}", job.wire.ntime),
            &format!("{nonce:08x}"),
            None,
            0,
        )?;
        if proof.block_pass && proof.share_pass {
            let hash = proof.block_hash_hex.clone();
            return Ok((proof, hash));
        }
    }
    bail!("no network-target solution found in the bounded search")
}

async fn until<F, Fut>(label: &str, seconds: u64, mut condition: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool>>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
    let mut last = None;
    loop {
        match condition().await {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(error) => last = Some(format!("{error:#}")),
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("timed out after {seconds} s waiting for {label}; last error: {last:?}");
        }
        sleep(Duration::from_millis(20)).await;
    }
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
            .prefix("b619-")
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

    fn diagnostics(&self) -> String {
        let mut report = format!("promoted: {}\n", self.promoted);
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
                                    // PostgreSQL disables Nagle on its own sockets; the
                                    // relay does too, or its small replies wait ~40 ms.
                                    let _ = (client.set_nodelay(true), upstream.set_nodelay(true));
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
