//! Issue #375: deterministic liveness races through public coordinator, ledger,
//! broadcaster and Stratum APIs. No sleeps decide when a race is injected.
use anyhow::{ensure, Context, Result};
use qbit_prism_server::{
    broadcaster, coordinator::Coordinator, metrics::Metrics, stratum::MiningBackend,
};
use serde_json::{json, Value};
use sqlx::Row;
use std::{sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinSet, time::timeout};

#[allow(dead_code)]
#[path = "support/compact_runtime_e2e/mod.rs"]
mod support;
use support::{
    ctv_fanout::{mature_fanouts, seed_candidate},
    execution::{Fault, FaultPhase},
    run,
    socket::{ordinary_submit, Client, Listener},
    Fixture, DIFFICULTY,
};

const BOUND: Duration = Duration::from_secs(5);
const CLAIM_BARRIER: i64 = 375_001;
const PREFIX: &str = "qbit_prism_ctv_fanout_broadcaster_";

#[derive(Clone, Copy, Debug)]
enum DisconnectAt {
    BeforeRefresh,
    ChainReply,
    TemplateReply,
    WindowRead,
    PreparedCommit,
    AfterPublication,
}

async fn clients(listener: &Listener) -> Result<Vec<Client>> {
    let mut clients = Vec::new();
    for name in ["representative.rig", "alice.rig", "bob.rig"] {
        let mut client = Client::connect(listener.address).await?;
        client.configure().await?;
        client.login(name).await?;
        clients.push(client);
    }
    Ok(clients)
}

async fn disconnect(client: Client, listener: &Listener, remaining: usize) -> Result<()> {
    drop(client);
    timeout(BOUND, async {
        while listener.stats.snapshot(0).connections != remaining {
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("server did not observe the disconnect at the barrier")?;
    Ok(())
}

async fn next_job(client: &mut Client, previous: &Value) -> Result<()> {
    timeout(BOUND, async {
        loop {
            let message = client.read().await?;
            if message["method"] == "mining.notify" && message["params"][0] != *previous {
                client.notify = message;
                return Ok::<_, anyhow::Error>(());
            }
        }
    })
    .await
    .context("survivor did not receive refreshed work")?
}

async fn accept_share(f: &Fixture, client: &mut Client, name: &str) -> Result<()> {
    let worker = f.a.authorize(name).await?;
    let id = client.notify["params"][0]
        .as_str()
        .context("job ID")?
        .to_owned();
    let job =
        f.a.resume_job(&worker, &id)
            .await?
            .context("durable survivor work")?;
    ensure!(
        job.context.worker.username == name,
        "work inherited another session's identity"
    );
    ensure!(
        job.context.prepared.storage_key
            == f.a
                .prepared
                .read()
                .await
                .as_ref()
                .context("current publication")?
                .storage_key,
        "survivor received a different shared publication"
    );
    ensure!(
        job.wire.extranonce1 == client.extranonce1,
        "session entropy was skewed"
    );
    if let Some(share) = &job.context.bootstrap_share {
        ensure!(
            share.miner_id == worker.payout_address,
            "bootstrap reward inherited the disconnected worker"
        );
    }
    let (request, _) = ordinary_submit(&job, &worker, 42)?;
    client.send(request).await?;
    let reply = client.response(42).await?;
    ensure!(reply["result"] == true, "live share refused: {reply}");
    let accepted: i64 =
        sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger WHERE job_id=$1 AND accepted")
            .bind(id)
            .fetch_one(f.pool())
            .await?;
    ensure!(
        accepted == 1,
        "ACK did not correspond to exactly one durable share"
    );
    Ok(())
}

async fn disconnect_race(f: &Fixture, nonempty: bool, at: DisconnectAt) -> Result<()> {
    f.refresh(nonempty).await?;
    let mut listener = Listener::start(&f.a, DIFFICULTY).await?;
    let mut connected = clients(&listener).await?;
    let representative = connected.remove(0);
    let original_ids: Vec<_> = connected
        .iter()
        .map(|c| c.notify["params"][0].clone())
        .collect();
    let original =
        f.a.prepared
            .read()
            .await
            .clone()
            .context("original publication")?;
    // Force a new shared build on the SAME parent. It must preserve the same
    // payout window and survive losing whichever session connected first.
    let mut template = original.template.clone();
    template["coinbasevalue"] = json!(5_000_000_001u64);
    f.node.set_template(Some(template));
    let mut representative = Some(representative);
    if matches!(at, DisconnectAt::BeforeRefresh) {
        disconnect(representative.take().unwrap(), &listener, 2).await?;
    }
    let mut rpc = match at {
        DisconnectAt::ChainReply => Some(f.node.pause_next("getblockchaininfo")?),
        DisconnectAt::TemplateReply => Some(f.node.pause_next("getblocktemplate")?),
        _ => None,
    };
    let mut sql_lock = if matches!(at, DisconnectAt::WindowRead) {
        let mut tx = f.pool().begin().await?;
        sqlx::raw_sql("LOCK TABLE qbit_share_ledger IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *tx)
            .await?;
        Some(tx)
    } else {
        None
    };
    let commit = if matches!(at, DisconnectAt::PreparedCommit) {
        Some(f.proxy.pause_after_commit("qbit_prism_jobs", "INSERT")?)
    } else {
        None
    };
    // JoinSet aborts any in-flight refresh on failure before fixture cleanup.
    let mut tasks = JoinSet::new();
    let a = f.a.clone();
    tasks.spawn(async move { a.refresh_once().await });
    if let Some(rpc) = &mut rpc {
        timeout(BOUND, rpc.entered()).await??;
    }
    if sql_lock.is_some() {
        f.wait_for_share_read_waiter().await?;
    }
    if let Some(commit) = &commit {
        timeout(BOUND, commit.entered()).await?;
    }
    if !matches!(
        at,
        DisconnectAt::BeforeRefresh | DisconnectAt::AfterPublication
    ) {
        disconnect(representative.take().unwrap(), &listener, 2).await?;
    }
    if let Some(rpc) = rpc {
        rpc.release();
    }
    if let Some(tx) = sql_lock.take() {
        tx.rollback().await?;
    }
    if let Some(commit) = commit {
        commit.release();
    }
    timeout(BOUND, tasks.join_next())
        .await?
        .context("refresh task missing")???;
    if let Some(representative) = representative {
        disconnect(representative, &listener, 2).await?;
    }
    let refreshed =
        f.a.prepared
            .read()
            .await
            .clone()
            .context("replacement publication")?;
    ensure!(
        refreshed.storage_key != original.storage_key,
        "race did not rebuild work"
    );
    ensure!(
        refreshed.window.shares == original.window.shares,
        "disconnect skewed the shared payout window"
    );
    for ((client, previous), name) in connected
        .iter_mut()
        .zip(&original_ids)
        .zip(["alice.rig", "bob.rig"])
    {
        next_job(client, previous).await?;
        accept_share(f, client, name).await?;
    }
    // A later authorization reuses the publication without representative
    // reselection or a template fetch.
    let retained = refreshed.storage_key.clone();
    let mut reconnected = Client::connect(listener.address).await?;
    reconnected.configure().await?;
    reconnected.login("replacement.rig").await?;
    accept_share(f, &mut reconnected, "replacement.rig").await?;
    ensure!(f.a.prepared.read().await.as_ref().unwrap().storage_key == retained);
    drop(connected);
    drop(reconnected);
    listener.close().await
}

macro_rules! disconnect_test {
    ($name:ident, $nonempty:literal, $point:ident) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $name() -> Result<()> {
            run(qbit_prism_test_gate::site!(), |f| {
                Box::pin(disconnect_race(f, $nonempty, DisconnectAt::$point))
            })
            .await
        }
    };
}
disconnect_test!(bootstrap_disconnect_before_refresh, false, BeforeRefresh);
disconnect_test!(
    bootstrap_disconnect_during_chain_observation,
    false,
    ChainReply
);
disconnect_test!(
    bootstrap_disconnect_during_template_fetch,
    false,
    TemplateReply
);
disconnect_test!(bootstrap_disconnect_during_window_read, false, WindowRead);
disconnect_test!(
    bootstrap_disconnect_after_prepared_commit,
    false,
    PreparedCommit
);
disconnect_test!(
    bootstrap_disconnect_after_publication,
    false,
    AfterPublication
);
disconnect_test!(window_disconnect_before_refresh, true, BeforeRefresh);
disconnect_test!(window_disconnect_during_chain_observation, true, ChainReply);
disconnect_test!(window_disconnect_during_template_fetch, true, TemplateReply);
disconnect_test!(window_disconnect_during_window_read, true, WindowRead);
disconnect_test!(
    window_disconnect_after_prepared_commit,
    true,
    PreparedCommit
);
disconnect_test!(window_disconnect_after_publication, true, AfterPublication);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_session_disconnects_then_new_tip_work_serves_a_new_identity() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(false).await?;
            let mut listener = Listener::start(&f.a, DIFFICULTY).await?;
            let connected = clients(&listener).await?;
            for (n, client) in connected.into_iter().enumerate() {
                disconnect(client, &listener, 2 - n).await?;
            }
            let next = "ef".repeat(32);
            f.node.set_tip(&next, &"ab".repeat(32), 101, "02");
            timeout(BOUND, f.a.refresh_once()).await??;
            let mut client = Client::connect(listener.address).await?;
            client.configure().await?;
            client.login("newcomer.rig").await?;
            let worker = f.a.authorize("newcomer.rig").await?;
            let job =
                f.a.resume_job(&worker, client.notify["params"][0].as_str().unwrap())
                    .await?
                    .context("new tip work")?;
            ensure!(
                job.wire.previousblockhash == next,
                "new identity received superseded work"
            );
            accept_share(f, &mut client, "newcomer.rig").await?;
            drop(client);
            listener.close().await
        })
    })
    .await
}

fn metric(body: &str, suffix: &str) -> Result<f64> {
    let key = format!("{PREFIX}{suffix} ");
    body.lines()
        .find_map(|line| line.strip_prefix(&key))
        .context(format!("missing metric {suffix}"))?
        .parse()
        .map_err(Into::into)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ctv_yields_after_current_chunk_and_a_later_pass_finishes_remaining_rows() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(ctv_yield_case(f, false))
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_tip_serves_work_while_ctv_completion_is_held_then_ctv_yields() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(ctv_yield_case(f, true))
    })
    .await
}

/// Mark the durable completion of a claimed fanout for the proxy; claims and
/// renewals leave the recheck schedule alone, so only completions match.
async fn mark_ctv_completions(f: &Fixture) -> Result<()> {
    sqlx::raw_sql(
        r#"
            CREATE FUNCTION observe_ctv_chunk() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW.next_broadcast_attempt_at IS DISTINCT FROM OLD.next_broadcast_attempt_at THEN
                    RAISE NOTICE 'prism-execution-marker ctv_chunk UPDATE';
                END IF;
                RETURN NEW;
            END $$;
            CREATE TRIGGER observe_ctv_chunk AFTER UPDATE ON qbit_ctv_fanout_artifacts
                FOR EACH ROW EXECUTE FUNCTION observe_ctv_chunk();
        "#,
    )
    .execute(f.pool())
    .await?;
    Ok(())
}

async fn ctv_yield_case(f: &Fixture, publish_before_completion: bool) -> Result<()> {
    f.refresh(true).await?;
    let count = mature_fanouts(f, true).await?;
    mark_ctv_completions(f).await?;
    let row = f.proxy.pause_after_commit("ctv_chunk", "UPDATE")?;
    let mut tasks = JoinSet::new();
    let a = f.a.clone();
    tasks.spawn(async move { broadcaster::run_once(&a).await });
    timeout(BOUND, row.entered()).await?;
    // Hold the newer template after its chain observation so the refresh
    // is provably pending while the current chunk finishes.
    let tip = "ef".repeat(32);
    f.node.set_tip(&tip, &"ab".repeat(32), 1102, "03");
    let mut refresh_pause = f.node.pause_next("getblocktemplate")?;
    let mut refresh = JoinSet::new();
    let a = f.a.clone();
    refresh.spawn(async move { a.refresh_once().await });
    timeout(BOUND, refresh_pause.entered()).await??;
    let mut refresh_pause = Some(refresh_pause);
    if publish_before_completion {
        refresh_pause.take().unwrap().release();
        timeout(BOUND, refresh.join_next())
            .await?
            .context("refresh task")???;
        let worker = f.a.authorize("during-ctv.rig").await?;
        let work = f.a.build_job(&worker, "55667788", DIFFICULTY, 0.).await?;
        ensure!(
            work.wire.previousblockhash == tip,
            "held CTV completion blocked current work"
        );
    }
    row.release();
    let processed = timeout(BOUND, tasks.join_next())
        .await?
        .context("CTV task")???;
    ensure!(
        processed == 1,
        "CTV pass consumed {processed} rows behind a pending refresh"
    );
    ensure!(metric(&f.a.metrics.render(), "tip_refresh_yields_total")? == 1.);
    let untouched: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_ctv_fanout_artifacts WHERE claim_token IS NULL AND next_broadcast_attempt_at IS NULL").fetch_one(f.pool()).await?;
    ensure!(
        untouched == count as i64 - 1,
        "yield stranded or consumed remaining claims"
    );
    if let Some(refresh_pause) = refresh_pause {
        refresh_pause.release();
        timeout(BOUND, refresh.join_next())
            .await?
            .context("refresh task")???;
    }
    // Freeze the completed row's recheck after verifying it was scheduled.
    // A slow CI machine must not turn its five-second recheck into another
    // first-pass row while this test examines the deferred rows.
    sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET next_broadcast_attempt_at='infinity' WHERE next_broadcast_attempt_at IS NOT NULL")
            .execute(f.pool()).await?;
    let worker = f.a.authorize("survivor.rig").await?;
    let work = f.a.build_job(&worker, "1a2b3c4d", DIFFICULTY, 0.).await?;
    ensure!(
        work.wire.previousblockhash == tip,
        "new tip did not serve work"
    );
    let later = timeout(BOUND, broadcaster::run_once(&f.a)).await??;
    ensure!(
        later == count - 1,
        "later CTV pass did not finish remaining rows"
    );
    let claimed: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_ctv_fanout_artifacts WHERE claim_token IS NOT NULL OR next_broadcast_attempt_at IS NULL").fetch_one(f.pool()).await?;
    ensure!(claimed == 0, "CTV left claims or unprocessed rows behind");
    let body = f.a.metrics.render();
    ensure!(metric(&body, "chunk_rows_count")? == count as f64);
    ensure!(metric(&body, "chunk_rows_sum")? == count as f64);
    ensure!(metric(&body, "chunk_seconds_count")? == count as f64);
    ensure!(metric(&body, "chunk_seconds_sum")?.is_finite());
    let confirmed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM qbit_ctv_fanout_artifacts WHERE settlement_status='confirmed'",
    )
    .fetch_one(f.pool())
    .await?;
    ensure!(
        confirmed == count as i64,
        "failed attempts were mistaken for completed fanouts"
    );
    for line in body.lines().filter(|line| line.starts_with(PREFIX)) {
        if let Some((_, labels)) = line.split_once('{') {
            let labels = labels.split('}').next().unwrap();
            ensure!(
                labels.starts_with("le=\"") && !labels.contains(','),
                "unbounded CTV labels: {line}"
            );
        }
    }
    let series = body.lines().filter(|line| line.starts_with(PREFIX)).count();
    ensure!(
        series == 19,
        "CTV metric series exceeded their fixed bucket ladders"
    );
    // A fresh, due pass after a same-tip poll must process every row.
    sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET next_broadcast_attempt_at=NULL")
        .execute(f.pool())
        .await?;
    f.a.refresh_once().await?;
    ensure!(
        broadcaster::run_once(&f.a).await? == count,
        "same-tip refresh starved CTV"
    );
    ensure!(metric(&f.a.metrics.render(), "tip_refresh_yields_total")? == 1.);
    ensure!(
        f.a.metrics
            .render()
            .lines()
            .filter(|line| line.starts_with(PREFIX))
            .count()
            == series
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ctv_chunk_metrics_count_an_attempt_whose_completion_failed_to_persist() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let count = mature_fanouts(f, true).await?;
            mark_ctv_completions(f).await?;
            // The attempt succeeds, then its completion write loses the socket
            // after executing: the transaction aborts and the pass ends early.
            f.proxy.plan(Fault {
                table: "ctv_chunk".into(),
                op: "UPDATE".into(),
                phase: FaultPhase::AfterExecution,
            });
            let error = timeout(BOUND, broadcaster::run_once(&f.a))
                .await?
                .err()
                .context("a lost completion acknowledgement reported success")?;
            ensure!(
                f.proxy.fired().is_some(),
                "completion fault did not fire: {error:#}"
            );
            let body = f.a.metrics.render();
            ensure!(
                metric(&body, "chunk_rows_count")? == 1. && metric(&body, "chunk_rows_sum")? == 1.,
                "chunk rows missed the attempt whose completion failed"
            );
            ensure!(
                metric(&body, "chunk_seconds_count")? == 1.,
                "chunk duration missed the attempt whose completion failed"
            );
            ensure!(metric(&body, "tip_refresh_yields_total")? == 0.);
            let fenced: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM qbit_ctv_fanout_artifacts WHERE claim_token IS NOT NULL",
            )
            .fetch_one(f.pool())
            .await?;
            // #569: the aborted completion hands its claim back rather than
            // fencing the row until it expires, so a later pass finishes all.
            ensure!(
                fenced == 0,
                "aborted completion left {fenced} fenced claims"
            );
            let later = timeout(BOUND, broadcaster::run_once(&f.a)).await??;
            ensure!(
                later == count,
                "later pass finished {later} of {count} rows"
            );
            ensure!(metric(&f.a.metrics.render(), "chunk_rows_count")? == (count + 1) as f64);
            Ok(())
        })
    })
    .await
}

/// #569: a landing between a successful attempt and its completion moves the
/// payout revision, so the completion is refused. The claim must not stay
/// held for its 120-second lease: the fanout is claimable at once, with no
/// attempt recorded, and the next pass settles it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ctv_releases_the_claim_when_a_revision_bump_refuses_a_successful_attempt() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let count = mature_fanouts(f, true).await?;
            // The attempt has read its chain view and payout revision when it
            // re-checks the tip; hold it there.
            let mut held = f.node.pause_next("getbestblockhash")?;
            let a = f.a.clone();
            let pass = tokio::spawn(async move { broadcaster::run_once(&a).await });
            timeout(BOUND, held.entered()).await??;
            let fanout: String = sqlx::query_scalar(
                "SELECT fanout_txid FROM qbit_ctv_fanout_artifacts WHERE claim_token IS NOT NULL",
            )
            .fetch_one(f.pool())
            .await?;
            sqlx::query(
                "UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1 WHERE singleton",
            )
            .execute(f.pool())
            .await?;
            held.release();
            let error = timeout(BOUND, pass)
                .await??
                .err()
                .context("a completion at a moved revision reported success")?;
            ensure!(
                format!("{error:#}").contains("payout revision changed"),
                "completion failed for another reason: {error:#}"
            );
            let row = sqlx::query("SELECT claim_token IS NULL AND claim_instance_id IS NULL AND claim_expires_at IS NULL AS released,broadcast_attempt_count,next_broadcast_attempt_at<=clock_timestamp() AS due FROM qbit_ctv_fanout_artifacts WHERE fanout_txid=$1")
                .bind(&fanout).fetch_one(f.pool()).await?;
            ensure!(
                row.try_get::<bool, _>("released")?,
                "the refused completion left its claim held"
            );
            ensure!(
                row.try_get::<i64, _>("broadcast_attempt_count")? == 0
                    && row.try_get::<Option<bool>, _>("due")? == Some(true),
                "releasing the claim recorded an attempt or deferred the recheck"
            );
            // Claimable at once: the next pass settles every row, this one too.
            let later = timeout(BOUND, broadcaster::run_once(&f.a)).await??;
            ensure!(later == count, "later pass settled {later} of {count} rows");
            let settled: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_ctv_fanout_artifacts WHERE settlement_status='confirmed' AND claim_token IS NULL AND next_broadcast_attempt_at IS NOT NULL")
                .fetch_one(f.pool()).await?;
            ensure!(
                settled == count as i64,
                "{settled} of {count} fanouts ended confirmed and unclaimed"
            );
            // The release is fenced by the holder's token: a late release of an
            // expired claim leaves the frontend that took it over alone.
            sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET next_broadcast_attempt_at=NULL")
                .execute(f.pool())
                .await?;
            let stale = f.a.ledger.claim_fanout(120).await?.context("claim")?;
            sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET claim_expires_at=clock_timestamp() WHERE fanout_txid=$1")
                .bind(&stale.fanout_txid).execute(f.pool()).await?;
            let taken = f.b.ledger.claim_fanout(120).await?.context("takeover")?;
            ensure!(
                taken.fanout_txid == stale.fanout_txid,
                "fixture took over another row"
            );
            ensure!(
                !f.a.ledger.release_fanout_claim(&stale).await?,
                "a stale token released another frontend's claim"
            );
            ensure!(
                f.b.ledger.release_fanout_claim(&taken).await?,
                "the holder could not release its own claim"
            );
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ctv_settles_after_the_replacement_build_budget_when_publication_keeps_failing(
) -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let count = mature_fanouts(f, true).await?;
            // A third frontend whose replacement-build budget elapses inside
            // the test bound; the fixture frontends keep the 120-second default.
            let budget = Duration::from_secs(1);
            let mut config = (*f.a.config).clone();
            config.instance_id = "runtime-c".into();
            config.template_refresh_failure_exit = budget;
            let c = Coordinator::new(config, Arc::new(Metrics::default())).await?;
            c.refresh_once().await?;
            // Every refresh detects the newer tip, then fails before publishing.
            f.node.set_tip(&"ef".repeat(32), &"ab".repeat(32), 1102, "03");
            f.node
                .set_reply("getblocktemplate", json!([{"rules":["segwit"]}]), json!({}));
            for frontend in [&f.a, &c] {
                ensure!(
                    frontend.refresh_once().await.is_err(),
                    "a broken template fetch published work"
                );
            }
            ensure!(
                broadcaster::run_once(&f.a).await? == 0,
                "CTV claimed rows inside the replacement-build budget"
            );
            ensure!(metric(&f.a.metrics.render(), "tip_refresh_yields_total")? == 1.);
            // Retries never renew the first departure. This wait is not a race
            // injection: it only lets the configured budget elapse.
            ensure!(c.refresh_once().await.is_err());
            tokio::time::sleep(budget).await;
            ensure!(
                broadcaster::run_once(&c).await? == count,
                "settlement stayed stranded after the replacement-build budget"
            );
            ensure!(metric(&c.metrics.render(), "tip_refresh_yields_total")? == 0.);
            let claimed: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_ctv_fanout_artifacts WHERE claim_token IS NOT NULL OR next_broadcast_attempt_at IS NULL").fetch_one(f.pool()).await?;
            ensure!(claimed == 0, "CTV left claims or unprocessed rows behind");
            c.ledger.pool.close().await;
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ctv_settles_when_the_observed_tip_predates_the_pass() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let count = mature_fanouts(f, true).await?;
            let published = "ab".repeat(32);
            // The node advances before any refresh or block notification
            // observes it, so the pass reads the newer tip while the last
            // observation still names the published one. No observer moves
            // it during the pass: a yield here has no budget and would last
            // until a blocked or absent refresh caught up with the node.
            f.node.set_tip(&"ef".repeat(32), &published, 1102, "03");
            ensure!(
                f.a.observed_tip.read().await.as_deref() == Some(published.as_str()),
                "fixture observed the newer tip before the pass"
            );
            let processed = timeout(BOUND, broadcaster::run_once(&f.a)).await??;
            ensure!(
                processed == count,
                "an observation older than the pass stranded settlement ({processed} of {count} rows)"
            );
            ensure!(metric(&f.a.metrics.render(), "tip_refresh_yields_total")? == 0.);
            ensure!(
                f.a.observed_tip.read().await.as_deref() == Some(published.as_str()),
                "the CTV pass moved the tip observation"
            );
            let claimed: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_ctv_fanout_artifacts WHERE claim_token IS NOT NULL OR next_broadcast_attempt_at IS NULL").fetch_one(f.pool()).await?;
            ensure!(claimed == 0, "CTV left claims or unprocessed rows behind");
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_claim_leaves_socket_share_appends_live_and_then_makes_progress() -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| Box::pin(async move {
        f.refresh(true).await?;
        let (candidate, _) = seed_candidate(f, false).await?;
        sqlx::raw_sql(&format!(r#"
            CREATE FUNCTION pause_startup_claim() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW.claim_token IS DISTINCT FROM OLD.claim_token AND NEW.claim_token IS NOT NULL THEN
                    PERFORM pg_advisory_xact_lock({CLAIM_BARRIER});
                END IF;
                RETURN NEW;
            END $$;
            CREATE TRIGGER pause_startup_claim BEFORE UPDATE ON qbit_block_candidate_outbox
                FOR EACH ROW EXECUTE FUNCTION pause_startup_claim();
        "#)).execute(f.pool()).await?;
        let mut held = f.pool().begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)").bind(CLAIM_BARRIER).execute(&mut *held).await?;
        let (shutdown, receiver) = watch::channel(false);
        let mut tasks = JoinSet::new();
        let a = Arc::clone(&f.a);
        tasks.spawn(a.submit_loop(receiver));
        timeout(BOUND, async {
            loop {
                let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND classid=0 AND objid=$1::bigint::oid AND NOT granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))")
                    .bind(CLAIM_BARRIER).fetch_one(f.pool()).await?;
                if blocked { return Ok::<_, anyhow::Error>(()); }
                tokio::task::yield_now().await;
            }
        }).await.context("startup never entered claim")??;
        let mut listener = Listener::start(&f.a, DIFFICULTY).await?;
        let mut client = Client::connect(listener.address).await?;
        client.configure().await?;
        client.login("startup-miner.rig").await?;
        timeout(BOUND, accept_share(f, &mut client, "startup-miner.rig")).await
            .context("startup claim blocked share append")??;
        let before: i32 = sqlx::query_scalar("SELECT attempt_count FROM qbit_block_candidate_outbox WHERE block_hash=$1")
            .bind(&candidate.block_hash).fetch_one(f.pool()).await?;
        ensure!(before == 0, "claim completed before the independent append");
        held.rollback().await?;
        timeout(BOUND, async {
            loop {
                let progressed: bool = sqlx::query_scalar("SELECT attempt_count>0 AND claim_token IS NULL FROM qbit_block_candidate_outbox WHERE block_hash=$1")
                    .bind(&candidate.block_hash).fetch_one(f.pool()).await?;
                if progressed { return Ok::<_, anyhow::Error>(()); }
                tokio::task::yield_now().await;
            }
        }).await.context("claim did not make progress after release")??;
        shutdown.send_replace(true);
        timeout(BOUND, tasks.join_next()).await?.context("submit loop")? ?;
        drop(client);
        listener.close().await
    })).await
}
